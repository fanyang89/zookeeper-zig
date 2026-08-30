const std = @import("std");
const jute = @import("jute.zig");

pub const data = @import("protocol/data.zig");
pub const proto = @import("protocol/proto.zig");
pub const quorum = @import("protocol/quorum.zig");
pub const persistence = @import("protocol/persistence.zig");
pub const txn = @import("protocol/txn.zig");

pub const record_types = .{
    data.Id,
    data.ACL,
    data.Stat,
    data.StatPersisted,
    data.ClientInfo,
    proto.ConnectRequest,
    proto.ConnectResponse,
    proto.SetWatches,
    proto.SetWatches2,
    proto.RequestHeader,
    proto.MultiHeader,
    proto.AuthPacket,
    proto.ReplyHeader,
    proto.GetDataRequest,
    proto.SetDataRequest,
    proto.ReconfigRequest,
    proto.SetDataResponse,
    proto.GetSASLRequest,
    proto.SetSASLRequest,
    proto.SetSASLResponse,
    proto.CreateRequest,
    proto.CreateTTLRequest,
    proto.DeleteRequest,
    proto.GetChildrenRequest,
    proto.GetAllChildrenNumberRequest,
    proto.GetChildren2Request,
    proto.CheckVersionRequest,
    proto.GetMaxChildrenRequest,
    proto.GetMaxChildrenResponse,
    proto.SetMaxChildrenRequest,
    proto.SyncRequest,
    proto.SyncResponse,
    proto.GetACLRequest,
    proto.SetACLRequest,
    proto.SetACLResponse,
    proto.AddWatchRequest,
    proto.WatcherEvent,
    proto.ErrorResponse,
    proto.CreateResponse,
    proto.Create2Response,
    proto.ExistsRequest,
    proto.ExistsResponse,
    proto.GetDataResponse,
    proto.GetChildrenResponse,
    proto.GetAllChildrenNumberResponse,
    proto.GetChildren2Response,
    proto.GetACLResponse,
    proto.CheckWatchesRequest,
    proto.RemoveWatchesRequest,
    proto.GetEphemeralsRequest,
    proto.GetEphemeralsResponse,
    proto.WhoAmIResponse,
    quorum.LearnerInfo,
    quorum.QuorumPacket,
    quorum.QuorumAuthPacket,
    persistence.FileHeader,
    txn.TxnDigest,
    txn.TxnHeader,
    txn.CreateTxnV0,
    txn.CreateTxn,
    txn.CreateTTLTxn,
    txn.CreateContainerTxn,
    txn.DeleteTxn,
    txn.SetDataTxn,
    txn.CheckVersionTxn,
    txn.SetACLTxn,
    txn.SetMaxChildrenTxn,
    txn.CreateSessionTxn,
    txn.CloseSessionTxn,
    txn.ErrorTxn,
    txn.Txn,
    txn.MultiTxn,
};

test {
    _ = data.Stat;
    _ = proto.ConnectRequest;
    _ = quorum.QuorumPacket;
    _ = persistence.FileHeader;
    _ = txn.TxnHeader;
}

test "all ZooKeeper records instantiate the reflection codec" {
    const testing = std.testing;
    try testing.expectEqual(@as(usize, 72), record_types.len);

    inline for (record_types) |Record| {
        const expected: Record = std.mem.zeroes(Record);
        var writer = jute.Writer.init(testing.allocator);
        defer writer.deinit();
        try jute.serialize(&writer, expected);

        var reader = jute.Reader.init(writer.bytes());
        const decoded = try jute.deserialize(Record, &reader, testing.allocator);
        defer jute.deinitDecoded(decoded, testing.allocator);
        try testing.expectEqual(writer.dataSize(), reader.consumed());
    }
}

test "ConnectRequest matches the ZooKeeper Jute wire layout" {
    const testing = std.testing;
    const request = proto.ConnectRequest{
        .protocolVersion = 0,
        .lastZxidSeen = 1,
        .timeOut = 30_000,
        .sessionId = 2,
        .passwd = &.{ 0xaa, 0xbb },
        .readOnly = true,
    };

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try jute.serialize(&writer, request);

    const expected = [_]u8{
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x75, 0x30,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x02,
        0xaa, 0xbb, 0x01,
    };
    try testing.expectEqualSlices(u8, &expected, writer.bytes());

    var reader = jute.Reader.init(writer.bytes());
    const decoded = try jute.deserialize(proto.ConnectRequest, &reader, testing.allocator);
    defer jute.deinitDecoded(decoded, testing.allocator);
    try testing.expectEqual(request.protocolVersion, decoded.protocolVersion);
    try testing.expectEqual(request.lastZxidSeen, decoded.lastZxidSeen);
    try testing.expectEqual(request.timeOut, decoded.timeOut);
    try testing.expectEqual(request.sessionId, decoded.sessionId);
    try testing.expectEqualSlices(u8, request.passwd.?, decoded.passwd.?);
    try testing.expectEqual(request.readOnly, decoded.readOnly);
}

test "reflection handles nested ZooKeeper vectors and records" {
    const testing = std.testing;
    const paths = [_]?[]const u8{ "/a", "/b" };
    const watches = proto.SetWatches{
        .relativeZxid = 9,
        .dataWatches = &paths,
        .existWatches = null,
        .childWatches = &.{},
    };

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try jute.serialize(&writer, watches);

    var reader = jute.Reader.init(writer.bytes());
    const decoded = try jute.deserialize(proto.SetWatches, &reader, testing.allocator);
    defer jute.deinitDecoded(decoded, testing.allocator);
    try testing.expectEqual(@as(i64, 9), decoded.relativeZxid);
    try testing.expectEqual(@as(usize, 2), decoded.dataWatches.?.len);
    try testing.expectEqualStrings("/a", decoded.dataWatches.?[0].?);
    try testing.expectEqualStrings("/b", decoded.dataWatches.?[1].?);
    try testing.expectEqual(@as(?[]const ?[]const u8, null), decoded.existWatches);
    try testing.expectEqual(@as(usize, 0), decoded.childWatches.?.len);
}
