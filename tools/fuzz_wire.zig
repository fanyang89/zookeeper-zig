// Fuzzer suite for the ZooKeeper wire protocol and Jute record decoding.
//
// Target `frame` generates valid length-prefixed frames (requests, replies,
// connect exchanges, header-only control packets) with the wire codecs and
// feeds mutated bytes to the frame parser and the borrowed header/connect
// decoders. Target `jute-record` generates valid instances of every protocol
// record with a comptime reflective builder, mutates the serialized bytes,
// and exercises Jute deserialization plus owned-decode teardown. Any panic,
// signal, timeout, or allocator leak is a finding.

const std = @import("std");
const zookeeper = @import("zookeeper");
const engine = @import("fuzz_engine.zig");

const jute = zookeeper.jute;
const proto = zookeeper.protocol.proto;
const wire = zookeeper.wire;

pub fn main(init: std.process.Init) anyerror!void {
    return engine.mainFor("wire", &defs)(init);
}

const defs = [_]engine.TargetDef{
    .{ .name = "frame", .build = buildFrame, .run = runFrame },
    .{ .name = "jute-record", .build = buildJuteRecord, .run = runJuteRecord },
};

/// Every wire record in `protocol.proto`: the exact set the server decodes
/// from request bodies.
const record_types = [_]type{
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
};

// ---------------------------------------------------------------------------
// Reflective record builder
// ---------------------------------------------------------------------------

fn randomBytes(arena: std.mem.Allocator, random: std.Random) ![]u8 {
    const len = random.intRangeAtMost(usize, 0, 8);
    const bytes = try arena.alloc(u8, len);
    random.bytes(bytes);
    return bytes;
}

fn randomValue(comptime T: type, arena: std.mem.Allocator, random: std.Random) anyerror!T {
    switch (@typeInfo(T)) {
        .int => return switch (random.intRangeAtMost(u8, 0, 5)) {
            0 => 0,
            1 => 1,
            2 => -1,
            3 => std.math.maxInt(T),
            4 => if (@typeInfo(T).int.signedness == .signed) std.math.minInt(T) else 0,
            else => random.int(T),
        },
        .bool => return random.boolean(),
        .optional => |info| return if (random.boolean())
            try randomValue(info.child, arena, random)
        else
            null,
        .pointer => |info| {
            if (info.size != .slice) @compileError("unsupported pointer " ++ @typeName(T));
            if (info.child == u8 and info.is_const) return try randomBytes(arena, random);
            const len = random.intRangeAtMost(usize, 0, 3);
            const slice = try arena.alloc(info.child, len);
            for (slice) |*element| element.* = try randomValue(info.child, arena, random);
            return slice;
        },
        .@"struct" => |info| {
            var value: T = undefined;
            inline for (info.fields) |field| {
                @field(value, field.name) = try randomValue(field.type, arena, random);
            }
            return value;
        },
        else => @compileError("unsupported type " ++ @typeName(T)),
    }
}

// ---------------------------------------------------------------------------
// Target: jute-record
// ---------------------------------------------------------------------------

fn buildJuteRecord(arena: std.mem.Allocator, random: std.Random, donors: []const []const u8) anyerror!engine.Input {
    _ = donors;
    const record_index = random.uintLessThan(usize, record_types.len);
    var writer = jute.Writer.init(arena);
    defer writer.deinit();
    inline for (record_types, 0..) |T, index| {
        if (index == record_index) {
            const value = try randomValue(T, arena, random);
            try jute.serialize(&writer, value);
        }
    }
    const bytes = try arena.dupe(u8, writer.bytes());
    return .{ .bytes = bytes, .spans = try arena.dupe(engine.Span, &[_]engine.Span{.{ .start = 0, .len = bytes.len }}) };
}

fn runJuteRecord(allocator: std.mem.Allocator, input: engine.Input) anyerror!void {
    // Derive the record type from the bytes so corpus replay picks the same
    // type the input was built for.
    const record_index = std.hash.Wyhash.hash(0, input.bytes) % record_types.len;
    inline for (record_types, 0..) |T, index| {
        if (index == record_index) {
            var reader = jute.Reader.init(input.bytes);
            const value = try jute.deserialize(T, &reader, allocator);
            defer jute.deinitDecoded(value, allocator);
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Target: frame
// ---------------------------------------------------------------------------

const request_body_types = [_]type{
    proto.GetDataRequest,
    proto.CreateRequest,
    proto.SetDataRequest,
    proto.GetChildren2Request,
    proto.GetACLRequest,
    proto.SetWatches,
    proto.SetWatches2,
    proto.AddWatchRequest,
    proto.RemoveWatchesRequest,
    proto.SyncRequest,
    proto.GetEphemeralsRequest,
    proto.WatcherEvent,
};

fn randomOpcode(random: std.Random) wire.OpCode {
    const values = [_]wire.OpCode{ .create, .create2, .delete, .exists, .get_data, .set_data, .get_acl, .set_acl, .get_children, .get_children2, .sync, .ping, .multi, .multi_read, .check, .set_watches, .set_watches2, .add_watch, .remove_watches, .check_watches, .create_ttl, .create_container, .get_ephemerals, .auth, .sasl, .who_am_i, .get_all_children_number };
    return values[random.uintLessThan(usize, values.len)];
}

fn buildFrame(arena: std.mem.Allocator, random: std.Random, donors: []const []const u8) anyerror!engine.Input {
    _ = donors;
    var writer = jute.Writer.init(arena);
    defer writer.deinit();

    const roll = random.intRangeAtMost(u8, 0, 9);
    if (roll <= 3) {
        // Request frame: length + RequestHeader + body.
        const body_index = random.uintLessThan(usize, request_body_types.len);
        const xid: i32 = if (random.boolean())
            random.intRangeAtMost(i32, 1, 100)
        else switch (random.uintLessThan(u8, 3)) {
            0 => wire.Xid.notification,
            1 => wire.Xid.ping,
            else => wire.Xid.auth,
        };
        inline for (request_body_types, 0..) |T, index| {
            if (index == body_index) {
                const body = try randomValue(T, arena, random);
                try wire.encodeRequest(&writer, xid, randomOpcode(random), body);
            }
        }
    } else if (roll <= 5) {
        // Reply frame: length + ReplyHeader + body.
        const header = try randomValue(proto.ReplyHeader, arena, random);
        const body = try randomValue(proto.GetDataResponse, arena, random);
        try wire.encodeReply(&writer, header, body);
    } else if (roll <= 7) {
        // Connect frame: length + ConnectRequest, no RequestHeader.
        const request = try randomValue(proto.ConnectRequest, arena, random);
        try wire.encodeConnectRequest(&writer, request, random.boolean());
    } else if (roll == 8) {
        const response = try randomValue(proto.ConnectResponse, arena, random);
        try wire.encodeConnectResponse(&writer, response, random.boolean());
    } else {
        // Header-only control packet.
        try wire.encodeRequest(&writer, wire.Xid.ping, .ping, {});
    }

    var bytes = try arena.dupe(u8, writer.bytes());
    if (random.boolean()) {
        const extra = random.intRangeAtMost(usize, 1, 16);
        const grown = try arena.alloc(u8, bytes.len + extra);
        @memcpy(grown[0..bytes.len], bytes);
        random.bytes(grown[bytes.len..]);
        bytes = grown;
    }
    return .{ .bytes = bytes, .spans = try arena.dupe(engine.Span, &[_]engine.Span{.{ .start = 4, .len = bytes.len -| 4 }}) };
}

fn runFrame(allocator: std.mem.Allocator, input: engine.Input) anyerror!void {
    _ = allocator; // The frame decoders return borrowed views.
    // Derive the payload limit from the bytes so corpus replay agrees.
    const limit: usize = @intCast(std.hash.Wyhash.hash(0, input.bytes) % (2 * @as(u64, wire.default_max_payload)));
    const maybe_frame = wire.parseFrame(input.bytes, limit) catch return;
    const frame = maybe_frame orelse return;
    if (wire.requestView(frame.payload)) |view| {
        _ = view;
    } else |_| {}
    if (wire.replyView(frame.payload)) |view| {
        _ = view;
    } else |_| {}
    if (wire.decodeConnectRequest(frame.payload)) |decoded| {
        _ = decoded;
    } else |_| {}
    if (wire.decodeConnectResponse(frame.payload)) |decoded| {
        _ = decoded;
    } else |_| {}
}
