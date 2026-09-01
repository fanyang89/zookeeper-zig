const std = @import("std");
const jute = @import("jute.zig");
const data = @import("protocol/data.zig");
const proto = @import("protocol/proto.zig");

pub const default_max_payload: usize = 0xfffff;

pub const Limits = struct {
    max_payload_size: usize = default_max_payload,
    jute: jute.Limits = .{},
};

pub const Xid = struct {
    pub const notification: i32 = -1;
    pub const ping: i32 = -2;
    pub const auth: i32 = -4;
    pub const set_watches: i32 = -8;
};

pub const OpCode = enum(i32) {
    notification = 0,
    create = 1,
    delete = 2,
    exists = 3,
    get_data = 4,
    set_data = 5,
    get_acl = 6,
    set_acl = 7,
    get_children = 8,
    sync = 9,
    ping = 11,
    get_children2 = 12,
    check = 13,
    multi = 14,
    create2 = 15,
    reconfig = 16,
    check_watches = 17,
    remove_watches = 18,
    create_container = 19,
    delete_container = 20,
    create_ttl = 21,
    multi_read = 22,
    auth = 100,
    set_watches = 101,
    sasl = 102,
    get_ephemerals = 103,
    get_all_children_number = 104,
    set_watches2 = 105,
    add_watch = 106,
    who_am_i = 107,
    create_session = -10,
    close_session = -11,
    @"error" = -1,

    pub fn fromInt(value: i32) ?OpCode {
        return std.enums.fromInt(OpCode, value);
    }
};

pub const FrameError = error{
    InvalidFrameLength,
    FrameTooLarge,
};

pub const DecodeError = jute.DeserializeError || FrameError || error{TrailingData};
pub const EncodeError = jute.SerializeError || error{FrameTooLarge};

/// A frame borrowed from the input passed to `parseFrame`. The input must not
/// move, change, or be freed while `payload` is in use.
pub const Frame = struct {
    payload: []const u8,
    consumed: usize,
};

/// A header and body borrowed from the request payload.
pub const RequestView = struct {
    header: proto.RequestHeader,
    body: []const u8,
};

/// A header and body borrowed from the reply payload.
pub const ReplyView = struct {
    header: proto.ReplyHeader,
    body: []const u8,
};

/// `value.passwd` borrows from the decoded payload.
pub const DecodedConnectRequest = struct {
    value: proto.ConnectRequest,
    read_only_supported: bool,
};

/// `value.passwd` borrows from the decoded payload.
pub const DecodedConnectResponse = struct {
    value: proto.ConnectResponse,
    read_only_supported: bool,
};

/// `body` may contain byte slices borrowed from the payload passed to
/// `decodeRequest`. Keep that payload stable until `deinit` has returned.
pub fn Request(comptime Body: type) type {
    return struct {
        header: proto.RequestHeader,
        body: Body,

        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (Body != void) jute.deinitDecoded(self.body, allocator);
        }
    };
}

/// `body` may contain byte slices borrowed from the payload passed to
/// `decodeReply`. Keep that payload stable until `deinit` has returned.
pub fn Reply(comptime Body: type) type {
    return struct {
        header: proto.ReplyHeader,
        body: Body,

        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (Body != void) jute.deinitDecoded(self.body, allocator);
        }
    };
}

/// Owns a stable payload copy as well as vector allocations in `body`.
pub fn OwnedRequest(comptime Body: type) type {
    return struct {
        payload: []u8,
        header: proto.RequestHeader,
        body: Body,

        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (Body != void) jute.deinitDecoded(self.body, allocator);
            allocator.free(self.payload);
        }
    };
}

/// Owns a stable payload copy as well as vector allocations in `body`.
pub fn OwnedReply(comptime Body: type) type {
    return struct {
        payload: []u8,
        header: proto.ReplyHeader,
        body: Body,

        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            if (Body != void) jute.deinitDecoded(self.body, allocator);
            allocator.free(self.payload);
        }
    };
}

/// Owns a stable payload copy as well as vector allocations in `value`.
pub fn OwnedRecord(comptime Record: type) type {
    return struct {
        payload: []u8,
        value: Record,

        pub fn deinit(self: @This(), allocator: std.mem.Allocator) void {
            jute.deinitDecoded(self.value, allocator);
            allocator.free(self.payload);
        }
    };
}

pub fn parseFrame(input: []const u8, max_payload: usize) FrameError!?Frame {
    if (input.len < 4) return null;

    const signed_length = std.mem.readInt(i32, input[0..4], .big);
    if (signed_length < 0) return error.InvalidFrameLength;
    const payload_length: usize = @intCast(signed_length);
    if (payload_length > max_payload) return error.FrameTooLarge;
    if (payload_length > input.len - 4) return null;

    return .{
        .payload = input[4 .. 4 + payload_length],
        .consumed = 4 + payload_length,
    };
}

pub fn parseFrameWithLimits(input: []const u8, limits: Limits) FrameError!?Frame {
    return parseFrame(input, limits.max_payload_size);
}

pub fn encodeFrame(writer: *jute.Writer, value: anytype) EncodeError!void {
    try encodeFrameWithLimit(writer, value, default_max_payload);
}

pub fn encodeFrameWithLimit(
    writer: *jute.Writer,
    value: anytype,
    max_payload: usize,
) EncodeError!void {
    const frame_start = writer.dataSize();
    errdefer writer.truncate(frame_start);

    try writer.writeInt(0);
    try jute.serialize(writer, value);
    try finishFrame(writer, frame_start, max_payload);
}

pub fn encodeRequest(
    writer: *jute.Writer,
    xid: i32,
    opcode: OpCode,
    body: anytype,
) EncodeError!void {
    try encodeRequestWithLimit(writer, xid, opcode, body, default_max_payload);
}

pub fn encodeRequestWithLimit(
    writer: *jute.Writer,
    xid: i32,
    opcode: OpCode,
    body: anytype,
    max_payload: usize,
) EncodeError!void {
    const frame_start = writer.dataSize();
    errdefer writer.truncate(frame_start);

    try writer.writeInt(0);
    try jute.serialize(writer, proto.RequestHeader{
        .xid = xid,
        .type = @intFromEnum(opcode),
    });
    if (@TypeOf(body) != void) try jute.serialize(writer, body);
    try finishFrame(writer, frame_start, max_payload);
}

pub fn encodeRequestPayloadWithLimit(
    writer: *jute.Writer,
    xid: i32,
    opcode: OpCode,
    body_payload: []const u8,
    max_payload: usize,
) EncodeError!void {
    const frame_start = writer.dataSize();
    errdefer writer.truncate(frame_start);

    try writer.writeInt(0);
    try jute.serialize(writer, proto.RequestHeader{
        .xid = xid,
        .type = @intFromEnum(opcode),
    });
    try writer.writeBytes(body_payload);
    try finishFrame(writer, frame_start, max_payload);
}

pub fn encodeReply(
    writer: *jute.Writer,
    header: proto.ReplyHeader,
    body: anytype,
) EncodeError!void {
    try encodeReplyWithLimit(writer, header, body, default_max_payload);
}

pub fn encodeReplyPayload(
    writer: *jute.Writer,
    header: proto.ReplyHeader,
    body_payload: []const u8,
) EncodeError!void {
    try encodeReplyPayloadWithLimit(writer, header, body_payload, default_max_payload);
}

pub fn encodeReplyPayloadWithLimit(
    writer: *jute.Writer,
    header: proto.ReplyHeader,
    body_payload: []const u8,
    max_payload: usize,
) EncodeError!void {
    const frame_start = writer.dataSize();
    errdefer writer.truncate(frame_start);

    try writer.writeInt(0);
    try jute.serialize(writer, header);
    try writer.writeBytes(body_payload);
    try finishFrame(writer, frame_start, max_payload);
}

pub fn encodeReplyWithLimit(
    writer: *jute.Writer,
    header: proto.ReplyHeader,
    body: anytype,
    max_payload: usize,
) EncodeError!void {
    const frame_start = writer.dataSize();
    errdefer writer.truncate(frame_start);

    try writer.writeInt(0);
    try jute.serialize(writer, header);
    if (@TypeOf(body) != void) try jute.serialize(writer, body);
    try finishFrame(writer, frame_start, max_payload);
}

pub fn encodeConnectRequest(
    writer: *jute.Writer,
    request: proto.ConnectRequest,
    include_read_only: bool,
) EncodeError!void {
    try encodeConnectRequestWithLimit(
        writer,
        request,
        include_read_only,
        default_max_payload,
    );
}

pub fn encodeConnectRequestWithLimit(
    writer: *jute.Writer,
    request: proto.ConnectRequest,
    include_read_only: bool,
    max_payload: usize,
) EncodeError!void {
    const frame_start = writer.dataSize();
    errdefer writer.truncate(frame_start);

    try writer.writeInt(0);
    try writer.writeInt(request.protocolVersion);
    try writer.writeLong(request.lastZxidSeen);
    try writer.writeInt(request.timeOut);
    try writer.writeLong(request.sessionId);
    try writer.writeBuffer(request.passwd);
    if (include_read_only) try writer.writeBool(request.readOnly);
    try finishFrame(writer, frame_start, max_payload);
}

pub fn encodeConnectResponse(
    writer: *jute.Writer,
    response: proto.ConnectResponse,
    include_read_only: bool,
) EncodeError!void {
    try encodeConnectResponseWithLimit(
        writer,
        response,
        include_read_only,
        default_max_payload,
    );
}

pub fn encodeConnectResponseWithLimit(
    writer: *jute.Writer,
    response: proto.ConnectResponse,
    include_read_only: bool,
    max_payload: usize,
) EncodeError!void {
    const frame_start = writer.dataSize();
    errdefer writer.truncate(frame_start);

    try writer.writeInt(0);
    try writer.writeInt(response.protocolVersion);
    try writer.writeInt(response.timeOut);
    try writer.writeLong(response.sessionId);
    try writer.writeBuffer(response.passwd);
    if (include_read_only) try writer.writeBool(response.readOnly);
    try finishFrame(writer, frame_start, max_payload);
}

pub fn decodeConnectRequest(payload: []const u8) DecodeError!DecodedConnectRequest {
    return decodeConnectRequestWithLimits(payload, .{});
}

pub fn decodeConnectRequestWithLimits(
    payload: []const u8,
    limits: Limits,
) DecodeError!DecodedConnectRequest {
    try checkPayloadSize(payload, limits);
    var reader = jute.Reader.initWithLimits(payload, limits.jute);
    var value = proto.ConnectRequest{
        .protocolVersion = try reader.readInt(),
        .lastZxidSeen = try reader.readLong(),
        .timeOut = try reader.readInt(),
        .sessionId = try reader.readLong(),
        .passwd = try reader.readBuffer(),
        .readOnly = false,
    };
    const supported = try readOptionalReadOnly(&reader, &value.readOnly);
    return .{ .value = value, .read_only_supported = supported };
}

pub fn decodeConnectResponse(payload: []const u8) DecodeError!DecodedConnectResponse {
    return decodeConnectResponseWithLimits(payload, .{});
}

pub fn decodeConnectResponseWithLimits(
    payload: []const u8,
    limits: Limits,
) DecodeError!DecodedConnectResponse {
    try checkPayloadSize(payload, limits);
    var reader = jute.Reader.initWithLimits(payload, limits.jute);
    var value = proto.ConnectResponse{
        .protocolVersion = try reader.readInt(),
        .timeOut = try reader.readInt(),
        .sessionId = try reader.readLong(),
        .passwd = try reader.readBuffer(),
        .readOnly = false,
    };
    const supported = try readOptionalReadOnly(&reader, &value.readOnly);
    return .{ .value = value, .read_only_supported = supported };
}

pub fn requestView(payload: []const u8) DecodeError!RequestView {
    return requestViewWithLimits(payload, .{});
}

pub fn requestViewWithLimits(payload: []const u8, limits: Limits) DecodeError!RequestView {
    try checkPayloadSize(payload, limits);
    var reader = jute.Reader.initWithLimits(payload, limits.jute);
    const header = proto.RequestHeader{
        .xid = try reader.readInt(),
        .type = try reader.readInt(),
    };
    return .{ .header = header, .body = payload[reader.consumed()..] };
}

pub fn replyView(payload: []const u8) DecodeError!ReplyView {
    return replyViewWithLimits(payload, .{});
}

pub fn replyViewWithLimits(payload: []const u8, limits: Limits) DecodeError!ReplyView {
    try checkPayloadSize(payload, limits);
    var reader = jute.Reader.initWithLimits(payload, limits.jute);
    const header = proto.ReplyHeader{
        .xid = try reader.readInt(),
        .zxid = try reader.readLong(),
        .err = try reader.readInt(),
    };
    return .{ .header = header, .body = payload[reader.consumed()..] };
}

pub fn decodeRequest(
    comptime Body: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
) DecodeError!Request(Body) {
    return decodeRequestWithLimits(Body, payload, allocator, .{});
}

pub fn decodeRequestWithLimits(
    comptime Body: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
    limits: Limits,
) DecodeError!Request(Body) {
    try checkPayloadSize(payload, limits);
    var reader = jute.Reader.initWithLimits(payload, limits.jute);
    const header = try jute.deserialize(proto.RequestHeader, &reader, allocator);
    const body: Body = if (Body == void) {} else try jute.deserialize(Body, &reader, allocator);
    errdefer if (Body != void) jute.deinitDecoded(body, allocator);
    if (reader.remaining() != 0) return error.TrailingData;
    return .{ .header = header, .body = body };
}

pub fn decodeReply(
    comptime Body: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
) DecodeError!Reply(Body) {
    return decodeReplyWithLimits(Body, payload, allocator, .{});
}

pub fn decodeReplyWithLimits(
    comptime Body: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
    limits: Limits,
) DecodeError!Reply(Body) {
    try checkPayloadSize(payload, limits);
    var reader = jute.Reader.initWithLimits(payload, limits.jute);
    const header = try jute.deserialize(proto.ReplyHeader, &reader, allocator);
    const body: Body = if (Body == void) {} else try jute.deserialize(Body, &reader, allocator);
    errdefer if (Body != void) jute.deinitDecoded(body, allocator);
    if (reader.remaining() != 0) return error.TrailingData;
    return .{ .header = header, .body = body };
}

pub fn decodeRequestOwned(
    comptime Body: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
) DecodeError!OwnedRequest(Body) {
    return decodeRequestOwnedWithLimits(Body, payload, allocator, .{});
}

pub fn decodeRequestOwnedWithLimits(
    comptime Body: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
    limits: Limits,
) DecodeError!OwnedRequest(Body) {
    const owned = try allocator.dupe(u8, payload);
    errdefer allocator.free(owned);
    const decoded = try decodeRequestWithLimits(Body, owned, allocator, limits);
    return .{ .payload = owned, .header = decoded.header, .body = decoded.body };
}

pub fn decodeReplyOwned(
    comptime Body: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
) DecodeError!OwnedReply(Body) {
    return decodeReplyOwnedWithLimits(Body, payload, allocator, .{});
}

pub fn decodeReplyOwnedWithLimits(
    comptime Body: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
    limits: Limits,
) DecodeError!OwnedReply(Body) {
    const owned = try allocator.dupe(u8, payload);
    errdefer allocator.free(owned);
    const decoded = try decodeReplyWithLimits(Body, owned, allocator, limits);
    return .{ .payload = owned, .header = decoded.header, .body = decoded.body };
}

pub fn decodeFrameRecord(
    comptime Record: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
) DecodeError!Record {
    return decodeFrameRecordWithLimits(Record, payload, allocator, .{});
}

pub fn decodeFrameRecordWithLimits(
    comptime Record: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
    limits: Limits,
) DecodeError!Record {
    try checkPayloadSize(payload, limits);
    var reader = jute.Reader.initWithLimits(payload, limits.jute);
    const value = try jute.deserialize(Record, &reader, allocator);
    errdefer jute.deinitDecoded(value, allocator);
    if (reader.remaining() != 0) return error.TrailingData;
    return value;
}

pub fn decodeFrameRecordOwned(
    comptime Record: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
) DecodeError!OwnedRecord(Record) {
    return decodeFrameRecordOwnedWithLimits(Record, payload, allocator, .{});
}

pub fn decodeFrameRecordOwnedWithLimits(
    comptime Record: type,
    payload: []const u8,
    allocator: std.mem.Allocator,
    limits: Limits,
) DecodeError!OwnedRecord(Record) {
    const owned = try allocator.dupe(u8, payload);
    errdefer allocator.free(owned);
    const value = try decodeFrameRecordWithLimits(Record, owned, allocator, limits);
    return .{ .payload = owned, .value = value };
}

fn readOptionalReadOnly(reader: *jute.Reader, value: *bool) DecodeError!bool {
    switch (reader.remaining()) {
        0 => return false,
        1 => {
            value.* = try reader.readBool();
            return true;
        },
        else => return error.TrailingData,
    }
}

fn checkPayloadSize(payload: []const u8, limits: Limits) FrameError!void {
    if (payload.len > limits.max_payload_size) return error.FrameTooLarge;
}

fn finishFrame(writer: *jute.Writer, frame_start: usize, max_payload: usize) EncodeError!void {
    const payload_length = writer.dataSize() - frame_start - 4;
    if (payload_length > max_payload or payload_length > std.math.maxInt(i32)) {
        return error.FrameTooLarge;
    }
    writer.patchInt(frame_start, @intCast(payload_length)) catch unreachable;
}

fn decodeOwnedAllocationFixture(allocator: std.mem.Allocator) !void {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x02,
        '/',  'a',  0x00, 0x00,
        0x00, 0x02, '/',  'b',
    };
    const decoded = try decodeFrameRecordOwned(
        proto.GetChildrenResponse,
        &payload,
        allocator,
    );
    defer decoded.deinit(allocator);
}

test "owned decode frees every allocation when allocation fails" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        decodeOwnedAllocationFixture,
        .{},
    );
}

test "all opcode and special xid values match ZooKeeper 3.9.5" {
    const testing = std.testing;
    const expected = [_]struct { OpCode, i32 }{
        .{ .notification, 0 },
        .{ .create, 1 },
        .{ .delete, 2 },
        .{ .exists, 3 },
        .{ .get_data, 4 },
        .{ .set_data, 5 },
        .{ .get_acl, 6 },
        .{ .set_acl, 7 },
        .{ .get_children, 8 },
        .{ .sync, 9 },
        .{ .ping, 11 },
        .{ .get_children2, 12 },
        .{ .check, 13 },
        .{ .multi, 14 },
        .{ .create2, 15 },
        .{ .reconfig, 16 },
        .{ .check_watches, 17 },
        .{ .remove_watches, 18 },
        .{ .create_container, 19 },
        .{ .delete_container, 20 },
        .{ .create_ttl, 21 },
        .{ .multi_read, 22 },
        .{ .auth, 100 },
        .{ .set_watches, 101 },
        .{ .sasl, 102 },
        .{ .get_ephemerals, 103 },
        .{ .get_all_children_number, 104 },
        .{ .set_watches2, 105 },
        .{ .add_watch, 106 },
        .{ .who_am_i, 107 },
        .{ .create_session, -10 },
        .{ .close_session, -11 },
        .{ .@"error", -1 },
    };
    for (expected) |item| {
        try testing.expectEqual(item[1], @intFromEnum(item[0]));
        try testing.expectEqual(item[0], OpCode.fromInt(item[1]).?);
    }
    try testing.expectEqual(@as(?OpCode, null), OpCode.fromInt(10));
    try testing.expectEqual(@as(i32, -1), Xid.notification);
    try testing.expectEqual(@as(i32, -2), Xid.ping);
    try testing.expectEqual(@as(i32, -4), Xid.auth);
    try testing.expectEqual(@as(i32, -8), Xid.set_watches);
}

test "frame parser handles boundaries fragmentation and consecutive frames" {
    const testing = std.testing;
    const input = [_]u8{
        0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c',
        0x00, 0x00, 0x00, 0x01, 'd',
    };

    try testing.expectEqual(@as(?Frame, null), try parseFrame(input[0..3], 16));
    try testing.expectEqual(@as(?Frame, null), try parseFrame(input[0..6], 16));

    const first = (try parseFrame(&input, 16)).?;
    try testing.expectEqualSlices(u8, "abc", first.payload);
    try testing.expectEqual(@as(usize, 7), first.consumed);
    const second = (try parseFrame(input[first.consumed..], 16)).?;
    try testing.expectEqualSlices(u8, "d", second.payload);

    const empty = (try parseFrame(&.{ 0, 0, 0, 0 }, 0)).?;
    try testing.expectEqual(@as(usize, 0), empty.payload.len);
    try testing.expectError(error.InvalidFrameLength, parseFrame(&.{ 0xff, 0xff, 0xff, 0xff }, 16));
    try testing.expectError(error.FrameTooLarge, parseFrame(&.{ 0x00, 0x00, 0x00, 0x11 }, 16));
}

test "modern and legacy ConnectRequest frames match golden bytes" {
    const testing = std.testing;
    const request = proto.ConnectRequest{
        .protocolVersion = 0,
        .lastZxidSeen = 1,
        .timeOut = 30_000,
        .sessionId = 2,
        .passwd = &.{ 0xaa, 0xbb },
        .readOnly = true,
    };
    const legacy = [_]u8{
        0x00, 0x00, 0x00, 0x1e,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x75, 0x30,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x02,
        0xaa, 0xbb,
    };

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try encodeConnectRequest(&writer, request, false);
    try testing.expectEqualSlices(u8, &legacy, writer.bytes());
    const legacy_frame = (try parseFrame(writer.bytes(), default_max_payload)).?;
    const legacy_decoded = try decodeConnectRequest(legacy_frame.payload);
    try testing.expect(!legacy_decoded.read_only_supported);
    try testing.expect(!legacy_decoded.value.readOnly);

    writer.truncate(0);
    try encodeConnectRequest(&writer, request, true);
    try testing.expectEqual(@as(usize, legacy.len + 1), writer.dataSize());
    try testing.expectEqual(@as(u8, 0x1f), writer.bytes()[3]);
    try testing.expectEqual(@as(u8, 1), writer.bytes()[writer.dataSize() - 1]);
    const modern_frame = (try parseFrame(writer.bytes(), default_max_payload)).?;
    const modern_decoded = try decodeConnectRequest(modern_frame.payload);
    try testing.expect(modern_decoded.read_only_supported);
    try testing.expect(modern_decoded.value.readOnly);
}

test "modern and legacy ConnectResponse negotiate readOnly support" {
    const testing = std.testing;
    const response = proto.ConnectResponse{
        .protocolVersion = 0,
        .timeOut = 30_000,
        .sessionId = 2,
        .passwd = &.{ 0xaa, 0xbb },
        .readOnly = true,
    };

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try encodeConnectResponse(&writer, response, false);
    const legacy_frame = (try parseFrame(writer.bytes(), default_max_payload)).?;
    const legacy = try decodeConnectResponse(legacy_frame.payload);
    try testing.expect(!legacy.read_only_supported);
    try testing.expect(!legacy.value.readOnly);

    writer.truncate(0);
    try encodeConnectResponse(&writer, response, true);
    const modern_frame = (try parseFrame(writer.bytes(), default_max_payload)).?;
    const modern = try decodeConnectResponse(modern_frame.payload);
    try testing.expect(modern.read_only_supported);
    try testing.expect(modern.value.readOnly);

    var trailing = std.ArrayList(u8).empty;
    defer trailing.deinit(testing.allocator);
    try trailing.appendSlice(testing.allocator, modern_frame.payload);
    try trailing.append(testing.allocator, 0);
    try testing.expectError(error.TrailingData, decodeConnectResponse(trailing.items));
}

test "typed request and reply packets round trip" {
    const testing = std.testing;
    const request = proto.GetDataRequest{ .path = "/node", .watch = true };

    var request_writer = jute.Writer.init(testing.allocator);
    defer request_writer.deinit();
    try encodeRequest(&request_writer, 42, .get_data, request);
    const request_frame = (try parseFrame(request_writer.bytes(), default_max_payload)).?;
    const request_view = try requestView(request_frame.payload);
    try testing.expectEqual(@as(i32, 42), request_view.header.xid);
    try testing.expectEqual(@as(i32, 4), request_view.header.type);

    const decoded_request = try decodeRequest(proto.GetDataRequest, request_frame.payload, testing.allocator);
    defer decoded_request.deinit(testing.allocator);
    try testing.expectEqualStrings("/node", decoded_request.body.path.?);
    try testing.expect(decoded_request.body.watch);

    const response = proto.GetDataResponse{
        .data = "value",
        .stat = std.mem.zeroes(data.Stat),
    };
    var reply_writer = jute.Writer.init(testing.allocator);
    defer reply_writer.deinit();
    try encodeReply(&reply_writer, .{ .xid = 42, .zxid = 9, .err = 0 }, response);
    const reply_frame = (try parseFrame(reply_writer.bytes(), default_max_payload)).?;
    const decoded_reply = try decodeReply(proto.GetDataResponse, reply_frame.payload, testing.allocator);
    defer decoded_reply.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 9), decoded_reply.header.zxid);
    try testing.expectEqualStrings("value", decoded_reply.body.data.?);

    var raw_reply_writer = jute.Writer.init(testing.allocator);
    defer raw_reply_writer.deinit();
    try encodeReplyPayload(
        &raw_reply_writer,
        .{ .xid = 43, .zxid = 10, .err = 0 },
        "raw-body",
    );
    const raw_reply_frame = (try parseFrame(raw_reply_writer.bytes(), default_max_payload)).?;
    const raw_reply = try replyView(raw_reply_frame.payload);
    try testing.expectEqual(@as(i32, 43), raw_reply.header.xid);
    try testing.expectEqualStrings("raw-body", raw_reply.body);
}

test "owned decode survives receive buffer reuse" {
    const testing = std.testing;
    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try encodeRequest(
        &writer,
        8,
        .get_children,
        proto.GetChildrenRequest{ .path = "/stable", .watch = false },
    );
    const frame = (try parseFrame(writer.bytes(), default_max_payload)).?;
    const decoded = try decodeRequestOwned(proto.GetChildrenRequest, frame.payload, testing.allocator);
    defer decoded.deinit(testing.allocator);

    @memset(writer.buffer.items, 0);
    try testing.expectEqualStrings("/stable", decoded.body.path.?);
}

test "header-only packets reject trailing bytes and partial headers" {
    const testing = std.testing;
    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();

    try encodeRequest(&writer, Xid.ping, .ping, {});
    const request_frame = (try parseFrame(writer.bytes(), default_max_payload)).?;
    const request = try decodeRequest(void, request_frame.payload, testing.allocator);
    try testing.expectEqual(Xid.ping, request.header.xid);
    var trailing = std.ArrayList(u8).empty;
    defer trailing.deinit(testing.allocator);
    try trailing.appendSlice(testing.allocator, request_frame.payload);
    try trailing.append(testing.allocator, 0);
    try testing.expectError(
        error.TrailingData,
        decodeRequest(void, trailing.items, testing.allocator),
    );

    try testing.expectError(error.EndOfStream, requestView(&.{ 0, 0, 0, 1 }));
    try testing.expectError(error.EndOfStream, replyView(&.{ 0, 0, 0, 1, 0, 0, 0, 2 }));
}

test "custom wire and Jute limits apply symmetrically" {
    const testing = std.testing;
    const body = proto.GetDataRequest{ .path = "ab", .watch = false };
    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();

    try encodeRequestWithLimit(&writer, 1, .get_data, body, 64);
    const frame = (try parseFrame(writer.bytes(), 64)).?;
    const restrictive = Limits{
        .max_payload_size = 64,
        .jute = .{ .max_buffer_size = 1, .extra_max_buffer_size = 0 },
    };
    try testing.expectError(
        error.LengthLimitExceeded,
        decodeRequestWithLimits(proto.GetDataRequest, frame.payload, testing.allocator, restrictive),
    );

    writer.truncate(0);
    try writer.writeByte(7);
    try testing.expectError(
        error.FrameTooLarge,
        encodeRequestWithLimit(&writer, 1, .get_data, body, 4),
    );
    try testing.expectEqualSlices(u8, &.{7}, writer.bytes());
}
