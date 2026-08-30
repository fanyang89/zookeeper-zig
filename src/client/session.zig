const std = @import("std");
const jute = @import("../jute.zig");
const proto = @import("../protocol/proto.zig");
const wire = @import("../wire.zig");

pub const session_password_length = 16;
pub const zero_password = [_]u8{0} ** session_password_length;

pub const State = enum {
    disconnected,
    associating,
    connected,
    connected_read_only,
    auth_failed,
    expired,
    closed,

    pub fn isConnected(self: State) bool {
        return self == .connected or self == .connected_read_only;
    }
};

pub const Config = struct {
    session_timeout_ms: i32 = 30_000,
    session_id: i64 = 0,
    passwd: []const u8 = &zero_password,
    last_zxid_seen: i64 = 0,
    allow_read_only: bool = false,
    seen_read_write_server: ?bool = null,
    wire_limits: wire.Limits = .{},
};

pub const Pending = struct {
    xid: i32,
    opcode: wire.OpCode,
};

pub const FailureReason = enum {
    connection_loss,
    session_expired,
    auth_failed,
    closed,
};

pub const FailedPending = struct {
    request: Pending,
    reason: FailureReason,
};

pub const ReplyKind = union(enum) {
    ping,
    auth,
    notification,
    response: Pending,
};

pub const Error = error{
    InvalidState,
    InvalidControlOpcode,
    InvalidSessionCredentials,
    PendingRequests,
    SessionExpired,
    UnexpectedReply,
    OutOfOrderReply,
};

pub const Session = struct {
    allocator: std.mem.Allocator,
    state: State = .disconnected,
    requested_timeout_ms: i32,
    negotiated_timeout_ms: i32 = 0,
    session_id: i64,
    passwd: []u8,
    last_zxid_seen: i64,
    allow_read_only: bool,
    seen_read_write_server: bool = false,
    connect_session_id_sent: i64 = 0,
    next_xid: i32 = 1,
    last_send_ms: i64 = 0,
    last_heard_ms: i64 = 0,
    limits: wire.Limits,
    pending: std.ArrayList(Pending) = .empty,
    pending_head: usize = 0,
    failed_pending: std.ArrayList(FailedPending) = .empty,

    pub fn init(allocator: std.mem.Allocator, config: Config) (std.mem.Allocator.Error || Error)!Session {
        if (config.passwd.len != session_password_length) return error.InvalidSessionCredentials;
        return .{
            .allocator = allocator,
            .requested_timeout_ms = config.session_timeout_ms,
            .session_id = config.session_id,
            .passwd = try allocator.dupe(u8, config.passwd),
            .last_zxid_seen = config.last_zxid_seen,
            .allow_read_only = config.allow_read_only,
            .seen_read_write_server = config.seen_read_write_server orelse (config.session_id != 0),
            .limits = config.wire_limits,
        };
    }

    pub fn deinit(self: *Session) void {
        self.allocator.free(self.passwd);
        self.pending.deinit(self.allocator);
        self.failed_pending.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn encodeConnect(self: *Session, writer: *jute.Writer, now_ms: i64) (wire.EncodeError || Error)!void {
        if (self.state != .disconnected) return error.InvalidState;
        self.connect_session_id_sent = if (self.seen_read_write_server) self.session_id else 0;
        const request = proto.ConnectRequest{
            .protocolVersion = 0,
            .lastZxidSeen = self.last_zxid_seen,
            .timeOut = self.requested_timeout_ms,
            .sessionId = self.connect_session_id_sent,
            .passwd = self.passwd,
            .readOnly = self.allow_read_only,
        };
        try wire.encodeConnectRequestWithLimit(
            writer,
            request,
            true,
            self.limits.max_payload_size,
        );
        self.state = .associating;
        self.last_send_ms = now_ms;
    }

    pub fn acceptConnectResponse(
        self: *Session,
        payload: []const u8,
        now_ms: i64,
    ) (wire.DecodeError || std.mem.Allocator.Error || Error)!void {
        if (self.state != .associating) return error.InvalidState;
        const decoded = try wire.decodeConnectResponseWithLimits(payload, self.limits);
        if (decoded.value.timeOut <= 0) {
            self.expirePending();
            return error.SessionExpired;
        }
        if (decoded.value.sessionId == 0 or
            (decoded.value.passwd orelse @as([]const u8, &.{})).len != session_password_length)
        {
            self.expirePending();
            return error.InvalidSessionCredentials;
        }
        if (self.connect_session_id_sent != 0 and decoded.value.sessionId != self.connect_session_id_sent) {
            self.expirePending();
            return error.SessionExpired;
        }

        const new_passwd = try self.allocator.dupe(u8, decoded.value.passwd.?);
        self.allocator.free(self.passwd);
        self.passwd = new_passwd;
        self.negotiated_timeout_ms = decoded.value.timeOut;
        self.session_id = decoded.value.sessionId;
        self.state = if (decoded.value.readOnly) .connected_read_only else .connected;
        self.seen_read_write_server = self.seen_read_write_server or !decoded.value.readOnly;
        self.last_send_ms = now_ms;
        self.last_heard_ms = now_ms;
    }

    pub fn encodeRequest(
        self: *Session,
        writer: *jute.Writer,
        opcode: wire.OpCode,
        body: anytype,
        now_ms: i64,
    ) (wire.EncodeError || std.mem.Allocator.Error || Error)!i32 {
        if (!self.state.isConnected()) return error.InvalidState;
        if (isControlOpcode(opcode)) return error.InvalidControlOpcode;
        const xid = self.next_xid;
        try self.reservePendingFailureCapacity(1);
        try wire.encodeRequestWithLimit(writer, xid, opcode, body, self.limits.max_payload_size);
        self.next_xid = nextXid(xid);
        self.pending.appendAssumeCapacity(.{ .xid = xid, .opcode = opcode });
        self.last_send_ms = now_ms;
        return xid;
    }

    pub fn encodeRequestPayload(
        self: *Session,
        writer: *jute.Writer,
        opcode: wire.OpCode,
        body_payload: []const u8,
        now_ms: i64,
    ) (wire.EncodeError || std.mem.Allocator.Error || Error)!i32 {
        if (!self.state.isConnected()) return error.InvalidState;
        if (isControlOpcode(opcode)) return error.InvalidControlOpcode;
        const xid = self.next_xid;
        try self.reservePendingFailureCapacity(1);
        try wire.encodeRequestPayloadWithLimit(
            writer,
            xid,
            opcode,
            body_payload,
            self.limits.max_payload_size,
        );
        self.next_xid = nextXid(xid);
        self.pending.appendAssumeCapacity(.{ .xid = xid, .opcode = opcode });
        self.last_send_ms = now_ms;
        return xid;
    }

    pub fn encodePing(self: *Session, writer: *jute.Writer, now_ms: i64) (wire.EncodeError || Error)!void {
        if (!self.state.isConnected()) return error.InvalidState;
        try wire.encodeRequestWithLimit(
            writer,
            wire.Xid.ping,
            .ping,
            {},
            self.limits.max_payload_size,
        );
        self.last_send_ms = now_ms;
    }

    pub fn encodeAuth(
        self: *Session,
        writer: *jute.Writer,
        scheme: []const u8,
        auth: []const u8,
        now_ms: i64,
    ) (wire.EncodeError || Error)!void {
        if (!self.state.isConnected()) return error.InvalidState;
        try wire.encodeRequestWithLimit(
            writer,
            wire.Xid.auth,
            .auth,
            proto.AuthPacket{ .type = 0, .scheme = scheme, .auth = auth },
            self.limits.max_payload_size,
        );
        self.last_send_ms = now_ms;
    }

    pub fn encodeSetWatches(
        self: *Session,
        writer: *jute.Writer,
        body: anytype,
        extended: bool,
        now_ms: i64,
    ) (wire.EncodeError || std.mem.Allocator.Error || Error)!void {
        if (!self.state.isConnected()) return error.InvalidState;
        try self.reservePendingFailureCapacity(1);
        const opcode: wire.OpCode = if (extended) .set_watches2 else .set_watches;
        try wire.encodeRequestWithLimit(
            writer,
            wire.Xid.set_watches,
            opcode,
            body,
            self.limits.max_payload_size,
        );
        self.pending.appendAssumeCapacity(.{ .xid = wire.Xid.set_watches, .opcode = opcode });
        self.last_send_ms = now_ms;
    }

    pub fn encodeClose(self: *Session, writer: *jute.Writer, now_ms: i64) (wire.EncodeError || std.mem.Allocator.Error || Error)!i32 {
        if (!self.state.isConnected()) return error.InvalidState;
        const xid = self.next_xid;
        try self.reservePendingFailureCapacity(1);
        try wire.encodeRequestWithLimit(
            writer,
            xid,
            .close_session,
            {},
            self.limits.max_payload_size,
        );
        self.next_xid = nextXid(xid);
        self.pending.appendAssumeCapacity(.{ .xid = xid, .opcode = .close_session });
        self.last_send_ms = now_ms;
        return xid;
    }

    pub fn matchReply(self: *Session, header: proto.ReplyHeader, now_ms: i64) Error!ReplyKind {
        if (!self.state.isConnected()) return error.InvalidState;
        self.last_heard_ms = now_ms;

        switch (header.xid) {
            wire.Xid.ping => return .ping,
            wire.Xid.auth => {
                if (header.err == -115) {
                    self.failPending(.auth_failed);
                    self.state = .auth_failed;
                }
                return .auth;
            },
            wire.Xid.notification => return .notification,
            else => {},
        }

        if (self.pending_head == self.pending.items.len) return error.UnexpectedReply;
        const expected = self.pending.items[self.pending_head];
        if (expected.xid != header.xid) return error.OutOfOrderReply;
        self.pending_head += 1;
        self.compactPending();
        if (header.zxid > self.last_zxid_seen) self.last_zxid_seen = header.zxid;
        return .{ .response = expected };
    }

    pub fn pendingCount(self: *const Session) usize {
        return self.pending.items.len - self.pending_head;
    }

    pub fn failedRequests(self: *const Session) []const FailedPending {
        return self.failed_pending.items;
    }

    pub fn clearFailedRequests(self: *Session) void {
        self.failed_pending.clearRetainingCapacity();
    }

    pub fn disconnect(self: *Session, reason: FailureReason) void {
        self.failPending(reason);
        self.state = switch (reason) {
            .connection_loss => .disconnected,
            .session_expired => .expired,
            .auth_failed => .auth_failed,
            .closed => .closed,
        };
    }

    pub fn markClosed(self: *Session) void {
        self.disconnect(.closed);
    }

    pub fn readTimeoutMs(self: *const Session) i64 {
        return @divTrunc(@as(i64, self.negotiated_timeout_ms) * 2, 3);
    }

    pub fn expirationTimeoutMs(self: *const Session) i64 {
        return @divTrunc(@as(i64, self.negotiated_timeout_ms) * 4, 3);
    }

    pub fn shouldPing(self: *const Session, now_ms: i64) bool {
        if (!self.state.isConnected()) return false;
        return now_ms - self.last_send_ms >= @divTrunc(self.readTimeoutMs(), 2);
    }

    pub fn hasReadTimedOut(self: *const Session, now_ms: i64) bool {
        if (!self.state.isConnected()) return false;
        return now_ms - self.last_heard_ms >= self.readTimeoutMs();
    }

    pub fn hasExpired(self: *const Session, now_ms: i64) bool {
        if (!self.state.isConnected()) return false;
        return now_ms - self.last_heard_ms >= self.expirationTimeoutMs();
    }

    fn reservePendingFailureCapacity(self: *Session, additional: usize) std.mem.Allocator.Error!void {
        try self.pending.ensureUnusedCapacity(self.allocator, additional);
        try self.failed_pending.ensureUnusedCapacity(
            self.allocator,
            self.pendingCount() + additional,
        );
    }

    fn failPending(self: *Session, reason: FailureReason) void {
        for (self.pending.items[self.pending_head..]) |request| {
            self.failed_pending.appendAssumeCapacity(.{ .request = request, .reason = reason });
        }
        self.pending.clearRetainingCapacity();
        self.pending_head = 0;
    }

    fn expirePending(self: *Session) void {
        self.failPending(.session_expired);
        self.state = .expired;
    }

    fn compactPending(self: *Session) void {
        if (self.pending_head == 0) return;
        if (self.pending_head == self.pending.items.len) {
            self.pending.clearRetainingCapacity();
            self.pending_head = 0;
            return;
        }
        if (self.pending_head < 64 or self.pending_head * 2 < self.pending.items.len) return;
        const remaining = self.pending.items[self.pending_head..];
        std.mem.copyForwards(Pending, self.pending.items[0..remaining.len], remaining);
        self.pending.items.len = remaining.len;
        self.pending_head = 0;
    }
};

fn isControlOpcode(opcode: wire.OpCode) bool {
    return switch (opcode) {
        .notification,
        .ping,
        .auth,
        .set_watches,
        .set_watches2,
        .create_session,
        .close_session,
        .@"error",
        => true,
        else => false,
    };
}

fn nextXid(current: i32) i32 {
    return if (current == std.math.maxInt(i32)) 1 else current + 1;
}

const test_password = "0123456789abcdef";

fn acceptHandshakeAllocationFixture(allocator: std.mem.Allocator) !void {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x5d, 0xc0,
        0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x63,
        0x00, 0x00, 0x00, 0x10,
        '0',  '1',  '2',  '3',
        '4',  '5',  '6',  '7',
        '8',  '9',  'a',  'b',
        'c',  'd',  'e',  'f',
        0x00,
    };
    var session = try Session.init(allocator, .{});
    defer session.deinit();
    session.state = .associating;
    try session.acceptConnectResponse(&payload, 0);
}

fn encodeRequestAllocationFixture(allocator: std.mem.Allocator) !void {
    var session = try Session.init(allocator, .{});
    defer session.deinit();
    session.state = .connected;
    var writer = jute.Writer.init(allocator);
    defer writer.deinit();
    _ = try session.encodeRequest(
        &writer,
        .get_data,
        proto.GetDataRequest{ .path = "/node", .watch = false },
        0,
    );
}

test "session frees allocations on handshake and request allocation failure" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        acceptHandshakeAllocationFixture,
        .{},
    );
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        encodeRequestAllocationFixture,
        .{},
    );
}

test "session negotiates handshake and ping timers" {
    const testing = std.testing;
    var session = try Session.init(testing.allocator, .{
        .session_timeout_ms = 30_000,
        .allow_read_only = true,
    });
    defer session.deinit();

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try session.encodeConnect(&writer, 100);
    try testing.expectEqual(State.associating, session.state);

    var response_writer = jute.Writer.init(testing.allocator);
    defer response_writer.deinit();
    try wire.encodeConnectResponse(
        &response_writer,
        .{
            .protocolVersion = 0,
            .timeOut = 24_000,
            .sessionId = 99,
            .passwd = test_password,
            .readOnly = false,
        },
        true,
    );
    const frame = (try wire.parseFrame(response_writer.bytes(), wire.default_max_payload)).?;
    try session.acceptConnectResponse(frame.payload, 200);

    try testing.expectEqual(State.connected, session.state);
    try testing.expectEqual(@as(i64, 99), session.session_id);
    try testing.expectEqualStrings(test_password, session.passwd);
    try testing.expectEqual(@as(i64, 16_000), session.readTimeoutMs());
    try testing.expectEqual(@as(i64, 32_000), session.expirationTimeoutMs());
    try testing.expect(!session.shouldPing(8_199));
    try testing.expect(session.shouldPing(8_200));
    try testing.expect(session.hasReadTimedOut(16_200));
    try testing.expect(session.hasExpired(32_200));
}

test "session queues normal xids and drains them on disconnect" {
    const testing = std.testing;
    var session = try Session.init(testing.allocator, .{});
    defer session.deinit();
    session.state = .connected;
    session.negotiated_timeout_ms = 30_000;

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    const first = try session.encodeRequest(
        &writer,
        .get_data,
        proto.GetDataRequest{ .path = "/a", .watch = false },
        1,
    );
    writer.truncate(0);
    const second = try session.encodeRequest(
        &writer,
        .exists,
        proto.ExistsRequest{ .path = "/b", .watch = true },
        2,
    );
    try testing.expectEqual(@as(i32, 1), first);
    try testing.expectEqual(@as(i32, 2), second);

    const ping = try session.matchReply(.{ .xid = wire.Xid.ping, .zxid = 99, .err = 0 }, 3);
    try testing.expectEqual(ReplyKind.ping, ping);
    try testing.expectEqual(@as(i64, 0), session.last_zxid_seen);
    const matched = try session.matchReply(.{ .xid = first, .zxid = 4, .err = 0 }, 4);
    try testing.expectEqual(first, matched.response.xid);
    try testing.expectEqual(@as(i64, 4), session.last_zxid_seen);

    session.disconnect(.connection_loss);
    try testing.expectEqual(State.disconnected, session.state);
    try testing.expectEqual(@as(usize, 0), session.pendingCount());
    try testing.expectEqual(@as(usize, 1), session.failedRequests().len);
    try testing.expectEqual(second, session.failedRequests()[0].request.xid);
    try testing.expectEqual(FailureReason.connection_loss, session.failedRequests()[0].reason);

    session.state = .connected;
    session.disconnect(.session_expired);
    try testing.expectEqual(State.expired, session.state);
}

test "set watches uses fixed xid and consumes the pending FIFO" {
    const testing = std.testing;
    var session = try Session.init(testing.allocator, .{});
    defer session.deinit();
    session.state = .connected;

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try session.encodeSetWatches(
        &writer,
        proto.SetWatches{
            .relativeZxid = 0,
            .dataWatches = null,
            .existWatches = null,
            .childWatches = null,
        },
        false,
        1,
    );
    try testing.expectEqual(@as(usize, 1), session.pendingCount());
    const reply = try session.matchReply(.{ .xid = wire.Xid.set_watches, .zxid = 0, .err = 0 }, 2);
    try testing.expectEqual(wire.OpCode.set_watches, reply.response.opcode);
    try testing.expectEqual(@as(usize, 0), session.pendingCount());
}

test "session rejects control opcodes and reassociation in an active generation" {
    const testing = std.testing;
    var session = try Session.init(testing.allocator, .{});
    defer session.deinit();
    session.state = .connected;
    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try testing.expectError(error.InvalidControlOpcode, session.encodeRequest(&writer, .ping, {}, 0));
    try testing.expectError(error.InvalidState, session.encodeConnect(&writer, 0));
    session.state = .associating;
    try testing.expectError(error.InvalidState, session.encodeConnect(&writer, 0));
}

test "session validates resume identity and credentials" {
    const testing = std.testing;
    try testing.expectError(
        error.InvalidSessionCredentials,
        Session.init(testing.allocator, .{ .session_id = 1, .passwd = "short" }),
    );

    var session = try Session.init(testing.allocator, .{
        .session_id = 7,
        .passwd = test_password,
    });
    defer session.deinit();
    var connect_writer = jute.Writer.init(testing.allocator);
    defer connect_writer.deinit();
    try session.encodeConnect(&connect_writer, 0);
    try testing.expectEqual(@as(i64, 7), session.connect_session_id_sent);

    var response_writer = jute.Writer.init(testing.allocator);
    defer response_writer.deinit();
    try wire.encodeConnectResponse(
        &response_writer,
        .{
            .protocolVersion = 0,
            .timeOut = 30_000,
            .sessionId = 8,
            .passwd = test_password,
            .readOnly = false,
        },
        true,
    );
    const frame = (try wire.parseFrame(response_writer.bytes(), wire.default_max_payload)).?;
    try testing.expectError(error.SessionExpired, session.acceptConnectResponse(frame.payload, 1));
    try testing.expectEqual(State.expired, session.state);
}

test "session xid wraps without entering the negative range" {
    const testing = std.testing;
    var session = try Session.init(testing.allocator, .{});
    defer session.deinit();
    session.state = .connected;
    session.next_xid = std.math.maxInt(i32);

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try testing.expectEqual(
        @as(i32, std.math.maxInt(i32)),
        try session.encodeRequest(
            &writer,
            .exists,
            proto.ExistsRequest{ .path = "/", .watch = false },
            1,
        ),
    );
    try testing.expectEqual(@as(i32, 1), session.next_xid);
}
