const std = @import("std");
const jute = @import("../jute.zig");
const proto = @import("../protocol/proto.zig");
const wire = @import("../wire.zig");
const session_mod = @import("session.zig");
const TcpTransport = @import("tcp_transport.zig").TcpTransport;

pub const Options = struct {
    connect: std.Io.net.IpAddress.ConnectOptions = .{
        .mode = .stream,
        .protocol = .tcp,
    },
    handshake_timeout: std.Io.Timeout = .none,
    io_timeout: std.Io.Timeout = .none,
};

pub const Inbound = struct {
    allocator: std.mem.Allocator,
    payload: []u8,
    header: proto.ReplyHeader,
    kind: session_mod.ReplyKind,
    body_offset: usize,

    pub fn body(self: *const Inbound) []const u8 {
        return self.payload[self.body_offset..];
    }

    pub fn deinit(self: *Inbound) void {
        self.allocator.free(self.payload);
        self.* = undefined;
    }
};

/// A synchronous, single-threaded ZooKeeper connection. Use finite I/O
/// timeouts when the same thread is responsible for ping and expiration checks.
pub const BlockingClient = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    transport: TcpTransport,
    session: session_mod.Session,
    io_timeout: std.Io.Timeout,

    pub fn connectAddress(
        allocator: std.mem.Allocator,
        io: std.Io,
        address: std.Io.net.IpAddress,
        options: Options,
        session_config: session_mod.Config,
    ) !BlockingClient {
        const transport = try TcpTransport.connectAddress(io, address, options.connect);
        return finishConnect(allocator, io, transport, options, session_config);
    }

    pub fn connectHost(
        allocator: std.mem.Allocator,
        io: std.Io,
        host: []const u8,
        port: u16,
        options: Options,
        session_config: session_mod.Config,
    ) !BlockingClient {
        const transport = try TcpTransport.connectHost(io, host, port, options.connect);
        return finishConnect(allocator, io, transport, options, session_config);
    }

    /// Abortive cleanup. Call `close` first for a graceful closeSession.
    pub fn deinit(self: *BlockingClient) void {
        self.transport.close(self.io);
        if (self.session.state != .closed) self.session.markClosed();
        self.session.deinit();
        self.* = undefined;
    }

    pub fn sendRequest(
        self: *BlockingClient,
        opcode: wire.OpCode,
        body: anytype,
    ) !i32 {
        if (!self.session.state.isConnected()) return error.ConnectionLoss;
        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        const xid = try self.session.encodeRequest(
            &writer,
            opcode,
            body,
            monotonicMs(self.io),
        );
        self.transport.writeFrameTimeout(self.io, writer.bytes(), self.io_timeout) catch |err| {
            self.failConnection();
            return err;
        };
        return xid;
    }

    pub fn sendEncodedRequest(
        self: *BlockingClient,
        opcode: wire.OpCode,
        body_payload: []const u8,
    ) !i32 {
        if (!self.session.state.isConnected()) return error.ConnectionLoss;
        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        const xid = try self.session.encodeRequestPayload(
            &writer,
            opcode,
            body_payload,
            monotonicMs(self.io),
        );
        self.transport.writeFrameTimeout(self.io, writer.bytes(), self.io_timeout) catch |err| {
            self.failConnection();
            return err;
        };
        return xid;
    }

    pub fn sendPing(self: *BlockingClient) !void {
        if (!self.session.state.isConnected()) return error.ConnectionLoss;
        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        try self.session.encodePing(&writer, monotonicMs(self.io));
        self.transport.writeFrameTimeout(self.io, writer.bytes(), self.io_timeout) catch |err| {
            self.failConnection();
            return err;
        };
    }

    pub fn sendAuth(self: *BlockingClient, scheme: []const u8, auth: []const u8) !void {
        if (!self.session.state.isConnected()) return error.ConnectionLoss;
        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        try self.session.encodeAuth(&writer, scheme, auth, monotonicMs(self.io));
        self.transport.writeFrameTimeout(self.io, writer.bytes(), self.io_timeout) catch |err| {
            self.failConnection();
            return err;
        };
    }

    pub fn sendSetWatches(self: *BlockingClient, body: anytype, extended: bool) !void {
        if (!self.session.state.isConnected()) return error.ConnectionLoss;
        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        try self.session.encodeSetWatches(
            &writer,
            body,
            extended,
            monotonicMs(self.io),
        );
        self.transport.writeFrameTimeout(self.io, writer.bytes(), self.io_timeout) catch |err| {
            self.failConnection();
            return err;
        };
    }

    pub fn shouldPing(self: *const BlockingClient) bool {
        return self.session.shouldPing(monotonicMs(self.io));
    }

    pub fn hasReadTimedOut(self: *const BlockingClient) bool {
        return self.session.hasReadTimedOut(monotonicMs(self.io));
    }

    /// Closes the transport and fails pending requests as session-expired when
    /// the negotiated expiration deadline has elapsed.
    pub fn expireIfTimedOut(self: *BlockingClient) bool {
        if (!self.session.hasExpired(monotonicMs(self.io))) return false;
        self.transport.close(self.io);
        self.session.disconnect(.session_expired);
        return true;
    }

    pub fn receive(self: *BlockingClient) !Inbound {
        return self.receiveTimeout(self.io_timeout);
    }

    /// Reads one reply with a deadline. The returned object owns its payload;
    /// decoded body slices may borrow from it until `Inbound.deinit`.
    pub fn receiveTimeout(self: *BlockingClient, timeout: std.Io.Timeout) !Inbound {
        if (!self.session.state.isConnected()) return error.ConnectionLoss;
        const payload = self.transport.readFrameAllocTimeout(
            self.allocator,
            self.io,
            self.session.limits.max_payload_size,
            timeout,
        ) catch |err| {
            if (err != error.Timeout) self.failConnection();
            return err;
        };
        return self.acceptPayload(payload);
    }

    /// Takes ownership of one payload allocated by this client's allocator and
    /// applies it to the session state machine.
    pub fn acceptPayload(self: *BlockingClient, payload: []u8) !Inbound {
        errdefer self.allocator.free(payload);
        const view = wire.replyViewWithLimits(payload, self.session.limits) catch |err| {
            self.failConnection();
            return err;
        };
        const kind = self.session.matchReply(view.header, monotonicMs(self.io)) catch |err| {
            self.failConnection();
            return err;
        };
        return .{
            .allocator = self.allocator,
            .payload = payload,
            .header = view.header,
            .kind = kind,
            .body_offset = @intFromPtr(view.body.ptr) - @intFromPtr(payload.ptr),
        };
    }

    /// Encodes and writes closeSession without receiving its reply.
    pub fn beginClose(self: *BlockingClient, timeout: std.Io.Timeout) !i32 {
        if (!self.session.state.isConnected()) return error.ConnectionLoss;
        if (self.session.pendingCount() != 0) return error.PendingRequests;

        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        const xid = try self.session.encodeClose(&writer, monotonicMs(self.io));
        self.transport.writeFrameTimeout(self.io, writer.bytes(), timeout) catch |err| {
            self.failConnection();
            return err;
        };
        return xid;
    }

    /// Sends closeSession and waits for its ordered reply. The transport is
    /// closed and state becomes `closed` whether the exchange succeeds or not.
    pub fn close(self: *BlockingClient, timeout: std.Io.Timeout) !void {
        if (!self.session.state.isConnected()) {
            self.transport.close(self.io);
            self.session.markClosed();
            return;
        }
        if (self.session.pendingCount() != 0) return error.PendingRequests;
        defer {
            self.transport.close(self.io);
            self.session.markClosed();
        }

        const deadline = timeout.toDeadline(self.io);
        const xid = try self.beginClose(deadline);

        while (true) {
            var inbound = try self.receiveTimeout(deadline);
            defer inbound.deinit();
            switch (inbound.kind) {
                .ping, .notification => {},
                .auth => if (self.session.state == .auth_failed) {
                    return error.AuthenticationFailed;
                },
                .response => |response| {
                    if (inbound.header.xid != xid or response.opcode != .close_session) {
                        return error.OutOfOrderReply;
                    }
                    return;
                },
            }
        }
    }

    pub fn failedRequests(self: *const BlockingClient) []const session_mod.FailedPending {
        return self.session.failedRequests();
    }

    pub fn clearFailedRequests(self: *BlockingClient) void {
        self.session.clearFailedRequests();
    }

    fn finishConnect(
        allocator: std.mem.Allocator,
        io: std.Io,
        transport: TcpTransport,
        options: Options,
        session_config: session_mod.Config,
    ) !BlockingClient {
        var owned_transport = transport;
        errdefer owned_transport.close(io);
        var session = try session_mod.Session.init(allocator, session_config);
        errdefer session.deinit();
        var client = BlockingClient{
            .allocator = allocator,
            .io = io,
            .transport = owned_transport,
            .session = session,
            .io_timeout = options.io_timeout,
        };

        var writer = jute.Writer.init(allocator);
        defer writer.deinit();
        try client.session.encodeConnect(&writer, monotonicMs(io));
        const handshake_deadline = options.handshake_timeout.toDeadline(io);
        try client.transport.writeFrameTimeout(io, writer.bytes(), handshake_deadline);

        const response = try client.transport.readFrameAllocTimeout(
            allocator,
            io,
            client.session.limits.max_payload_size,
            handshake_deadline,
        );
        defer allocator.free(response);
        try client.session.acceptConnectResponse(response, monotonicMs(io));
        return client;
    }

    fn failConnection(self: *BlockingClient) void {
        self.transport.close(self.io);
        self.session.disconnect(.connection_loss);
    }
};

fn monotonicMs(io: std.Io) i64 {
    return std.Io.Clock.awake.now(io).toMilliseconds();
}

const test_password = "0123456789abcdef";

fn clientServerFixture(server: *std.Io.net.Server, io: std.Io) !void {
    const stream = try server.accept(io);
    var transport = TcpTransport.fromStream(stream);
    defer transport.close(io);

    const connect_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(connect_payload);
    const connect = try wire.decodeConnectRequest(connect_payload);
    try std.testing.expectEqual(@as(i32, 30_000), connect.value.timeOut);

    var writer = jute.Writer.init(std.testing.allocator);
    defer writer.deinit();
    try wire.encodeConnectResponse(
        &writer,
        .{
            .protocolVersion = 0,
            .timeOut = 24_000,
            .sessionId = 55,
            .passwd = test_password,
            .readOnly = false,
        },
        connect.read_only_supported,
    );
    try transport.writeFrame(io, writer.bytes());

    const request_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(request_payload);
    const request = try wire.decodeRequest(proto.GetDataRequest, request_payload, std.testing.allocator);
    defer request.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("/node", request.body.path.?);

    writer.truncate(0);
    try wire.encodeReply(
        &writer,
        .{ .xid = request.header.xid, .zxid = 9, .err = 0 },
        proto.GetDataResponse{
            .data = "value",
            .stat = std.mem.zeroes(@import("../protocol/data.zig").Stat),
        },
    );
    try transport.writeFrame(io, writer.bytes());

    const close_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(close_payload);
    const close_request = try wire.decodeRequest(void, close_payload, std.testing.allocator);
    try std.testing.expectEqual(@intFromEnum(wire.OpCode.close_session), close_request.header.type);
    writer.truncate(0);
    try wire.encodeReply(
        &writer,
        .{ .xid = wire.Xid.ping, .zxid = 0, .err = 0 },
        {},
    );
    try transport.writeFrame(io, writer.bytes());
    writer.truncate(0);
    try wire.encodeReply(
        &writer,
        .{ .xid = close_request.header.xid, .zxid = 9, .err = 0 },
        {},
    );
    try transport.writeFrame(io, writer.bytes());
}

test "blocking client performs handshake request reply and graceful close" {
    const testing = std.testing;
    const listen_address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try listen_address.listen(testing.io, .{});
    defer server.deinit(testing.io);

    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        server.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    const port = std.mem.bigToNative(u16, local_address.port);

    var server_future = try testing.io.concurrent(clientServerFixture, .{ &server, testing.io });
    defer server_future.cancel(testing.io) catch {};

    const one_second: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(1),
        .clock = .awake,
    } };
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    var client = try BlockingClient.connectAddress(
        testing.allocator,
        testing.io,
        address,
        .{
            .handshake_timeout = one_second,
            .io_timeout = one_second,
        },
        .{},
    );
    defer client.deinit();
    try testing.expectEqual(session_mod.State.connected, client.session.state);
    try testing.expectEqual(@as(i64, 55), client.session.session_id);

    const xid = try client.sendRequest(
        .get_data,
        proto.GetDataRequest{ .path = "/node", .watch = false },
    );
    var inbound = try client.receive();
    defer inbound.deinit();
    try testing.expectEqual(xid, inbound.header.xid);
    try testing.expectEqual(xid, inbound.kind.response.xid);

    const response = try wire.decodeFrameRecord(proto.GetDataResponse, inbound.body(), testing.allocator);
    defer jute.deinitDecoded(response, testing.allocator);
    try testing.expectEqualStrings("value", response.data.?);
    try testing.expectEqual(@as(i64, 9), client.session.last_zxid_seen);

    try client.close(one_second);
    try testing.expectEqual(session_mod.State.closed, client.session.state);
    try server_future.await(testing.io);
}

fn idleServerFixture(server: *std.Io.net.Server, io: std.Io) !void {
    const stream = try server.accept(io);
    var transport = TcpTransport.fromStream(stream);
    defer transport.close(io);

    const connect_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(connect_payload);
    _ = try wire.decodeConnectRequest(connect_payload);

    var writer = jute.Writer.init(std.testing.allocator);
    defer writer.deinit();
    try wire.encodeConnectResponse(
        &writer,
        .{
            .protocolVersion = 0,
            .timeOut = 24_000,
            .sessionId = 56,
            .passwd = test_password,
            .readOnly = false,
        },
        true,
    );
    try transport.writeFrame(io, writer.bytes());

    const close_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(close_payload);
    const close_request = try wire.decodeRequest(void, close_payload, std.testing.allocator);
    writer.truncate(0);
    try wire.encodeReply(
        &writer,
        .{ .xid = close_request.header.xid, .zxid = 0, .err = 0 },
        {},
    );
    try transport.writeFrame(io, writer.bytes());
}

test "blocking receive timeout leaves an idle connection usable" {
    const testing = std.testing;
    const listen_address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try listen_address.listen(testing.io, .{});
    defer server.deinit(testing.io);

    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        server.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    const port = std.mem.bigToNative(u16, local_address.port);

    var server_future = try testing.io.concurrent(idleServerFixture, .{ &server, testing.io });
    defer server_future.cancel(testing.io) catch {};

    const one_second: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(1),
        .clock = .awake,
    } };
    const short_timeout: std.Io.Timeout = .{ .duration = .{
        .raw = .fromMilliseconds(10),
        .clock = .awake,
    } };
    var client = try BlockingClient.connectHost(
        testing.allocator,
        testing.io,
        "127.0.0.1",
        port,
        .{
            .connect = .{
                .mode = .stream,
                .protocol = .tcp,
                .timeout = one_second,
            },
            .handshake_timeout = one_second,
        },
        .{},
    );
    defer client.deinit();

    try testing.expectError(error.Timeout, client.receiveTimeout(short_timeout));
    try testing.expectEqual(session_mod.State.connected, client.session.state);
    try client.close(one_second);
    try server_future.await(testing.io);
}

fn partialFrameServerFixture(server: *std.Io.net.Server, io: std.Io) !void {
    const stream = try server.accept(io);
    var transport = TcpTransport.fromStream(stream);
    defer transport.close(io);

    const connect_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(connect_payload);
    _ = try wire.decodeConnectRequest(connect_payload);

    var writer = jute.Writer.init(std.testing.allocator);
    defer writer.deinit();
    try wire.encodeConnectResponse(
        &writer,
        .{
            .protocolVersion = 0,
            .timeOut = 24_000,
            .sessionId = 57,
            .passwd = test_password,
            .readOnly = false,
        },
        true,
    );
    try transport.writeFrame(io, writer.bytes());

    const request_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(request_payload);
    const request = try wire.decodeRequest(proto.GetDataRequest, request_payload, std.testing.allocator);
    defer request.deinit(std.testing.allocator);

    var stream_buffer: [0]u8 = .{};
    var stream_writer = stream.writer(io, &stream_buffer);
    try stream_writer.interface.writeAll(&.{ 0, 0 });
    try stream_writer.interface.flush();
    try (std.Io.Timeout{ .duration = .{
        .raw = .fromMilliseconds(50),
        .clock = .awake,
    } }).sleep(io);
}

test "partial frame timeout disconnects and fails outstanding requests" {
    const testing = std.testing;
    const listen_address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try listen_address.listen(testing.io, .{});
    defer server.deinit(testing.io);

    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        server.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    const port = std.mem.bigToNative(u16, local_address.port);

    var server_future = try testing.io.concurrent(partialFrameServerFixture, .{ &server, testing.io });
    defer server_future.cancel(testing.io) catch {};

    const one_second: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(1),
        .clock = .awake,
    } };
    const short_timeout: std.Io.Timeout = .{ .duration = .{
        .raw = .fromMilliseconds(10),
        .clock = .awake,
    } };
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    var client = try BlockingClient.connectAddress(
        testing.allocator,
        testing.io,
        address,
        .{
            .handshake_timeout = one_second,
            .io_timeout = one_second,
        },
        .{},
    );
    defer client.deinit();

    const xid = try client.sendRequest(
        .get_data,
        proto.GetDataRequest{ .path = "/lost", .watch = false },
    );
    try testing.expectError(error.PartialFrameTimeout, client.receiveTimeout(short_timeout));
    try testing.expectEqual(session_mod.State.disconnected, client.session.state);
    try testing.expectEqual(@as(usize, 0), client.session.pendingCount());
    try testing.expectEqual(@as(usize, 1), client.failedRequests().len);
    try testing.expectEqual(xid, client.failedRequests()[0].request.xid);
    try testing.expectEqual(
        session_mod.FailureReason.connection_loss,
        client.failedRequests()[0].reason,
    );
    try server_future.await(testing.io);
}
