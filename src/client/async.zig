const std = @import("std");
const builtin = @import("builtin");
const jute = @import("../jute.zig");
const proto = @import("../protocol/proto.zig");
const wire = @import("../wire.zig");
const blocking = @import("blocking.zig");
const session_mod = @import("session.zig");
const TcpTransport = @import("tcp_transport.zig").TcpTransport;

pub const Inbound = blocking.Inbound;
pub const RequestFuture = std.Io.Future(anyerror!Inbound);
const ReaderFuture = std.Io.Future(std.Io.Cancelable!void);

const TestStage = enum(u8) {
    idle,
    close_queued,
    close_received,
    close_sent,
    reply_read,
    reply_queued,
    reply_received,
    close_completed,
};

var test_stage: std.atomic.Value(TestStage) = .init(.idle);

fn recordTestStage(stage: TestStage) void {
    if (builtin.is_test) test_stage.store(stage, .release);
}

pub const Options = struct {
    connection: blocking.Options = .{},
    command_queue_capacity: usize = 64,
    /// A full notification queue disconnects the client with
    /// NotificationQueueFull rather than stalling protocol progress.
    notification_queue_capacity: usize = 64,
};

const RequestCompletion = union(enum) {
    pending,
    inbound: Inbound,
    failure: anyerror,
};

const RequestCall = struct {
    opcode: wire.OpCode,
    body_payload: []const u8,
    done: std.Io.Event = .unset,
    completion: RequestCompletion = .pending,
};

const CloseCompletion = union(enum) {
    pending,
    success,
    failure: anyerror,
};

const CloseCall = struct {
    deadline: std.Io.Timeout,
    xid: ?i32 = null,
    done: std.Io.Event = .unset,
    completion: CloseCompletion = .pending,
};

const EngineEvent = union(enum) {
    request: *RequestCall,
    close: *CloseCall,
    inbound: []u8,
    read_failure: anyerror,
    tick,
};

/// A concurrent ZooKeeper client with a dedicated read pump, serialized writes,
/// request futures, notification delivery, and automatic ping/timeout checks.
/// The returned pointer has a stable address required by its background tasks.
pub const AsyncClient = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    core: blocking.BlockingClient,
    events: std.Io.Queue(EngineEvent),
    notifications: std.Io.Queue(Inbound),
    event_buffer: []EngineEvent,
    notification_buffer: []Inbound,
    tasks: std.Io.Group = .init,
    reader: ?ReaderFuture = null,
    tasks_running: bool = true,
    terminal_error: ?anyerror = null,
    operation_mutex: std.Io.Mutex = .init,
    operation_condition: std.Io.Condition = .init,
    terminated: std.Io.Event = .unset,
    operation_count: usize = 0,
    shutting_down: bool = false,

    pub fn connectAddress(
        allocator: std.mem.Allocator,
        io: std.Io,
        address: std.Io.net.IpAddress,
        options: Options,
        session_config: session_mod.Config,
    ) !*AsyncClient {
        var core = try blocking.BlockingClient.connectAddress(
            allocator,
            io,
            address,
            options.connection,
            session_config,
        );
        errdefer core.deinit();
        return start(allocator, io, core, options);
    }

    pub fn connectHost(
        allocator: std.mem.Allocator,
        io: std.Io,
        host: []const u8,
        port: u16,
        options: Options,
        session_config: session_mod.Config,
    ) !*AsyncClient {
        var core = try blocking.BlockingClient.connectHost(
            allocator,
            io,
            host,
            port,
            options.connection,
            session_config,
        );
        errdefer core.deinit();
        return start(allocator, io, core, options);
    }

    /// Stops background tasks, completes active operations, closes the
    /// transport, and releases queued notifications.
    pub fn deinit(self: *AsyncClient) void {
        self.beginShutdown();
        self.stopBackgroundTasks();
        self.waitForOperations();
        self.drainNotifications();
        self.core.deinit();
        self.allocator.free(self.notification_buffer);
        self.allocator.free(self.event_buffer);
        const allocator = self.allocator;
        self.* = undefined;
        allocator.destroy(self);
    }

    /// Submits one request and suspends until its matching reply arrives. The
    /// returned value owns its payload. Canceling the wait does not cancel the
    /// ZooKeeper operation; the reply is consumed before Canceled is returned.
    pub fn request(self: *AsyncClient, opcode: wire.OpCode, body: anytype) anyerror!Inbound {
        try self.beginOperation();
        defer self.endOperation();
        return self.requestImpl(opcode, body);
    }

    /// Starts a request on a guaranteed unit of concurrency. The client tracks
    /// the operation so `deinit` cannot release its storage before completion.
    pub fn requestAsync(
        self: *AsyncClient,
        opcode: wire.OpCode,
        body: anytype,
    ) anyerror!RequestFuture {
        try self.beginOperation();
        errdefer self.endOperation();
        const Task = struct {
            fn run(client: *AsyncClient, request_opcode: wire.OpCode, request_body: @TypeOf(body)) anyerror!Inbound {
                defer client.endOperation();
                return client.requestImpl(request_opcode, request_body);
            }
        };
        return self.io.concurrent(Task.run, .{ self, opcode, body });
    }

    /// Waits for the next watch notification. Normal request replies are
    /// delivered directly to their request futures.
    pub fn receiveNotification(self: *AsyncClient) anyerror!Inbound {
        try self.beginOperation();
        defer self.endOperation();
        return self.notifications.getOne(self.io) catch |err| switch (err) {
            error.Closed => return self.terminal_error orelse error.ConnectionLoss,
            error.Canceled => return error.Canceled,
        };
    }

    /// Performs the ordered closeSession exchange. Outstanding requests must
    /// complete before close begins.
    pub fn close(self: *AsyncClient, timeout: std.Io.Timeout) anyerror!void {
        try self.beginOperation();
        defer self.endOperation();
        if (!self.tasks_running) return;
        var call = CloseCall{ .deadline = timeout.toDeadline(self.io) };
        self.events.putOne(self.io, .{ .close = &call }) catch |err| switch (err) {
            error.Closed => return self.terminal_error orelse error.ConnectionLoss,
            error.Canceled => return error.Canceled,
        };
        recordTestStage(.close_queued);
        call.done.waitTimeout(self.io, call.deadline) catch |err| {
            if (builtin.is_test) {
                std.debug.print(
                    "async close timeout: stage={s} pending={} terminated={} terminal={?}\n",
                    .{
                        @tagName(test_stage.load(.acquire)),
                        self.core.session.pendingCount(),
                        self.terminated.isSet(),
                        self.terminal_error,
                    },
                );
            }
            self.stopBackgroundTasks();
            return err;
        };
        const result: anyerror!void = switch (call.completion) {
            .pending => unreachable,
            .success => {},
            .failure => |err| err,
        };
        try result;
        self.stopBackgroundTasks();
    }

    fn start(
        allocator: std.mem.Allocator,
        io: std.Io,
        core: blocking.BlockingClient,
        options: Options,
    ) !*AsyncClient {
        if (options.command_queue_capacity == 0 or options.notification_queue_capacity == 0) {
            return error.InvalidQueueCapacity;
        }
        const event_buffer = try allocator.alloc(EngineEvent, options.command_queue_capacity);
        errdefer allocator.free(event_buffer);
        const notification_buffer = try allocator.alloc(Inbound, options.notification_queue_capacity);
        errdefer allocator.free(notification_buffer);
        const self = try allocator.create(AsyncClient);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .core = core,
            .events = .init(event_buffer),
            .notifications = .init(notification_buffer),
            .event_buffer = event_buffer,
            .notification_buffer = notification_buffer,
        };

        self.reader = try io.concurrent(readerMain, .{self});
        self.tasks.concurrent(io, engineMain, .{self}) catch |err| {
            self.stopReader();
            self.tasks_running = false;
            return err;
        };
        self.tasks.concurrent(io, timerMain, .{self}) catch |err| {
            self.stopBackgroundTasks();
            return err;
        };
        return self;
    }

    fn requestImpl(self: *AsyncClient, opcode: wire.OpCode, body: anytype) anyerror!Inbound {
        var body_writer = jute.Writer.init(self.allocator);
        defer body_writer.deinit();
        if (@TypeOf(body) != void) try jute.serialize(&body_writer, body);

        var call = RequestCall{
            .opcode = opcode,
            .body_payload = body_writer.bytes(),
        };
        self.events.putOne(self.io, .{ .request = &call }) catch |err| switch (err) {
            error.Closed => return self.terminal_error orelse error.ConnectionLoss,
            error.Canceled => return error.Canceled,
        };
        call.done.wait(self.io) catch |err| switch (err) {
            error.Canceled => {
                call.done.waitUncancelable(self.io);
                switch (call.completion) {
                    .inbound => |inbound_value| {
                        var inbound = inbound_value;
                        inbound.deinit();
                    },
                    else => {},
                }
                return error.Canceled;
            },
        };
        return takeRequestCompletion(&call);
    }

    fn beginOperation(self: *AsyncClient) !void {
        self.operation_mutex.lockUncancelable(self.io);
        defer self.operation_mutex.unlock(self.io);
        if (self.shutting_down) return error.ConnectionLoss;
        self.operation_count += 1;
    }

    fn endOperation(self: *AsyncClient) void {
        self.operation_mutex.lockUncancelable(self.io);
        defer self.operation_mutex.unlock(self.io);
        std.debug.assert(self.operation_count != 0);
        self.operation_count -= 1;
        if (self.operation_count == 0) self.operation_condition.signal(self.io);
    }

    fn beginShutdown(self: *AsyncClient) void {
        self.operation_mutex.lockUncancelable(self.io);
        defer self.operation_mutex.unlock(self.io);
        self.shutting_down = true;
    }

    fn waitForOperations(self: *AsyncClient) void {
        self.operation_mutex.lockUncancelable(self.io);
        defer self.operation_mutex.unlock(self.io);
        while (self.operation_count != 0) {
            self.operation_condition.waitUncancelable(self.io, &self.operation_mutex);
        }
    }

    fn drainNotifications(self: *AsyncClient) void {
        self.notifications.close(self.io);
        while (self.notifications.getOneUncancelable(self.io)) |inbound_value| {
            var inbound = inbound_value;
            inbound.deinit();
        } else |err| switch (err) {
            error.Closed => {},
        }
    }

    fn stopBackgroundTasks(self: *AsyncClient) void {
        if (!self.tasks_running) return;
        self.core.transport.shutdown(self.io);
        self.tasks.cancel(self.io);
        self.stopReader();
        self.core.transport.close(self.io);
        self.tasks_running = false;
    }

    fn stopReader(self: *AsyncClient) void {
        if (self.reader) |*reader| {
            _ = reader.cancel(self.io) catch {};
            self.reader = null;
        }
    }
};

fn sendEncodedRequest(client: *AsyncClient, opcode: wire.OpCode, body_payload: []const u8) !i32 {
    if (!client.core.session.state.isConnected()) return error.ConnectionLoss;
    var writer = jute.Writer.init(client.allocator);
    defer writer.deinit();
    const xid = try client.core.session.encodeRequestPayload(
        &writer,
        opcode,
        body_payload,
        monotonicMs(client.io),
    );
    client.core.transport.writeFrameTimeout(
        client.io,
        writer.bytes(),
        client.core.io_timeout,
    ) catch |err| {
        client.core.session.disconnect(.connection_loss);
        return err;
    };
    return xid;
}

fn sendPing(client: *AsyncClient) !void {
    if (!client.core.session.state.isConnected()) return error.ConnectionLoss;
    var writer = jute.Writer.init(client.allocator);
    defer writer.deinit();
    try client.core.session.encodePing(&writer, monotonicMs(client.io));
    client.core.transport.writeFrameTimeout(
        client.io,
        writer.bytes(),
        client.core.io_timeout,
    ) catch |err| {
        client.core.session.disconnect(.connection_loss);
        return err;
    };
}

fn beginClose(client: *AsyncClient, timeout: std.Io.Timeout) !i32 {
    if (!client.core.session.state.isConnected()) return error.ConnectionLoss;
    if (client.core.session.pendingCount() != 0) return error.PendingRequests;
    var writer = jute.Writer.init(client.allocator);
    defer writer.deinit();
    const xid = try client.core.session.encodeClose(&writer, monotonicMs(client.io));
    client.core.transport.writeFrameTimeout(client.io, writer.bytes(), timeout) catch |err| {
        client.core.session.disconnect(.connection_loss);
        return err;
    };
    return xid;
}

fn acceptPayload(client: *AsyncClient, payload: []u8) !Inbound {
    errdefer client.allocator.free(payload);
    const view = try wire.replyViewWithLimits(payload, client.core.session.limits);
    const kind = try client.core.session.matchReply(view.header, monotonicMs(client.io));
    return .{
        .allocator = client.allocator,
        .payload = payload,
        .header = view.header,
        .kind = kind,
        .body_offset = @intFromPtr(view.body.ptr) - @intFromPtr(payload.ptr),
    };
}

fn monotonicMs(io: std.Io) i64 {
    return std.Io.Clock.awake.now(io).toMilliseconds();
}

fn engineMain(client: *AsyncClient) std.Io.Cancelable!void {
    var waiters: std.AutoHashMapUnmanaged(i32, *RequestCall) = .empty;
    var close_call: ?*CloseCall = null;
    var terminal: anyerror = error.ConnectionLoss;
    defer {
        shutdownEngine(client, &waiters, close_call, terminal);
        waiters.deinit(client.allocator);
    }

    const poll_interval: std.Io.Timeout = .{ .duration = .{
        .raw = .fromMilliseconds(1),
        .clock = .awake,
    } };
    var event_buffer: [1]EngineEvent = undefined;
    while (true) {
        const event_count = client.events.get(client.io, &event_buffer, 0) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Closed => return,
        };
        if (event_count == 0) {
            if (close_call != null) {
                std.atomic.spinLoopHint();
            } else {
                try poll_interval.sleep(client.io);
            }
            continue;
        }
        switch (event_buffer[0]) {
            .request => |call| {
                if (close_call != null) {
                    completeRequest(call, .{ .failure = error.ConnectionLoss }, client.io);
                    continue;
                }
                waiters.ensureUnusedCapacity(client.allocator, 1) catch |err| {
                    completeRequest(call, .{ .failure = err }, client.io);
                    continue;
                };
                const xid = sendEncodedRequest(client, call.opcode, call.body_payload) catch |err| {
                    completeRequest(call, .{ .failure = err }, client.io);
                    if (!client.core.session.state.isConnected()) {
                        terminal = err;
                        return;
                    }
                    continue;
                };
                waiters.putAssumeCapacity(xid, call);
            },
            .close => |call| {
                recordTestStage(.close_received);
                if (close_call != null) {
                    completeClose(call, .{ .failure = error.ConnectionLoss }, client.io);
                    continue;
                }
                const xid = beginClose(client, call.deadline) catch |err| {
                    completeClose(call, .{ .failure = err }, client.io);
                    if (!client.core.session.state.isConnected()) {
                        terminal = err;
                        return;
                    }
                    continue;
                };
                call.xid = xid;
                close_call = call;
                recordTestStage(.close_sent);
            },
            .inbound => |payload| {
                recordTestStage(.reply_received);
                var inbound = acceptPayload(client, payload) catch |err| {
                    terminal = err;
                    return;
                };
                switch (inbound.kind) {
                    .response => |response| {
                        if (close_call) |call| {
                            if (call.xid == inbound.header.xid and response.opcode == .close_session) {
                                inbound.deinit();
                                client.core.session.markClosed();
                                completeClose(call, .success, client.io);
                                recordTestStage(.close_completed);
                                close_call = null;
                                terminal = error.Closed;
                                return;
                            }
                        }
                        if (waiters.fetchRemove(inbound.header.xid)) |entry| {
                            completeRequest(entry.value, .{ .inbound = inbound }, client.io);
                        } else {
                            inbound.deinit();
                            terminal = error.UnexpectedReply;
                            return;
                        }
                    },
                    .notification => {
                        const queued = client.notifications.put(client.io, &.{inbound}, 0) catch |err| {
                            inbound.deinit();
                            switch (err) {
                                error.Canceled => return error.Canceled,
                                error.Closed => return,
                            }
                        };
                        if (queued == 0) {
                            inbound.deinit();
                            terminal = error.NotificationQueueFull;
                            return;
                        }
                    },
                    .ping => inbound.deinit(),
                    .auth => {
                        inbound.deinit();
                        if (client.core.session.state == .auth_failed) {
                            terminal = error.AuthenticationFailed;
                            return;
                        }
                    },
                }
            },
            .read_failure => |err| {
                terminal = err;
                return;
            },
            .tick => {
                const now_ms = monotonicMs(client.io);
                if (client.core.session.hasExpired(now_ms)) {
                    terminal = error.SessionExpired;
                    return;
                }
                if (client.core.session.hasReadTimedOut(now_ms)) {
                    terminal = error.ConnectionLoss;
                    return;
                }
                if (client.core.session.shouldPing(now_ms)) {
                    sendPing(client) catch |err| {
                        terminal = err;
                        return;
                    };
                }
            },
        }
    }
}

fn readerMain(client: *AsyncClient) std.Io.Cancelable!void {
    while (true) {
        const payload = client.core.transport.readFrameAllocTimeout(
            client.allocator,
            client.io,
            client.core.session.limits.max_payload_size,
            client.core.io_timeout,
        ) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Timeout => switch (client.core.io_timeout) {
                .deadline => {
                    client.events.putOne(client.io, .{ .read_failure = error.Timeout }) catch |put_err| switch (put_err) {
                        error.Canceled => return error.Canceled,
                        error.Closed => return,
                    };
                    return;
                },
                .none, .duration => continue,
            },
            else => {
                client.events.putOne(client.io, .{ .read_failure = error.ConnectionLoss }) catch |put_err| switch (put_err) {
                    error.Canceled => return error.Canceled,
                    error.Closed => return,
                };
                return;
            },
        };
        recordTestStage(.reply_read);
        client.events.putOne(client.io, .{ .inbound = payload }) catch |err| {
            client.allocator.free(payload);
            switch (err) {
                error.Canceled => return error.Canceled,
                error.Closed => return,
            }
        };
        recordTestStage(.reply_queued);
    }
}

fn timerMain(client: *AsyncClient) std.Io.Cancelable!void {
    const interval_ms = @max(@divTrunc(client.core.session.readTimeoutMs(), 4), 1);
    const interval: std.Io.Timeout = .{ .duration = .{
        .raw = .fromMilliseconds(interval_ms),
        .clock = .awake,
    } };
    while (true) {
        try interval.sleep(client.io);
        client.events.putOne(client.io, .tick) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Closed => return,
        };
    }
}

fn shutdownEngine(
    client: *AsyncClient,
    waiters: *std.AutoHashMapUnmanaged(i32, *RequestCall),
    close_call: ?*CloseCall,
    terminal: anyerror,
) void {
    client.terminal_error = terminal;
    client.events.close(client.io);
    client.notifications.close(client.io);

    if (client.core.session.state.isConnected()) {
        client.core.session.disconnect(if (terminal == error.SessionExpired) .session_expired else .connection_loss);
    }

    var iterator = waiters.valueIterator();
    while (iterator.next()) |call| {
        completeRequest(call.*, .{ .failure = terminal }, client.io);
    }
    waiters.clearRetainingCapacity();
    if (close_call) |call| completeClose(call, .{ .failure = terminal }, client.io);

    while (client.events.getOneUncancelable(client.io)) |event| {
        switch (event) {
            .request => |call| completeRequest(call, .{ .failure = terminal }, client.io),
            .close => |call| completeClose(call, .{ .failure = terminal }, client.io),
            .inbound => |payload| client.allocator.free(payload),
            .read_failure, .tick => {},
        }
    } else |err| switch (err) {
        error.Closed => {},
    }
    client.terminated.set(client.io);
}

fn completeRequest(call: *RequestCall, completion: RequestCompletion, io: std.Io) void {
    std.debug.assert(call.completion == .pending);
    call.completion = completion;
    call.done.set(io);
}

fn takeRequestCompletion(call: *RequestCall) anyerror!Inbound {
    return switch (call.completion) {
        .pending => unreachable,
        .inbound => |inbound| inbound,
        .failure => |err| err,
    };
}

fn completeClose(call: *CloseCall, completion: CloseCompletion, io: std.Io) void {
    std.debug.assert(call.completion == .pending);
    call.completion = completion;
    call.done.set(io);
}

const test_password = "0123456789abcdef";

fn asyncServerFixture(server: *std.Io.net.Server, io: std.Io) !void {
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
            .sessionId = 80,
            .passwd = test_password,
            .readOnly = false,
        },
        true,
    );
    try transport.writeFrame(io, writer.bytes());

    const first_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(first_payload);
    const first = try wire.decodeRequest(proto.GetDataRequest, first_payload, std.testing.allocator);
    defer first.deinit(std.testing.allocator);

    const second_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(second_payload);
    const second = try wire.decodeRequest(proto.GetDataRequest, second_payload, std.testing.allocator);
    defer second.deinit(std.testing.allocator);

    writer.truncate(0);
    try wire.encodeReply(&writer, .{ .xid = wire.Xid.notification, .zxid = 1, .err = 0 }, {});
    try transport.writeFrame(io, writer.bytes());

    writer.truncate(0);
    try wire.encodeReply(
        &writer,
        .{ .xid = first.header.xid, .zxid = 2, .err = 0 },
        proto.GetDataResponse{
            .data = if (std.mem.eql(u8, first.body.path.?, "/one")) "one" else "two",
            .stat = std.mem.zeroes(@import("../protocol/data.zig").Stat),
        },
    );
    try transport.writeFrame(io, writer.bytes());

    writer.truncate(0);
    try wire.encodeReply(
        &writer,
        .{ .xid = second.header.xid, .zxid = 3, .err = 0 },
        proto.GetDataResponse{
            .data = if (std.mem.eql(u8, second.body.path.?, "/one")) "one" else "two",
            .stat = std.mem.zeroes(@import("../protocol/data.zig").Stat),
        },
    );
    try transport.writeFrame(io, writer.bytes());

    const close_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(close_payload);
    const close_request = try wire.decodeRequest(void, close_payload, std.testing.allocator);
    writer.truncate(0);
    try wire.encodeReply(
        &writer,
        .{ .xid = close_request.header.xid, .zxid = 3, .err = 0 },
        {},
    );
    try transport.writeFrame(io, writer.bytes());
}

test "async client pipelines requests and delivers notifications" {
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

    var server_future = try testing.io.concurrent(asyncServerFixture, .{ &server, testing.io });
    defer server_future.cancel(testing.io) catch {};

    const io_timeout: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(1),
        .clock = .awake,
    } };
    const close_timeout: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(3),
        .clock = .awake,
    } };
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    const client = try AsyncClient.connectAddress(
        testing.allocator,
        testing.io,
        address,
        .{ .connection = .{
            .handshake_timeout = io_timeout,
            .io_timeout = io_timeout,
        } },
        .{},
    );
    defer client.deinit();

    var first_future = try client.requestAsync(
        .get_data,
        proto.GetDataRequest{ .path = "/one", .watch = false },
    );
    var second_future = try client.requestAsync(
        .get_data,
        proto.GetDataRequest{ .path = "/two", .watch = false },
    );

    var notification = try client.receiveNotification();
    defer notification.deinit();
    try testing.expectEqual(wire.Xid.notification, notification.header.xid);

    var first = try first_future.await(testing.io);
    defer first.deinit();
    var second = try second_future.await(testing.io);
    defer second.deinit();

    const first_response = try wire.decodeFrameRecord(proto.GetDataResponse, first.body(), testing.allocator);
    defer jute.deinitDecoded(first_response, testing.allocator);
    try testing.expectEqualStrings("one", first_response.data.?);
    const second_response = try wire.decodeFrameRecord(proto.GetDataResponse, second.body(), testing.allocator);
    defer jute.deinitDecoded(second_response, testing.allocator);
    try testing.expectEqualStrings("two", second_response.data.?);

    try client.close(close_timeout);
    try server_future.await(testing.io);
}

fn pendingServerFixture(server: *std.Io.net.Server, io: std.Io, request_received: *std.Io.Event) !void {
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
            .sessionId = 81,
            .passwd = test_password,
            .readOnly = false,
        },
        true,
    );
    try transport.writeFrame(io, writer.bytes());

    const request_payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(request_payload);
    request_received.set(io);
    const trailing = transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload) catch return;
    std.testing.allocator.free(trailing);
}

fn notificationOverflowServerFixture(server: *std.Io.net.Server, io: std.Io) !void {
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
            .sessionId = 82,
            .passwd = test_password,
            .readOnly = false,
        },
        true,
    );
    try transport.writeFrame(io, writer.bytes());

    for (0..2) |_| {
        writer.truncate(0);
        try wire.encodeReply(&writer, .{ .xid = wire.Xid.notification, .zxid = 1, .err = 0 }, {});
        try transport.writeFrame(io, writer.bytes());
    }
    const trailing = transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload) catch return;
    std.testing.allocator.free(trailing);
}

test "async client disconnects instead of blocking on notification overflow" {
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

    var server_future = try testing.io.concurrent(notificationOverflowServerFixture, .{ &server, testing.io });
    defer server_future.cancel(testing.io) catch {};

    const one_second: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(1),
        .clock = .awake,
    } };
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    const client = try AsyncClient.connectAddress(
        testing.allocator,
        testing.io,
        address,
        .{
            .connection = .{
                .handshake_timeout = one_second,
                .io_timeout = one_second,
            },
            .notification_queue_capacity = 1,
        },
        .{},
    );
    var client_live = true;
    defer if (client_live) client.deinit();

    try client.terminated.wait(testing.io);
    var notification = try client.receiveNotification();
    notification.deinit();
    try testing.expectError(error.NotificationQueueFull, client.receiveNotification());
    client.deinit();
    client_live = false;
    try server_future.await(testing.io);
}

test "async deinit completes an outstanding request future" {
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

    var request_received: std.Io.Event = .unset;
    var server_future = try testing.io.concurrent(pendingServerFixture, .{ &server, testing.io, &request_received });
    defer server_future.cancel(testing.io) catch {};

    const one_second: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(1),
        .clock = .awake,
    } };
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    const client = try AsyncClient.connectAddress(
        testing.allocator,
        testing.io,
        address,
        .{ .connection = .{
            .handshake_timeout = one_second,
            .io_timeout = one_second,
        } },
        .{},
    );

    var request_future = try client.requestAsync(
        .get_data,
        proto.GetDataRequest{ .path = "/pending", .watch = false },
    );
    try request_received.wait(testing.io);
    client.deinit();
    try testing.expectError(error.ConnectionLoss, request_future.await(testing.io));
    try server_future.await(testing.io);
}
