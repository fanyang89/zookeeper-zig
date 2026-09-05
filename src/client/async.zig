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
    /// Automatic reconnection with session resume. Matches the upstream
    /// client: outstanding requests are re-sent, addAuth credentials are
    /// replayed, and registered watches are restored with setWatches2
    /// relative to the last zxid seen. Reconnection stops when the session
    /// expiry budget runs out.
    reconnect: ReconnectPolicy = .{},
};

pub const ReconnectPolicy = struct {
    enabled: bool = true,
    /// Zero means unlimited attempts, bounded by the session timeout.
    max_attempts: usize = 0,
    first_backoff_ms: u64 = 100,
    max_backoff_ms: u64 = 4_000,
};

/// KeeperState values delivered with synthetic WatcherEvent notifications
/// (event type None) on connection state changes, matching upstream.
pub const KeeperState = struct {
    pub const disconnected: i32 = 0;
    pub const auth_failed: i32 = 4;
    pub const sync_connected: i32 = 3;
    pub const expired: i32 = -112;
};

const Endpoint = union(enum) {
    address: std.Io.net.IpAddress,
    host: struct { name: []u8, port: u16 },

    fn deinit(self: *Endpoint, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .host => |value| allocator.free(value.name),
            .address => {},
        }
        self.* = undefined;
    }
};

const WatchKind = enum { data, exist, children, persistent, persistent_recursive };

const WatchEntry = struct {
    path: []u8,
    kind: WatchKind,
};

const AuthReplay = struct {
    scheme: []u8,
    credential: []u8,
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
    endpoint: ?Endpoint = null,
    reconnect: ReconnectPolicy = .{},
    auth_replays: std.ArrayListUnmanaged(AuthReplay) = .empty,
    pending_auth: ?*RequestCall = null,
    watches: std.ArrayListUnmanaged(WatchEntry) = .empty,
    last_restore_zxid: i64 = 0,
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
        return start(allocator, io, core, options, .{ .address = address });
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
        const name = try allocator.dupe(u8, host);
        errdefer allocator.free(name);
        return start(allocator, io, core, options, .{ .host = .{ .name = name, .port = port } });
    }

    /// Stops background tasks, completes active operations, closes the
    /// transport, and releases queued notifications.
    pub fn deinit(self: *AsyncClient) void {
        self.beginShutdown();
        self.stopBackgroundTasks();
        self.waitForOperations();
        self.drainNotifications();
        self.core.deinit();
        for (self.auth_replays.items) |replay| {
            self.allocator.free(replay.scheme);
            self.allocator.free(replay.credential);
        }
        self.auth_replays.deinit(self.allocator);
        for (self.watches.items) |entry| self.allocator.free(entry.path);
        self.watches.deinit(self.allocator);
        if (self.endpoint) |*saved| saved.deinit(self.allocator);
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
        endpoint: Endpoint,
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
            .endpoint = endpoint,
            .reconnect = options.reconnect,
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
        self.recordRegistration(opcode, body);

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

    /// Records auth credentials and watch registrations so they can be
    /// replayed or restored after a reconnect, matching the upstream client.
    fn recordRegistration(self: *AsyncClient, opcode: wire.OpCode, body: anytype) void {
        const T = @TypeOf(body);
        if (T == void) return;
        if (T == proto.AuthPacket and opcode == .auth) {
            const scheme = body.scheme orelse return;
            const credential = body.auth orelse return;
            self.addAuthReplay(scheme, credential) catch return;
            return;
        }
        if (T == proto.ExistsRequest and opcode == .exists) {
            if (body.watch) self.addWatch(.exist, body.path) catch {};
            return;
        }
        if (T == proto.GetDataRequest and opcode == .get_data) {
            if (body.watch) self.addWatch(.data, body.path) catch {};
            return;
        }
        if ((T == proto.GetChildrenRequest and opcode == .get_children) or
            (T == proto.GetChildren2Request and opcode == .get_children2))
        {
            if (body.watch) self.addWatch(.children, body.path) catch {};
            return;
        }
        if (T == proto.AddWatchRequest and opcode == .add_watch) {
            const kind: WatchKind = switch (body.mode) {
                1 => .persistent,
                2 => .persistent_recursive,
                else => return,
            };
            self.addWatch(kind, body.path) catch {};
            return;
        }
        if ((T == proto.RemoveWatchesRequest and opcode == .remove_watches) or
            (T == proto.CheckWatchesRequest and opcode == .check_watches))
        {
            const path = body.path orelse return;
            const kind: WatchKind = switch (body.type) {
                1 => .children,
                2 => .data,
                4 => .persistent,
                5 => .persistent_recursive,
                else => return,
            };
            self.removeWatch(kind, path);
        }
    }

    fn addAuthReplay(self: *AsyncClient, scheme: []const u8, credential: []const u8) !void {
        for (self.auth_replays.items) |existing| {
            if (std.mem.eql(u8, existing.scheme, scheme) and
                std.mem.eql(u8, existing.credential, credential)) return;
        }
        try self.auth_replays.append(self.allocator, .{
            .scheme = try self.allocator.dupe(u8, scheme),
            .credential = try self.allocator.dupe(u8, credential),
        });
    }

    fn addWatch(self: *AsyncClient, kind: WatchKind, maybe_path: ?[]const u8) !void {
        const path = maybe_path orelse return;
        for (self.watches.items) |existing| {
            if (existing.kind == kind and std.mem.eql(u8, existing.path, path)) return;
        }
        try self.watches.append(self.allocator, .{
            .path = try self.allocator.dupe(u8, path),
            .kind = kind,
        });
    }

    fn removeWatch(self: *AsyncClient, kind: WatchKind, path: []const u8) void {
        for (self.watches.items, 0..) |entry, index| {
            if (entry.kind == kind and std.mem.eql(u8, entry.path, path)) {
                self.allocator.free(entry.path);
                _ = self.watches.orderedRemove(index);
                return;
            }
        }
    }

    /// Drops one-shot watches when their event fires. Persistent watches
    /// survive, matching upstream delivery semantics.
    fn consumeWatchEvents(self: *AsyncClient, event_type: i32, path: []const u8) void {
        switch (event_type) {
            1 => self.removeWatch(.exist, path), // NodeCreated
            2 => { // NodeDeleted
                self.removeWatch(.data, path);
                self.removeWatch(.exist, path);
                self.removeWatch(.children, path);
            },
            3 => { // NodeDataChanged
                self.removeWatch(.data, path);
                self.removeWatch(.exist, path);
            },
            4 => self.removeWatch(.children, path), // NodeChildrenChanged
            else => {},
        }
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

fn sendAuthCall(
    client: *AsyncClient,
    call: *RequestCall,
    waiters: *std.AutoHashMapUnmanaged(i32, *RequestCall),
    terminal: *anyerror,
) !void {
    _ = waiters;
    _ = terminal;
    if (client.pending_auth != null) {
        completeRequest(call, .{ .failure = error.AuthInProgress }, client.io);
        return;
    }
    var reader = jute.Reader.init(call.body_payload);
    const packet = jute.deserialize(proto.AuthPacket, &reader, client.allocator) catch |err| {
        completeRequest(call, .{ .failure = err }, client.io);
        return;
    };
    defer jute.deinitDecoded(packet, client.allocator);
    client.core.sendAuth(packet.scheme orelse "", packet.auth orelse "") catch |err| {
        if (client.core.session.state.isConnected()) {
            completeRequest(call, .{ .failure = err }, client.io);
            return;
        }
        // The reconnect path replays recorded credentials, so this call
        // completes when the replayed auth reply arrives.
        client.pending_auth = call;
        return;
    };
    client.pending_auth = call;
}

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

    while (true) {
        const event = client.events.getOne(client.io) catch |err| switch (err) {
            error.Canceled => return error.Canceled,
            error.Closed => return,
        };
        switch (event) {
            .request => |call| {
                if (close_call != null) {
                    completeRequest(call, .{ .failure = error.ConnectionLoss }, client.io);
                    continue;
                }
                if (call.opcode == .auth) {
                    sendAuthCall(client, call, &waiters, &terminal) catch |err| {
                        terminal = err;
                        return;
                    };
                    continue;
                }
                waiters.ensureUnusedCapacity(client.allocator, 1) catch |err| {
                    completeRequest(call, .{ .failure = err }, client.io);
                    continue;
                };
                const xid = sendEncodedRequest(client, call.opcode, call.body_payload) catch |err| {
                    if (client.core.session.state.isConnected()) {
                        completeRequest(call, .{ .failure = err }, client.io);
                        continue;
                    }
                    const resume_error = reconnectOrTerminal(client, &waiters, err);
                    if (resume_error) |fatal| {
                        terminal = fatal;
                        return;
                    }
                    const retry_xid = sendEncodedRequest(client, call.opcode, call.body_payload) catch |retry_err| {
                        completeRequest(call, .{ .failure = retry_err }, client.io);
                        continue;
                    };
                    waiters.putAssumeCapacity(retry_xid, call);
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
                    if (client.core.session.state.isConnected()) {
                        completeClose(call, .{ .failure = err }, client.io);
                        continue;
                    }
                    const resume_error = reconnectOrTerminal(client, &waiters, err);
                    if (resume_error) |fatal| {
                        terminal = fatal;
                        return;
                    }
                    const retry_xid = beginClose(client, call.deadline) catch |retry_err| {
                        completeClose(call, .{ .failure = retry_err }, client.io);
                        continue;
                    };
                    call.xid = retry_xid;
                    close_call = call;
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
                        if (inbound.header.xid == wire.Xid.set_watches) {
                            // Internal watch-restoration ack, not a user call.
                            inbound.deinit();
                            continue;
                        }
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
                        consumeNotificationWatches(client, inbound.body());
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
                            if (client.pending_auth) |call| {
                                client.pending_auth = null;
                                completeRequest(call, .{ .failure = error.AuthenticationFailed }, client.io);
                            }
                            terminal = error.AuthenticationFailed;
                            return;
                        }
                        if (client.pending_auth) |call| {
                            client.pending_auth = null;
                            completeRequest(call, .{ .inbound = .{
                                .allocator = client.allocator,
                                .payload = &.{},
                                .header = inbound.header,
                                .kind = .auth,
                                .body_offset = 0,
                            } }, client.io);
                        }
                    },
                }
            },
            .read_failure => |err| {
                if (resume_error: {
                    break :resume_error reconnectOrTerminal(client, &waiters, err);
                }) |fatal| {
                    terminal = fatal;
                    return;
                }
            },
            .tick => {
                const now_ms = monotonicMs(client.io);
                if (client.core.session.hasExpired(now_ms)) {
                    terminal = error.SessionExpired;
                    return;
                }
                if (client.core.session.hasReadTimedOut(now_ms)) {
                    const resume_error = reconnectOrTerminal(client, &waiters, error.ConnectionLoss);
                    if (resume_error) |fatal| {
                        terminal = fatal;
                        return;
                    }
                    continue;
                }
                if (client.core.session.shouldPing(now_ms)) {
                    sendPing(client) catch |err| {
                        if (client.core.session.state.isConnected()) {
                            terminal = err;
                            return;
                        }
                        const resume_error = reconnectOrTerminal(client, &waiters, err);
                        if (resume_error) |fatal| {
                            terminal = fatal;
                            return;
                        }
                    };
                }
            },
        }
    }
}

/// Attempts a reconnect with session resume. Returns null on success (the
/// engine continues with the new connection) or the terminal error that ended
/// the session. Outstanding requests are re-sent, auth credentials replayed,
/// and watches restored relative to the last zxid seen before the drop.
fn reconnectOrTerminal(
    client: *AsyncClient,
    waiters: *std.AutoHashMapUnmanaged(i32, *RequestCall),
    cause: anyerror,
) ?anyerror {
    if (!client.reconnect.enabled) return cause;
    if (client.shutting_down) return cause;
    const endpoint = client.endpoint orelse return cause;
    if (client.core.session.session_id == 0) return cause;
    if (client.core.session.state == .closed or client.core.session.state == .auth_failed) {
        return cause;
    }

    client.stopReader();
    client.core.transport.shutdown(client.io);

    const session = &client.core.session;
    const resume_config = session_mod.Config{
        .session_timeout_ms = session.requested_timeout_ms,
        .session_id = session.session_id,
        .passwd = session.passwd,
        .last_zxid_seen = session.last_zxid_seen,
        .allow_read_only = session.allow_read_only,
        .seen_read_write_server = session.seen_read_write_server,
        .wire_limits = session.limits,
    };
    const restore_zxid = session.last_zxid_seen;

    if (session.state.isConnected()) session.disconnect(.connection_loss);

    // Requests that were in flight move to the resend queue.
    var resends: std.ArrayListUnmanaged(*RequestCall) = .empty;
    defer resends.deinit(client.allocator);
    for (session.failedRequests()) |failed| {
        if (waiters.fetchRemove(failed.request.xid)) |entry| {
            resends.append(client.allocator, entry.value) catch {
                completeRequest(entry.value, .{ .failure = error.ConnectionLoss }, client.io);
            };
        }
    }
    session.clearFailedRequests();

    client.last_restore_zxid = restore_zxid;
    publishStateEvent(client, KeeperState.disconnected);

    const deadline_ms = monotonicMs(client.io) + @max(session.expirationTimeoutMs(), 0);
    var backoff_ms = client.reconnect.first_backoff_ms;
    var attempts: usize = 0;
    reconnect: while (true) {
        if (client.shutting_down) return cause;
        attempts += 1;
        if (client.reconnect.max_attempts != 0 and attempts > client.reconnect.max_attempts) {
            return cause;
        }
        const now_ms = monotonicMs(client.io);
        if (now_ms >= deadline_ms) return error.SessionExpired;
        const sleep_ms = @min(backoff_ms, client.reconnect.max_backoff_ms);
        const sleep: std.Io.Timeout = .{ .duration = .{
            .raw = .fromMilliseconds(@intCast(sleep_ms)),
            .clock = .awake,
        } };
        sleep.sleep(client.io) catch |err| {
            if (client.shutting_down) return cause;
            return err;
        };
        backoff_ms = @min(backoff_ms * 2, client.reconnect.max_backoff_ms);

        var candidate = connectEndpoint(client, endpoint, resume_config) catch |err| switch (err) {
            error.SessionExpired, error.InvalidSessionCredentials => {
                publishStateEvent(client, KeeperState.expired);
                return error.SessionExpired;
            },
            else => continue :reconnect,
        };
        for (client.auth_replays.items) |replay| {
            candidate.sendAuth(replay.scheme, replay.credential) catch {
                candidate.deinit();
                continue :reconnect;
            };
        }
        if (client.watches.items.len > 0) {
            restoreWatches(client, &candidate, restore_zxid) catch {
                candidate.deinit();
                continue :reconnect;
            };
        }

        // Swap in the resumed connection. The candidate session already
        // duped the credentials, so freeing the old session is safe.
        client.core.deinit();
        client.core = candidate;

        client.reader = client.io.concurrent(readerMain, .{client}) catch |err| {
            return err;
        };
        for (resends.items) |call| {
            const xid = sendEncodedRequest(client, call.opcode, call.body_payload) catch |err| {
                completeRequest(call, .{ .failure = err }, client.io);
                continue;
            };
            waiters.put(client.allocator, xid, call) catch {
                completeRequest(call, .{ .failure = error.OutOfMemory }, client.io);
            };
        }
        publishStateEvent(client, KeeperState.sync_connected);
        return null;
    }
}

fn connectEndpoint(
    client: *AsyncClient,
    endpoint: Endpoint,
    resume_config: session_mod.Config,
) !blocking.BlockingClient {
    switch (endpoint) {
        .address => |address| return blocking.BlockingClient.connectAddress(
            client.allocator,
            client.io,
            address,
            .{ .connect = .{ .mode = .stream, .protocol = .tcp } },
            resume_config,
        ),
        .host => |value| return blocking.BlockingClient.connectHost(
            client.allocator,
            client.io,
            value.name,
            value.port,
            .{ .connect = .{ .mode = .stream, .protocol = .tcp } },
            resume_config,
        ),
    }
}

fn restoreWatches(
    client: *AsyncClient,
    core: *blocking.BlockingClient,
    relative_zxid: i64,
) !void {
    var data: std.ArrayListUnmanaged(?[]const u8) = .empty;
    var exist: std.ArrayListUnmanaged(?[]const u8) = .empty;
    var children: std.ArrayListUnmanaged(?[]const u8) = .empty;
    var persistent: std.ArrayListUnmanaged(?[]const u8) = .empty;
    var persistent_recursive: std.ArrayListUnmanaged(?[]const u8) = .empty;
    defer {
        data.deinit(client.allocator);
        exist.deinit(client.allocator);
        children.deinit(client.allocator);
        persistent.deinit(client.allocator);
        persistent_recursive.deinit(client.allocator);
    }
    for (client.watches.items) |entry| {
        const target = switch (entry.kind) {
            .data => &data,
            .exist => &exist,
            .children => &children,
            .persistent => &persistent,
            .persistent_recursive => &persistent_recursive,
        };
        try target.append(client.allocator, entry.path);
    }
    const body = proto.SetWatches2{
        .relativeZxid = relative_zxid,
        .dataWatches = data.items,
        .existWatches = exist.items,
        .childWatches = children.items,
        .persistentWatches = persistent.items,
        .persistentRecursiveWatches = persistent_recursive.items,
    };
    try core.sendSetWatches(body, true);
}

/// Drops one-shot watch registrations when their notification is delivered.
fn consumeNotificationWatches(client: *AsyncClient, body: []const u8) void {
    var reader = jute.Reader.init(body);
    const event = jute.deserialize(proto.WatcherEvent, &reader, client.allocator) catch return;
    defer jute.deinitDecoded(event, client.allocator);
    const path = event.path orelse return;
    client.consumeWatchEvents(event.type, path);
}

/// Enqueues a synthetic WatcherEvent (type None) describing a connection
/// state change, mirroring the upstream default-watcher state events.
fn publishStateEvent(client: *AsyncClient, state: i32) void {
    var writer = jute.Writer.init(client.allocator);
    defer writer.deinit();
    wire.encodeReply(
        &writer,
        .{ .xid = wire.Xid.notification, .zxid = -1, .err = 0 },
        proto.WatcherEvent{ .type = -1, .state = state, .path = "" },
    ) catch return;
    if (writer.bytes().len < wire.reply_header_size + 4) return;
    const payload = client.allocator.dupe(u8, writer.bytes()[4..]) catch return;
    var inbound = Inbound{
        .allocator = client.allocator,
        .payload = payload,
        .header = .{ .xid = wire.Xid.notification, .zxid = -1, .err = 0 },
        .kind = .notification,
        .body_offset = wire.reply_header_size,
    };
    const queued = client.notifications.put(client.io, &.{inbound}, 0) catch {
        inbound.deinit();
        return;
    };
    if (queued == 0) inbound.deinit();
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
    if (client.pending_auth) |call| {
        client.pending_auth = null;
        completeRequest(call, .{ .failure = terminal }, client.io);
    }
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

fn reconnectFixture(server: *std.Io.net.Server, io: std.Io) !void {
    const allocator = std.testing.allocator;
    var writer = jute.Writer.init(allocator);
    defer writer.deinit();

    // ---- Connection 1: fresh session, auth, watched read, unanswered read.
    {
        const stream = try server.accept(io);
        var transport = TcpTransport.fromStream(stream);
        defer transport.close(io);

        const connect_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(connect_payload);
        const connect = try wire.decodeConnectRequest(connect_payload);
        try std.testing.expectEqual(@as(i64, 0), connect.value.sessionId);

        writer.truncate(0);
        try wire.encodeConnectResponse(&writer, .{
            .protocolVersion = 0,
            .timeOut = 24_000,
            .sessionId = 80,
            .passwd = test_password,
            .readOnly = false,
        }, true);
        try transport.writeFrame(io, writer.bytes());

        const auth_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(auth_payload);
        const auth = try wire.decodeRequest(proto.AuthPacket, auth_payload, allocator);
        defer auth.deinit(allocator);
        try std.testing.expectEqualStrings("digest", auth.body.scheme.?);
        try std.testing.expectEqualStrings("user:pass", auth.body.auth.?);
        writer.truncate(0);
        try wire.encodeReply(&writer, .{ .xid = wire.Xid.auth, .zxid = 0, .err = 0 }, {});
        try transport.writeFrame(io, writer.bytes());

        const watched_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(watched_payload);
        const watched = try wire.decodeRequest(proto.GetDataRequest, watched_payload, allocator);
        defer watched.deinit(allocator);
        try std.testing.expect(watched.body.watch);
        try std.testing.expectEqualStrings("/watched", watched.body.path.?);
        writer.truncate(0);
        try wire.encodeReply(
            &writer,
            .{ .xid = watched.header.xid, .zxid = 5, .err = 0 },
            proto.GetDataResponse{
                .data = "first",
                .stat = std.mem.zeroes(@import("../protocol/data.zig").Stat),
            },
        );
        try transport.writeFrame(io, writer.bytes());

        // The pending request is read but never answered; the connection
        // drops right after.
        const pending_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(pending_payload);
        _ = try wire.decodeRequest(proto.GetDataRequest, pending_payload, allocator);
    }

    // ---- Connection 2: session resume, auth replay, watch restore, resend.
    {
        const stream = try server.accept(io);
        var transport = TcpTransport.fromStream(stream);
        defer transport.close(io);

        const connect_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(connect_payload);
        const connect = try wire.decodeConnectRequest(connect_payload);
        try std.testing.expectEqual(@as(i64, 80), connect.value.sessionId);
        try std.testing.expectEqualSlices(u8, test_password, connect.value.passwd.?);
        try std.testing.expectEqual(@as(i64, 5), connect.value.lastZxidSeen);

        writer.truncate(0);
        try wire.encodeConnectResponse(&writer, .{
            .protocolVersion = 0,
            .timeOut = 24_000,
            .sessionId = 80,
            .passwd = test_password,
            .readOnly = false,
        }, true);
        try transport.writeFrame(io, writer.bytes());

        const auth_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(auth_payload);
        const auth = try wire.decodeRequest(proto.AuthPacket, auth_payload, allocator);
        defer auth.deinit(allocator);
        try std.testing.expectEqualStrings("digest", auth.body.scheme.?);
        writer.truncate(0);
        try wire.encodeReply(&writer, .{ .xid = wire.Xid.auth, .zxid = 0, .err = 0 }, {});
        try transport.writeFrame(io, writer.bytes());

        const watches_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(watches_payload);
        const watches = try wire.decodeRequest(proto.SetWatches2, watches_payload, allocator);
        defer watches.deinit(allocator);
        try std.testing.expectEqual(@as(i64, 5), watches.body.relativeZxid);
        const data_watches = watches.body.dataWatches.?;
        try std.testing.expectEqual(@as(usize, 1), data_watches.len);
        try std.testing.expectEqualStrings("/watched", data_watches[0].?);
        writer.truncate(0);
        try wire.encodeReply(&writer, .{ .xid = wire.Xid.set_watches, .zxid = 5, .err = 0 }, {});
        try transport.writeFrame(io, writer.bytes());

        const resent_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(resent_payload);
        const resent = try wire.decodeRequest(proto.GetDataRequest, resent_payload, allocator);
        defer resent.deinit(allocator);
        try std.testing.expectEqualStrings("/pending", resent.body.path.?);
        writer.truncate(0);
        try wire.encodeReply(
            &writer,
            .{ .xid = resent.header.xid, .zxid = 6, .err = 0 },
            proto.GetDataResponse{
                .data = "replayed",
                .stat = std.mem.zeroes(@import("../protocol/data.zig").Stat),
            },
        );
        try transport.writeFrame(io, writer.bytes());

        const close_payload = try transport.readFrameAlloc(allocator, io, wire.default_max_payload);
        defer allocator.free(close_payload);
        const close_request = try wire.decodeRequest(void, close_payload, allocator);
        writer.truncate(0);
        try wire.encodeReply(
            &writer,
            .{ .xid = close_request.header.xid, .zxid = 6, .err = 0 },
            {},
        );
        try transport.writeFrame(io, writer.bytes());
    }
}

test "async client reconnects with session resume and replays state" {
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

    var server_future = try testing.io.concurrent(reconnectFixture, .{ &server, testing.io });
    defer server_future.cancel(testing.io) catch {};

    const io_timeout: std.Io.Timeout = .{ .duration = .{
        .raw = .fromSeconds(5),
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
        }, .reconnect = .{
            .first_backoff_ms = 10,
            .max_backoff_ms = 50,
        } },
        .{},
    );
    defer client.deinit();

    var auth_future = try client.requestAsync(.auth, proto.AuthPacket{
        .type = 0,
        .scheme = "digest",
        .auth = "user:pass",
    });
    var auth_reply = try auth_future.await(testing.io);
    auth_reply.deinit();

    var watched_future = try client.requestAsync(
        .get_data,
        proto.GetDataRequest{ .path = "/watched", .watch = true },
    );
    var watched = try watched_future.await(testing.io);
    watched.deinit();

    var pending_future = try client.requestAsync(
        .get_data,
        proto.GetDataRequest{ .path = "/pending", .watch = false },
    );

    // The drop produces a synthetic Disconnected event followed by
    // SyncConnected after the resume completes.
    var disconnected = try client.receiveNotification();
    defer disconnected.deinit();
    try testing.expectEqual(@as(i32, -1), readEventType(disconnected.body()));
    try testing.expectEqual(KeeperState.disconnected, readEventState(disconnected.body()));

    var restored = try client.receiveNotification();
    defer restored.deinit();
    try testing.expectEqual(KeeperState.sync_connected, readEventState(restored.body()));

    try testing.expectEqual(@as(i64, 80), client.core.session.session_id);

    var replayed = try pending_future.await(testing.io);
    defer replayed.deinit();
    try testing.expectEqualStrings("replayed", decodeGetData(replayed.body()));

    try client.close(io_timeout);
}

fn readEventType(body: []const u8) i32 {
    var reader = jute.Reader.init(body);
    const event = jute.deserialize(proto.WatcherEvent, &reader, std.testing.allocator) catch return std.math.minInt(i32);
    defer jute.deinitDecoded(event, std.testing.allocator);
    return event.type;
}

fn readEventState(body: []const u8) i32 {
    var reader = jute.Reader.init(body);
    const event = jute.deserialize(proto.WatcherEvent, &reader, std.testing.allocator) catch return std.math.minInt(i32);
    defer jute.deinitDecoded(event, std.testing.allocator);
    return event.state;
}

fn decodeGetData(body: []const u8) []const u8 {
    var reader = jute.Reader.init(body);
    const response = jute.deserialize(proto.GetDataResponse, &reader, std.testing.allocator) catch return "";
    defer jute.deinitDecoded(response, std.testing.allocator);
    return response.data orelse "";
}
