const std = @import("std");

pub const ConnectionId = u64;
pub const BatchId = u64;

pub const RegistrationContext = struct {
    connection_id: ConnectionId,
    batch_id: BatchId,
};

pub const Kind = enum {
    data,
    children,
};

pub const EventType = enum(i32) {
    node_created = 1,
    node_deleted = 2,
    node_data_changed = 3,
    node_children_changed = 4,
};

pub const Event = struct {
    type: EventType,
    zxid: i64,
    path: []const u8,
};

pub const Sink = struct {
    context: *anyopaque,
    notifyFn: *const fn (context: *anyopaque, event: Event) bool,
    failFn: *const fn (context: *anyopaque) void = ignoreFailure,

    pub fn notify(self: Sink, event: Event) bool {
        return self.notifyFn(self.context, event);
    }

    pub fn fail(self: Sink) void {
        self.failFn(self.context);
    }
};

fn ignoreFailure(_: *anyopaque) void {}

const Registration = struct {
    kind: Kind,
    path: []u8,
    batch_id: BatchId,
    armed: bool = false,
    pending: ?PendingEvent = null,
};

const PendingEvent = struct {
    type: EventType,
    zxid: i64,
};

const Connection = struct {
    sink: Sink,
    session_id: i64,
    generation: u64,
    active: bool = true,
    watches: std.ArrayListUnmanaged(Registration) = .empty,
};

pub const Manager = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    connections: std.AutoHashMapUnmanaged(ConnectionId, Connection) = .empty,
    next_connection_id: ConnectionId = 1,
    next_batch_id: BatchId = 1,

    pub fn init(allocator: std.mem.Allocator) Manager {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Manager) void {
        lock(&self.mutex);
        var connections = self.connections.valueIterator();
        while (connections.next()) |connection| deinitConnection(self.allocator, connection);
        self.connections.deinit(self.allocator);
        self.mutex.unlock();
        self.* = undefined;
    }

    pub fn addConnection(self: *Manager, sink: Sink) !ConnectionId {
        return self.addSessionConnection(0, 0, sink);
    }

    pub fn addSessionConnection(
        self: *Manager,
        session_id: i64,
        generation: u64,
        sink: Sink,
    ) !ConnectionId {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const id = self.next_connection_id;
        self.next_connection_id = std.math.add(ConnectionId, id, 1) catch
            return error.ConnectionIdOverflow;
        try self.connections.putNoClobber(self.allocator, id, .{
            .sink = sink,
            .session_id = session_id,
            .generation = generation,
        });
        return id;
    }

    pub fn removeConnection(self: *Manager, connection_id: ConnectionId) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.connections.fetchRemove(connection_id)) |removed| {
            var connection = removed.value;
            deinitConnection(self.allocator, &connection);
        }
    }

    pub fn invalidateSession(
        self: *Manager,
        session_id: i64,
        generation: ?u64,
    ) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var connections = self.connections.valueIterator();
        while (connections.next()) |connection| {
            if (!connection.active or connection.session_id != session_id or
                (generation != null and connection.generation != generation.?)) continue;
            connection.sink.fail();
            deactivateConnection(self.allocator, connection);
        }
    }

    pub fn invalidateAll(self: *Manager) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var connections = self.connections.valueIterator();
        while (connections.next()) |connection| {
            if (!connection.active) continue;
            connection.sink.fail();
            deactivateConnection(self.allocator, connection);
        }
    }

    pub fn beginBatch(self: *Manager, connection_id: ConnectionId) !BatchId {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const connection = self.connections.getPtr(connection_id) orelse
            return error.ConnectionClosed;
        if (!connection.active) return error.ConnectionClosed;
        const id = self.next_batch_id;
        self.next_batch_id = std.math.add(BatchId, id, 1) catch
            return error.BatchIdOverflow;
        return id;
    }

    pub fn register(
        self: *Manager,
        connection_id: ConnectionId,
        batch_id: BatchId,
        kind: Kind,
        path: []const u8,
    ) !void {
        return self.registerImpl(connection_id, batch_id, kind, path, null);
    }

    pub fn registerImmediate(
        self: *Manager,
        connection_id: ConnectionId,
        batch_id: BatchId,
        kind: Kind,
        path: []const u8,
        event_type: EventType,
        zxid: i64,
    ) !void {
        return self.registerImpl(connection_id, batch_id, kind, path, .{
            .type = event_type,
            .zxid = zxid,
        });
    }

    fn registerImpl(
        self: *Manager,
        connection_id: ConnectionId,
        batch_id: BatchId,
        kind: Kind,
        path: []const u8,
        pending: ?PendingEvent,
    ) !void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const connection = self.connections.getPtr(connection_id) orelse
            return error.ConnectionClosed;
        if (!connection.active) return error.ConnectionClosed;
        for (connection.watches.items) |*registration| {
            if (registration.kind != kind or !std.mem.eql(u8, registration.path, path)) continue;
            if (pending) |event| {
                if (registration.pending == null) {
                    registration.batch_id = batch_id;
                    registration.armed = false;
                    registration.pending = event;
                }
            }
            return;
        }
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        try connection.watches.append(self.allocator, .{
            .kind = kind,
            .path = owned_path,
            .batch_id = batch_id,
            .pending = pending,
        });
    }

    pub fn activateBatch(
        self: *Manager,
        connection_id: ConnectionId,
        batch_id: BatchId,
    ) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const connection = self.connections.getPtr(connection_id) orelse return;
        if (!connection.active) return;
        var index: usize = 0;
        while (index < connection.watches.items.len) {
            const registration = &connection.watches.items[index];
            if (registration.batch_id != batch_id) {
                index += 1;
                continue;
            }
            if (registration.pending) |pending| {
                const event = Event{
                    .type = pending.type,
                    .zxid = pending.zxid,
                    .path = registration.path,
                };
                const delivered = connection.sink.notify(event);
                removeRegistration(self.allocator, &connection.watches, index);
                if (!delivered) {
                    deactivateConnection(self.allocator, connection);
                    return;
                }
                continue;
            }
            registration.armed = true;
            index += 1;
        }
    }

    pub fn publish(self: *Manager, event: Event) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var connections = self.connections.valueIterator();
        while (connections.next()) |connection| {
            if (!connection.active) continue;
            var matched = false;
            var armed = false;
            var pending_index: ?usize = null;
            for (connection.watches.items, 0..) |registration, index| {
                if (registration.pending != null or
                    !std.mem.eql(u8, registration.path, event.path) or
                    !matches(registration.kind, event.type)) continue;
                matched = true;
                armed = armed or registration.armed;
                if (pending_index == null) pending_index = index;
            }
            if (!matched) continue;
            if (armed) {
                removeMatching(self.allocator, &connection.watches, event);
                if (!connection.sink.notify(event)) deactivateConnection(self.allocator, connection);
                continue;
            }

            const retained = pending_index.?;
            connection.watches.items[retained].pending = .{
                .type = event.type,
                .zxid = event.zxid,
            };
            var index = connection.watches.items.len;
            while (index > 0) {
                index -= 1;
                if (index == retained) continue;
                const registration = connection.watches.items[index];
                if (registration.pending == null and
                    std.mem.eql(u8, registration.path, event.path) and
                    matches(registration.kind, event.type))
                {
                    removeRegistration(self.allocator, &connection.watches, index);
                }
            }
        }
    }
};

fn matches(kind: Kind, event_type: EventType) bool {
    return switch (event_type) {
        .node_created, .node_data_changed => kind == .data,
        .node_deleted => true,
        .node_children_changed => kind == .children,
    };
}

fn removeMatching(
    allocator: std.mem.Allocator,
    watches: *std.ArrayListUnmanaged(Registration),
    event: Event,
) void {
    var index = watches.items.len;
    while (index > 0) {
        index -= 1;
        const registration = watches.items[index];
        if (registration.pending == null and
            std.mem.eql(u8, registration.path, event.path) and
            matches(registration.kind, event.type))
        {
            removeRegistration(allocator, watches, index);
        }
    }
}

fn removeRegistration(
    allocator: std.mem.Allocator,
    watches: *std.ArrayListUnmanaged(Registration),
    index: usize,
) void {
    const removed = watches.swapRemove(index);
    allocator.free(removed.path);
}

fn deactivateConnection(allocator: std.mem.Allocator, connection: *Connection) void {
    connection.active = false;
    for (connection.watches.items) |registration| allocator.free(registration.path);
    connection.watches.clearRetainingCapacity();
}

fn deinitConnection(allocator: std.mem.Allocator, connection: *Connection) void {
    for (connection.watches.items) |registration| allocator.free(registration.path);
    connection.watches.deinit(allocator);
    connection.* = undefined;
}

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

test "pending watch fires only after its response batch activates" {
    const testing = std.testing;
    const Recorder = struct {
        events: std.ArrayListUnmanaged(EventType) = .empty,

        fn notify(context: *anyopaque, event: Event) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.events.append(testing.allocator, event.type) catch return false;
            return true;
        }
    };
    var recorder = Recorder{};
    defer recorder.events.deinit(testing.allocator);
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const connection = try manager.addConnection(.{
        .context = &recorder,
        .notifyFn = Recorder.notify,
    });
    const batch = try manager.beginBatch(connection);
    try manager.register(connection, batch, .data, "/node");
    manager.publish(.{ .type = .node_created, .zxid = 7, .path = "/node" });
    try testing.expectEqual(@as(usize, 0), recorder.events.items.len);
    manager.activateBatch(connection, batch);
    try testing.expectEqualSlices(EventType, &.{.node_created}, recorder.events.items);
    manager.publish(.{ .type = .node_data_changed, .zxid = 8, .path = "/node" });
    try testing.expectEqual(@as(usize, 1), recorder.events.items.len);
}

test "delete suppresses duplicate data and child notifications" {
    const testing = std.testing;
    const Recorder = struct {
        count: usize = 0,

        fn notify(context: *anyopaque, event: Event) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.count += 1;
            testing.expectEqual(EventType.node_deleted, event.type) catch return false;
            return true;
        }
    };
    var recorder = Recorder{};
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const connection = try manager.addConnection(.{
        .context = &recorder,
        .notifyFn = Recorder.notify,
    });
    const batch = try manager.beginBatch(connection);
    try manager.register(connection, batch, .data, "/node");
    try manager.register(connection, batch, .children, "/node");
    manager.activateBatch(connection, batch);
    manager.publish(.{ .type = .node_deleted, .zxid = 9, .path = "/node" });
    try testing.expectEqual(@as(usize, 1), recorder.count);
}

test "immediate restoration event waits for response batch activation" {
    const testing = std.testing;
    const Recorder = struct {
        event_type: ?EventType = null,
        zxid: i64 = 0,
        path_matches: bool = false,

        fn notify(context: *anyopaque, event: Event) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.event_type = event.type;
            self.zxid = event.zxid;
            self.path_matches = std.mem.eql(u8, "/restored", event.path);
            return true;
        }
    };

    var recorder = Recorder{};
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const connection = try manager.addConnection(.{
        .context = &recorder,
        .notifyFn = Recorder.notify,
    });
    const batch = try manager.beginBatch(connection);
    try manager.registerImmediate(
        connection,
        batch,
        .data,
        "/restored",
        .node_data_changed,
        -1,
    );
    try testing.expect(recorder.event_type == null);
    manager.activateBatch(connection, batch);
    try testing.expectEqual(EventType.node_data_changed, recorder.event_type.?);
    try testing.expectEqual(@as(i64, -1), recorder.zxid);
    try testing.expect(recorder.path_matches);
}

test "connection removal discards watches" {
    const testing = std.testing;
    const Recorder = struct {
        count: usize = 0,

        fn notify(context: *anyopaque, _: Event) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.count += 1;
            return true;
        }
    };

    var recorder = Recorder{};
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const connection = try manager.addConnection(.{
        .context = &recorder,
        .notifyFn = Recorder.notify,
    });
    const batch = try manager.beginBatch(connection);
    try manager.register(connection, batch, .data, "/removed");
    manager.activateBatch(connection, batch);
    manager.removeConnection(connection);
    manager.publish(.{ .type = .node_data_changed, .zxid = 10, .path = "/removed" });
    try testing.expectEqual(@as(usize, 0), recorder.count);
    try testing.expectError(error.ConnectionClosed, manager.beginBatch(connection));
}

test "failed notification deactivates a connection" {
    const testing = std.testing;
    const Recorder = struct {
        count: usize = 0,

        fn notify(context: *anyopaque, _: Event) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.count += 1;
            return false;
        }
    };

    var recorder = Recorder{};
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const connection = try manager.addConnection(.{
        .context = &recorder,
        .notifyFn = Recorder.notify,
    });
    const batch = try manager.beginBatch(connection);
    try manager.register(connection, batch, .data, "/failed");
    manager.activateBatch(connection, batch);
    manager.publish(.{ .type = .node_data_changed, .zxid = 11, .path = "/failed" });
    try testing.expectEqual(@as(usize, 1), recorder.count);
    try testing.expectError(error.ConnectionClosed, manager.beginBatch(connection));
    manager.publish(.{ .type = .node_data_changed, .zxid = 12, .path = "/failed" });
    try testing.expectEqual(@as(usize, 1), recorder.count);
}

test "immediate restoration replaces an armed duplicate watch" {
    const testing = std.testing;
    const Recorder = struct {
        count: usize = 0,
        event_type: ?EventType = null,

        fn notify(context: *anyopaque, event: Event) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.count += 1;
            self.event_type = event.type;
            return true;
        }
    };

    var recorder = Recorder{};
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const connection = try manager.addConnection(.{
        .context = &recorder,
        .notifyFn = Recorder.notify,
    });
    const original_batch = try manager.beginBatch(connection);
    try manager.register(connection, original_batch, .data, "/duplicate");
    manager.activateBatch(connection, original_batch);

    const restore_batch = try manager.beginBatch(connection);
    try manager.registerImmediate(
        connection,
        restore_batch,
        .data,
        "/duplicate",
        .node_data_changed,
        -1,
    );
    try testing.expectEqual(@as(usize, 0), recorder.count);
    manager.activateBatch(connection, restore_batch);
    try testing.expectEqual(@as(usize, 1), recorder.count);
    try testing.expectEqual(EventType.node_data_changed, recorder.event_type.?);
}

test "session invalidation fences only the selected generation" {
    const testing = std.testing;
    const Recorder = struct {
        failed: bool = false,

        fn notify(_: *anyopaque, _: Event) bool {
            return true;
        }

        fn fail(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.failed = true;
        }
    };

    var old = Recorder{};
    var current = Recorder{};
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const old_connection = try manager.addSessionConnection(42, 1, .{
        .context = &old,
        .notifyFn = Recorder.notify,
        .failFn = Recorder.fail,
    });
    const current_connection = try manager.addSessionConnection(42, 2, .{
        .context = &current,
        .notifyFn = Recorder.notify,
        .failFn = Recorder.fail,
    });

    manager.invalidateSession(42, 1);
    try testing.expect(old.failed);
    try testing.expect(!current.failed);
    try testing.expectError(error.ConnectionClosed, manager.beginBatch(old_connection));
    _ = try manager.beginBatch(current_connection);
}
