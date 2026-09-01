const std = @import("std");
const acl = @import("acl.zig");

pub const ConnectionId = u64;
pub const BatchId = u64;

pub const RegistrationContext = struct {
    connection_id: ConnectionId,
    batch_id: BatchId,
};

pub const Kind = enum {
    data,
    children,
    persistent,
    persistent_recursive,
};

pub const WatcherType = enum(i32) {
    children = 1,
    data = 2,
    any = 3,
    persistent = 4,
    persistent_recursive = 5,

    pub fn fromInt(value: i32) ?WatcherType {
        return std.enums.fromInt(WatcherType, value);
    }
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
    acl: ?[]const u8 = null,
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
};

const PendingEvent = struct {
    batch_id: BatchId,
    type: EventType,
    zxid: i64,
    path: []u8,
};

const Connection = struct {
    sink: Sink,
    session_id: i64,
    generation: u64,
    active: bool = true,
    pending_capacity: usize,
    identities: ?[]u8 = null,
    watches: std.ArrayListUnmanaged(Registration) = .empty,
    pending_events: std.ArrayListUnmanaged(PendingEvent) = .empty,
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
        return self.addSessionConnectionWithCapacity(session_id, generation, sink, 256);
    }

    pub fn addSessionConnectionWithCapacity(
        self: *Manager,
        session_id: i64,
        generation: u64,
        sink: Sink,
        pending_capacity: usize,
    ) !ConnectionId {
        if (pending_capacity == 0) return error.InvalidPendingCapacity;
        lock(&self.mutex);
        defer self.mutex.unlock();
        const id = self.next_connection_id;
        self.next_connection_id = std.math.add(ConnectionId, id, 1) catch
            return error.ConnectionIdOverflow;
        try self.connections.putNoClobber(self.allocator, id, .{
            .sink = sink,
            .session_id = session_id,
            .generation = generation,
            .pending_capacity = pending_capacity,
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

    pub fn setIdentities(
        self: *Manager,
        connection_id: ConnectionId,
        identities: []const u8,
    ) !void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const connection = self.connections.getPtr(connection_id) orelse
            return error.ConnectionClosed;
        if (!connection.active) return error.ConnectionClosed;
        const owned = try self.allocator.dupe(u8, identities);
        if (connection.identities) |previous| self.allocator.free(previous);
        connection.identities = owned;
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
        lock(&self.mutex);
        defer self.mutex.unlock();
        const connection = self.connections.getPtr(connection_id) orelse
            return error.ConnectionClosed;
        if (!connection.active) return error.ConnectionClosed;
        for (connection.watches.items) |*registration| {
            if (registration.kind != kind or !std.mem.eql(u8, registration.path, path)) continue;
            registration.batch_id = batch_id;
            registration.armed = false;
            return;
        }
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        try connection.watches.append(self.allocator, .{
            .kind = kind,
            .path = owned_path,
            .batch_id = batch_id,
        });
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
        lock(&self.mutex);
        defer self.mutex.unlock();
        const connection = self.connections.getPtr(connection_id) orelse
            return error.ConnectionClosed;
        if (!connection.active) return error.ConnectionClosed;
        removeExactStandard(self.allocator, &connection.watches, kind, path);
        try queuePendingEvent(self.allocator, connection, batch_id, .{
            .type = event_type,
            .zxid = zxid,
            .path = path,
        });
    }

    pub fn contains(
        self: *Manager,
        connection_id: ConnectionId,
        path: []const u8,
        watcher_type: WatcherType,
    ) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const connection = self.connections.getPtr(connection_id) orelse return false;
        if (!connection.active) return false;
        for (connection.watches.items) |registration| {
            if (std.mem.eql(u8, registration.path, path) and
                typeMatches(registration.kind, watcher_type)) return true;
        }
        return false;
    }

    pub fn remove(
        self: *Manager,
        connection_id: ConnectionId,
        path: []const u8,
        watcher_type: WatcherType,
    ) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const connection = self.connections.getPtr(connection_id) orelse return false;
        if (!connection.active) return false;
        var removed = false;
        var index = connection.watches.items.len;
        while (index > 0) {
            index -= 1;
            const registration = connection.watches.items[index];
            if (std.mem.eql(u8, registration.path, path) and
                typeMatches(registration.kind, watcher_type))
            {
                removeRegistration(self.allocator, &connection.watches, index);
                removed = true;
            }
        }
        return removed;
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
        var event_index: usize = 0;
        while (event_index < connection.pending_events.items.len) {
            const pending = connection.pending_events.items[event_index];
            if (pending.batch_id != batch_id) {
                event_index += 1;
                continue;
            }
            const delivered = connection.sink.notify(.{
                .type = pending.type,
                .zxid = pending.zxid,
                .path = pending.path,
            });
            const removed = connection.pending_events.orderedRemove(event_index);
            self.allocator.free(removed.path);
            if (!delivered) {
                deactivateConnection(self.allocator, connection);
                return;
            }
        }
        for (connection.watches.items) |*registration| {
            if (registration.batch_id == batch_id) registration.armed = true;
        }
    }

    pub fn publish(self: *Manager, event: Event) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var connections = self.connections.valueIterator();
        while (connections.next()) |connection| {
            if (!connection.active) continue;
            const authorized = acl.allows(event.acl, acl.read, connection.identities) catch {
                connection.sink.fail();
                deactivateConnection(self.allocator, connection);
                continue;
            };
            if (!authorized) continue;
            var matched = false;
            var armed = false;
            var pending_batch: ?BatchId = null;
            for (connection.watches.items) |registration| {
                if (!registrationMatches(registration, event)) continue;
                matched = true;
                armed = armed or registration.armed;
                if (!registration.armed and pending_batch == null) {
                    pending_batch = registration.batch_id;
                }
            }
            if (!matched) continue;

            removeMatchingStandard(self.allocator, &connection.watches, event);
            const barrier_batch = if (connection.pending_events.items.len != 0)
                connection.pending_events.items[0].batch_id
            else
                pending_batch;
            if (barrier_batch == null) {
                if (!armed) continue;
                if (!connection.sink.notify(event)) deactivateConnection(self.allocator, connection);
                continue;
            }
            queuePendingEvent(
                self.allocator,
                connection,
                barrier_batch.?,
                event,
            ) catch {
                connection.sink.fail();
                deactivateConnection(self.allocator, connection);
            };
        }
    }
};

fn standardMatches(kind: Kind, event_type: EventType) bool {
    return switch (event_type) {
        .node_created, .node_data_changed => kind == .data,
        .node_deleted => kind == .data or kind == .children,
        .node_children_changed => kind == .children,
    };
}

fn registrationMatches(registration: Registration, event: Event) bool {
    return switch (registration.kind) {
        .data, .children => std.mem.eql(u8, registration.path, event.path) and
            standardMatches(registration.kind, event.type),
        .persistent => std.mem.eql(u8, registration.path, event.path),
        .persistent_recursive => event.type != .node_children_changed and
            pathContains(registration.path, event.path),
    };
}

fn pathContains(base: []const u8, path: []const u8) bool {
    if (std.mem.eql(u8, base, "/")) return path.len != 0 and path[0] == '/';
    return std.mem.eql(u8, base, path) or
        (path.len > base.len and path[base.len] == '/' and std.mem.startsWith(u8, path, base));
}

fn typeMatches(kind: Kind, watcher_type: WatcherType) bool {
    return switch (watcher_type) {
        .children => kind == .children,
        .data => kind == .data,
        .any => true,
        .persistent => kind == .persistent,
        .persistent_recursive => kind == .persistent_recursive,
    };
}

fn removeExactStandard(
    allocator: std.mem.Allocator,
    watches: *std.ArrayListUnmanaged(Registration),
    kind: Kind,
    path: []const u8,
) void {
    var index = watches.items.len;
    while (index > 0) {
        index -= 1;
        const registration = watches.items[index];
        if (registration.kind == kind and std.mem.eql(u8, registration.path, path)) {
            removeRegistration(allocator, watches, index);
        }
    }
}

fn removeMatchingStandard(
    allocator: std.mem.Allocator,
    watches: *std.ArrayListUnmanaged(Registration),
    event: Event,
) void {
    var index = watches.items.len;
    while (index > 0) {
        index -= 1;
        const registration = watches.items[index];
        if ((registration.kind == .data or registration.kind == .children) and
            registrationMatches(registration, event))
        {
            removeRegistration(allocator, watches, index);
        }
    }
}

fn queuePendingEvent(
    allocator: std.mem.Allocator,
    connection: *Connection,
    batch_id: BatchId,
    event: Event,
) !void {
    if (connection.pending_events.items.len >= connection.pending_capacity) {
        return error.PendingQueueFull;
    }
    const owned_path = try allocator.dupe(u8, event.path);
    errdefer allocator.free(owned_path);
    try connection.pending_events.append(allocator, .{
        .batch_id = batch_id,
        .type = event.type,
        .zxid = event.zxid,
        .path = owned_path,
    });
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
    for (connection.pending_events.items) |event| allocator.free(event.path);
    connection.pending_events.clearRetainingCapacity();
}

fn deinitConnection(allocator: std.mem.Allocator, connection: *Connection) void {
    for (connection.watches.items) |registration| allocator.free(registration.path);
    connection.watches.deinit(allocator);
    for (connection.pending_events.items) |event| allocator.free(event.path);
    connection.pending_events.deinit(allocator);
    if (connection.identities) |identities| allocator.free(identities);
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

test "persistent watch survives events and supports exact removal" {
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
    try manager.register(connection, batch, .persistent, "/node");
    manager.activateBatch(connection, batch);

    manager.publish(.{ .type = .node_created, .zxid = 1, .path = "/node" });
    manager.publish(.{ .type = .node_data_changed, .zxid = 2, .path = "/node" });
    manager.publish(.{ .type = .node_children_changed, .zxid = 3, .path = "/node" });
    manager.publish(.{ .type = .node_deleted, .zxid = 4, .path = "/node" });
    try testing.expectEqual(@as(usize, 4), recorder.count);
    try testing.expect(manager.contains(connection, "/node", .persistent));
    try testing.expect(manager.remove(connection, "/node", .persistent));
    try testing.expect(!manager.contains(connection, "/node", .persistent));
    manager.publish(.{ .type = .node_created, .zxid = 5, .path = "/node" });
    try testing.expectEqual(@as(usize, 4), recorder.count);
}

test "recursive watch matches descendants without child events" {
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
    try manager.register(connection, batch, .persistent_recursive, "/a");
    manager.activateBatch(connection, batch);

    manager.publish(.{ .type = .node_created, .zxid = 1, .path = "/a" });
    manager.publish(.{ .type = .node_data_changed, .zxid = 2, .path = "/a/b" });
    manager.publish(.{ .type = .node_data_changed, .zxid = 3, .path = "/ab" });
    manager.publish(.{ .type = .node_children_changed, .zxid = 4, .path = "/a" });
    try testing.expectEqual(@as(usize, 2), recorder.count);
}

test "overlapping watch modes deduplicate and persistent batches queue every event" {
    const testing = std.testing;
    const Recorder = struct {
        count: usize = 0,
        failed: bool = false,

        fn notify(context: *anyopaque, _: Event) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.count += 1;
            return true;
        }

        fn fail(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.failed = true;
        }
    };

    var recorder = Recorder{};
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const connection = try manager.addSessionConnectionWithCapacity(1, 1, .{
        .context = &recorder,
        .notifyFn = Recorder.notify,
        .failFn = Recorder.fail,
    }, 4);
    const batch = try manager.beginBatch(connection);
    try manager.register(connection, batch, .persistent, "/a");
    try manager.register(connection, batch, .persistent_recursive, "/");
    try manager.register(connection, batch, .data, "/a");
    manager.publish(.{ .type = .node_created, .zxid = 1, .path = "/a" });
    manager.publish(.{ .type = .node_data_changed, .zxid = 2, .path = "/a" });
    try testing.expectEqual(@as(usize, 0), recorder.count);
    manager.activateBatch(connection, batch);
    try testing.expectEqual(@as(usize, 2), recorder.count);
    try testing.expect(!manager.contains(connection, "/a", .data));
    try testing.expect(manager.contains(connection, "/a", .persistent));
    try testing.expect(manager.contains(connection, "/", .persistent_recursive));
    manager.publish(.{ .type = .node_deleted, .zxid = 3, .path = "/a" });
    try testing.expectEqual(@as(usize, 3), recorder.count);
    try testing.expect(!recorder.failed);
}

test "persistent watch filters events using current connection identities" {
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
    try manager.register(connection, batch, .persistent, "/secure");
    manager.activateBatch(connection, batch);

    const denied_acl = try acl.encode(testing.allocator, &.{.{
        .perms = acl.write,
        .scheme = "world",
        .id = "anyone",
    }});
    defer testing.allocator.free(denied_acl);
    manager.publish(.{
        .type = .node_data_changed,
        .zxid = 1,
        .path = "/secure",
        .acl = denied_acl,
    });
    try testing.expectEqual(@as(usize, 0), recorder.count);

    const digest_acl = try acl.encode(testing.allocator, &.{.{
        .perms = acl.read,
        .scheme = "digest",
        .id = "user:digest",
    }});
    defer testing.allocator.free(digest_acl);
    const identities = try acl.encodeIdentities(testing.allocator, &.{.{
        .scheme = "digest",
        .id = "user:digest",
    }});
    defer testing.allocator.free(identities);
    try manager.setIdentities(connection, identities);
    manager.publish(.{
        .type = .node_data_changed,
        .zxid = 2,
        .path = "/secure",
        .acl = digest_acl,
    });
    try testing.expectEqual(@as(usize, 1), recorder.count);
}

test "armed persistent watch respects a matching pending response barrier" {
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
    const persistent_batch = try manager.beginBatch(connection);
    try manager.register(connection, persistent_batch, .persistent, "/node");
    manager.activateBatch(connection, persistent_batch);
    const data_batch = try manager.beginBatch(connection);
    try manager.register(connection, data_batch, .data, "/node");

    manager.publish(.{ .type = .node_data_changed, .zxid = 1, .path = "/node" });
    try testing.expectEqual(@as(usize, 0), recorder.count);
    manager.activateBatch(connection, data_batch);
    try testing.expectEqual(@as(usize, 1), recorder.count);
    try testing.expect(manager.contains(connection, "/node", .persistent));
    try testing.expect(!manager.contains(connection, "/node", .data));
}

test "persistent pending event overflow fails and deactivates the connection" {
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

    var recorder = Recorder{};
    var manager = Manager.init(testing.allocator);
    defer manager.deinit();
    const connection = try manager.addSessionConnectionWithCapacity(1, 1, .{
        .context = &recorder,
        .notifyFn = Recorder.notify,
        .failFn = Recorder.fail,
    }, 1);
    const batch = try manager.beginBatch(connection);
    try manager.register(connection, batch, .persistent, "/node");
    manager.publish(.{ .type = .node_created, .zxid = 1, .path = "/node" });
    manager.publish(.{ .type = .node_data_changed, .zxid = 2, .path = "/node" });
    try testing.expect(recorder.failed);
    try testing.expect(!manager.contains(connection, "/node", .persistent));
}

test "watcher types check and remove only their matching mode" {
    const testing = std.testing;
    const Recorder = struct {
        fn notify(_: *anyopaque, _: Event) bool {
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
    inline for (.{ Kind.data, Kind.children, Kind.persistent, Kind.persistent_recursive }) |kind| {
        try manager.register(connection, batch, kind, "/node");
    }
    manager.activateBatch(connection, batch);

    try testing.expect(manager.contains(connection, "/node", .data));
    try testing.expect(manager.contains(connection, "/node", .children));
    try testing.expect(manager.contains(connection, "/node", .persistent));
    try testing.expect(manager.contains(connection, "/node", .persistent_recursive));
    try testing.expect(manager.contains(connection, "/node", .any));
    try testing.expect(manager.remove(connection, "/node", .persistent));
    try testing.expect(!manager.contains(connection, "/node", .persistent));
    try testing.expect(manager.contains(connection, "/node", .data));
    try testing.expect(manager.remove(connection, "/node", .any));
    try testing.expect(!manager.contains(connection, "/node", .any));
    try testing.expect(!manager.remove(connection, "/node", .any));
}

test "pending response barrier preserves order for armed recursive events" {
    const testing = std.testing;
    const Recorder = struct {
        zxids: [4]i64 = undefined,
        count: usize = 0,

        fn notify(context: *anyopaque, event: Event) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.zxids[self.count] = event.zxid;
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
    const recursive_batch = try manager.beginBatch(connection);
    try manager.register(connection, recursive_batch, .persistent_recursive, "/a");
    manager.activateBatch(connection, recursive_batch);
    const data_batch = try manager.beginBatch(connection);
    try manager.register(connection, data_batch, .data, "/a");

    manager.publish(.{ .type = .node_data_changed, .zxid = 1, .path = "/a" });
    manager.publish(.{ .type = .node_created, .zxid = 2, .path = "/a/child" });
    try testing.expectEqual(@as(usize, 0), recorder.count);
    manager.activateBatch(connection, data_batch);
    try testing.expectEqualSlices(i64, &.{ 1, 2 }, recorder.zxids[0..recorder.count]);
}

test "duplicate persistent registration installs a new response barrier" {
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
    const first_batch = try manager.beginBatch(connection);
    try manager.register(connection, first_batch, .persistent, "/node");
    manager.activateBatch(connection, first_batch);
    const duplicate_batch = try manager.beginBatch(connection);
    try manager.register(connection, duplicate_batch, .persistent, "/node");

    manager.publish(.{ .type = .node_data_changed, .zxid = 1, .path = "/node" });
    try testing.expectEqual(@as(usize, 0), recorder.count);
    manager.activateBatch(connection, duplicate_batch);
    try testing.expectEqual(@as(usize, 1), recorder.count);
}
