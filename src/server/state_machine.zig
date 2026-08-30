const std = @import("std");
const raft = @import("raftz");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");
const acl = @import("acl.zig");
const command = @import("command.zig");
const data_tree = @import("data_tree.zig");
const rocks_store = @import("rocks_store.zig");

pub const max_snapshot_bytes = rocks_store.max_snapshot_bytes;

pub const DataResult = struct {
    data: []u8,
    stat: protocol.data.Stat,

    pub fn deinit(self: *DataResult, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
        self.* = undefined;
    }
};

pub const ChildrenResult = struct {
    names: [][]u8,
    stat: protocol.data.Stat,

    pub fn deinit(self: *ChildrenResult, allocator: std.mem.Allocator) void {
        for (self.names) |name| allocator.free(name);
        allocator.free(self.names);
        self.* = undefined;
    }
};

pub const AclResult = struct {
    blob: ?[]u8,
    stat: protocol.data.Stat,
    redact_digest: bool,

    pub fn deinit(self: *AclResult, allocator: std.mem.Allocator) void {
        if (self.blob) |value| allocator.free(value);
        self.* = undefined;
    }
};

pub const SessionReadError = error{ SessionExpired, SessionMoved, NoAuth };

pub const ZooKeeperStateMachine = struct {
    allocator: std.mem.Allocator,
    store: rocks_store.RocksStore,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !ZooKeeperStateMachine {
        return .{
            .allocator = allocator,
            .store = try rocks_store.RocksStore.open(allocator, path),
        };
    }

    pub fn deinit(self: *ZooKeeperStateMachine) void {
        self.store.deinit();
        self.* = undefined;
    }

    pub fn stateMachine(self: *ZooKeeperStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }

    pub fn exists(self: *ZooKeeperStateMachine, path: []const u8) !?protocol.data.Stat {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.store.exists(path);
    }

    pub fn getData(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !?DataResult {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const result = (try self.store.getData(allocator, path)) orelse return null;
        return .{ .data = result.data, .stat = result.stat };
    }

    pub fn getChildren(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !?ChildrenResult {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const stat = (try self.store.exists(path)) orelse return null;
        const names = (try self.store.getChildren(allocator, path)).?;
        return .{ .names = names, .stat = stat };
    }

    pub fn existsForSession(
        self: *ZooKeeperStateMachine,
        session_id: i64,
        generation: u64,
        path: []const u8,
    ) !?protocol.data.Stat {
        return self.existsAuthorizedForSession(session_id, generation, path, null);
    }

    pub fn existsAuthorizedForSession(
        self: *ZooKeeperStateMachine,
        session_id: i64,
        generation: u64,
        path: []const u8,
        identities: ?[]const u8,
    ) !?protocol.data.Stat {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.validateSessionLocked(session_id, generation);
        switch (try self.store.authorize(path, acl.read, identities)) {
            .ok => return self.store.exists(path),
            .no_node => return null,
            .no_auth => return error.NoAuth,
            else => return error.NoAuth,
        }
    }

    pub fn getDataForSession(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        session_id: i64,
        generation: u64,
        path: []const u8,
    ) !?DataResult {
        return self.getDataAuthorizedForSession(allocator, session_id, generation, path, null);
    }

    pub fn getDataAuthorizedForSession(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        session_id: i64,
        generation: u64,
        path: []const u8,
        identities: ?[]const u8,
    ) !?DataResult {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.validateSessionLocked(session_id, generation);
        switch (try self.store.authorize(path, acl.read, identities)) {
            .ok => {},
            .no_node => return null,
            .no_auth => return error.NoAuth,
            else => return error.NoAuth,
        }
        const result = (try self.store.getData(allocator, path)) orelse return null;
        return .{ .data = result.data, .stat = result.stat };
    }

    pub fn getChildrenForSession(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        session_id: i64,
        generation: u64,
        path: []const u8,
    ) !?ChildrenResult {
        return self.getChildrenAuthorizedForSession(allocator, session_id, generation, path, null);
    }

    pub fn getChildrenAuthorizedForSession(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        session_id: i64,
        generation: u64,
        path: []const u8,
        identities: ?[]const u8,
    ) !?ChildrenResult {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.validateSessionLocked(session_id, generation);
        switch (try self.store.authorize(path, acl.read, identities)) {
            .ok => {},
            .no_node => return null,
            .no_auth => return error.NoAuth,
            else => return error.NoAuth,
        }
        const stat = (try self.store.exists(path)) orelse return null;
        const names = (try self.store.getChildren(allocator, path)).?;
        return .{ .names = names, .stat = stat };
    }

    pub fn getAclForSession(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        session_id: i64,
        generation: u64,
        path: []const u8,
        identities: ?[]const u8,
    ) !?AclResult {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.validateSessionLocked(session_id, generation);
        switch (try self.store.authorize(path, acl.read | acl.admin, identities)) {
            .ok => {},
            .no_node => return null,
            .no_auth => return error.NoAuth,
            else => return error.NoAuth,
        }
        const redact_digest = switch (try self.store.authorize(path, acl.admin, identities)) {
            .ok => false,
            .no_auth => true,
            else => return error.NoAuth,
        };
        const result = (try self.store.getAcl(allocator, path)) orelse return null;
        return .{
            .blob = result.blob,
            .stat = result.stat,
            .redact_digest = redact_digest,
        };
    }

    pub fn validateSession(
        self: *ZooKeeperStateMachine,
        session_id: i64,
        generation: u64,
    ) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.validateSessionLocked(session_id, generation);
    }

    pub fn getSession(
        self: *ZooKeeperStateMachine,
        session_id: i64,
    ) !?rocks_store.Session {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.store.getSession(session_id);
    }

    pub fn expiredSessions(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
    ) ![]rocks_store.ExpiredSession {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.store.expiredSessions(allocator);
    }

    fn validateSessionLocked(
        self: *ZooKeeperStateMachine,
        session_id: i64,
        generation: u64,
    ) !void {
        return switch (try self.store.validateSession(session_id, generation)) {
            .ok => {},
            .session_expired => error.SessionExpired,
            .session_moved => error.SessionMoved,
            else => error.SessionMoved,
        };
    }

    pub fn appliedIndex(self: *ZooKeeperStateMachine) u64 {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.store.durableApplied().index;
    }

    fn applyImpl(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self: *ZooKeeperStateMachine = @ptrCast(@alignCast(ctx));
        if (entry.data.len == 0) {
            spinLock(&self.mutex);
            defer self.mutex.unlock();
            self.store.advanceApplied(entry.index, entry.term) catch |err| return mapStoreError(err);
            return .{};
        }
        const mutation = command.decode(entry.data) catch return error.Fatal;
        const zxid: i64 = std.math.cast(i64, entry.index) orelse return error.Fatal;

        const success_response_size = command.resultCapacity(mutation) catch return error.Fatal;
        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        writer.ensureTotalCapacityPrecise(success_response_size) catch return error.OutOfMemory;

        spinLock(&self.mutex);
        defer self.mutex.unlock();
        var result = self.store.apply(mutation, entry.index, entry.term) catch |err|
            return mapStoreError(err);
        defer result.deinit(self.allocator);

        writer.writeInt(@intFromEnum(result.code)) catch unreachable;
        writer.writeLong(zxid) catch unreachable;
        if (result.code == .ok) switch (mutation) {
            .create => |value| jute.serialize(&writer, protocol.proto.Create2Response{
                .path = result.created_path orelse value.path,
                .stat = result.stat.?,
            }) catch unreachable,
            .delete, .open_session, .touch_session, .close_session, .expire_session, .move_session, .session_tick => {},
            .set_acl => jute.serialize(&writer, protocol.proto.SetACLResponse{
                .stat = result.stat.?,
            }) catch unreachable,
            .set_data => jute.serialize(&writer, protocol.proto.SetDataResponse{
                .stat = result.stat.?,
            }) catch unreachable,
        };
        return .{ .response = writer.toOwnedSliceAssert() };
    }

    fn takeSnapshotImpl(
        ctx: *anyopaque,
        allocator: std.mem.Allocator,
        applied_index: u64,
        applied_term: u64,
        conf_state: raft.ConfState,
    ) raft.Error!raft.Snapshot {
        const self: *ZooKeeperStateMachine = @ptrCast(@alignCast(ctx));
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const durable = self.store.durableApplied();
        if (durable.index != applied_index or durable.term != applied_term) return error.Fatal;
        const data = self.store.snapshot(allocator) catch |err| return mapStoreError(err);
        errdefer allocator.free(data);
        const owned_conf_state = raft.cloneConfState(allocator, conf_state) catch return error.OutOfMemory;
        return .{
            .data = data,
            .metadata = .{
                .index = applied_index,
                .term = applied_term,
                .conf_state = owned_conf_state,
            },
        };
    }

    fn restoreSnapshotImpl(
        ctx: *anyopaque,
        metadata: raft.SnapshotMetadata,
        snapshot_reader: raft.SnapshotReader,
    ) raft.Error!void {
        const self: *ZooKeeperStateMachine = @ptrCast(@alignCast(ctx));
        var bytes: std.ArrayList(u8) = .empty;
        defer bytes.deinit(self.allocator);
        var buffer: [16 * 1024]u8 = undefined;
        while (true) {
            const count = try snapshot_reader.read(&buffer);
            if (count == 0) break;
            if (bytes.items.len > max_snapshot_bytes -| count) return error.Fatal;
            bytes.appendSlice(self.allocator, buffer[0..count]) catch return error.OutOfMemory;
        }
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        self.store.restore(bytes.items, metadata.index, metadata.term) catch |err|
            return mapStoreError(err);
    }

    fn durableAppliedImpl(ctx: *anyopaque) raft.Error!raft.DurableApplied {
        const self: *ZooKeeperStateMachine = @ptrCast(@alignCast(ctx));
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const applied = self.store.durableApplied();
        return .{ .index = applied.index, .term = applied.term };
    }

    const vtable: raft.StateMachine.VTable = .{
        .apply = applyImpl,
        .take_snapshot = takeSnapshotImpl,
        .restore_snapshot = restoreSnapshotImpl,
        .durable_applied = durableAppliedImpl,
    };
};

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn mapStoreError(err: anyerror) raft.Error {
    return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.Fatal,
    };
}

const SnapshotSource = struct {
    data: []const u8,
    offset: usize = 0,

    fn snapshotReader(self: *SnapshotSource) raft.SnapshotReader {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn read(ctx: *anyopaque, output: []u8) raft.Error!usize {
        const self: *SnapshotSource = @ptrCast(@alignCast(ctx));
        const count = @min(output.len, self.data.len - self.offset);
        @memcpy(output[0..count], self.data[self.offset..][0..count]);
        self.offset += count;
        return count;
    }

    const vtable: raft.SnapshotReader.VTable = .{ .read = read };
};

const AppliedMutation = struct {
    code: data_tree.ErrorCode,
    zxid: i64,
};

fn applyMutation(
    machine: *ZooKeeperStateMachine,
    allocator: std.mem.Allocator,
    index: u64,
    mutation: command.Mutation,
) !AppliedMutation {
    const encoded = try command.encode(allocator, mutation);
    defer allocator.free(encoded);
    const entry = raft.Entry{ .index = index, .term = 1, .data = encoded };
    var result = try machine.stateMachine().apply(entry);
    defer result.deinit(allocator);
    const decoded = try command.decodeResult(result.response.?);
    return .{ .code = decoded.code, .zxid = decoded.zxid };
}

test "RocksDB state machine atomically persists commands and restores snapshots" {
    const testing = std.testing;
    var source_dir = std.testing.tmpDir(.{});
    defer source_dir.cleanup();
    const source_path = try source_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(source_path);
    var machine = try ZooKeeperStateMachine.init(testing.allocator, source_path);
    defer machine.deinit();

    var no_op_result = try machine.stateMachine().apply(.{ .index = 1, .term = 1, .data = "" });
    defer no_op_result.deinit(testing.allocator);
    try testing.expectEqual(@as(?[]u8, null), no_op_result.response);
    try testing.expectEqual(@as(u64, 1), machine.appliedIndex());

    const created = try applyMutation(&machine, testing.allocator, 2, .{ .create = .{
        .path = "/app",
        .data = "one",
        .time_ms = 100,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, created.code);
    const changed = try applyMutation(&machine, testing.allocator, 3, .{ .set_data = .{
        .path = "/app",
        .data = "two",
        .expected_version = 0,
        .time_ms = 200,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, changed.code);
    const rejected = try applyMutation(&machine, testing.allocator, 4, .{ .set_data = .{
        .path = "/missing",
        .data = "ignored",
        .expected_version = -1,
        .time_ms = 300,
    } });
    try testing.expectEqual(data_tree.ErrorCode.no_node, rejected.code);
    const password = [_]u8{0x5a} ** 16;
    const opened = try applyMutation(&machine, testing.allocator, 5, .{ .open_session = .{
        .session_id = 99,
        .password = &password,
        .timeout_ms = 3_000,
        .tick_grace_ms = 500,
        .generation = 7,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, opened.code);
    const ticked = try applyMutation(&machine, testing.allocator, 6, .{ .session_tick = .{
        .leader_term = 1,
        .elapsed_ms = 1_000,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, ticked.code);
    const touched = try applyMutation(&machine, testing.allocator, 7, .{ .touch_session = .{
        .session_id = 99,
        .password = &password,
        .generation = 7,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, touched.code);
    try testing.expectEqual(@as(i64, 4_500), (try machine.getSession(99)).?.expires_at_ms);
    const moved = try applyMutation(&machine, testing.allocator, 8, .{ .move_session = .{
        .session_id = 99,
        .password = &password,
        .expected_generation = 7,
        .new_generation = 8,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, moved.code);
    const stale_create = try applyMutation(&machine, testing.allocator, 9, .{ .create = .{
        .path = "/stale",
        .data = "rejected",
        .time_ms = 2_000,
        .session_id = 99,
        .session_generation = 7,
    } });
    try testing.expectEqual(data_tree.ErrorCode.session_moved, stale_create.code);
    try testing.expect((try machine.exists("/stale")) == null);
    try testing.expectEqual(@as(u64, 9), machine.appliedIndex());

    const conf_state = raft.ConfState{};
    var snapshot = try machine.stateMachine().takeSnapshot(testing.allocator, 9, 1, conf_state);
    defer snapshot.deinit(testing.allocator);

    var target_dir = std.testing.tmpDir(.{});
    defer target_dir.cleanup();
    const target_path = try target_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(target_path);
    var restored = try ZooKeeperStateMachine.init(testing.allocator, target_path);
    defer restored.deinit();
    var snapshot_source = SnapshotSource{ .data = snapshot.data };
    try restored.stateMachine().restoreSnapshot(snapshot.metadata, snapshot_source.snapshotReader());
    var data = (try restored.getData(testing.allocator, "/app")).?;
    defer data.deinit(testing.allocator);
    try testing.expectEqualStrings("two", data.data);
    try testing.expectEqual(@as(i32, 1), data.stat.version);
    const restored_session = (try restored.getSession(99)).?;
    try testing.expectEqualSlices(u8, &password, &restored_session.password);
    try testing.expectEqual(@as(i64, 4_500), restored_session.expires_at_ms);
    try testing.expectEqual(@as(u64, 9), restored.appliedIndex());
    try testing.expectEqual(@as(u64, 8), restored_session.generation);
    const closed = try applyMutation(&restored, testing.allocator, 10, .{ .close_session = .{
        .session_id = 99,
        .password = &password,
        .generation = 8,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, closed.code);
    try testing.expectEqual(@as(?rocks_store.Session, null), try restored.getSession(99));
}
