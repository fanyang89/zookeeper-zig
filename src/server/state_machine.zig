const std = @import("std");
const raft = @import("raftz");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");
const command = @import("command.zig");
const data_tree = @import("data_tree.zig");

pub const max_snapshot_bytes: usize = 256 * 1024 * 1024;

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

pub const ZooKeeperStateMachine = struct {
    allocator: std.mem.Allocator,
    tree: data_tree.DataTree,
    mutex: std.atomic.Mutex = .unlocked,
    applied_index: u64 = 0,
    applied_term: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) !ZooKeeperStateMachine {
        return .{
            .allocator = allocator,
            .tree = try data_tree.DataTree.init(allocator),
        };
    }

    pub fn deinit(self: *ZooKeeperStateMachine) void {
        self.tree.deinit();
        self.* = undefined;
    }

    pub fn stateMachine(self: *ZooKeeperStateMachine) raft.StateMachine {
        return .{ .ctx = self, .vtable = &vtable };
    }

    pub fn exists(self: *ZooKeeperStateMachine, path: []const u8) ?protocol.data.Stat {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.tree.stat(path);
    }

    pub fn getData(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !?DataResult {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const result = (try self.tree.copyData(allocator, path)) orelse return null;
        return .{ .data = result.data, .stat = result.stat };
    }

    pub fn getChildren(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !?ChildrenResult {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const result = (try self.tree.children(allocator, path)) orelse return null;
        return .{ .names = result.names, .stat = result.stat };
    }

    pub fn appliedIndex(self: *ZooKeeperStateMachine) u64 {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.applied_index;
    }

    fn applyImpl(ctx: *anyopaque, entry: raft.Entry) raft.Error!raft.ApplyResult {
        const self: *ZooKeeperStateMachine = @ptrCast(@alignCast(ctx));
        const mutation = command.decode(entry.data) catch return error.Fatal;
        const zxid: i64 = std.math.cast(i64, entry.index) orelse return error.Fatal;

        const success_response_size: usize = switch (mutation) {
            .create => |value| 16 + value.path.len,
            .delete => 12,
            .set_data => 80,
        };
        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        writer.ensureTotalCapacityPrecise(success_response_size) catch return error.OutOfMemory;

        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const result = switch (mutation) {
            .create => |value| self.tree.create(value.path, value.data, zxid, value.time_ms) catch
                return error.OutOfMemory,
            .delete => |value| self.tree.delete(value.path, value.expected_version, zxid),
            .set_data => |value| self.tree.setData(
                value.path,
                value.data,
                value.expected_version,
                zxid,
                value.time_ms,
            ) catch return error.OutOfMemory,
        };

        writer.writeInt(@intFromEnum(result.code)) catch unreachable;
        writer.writeLong(zxid) catch unreachable;
        if (result.code == .ok) switch (mutation) {
            .create => |value| jute.serialize(&writer, protocol.proto.CreateResponse{
                .path = value.path,
            }) catch unreachable,
            .delete => {},
            .set_data => jute.serialize(&writer, protocol.proto.SetDataResponse{
                .stat = result.stat.?,
            }) catch unreachable,
        };
        const response = if (result.code == .ok)
            writer.toOwnedSliceAssert()
        else
            self.allocator.dupe(u8, writer.bytes()) catch return error.OutOfMemory;
        self.applied_index = entry.index;
        self.applied_term = entry.term;
        return .{ .response = response };
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
        var writer = jute.Writer.init(allocator);
        defer writer.deinit();
        self.tree.writeSnapshot(&writer) catch |err| return mapSnapshotError(err);
        const data = allocator.dupe(u8, writer.bytes()) catch return error.OutOfMemory;
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
        var reader = jute.Reader.initWithLimits(bytes.items, .{
            .max_buffer_size = max_snapshot_bytes,
            .extra_max_buffer_size = 0,
            .max_collection_elements = 10_000_000,
        });
        var replacement = data_tree.DataTree.readSnapshot(self.allocator, &reader) catch |err|
            return mapSnapshotError(err);
        errdefer replacement.deinit();
        if (reader.remaining() != 0) return error.Fatal;

        spinLock(&self.mutex);
        var old = self.tree;
        self.tree = replacement;
        self.applied_index = metadata.index;
        self.applied_term = metadata.term;
        self.mutex.unlock();
        old.deinit();
    }

    const vtable: raft.StateMachine.VTable = .{
        .apply = applyImpl,
        .take_snapshot = takeSnapshotImpl,
        .restore_snapshot = restoreSnapshotImpl,
    };
};

fn spinLock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn mapSnapshotError(err: anyerror) raft.Error {
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

test "state machine applies commands and restores snapshots" {
    const testing = std.testing;
    var machine = try ZooKeeperStateMachine.init(testing.allocator);
    defer machine.deinit();

    const created = try applyMutation(&machine, testing.allocator, 1, .{ .create = .{
        .path = "/app",
        .data = "one",
        .time_ms = 100,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, created.code);
    try testing.expectEqual(@as(i64, 1), created.zxid);
    const changed = try applyMutation(&machine, testing.allocator, 2, .{ .set_data = .{
        .path = "/app",
        .data = "two",
        .expected_version = 0,
        .time_ms = 200,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, changed.code);

    const conf_state = raft.ConfState{};
    var snapshot = try machine.stateMachine().takeSnapshot(testing.allocator, 2, 1, conf_state);
    defer snapshot.deinit(testing.allocator);

    var restored = try ZooKeeperStateMachine.init(testing.allocator);
    defer restored.deinit();
    var snapshot_source = SnapshotSource{ .data = snapshot.data };
    try restored.stateMachine().restoreSnapshot(snapshot.metadata, snapshot_source.snapshotReader());
    var data = (try restored.getData(testing.allocator, "/app")).?;
    defer data.deinit(testing.allocator);
    try testing.expectEqualStrings("two", data.data);
    try testing.expectEqual(@as(i32, 1), data.stat.version);
    try testing.expectEqual(@as(u64, 2), restored.appliedIndex());
}
