const std = @import("std");
const rocksdb = @import("rocksdb");
const rocksdb_c = @import("rocksdb_c");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");
const command = @import("command.zig");
const data_tree = @import("data_tree.zig");

const Node = data_tree.Node;
const ErrorCode = data_tree.ErrorCode;

const applied_key = "\x00applied";
const snapshot_version: u32 = 1;
pub const max_snapshot_bytes: usize = 256 * 1024 * 1024;

pub const Applied = struct {
    index: u64 = 0,
    term: u64 = 0,
};

pub const DataResult = struct {
    data: []u8,
    stat: protocol.data.Stat,
};

pub const RocksStore = struct {
    allocator: std.mem.Allocator,
    db: rocksdb.DB,
    families: []const rocksdb.ColumnFamily,
    default_family: rocksdb.ColumnFamilyHandle,
    applied: Applied,

    pub fn open(allocator: std.mem.Allocator, path: []const u8) !RocksStore {
        const path_z = try allocator.dupeZ(u8, path);
        defer allocator.free(path_z);
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        var db, const families = try rocksdb.DB.open(
            allocator,
            path_z,
            .{ .create_if_missing = true },
            null,
            false,
            &error_data,
        );
        errdefer {
            db.deinit();
            deinitFamilies(allocator, families);
        }
        const default_family = families[0].handle;
        db = db.withDefaultColumnFamily(default_family);
        var store = RocksStore{
            .allocator = allocator,
            .db = db,
            .families = families,
            .default_family = default_family,
            .applied = .{},
        };
        if (try store.getNode("/")) |root| {
            var owned_root = root;
            owned_root.deinit(allocator);
            store.applied = try store.loadApplied();
        } else {
            var root = Node{
                .data = try allocator.alloc(u8, 0),
                .czxid = 0,
                .mzxid = 0,
                .ctime = 0,
                .mtime = 0,
                .version = 0,
                .cversion = 0,
                .pzxid = 0,
                .child_count = 0,
            };
            defer root.deinit(allocator);
            var batch = rocksdb.WriteBatch.init();
            defer batch.deinit();
            const encoded_root = try encodeNode(allocator, root);
            defer allocator.free(encoded_root);
            batch.put(default_family, "/", encoded_root);
            putApplied(&batch, default_family, .{});
            try store.writeSync(batch);
        }
        return store;
    }

    pub fn deinit(self: *RocksStore) void {
        self.db.deinit();
        deinitFamilies(self.allocator, self.families);
        self.* = undefined;
    }

    pub fn durableApplied(self: *const RocksStore) Applied {
        return self.applied;
    }

    pub fn apply(
        self: *RocksStore,
        operation: command.Mutation,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        const result = switch (operation) {
            .create => |request| try self.create(request.path, request.data, request.time_ms, index, term),
            .delete => |request| try self.delete(request.path, request.expected_version, index, term),
            .set_data => |request| try self.setData(
                request.path,
                request.data,
                request.expected_version,
                request.time_ms,
                index,
                term,
            ),
        };
        if (result.code != .ok) {
            var batch = rocksdb.WriteBatch.init();
            defer batch.deinit();
            putApplied(&batch, self.default_family, .{ .index = index, .term = term });
            try self.writeSync(batch);
        }
        self.applied = .{ .index = index, .term = term };
        return result;
    }

    pub fn advanceApplied(self: *RocksStore, index: u64, term: u64) !void {
        var batch = rocksdb.WriteBatch.init();
        defer batch.deinit();
        putApplied(&batch, self.default_family, .{ .index = index, .term = term });
        try self.writeSync(batch);
        self.applied = .{ .index = index, .term = term };
    }

    pub fn exists(self: *RocksStore, path: []const u8) !?protocol.data.Stat {
        const maybe_node = try self.getNode(path);
        if (maybe_node) |value| {
            var node = value;
            defer node.deinit(self.allocator);
            return node.stat();
        }
        return null;
    }

    pub fn getData(
        self: *RocksStore,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !?DataResult {
        const maybe_node = try self.getNode(path);
        if (maybe_node) |value| {
            var node = value;
            defer node.deinit(self.allocator);
            return .{
                .data = try allocator.dupe(u8, node.data),
                .stat = node.stat(),
            };
        }
        return null;
    }

    pub fn getChildren(
        self: *RocksStore,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !?[][]u8 {
        const maybe_parent = try self.getNode(path);
        if (maybe_parent == null) return null;
        var parent = maybe_parent.?;
        parent.deinit(self.allocator);

        var children: std.ArrayList([]u8) = .empty;
        errdefer {
            for (children.items) |child| allocator.free(child);
            children.deinit(allocator);
        }
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        var iterator = self.db.iterator(self.default_family, .forward, "/");
        defer iterator.deinit();
        while (try iterator.nextKey(&error_data)) |key| {
            if (key.data.len == 0 or key.data[0] != '/') break;
            if (data_tree.directChildName(path, key.data)) |name| {
                const owned_name = try allocator.dupe(u8, name);
                children.append(allocator, owned_name) catch |err| {
                    allocator.free(owned_name);
                    return err;
                };
            }
        }
        std.mem.sort([]u8, children.items, {}, struct {
            fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
                return std.mem.lessThan(u8, lhs, rhs);
            }
        }.lessThan);
        return try children.toOwnedSlice(allocator);
    }

    pub fn snapshot(self: *RocksStore, allocator: std.mem.Allocator) ![]u8 {
        var writer = jute.Writer.init(allocator);
        errdefer writer.deinit();
        try writer.writeInt(@intCast(snapshot_version));
        try writer.writeLong(@bitCast(self.applied.index));
        try writer.writeLong(@bitCast(self.applied.term));
        const count_offset = writer.bytes().len;
        try writer.writeInt(0);
        var count: u32 = 0;
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        var iterator = self.db.iterator(self.default_family, .forward, "/");
        defer iterator.deinit();
        while (try iterator.next(&error_data)) |entry| {
            if (entry[0].data.len == 0 or entry[0].data[0] != '/') break;
            const encoded_size = std.math.add(
                usize,
                8,
                std.math.add(usize, entry[0].data.len, entry[1].data.len) catch
                    return error.SnapshotTooLarge,
            ) catch return error.SnapshotTooLarge;
            if (writer.bytes().len > max_snapshot_bytes -| encoded_size) {
                return error.SnapshotTooLarge;
            }
            try writer.writeBuffer(entry[0].data);
            try writer.writeBuffer(entry[1].data);
            count = std.math.add(u32, count, 1) catch return error.SnapshotTooLarge;
            if (count > std.math.maxInt(i32)) return error.SnapshotTooLarge;
        }
        try writer.patchInt(count_offset, @intCast(count));
        return writer.toOwnedSliceAssert();
    }

    pub fn restore(
        self: *RocksStore,
        bytes: []const u8,
        expected_index: u64,
        expected_term: u64,
    ) !void {
        if (bytes.len > max_snapshot_bytes) return error.SnapshotTooLarge;
        var reader = jute.Reader.init(bytes);
        if (try reader.readInt() != snapshot_version) return error.UnsupportedSnapshotVersion;
        const applied = Applied{
            .index = @bitCast(try reader.readLong()),
            .term = @bitCast(try reader.readLong()),
        };
        if (applied.index != expected_index or applied.term != expected_term) {
            return error.SnapshotMetadataMismatch;
        }
        const signed_count = try reader.readInt();
        if (signed_count < 0) return error.InvalidSnapshot;
        const count: u32 = @intCast(signed_count);
        var batch = rocksdb.WriteBatch.init();
        defer batch.deinit();
        batch.deleteRange(self.default_family, "\x00", "\xff");
        const Validation = struct { declared_children: usize, actual_children: usize = 0 };
        var nodes: std.StringHashMapUnmanaged(Validation) = .empty;
        defer nodes.deinit(self.allocator);
        var index: u32 = 0;
        while (index < count) : (index += 1) {
            const key = (try reader.readBuffer()) orelse return error.InvalidSnapshot;
            const value = (try reader.readBuffer()) orelse return error.InvalidSnapshot;
            if (!data_tree.isValidPath(key)) return error.InvalidSnapshot;
            var node = decodeNode(self.allocator, value) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return error.InvalidSnapshot,
            };
            defer node.deinit(self.allocator);
            const result = try nodes.getOrPut(self.allocator, key);
            if (result.found_existing) return error.InvalidSnapshot;
            result.value_ptr.* = .{ .declared_children = node.child_count };
            batch.put(self.default_family, key, value);
        }
        if (reader.remaining() != 0 or !nodes.contains("/")) return error.InvalidSnapshot;
        var paths = nodes.iterator();
        while (paths.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, "/")) continue;
            const parent_path = data_tree.parentPath(entry.key_ptr.*) orelse return error.InvalidSnapshot;
            const parent = nodes.getPtr(parent_path) orelse return error.InvalidSnapshot;
            parent.actual_children = std.math.add(usize, parent.actual_children, 1) catch
                return error.InvalidSnapshot;
        }
        paths = nodes.iterator();
        while (paths.next()) |entry| {
            if (entry.value_ptr.declared_children != entry.value_ptr.actual_children) {
                return error.InvalidSnapshot;
            }
        }
        putApplied(&batch, self.default_family, applied);
        try self.writeSync(batch);
        self.applied = applied;
    }

    fn create(
        self: *RocksStore,
        path: []const u8,
        data: []const u8,
        time_ms: i64,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        if (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/") or
            data.len > std.math.maxInt(i32))
        {
            return .{ .code = .bad_arguments };
        }
        if (try self.getNode(path)) |value| {
            var existing = value;
            existing.deinit(self.allocator);
            return .{ .code = .node_exists };
        }
        const parent_path = data_tree.parentPath(path) orelse return .{ .code = .bad_arguments };
        const maybe_parent = try self.getNode(parent_path);
        if (maybe_parent == null) return .{ .code = .no_node };
        var parent = maybe_parent.?;
        defer parent.deinit(self.allocator);
        if (parent.child_count == std.math.maxInt(i32) or
            parent.cversion == std.math.maxInt(i32)) return .{ .code = .bad_arguments };

        var node = Node{
            .data = try self.allocator.dupe(u8, data),
            .czxid = @intCast(index),
            .mzxid = @intCast(index),
            .ctime = time_ms,
            .mtime = time_ms,
            .version = 0,
            .cversion = 0,
            .pzxid = @intCast(index),
            .child_count = 0,
        };
        defer node.deinit(self.allocator);
        parent.cversion += 1;
        parent.pzxid = @intCast(index);
        parent.child_count += 1;
        try self.commitNodes(&.{
            .{ .path = path, .node = node },
            .{ .path = parent_path, .node = parent },
        }, null, .{ .index = index, .term = term });
        return .{ .code = .ok, .stat = node.stat() };
    }

    fn delete(
        self: *RocksStore,
        path: []const u8,
        expected_version: i32,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        if (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/")) {
            return .{ .code = .bad_arguments };
        }
        const maybe_node = try self.getNode(path);
        if (maybe_node == null) return .{ .code = .no_node };
        var node = maybe_node.?;
        defer node.deinit(self.allocator);
        if (node.child_count != 0) return .{ .code = .not_empty };
        if (expected_version != -1 and expected_version != node.version) return .{ .code = .bad_version };
        const parent_path = data_tree.parentPath(path).?;
        var parent = (try self.getNode(parent_path)).?;
        defer parent.deinit(self.allocator);
        if (parent.cversion == std.math.maxInt(i32)) return .{ .code = .bad_arguments };
        parent.cversion += 1;
        parent.pzxid = @intCast(index);
        parent.child_count -= 1;
        try self.commitNodes(
            &.{.{ .path = parent_path, .node = parent }},
            path,
            .{ .index = index, .term = term },
        );
        return .{ .code = .ok };
    }

    fn setData(
        self: *RocksStore,
        path: []const u8,
        data: []const u8,
        expected_version: i32,
        time_ms: i64,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        const maybe_node = try self.getNode(path);
        if (maybe_node == null) return .{ .code = .no_node };
        var node = maybe_node.?;
        defer node.deinit(self.allocator);
        if (expected_version != -1 and expected_version != node.version) return .{ .code = .bad_version };
        if (data.len > std.math.maxInt(i32) or node.version == std.math.maxInt(i32)) {
            return .{ .code = .bad_arguments };
        }
        const replacement = try self.allocator.dupe(u8, data);
        self.allocator.free(node.data);
        node.data = replacement;
        node.mzxid = @intCast(index);
        node.mtime = time_ms;
        node.version += 1;
        try self.commitNodes(
            &.{.{ .path = path, .node = node }},
            null,
            .{ .index = index, .term = term },
        );
        return .{ .code = .ok, .stat = node.stat() };
    }

    const NodeUpdate = struct {
        path: []const u8,
        node: Node,
    };

    fn commitNodes(
        self: *RocksStore,
        updates: []const NodeUpdate,
        delete_path: ?[]const u8,
        applied: Applied,
    ) !void {
        var batch = rocksdb.WriteBatch.init();
        defer batch.deinit();
        var encoded: std.ArrayList([]u8) = .empty;
        defer {
            for (encoded.items) |bytes| self.allocator.free(bytes);
            encoded.deinit(self.allocator);
        }
        for (updates) |update| {
            const bytes = try encodeNode(self.allocator, update.node);
            encoded.append(self.allocator, bytes) catch |err| {
                self.allocator.free(bytes);
                return err;
            };
            batch.put(self.default_family, update.path, bytes);
        }
        if (delete_path) |path| batch.delete(self.default_family, path);
        putApplied(&batch, self.default_family, applied);
        try self.writeSync(batch);
    }

    fn getNode(self: *RocksStore, path: []const u8) !?Node {
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        const maybe_data = try self.db.get(self.default_family, path, &error_data);
        if (maybe_data) |data| {
            defer data.deinit();
            return try decodeNode(self.allocator, data.data);
        }
        return null;
    }

    fn loadApplied(self: *RocksStore) !Applied {
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        const maybe_data = try self.db.get(self.default_family, applied_key, &error_data);
        if (maybe_data) |data| {
            defer data.deinit();
            if (data.data.len != 16) return error.InvalidAppliedCursor;
            return .{
                .index = std.mem.readInt(u64, data.data[0..8], .big),
                .term = std.mem.readInt(u64, data.data[8..16], .big),
            };
        }
        return error.MissingAppliedCursor;
    }

    fn writeSync(self: *RocksStore, batch: rocksdb.WriteBatch) !void {
        const options = rocksdb_c.rocksdb_writeoptions_create() orelse return error.OutOfMemory;
        defer rocksdb_c.rocksdb_writeoptions_destroy(options);
        rocksdb_c.rocksdb_writeoptions_set_sync(options, 1);
        var error_string: ?[*:0]u8 = null;
        rocksdb_c.rocksdb_write(
            self.db.db,
            options,
            batch.inner,
            @ptrCast(&error_string),
        );
        if (error_string) |message| {
            defer rocksdb_c.rocksdb_free(message);
            std.log.err("RocksDB write failed: {s}", .{std.mem.span(message)});
            return error.RocksDBWrite;
        }
    }
};

fn putApplied(
    batch: *const rocksdb.WriteBatch,
    family: rocksdb.ColumnFamilyHandle,
    applied: Applied,
) void {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], applied.index, .big);
    std.mem.writeInt(u64, bytes[8..16], applied.term, .big);
    batch.put(family, applied_key, &bytes);
}

fn encodeNode(allocator: std.mem.Allocator, node: Node) ![]u8 {
    var writer = jute.Writer.init(allocator);
    errdefer writer.deinit();
    try writer.writeBuffer(node.data);
    try writer.writeLong(node.czxid);
    try writer.writeLong(node.mzxid);
    try writer.writeLong(node.ctime);
    try writer.writeLong(node.mtime);
    try writer.writeInt(node.version);
    try writer.writeInt(node.cversion);
    try writer.writeLong(node.pzxid);
    try writer.writeLong(@bitCast(node.child_count));
    return writer.toOwnedSliceAssert();
}

fn decodeNode(allocator: std.mem.Allocator, bytes: []const u8) !Node {
    var reader = jute.Reader.init(bytes);
    const data = try allocator.dupe(u8, (try reader.readBuffer()) orelse return error.InvalidNode);
    errdefer allocator.free(data);
    const node = Node{
        .data = data,
        .czxid = try reader.readLong(),
        .mzxid = try reader.readLong(),
        .ctime = try reader.readLong(),
        .mtime = try reader.readLong(),
        .version = try reader.readInt(),
        .cversion = try reader.readInt(),
        .pzxid = try reader.readLong(),
        .child_count = std.math.cast(usize, @as(u64, @bitCast(try reader.readLong()))) orelse
            return error.InvalidNode,
    };
    if (reader.remaining() != 0) return error.InvalidNode;
    return node;
}

fn deinitFamilies(allocator: std.mem.Allocator, families: []const rocksdb.ColumnFamily) void {
    for (families) |family| allocator.free(family.name);
    allocator.free(families);
}

fn deinitErrorData(error_data: *?rocksdb.Data) void {
    if (error_data.*) |data| data.deinit();
    error_data.* = null;
}

test "snapshot restore rejects a missing root without changing live state" {
    const testing = std.testing;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(path);
    var store = try RocksStore.open(testing.allocator, path);
    defer store.deinit();

    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try writer.writeInt(@intCast(snapshot_version));
    try writer.writeLong(5);
    try writer.writeLong(1);
    try writer.writeInt(0);
    try testing.expectError(error.InvalidSnapshot, store.restore(writer.bytes(), 5, 1));
    try testing.expect((try store.exists("/")) != null);
    try testing.expectEqual(Applied{}, store.durableApplied());
}
