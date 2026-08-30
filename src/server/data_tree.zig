const std = @import("std");
const protocol = @import("../protocol.zig");

pub const ErrorCode = enum(i32) {
    ok = 0,
    connection_loss = -4,
    unimplemented = -6,
    bad_arguments = -8,
    no_node = -101,
    no_auth = -102,
    bad_version = -103,
    no_children_for_ephemerals = -108,
    node_exists = -110,
    not_empty = -111,
    session_expired = -112,
    invalid_acl = -114,
    auth_failed = -115,
    session_moved = -118,
};

pub const MutationResult = struct {
    code: ErrorCode,
    stat: ?protocol.data.Stat = null,
    created_path: ?[]const u8 = null,
    owned_created_path: ?[]u8 = null,

    pub fn deinit(self: *MutationResult, allocator: std.mem.Allocator) void {
        if (self.owned_created_path) |path| allocator.free(path);
        self.* = undefined;
    }
};

pub const Node = struct {
    data: []u8,
    data_is_null: bool = false,
    czxid: i64,
    mzxid: i64,
    ctime: i64,
    mtime: i64,
    version: i32 = 0,
    cversion: i32 = 0,
    sequence_counter: i32 = 0,
    aversion: i32 = 0,
    pzxid: i64,
    child_count: usize = 0,
    ephemeral_owner: i64 = 0,
    acl: ?[]u8 = null,

    pub fn deinit(self: *Node, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
        if (self.acl) |value| allocator.free(value);
        self.* = undefined;
    }

    pub fn stat(self: *const Node) protocol.data.Stat {
        return .{
            .czxid = self.czxid,
            .mzxid = self.mzxid,
            .ctime = self.ctime,
            .mtime = self.mtime,
            .version = self.version,
            .cversion = self.cversion,
            .aversion = self.aversion,
            .ephemeralOwner = self.ephemeral_owner,
            .dataLength = if (self.data_is_null) 0 else @intCast(self.data.len),
            .numChildren = @intCast(self.child_count),
            .pzxid = self.pzxid,
        };
    }
};

pub const DataTree = struct {
    allocator: std.mem.Allocator,
    nodes: std.StringHashMap(Node),

    pub fn init(allocator: std.mem.Allocator) !DataTree {
        var self = DataTree{
            .allocator = allocator,
            .nodes = std.StringHashMap(Node).init(allocator),
        };
        errdefer self.nodes.deinit();
        const root_path = try allocator.dupe(u8, "/");
        errdefer allocator.free(root_path);
        const root_data = try allocator.alloc(u8, 0);
        errdefer allocator.free(root_data);
        try self.nodes.putNoClobber(root_path, .{
            .data = root_data,
            .czxid = 0,
            .mzxid = 0,
            .ctime = 0,
            .mtime = 0,
            .pzxid = 0,
        });
        return self;
    }

    pub fn deinit(self: *DataTree) void {
        var iterator = self.nodes.iterator();
        while (iterator.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.nodes.deinit();
        self.* = undefined;
    }

    pub fn create(
        self: *DataTree,
        path: []const u8,
        data: []const u8,
        zxid: i64,
        time_ms: i64,
    ) !MutationResult {
        const parent_path = parentPath(path) orelse return .{ .code = .bad_arguments };
        if (self.nodes.contains(path)) return .{ .code = .node_exists };
        if (!self.nodes.contains(parent_path)) return .{ .code = .no_node };
        if (data.len > std.math.maxInt(i32)) return .{ .code = .bad_arguments };

        try self.nodes.ensureUnusedCapacity(1);
        const parent = self.nodes.getPtr(parent_path).?;
        if (parent.ephemeral_owner != 0) return .{ .code = .no_children_for_ephemerals };
        if (parent.child_count == std.math.maxInt(i32) or
            parent.cversion == std.math.maxInt(i32)) return .{ .code = .bad_arguments };
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        const owned_data = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(owned_data);
        self.nodes.putAssumeCapacityNoClobber(owned_path, .{
            .data = owned_data,
            .czxid = zxid,
            .mzxid = zxid,
            .ctime = time_ms,
            .mtime = time_ms,
            .pzxid = zxid,
        });
        parent.child_count += 1;
        parent.cversion += 1;
        parent.pzxid = zxid;
        return .{ .code = .ok, .stat = self.nodes.getPtr(path).?.stat() };
    }

    pub fn delete(self: *DataTree, path: []const u8, version: i32, zxid: i64) MutationResult {
        const parent_path = parentPath(path) orelse return .{ .code = .bad_arguments };
        const node = self.nodes.getPtr(path) orelse return .{ .code = .no_node };
        if (node.child_count != 0) return .{ .code = .not_empty };
        if (version != -1 and version != node.version) return .{ .code = .bad_version };
        if (self.nodes.get(parent_path).?.cversion == std.math.maxInt(i32)) {
            return .{ .code = .bad_arguments };
        }

        const removed = self.nodes.fetchRemove(path).?;
        self.allocator.free(removed.key);
        var removed_node = removed.value;
        removed_node.deinit(self.allocator);
        const parent = self.nodes.getPtr(parent_path).?;
        parent.child_count -= 1;
        parent.cversion += 1;
        parent.pzxid = zxid;
        return .{ .code = .ok };
    }

    pub fn setData(
        self: *DataTree,
        path: []const u8,
        data: []const u8,
        version: i32,
        zxid: i64,
        time_ms: i64,
    ) !MutationResult {
        const node = self.nodes.getPtr(path) orelse return .{ .code = .no_node };
        if (version != -1 and version != node.version) return .{ .code = .bad_version };
        if (data.len > std.math.maxInt(i32) or node.version == std.math.maxInt(i32)) {
            return .{ .code = .bad_arguments };
        }
        const owned_data = try self.allocator.dupe(u8, data);
        self.allocator.free(node.data);
        node.data = owned_data;
        node.version += 1;
        node.mzxid = zxid;
        node.mtime = time_ms;
        return .{ .code = .ok, .stat = node.stat() };
    }

    pub fn stat(self: *const DataTree, path: []const u8) ?protocol.data.Stat {
        const node = self.nodes.get(path) orelse return null;
        return node.stat();
    }

    pub fn copyData(self: *const DataTree, allocator: std.mem.Allocator, path: []const u8) !?struct {
        data: []u8,
        stat: protocol.data.Stat,
    } {
        const node = self.nodes.get(path) orelse return null;
        return .{ .data = try allocator.dupe(u8, node.data), .stat = node.stat() };
    }

    pub fn children(
        self: *const DataTree,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !?struct {
        names: [][]u8,
        stat: protocol.data.Stat,
    } {
        const parent = self.nodes.get(path) orelse return null;
        var names: std.ArrayList([]u8) = .empty;
        errdefer {
            for (names.items) |name| allocator.free(name);
            names.deinit(allocator);
        }
        var iterator = self.nodes.keyIterator();
        while (iterator.next()) |candidate| {
            if (directChildName(path, candidate.*)) |name| {
                try names.append(allocator, try allocator.dupe(u8, name));
            }
        }
        std.mem.sort([]u8, names.items, {}, struct {
            fn lessThan(_: void, left: []u8, right: []u8) bool {
                return std.mem.lessThan(u8, left, right);
            }
        }.lessThan);
        return .{ .names = try names.toOwnedSlice(allocator), .stat = parent.stat() };
    }

    pub fn nodeCount(self: *const DataTree) usize {
        return self.nodes.count();
    }

    pub fn writeSnapshot(self: *const DataTree, writer: anytype) !void {
        var paths = try self.allocator.alloc([]const u8, self.nodes.count());
        defer self.allocator.free(paths);
        var index: usize = 0;
        var keys = self.nodes.keyIterator();
        while (keys.next()) |path| : (index += 1) paths[index] = path.*;
        std.mem.sort([]const u8, paths, {}, struct {
            fn lessThan(_: void, left: []const u8, right: []const u8) bool {
                return std.mem.lessThan(u8, left, right);
            }
        }.lessThan);

        try writer.writeInt(-3);
        try writer.writeInt(@intCast(paths.len));
        for (paths) |path| {
            const node = self.nodes.get(path).?;
            try writer.writeString(path);
            try writer.writeBuffer(if (node.data_is_null) null else node.data);
            try writer.writeLong(node.czxid);
            try writer.writeLong(node.mzxid);
            try writer.writeLong(node.ctime);
            try writer.writeLong(node.mtime);
            try writer.writeInt(node.version);
            try writer.writeInt(node.cversion);
            try writer.writeLong(node.pzxid);
            try writer.writeInt(@intCast(node.child_count));
            try writer.writeLong(node.ephemeral_owner);
            try writer.writeInt(node.sequence_counter);
        }
    }

    pub fn readSnapshot(allocator: std.mem.Allocator, reader: anytype) !DataTree {
        const marker = try reader.readInt();
        const has_ephemeral_owner = marker == -1 or marker == -2 or marker == -3;
        const has_nullable_data = marker == -2 or marker == -3;
        const has_sequence_counter = marker == -3;
        const count = if (has_ephemeral_owner) try reader.readInt() else marker;
        if (count <= 0 or count > 10_000_000) return error.InvalidSnapshot;
        var self = DataTree{
            .allocator = allocator,
            .nodes = std.StringHashMap(Node).init(allocator),
        };
        errdefer self.deinit();
        try self.nodes.ensureTotalCapacity(@intCast(count));
        var index: i32 = 0;
        while (index < count) : (index += 1) {
            const path = (try reader.readStringAlloc(allocator)) orelse return error.InvalidSnapshot;
            errdefer allocator.free(path);
            const maybe_data = try reader.readBufferAlloc(allocator);
            if (!has_nullable_data and maybe_data == null) return error.InvalidSnapshot;
            const data = maybe_data orelse try allocator.alloc(u8, 0);
            errdefer allocator.free(data);
            if (self.nodes.contains(path)) return error.InvalidSnapshot;
            const czxid = try reader.readLong();
            const mzxid = try reader.readLong();
            const ctime = try reader.readLong();
            const mtime = try reader.readLong();
            const version = try reader.readInt();
            const cversion = try reader.readInt();
            const pzxid = try reader.readLong();
            const child_count = try reader.readInt();
            if (child_count < 0) return error.InvalidSnapshot;
            const ephemeral_owner = if (has_ephemeral_owner) try reader.readLong() else 0;
            const sequence_counter = if (has_sequence_counter) try reader.readInt() else cversion;
            self.nodes.putAssumeCapacityNoClobber(path, .{
                .data = data,
                .data_is_null = maybe_data == null,
                .czxid = czxid,
                .mzxid = mzxid,
                .ctime = ctime,
                .mtime = mtime,
                .version = version,
                .cversion = cversion,
                .sequence_counter = sequence_counter,
                .pzxid = pzxid,
                .child_count = @intCast(child_count),
                .ephemeral_owner = ephemeral_owner,
            });
        }
        const root = self.nodes.get("/") orelse return error.InvalidSnapshot;
        if (root.ephemeral_owner != 0) return error.InvalidSnapshot;
        var actual_children: std.StringHashMapUnmanaged(usize) = .empty;
        defer actual_children.deinit(allocator);
        try actual_children.ensureTotalCapacity(allocator, @intCast(self.nodes.count()));
        var nodes = self.nodes.iterator();
        while (nodes.next()) |entry| {
            actual_children.putAssumeCapacity(entry.key_ptr.*, 0);
            if (entry.value_ptr.ephemeral_owner != 0 and entry.value_ptr.child_count != 0) {
                return error.InvalidSnapshot;
            }
        }
        nodes = self.nodes.iterator();
        while (nodes.next()) |entry| {
            if (std.mem.eql(u8, entry.key_ptr.*, "/")) continue;
            const parent_path = parentPath(entry.key_ptr.*) orelse return error.InvalidSnapshot;
            const parent_count = actual_children.getPtr(parent_path) orelse return error.InvalidSnapshot;
            parent_count.* = std.math.add(usize, parent_count.*, 1) catch return error.InvalidSnapshot;
        }
        nodes = self.nodes.iterator();
        while (nodes.next()) |entry| {
            if (entry.value_ptr.child_count != actual_children.get(entry.key_ptr.*).?) {
                return error.InvalidSnapshot;
            }
        }
        return self;
    }
};

pub fn isValidPath(path: []const u8) bool {
    return std.mem.eql(u8, path, "/") or parentPath(path) != null;
}

pub fn parentPath(path: []const u8) ?[]const u8 {
    if (path.len < 2 or path[0] != '/' or path[path.len - 1] == '/') return null;
    if (std.mem.indexOf(u8, path, "//") != null) return null;
    const separator = std.mem.lastIndexOfScalar(u8, path, '/') orelse return null;
    return if (separator == 0) "/" else path[0..separator];
}

pub fn directChildName(parent: []const u8, candidate: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, parent, candidate)) return null;
    if (std.mem.eql(u8, parent, "/")) {
        if (candidate.len < 2 or candidate[0] != '/') return null;
        const name = candidate[1..];
        return if (std.mem.indexOfScalar(u8, name, '/') == null) name else null;
    }
    if (!std.mem.startsWith(u8, candidate, parent) or candidate.len <= parent.len + 1 or
        candidate[parent.len] != '/') return null;
    const name = candidate[parent.len + 1 ..];
    return if (std.mem.indexOfScalar(u8, name, '/') == null) name else null;
}

fn freeChildren(allocator: std.mem.Allocator, names: [][]u8) void {
    for (names) |name| allocator.free(name);
    allocator.free(names);
}

test "data tree applies CRUD with ZooKeeper versions and child metadata" {
    const testing = std.testing;
    var tree = try DataTree.init(testing.allocator);
    defer tree.deinit();

    const created = try tree.create("/app", "one", 1, 100);
    try testing.expectEqual(ErrorCode.ok, created.code);
    try testing.expectEqual(@as(i64, 1), created.stat.?.czxid);
    try testing.expectEqual(@as(i32, 1), tree.stat("/").?.cversion);

    const child = try tree.create("/app/child", "two", 2, 200);
    try testing.expectEqual(ErrorCode.ok, child.code);
    try testing.expectEqual(@as(i32, 1), tree.stat("/app").?.numChildren);
    try testing.expectEqual(ErrorCode.node_exists, (try tree.create("/app", "", 3, 300)).code);

    const changed = try tree.setData("/app", "updated", 0, 3, 300);
    try testing.expectEqual(ErrorCode.ok, changed.code);
    try testing.expectEqual(@as(i32, 1), changed.stat.?.version);
    try testing.expectEqual(ErrorCode.bad_version, (try tree.setData("/app", "bad", 0, 4, 400)).code);
    try testing.expectEqual(ErrorCode.not_empty, tree.delete("/app", -1, 5).code);
    try testing.expectEqual(ErrorCode.ok, tree.delete("/app/child", -1, 5).code);

    const copied = (try tree.copyData(testing.allocator, "/app")).?;
    defer testing.allocator.free(copied.data);
    try testing.expectEqualStrings("updated", copied.data);
    const listed = (try tree.children(testing.allocator, "/")).?;
    defer freeChildren(testing.allocator, listed.names);
    try testing.expectEqual(@as(usize, 1), listed.names.len);
    try testing.expectEqualStrings("app", listed.names[0]);
}
