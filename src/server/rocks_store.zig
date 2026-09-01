const std = @import("std");
const rocksdb = @import("rocksdb");
const rocksdb_c = @import("rocksdb_c");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");
const acl = @import("acl.zig");
const command = @import("command.zig");
const data_tree = @import("data_tree.zig");
const multi = @import("multi.zig");
const ephemeral = @import("ephemeral.zig");

const Node = data_tree.Node;
const ErrorCode = data_tree.ErrorCode;

const applied_key = "\x00applied";
const import_zxid_key = "\x00import-zxid";
const state_prefix = "\x01";
const session_clock_key = "\x01clock";
const zxid_base_key = "\x01zxid-base";
const maintenance_clock_key = "\x01maintenance-clock";
const session_prefix = "\x01session/";
const snapshot_version: u32 = 1;
pub const max_snapshot_bytes: usize = 256 * 1024 * 1024;

pub const Applied = struct {
    index: u64 = 0,
    term: u64 = 0,
};

pub const DataResult = struct {
    data: ?[]u8,
    stat: protocol.data.Stat,
};

pub const AclResult = struct {
    blob: ?[]u8,
    stat: protocol.data.Stat,

    pub fn deinit(self: *AclResult, allocator: std.mem.Allocator) void {
        if (self.blob) |value| allocator.free(value);
        self.* = undefined;
    }
};

pub const Session = struct {
    password: [16]u8,
    timeout_ms: i32,
    tick_grace_ms: i32,
    expires_at_ms: i64,
    generation: u64,
};

pub const ExpiredSession = struct {
    session_id: i64,
    expires_at_ms: i64,
};

pub const ExtendedCandidate = struct {
    path: []u8,
    czxid: i64,
    kind: ephemeral.NodeKind,
};

pub const ImportNode = struct {
    path: []const u8,
    data: ?[]const u8,
    acl: ?[]const u8,
    czxid: i64,
    mzxid: i64,
    ctime: i64,
    mtime: i64,
    version: i32,
    cversion: i32,
    sequence_counter: i32,
    aversion: i32,
    ephemeral_owner: i64,
    kind: ephemeral.NodeKind = .persistent,
    pzxid: i64,
    child_count: usize,
};

pub const ImportSession = struct {
    session_id: i64,
    password: [16]u8,
    timeout_ms: i32,
};

pub const RocksStore = struct {
    allocator: std.mem.Allocator,
    db: rocksdb.DB,
    families: []const rocksdb.ColumnFamily,
    default_family: rocksdb.ColumnFamilyHandle,
    applied: Applied,
    session_clock_ms: i64,
    zxid_base: i64,
    maintenance_clock_ms: i64,

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
            .session_clock_ms = 0,
            .zxid_base = 0,
            .maintenance_clock_ms = 0,
        };
        if (try store.getNode("/")) |root| {
            var owned_root = root;
            owned_root.deinit(allocator);
            store.applied = try store.loadApplied();
            store.session_clock_ms = (try store.loadSessionClock()) orelse blk: {
                var batch = rocksdb.WriteBatch.init();
                defer batch.deinit();
                putSessionClock(&batch, default_family, 0);
                try store.writeSync(batch);
                break :blk 0;
            };
            store.zxid_base = (try store.loadZxidBase()) orelse 0;
            store.maintenance_clock_ms = (try store.loadMaintenanceClock()) orelse 0;
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
            putSessionClock(&batch, default_family, 0);
            putZxidBase(&batch, default_family, 0);
            putMaintenanceClock(&batch, default_family, 0);
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

    pub fn installImported(
        self: *RocksStore,
        nodes: []const ImportNode,
        sessions: []const ImportSession,
        tick_grace_ms: i32,
        source_zxid: i64,
    ) !void {
        if (tick_grace_ms <= 0) return error.InvalidTickGrace;
        var batch = rocksdb.WriteBatch.init();
        defer batch.deinit();
        batch.deleteRange(self.default_family, "\x00", "\xff");
        var encoded_nodes: std.ArrayList([]u8) = .empty;
        defer {
            for (encoded_nodes.items) |bytes| self.allocator.free(bytes);
            encoded_nodes.deinit(self.allocator);
        }
        var found_root = false;
        for (nodes) |imported| {
            if (!data_tree.isValidPath(imported.path)) return error.InvalidImport;
            found_root = found_root or std.mem.eql(u8, imported.path, "/");
            ephemeral.validate(imported.kind, imported.ephemeral_owner) catch return error.InvalidImport;
            const node = Node{
                .data = @constCast(imported.data orelse &.{}),
                .data_is_null = imported.data == null,
                .czxid = imported.czxid,
                .mzxid = imported.mzxid,
                .ctime = imported.ctime,
                .mtime = imported.mtime,
                .version = imported.version,
                .cversion = imported.cversion,
                .sequence_counter = imported.sequence_counter,
                .aversion = imported.aversion,
                .pzxid = imported.pzxid,
                .child_count = imported.child_count,
                .ephemeral_owner = imported.ephemeral_owner,
                .kind = imported.kind,
                .acl = if (imported.acl) |value| @constCast(value) else null,
            };
            const encoded = try encodeNode(self.allocator, node);
            encoded_nodes.append(self.allocator, encoded) catch |err| {
                self.allocator.free(encoded);
                return err;
            };
            batch.put(self.default_family, imported.path, encoded);
        }
        if (!found_root) return error.InvalidImport;
        for (sessions) |imported| {
            if (imported.session_id == 0 or imported.timeout_ms <= 0) return error.InvalidImport;
            var key_buffer: [session_prefix.len + 8]u8 = undefined;
            const key = sessionKey(imported.session_id, &key_buffer);
            var encoded: [40]u8 = undefined;
            encodeSession(.{
                .password = imported.password,
                .timeout_ms = imported.timeout_ms,
                .tick_grace_ms = tick_grace_ms,
                .expires_at_ms = try renewedExpiry(0, imported.timeout_ms, tick_grace_ms),
                .generation = 1,
            }, &encoded);
            batch.put(self.default_family, key, &encoded);
        }
        putSessionClock(&batch, self.default_family, 0);
        putZxidBase(&batch, self.default_family, source_zxid);
        putMaintenanceClock(&batch, self.default_family, 0);
        putApplied(&batch, self.default_family, .{});
        var source_zxid_bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &source_zxid_bytes, source_zxid, .big);
        batch.put(self.default_family, import_zxid_key, &source_zxid_bytes);
        try self.writeSync(batch);
        self.applied = .{};
        self.session_clock_ms = 0;
        self.zxid_base = source_zxid;
        self.maintenance_clock_ms = 0;
    }

    pub fn apply(
        self: *RocksStore,
        operation: command.Mutation,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        const result = if (try self.validateMutationConnection(operation)) |rejected|
            rejected
        else switch (operation) {
            .create => |request| try self.create(
                request.path,
                request.data,
                request.time_ms,
                request.node_kind,
                request.ttl_ms,
                request.session_id,
                request.sequential,
                request.acl,
                request.identities,
                index,
                term,
            ),
            .delete => |request| try self.delete(
                request.path,
                request.expected_version,
                request.identities,
                index,
                term,
            ),
            .set_acl => |request| try self.setAcl(
                request.path,
                request.acl,
                request.expected_version,
                request.identities,
                index,
                term,
            ),
            .set_data => |request| try self.setData(
                request.path,
                request.data,
                request.expected_version,
                request.time_ms,
                request.identities,
                index,
                term,
            ),
            .open_session => |request| try self.openSession(
                request.session_id,
                request.password,
                request.timeout_ms,
                request.tick_grace_ms,
                request.generation,
                index,
                term,
            ),
            .touch_session => |request| try self.touchSession(
                request.session_id,
                request.password,
                request.generation,
                index,
                term,
            ),
            .close_session => |request| try self.closeSession(
                request.session_id,
                request.password,
                request.generation,
                index,
                term,
            ),
            .expire_session => |request| try self.expireSession(
                request.session_id,
                request.expected_expires_at_ms,
                index,
                term,
            ),
            .move_session => |request| try self.moveSession(
                request.session_id,
                request.password,
                request.expected_generation,
                request.new_generation,
                index,
                term,
            ),
            .session_tick => |request| try self.sessionTick(
                request.leader_term,
                request.elapsed_ms,
                request.leader_wall_ms,
                index,
                term,
            ),
            .delete_extended => |request| try self.deleteExtended(
                request.path,
                request.expected_czxid,
                request.expected_kind,
                index,
                term,
            ),
            .multi => |request| try self.applyMulti(
                request.body,
                request.time_ms,
                request.session_id,
                request.identities,
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

    pub fn importedSourceZxid(self: *RocksStore) !?i64 {
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        const maybe_data = try self.db.get(self.default_family, import_zxid_key, &error_data);
        if (maybe_data) |data| {
            defer data.deinit();
            if (data.data.len != 8) return error.InvalidImportMarker;
            return std.mem.readInt(i64, data.data[0..8], .big);
        }
        return null;
    }

    pub fn clientZxid(self: *const RocksStore, raft_index: u64) !i64 {
        const index = std.math.cast(i64, raft_index) orelse return error.ZxidOverflow;
        return std.math.add(i64, self.zxid_base, index) catch error.ZxidOverflow;
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
                .data = if (node.data_is_null) null else try allocator.dupe(u8, node.data),
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

    pub fn authorize(
        self: *RocksStore,
        path: []const u8,
        permission: i32,
        identities: ?[]const u8,
    ) !data_tree.ErrorCode {
        const maybe_node = try self.getNode(path);
        if (maybe_node == null) return .no_node;
        var node = maybe_node.?;
        defer node.deinit(self.allocator);
        return if (try acl.allows(node.acl, permission, identities)) .ok else .no_auth;
    }

    pub fn getAcl(
        self: *RocksStore,
        allocator: std.mem.Allocator,
        path: []const u8,
    ) !?AclResult {
        const maybe_node = try self.getNode(path);
        if (maybe_node == null) return null;
        var node = maybe_node.?;
        defer node.deinit(self.allocator);
        return .{
            .blob = if (node.acl) |value| try allocator.dupe(u8, value) else null,
            .stat = node.stat(),
        };
    }

    pub fn ephemeralPaths(
        self: *RocksStore,
        allocator: std.mem.Allocator,
        session_id: i64,
    ) ![][]u8 {
        var paths: std.ArrayList([]u8) = .empty;
        errdefer {
            for (paths.items) |path| allocator.free(path);
            paths.deinit(allocator);
        }
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        var iterator = self.db.iterator(self.default_family, .forward, "/");
        defer iterator.deinit();
        while (try iterator.next(&error_data)) |entry| {
            if (entry[0].data.len == 0 or entry[0].data[0] != '/') break;
            var node = try decodeNode(self.allocator, entry[1].data);
            defer node.deinit(self.allocator);
            if (node.kind != .ephemeral or node.ephemeral_owner != session_id) continue;
            const owned_path = try allocator.dupe(u8, entry[0].data);
            paths.append(allocator, owned_path) catch |err| {
                allocator.free(owned_path);
                return err;
            };
        }
        return paths.toOwnedSlice(allocator);
    }

    pub fn getSession(self: *RocksStore, session_id: i64) !?Session {
        var key_buffer: [session_prefix.len + 8]u8 = undefined;
        const key = sessionKey(session_id, &key_buffer);
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        const maybe_data = try self.db.get(self.default_family, key, &error_data);
        if (maybe_data) |data| {
            defer data.deinit();
            return try decodeSession(data.data);
        }
        return null;
    }

    pub fn validateSession(
        self: *RocksStore,
        session_id: i64,
        generation: u64,
    ) !data_tree.ErrorCode {
        const session = (try self.getSession(session_id)) orelse return .session_expired;
        if (session.generation != generation) return .session_moved;
        if (session.expires_at_ms <= self.session_clock_ms) return .session_expired;
        return .ok;
    }

    pub fn expiredSessions(
        self: *RocksStore,
        allocator: std.mem.Allocator,
    ) ![]ExpiredSession {
        var expired: std.ArrayList(ExpiredSession) = .empty;
        errdefer expired.deinit(allocator);
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        var iterator = self.db.iterator(self.default_family, .forward, session_prefix);
        defer iterator.deinit();
        while (try iterator.next(&error_data)) |entry| {
            if (!std.mem.startsWith(u8, entry[0].data, session_prefix)) break;
            const session_id = try decodeSessionKey(entry[0].data);
            const session = try decodeSession(entry[1].data);
            if (session.expires_at_ms <= self.session_clock_ms) {
                try expired.append(allocator, .{
                    .session_id = session_id,
                    .expires_at_ms = session.expires_at_ms,
                });
            }
        }
        return try expired.toOwnedSlice(allocator);
    }

    pub fn expiredExtendedNodes(
        self: *RocksStore,
        allocator: std.mem.Allocator,
        limit: usize,
    ) ![]ExtendedCandidate {
        var candidates: std.ArrayList(ExtendedCandidate) = .empty;
        errdefer {
            for (candidates.items) |candidate| allocator.free(candidate.path);
            candidates.deinit(allocator);
        }
        if (limit == 0 or self.maintenance_clock_ms == 0) return candidates.toOwnedSlice(allocator);
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        var iterator = self.db.iterator(self.default_family, .forward, "/");
        defer iterator.deinit();
        while (candidates.items.len < limit) {
            const entry = (try iterator.next(&error_data)) orelse break;
            if (entry[0].data.len == 0 or entry[0].data[0] != '/') break;
            var node = try decodeNode(self.allocator, entry[1].data);
            defer node.deinit(self.allocator);
            if (!extendedEligible(node, self.maintenance_clock_ms)) continue;
            const owned_path = try allocator.dupe(u8, entry[0].data);
            candidates.append(allocator, .{
                .path = owned_path,
                .czxid = node.czxid,
                .kind = node.kind,
            }) catch |err| {
                allocator.free(owned_path);
                return err;
            };
        }
        return candidates.toOwnedSlice(allocator);
    }

    pub fn snapshot(self: *RocksStore, allocator: std.mem.Allocator) ![]u8 {
        var writer = jute.Writer.init(allocator);
        defer writer.deinit();
        try writer.writeInt(@intCast(snapshot_version));
        try writer.writeLong(@bitCast(self.applied.index));
        try writer.writeLong(@bitCast(self.applied.term));
        const count_offset = writer.bytes().len;
        try writer.writeInt(0);
        var count: u32 = 0;
        if (try self.importedSourceZxid()) |source_zxid| {
            var encoded_zxid: [8]u8 = undefined;
            std.mem.writeInt(i64, &encoded_zxid, source_zxid, .big);
            try writer.writeBuffer(import_zxid_key);
            try writer.writeBuffer(&encoded_zxid);
            count += 1;
        }
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        var iterator = self.db.iterator(self.default_family, .forward, state_prefix);
        defer iterator.deinit();
        while (try iterator.next(&error_data)) |entry| {
            if (!isSnapshotRecordKey(entry[0].data)) break;
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
        return allocator.dupe(u8, writer.bytes());
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
        const Validation = struct {
            declared_children: usize,
            actual_children: usize = 0,
            ephemeral_owner: i64,
            kind: ephemeral.NodeKind,
        };
        var nodes: std.StringHashMapUnmanaged(Validation) = .empty;
        defer nodes.deinit(self.allocator);
        var sessions: std.AutoHashMapUnmanaged(i64, void) = .empty;
        defer sessions.deinit(self.allocator);
        var restored_clock: ?i64 = null;
        var restored_zxid_base: ?i64 = null;
        var restored_maintenance_clock: ?i64 = null;
        var restored_import_zxid: ?i64 = null;
        var index: u32 = 0;
        while (index < count) : (index += 1) {
            const key = (try reader.readBuffer()) orelse return error.InvalidSnapshot;
            const value = (try reader.readBuffer()) orelse return error.InvalidSnapshot;
            if (data_tree.isValidPath(key)) {
                var node = decodeNode(self.allocator, value) catch |err| switch (err) {
                    error.OutOfMemory => return error.OutOfMemory,
                    else => return error.InvalidSnapshot,
                };
                defer node.deinit(self.allocator);
                const result = try nodes.getOrPut(self.allocator, key);
                if (result.found_existing) return error.InvalidSnapshot;
                result.value_ptr.* = .{
                    .declared_children = node.child_count,
                    .ephemeral_owner = node.ephemeral_owner,
                    .kind = node.kind,
                };
            } else if (std.mem.eql(u8, key, import_zxid_key)) {
                if (restored_import_zxid != null or value.len != 8) return error.InvalidSnapshot;
                restored_import_zxid = std.mem.readInt(i64, value[0..8], .big);
            } else if (std.mem.eql(u8, key, zxid_base_key)) {
                if (restored_zxid_base != null or value.len != 8) return error.InvalidSnapshot;
                restored_zxid_base = std.mem.readInt(i64, value[0..8], .big);
            } else if (std.mem.eql(u8, key, maintenance_clock_key)) {
                if (restored_maintenance_clock != null or value.len != 8) return error.InvalidSnapshot;
                const clock = std.mem.readInt(i64, value[0..8], .big);
                if (clock < 0) return error.InvalidSnapshot;
                restored_maintenance_clock = clock;
            } else if (std.mem.eql(u8, key, session_clock_key)) {
                if (restored_clock != null or value.len != 8) return error.InvalidSnapshot;
                const clock = std.mem.readInt(i64, value[0..8], .big);
                if (clock < 0) return error.InvalidSnapshot;
                restored_clock = clock;
            } else if (std.mem.startsWith(u8, key, session_prefix)) {
                const session_id = decodeSessionKey(key) catch return error.InvalidSnapshot;
                _ = decodeSession(value) catch return error.InvalidSnapshot;
                const result = try sessions.getOrPut(self.allocator, session_id);
                if (result.found_existing) return error.InvalidSnapshot;
            } else {
                return error.InvalidSnapshot;
            }
            batch.put(self.default_family, key, value);
        }
        if (reader.remaining() != 0 or !nodes.contains("/") or
            (restored_clock == null and sessions.count() != 0)) return error.InvalidSnapshot;
        const effective_clock = restored_clock orelse 0;
        var paths = nodes.iterator();
        while (paths.next()) |entry| {
            ephemeral.validate(entry.value_ptr.kind, entry.value_ptr.ephemeral_owner) catch
                return error.InvalidSnapshot;
            if (entry.value_ptr.kind == .ephemeral) {
                if (!sessions.contains(entry.value_ptr.ephemeral_owner) or
                    entry.value_ptr.declared_children != 0 or
                    std.mem.eql(u8, entry.key_ptr.*, "/")) return error.InvalidSnapshot;
            }
            if (std.mem.eql(u8, entry.key_ptr.*, "/") and entry.value_ptr.kind != .persistent) {
                return error.InvalidSnapshot;
            }
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
        if (restored_import_zxid) |source_zxid| {
            if (restored_zxid_base == null or restored_zxid_base.? != source_zxid) {
                return error.InvalidSnapshot;
            }
        }
        const effective_zxid_base = restored_zxid_base orelse 0;
        putSessionClock(&batch, self.default_family, effective_clock);
        const effective_maintenance_clock = restored_maintenance_clock orelse 0;
        putZxidBase(&batch, self.default_family, effective_zxid_base);
        putMaintenanceClock(&batch, self.default_family, effective_maintenance_clock);
        putApplied(&batch, self.default_family, applied);
        try self.writeSync(batch);
        self.applied = applied;
        self.session_clock_ms = effective_clock;
        self.zxid_base = effective_zxid_base;
        self.maintenance_clock_ms = effective_maintenance_clock;
    }

    fn validateMutationConnection(
        self: *RocksStore,
        operation: command.Mutation,
    ) !?data_tree.MutationResult {
        const identity: ?struct { id: i64, generation: u64 } = switch (operation) {
            .create => |request| .{ .id = request.session_id, .generation = request.session_generation },
            .delete => |request| .{ .id = request.session_id, .generation = request.session_generation },
            .set_acl => |request| .{ .id = request.session_id, .generation = request.session_generation },
            .set_data => |request| .{ .id = request.session_id, .generation = request.session_generation },
            .multi => |request| .{ .id = request.session_id, .generation = request.session_generation },
            else => null,
        };
        const connection = identity orelse return null;
        if (connection.id == 0 and connection.generation == 0) return null;
        if (connection.id == 0 or connection.generation == 0) {
            return .{ .code = .session_moved };
        }
        const code = try self.validateSession(connection.id, connection.generation);
        return if (code == .ok) null else .{ .code = code };
    }

    fn openSession(
        self: *RocksStore,
        session_id: i64,
        password: []const u8,
        timeout_ms: i32,
        tick_grace_ms: i32,
        generation: u64,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        if (session_id == 0 or password.len != 16 or timeout_ms <= 0 or
            tick_grace_ms <= 0 or generation == 0)
        {
            return .{ .code = .bad_arguments };
        }
        if (try self.getSession(session_id) != null) return .{ .code = .session_moved };
        var owned_password: [16]u8 = undefined;
        @memcpy(&owned_password, password);
        try self.commitSession(session_id, .{
            .password = owned_password,
            .timeout_ms = timeout_ms,
            .tick_grace_ms = tick_grace_ms,
            .expires_at_ms = try renewedExpiry(self.session_clock_ms, timeout_ms, tick_grace_ms),
            .generation = generation,
        }, .{ .index = index, .term = term });
        return .{ .code = .ok };
    }

    fn touchSession(
        self: *RocksStore,
        session_id: i64,
        password: []const u8,
        generation: u64,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        var session = (try self.getSession(session_id)) orelse
            return .{ .code = .session_expired };
        if (password.len != session.password.len or !std.mem.eql(u8, &session.password, password)) {
            return .{ .code = .auth_failed };
        }
        if (session.generation != generation) return .{ .code = .session_moved };
        if (session.expires_at_ms <= self.session_clock_ms) return .{ .code = .session_expired };
        session.expires_at_ms = try renewedExpiry(
            self.session_clock_ms,
            session.timeout_ms,
            session.tick_grace_ms,
        );
        try self.commitSession(session_id, session, .{ .index = index, .term = term });
        return .{ .code = .ok };
    }

    fn moveSession(
        self: *RocksStore,
        session_id: i64,
        password: []const u8,
        expected_generation: u64,
        new_generation: u64,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        var session = (try self.getSession(session_id)) orelse
            return .{ .code = .session_expired };
        if (password.len != session.password.len or !std.mem.eql(u8, &session.password, password)) {
            return .{ .code = .auth_failed };
        }
        const required_generation = std.math.add(u64, expected_generation, 1) catch
            return .{ .code = .session_moved };
        if (session.generation != expected_generation or new_generation != required_generation) {
            return .{ .code = .session_moved };
        }
        if (session.expires_at_ms <= self.session_clock_ms) return .{ .code = .session_expired };
        session.generation = new_generation;
        session.expires_at_ms = try renewedExpiry(
            self.session_clock_ms,
            session.timeout_ms,
            session.tick_grace_ms,
        );
        try self.commitSession(session_id, session, .{ .index = index, .term = term });
        return .{ .code = .ok, .changed = true };
    }

    fn closeSession(
        self: *RocksStore,
        session_id: i64,
        password: []const u8,
        generation: u64,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        const session = (try self.getSession(session_id)) orelse
            return .{ .code = .session_expired };
        if (password.len != session.password.len or !std.mem.eql(u8, &session.password, password)) {
            return .{ .code = .auth_failed };
        }
        if (session.generation != generation) return .{ .code = .session_moved };
        try self.commitSessionRemoval(session_id, .{ .index = index, .term = term });
        return .{ .code = .ok, .changed = true };
    }

    fn expireSession(
        self: *RocksStore,
        session_id: i64,
        expected_expires_at_ms: i64,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        const session = (try self.getSession(session_id)) orelse {
            try self.advanceApplied(index, term);
            return .{ .code = .ok };
        };
        if (session.expires_at_ms != expected_expires_at_ms or
            session.expires_at_ms > self.session_clock_ms)
        {
            try self.advanceApplied(index, term);
            return .{ .code = .ok };
        }
        try self.commitSessionRemoval(session_id, .{ .index = index, .term = term });
        return .{ .code = .ok, .changed = true };
    }

    fn sessionTick(
        self: *RocksStore,
        leader_term: u64,
        elapsed_ms: i64,
        leader_wall_ms: i64,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        if (leader_term != term or elapsed_ms <= 0 or elapsed_ms > 60_000) {
            return .{ .code = .session_moved };
        }
        const next_clock = std.math.add(i64, self.session_clock_ms, elapsed_ms) catch
            return .{ .code = .bad_arguments };
        const next_maintenance_clock = if (self.maintenance_clock_ms == 0) blk: {
            break :blk if (leader_wall_ms > 0) leader_wall_ms else 0;
        } else std.math.add(i64, self.maintenance_clock_ms, elapsed_ms) catch
            return .{ .code = .bad_arguments };
        var batch = rocksdb.WriteBatch.init();
        defer batch.deinit();
        putSessionClock(&batch, self.default_family, next_clock);
        putMaintenanceClock(&batch, self.default_family, next_maintenance_clock);
        putApplied(&batch, self.default_family, .{ .index = index, .term = term });
        try self.writeSync(batch);
        self.session_clock_ms = next_clock;
        self.maintenance_clock_ms = next_maintenance_clock;
        return .{ .code = .ok };
    }

    fn commitSession(
        self: *RocksStore,
        session_id: i64,
        session: ?Session,
        applied: Applied,
    ) !void {
        var key_buffer: [session_prefix.len + 8]u8 = undefined;
        const key = sessionKey(session_id, &key_buffer);
        var batch = rocksdb.WriteBatch.init();
        defer batch.deinit();
        if (session) |value| {
            var encoded: [40]u8 = undefined;
            encodeSession(value, &encoded);
            batch.put(self.default_family, key, &encoded);
        } else {
            batch.delete(self.default_family, key);
        }
        putApplied(&batch, self.default_family, applied);
        try self.writeSync(batch);
    }

    fn commitSessionRemoval(
        self: *RocksStore,
        session_id: i64,
        applied: Applied,
    ) !void {
        var ephemeral_paths: std.ArrayList([]u8) = .empty;
        defer {
            for (ephemeral_paths.items) |path| self.allocator.free(path);
            ephemeral_paths.deinit(self.allocator);
        }
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        var iterator = self.db.iterator(self.default_family, .forward, "/");
        defer iterator.deinit();
        while (try iterator.next(&error_data)) |entry| {
            if (entry[0].data.len == 0 or entry[0].data[0] != '/') break;
            var node = try decodeNode(self.allocator, entry[1].data);
            defer node.deinit(self.allocator);
            if (node.kind != .ephemeral or node.ephemeral_owner != session_id) continue;
            if (node.child_count != 0) return error.InvalidEphemeralNode;
            const owned_path = try self.allocator.dupe(u8, entry[0].data);
            ephemeral_paths.append(self.allocator, owned_path) catch |err| {
                self.allocator.free(owned_path);
                return err;
            };
        }

        var parents: std.StringHashMapUnmanaged(Node) = .empty;
        defer {
            var values = parents.valueIterator();
            while (values.next()) |node| node.deinit(self.allocator);
            parents.deinit(self.allocator);
        }
        for (ephemeral_paths.items) |path| {
            const parent_path = data_tree.parentPath(path) orelse return error.InvalidEphemeralNode;
            var parent = parents.getPtr(parent_path);
            if (parent == null) {
                var loaded = (try self.getNode(parent_path)) orelse return error.InvalidEphemeralNode;
                parents.put(self.allocator, parent_path, loaded) catch |err| {
                    loaded.deinit(self.allocator);
                    return err;
                };
                parent = parents.getPtr(parent_path).?;
            }
            if (parent.?.child_count == 0 or parent.?.cversion == std.math.maxInt(i32)) {
                return error.InvalidEphemeralNode;
            }
            parent.?.child_count -= 1;
            parent.?.cversion += 1;
            parent.?.pzxid = self.clientZxid(applied.index) catch
                return error.InvalidEphemeralNode;
        }

        var batch = rocksdb.WriteBatch.init();
        defer batch.deinit();
        var session_key_buffer: [session_prefix.len + 8]u8 = undefined;
        batch.delete(self.default_family, sessionKey(session_id, &session_key_buffer));
        for (ephemeral_paths.items) |path| batch.delete(self.default_family, path);
        var encoded_parents: std.ArrayList([]u8) = .empty;
        defer {
            for (encoded_parents.items) |bytes| self.allocator.free(bytes);
            encoded_parents.deinit(self.allocator);
        }
        var parent_entries = parents.iterator();
        while (parent_entries.next()) |entry| {
            const encoded = try encodeNode(self.allocator, entry.value_ptr.*);
            encoded_parents.append(self.allocator, encoded) catch |err| {
                self.allocator.free(encoded);
                return err;
            };
            batch.put(self.default_family, entry.key_ptr.*, encoded);
        }
        putApplied(&batch, self.default_family, applied);
        try self.writeSync(batch);
    }

    const MultiState = struct {
        allocator: std.mem.Allocator,
        nodes: std.StringHashMap(Node),
        dirty: std.StringHashMap(void),
        deleted: std.StringHashMap(void),

        fn load(store: *RocksStore) MultiState {
            return .{
                .allocator = store.allocator,
                .nodes = std.StringHashMap(Node).init(store.allocator),
                .dirty = std.StringHashMap(void).init(store.allocator),
                .deleted = std.StringHashMap(void).init(store.allocator),
            };
        }

        fn deinit(state: *MultiState) void {
            var nodes = state.nodes.iterator();
            while (nodes.next()) |entry| {
                state.allocator.free(entry.key_ptr.*);
                entry.value_ptr.deinit(state.allocator);
            }
            state.nodes.deinit();
            var dirty = state.dirty.keyIterator();
            while (dirty.next()) |path| state.allocator.free(path.*);
            state.dirty.deinit();
            var deleted = state.deleted.keyIterator();
            while (deleted.next()) |path| state.allocator.free(path.*);
            state.deleted.deinit();
            state.* = undefined;
        }

        fn getNode(state: *MultiState, store: *RocksStore, path: []const u8) !?*Node {
            if (state.nodes.getPtr(path)) |node| return node;
            if (state.deleted.contains(path)) return null;
            var node = (try store.getNode(path)) orelse return null;
            errdefer node.deinit(state.allocator);
            const owned_path = try state.allocator.dupe(u8, path);
            errdefer state.allocator.free(owned_path);
            try state.nodes.putNoClobber(owned_path, node);
            return state.nodes.getPtr(owned_path).?;
        }

        fn markDirty(state: *MultiState, path: []const u8) !void {
            if (state.dirty.contains(path)) return;
            const owned_path = try state.allocator.dupe(u8, path);
            errdefer state.allocator.free(owned_path);
            try state.dirty.putNoClobber(owned_path, {});
        }

        fn clearDirty(state: *MultiState, path: []const u8) void {
            if (state.dirty.fetchRemove(path)) |removed| state.allocator.free(removed.key);
        }

        fn markDeleted(state: *MultiState, path: []const u8) !void {
            state.clearDirty(path);
            if (state.deleted.contains(path)) return;
            const owned_path = try state.allocator.dupe(u8, path);
            errdefer state.allocator.free(owned_path);
            try state.deleted.putNoClobber(owned_path, {});
        }

        fn clearDeleted(state: *MultiState, path: []const u8) void {
            if (state.deleted.fetchRemove(path)) |removed| state.allocator.free(removed.key);
        }

        fn applyOperation(
            state: *MultiState,
            store: *RocksStore,
            operation: multi.Operation,
            time_ms: i64,
            session_id: i64,
            identities: ?[]const u8,
            index: u64,
        ) !data_tree.MutationResult {
            return switch (operation) {
                .create, .create2, .create_container => |request| try state.create(
                    store,
                    request,
                    null,
                    time_ms,
                    session_id,
                    identities,
                    index,
                ),
                .create_ttl => |request| try state.create(
                    store,
                    .{
                        .path = request.path,
                        .data = request.data,
                        .acl = request.acl,
                        .flags = request.flags,
                    },
                    request.ttl,
                    time_ms,
                    session_id,
                    identities,
                    index,
                ),
                .delete => |request| try state.delete(store, request, identities, index),
                .set_data => |request| try state.setData(store, request, time_ms, identities, index),
                .check => |request| try state.check(store, request, identities),
            };
        }

        fn create(
            state: *MultiState,
            store: *RocksStore,
            request: protocol.proto.CreateRequest,
            ttl_ms: ?i64,
            time_ms: i64,
            session_id: i64,
            identities: ?[]const u8,
            index: u64,
        ) !data_tree.MutationResult {
            const path = request.path orelse return .{ .code = .bad_arguments };
            const kind: ephemeral.NodeKind = if (ttl_ms) |ttl| blk: {
                if ((request.flags != 5 and request.flags != 6) or !ephemeral.isValidTtl(ttl)) {
                    return .{ .code = .bad_arguments };
                }
                break :blk .ttl;
            } else blk: {
                if (request.flags < 0 or request.flags > 4) return .{ .code = .bad_arguments };
                break :blk if (request.flags == 4)
                    .container
                else if ((request.flags & 1) != 0)
                    .ephemeral
                else
                    .persistent;
            };
            const sequential = (request.flags & 2) != 0;
            const parent_path = if (sequential)
                sequentialParentPath(path) orelse return .{ .code = .bad_arguments }
            else
                data_tree.parentPath(path) orelse return .{ .code = .bad_arguments };
            const node_data = request.data orelse &.{};
            if ((!sequential and (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/"))) or
                node_data.len > std.math.maxInt(i32))
            {
                return .{ .code = .bad_arguments };
            }
            ephemeral.validate(kind, switch (kind) {
                .persistent => 0,
                .ephemeral => session_id,
                .container => ephemeral.container_owner,
                .ttl => ephemeral.ttlOwner(ttl_ms.?) catch return .{ .code = .bad_arguments },
            }) catch return .{ .code = .bad_arguments };
            if (kind == .ephemeral) {
                const session = (try store.getSession(session_id)) orelse
                    return .{ .code = .session_expired };
                if (session.expires_at_ms <= store.session_clock_ms) return .{ .code = .session_expired };
            }
            const initial_parent = (try state.getNode(store, parent_path)) orelse
                return .{ .code = .no_node };
            if (!try acl.allows(initial_parent.acl, acl.create, identities)) return .{ .code = .no_auth };
            if (!ephemeral.permitsChildren(initial_parent.kind)) {
                return .{ .code = .no_children_for_ephemerals };
            }
            if (initial_parent.child_count == std.math.maxInt(i32) or
                initial_parent.cversion == std.math.maxInt(i32) or
                initial_parent.sequence_counter == std.math.maxInt(i32))
            {
                return .{ .code = .bad_arguments };
            }
            const owned_path = if (sequential)
                try sequentialPath(state.allocator, path, initial_parent.sequence_counter)
            else
                try state.allocator.dupe(u8, path);
            errdefer state.allocator.free(owned_path);
            if (!data_tree.isValidPath(owned_path)) return .{ .code = .bad_arguments };
            if ((try state.getNode(store, owned_path)) != null) return .{ .code = .node_exists };
            try state.nodes.ensureUnusedCapacity(1);
            const normalized_acl = acl.normalizeEncodedIdentities(
                state.allocator,
                request.acl,
                identities,
            ) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                else => return .{ .code = .invalid_acl },
            };
            errdefer state.allocator.free(normalized_acl);
            const owned_data = try state.allocator.dupe(u8, node_data);
            errdefer state.allocator.free(owned_data);
            try state.markDirty(parent_path);
            try state.markDirty(owned_path);
            state.clearDeleted(owned_path);
            const zxid = try store.clientZxid(index);
            const parent = state.nodes.getPtr(parent_path).?;
            parent.cversion += 1;
            parent.sequence_counter += 1;
            parent.pzxid = zxid;
            parent.child_count += 1;
            state.nodes.putAssumeCapacityNoClobber(owned_path, .{
                .data = owned_data,
                .czxid = zxid,
                .mzxid = zxid,
                .ctime = time_ms,
                .mtime = time_ms,
                .pzxid = zxid,
                .ephemeral_owner = switch (kind) {
                    .persistent => 0,
                    .ephemeral => session_id,
                    .container => ephemeral.container_owner,
                    .ttl => ephemeral.ttlOwner(ttl_ms.?) catch unreachable,
                },
                .kind = kind,
                .acl = normalized_acl,
            });
            return .{
                .code = .ok,
                .stat = state.nodes.getPtr(owned_path).?.stat(),
                .created_path = owned_path,
            };
        }

        fn delete(
            state: *MultiState,
            store: *RocksStore,
            request: protocol.proto.DeleteRequest,
            identities: ?[]const u8,
            index: u64,
        ) !data_tree.MutationResult {
            const path = request.path orelse return .{ .code = .bad_arguments };
            if (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/")) {
                return .{ .code = .bad_arguments };
            }
            const parent_path = data_tree.parentPath(path).?;
            const initial_parent = (try state.getNode(store, parent_path)) orelse
                return .{ .code = .no_node };
            if (!try acl.allows(initial_parent.acl, acl.delete, identities)) return .{ .code = .no_auth };
            const node = (try state.getNode(store, path)) orelse return .{ .code = .no_node };
            if (request.version != -1 and request.version != node.version) return .{ .code = .bad_version };
            if (node.child_count != 0) return .{ .code = .not_empty };
            const parent = state.nodes.getPtr(parent_path).?;
            if (parent.cversion == std.math.maxInt(i32)) return .{ .code = .bad_arguments };
            try state.markDirty(parent_path);
            try state.markDeleted(path);
            parent.cversion += 1;
            parent.pzxid = try store.clientZxid(index);
            parent.child_count -= 1;
            const removed = state.nodes.fetchRemove(path).?;
            state.allocator.free(removed.key);
            var removed_node = removed.value;
            removed_node.deinit(state.allocator);
            return .{ .code = .ok };
        }

        fn setData(
            state: *MultiState,
            store: *RocksStore,
            request: protocol.proto.SetDataRequest,
            time_ms: i64,
            identities: ?[]const u8,
            index: u64,
        ) !data_tree.MutationResult {
            const path = request.path orelse return .{ .code = .bad_arguments };
            const node = (try state.getNode(store, path)) orelse return .{ .code = .no_node };
            if (!try acl.allows(node.acl, acl.write, identities)) return .{ .code = .no_auth };
            if (request.version != -1 and request.version != node.version) return .{ .code = .bad_version };
            const node_data = request.data orelse &.{};
            if (node_data.len > std.math.maxInt(i32) or node.version == std.math.maxInt(i32)) {
                return .{ .code = .bad_arguments };
            }
            const replacement = try state.allocator.dupe(u8, node_data);
            errdefer state.allocator.free(replacement);
            try state.markDirty(path);
            state.allocator.free(node.data);
            node.data = replacement;
            node.mzxid = try store.clientZxid(index);
            node.mtime = time_ms;
            node.version += 1;
            return .{ .code = .ok, .stat = node.stat() };
        }

        fn check(
            state: *MultiState,
            store: *RocksStore,
            request: protocol.proto.CheckVersionRequest,
            identities: ?[]const u8,
        ) !data_tree.MutationResult {
            const path = request.path orelse return .{ .code = .bad_arguments };
            const node = (try state.getNode(store, path)) orelse return .{ .code = .no_node };
            if (!try acl.allows(node.acl, acl.read, identities)) return .{ .code = .no_auth };
            if (request.version != -1 and request.version != node.version) return .{ .code = .bad_version };
            return .{ .code = .ok };
        }

        fn commit(state: *MultiState, store: *RocksStore, applied: Applied) !void {
            var batch = rocksdb.WriteBatch.init();
            defer batch.deinit();
            var deleted = state.deleted.keyIterator();
            while (deleted.next()) |path| batch.delete(store.default_family, path.*);
            var encoded: std.ArrayList([]u8) = .empty;
            defer {
                for (encoded.items) |bytes| state.allocator.free(bytes);
                encoded.deinit(state.allocator);
            }
            var dirty = state.dirty.keyIterator();
            while (dirty.next()) |path| {
                const node = state.nodes.get(path.*) orelse return error.InvalidMultiState;
                const bytes = try encodeNode(state.allocator, node);
                encoded.append(state.allocator, bytes) catch |err| {
                    state.allocator.free(bytes);
                    return err;
                };
                batch.put(store.default_family, path.*, bytes);
            }
            putApplied(&batch, store.default_family, applied);
            try store.writeSync(batch);
        }
    };

    fn appendMultiCurrentEvent(
        self: *RocksStore,
        state: *MultiState,
        events: *std.ArrayList(data_tree.MutationEvent),
        event_type: data_tree.MutationEventType,
        path: []const u8,
    ) !void {
        const node = (try state.getNode(self, path)) orelse return error.InvalidMultiState;
        return appendMultiEvent(self.allocator, events, event_type, path, node.acl);
    }

    fn appendMultiEvent(
        allocator: std.mem.Allocator,
        events: *std.ArrayList(data_tree.MutationEvent),
        event_type: data_tree.MutationEventType,
        path: []const u8,
        acl_blob: ?[]const u8,
    ) !void {
        var event = data_tree.MutationEvent{
            .type = event_type,
            .path = try allocator.dupe(u8, path),
            .acl = null,
        };
        errdefer event.deinit(allocator);
        event.acl = if (acl_blob) |value| try allocator.dupe(u8, value) else null;
        try events.append(allocator, event);
    }

    fn encodeCommandResponse(
        self: *RocksStore,
        zxid: i64,
        body: []const u8,
    ) ![]u8 {
        var writer = jute.Writer.init(self.allocator);
        defer writer.deinit();
        try writer.writeInt(@intFromEnum(ErrorCode.ok));
        try writer.writeLong(zxid);
        try writer.writeBytes(body);
        return writer.toOwnedSlice();
    }

    fn applyMulti(
        self: *RocksStore,
        body: []const u8,
        time_ms: i64,
        session_id: i64,
        identities: ?[]const u8,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        var operation_count: usize = 0;
        var validation = multi.RequestIterator.init(body);
        while (try validation.next(self.allocator)) |value| {
            var operation = value;
            operation.deinit(self.allocator);
            operation_count = try std.math.add(usize, operation_count, 1);
        }

        var state = MultiState.load(self);
        defer state.deinit();
        var response = jute.Writer.init(self.allocator);
        defer response.deinit();
        var events: std.ArrayList(data_tree.MutationEvent) = .empty;
        defer {
            for (events.items) |*event| event.deinit(self.allocator);
            events.deinit(self.allocator);
        }
        var iterator = multi.RequestIterator.init(body);
        var operation_index: usize = 0;
        while (try iterator.next(self.allocator)) |value| : (operation_index += 1) {
            var operation = value;
            defer operation.deinit(self.allocator);
            var deleted_acl: ?[]u8 = null;
            defer if (deleted_acl) |value_acl| self.allocator.free(value_acl);
            switch (operation) {
                .delete => |request| if (request.path) |path| {
                    if (try state.getNode(self, path)) |node| {
                        deleted_acl = if (node.acl) |value_acl|
                            try self.allocator.dupe(u8, value_acl)
                        else
                            null;
                    }
                },
                else => {},
            }
            var result = try state.applyOperation(
                self,
                operation,
                time_ms,
                session_id,
                identities,
                index,
            );
            defer result.deinit(self.allocator);
            if (result.code != .ok) {
                response.truncate(0);
                var response_index: usize = 0;
                while (response_index < operation_count) : (response_index += 1) {
                    const code: ErrorCode = if (response_index < operation_index)
                        .ok
                    else if (response_index == operation_index)
                        result.code
                    else
                        .runtime_inconsistency;
                    try multi.writeHeader(&response, -1, false, @intFromEnum(code));
                    try jute.serialize(&response, protocol.proto.ErrorResponse{
                        .err = @intFromEnum(code),
                    });
                }
                try multi.writeTerminator(&response);
                const owned = try self.allocator.dupe(u8, response.bytes());
                errdefer self.allocator.free(owned);
                const command_response = try self.encodeCommandResponse(
                    try self.clientZxid(index),
                    owned,
                );
                errdefer self.allocator.free(command_response);
                try self.advanceApplied(index, term);
                return .{
                    .code = .ok,
                    .response_body = owned,
                    .owned_response_body = owned,
                    .command_response = command_response,
                    .owned_command_response = command_response,
                };
            }
            switch (operation) {
                .create, .create2, .create_container, .create_ttl => {
                    const path = result.created_path orelse return error.InvalidMultiState;
                    try self.appendMultiCurrentEvent(&state, &events, .node_created, path);
                    if (data_tree.parentPath(path)) |parent| {
                        try self.appendMultiCurrentEvent(
                            &state,
                            &events,
                            .node_children_changed,
                            parent,
                        );
                    }
                },
                .delete => |request| {
                    const path = request.path orelse return error.InvalidMultiState;
                    try appendMultiEvent(
                        self.allocator,
                        &events,
                        .node_deleted,
                        path,
                        deleted_acl,
                    );
                    if (data_tree.parentPath(path)) |parent| {
                        try self.appendMultiCurrentEvent(
                            &state,
                            &events,
                            .node_children_changed,
                            parent,
                        );
                    }
                },
                .set_data => |request| try self.appendMultiCurrentEvent(
                    &state,
                    &events,
                    .node_data_changed,
                    request.path orelse return error.InvalidMultiState,
                ),
                .check => {},
            }
            const kind: multi.Kind = operation;
            const response_kind: i32 = switch (operation) {
                .create2, .create_container, .create_ttl => @intFromEnum(multi.Kind.create2),
                else => @intFromEnum(kind),
            };
            try multi.writeHeader(&response, response_kind, false, 0);
            switch (operation) {
                .create => try jute.serialize(&response, protocol.proto.CreateResponse{
                    .path = result.created_path,
                }),
                .create2, .create_container, .create_ttl => try jute.serialize(
                    &response,
                    protocol.proto.Create2Response{
                        .path = result.created_path,
                        .stat = result.stat.?,
                    },
                ),
                .delete, .check => {},
                .set_data => try jute.serialize(&response, protocol.proto.SetDataResponse{
                    .stat = result.stat.?,
                }),
            }
        }
        try multi.writeTerminator(&response);
        const owned = try self.allocator.dupe(u8, response.bytes());
        errdefer self.allocator.free(owned);
        const command_response = try self.encodeCommandResponse(
            try self.clientZxid(index),
            owned,
        );
        errdefer self.allocator.free(command_response);
        const owned_events = try events.toOwnedSlice(self.allocator);
        errdefer {
            for (owned_events) |*event| event.deinit(self.allocator);
            self.allocator.free(owned_events);
        }
        try state.commit(self, .{ .index = index, .term = term });
        return .{
            .code = .ok,
            .response_body = owned,
            .owned_response_body = owned,
            .command_response = command_response,
            .owned_command_response = command_response,
            .events = owned_events,
        };
    }

    fn create(
        self: *RocksStore,
        path: []const u8,
        data: []const u8,
        time_ms: i64,
        kind: ephemeral.NodeKind,
        ttl_ms: i64,
        session_id: i64,
        sequential: bool,
        acl_blob: ?[]const u8,
        identities: ?[]const u8,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        const parent_path = if (sequential)
            sequentialParentPath(path) orelse return .{ .code = .bad_arguments }
        else
            data_tree.parentPath(path) orelse return .{ .code = .bad_arguments };
        if ((!sequential and (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/"))) or
            data.len > std.math.maxInt(i32))
        {
            return .{ .code = .bad_arguments };
        }
        if (acl_blob) |value| acl.validate(value) catch return .{ .code = .invalid_acl };
        if (kind != .ttl and ttl_ms != 0) return .{ .code = .bad_arguments };
        ephemeral.validate(kind, switch (kind) {
            .persistent => 0,
            .ephemeral => session_id,
            .container => ephemeral.container_owner,
            .ttl => ephemeral.ttlOwner(ttl_ms) catch return .{ .code = .bad_arguments },
        }) catch return .{ .code = .bad_arguments };
        if (kind == .ephemeral) {
            const session = (try self.getSession(session_id)) orelse
                return .{ .code = .session_expired };
            if (session.expires_at_ms <= self.session_clock_ms) return .{ .code = .session_expired };
        }
        const maybe_parent = try self.getNode(parent_path);
        if (maybe_parent == null) return .{ .code = .no_node };
        var parent = maybe_parent.?;
        defer parent.deinit(self.allocator);
        if (!try acl.allows(parent.acl, acl.create, identities)) return .{ .code = .no_auth };
        if (!ephemeral.permitsChildren(parent.kind)) return .{ .code = .no_children_for_ephemerals };
        if (parent.child_count == std.math.maxInt(i32) or
            parent.cversion == std.math.maxInt(i32) or
            parent.sequence_counter == std.math.maxInt(i32))
        {
            return .{ .code = .bad_arguments };
        }

        var owned_path: ?[]u8 = null;
        var transfer_path = false;
        defer if (!transfer_path) if (owned_path) |value| self.allocator.free(value);
        const resolved_path: []const u8 = if (sequential) blk: {
            owned_path = try sequentialPath(self.allocator, path, parent.sequence_counter);
            break :blk owned_path.?;
        } else path;
        if (!data_tree.isValidPath(resolved_path)) return .{ .code = .bad_arguments };
        if (try self.getNode(resolved_path)) |value| {
            var existing = value;
            existing.deinit(self.allocator);
            return .{ .code = .node_exists };
        }

        const zxid = try self.clientZxid(index);
        const node_data = try self.allocator.dupe(u8, data);
        var node_data_owned = true;
        defer if (node_data_owned) self.allocator.free(node_data);
        const node_acl = if (acl_blob) |value| try self.allocator.dupe(u8, value) else null;
        var node_acl_owned = true;
        defer if (node_acl_owned) if (node_acl) |value| self.allocator.free(value);
        var node = Node{
            .data = node_data,
            .czxid = zxid,
            .mzxid = zxid,
            .ctime = time_ms,
            .mtime = time_ms,
            .version = 0,
            .cversion = 0,
            .pzxid = zxid,
            .child_count = 0,
            .ephemeral_owner = switch (kind) {
                .persistent => 0,
                .ephemeral => session_id,
                .container => ephemeral.container_owner,
                .ttl => try ephemeral.ttlOwner(ttl_ms),
            },
            .kind = kind,
            .acl = node_acl,
        };
        node_data_owned = false;
        node_acl_owned = false;
        defer node.deinit(self.allocator);
        parent.cversion += 1;
        parent.sequence_counter += 1;
        parent.pzxid = zxid;
        parent.child_count += 1;
        try self.commitNodes(&.{
            .{ .path = resolved_path, .node = node },
            .{ .path = parent_path, .node = parent },
        }, null, .{ .index = index, .term = term });
        transfer_path = true;
        return .{
            .code = .ok,
            .stat = node.stat(),
            .created_path = resolved_path,
            .owned_created_path = owned_path,
        };
    }

    fn delete(
        self: *RocksStore,
        path: []const u8,
        expected_version: i32,
        identities: ?[]const u8,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        if (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/")) {
            return .{ .code = .bad_arguments };
        }
        const parent_path = data_tree.parentPath(path).?;
        var parent = (try self.getNode(parent_path)) orelse return .{ .code = .no_node };
        defer parent.deinit(self.allocator);
        if (!try acl.allows(parent.acl, acl.delete, identities)) return .{ .code = .no_auth };
        const maybe_node = try self.getNode(path);
        if (maybe_node == null) return .{ .code = .no_node };
        var node = maybe_node.?;
        defer node.deinit(self.allocator);
        if (expected_version != -1 and expected_version != node.version) return .{ .code = .bad_version };
        if (node.child_count != 0) return .{ .code = .not_empty };
        if (parent.cversion == std.math.maxInt(i32)) return .{ .code = .bad_arguments };
        parent.cversion += 1;
        parent.pzxid = try self.clientZxid(index);
        parent.child_count -= 1;
        try self.commitNodes(
            &.{.{ .path = parent_path, .node = parent }},
            path,
            .{ .index = index, .term = term },
        );
        return .{ .code = .ok };
    }

    fn deleteExtended(
        self: *RocksStore,
        path: []const u8,
        expected_czxid: i64,
        expected_kind: ephemeral.NodeKind,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        const maybe_node = try self.getNode(path);
        if (maybe_node == null) {
            try self.advanceApplied(index, term);
            return .{ .code = .ok };
        }
        var node = maybe_node.?;
        defer node.deinit(self.allocator);
        if (node.czxid != expected_czxid or node.kind != expected_kind or
            !extendedEligible(node, self.maintenance_clock_ms))
        {
            try self.advanceApplied(index, term);
            return .{ .code = .ok };
        }
        const parent_path = data_tree.parentPath(path) orelse {
            try self.advanceApplied(index, term);
            return .{ .code = .ok };
        };
        var parent = (try self.getNode(parent_path)) orelse {
            try self.advanceApplied(index, term);
            return .{ .code = .ok };
        };
        defer parent.deinit(self.allocator);
        if (parent.cversion == std.math.maxInt(i32) or parent.child_count == 0) {
            try self.advanceApplied(index, term);
            return .{ .code = .ok };
        }
        parent.cversion += 1;
        parent.child_count -= 1;
        parent.pzxid = try self.clientZxid(index);
        try self.commitNodes(
            &.{.{ .path = parent_path, .node = parent }},
            path,
            .{ .index = index, .term = term },
        );
        return .{ .code = .ok, .changed = true };
    }

    fn setAcl(
        self: *RocksStore,
        path: []const u8,
        acl_blob: []const u8,
        expected_version: i32,
        identities: ?[]const u8,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        acl.validate(acl_blob) catch return .{ .code = .invalid_acl };
        const maybe_node = try self.getNode(path);
        if (maybe_node == null) return .{ .code = .no_node };
        var node = maybe_node.?;
        defer node.deinit(self.allocator);
        if (!try acl.allows(node.acl, acl.admin, identities)) return .{ .code = .no_auth };
        if (expected_version != -1 and expected_version != node.aversion) return .{ .code = .bad_version };
        if (node.aversion == std.math.maxInt(i32)) return .{ .code = .bad_arguments };
        const replacement = try self.allocator.dupe(u8, acl_blob);
        if (node.acl) |value| self.allocator.free(value);
        node.acl = replacement;
        node.aversion += 1;
        try self.commitNodes(
            &.{.{ .path = path, .node = node }},
            null,
            .{ .index = index, .term = term },
        );
        return .{ .code = .ok, .stat = node.stat() };
    }

    fn setData(
        self: *RocksStore,
        path: []const u8,
        data: []const u8,
        expected_version: i32,
        time_ms: i64,
        identities: ?[]const u8,
        index: u64,
        term: u64,
    ) !data_tree.MutationResult {
        const maybe_node = try self.getNode(path);
        if (maybe_node == null) return .{ .code = .no_node };
        var node = maybe_node.?;
        defer node.deinit(self.allocator);
        if (!try acl.allows(node.acl, acl.write, identities)) return .{ .code = .no_auth };
        if (expected_version != -1 and expected_version != node.version) return .{ .code = .bad_version };
        if (data.len > std.math.maxInt(i32) or node.version == std.math.maxInt(i32)) {
            return .{ .code = .bad_arguments };
        }
        const replacement = try self.allocator.dupe(u8, data);
        self.allocator.free(node.data);
        node.data = replacement;
        node.mzxid = try self.clientZxid(index);
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

    fn loadZxidBase(self: *RocksStore) !?i64 {
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        const maybe_data = try self.db.get(self.default_family, zxid_base_key, &error_data);
        if (maybe_data) |data| {
            defer data.deinit();
            if (data.data.len != 8) return error.InvalidZxidBase;
            return std.mem.readInt(i64, data.data[0..8], .big);
        }
        return null;
    }

    fn loadMaintenanceClock(self: *RocksStore) !?i64 {
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        const maybe_data = try self.db.get(self.default_family, maintenance_clock_key, &error_data);
        if (maybe_data) |data| {
            defer data.deinit();
            if (data.data.len != 8) return error.InvalidMaintenanceClock;
            const value = std.mem.readInt(i64, data.data[0..8], .big);
            if (value < 0) return error.InvalidMaintenanceClock;
            return value;
        }
        return null;
    }

    fn loadSessionClock(self: *RocksStore) !?i64 {
        var error_data: ?rocksdb.Data = null;
        defer deinitErrorData(&error_data);
        const maybe_data = try self.db.get(self.default_family, session_clock_key, &error_data);
        if (maybe_data) |data| {
            defer data.deinit();
            if (data.data.len != 8) return error.InvalidSessionClock;
            const value = std.mem.readInt(i64, data.data[0..8], .big);
            if (value < 0) return error.InvalidSessionClock;
            return value;
        }
        return null;
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

fn extendedEligible(node: Node, maintenance_clock_ms: i64) bool {
    if (node.child_count != 0 or maintenance_clock_ms <= node.mtime) return false;
    const elapsed = maintenance_clock_ms - node.mtime;
    return switch (node.kind) {
        .container => node.cversion > 0 or elapsed > ephemeral.container_unused_timeout_ms,
        .ttl => elapsed > (ephemeral.ttlValue(node.ephemeral_owner) catch return false),
        else => false,
    };
}

fn sessionKey(session_id: i64, output: *[session_prefix.len + 8]u8) []const u8 {
    @memcpy(output[0..session_prefix.len], session_prefix);
    std.mem.writeInt(i64, output[session_prefix.len..][0..8], session_id, .big);
    return output;
}

fn decodeSessionKey(key: []const u8) !i64 {
    if (key.len != session_prefix.len + 8 or !std.mem.startsWith(u8, key, session_prefix)) {
        return error.InvalidSessionKey;
    }
    const session_id = std.mem.readInt(i64, key[session_prefix.len..][0..8], .big);
    if (session_id == 0) return error.InvalidSessionKey;
    return session_id;
}

fn encodeSession(session: Session, output: *[40]u8) void {
    @memcpy(output[0..16], &session.password);
    std.mem.writeInt(i32, output[16..20], session.timeout_ms, .big);
    std.mem.writeInt(i32, output[20..24], session.tick_grace_ms, .big);
    std.mem.writeInt(i64, output[24..32], session.expires_at_ms, .big);
    std.mem.writeInt(u64, output[32..40], session.generation, .big);
}

fn decodeSession(bytes: []const u8) !Session {
    if (bytes.len != 28 and bytes.len != 36 and bytes.len != 40) return error.InvalidSession;
    var password: [16]u8 = undefined;
    @memcpy(&password, bytes[0..16]);
    const timeout_ms = std.mem.readInt(i32, bytes[16..20], .big);
    const tick_grace_ms: i32 = if (bytes.len == 40)
        std.mem.readInt(i32, bytes[20..24], .big)
    else
        500;
    const expires_at_ms = if (bytes.len == 40)
        std.mem.readInt(i64, bytes[24..32], .big)
    else
        std.mem.readInt(i64, bytes[20..28], .big);
    const generation = if (bytes.len == 40)
        std.mem.readInt(u64, bytes[32..40], .big)
    else if (bytes.len == 36)
        std.mem.readInt(u64, bytes[28..36], .big)
    else
        1;
    if (timeout_ms <= 0 or tick_grace_ms <= 0 or expires_at_ms <= 0 or generation == 0) {
        return error.InvalidSession;
    }
    return .{
        .password = password,
        .timeout_ms = timeout_ms,
        .tick_grace_ms = tick_grace_ms,
        .expires_at_ms = expires_at_ms,
        .generation = generation,
    };
}

fn sequentialParentPath(path: []const u8) ?[]const u8 {
    if (path.len < 2 or path[0] != '/' or std.mem.indexOfScalar(u8, path, 0) != null or
        std.mem.indexOf(u8, path, "//") != null) return null;
    if (path[path.len - 1] == '/') {
        const parent = path[0 .. path.len - 1];
        return if (data_tree.isValidPath(parent)) parent else null;
    }
    return data_tree.parentPath(path);
}

fn sequentialPath(
    allocator: std.mem.Allocator,
    prefix: []const u8,
    sequence: i32,
) ![]u8 {
    if (sequence < 0) return error.InvalidSequence;
    const path = try allocator.alloc(u8, std.math.add(usize, prefix.len, 10) catch
        return error.LengthOverflow);
    @memcpy(path[0..prefix.len], prefix);
    var value: u32 = @intCast(sequence);
    var index: usize = path.len;
    while (index > prefix.len) {
        index -= 1;
        path[index] = '0' + @as(u8, @intCast(value % 10));
        value /= 10;
    }
    return path;
}

fn renewedExpiry(session_clock_ms: i64, timeout_ms: i32, tick_grace_ms: i32) !i64 {
    const lifetime = std.math.add(i64, timeout_ms, tick_grace_ms) catch
        return error.SessionClockOverflow;
    return std.math.add(i64, session_clock_ms, lifetime) catch error.SessionClockOverflow;
}

fn isSnapshotRecordKey(key: []const u8) bool {
    return data_tree.isValidPath(key) or std.mem.eql(u8, key, session_clock_key) or
        std.mem.eql(u8, key, zxid_base_key) or
        std.mem.eql(u8, key, maintenance_clock_key) or
        std.mem.startsWith(u8, key, session_prefix);
}

fn putZxidBase(
    batch: *const rocksdb.WriteBatch,
    family: rocksdb.ColumnFamilyHandle,
    value: i64,
) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &bytes, value, .big);
    batch.put(family, zxid_base_key, &bytes);
}

fn putMaintenanceClock(
    batch: *const rocksdb.WriteBatch,
    family: rocksdb.ColumnFamilyHandle,
    value: i64,
) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &bytes, value, .big);
    batch.put(family, maintenance_clock_key, &bytes);
}

fn putSessionClock(
    batch: *const rocksdb.WriteBatch,
    family: rocksdb.ColumnFamilyHandle,
    value: i64,
) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(i64, &bytes, value, .big);
    batch.put(family, session_clock_key, &bytes);
}

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
    defer writer.deinit();
    try writer.writeBuffer(if (node.data_is_null) null else node.data);
    try writer.writeLong(node.czxid);
    try writer.writeLong(node.mzxid);
    try writer.writeLong(node.ctime);
    try writer.writeLong(node.mtime);
    try writer.writeInt(node.version);
    try writer.writeInt(node.cversion);
    try writer.writeLong(node.pzxid);
    try writer.writeLong(@bitCast(node.child_count));
    try writer.writeLong(node.ephemeral_owner);
    try writer.writeInt(node.aversion);
    try writer.writeBuffer(node.acl);
    try writer.writeInt(node.sequence_counter);
    try writer.writeByte(@bitCast(@intFromEnum(node.kind)));
    return allocator.dupe(u8, writer.bytes());
}

fn decodeNode(allocator: std.mem.Allocator, bytes: []const u8) !Node {
    var reader = jute.Reader.init(bytes);
    const maybe_data = try reader.readBuffer();
    const data = try allocator.dupe(u8, maybe_data orelse &.{});
    errdefer allocator.free(data);
    const czxid = try reader.readLong();
    const mzxid = try reader.readLong();
    const ctime = try reader.readLong();
    const mtime = try reader.readLong();
    const version = try reader.readInt();
    const cversion = try reader.readInt();
    const pzxid = try reader.readLong();
    const child_count = std.math.cast(usize, @as(u64, @bitCast(try reader.readLong()))) orelse
        return error.InvalidNode;
    const ephemeral_owner = if (reader.remaining() == 0) 0 else try reader.readLong();
    const aversion = if (reader.remaining() == 0) 0 else try reader.readInt();
    const acl_blob = if (reader.remaining() == 0)
        null
    else if (try reader.readBuffer()) |value|
        try allocator.dupe(u8, value)
    else
        null;
    errdefer if (acl_blob) |value| allocator.free(value);
    const sequence_counter = if (reader.remaining() == 0) cversion else try reader.readInt();
    const kind: ephemeral.NodeKind = if (reader.remaining() == 0)
        if (ephemeral_owner == 0) .persistent else .ephemeral
    else
        ephemeral.kindFromByte(@bitCast(try reader.readByte())) orelse return error.InvalidNode;
    if (reader.remaining() != 0) return error.InvalidNode;
    ephemeral.validate(kind, ephemeral_owner) catch return error.InvalidNode;
    if (acl_blob) |value| acl.validate(value) catch return error.InvalidNode;
    return .{
        .data = data,
        .data_is_null = maybe_data == null,
        .czxid = czxid,
        .mzxid = mzxid,
        .ctime = ctime,
        .mtime = mtime,
        .version = version,
        .cversion = cversion,
        .sequence_counter = sequence_counter,
        .aversion = aversion,
        .pzxid = pzxid,
        .child_count = child_count,
        .ephemeral_owner = ephemeral_owner,
        .kind = kind,
        .acl = acl_blob,
    };
}

fn deinitFamilies(allocator: std.mem.Allocator, families: []const rocksdb.ColumnFamily) void {
    // DB.deinit owns the names through its column-family name map.
    allocator.free(families);
}

fn deinitErrorData(error_data: *?rocksdb.Data) void {
    if (error_data.*) |data| data.deinit();
    error_data.* = null;
}

test "node encoding appends kind while legacy nonzero owners remain ephemeral" {
    const testing = std.testing;
    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try writer.writeBuffer("");
    try writer.writeLong(1);
    try writer.writeLong(1);
    try writer.writeLong(1);
    try writer.writeLong(1);
    try writer.writeInt(0);
    try writer.writeInt(0);
    try writer.writeLong(1);
    try writer.writeLong(0);
    try writer.writeLong(-1);
    try writer.writeInt(0);
    try writer.writeBuffer(null);
    try writer.writeInt(0);
    var legacy = try decodeNode(testing.allocator, writer.bytes());
    defer legacy.deinit(testing.allocator);
    try testing.expectEqual(ephemeral.NodeKind.ephemeral, legacy.kind);

    legacy.kind = .container;
    legacy.ephemeral_owner = ephemeral.container_owner;
    const encoded = try encodeNode(testing.allocator, legacy);
    defer testing.allocator.free(encoded);
    var decoded = try decodeNode(testing.allocator, encoded);
    defer decoded.deinit(testing.allocator);
    try testing.expectEqual(ephemeral.NodeKind.container, decoded.kind);
    try testing.expectEqual(@as(i64, 0), decoded.stat().ephemeralOwner);
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

test "sequential creates use persistent parent cversion suffixes" {
    const testing = std.testing;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(path);

    {
        var store = try RocksStore.open(testing.allocator, path);
        defer store.deinit();
        var first = try store.apply(.{ .create = .{
            .path = "/member-",
            .data = "one",
            .time_ms = 1,
            .sequential = true,
        } }, 1, 1);
        defer first.deinit(testing.allocator);
        try testing.expectEqual(ErrorCode.ok, first.code);
        try testing.expectEqualStrings("/member-0000000000", first.created_path.?);

        var second = try store.apply(.{ .create = .{
            .path = "/member-",
            .data = "two",
            .time_ms = 2,
            .sequential = true,
        } }, 2, 1);
        defer second.deinit(testing.allocator);
        try testing.expectEqualStrings("/member-0000000001", second.created_path.?);

        var group = try store.apply(.{ .create = .{
            .path = "/group",
            .data = "",
            .time_ms = 3,
        } }, 3, 1);
        defer group.deinit(testing.allocator);
        var slash_prefix = try store.apply(.{ .create = .{
            .path = "/group/",
            .data = "child",
            .time_ms = 4,
            .sequential = true,
        } }, 4, 1);
        defer slash_prefix.deinit(testing.allocator);
        try testing.expectEqualStrings("/group/0000000000", slash_prefix.created_path.?);
    }

    {
        var store = try RocksStore.open(testing.allocator, path);
        defer store.deinit();
        var third = try store.apply(.{ .create = .{
            .path = "/member-",
            .data = "three",
            .time_ms = 3,
            .sequential = true,
        } }, 5, 1);
        defer third.deinit(testing.allocator);
        try testing.expectEqualStrings("/member-0000000003", third.created_path.?);
    }
}

test "container and TTL nodes use replicated lifecycle cleanup" {
    const testing = std.testing;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(path);
    var store = try RocksStore.open(testing.allocator, path);
    defer store.deinit();

    _ = try store.apply(.{ .session_tick = .{ .leader_term = 1, .elapsed_ms = 1, .leader_wall_ms = 1_000 } }, 1, 1);
    var container = try store.apply(.{ .create = .{
        .path = "/container",
        .data = "",
        .time_ms = 1_000,
        .node_kind = .container,
    } }, 2, 1);
    defer container.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 0), container.stat.?.ephemeralOwner);
    var child = try store.apply(.{ .create = .{ .path = "/container/child", .data = "", .time_ms = 1_000 } }, 3, 1);
    defer child.deinit(testing.allocator);
    _ = try store.apply(.{ .delete = .{ .path = "/container/child", .expected_version = -1 } }, 4, 1);
    var ttl = try store.apply(.{ .create = .{
        .path = "/ttl-",
        .data = "",
        .time_ms = 1_000,
        .sequential = true,
        .node_kind = .ttl,
        .ttl_ms = 10,
    } }, 5, 1);
    defer ttl.deinit(testing.allocator);
    try testing.expectEqualStrings("/ttl-0000000001", ttl.created_path.?);
    _ = try store.apply(.{ .session_tick = .{ .leader_term = 1, .elapsed_ms = 11, .leader_wall_ms = 9_999 } }, 6, 1);

    const candidates = try store.expiredExtendedNodes(testing.allocator, 16);
    defer {
        for (candidates) |candidate| testing.allocator.free(candidate.path);
        testing.allocator.free(candidates);
    }
    try testing.expectEqual(@as(usize, 2), candidates.len);
    const candidate = for (candidates) |value| {
        if (std.mem.eql(u8, value.path, "/container")) break value;
    } else return error.MissingCandidate;
    var stale_delete = try store.apply(.{ .delete_extended = .{
        .path = candidate.path,
        .expected_czxid = candidate.czxid + 1,
        .expected_kind = candidate.kind,
    } }, 7, 1);
    defer stale_delete.deinit(testing.allocator);
    try testing.expect(!stale_delete.changed);
    try testing.expect((try store.exists("/container")) != null);
    var applied_delete = try store.apply(.{ .delete_extended = .{
        .path = candidate.path,
        .expected_czxid = candidate.czxid,
        .expected_kind = candidate.kind,
    } }, 8, 1);
    defer applied_delete.deinit(testing.allocator);
    try testing.expect(applied_delete.changed);
    try testing.expect((try store.exists("/container")) == null);

    const snapshot_bytes = try store.snapshot(testing.allocator);
    defer testing.allocator.free(snapshot_bytes);
    var restored_directory = std.testing.tmpDir(.{});
    defer restored_directory.cleanup();
    const restored_path = try restored_directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(restored_path);
    var restored = try RocksStore.open(testing.allocator, restored_path);
    defer restored.deinit();
    try restored.restore(snapshot_bytes, 8, 1);
    const restored_candidates = try restored.expiredExtendedNodes(testing.allocator, 16);
    defer {
        for (restored_candidates) |value| testing.allocator.free(value.path);
        testing.allocator.free(restored_candidates);
    }
    try testing.expectEqual(@as(usize, 1), restored_candidates.len);
    try testing.expectEqual(ephemeral.NodeKind.ttl, restored_candidates[0].kind);
}

test "ACL authorization and snapshot restore preserve digest ownership" {
    const testing = std.testing;
    var source_directory = std.testing.tmpDir(.{});
    defer source_directory.cleanup();
    var target_directory = std.testing.tmpDir(.{});
    defer target_directory.cleanup();
    const source_path = try source_directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(source_path);
    const target_path = try target_directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(target_path);

    const digest_id = try acl.digestIdentity(testing.allocator, "owner:secret");
    defer testing.allocator.free(digest_id);
    const identities = [_]acl.Identity{.{ .scheme = "digest", .id = digest_id }};
    const identities_blob = try acl.encodeIdentities(testing.allocator, &identities);
    defer testing.allocator.free(identities_blob);
    const secure_acl = [_]acl.Entry{.{
        .perms = acl.all,
        .scheme = "digest",
        .id = digest_id,
    }};
    const acl_blob = try acl.encode(testing.allocator, &secure_acl);
    defer testing.allocator.free(acl_blob);

    var source = try RocksStore.open(testing.allocator, source_path);
    defer source.deinit();
    var created = try source.apply(.{ .create = .{
        .path = "/secure",
        .data = "private",
        .time_ms = 1,
        .acl = acl_blob,
        .identities = identities_blob,
    } }, 1, 1);
    defer created.deinit(testing.allocator);
    try testing.expectEqual(ErrorCode.no_auth, try source.authorize("/secure", acl.read, null));
    try testing.expectEqual(ErrorCode.ok, try source.authorize("/secure", acl.read, identities_blob));

    const read_admin_acl = [_]acl.Entry{.{
        .perms = acl.read | acl.admin,
        .scheme = "digest",
        .id = digest_id,
    }};
    const replacement = try acl.encode(testing.allocator, &read_admin_acl);
    defer testing.allocator.free(replacement);
    var updated = try source.apply(.{ .set_acl = .{
        .path = "/secure",
        .acl = replacement,
        .expected_version = 0,
        .identities = identities_blob,
    } }, 2, 1);
    defer updated.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 1), updated.stat.?.aversion);

    const snapshot = try source.snapshot(testing.allocator);
    defer testing.allocator.free(snapshot);
    var target = try RocksStore.open(testing.allocator, target_path);
    defer target.deinit();
    try target.restore(snapshot, 2, 1);
    try testing.expectEqual(ErrorCode.no_auth, try target.authorize("/secure", acl.read, null));
    try testing.expectEqual(ErrorCode.ok, try target.authorize("/secure", acl.admin, identities_blob));
}

test "multi commits atomically and rolls back on failure" {
    const testing = std.testing;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(path);
    var store = try RocksStore.open(testing.allocator, path);
    defer store.deinit();
    const open_acl = [_]protocol.data.ACL{.{
        .perms = acl.all,
        .id = .{ .scheme = "world", .id = "anyone" },
    }};

    var success_request = jute.Writer.init(testing.allocator);
    defer success_request.deinit();
    try multi.writeHeader(&success_request, @intFromEnum(multi.Kind.create), false, -1);
    try jute.serialize(&success_request, protocol.proto.CreateRequest{
        .path = "/multi",
        .data = "first",
        .acl = &open_acl,
        .flags = 0,
    });
    try multi.writeHeader(&success_request, @intFromEnum(multi.Kind.set_data), false, -1);
    try jute.serialize(&success_request, protocol.proto.SetDataRequest{
        .path = "/multi",
        .data = "second",
        .version = 0,
    });
    try multi.writeHeader(&success_request, @intFromEnum(multi.Kind.check), false, -1);
    try jute.serialize(&success_request, protocol.proto.CheckVersionRequest{
        .path = "/multi",
        .version = 1,
    });
    try multi.writeHeader(&success_request, @intFromEnum(multi.Kind.create_ttl), false, -1);
    try jute.serialize(&success_request, protocol.proto.CreateTTLRequest{
        .path = "/multi-ttl",
        .data = "ttl",
        .acl = &open_acl,
        .flags = 5,
        .ttl = 100,
    });
    try multi.writeTerminator(&success_request);
    var success = try store.apply(.{ .multi = .{
        .body = success_request.bytes(),
        .time_ms = 10,
        .session_id = 0,
        .session_generation = 0,
    } }, 1, 1);
    defer success.deinit(testing.allocator);
    try testing.expectEqual(ErrorCode.ok, success.code);
    const stored = (try store.getData(testing.allocator, "/multi")).?;
    defer if (stored.data) |value| testing.allocator.free(value);
    try testing.expectEqualStrings("second", stored.data.?);
    try testing.expectEqual(@as(i32, 1), stored.stat.version);
    var ttl_node = (try store.getNode("/multi-ttl")).?;
    defer ttl_node.deinit(testing.allocator);
    try testing.expectEqual(ephemeral.NodeKind.ttl, ttl_node.kind);
    try testing.expectEqual(@as(usize, 5), success.events.?.len);
    try testing.expectEqual(data_tree.MutationEventType.node_created, success.events.?[0].type);
    try testing.expectEqualStrings("/multi", success.events.?[0].path);
    try testing.expectEqual(data_tree.MutationEventType.node_data_changed, success.events.?[2].type);
    try testing.expectEqualStrings("/multi", success.events.?[2].path);
    try testing.expectEqualStrings("/multi-ttl", success.events.?[3].path);
    const success_command = try command.decodeResult(success.command_response.?);
    try testing.expectEqual(ErrorCode.ok, success_command.code);
    try testing.expectEqualSlices(u8, success.response_body.?, success_command.body);

    var rollback_request = jute.Writer.init(testing.allocator);
    defer rollback_request.deinit();
    try multi.writeHeader(&rollback_request, @intFromEnum(multi.Kind.create), false, -1);
    try jute.serialize(&rollback_request, protocol.proto.CreateRequest{
        .path = "/rolled-back",
        .data = "temporary",
        .acl = &open_acl,
        .flags = 0,
    });
    try multi.writeHeader(&rollback_request, @intFromEnum(multi.Kind.set_data), false, -1);
    try jute.serialize(&rollback_request, protocol.proto.SetDataRequest{
        .path = "/missing",
        .data = "failure",
        .version = -1,
    });
    try multi.writeHeader(&rollback_request, @intFromEnum(multi.Kind.delete), false, -1);
    try jute.serialize(&rollback_request, protocol.proto.DeleteRequest{
        .path = "/multi",
        .version = -1,
    });
    try multi.writeTerminator(&rollback_request);
    var rollback = try store.apply(.{ .multi = .{
        .body = rollback_request.bytes(),
        .time_ms = 11,
        .session_id = 0,
        .session_generation = 0,
    } }, 2, 1);
    defer rollback.deinit(testing.allocator);
    try testing.expectEqual(ErrorCode.ok, rollback.code);
    try testing.expectEqual(@as(?[]data_tree.MutationEvent, null), rollback.events);
    const rollback_command = try command.decodeResult(rollback.command_response.?);
    try testing.expectEqualSlices(u8, rollback.response_body.?, rollback_command.body);
    try testing.expect((try store.exists("/rolled-back")) == null);
    try testing.expect((try store.exists("/multi")) != null);

    var response = jute.Reader.init(rollback.response_body.?);
    const create_header = try jute.deserialize(protocol.proto.MultiHeader, &response, testing.allocator);
    try testing.expectEqual(@as(i32, -1), create_header.type);
    try testing.expectEqual(@as(i32, @intFromEnum(ErrorCode.ok)), create_header.err);
    const create_response = try jute.deserialize(protocol.proto.ErrorResponse, &response, testing.allocator);
    try testing.expectEqual(@as(i32, @intFromEnum(ErrorCode.ok)), create_response.err);
    const failure_header = try jute.deserialize(protocol.proto.MultiHeader, &response, testing.allocator);
    try testing.expectEqual(@as(i32, -1), failure_header.type);
    try testing.expectEqual(@as(i32, @intFromEnum(ErrorCode.no_node)), failure_header.err);
    const failure = try jute.deserialize(protocol.proto.ErrorResponse, &response, testing.allocator);
    try testing.expectEqual(@as(i32, @intFromEnum(ErrorCode.no_node)), failure.err);
    const skipped_header = try jute.deserialize(protocol.proto.MultiHeader, &response, testing.allocator);
    try testing.expectEqual(@as(i32, @intFromEnum(ErrorCode.runtime_inconsistency)), skipped_header.err);
}

test "stale session expiration reports no znode change" {
    const testing = std.testing;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(path);
    var store = try RocksStore.open(testing.allocator, path);
    defer store.deinit();

    const password = [_]u8{7} ** 16;
    var opened = try store.apply(.{ .open_session = .{
        .session_id = 42,
        .password = &password,
        .timeout_ms = 1_000,
        .tick_grace_ms = 100,
        .generation = 1,
    } }, 1, 1);
    defer opened.deinit(testing.allocator);
    const session = (try store.getSession(42)).?;

    var stale = try store.apply(.{ .expire_session = .{
        .session_id = 42,
        .expected_expires_at_ms = session.expires_at_ms + 1,
    } }, 2, 1);
    defer stale.deinit(testing.allocator);
    try testing.expect(!stale.changed);
    try testing.expect((try store.getSession(42)) != null);

    var tick = try store.apply(.{ .session_tick = .{
        .leader_term = 1,
        .elapsed_ms = 1_100,
        .leader_wall_ms = 1_100,
    } }, 3, 1);
    defer tick.deinit(testing.allocator);
    var expired = try store.apply(.{ .expire_session = .{
        .session_id = 42,
        .expected_expires_at_ms = session.expires_at_ms,
    } }, 4, 1);
    defer expired.deinit(testing.allocator);
    try testing.expect(expired.changed);
    try testing.expect((try store.getSession(42)) == null);
}

test "multi watch events retain intermediate state and deletion ACLs" {
    const testing = std.testing;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(path);
    var store = try RocksStore.open(testing.allocator, path);
    defer store.deinit();

    const denied_acl = try acl.encode(testing.allocator, &.{.{
        .perms = acl.write,
        .scheme = "world",
        .id = "anyone",
    }});
    defer testing.allocator.free(denied_acl);
    var created = try store.apply(.{ .create = .{
        .path = "/secure",
        .data = "value",
        .time_ms = 1,
        .acl = denied_acl,
    } }, 1, 1);
    defer created.deinit(testing.allocator);

    var delete_request = jute.Writer.init(testing.allocator);
    defer delete_request.deinit();
    try multi.writeHeader(&delete_request, @intFromEnum(multi.Kind.delete), false, -1);
    try jute.serialize(&delete_request, protocol.proto.DeleteRequest{
        .path = "/secure",
        .version = -1,
    });
    try multi.writeTerminator(&delete_request);
    var deleted = try store.apply(.{ .multi = .{
        .body = delete_request.bytes(),
        .time_ms = 2,
        .session_id = 0,
        .session_generation = 0,
    } }, 2, 1);
    defer deleted.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 2), deleted.events.?.len);
    try testing.expectEqual(data_tree.MutationEventType.node_deleted, deleted.events.?[0].type);
    try testing.expectEqualStrings("/secure", deleted.events.?[0].path);
    try testing.expect(!try acl.allows(deleted.events.?[0].acl, acl.read, null));

    const open_acl = [_]protocol.data.ACL{.{
        .perms = acl.all,
        .id = .{ .scheme = "world", .id = "anyone" },
    }};
    var transient_request = jute.Writer.init(testing.allocator);
    defer transient_request.deinit();
    try multi.writeHeader(&transient_request, @intFromEnum(multi.Kind.create), false, -1);
    try jute.serialize(&transient_request, protocol.proto.CreateRequest{
        .path = "/transient",
        .data = "one",
        .acl = &open_acl,
        .flags = 0,
    });
    try multi.writeHeader(&transient_request, @intFromEnum(multi.Kind.set_data), false, -1);
    try jute.serialize(&transient_request, protocol.proto.SetDataRequest{
        .path = "/transient",
        .data = "two",
        .version = 0,
    });
    try multi.writeHeader(&transient_request, @intFromEnum(multi.Kind.delete), false, -1);
    try jute.serialize(&transient_request, protocol.proto.DeleteRequest{
        .path = "/transient",
        .version = 1,
    });
    try multi.writeTerminator(&transient_request);
    var transient = try store.apply(.{ .multi = .{
        .body = transient_request.bytes(),
        .time_ms = 3,
        .session_id = 0,
        .session_generation = 0,
    } }, 3, 1);
    defer transient.deinit(testing.allocator);
    try testing.expect((try store.exists("/transient")) == null);
    try testing.expectEqual(@as(usize, 5), transient.events.?.len);
    try testing.expectEqual(data_tree.MutationEventType.node_created, transient.events.?[0].type);
    try testing.expectEqual(data_tree.MutationEventType.node_data_changed, transient.events.?[2].type);
    try testing.expectEqual(data_tree.MutationEventType.node_deleted, transient.events.?[3].type);
}
