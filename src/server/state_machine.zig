const std = @import("std");
const raft = @import("raftz");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");
const acl = @import("acl.zig");
const command = @import("command.zig");
const data_tree = @import("data_tree.zig");
const multi = @import("multi.zig");
const rocks_store = @import("rocks_store.zig");
const watch = @import("watch.zig");

pub const max_snapshot_bytes = rocks_store.max_snapshot_bytes;

pub const DataResult = struct {
    data: ?[]u8,
    stat: protocol.data.Stat,

    pub fn deinit(self: *DataResult, allocator: std.mem.Allocator) void {
        if (self.data) |value| allocator.free(value);
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
    watch_manager: ?*watch.Manager = null,

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

    pub fn attachWatchManager(self: *ZooKeeperStateMachine, manager: *watch.Manager) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(self.watch_manager == null);
        self.watch_manager = manager;
    }

    pub fn detachWatchManager(self: *ZooKeeperStateMachine, manager: *watch.Manager) void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        if (self.watch_manager == manager) self.watch_manager = null;
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
        return self.existsAuthorizedWatchingForSession(
            session_id,
            generation,
            path,
            identities,
            null,
        );
    }

    pub fn existsAuthorizedWatchingForSession(
        self: *ZooKeeperStateMachine,
        session_id: i64,
        generation: u64,
        path: []const u8,
        identities: ?[]const u8,
        registration: ?watch.RegistrationContext,
    ) !?protocol.data.Stat {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.validateSessionLocked(session_id, generation);
        switch (try self.store.authorize(path, acl.read, identities)) {
            .ok => {
                try self.registerWatchLocked(registration, .data, path);
                return self.store.exists(path);
            },
            .no_node => {
                try self.registerWatchLocked(registration, .data, path);
                return null;
            },
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
        return self.getDataAuthorizedWatchingForSession(
            allocator,
            session_id,
            generation,
            path,
            identities,
            null,
        );
    }

    pub fn getDataAuthorizedWatchingForSession(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        session_id: i64,
        generation: u64,
        path: []const u8,
        identities: ?[]const u8,
        registration: ?watch.RegistrationContext,
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
        errdefer if (result.data) |data| allocator.free(data);
        try self.registerWatchLocked(registration, .data, path);
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
        return self.getChildrenAuthorizedWatchingForSession(
            allocator,
            session_id,
            generation,
            path,
            identities,
            null,
        );
    }

    pub fn getChildrenAuthorizedWatchingForSession(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        session_id: i64,
        generation: u64,
        path: []const u8,
        identities: ?[]const u8,
        registration: ?watch.RegistrationContext,
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
        errdefer {
            for (names) |name| allocator.free(name);
            allocator.free(names);
        }
        try self.registerWatchLocked(registration, .children, path);
        return .{ .names = names, .stat = stat };
    }

    pub fn multiReadAuthorizedForSession(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        session_id: i64,
        generation: u64,
        identities: ?[]const u8,
        request_body: []const u8,
        response_body_limit: usize,
    ) ![]u8 {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.validateSessionLocked(session_id, generation);

        var response = jute.Writer.init(allocator);
        defer response.deinit();
        try multi.ensureReadResponseRoom(0, 0, response_body_limit);
        var iterator = multi.ReadRequestIterator.init(request_body);
        while (try iterator.next()) |operation| {
            const path = operation.path() orelse {
                try multi.ensureReadResponseRoom(
                    response.dataSize(),
                    multi.error_result_size,
                    response_body_limit,
                );
                try multi.writeErrorResult(&response, @intFromEnum(data_tree.ErrorCode.bad_arguments));
                continue;
            };
            const authorization = try self.store.authorize(path, acl.read, identities);
            if (authorization != .ok) {
                try multi.ensureReadResponseRoom(
                    response.dataSize(),
                    multi.error_result_size,
                    response_body_limit,
                );
                try multi.writeErrorResult(&response, @intFromEnum(authorization));
                continue;
            }
            switch (operation) {
                .get_data => {
                    const result = (try self.store.getData(allocator, path)) orelse {
                        try multi.ensureReadResponseRoom(
                            response.dataSize(),
                            multi.error_result_size,
                            response_body_limit,
                        );
                        try multi.writeErrorResult(&response, @intFromEnum(data_tree.ErrorCode.no_node));
                        continue;
                    };
                    defer if (result.data) |data| allocator.free(data);
                    const operation_size = std.math.add(
                        usize,
                        multi.get_data_result_base_size,
                        if (result.data) |data| data.len else 0,
                    ) catch return error.MultiReadResponseTooLarge;
                    try multi.ensureReadResponseRoom(
                        response.dataSize(),
                        operation_size,
                        response_body_limit,
                    );
                    try multi.writeHeader(
                        &response,
                        @intFromEnum(multi.ReadKind.get_data),
                        false,
                        0,
                    );
                    try jute.serialize(&response, protocol.proto.GetDataResponse{
                        .data = result.data,
                        .stat = result.stat,
                    });
                },
                .get_children => {
                    const maybe_names = try self.store.getChildren(allocator, path);
                    if (maybe_names == null) {
                        try multi.ensureReadResponseRoom(
                            response.dataSize(),
                            multi.error_result_size,
                            response_body_limit,
                        );
                        try multi.writeErrorResult(&response, @intFromEnum(data_tree.ErrorCode.no_node));
                        continue;
                    }
                    const names = maybe_names.?;
                    defer {
                        for (names) |name| allocator.free(name);
                        allocator.free(names);
                    }
                    var operation_size = multi.get_children_result_base_size;
                    for (names) |name| {
                        operation_size = std.math.add(usize, operation_size, 4) catch
                            return error.MultiReadResponseTooLarge;
                        operation_size = std.math.add(usize, operation_size, name.len) catch
                            return error.MultiReadResponseTooLarge;
                    }
                    try multi.ensureReadResponseRoom(
                        response.dataSize(),
                        operation_size,
                        response_body_limit,
                    );
                    const children = try allocator.alloc(?[]const u8, names.len);
                    defer allocator.free(children);
                    for (names, 0..) |name, index| children[index] = name;
                    try multi.writeHeader(
                        &response,
                        @intFromEnum(multi.ReadKind.get_children),
                        false,
                        0,
                    );
                    try jute.serialize(&response, protocol.proto.GetChildrenResponse{
                        .children = children,
                    });
                },
            }
        }
        try multi.writeTerminator(&response);
        return response.toOwnedSlice();
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

    pub fn expiredExtendedNodes(
        self: *ZooKeeperStateMachine,
        allocator: std.mem.Allocator,
        limit: usize,
    ) ![]rocks_store.ExtendedCandidate {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.store.expiredExtendedNodes(allocator, limit);
    }

    pub fn restoreWatchesForSession(
        self: *ZooKeeperStateMachine,
        session_id: i64,
        generation: u64,
        relative_zxid: i64,
        data_watches: ?[]const ?[]const u8,
        exist_watches: ?[]const ?[]const u8,
        child_watches: ?[]const ?[]const u8,
        persistent_watches: ?[]const ?[]const u8,
        persistent_recursive_watches: ?[]const ?[]const u8,
        registration: watch.RegistrationContext,
    ) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        try self.validateSessionLocked(session_id, generation);
        for (data_watches orelse &.{}) |maybe_path| {
            const path = maybe_path orelse return error.InvalidWatchPath;
            const stat = try self.store.exists(path);
            if (stat == null) {
                try self.registerImmediateWatchLocked(
                    registration,
                    .data,
                    path,
                    .node_deleted,
                );
            } else if (stat.?.mzxid > relative_zxid) {
                try self.registerImmediateWatchLocked(
                    registration,
                    .data,
                    path,
                    .node_data_changed,
                );
            } else {
                try self.registerWatchLocked(registration, .data, path);
            }
        }
        for (exist_watches orelse &.{}) |maybe_path| {
            const path = maybe_path orelse return error.InvalidWatchPath;
            if (try self.store.exists(path) != null) {
                try self.registerImmediateWatchLocked(
                    registration,
                    .data,
                    path,
                    .node_created,
                );
            } else {
                try self.registerWatchLocked(registration, .data, path);
            }
        }
        for (child_watches orelse &.{}) |maybe_path| {
            const path = maybe_path orelse return error.InvalidWatchPath;
            const stat = try self.store.exists(path);
            if (stat == null) {
                try self.registerImmediateWatchLocked(
                    registration,
                    .children,
                    path,
                    .node_deleted,
                );
            } else if (stat.?.pzxid > relative_zxid) {
                try self.registerImmediateWatchLocked(
                    registration,
                    .children,
                    path,
                    .node_children_changed,
                );
            } else {
                try self.registerWatchLocked(registration, .children, path);
            }
        }
        for (persistent_watches orelse &.{}) |maybe_path| {
            const path = maybe_path orelse return error.InvalidWatchPath;
            try self.registerWatchLocked(registration, .persistent, path);
        }
        for (persistent_recursive_watches orelse &.{}) |maybe_path| {
            const path = maybe_path orelse return error.InvalidWatchPath;
            try self.registerWatchLocked(registration, .persistent_recursive, path);
        }
    }

    fn registerWatchLocked(
        self: *ZooKeeperStateMachine,
        registration: ?watch.RegistrationContext,
        kind: watch.Kind,
        path: []const u8,
    ) !void {
        const value = registration orelse return;
        const manager = self.watch_manager orelse return error.WatchManagerUnavailable;
        try manager.register(value.connection_id, value.batch_id, kind, path);
    }

    fn registerImmediateWatchLocked(
        self: *ZooKeeperStateMachine,
        registration: watch.RegistrationContext,
        kind: watch.Kind,
        path: []const u8,
        event_type: watch.EventType,
    ) !void {
        const manager = self.watch_manager orelse return error.WatchManagerUnavailable;
        try manager.registerImmediate(
            registration.connection_id,
            registration.batch_id,
            kind,
            path,
            event_type,
            -1,
        );
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

    pub fn clientZxid(self: *ZooKeeperStateMachine) i64 {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        return self.store.clientZxid(self.store.durableApplied().index) catch std.math.maxInt(i64);
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
        var error_response: ?[]u8 = self.allocator.alloc(u8, 12) catch
            return error.OutOfMemory;
        var success_response: ?[]u8 = null;
        if (mutation != .multi) {
            const success_size = mutationSuccessResponseSize(mutation) catch {
                self.allocator.free(error_response.?);
                return error.Fatal;
            };
            success_response = self.allocator.alloc(u8, success_size) catch {
                self.allocator.free(error_response.?);
                return error.OutOfMemory;
            };
        }
        defer if (error_response) |response| self.allocator.free(response);
        defer if (success_response) |response| self.allocator.free(response);

        spinLock(&self.mutex);
        defer self.mutex.unlock();
        const zxid = self.store.clientZxid(entry.index) catch return error.Fatal;
        const deleted_acl: ?rocks_store.AclResult = if (self.watch_manager != null) switch (mutation) {
            .delete => |request| self.store.getAcl(self.allocator, request.path) catch |err|
                return mapStoreError(err),
            .delete_extended => |request| self.store.getAcl(self.allocator, request.path) catch |err|
                return mapStoreError(err),
            else => null,
        } else null;
        defer if (deleted_acl) |snapshot| if (snapshot.blob) |blob| self.allocator.free(blob);
        const removed_ephemeral_paths: ?[][]u8 = if (self.watch_manager != null) switch (mutation) {
            .close_session => |request| self.store.ephemeralPaths(
                self.allocator,
                request.session_id,
            ) catch |err| return mapStoreError(err),
            .expire_session => |request| self.store.ephemeralPaths(
                self.allocator,
                request.session_id,
            ) catch |err| return mapStoreError(err),
            else => null,
        } else null;
        defer if (removed_ephemeral_paths) |paths| {
            for (paths) |path| self.allocator.free(path);
            self.allocator.free(paths);
        };
        const removed_ephemeral_acls: ?[]?[]u8 = if (removed_ephemeral_paths) |paths| blk: {
            const snapshots = self.allocator.alloc(?[]u8, paths.len) catch return error.OutOfMemory;
            errdefer self.allocator.free(snapshots);
            var initialized: usize = 0;
            errdefer for (snapshots[0..initialized]) |blob| if (blob) |value| self.allocator.free(value);
            for (paths, snapshots) |path, *snapshot| {
                const current = self.store.getAcl(self.allocator, path) catch |err|
                    return mapStoreError(err);
                snapshot.* = if (current) |value| value.blob else null;
                initialized += 1;
            }
            break :blk snapshots;
        } else null;
        defer if (removed_ephemeral_acls) |snapshots| {
            for (snapshots) |blob| if (blob) |value| self.allocator.free(value);
            self.allocator.free(snapshots);
        };
        var result = self.store.apply(mutation, entry.index, entry.term) catch |err|
            return mapStoreError(err);
        defer result.deinit(self.allocator);

        if (result.code == .ok) {
            if (self.watch_manager) |manager| {
                publishMutationEvents(
                    manager,
                    &self.store,
                    self.allocator,
                    mutation,
                    result,
                    zxid,
                    removed_ephemeral_paths orelse &.{},
                    removed_ephemeral_acls orelse &.{},
                    if (deleted_acl) |snapshot| snapshot.blob else null,
                ) catch manager.invalidateAll();
            }
        }
        if (mutation == .multi and result.code == .ok) {
            const response = result.owned_command_response orelse return error.Fatal;
            result.command_response = null;
            result.owned_command_response = null;
            return .{ .response = response };
        }

        const response = if (result.code == .ok) blk: {
            const value = success_response.?;
            success_response = null;
            break :blk value;
        } else blk: {
            const value = error_response.?;
            error_response = null;
            break :blk value;
        };
        var writer = jute.Writer.initOwnedBuffer(self.allocator, response);
        defer writer.deinit();
        writer.writeInt(@intFromEnum(result.code)) catch unreachable;
        writer.writeLong(zxid) catch unreachable;
        if (result.code == .ok) switch (mutation) {
            .create => |value| jute.serialize(&writer, protocol.proto.Create2Response{
                .path = result.created_path orelse value.path,
                .stat = result.stat.?,
            }) catch unreachable,
            .delete, .open_session, .touch_session, .close_session, .expire_session, .move_session, .session_tick, .delete_extended => {},
            .set_acl => jute.serialize(&writer, protocol.proto.SetACLResponse{
                .stat = result.stat.?,
            }) catch unreachable,
            .set_data => jute.serialize(&writer, protocol.proto.SetDataResponse{
                .stat = result.stat.?,
            }) catch unreachable,
            .multi => unreachable,
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

fn publishMutationEvents(
    manager: *watch.Manager,
    store: *rocks_store.RocksStore,
    allocator: std.mem.Allocator,
    mutation: command.Mutation,
    result: data_tree.MutationResult,
    zxid: i64,
    removed_ephemeral_paths: []const []const u8,
    removed_ephemeral_acls: []const ?[]u8,
    deleted_acl: ?[]const u8,
) !void {
    switch (mutation) {
        .create => |request| try publishCreated(
            manager,
            store,
            allocator,
            zxid,
            result.created_path orelse request.path,
        ),
        .delete => |request| try publishDeleted(
            manager,
            store,
            allocator,
            zxid,
            request.path,
            deleted_acl,
        ),
        .set_data => |request| try publishCurrentEvent(
            manager,
            store,
            allocator,
            .node_data_changed,
            zxid,
            request.path,
        ),
        .delete_extended => |request| {
            if (result.changed) try publishDeleted(
                manager,
                store,
                allocator,
                zxid,
                request.path,
                deleted_acl,
            );
        },
        .multi => {
            for (result.events orelse &.{}) |event| {
                manager.publish(.{
                    .type = switch (event.type) {
                        .node_created => .node_created,
                        .node_deleted => .node_deleted,
                        .node_data_changed => .node_data_changed,
                        .node_children_changed => .node_children_changed,
                    },
                    .zxid = zxid,
                    .path = event.path,
                    .acl = event.acl,
                });
            }
        },
        .close_session => |request| {
            if (result.changed) {
                manager.invalidateSession(request.session_id, request.generation);
                for (removed_ephemeral_paths, removed_ephemeral_acls) |path, event_acl| {
                    try publishDeleted(
                        manager,
                        store,
                        allocator,
                        zxid,
                        path,
                        event_acl,
                    );
                }
            }
        },
        .expire_session => |request| {
            if (result.changed) {
                manager.invalidateSession(request.session_id, null);
                for (removed_ephemeral_paths, removed_ephemeral_acls) |path, event_acl| {
                    try publishDeleted(
                        manager,
                        store,
                        allocator,
                        zxid,
                        path,
                        event_acl,
                    );
                }
            }
        },
        .move_session => |request| {
            if (result.changed) {
                manager.invalidateSession(request.session_id, request.expected_generation);
            }
        },
        .set_acl,
        .open_session,
        .touch_session,
        .session_tick,
        => {},
    }
}

fn publishCreated(
    manager: *watch.Manager,
    store: *rocks_store.RocksStore,
    allocator: std.mem.Allocator,
    zxid: i64,
    path: []const u8,
) !void {
    try publishCurrentEvent(manager, store, allocator, .node_created, zxid, path);
    if (data_tree.parentPath(path)) |parent| {
        try publishCurrentEvent(
            manager,
            store,
            allocator,
            .node_children_changed,
            zxid,
            parent,
        );
    }
}

fn publishDeleted(
    manager: *watch.Manager,
    store: *rocks_store.RocksStore,
    allocator: std.mem.Allocator,
    zxid: i64,
    path: []const u8,
    deleted_acl: ?[]const u8,
) !void {
    manager.publish(.{
        .type = .node_deleted,
        .zxid = zxid,
        .path = path,
        .acl = deleted_acl,
    });
    if (data_tree.parentPath(path)) |parent| {
        try publishCurrentEvent(
            manager,
            store,
            allocator,
            .node_children_changed,
            zxid,
            parent,
        );
    }
}

fn publishCurrentEvent(
    manager: *watch.Manager,
    store: *rocks_store.RocksStore,
    allocator: std.mem.Allocator,
    event_type: watch.EventType,
    zxid: i64,
    path: []const u8,
) !void {
    const result = (try store.getAcl(allocator, path)) orelse return error.MissingWatchAcl;
    defer if (result.blob) |blob| allocator.free(blob);
    manager.publish(.{
        .type = event_type,
        .zxid = zxid,
        .path = path,
        .acl = result.blob,
    });
}

fn mutationSuccessResponseSize(mutation: command.Mutation) !usize {
    return switch (mutation) {
        .create => |request| std.math.add(
            usize,
            84,
            std.math.add(
                usize,
                request.path.len,
                if (request.sequential) 10 else 0,
            ) catch return error.SizeOverflow,
        ) catch error.SizeOverflow,
        .set_acl, .set_data => 80,
        .delete,
        .open_session,
        .touch_session,
        .close_session,
        .expire_session,
        .move_session,
        .session_tick,
        .delete_extended,
        => 12,
        .multi => error.SizeOverflow,
    };
}

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
    try testing.expectEqualStrings("two", data.data.?);
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

test "prepared mutation response sizes are exact" {
    const testing = std.testing;
    const stat = std.mem.zeroes(protocol.data.Stat);
    const plain_create = command.Mutation{ .create = .{
        .path = "/node",
        .data = "",
        .time_ms = 1,
    } };
    const plain_response = try command.encodeResult(
        testing.allocator,
        .ok,
        1,
        protocol.proto.Create2Response{ .path = "/node", .stat = stat },
    );
    defer testing.allocator.free(plain_response);
    try testing.expectEqual(plain_response.len, try mutationSuccessResponseSize(plain_create));

    const sequential_create = command.Mutation{ .create = .{
        .path = "/node",
        .data = "",
        .time_ms = 1,
        .sequential = true,
    } };
    const sequential_response = try command.encodeResult(
        testing.allocator,
        .ok,
        1,
        protocol.proto.Create2Response{ .path = "/node0000000000", .stat = stat },
    );
    defer testing.allocator.free(sequential_response);
    try testing.expectEqual(
        sequential_response.len,
        try mutationSuccessResponseSize(sequential_create),
    );

    const set_data = command.Mutation{ .set_data = .{
        .path = "/node",
        .data = "value",
        .expected_version = -1,
        .time_ms = 1,
    } };
    const set_response = try command.encodeResult(
        testing.allocator,
        .ok,
        1,
        protocol.proto.SetDataResponse{ .stat = stat },
    );
    defer testing.allocator.free(set_response);
    try testing.expectEqual(set_response.len, try mutationSuccessResponseSize(set_data));
}

test "session-rejected multi returns an error after advancing applied state" {
    const testing = std.testing;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try directory.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(path);
    var machine = try ZooKeeperStateMachine.init(testing.allocator, path);
    defer machine.deinit();

    const password = [_]u8{0x3c} ** 16;
    const opened = try applyMutation(&machine, testing.allocator, 1, .{ .open_session = .{
        .session_id = 77,
        .password = &password,
        .timeout_ms = 3_000,
        .tick_grace_ms = 100,
        .generation = 1,
    } });
    try testing.expectEqual(data_tree.ErrorCode.ok, opened.code);

    var body = jute.Writer.init(testing.allocator);
    defer body.deinit();
    try multi.writeTerminator(&body);
    const rejected = try applyMutation(&machine, testing.allocator, 2, .{ .multi = .{
        .body = body.bytes(),
        .time_ms = 1,
        .session_id = 77,
        .session_generation = 2,
    } });
    try testing.expectEqual(data_tree.ErrorCode.session_moved, rejected.code);
    try testing.expectEqual(@as(u64, 2), machine.appliedIndex());
}
