const std = @import("std");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");
const acl = @import("acl.zig");
const data_tree = @import("data_tree.zig");
const rocks_store = @import("rocks_store.zig");
const ephemeral = @import("ephemeral.zig");

extern fn snappy_uncompressed_length(compressed: [*]const u8, compressed_length: usize, result: *usize) c_int;
extern fn snappy_uncompress(compressed: [*]const u8, compressed_length: usize, uncompressed: [*]u8, uncompressed_length: *usize) c_int;

const snapshot_magic: i32 = 0x5a4b534e;
const log_magic: i32 = 0x5a4b4c47;
const persistence_version: i32 = 2;
const transaction_eor: u8 = 0x42;
const max_import_bytes: usize = rocks_store.max_snapshot_bytes;

pub const Options = struct {
    source_data_dir: []const u8,
    source_log_dir: ?[]const u8 = null,
    target_data_dir: []const u8,
    tick_grace_ms: i32 = 500,
};

const FileCandidate = struct {
    name: []u8,
    zxid: i64,
    compression: Compression = .plain,
};

const Compression = enum { plain, gzip, snappy };

const ImportedNode = struct {
    data: ?[]u8,
    acl_blob: ?[]u8,
    czxid: i64,
    mzxid: i64,
    ctime: i64,
    mtime: i64,
    version: i32,
    internal_cversion: i32,
    sequence_counter: i32 = 0,
    aversion: i32,
    ephemeral_owner: i64,
    kind: ephemeral.NodeKind,
    pzxid: i64,
    child_count: usize = 0,

    fn deinit(self: *ImportedNode, allocator: std.mem.Allocator) void {
        if (self.data) |value| allocator.free(value);
        if (self.acl_blob) |value| allocator.free(value);
        self.* = undefined;
    }
};

const ImportState = struct {
    allocator: std.mem.Allocator,
    nodes: std.StringHashMap(ImportedNode),
    sessions: std.AutoHashMap(i64, i32),
    source_zxid: i64,

    fn init(allocator: std.mem.Allocator, source_zxid: i64) ImportState {
        return .{
            .allocator = allocator,
            .nodes = std.StringHashMap(ImportedNode).init(allocator),
            .sessions = std.AutoHashMap(i64, i32).init(allocator),
            .source_zxid = source_zxid,
        };
    }

    fn deinit(self: *ImportState) void {
        var nodes = self.nodes.iterator();
        while (nodes.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.nodes.deinit();
        self.sessions.deinit();
        self.* = undefined;
    }
};

pub fn importOnFirstStart(
    allocator: std.mem.Allocator,
    io: std.Io,
    options: Options,
) !void {
    if (options.tick_grace_ms <= 0) return error.InvalidTickGrace;
    try ensureTargetDataDirectoryEmpty(io, options.target_data_dir);
    const staging_path = try std.fmt.allocPrint(allocator, "{s}/state.rocksdb.importing", .{options.target_data_dir});
    defer allocator.free(staging_path);
    const final_path = try std.fmt.allocPrint(allocator, "{s}/state.rocksdb", .{options.target_data_dir});
    defer allocator.free(final_path);

    // Atomically claim the empty target before reading the source. Concurrent
    // import attempts cannot share or delete another importer's staging state.
    const cwd = std.Io.Dir.cwd();
    cwd.createDir(io, staging_path, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => return error.TargetDataDirectoryNotEmpty,
        else => return err,
    };
    errdefer cwd.deleteTree(io, staging_path) catch {};

    var state = try loadSource(allocator, io, options.source_data_dir, options.source_log_dir);
    defer state.deinit();
    try finalizeState(&state);

    var store = try rocks_store.RocksStore.open(allocator, staging_path);
    {
        defer store.deinit();
        const nodes = try materializeNodes(allocator, &state);
        defer allocator.free(nodes);
        const sessions = try materializeSessions(allocator, &state);
        defer allocator.free(sessions);
        try store.installImported(nodes, sessions, options.tick_grace_ms, state.source_zxid);
    }
    try cwd.rename(staging_path, cwd, final_path, io);
}

fn loadSource(
    allocator: std.mem.Allocator,
    io: std.Io,
    data_dir_path: []const u8,
    maybe_log_dir_path: ?[]const u8,
) !ImportState {
    var data_dir = try openVersionDir(io, data_dir_path);
    defer data_dir.close(io);
    var snapshots = try discoverFiles(allocator, io, data_dir, "snapshot", true);
    defer deinitCandidates(allocator, &snapshots);
    if (snapshots.items.len == 0) return error.NoSnapshot;
    std.mem.sort(FileCandidate, snapshots.items, {}, newerFirst);

    var last_error: anyerror = error.NoValidSnapshot;
    for (snapshots.items) |candidate| {
        var state = parseSnapshotFile(allocator, io, data_dir, candidate) catch |err| {
            if (isNonFallbackImportError(err)) return err;
            last_error = err;
            continue;
        };
        errdefer state.deinit();
        var log_dir = try openVersionDir(io, maybe_log_dir_path orelse data_dir_path);
        defer log_dir.close(io);
        replayLogs(allocator, io, log_dir, &state) catch |err| {
            state.deinit();
            if (isNonFallbackImportError(err)) return err;
            last_error = err;
            continue;
        };
        return state;
    }
    return last_error;
}

fn isNonFallbackImportError(err: anyerror) bool {
    return switch (err) {
        error.OutOfMemory,
        error.SnapshotTooLarge,
        error.TransactionLogTooLarge,
        error.UnsupportedAclScheme,
        error.IncompatibleSnappyVersion,
        error.InvalidExtendedOwner,
        error.UnsupportedExtendedNodeType,
        error.UnsupportedTransaction,
        => true,
        else => false,
    };
}

fn parseSnapshotFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    candidate: FileCandidate,
) !ImportState {
    const encoded = try dir.readFileAlloc(io, candidate.name, allocator, .limited(max_import_bytes));
    defer allocator.free(encoded);
    const bytes = switch (candidate.compression) {
        .plain => try allocator.dupe(u8, encoded),
        .gzip => try decompressGzip(allocator, encoded),
        .snappy => try decompressSnappy(allocator, encoded),
    };
    defer allocator.free(bytes);
    if (bytes.len > max_import_bytes) return error.SnapshotTooLarge;

    var reader = jute.Reader.init(bytes);
    try readFileHeader(&reader, snapshot_magic, -1);
    var state = ImportState.init(allocator, candidate.zxid);
    errdefer state.deinit();

    const session_count = try readCount(&reader);
    var session_index: usize = 0;
    while (session_index < session_count) : (session_index += 1) {
        const session_id = try reader.readLong();
        const timeout_ms = try reader.readInt();
        if (session_id == 0 or session_id == ephemeral.container_owner or timeout_ms <= 0) {
            return error.InvalidSession;
        }
        const result = try state.sessions.getOrPut(session_id);
        if (result.found_existing) return error.DuplicateSession;
        result.value_ptr.* = timeout_ms;
    }

    var acl_cache = std.AutoHashMap(i64, ?[]u8).init(allocator);
    defer {
        var values = acl_cache.valueIterator();
        while (values.next()) |value| if (value.*) |blob| allocator.free(blob);
        acl_cache.deinit();
    }
    const acl_count = try readCount(&reader);
    var acl_index: usize = 0;
    while (acl_index < acl_count) : (acl_index += 1) {
        const acl_id = try reader.readLong();
        const blob = try readAclBlob(allocator, &reader);
        errdefer allocator.free(blob);
        const result = try acl_cache.getOrPut(acl_id);
        if (result.found_existing) return error.DuplicateAclId;
        result.value_ptr.* = blob;
    }

    while (true) {
        const source_path = (try reader.readString()) orelse return error.InvalidSnapshot;
        if (std.mem.eql(u8, source_path, "/")) break;
        const path = if (source_path.len == 0) "/" else source_path;
        if (!data_tree.isValidPath(path)) return error.InvalidPath;
        const maybe_data = try reader.readBuffer();
        const acl_id = try reader.readLong();
        const acl_blob = if (acl_id == -1)
            null
        else if (acl_cache.get(acl_id)) |cached|
            if (cached) |value| try allocator.dupe(u8, value) else null
        else
            return error.UnknownAclId;
        errdefer if (acl_blob) |value| allocator.free(value);
        const czxid = try reader.readLong();
        const mzxid = try reader.readLong();
        const ctime = try reader.readLong();
        const mtime = try reader.readLong();
        const node_version = try reader.readInt();
        const internal_cversion = try reader.readInt();
        const aversion = try reader.readInt();
        const owner = try reader.readLong();
        const pzxid = try reader.readLong();
        const kind: ephemeral.NodeKind = if (owner == ephemeral.container_owner)
            .container
        else if (owner != 0 and state.sessions.contains(owner))
            .ephemeral
        else
            ephemeral.classifyOwner(owner) catch return error.InvalidExtendedOwner;
        const node = ImportedNode{
            .data = if (maybe_data) |value| try allocator.dupe(u8, value) else null,
            .acl_blob = acl_blob,
            .czxid = czxid,
            .mzxid = mzxid,
            .ctime = ctime,
            .mtime = mtime,
            .version = node_version,
            .internal_cversion = internal_cversion,
            .aversion = aversion,
            .ephemeral_owner = owner,
            .kind = kind,
            .pzxid = pzxid,
        };
        errdefer if (node.data) |value| allocator.free(value);
        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);
        const result = try state.nodes.getOrPut(owned_path);
        if (result.found_existing) return error.DuplicateNode;
        result.key_ptr.* = owned_path;
        result.value_ptr.* = node;
    }
    try readSeal(bytes, &reader);
    try readSnapshotTrailers(bytes, &reader);
    if (reader.remaining() != 0) return error.TrailingSnapshotData;
    if (!state.nodes.contains("/")) return error.MissingRoot;
    return state;
}

fn replayLogs(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    state: *ImportState,
) !void {
    var logs = try discoverFiles(allocator, io, dir, "log", false);
    defer deinitCandidates(allocator, &logs);
    std.mem.sort(FileCandidate, logs.items, {}, olderFirst);
    var start_index: usize = 0;
    for (logs.items, 0..) |candidate, index| {
        if (candidate.zxid <= state.source_zxid) start_index = index;
        if (candidate.zxid > state.source_zxid) break;
    }
    for (logs.items[start_index..]) |candidate| {
        const bytes = try dir.readFileAlloc(io, candidate.name, allocator, .limited(max_import_bytes));
        defer allocator.free(bytes);
        try replayLog(bytes, candidate.zxid, state);
    }
}

fn replayLog(bytes: []const u8, filename_zxid: i64, state: *ImportState) !void {
    var reader = jute.Reader.init(bytes);
    try readFileHeader(&reader, log_magic, null);
    var first_zxid: ?i64 = null;
    var previous_zxid: ?i64 = null;
    while (reader.remaining() != 0) {
        if (reader.remaining() < 12) {
            if (allZero(bytes[reader.consumed()..])) {
                reader.position = reader.input.len;
                break;
            }
            return error.TruncatedLogEnvelope;
        }
        const checksum = try reader.readLong();
        const length = try reader.readInt();
        if (checksum == 0 and length == 0) {
            if (!allZero(bytes[reader.consumed()..])) return error.NonZeroLogTail;
            reader.position = reader.input.len;
            break;
        }
        if (length < 0) return error.InvalidLogLength;
        const payload_length: usize = @intCast(length);
        if (payload_length > reader.remaining() -| 1) return error.TruncatedLogPayload;
        const payload_start = reader.consumed();
        const payload = bytes[payload_start .. payload_start + payload_length];
        reader.position += payload_length;
        if (@as(u64, @bitCast(checksum)) != std.hash.Adler32.hash(payload)) return error.LogChecksumMismatch;
        if (@as(u8, @bitCast(try reader.readByte())) != transaction_eor) return error.InvalidTransactionMarker;
        var payload_reader = jute.Reader.init(payload);
        const header = try jute.deserialize(protocol.txn.TxnHeader, &payload_reader, state.allocator);
        if (first_zxid == null) first_zxid = header.zxid;
        if (previous_zxid) |previous| if (header.zxid <= previous) return error.NonMonotonicZxid;
        previous_zxid = header.zxid;
        if (header.zxid > state.source_zxid) {
            try applyTransaction(state, header, &payload_reader);
            state.source_zxid = header.zxid;
        } else {
            payload_reader.position = payload_reader.input.len;
        }
    }
    if (first_zxid) |value| if (value != filename_zxid) return error.LogFilenameMismatch;
}

fn applyTransaction(
    state: *ImportState,
    header: protocol.txn.TxnHeader,
    reader: *jute.Reader,
) !void {
    switch (header.type) {
        -1 => {
            _ = try decodeBody(protocol.txn.ErrorTxn, reader, state.allocator);
        },
        -10 => {
            const txn = try decodeBody(protocol.txn.CreateSessionTxn, reader, state.allocator);
            if (header.clientId == 0 or header.clientId == ephemeral.container_owner or txn.timeOut <= 0) {
                return error.InvalidSession;
            }
            try state.sessions.put(header.clientId, txn.timeOut);
        },
        -11 => try applyCloseSession(state, header, reader),
        1, 15 => try applyCreate(state, header, reader),
        2 => try applyDelete(state, header, reader),
        5, 16 => try applySetData(state, header, reader),
        7 => try applySetAcl(state, reader),
        13 => {
            _ = try decodeBody(protocol.txn.CheckVersionTxn, reader, state.allocator);
        },
        14 => try applyMulti(state, header, reader),
        19 => try applyCreateContainer(state, header, reader),
        20 => try applyDelete(state, header, reader),
        21 => try applyCreateTtl(state, header, reader),
        else => return error.UnsupportedTransaction,
    }
    try consumeOptionalDigest(reader);
}

fn applyCreate(state: *ImportState, header: protocol.txn.TxnHeader, reader: *jute.Reader) !void {
    const saved = reader.*;
    const current = jute.deserialize(protocol.txn.CreateTxn, reader, state.allocator) catch {
        reader.* = saved;
        const legacy = try jute.deserialize(protocol.txn.CreateTxnV0, reader, state.allocator);
        defer jute.deinitDecoded(legacy, state.allocator);
        const kind: ephemeral.NodeKind = if (legacy.ephemeral) .ephemeral else .persistent;
        return applyCreateFields(state, header, legacy.path, legacy.data, legacy.acl, kind, if (legacy.ephemeral) header.clientId else 0, -1);
    };
    defer jute.deinitDecoded(current, state.allocator);
    if (!hasValidDigestRemainder(reader.remaining())) {
        reader.* = saved;
        const legacy = try jute.deserialize(protocol.txn.CreateTxnV0, reader, state.allocator);
        defer jute.deinitDecoded(legacy, state.allocator);
        const kind: ephemeral.NodeKind = if (legacy.ephemeral) .ephemeral else .persistent;
        return applyCreateFields(state, header, legacy.path, legacy.data, legacy.acl, kind, if (legacy.ephemeral) header.clientId else 0, -1);
    }
    const kind: ephemeral.NodeKind = if (current.ephemeral) .ephemeral else .persistent;
    try applyCreateFields(state, header, current.path, current.data, current.acl, kind, if (current.ephemeral) header.clientId else 0, current.parentCVersion);
}

fn applyCreateContainer(state: *ImportState, header: protocol.txn.TxnHeader, reader: *jute.Reader) !void {
    const txn = try jute.deserialize(protocol.txn.CreateContainerTxn, reader, state.allocator);
    defer jute.deinitDecoded(txn, state.allocator);
    try applyCreateFields(state, header, txn.path, txn.data, txn.acl, .container, ephemeral.container_owner, txn.parentCVersion);
}

fn applyCreateTtl(state: *ImportState, header: protocol.txn.TxnHeader, reader: *jute.Reader) !void {
    const txn = try jute.deserialize(protocol.txn.CreateTTLTxn, reader, state.allocator);
    defer jute.deinitDecoded(txn, state.allocator);
    const owner = try ephemeral.ttlOwner(txn.ttl);
    try applyCreateFields(state, header, txn.path, txn.data, txn.acl, .ttl, owner, txn.parentCVersion);
}

fn applyCreateFields(
    state: *ImportState,
    header: protocol.txn.TxnHeader,
    maybe_path: ?[]const u8,
    data: ?[]const u8,
    entries: ?[]const protocol.data.ACL,
    kind: ephemeral.NodeKind,
    owner: i64,
    parent_cversion: i32,
) !void {
    const path = maybe_path orelse return error.InvalidTransaction;
    if (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/")) return error.InvalidPath;
    ephemeral.validate(kind, owner) catch return error.InvalidExtendedOwner;
    try validateImportedAclSchemes(entries);
    const parent_path = data_tree.parentPath(path) orelse return error.InvalidPath;
    const parent = state.nodes.getPtr(parent_path) orelse return error.MissingParent;
    if (state.nodes.contains(path)) {
        // Fuzzy snapshots can already contain a node created after their header
        // zxid. Preserve that complete node image while replaying its create,
        // but repair parent metadata when the transaction carries an
        // authoritative parent cversion.
        if (parent_cversion != -1 and parent_cversion > parent.internal_cversion) {
            parent.internal_cversion = parent_cversion;
        }
        if (header.zxid > parent.pzxid) parent.pzxid = header.zxid;
        return;
    }
    const next_cversion = if (parent_cversion == -1) parent.internal_cversion +% 1 else parent_cversion;
    if (next_cversion > parent.internal_cversion) parent.internal_cversion = next_cversion;
    if (header.zxid > parent.pzxid) parent.pzxid = header.zxid;
    const blob = try acl.normalize(state.allocator, entries, &.{});
    errdefer state.allocator.free(blob);
    const owned_data = if (data) |value| try state.allocator.dupe(u8, value) else null;
    errdefer if (owned_data) |value| state.allocator.free(value);
    const owned_path = try state.allocator.dupe(u8, path);
    errdefer state.allocator.free(owned_path);
    try state.nodes.putNoClobber(owned_path, .{
        .data = owned_data,
        .acl_blob = blob,
        .czxid = header.zxid,
        .mzxid = header.zxid,
        .ctime = header.time,
        .mtime = header.time,
        .version = 0,
        .internal_cversion = 0,
        .aversion = 0,
        .ephemeral_owner = owner,
        .kind = kind,
        .pzxid = header.zxid,
    });
}

fn applyDelete(
    state: *ImportState,
    header: protocol.txn.TxnHeader,
    reader: *jute.Reader,
) !void {
    const txn = try decodeBody(protocol.txn.DeleteTxn, reader, state.allocator);
    const path = txn.path orelse return error.InvalidTransaction;
    const parent_path = data_tree.parentPath(path) orelse return error.InvalidPath;
    if (state.nodes.getPtr(parent_path)) |parent| {
        if (header.zxid > parent.pzxid) parent.pzxid = header.zxid;
    }
    if (state.nodes.fetchRemove(path)) |removed| {
        state.allocator.free(removed.key);
        var node = removed.value;
        node.deinit(state.allocator);
    }
}

fn applySetData(state: *ImportState, header: protocol.txn.TxnHeader, reader: *jute.Reader) !void {
    const txn = try decodeBody(protocol.txn.SetDataTxn, reader, state.allocator);
    const path = txn.path orelse return error.InvalidTransaction;
    const node = state.nodes.getPtr(path) orelse return;
    const replacement = if (txn.data) |value| try state.allocator.dupe(u8, value) else null;
    if (node.data) |value| state.allocator.free(value);
    node.data = replacement;
    node.mzxid = header.zxid;
    node.mtime = header.time;
    node.version = txn.version;
}

fn applySetAcl(state: *ImportState, reader: *jute.Reader) !void {
    const txn = try jute.deserialize(protocol.txn.SetACLTxn, reader, state.allocator);
    defer jute.deinitDecoded(txn, state.allocator);
    const path = txn.path orelse return error.InvalidTransaction;
    const node = state.nodes.getPtr(path) orelse return;
    try validateImportedAclSchemes(txn.acl);
    const replacement = try acl.normalize(state.allocator, txn.acl, &.{});
    if (node.acl_blob) |value| state.allocator.free(value);
    node.acl_blob = replacement;
    node.aversion = txn.version;
}

fn applyCloseSession(
    state: *ImportState,
    header: protocol.txn.TxnHeader,
    reader: *jute.Reader,
) !void {
    const session_id = header.clientId;
    _ = state.sessions.remove(session_id);
    const saved = reader.*;
    const could_be_close_body = reader.remaining() != 0 and
        (reader.remaining() != 12 or std.mem.readInt(i32, reader.input[reader.position..][0..4], .big) == 1);
    if (could_be_close_body) parse_close: {
        const txn = jute.deserialize(protocol.txn.CloseSessionTxn, reader, state.allocator) catch {
            reader.* = saved;
            break :parse_close;
        };
        defer jute.deinitDecoded(txn, state.allocator);
        if (!hasValidDigestRemainder(reader.remaining()) or !validClosePaths(txn.paths2Delete)) {
            reader.* = saved;
            break :parse_close;
        }
        if (txn.paths2Delete) |paths| for (paths) |maybe_path| if (maybe_path) |path|
            removeNodeAtZxid(state, path, header.zxid);
    }
    var paths: std.ArrayList([]u8) = .empty;
    defer {
        for (paths.items) |path| state.allocator.free(path);
        paths.deinit(state.allocator);
    }
    var nodes = state.nodes.iterator();
    while (nodes.next()) |entry| if (entry.value_ptr.kind == .ephemeral and entry.value_ptr.ephemeral_owner == session_id) {
        try paths.append(state.allocator, try state.allocator.dupe(u8, entry.key_ptr.*));
    };
    for (paths.items) |path| removeNodeAtZxid(state, path, header.zxid);
}

fn validClosePaths(maybe_paths: ?[]const ?[]const u8) bool {
    const paths = maybe_paths orelse return false;
    for (paths) |maybe_path| {
        const path = maybe_path orelse return false;
        if (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/")) return false;
    }
    return true;
}

fn applyMulti(state: *ImportState, header: protocol.txn.TxnHeader, reader: *jute.Reader) !void {
    const multi = try jute.deserialize(protocol.txn.MultiTxn, reader, state.allocator);
    defer jute.deinitDecoded(multi, state.allocator);
    const txns = multi.txns orelse return error.InvalidTransaction;
    for (txns) |txn| if (txn.type == -1) return;
    for (txns) |txn| {
        const bytes = txn.data orelse return error.InvalidTransaction;
        var nested = jute.Reader.init(bytes);
        switch (txn.type) {
            1, 15 => try applyCreate(state, header, &nested),
            2 => try applyDelete(state, header, &nested),
            5 => try applySetData(state, header, &nested),
            13 => _ = try decodeBody(protocol.txn.CheckVersionTxn, &nested, state.allocator),
            19 => try applyCreateContainer(state, header, &nested),
            20 => try applyDelete(state, header, &nested),
            21 => try applyCreateTtl(state, header, &nested),
            else => return error.UnsupportedTransaction,
        }
        if (nested.remaining() != 0) return error.InvalidTransaction;
    }
}

fn finalizeState(state: *ImportState) !void {
    var orphan_paths: std.ArrayList([]u8) = .empty;
    defer {
        for (orphan_paths.items) |path| state.allocator.free(path);
        orphan_paths.deinit(state.allocator);
    }
    var nodes = state.nodes.iterator();
    while (nodes.next()) |entry| {
        entry.value_ptr.child_count = 0;
        const owner = entry.value_ptr.ephemeral_owner;
        ephemeral.validate(entry.value_ptr.kind, owner) catch return error.InvalidExtendedOwner;
        if (std.mem.eql(u8, entry.key_ptr.*, "/") and entry.value_ptr.kind != .persistent) {
            return error.InvalidRoot;
        }
        if (entry.value_ptr.kind == .ephemeral and !state.sessions.contains(owner)) {
            try orphan_paths.append(state.allocator, try state.allocator.dupe(u8, entry.key_ptr.*));
        }
    }
    for (orphan_paths.items) |path| removeNodeAtZxid(state, path, state.source_zxid);
    nodes = state.nodes.iterator();
    while (nodes.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "/")) continue;
        const parent_path = data_tree.parentPath(entry.key_ptr.*) orelse return error.InvalidPath;
        const parent = state.nodes.getPtr(parent_path) orelse return error.MissingParent;
        parent.child_count = std.math.add(usize, parent.child_count, 1) catch return error.TooManyChildren;
    }
    nodes = state.nodes.iterator();
    while (nodes.next()) |entry| {
        if (entry.value_ptr.kind == .ephemeral and entry.value_ptr.child_count != 0) {
            return error.EphemeralHasChildren;
        }
        const child_count: i32 = std.math.cast(i32, entry.value_ptr.child_count) orelse
            return error.TooManyChildren;
        // ZooKeeper persists a create-only counter in StatPersisted.cversion.
        // Sequential suffixes use that raw value, while copyStat exposes
        // raw_cversion * 2 - child_count to clients.
        entry.value_ptr.sequence_counter = entry.value_ptr.internal_cversion;
        entry.value_ptr.internal_cversion = entry.value_ptr.internal_cversion *% 2 -% child_count;
    }
}

fn materializeNodes(allocator: std.mem.Allocator, state: *ImportState) ![]rocks_store.ImportNode {
    const output = try allocator.alloc(rocks_store.ImportNode, state.nodes.count());
    var index: usize = 0;
    var nodes = state.nodes.iterator();
    while (nodes.next()) |entry| : (index += 1) output[index] = .{
        .path = entry.key_ptr.*,
        .data = entry.value_ptr.data,
        .acl = entry.value_ptr.acl_blob,
        .czxid = entry.value_ptr.czxid,
        .mzxid = entry.value_ptr.mzxid,
        .ctime = entry.value_ptr.ctime,
        .mtime = entry.value_ptr.mtime,
        .version = entry.value_ptr.version,
        .cversion = entry.value_ptr.internal_cversion,
        .sequence_counter = entry.value_ptr.sequence_counter,
        .aversion = entry.value_ptr.aversion,
        .ephemeral_owner = entry.value_ptr.ephemeral_owner,
        .kind = entry.value_ptr.kind,
        .pzxid = entry.value_ptr.pzxid,
        .child_count = entry.value_ptr.child_count,
    };
    return output;
}

fn materializeSessions(allocator: std.mem.Allocator, state: *ImportState) ![]rocks_store.ImportSession {
    const output = try allocator.alloc(rocks_store.ImportSession, state.sessions.count());
    var index: usize = 0;
    var sessions = state.sessions.iterator();
    while (sessions.next()) |entry| : (index += 1) output[index] = .{
        .session_id = entry.key_ptr.*,
        .password = javaSessionPassword(entry.key_ptr.*),
        .timeout_ms = entry.value_ptr.*,
    };
    return output;
}

pub fn javaSessionPassword(session_id: i64) [16]u8 {
    const multiplier: u64 = 0x5deece66d;
    const mask: u64 = (@as(u64, 1) << 48) - 1;
    var seed = ((@as(u64, @bitCast(session_id)) ^ 0xb3415c00) ^ multiplier) & mask;
    var output: [16]u8 = undefined;
    var word_index: usize = 0;
    while (word_index < 4) : (word_index += 1) {
        seed = (seed *% multiplier +% 0xb) & mask;
        const word: u32 = @truncate(seed >> 16);
        std.mem.writeInt(u32, output[word_index * 4 ..][0..4], word, .little);
    }
    return output;
}

fn removeNodeAtZxid(state: *ImportState, path: []const u8, zxid: i64) void {
    if (data_tree.parentPath(path)) |parent_path| if (state.nodes.getPtr(parent_path)) |parent| {
        if (zxid > parent.pzxid) parent.pzxid = zxid;
    };
    removeNode(state, path);
}

fn removeNode(state: *ImportState, path: []const u8) void {
    if (state.nodes.fetchRemove(path)) |removed| {
        state.allocator.free(removed.key);
        var node = removed.value;
        node.deinit(state.allocator);
    }
}

fn readFileHeader(reader: *jute.Reader, expected_magic: i32, expected_dbid: ?i64) !void {
    if (try reader.readInt() != expected_magic or try reader.readInt() != persistence_version) {
        return error.InvalidFileHeader;
    }
    const dbid = try reader.readLong();
    if (expected_dbid) |expected| if (dbid != expected) return error.InvalidFileHeader;
}

fn readCount(reader: *jute.Reader) !usize {
    const count = try reader.readInt();
    if (count < 0) return error.InvalidCount;
    return @intCast(count);
}

fn validateImportedAclSchemes(entries: ?[]const protocol.data.ACL) !void {
    for (entries orelse return error.InvalidAcl) |entry| {
        const scheme = entry.id.scheme orelse return error.InvalidAcl;
        if (!std.mem.eql(u8, scheme, "world") and
            !std.mem.eql(u8, scheme, "digest") and
            !std.mem.eql(u8, scheme, "ip"))
        {
            return error.UnsupportedAclScheme;
        }
    }
}

fn readAclBlob(allocator: std.mem.Allocator, reader: *jute.Reader) ![]u8 {
    const count = try readCount(reader);
    if (count == 0) return error.InvalidAcl;
    var entries: std.ArrayList(acl.Entry) = .empty;
    defer entries.deinit(allocator);
    try entries.ensureTotalCapacity(allocator, count);
    var index: usize = 0;
    while (index < count) : (index += 1) {
        const entry: acl.Entry = .{
            .perms = try reader.readInt(),
            .scheme = (try reader.readString()) orelse return error.InvalidAcl,
            .id = (try reader.readString()) orelse return error.InvalidAcl,
        };
        if (!std.mem.eql(u8, entry.scheme, "world") and
            !std.mem.eql(u8, entry.scheme, "digest") and
            !std.mem.eql(u8, entry.scheme, "ip"))
        {
            return error.UnsupportedAclScheme;
        }
        entries.appendAssumeCapacity(entry);
    }
    return acl.encode(allocator, entries.items);
}

fn readSeal(bytes: []const u8, reader: *jute.Reader) !void {
    const expected = std.hash.Adler32.hash(bytes[0..reader.consumed()]);
    const stored: u64 = @bitCast(try reader.readLong());
    if (stored != expected) return error.SnapshotChecksumMismatch;
    const marker = (try reader.readString()) orelse return error.InvalidSnapshotSeal;
    if (!std.mem.eql(u8, marker, "/")) return error.InvalidSnapshotSeal;
}

fn readSnapshotTrailers(bytes: []const u8, reader: *jute.Reader) !void {
    if (reader.remaining() == 0) return;
    if (reader.remaining() == 21) {
        _ = try reader.readLong();
        return readSeal(bytes, reader);
    }
    _ = try reader.readLong();
    const digest_version = try reader.readInt();
    if (digest_version < 2) {
        _ = (try reader.readString()) orelse return error.InvalidSnapshotDigest;
    } else {
        _ = try reader.readLong();
    }
    try readSeal(bytes, reader);
    if (reader.remaining() != 0) {
        _ = try reader.readLong();
        try readSeal(bytes, reader);
    }
}

fn consumeOptionalDigest(reader: *jute.Reader) !void {
    if (reader.remaining() == 0) return;
    if (reader.remaining() != 12) return error.InvalidTransaction;
    _ = try reader.readInt();
    _ = try reader.readLong();
}

fn hasValidDigestRemainder(remaining: usize) bool {
    return remaining == 0 or remaining == 12;
}

fn decodeBody(comptime T: type, reader: *jute.Reader, allocator: std.mem.Allocator) !T {
    return jute.deserialize(T, reader, allocator);
}

fn discoverFiles(
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    prefix: []const u8,
    allow_compression: bool,
) !std.ArrayList(FileCandidate) {
    var output: std.ArrayList(FileCandidate) = .empty;
    errdefer deinitCandidates(allocator, &output);
    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != .file) continue;
        const candidate = parseCandidate(entry.name, prefix, allow_compression) orelse continue;
        for (output.items) |existing| if (existing.zxid == candidate.zxid) return error.DuplicatePersistenceFile;
        try output.append(allocator, .{
            .name = try allocator.dupe(u8, entry.name),
            .zxid = candidate.zxid,
            .compression = candidate.compression,
        });
    }
    return output;
}

fn parseCandidate(name: []const u8, prefix: []const u8, allow_compression: bool) ?FileCandidate {
    if (!std.mem.startsWith(u8, name, prefix) or name.len <= prefix.len or name[prefix.len] != '.') return null;
    const suffix = name[prefix.len + 1 ..];
    const dot = std.mem.indexOfScalar(u8, suffix, '.');
    const hex = if (dot) |index| suffix[0..index] else suffix;
    if (hex.len == 0 or hex.len > 16) return null;
    const bits = std.fmt.parseUnsigned(u64, hex, 16) catch return null;
    const compression: Compression = if (dot) |index| blk: {
        if (!allow_compression) return null;
        const extension = suffix[index + 1 ..];
        if (std.mem.eql(u8, extension, "gz")) break :blk .gzip;
        if (std.mem.eql(u8, extension, "snappy")) break :blk .snappy;
        return null;
    } else .plain;
    return .{ .name = undefined, .zxid = @bitCast(bits), .compression = compression };
}

fn openVersionDir(io: std.Io, base_path: []const u8) !std.Io.Dir {
    const version_path = try std.fmt.allocPrint(std.heap.smp_allocator, "{s}/version-2", .{base_path});
    defer std.heap.smp_allocator.free(version_path);
    return openConfiguredDir(io, version_path) catch |err| switch (err) {
        error.FileNotFound => openConfiguredDir(io, base_path),
        else => err,
    };
}

fn openConfiguredDir(io: std.Io, path: []const u8) !std.Io.Dir {
    if (std.fs.path.isAbsolute(path)) {
        return std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true });
    }
    return std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
}

fn ensureTargetDataDirectoryEmpty(io: std.Io, target_data_dir: []const u8) !void {
    try std.Io.Dir.cwd().createDirPath(io, target_data_dir);
    var target_dir = try openConfiguredDir(io, target_data_dir);
    defer target_dir.close(io);
    var iterator = target_dir.iterate();
    if (try iterator.next(io) != null) return error.TargetDataDirectoryNotEmpty;
}

fn decompressSnappy(allocator: std.mem.Allocator, encoded: []const u8) ![]u8 {
    const magic = [_]u8{ 0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0 };
    var position: usize = 0;
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);
    var saw_header = false;
    while (position < encoded.len) {
        if (encoded.len - position >= 16 and std.mem.eql(u8, encoded[position..][0..8], &magic)) {
            const version = std.mem.readInt(i32, encoded[position + 8 ..][0..4], .big);
            _ = std.mem.readInt(i32, encoded[position + 12 ..][0..4], .big);
            if (version < 1) return error.IncompatibleSnappyVersion;
            position += 16;
            saw_header = true;
            continue;
        }
        if (!saw_header or encoded.len - position < 4) return error.TruncatedSnappyStream;
        const signed_length = std.mem.readInt(i32, encoded[position..][0..4], .big);
        position += 4;
        if (signed_length <= 0) return error.InvalidSnappyChunk;
        const compressed_length: usize = @intCast(signed_length);
        if (compressed_length > encoded.len - position) return error.TruncatedSnappyStream;
        const compressed = encoded[position..][0..compressed_length];
        position += compressed_length;
        var uncompressed_length: usize = 0;
        if (snappy_uncompressed_length(compressed.ptr, compressed.len, &uncompressed_length) != 0) {
            return error.InvalidSnappyChunk;
        }
        if (uncompressed_length == 0) return error.InvalidSnappyChunk;
        if (uncompressed_length > max_import_bytes -| output.items.len) return error.SnapshotTooLarge;
        const old_length = output.items.len;
        try output.resize(allocator, old_length + uncompressed_length);
        var actual_length = uncompressed_length;
        if (snappy_uncompress(compressed.ptr, compressed.len, output.items[old_length..].ptr, &actual_length) != 0 or
            actual_length != uncompressed_length)
        {
            return error.InvalidSnappyChunk;
        }
    }
    if (!saw_header) return error.InvalidSnappyHeader;
    return output.toOwnedSlice(allocator);
}

fn decompressGzip(allocator: std.mem.Allocator, encoded: []const u8) ![]u8 {
    var input: std.Io.Reader = .fixed(encoded);
    var output: std.Io.Writer.Allocating = .init(allocator);
    defer output.deinit();
    var decompressor: std.compress.flate.Decompress = .init(&input, .gzip, &.{});
    _ = try decompressor.reader.stream(&output.writer, .limited(max_import_bytes + 1));
    if (decompressor.err) |err| return err;
    if (output.written().len > max_import_bytes) return error.SnapshotTooLarge;
    return output.toOwnedSlice();
}

test "Xerial legacy Snappy stream is decoded with libsnappy" {
    const encoded = [_]u8{
        0x82, 'S', 'N', 'A', 'P', 'P',  'Y', 0,
        0,    0,   0,   1,   0,   0,    0,   1,
        0,    0,   0,   7,   5,   0x10, 'h', 'e',
        'l',  'l', 'o',
    };
    const decoded = try decompressSnappy(std.testing.allocator, &encoded);
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqualStrings("hello", decoded);
}

test "gzip snapshots are decompressed through the bounded path" {
    const encoded = [_]u8{
        0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x03, 0xcb, 0x48, 0xcd,
        0xc9, 0xc9, 0x07, 0x00, 0x86, 0xa6, 0x10, 0x36, 0x05, 0x00, 0x00, 0x00,
    };
    const decoded = try decompressGzip(std.testing.allocator, &encoded);
    defer std.testing.allocator.free(decoded);
    try std.testing.expectEqualStrings("hello", decoded);
}

fn deinitCandidates(allocator: std.mem.Allocator, candidates: *std.ArrayList(FileCandidate)) void {
    for (candidates.items) |candidate| allocator.free(candidate.name);
    candidates.deinit(allocator);
}

fn newerFirst(_: void, left: FileCandidate, right: FileCandidate) bool {
    return left.zxid > right.zxid;
}

fn olderFirst(_: void, left: FileCandidate, right: FileCandidate) bool {
    return left.zxid < right.zxid;
}

fn allZero(bytes: []const u8) bool {
    return std.mem.allEqual(u8, bytes, 0);
}

test "replays container TTL and delete-container transactions" {
    const testing = std.testing;
    var state = ImportState.init(testing.allocator, 0);
    defer state.deinit();
    const root_path = try testing.allocator.dupe(u8, "/");
    const root_data = try testing.allocator.alloc(u8, 0);
    try state.nodes.putNoClobber(root_path, .{
        .data = root_data,
        .acl_blob = null,
        .czxid = 0,
        .mzxid = 0,
        .ctime = 0,
        .mtime = 0,
        .version = 0,
        .internal_cversion = 0,
        .aversion = 0,
        .ephemeral_owner = 0,
        .kind = .persistent,
        .pzxid = 0,
    });
    const open_acl = [_]protocol.data.ACL{.{
        .perms = acl.all,
        .id = .{ .scheme = "world", .id = "anyone" },
    }};

    var container_body = jute.Writer.init(testing.allocator);
    defer container_body.deinit();
    try jute.serialize(&container_body, protocol.txn.CreateContainerTxn{
        .path = "/container",
        .data = "",
        .acl = &open_acl,
        .parentCVersion = 1,
    });
    var container_reader = jute.Reader.init(container_body.bytes());
    try applyTransaction(&state, .{
        .clientId = 1,
        .cxid = 1,
        .zxid = 1,
        .time = 100,
        .type = 19,
    }, &container_reader);
    try testing.expectEqual(ephemeral.NodeKind.container, state.nodes.get("/container").?.kind);

    // A fuzzy snapshot may contain the child but stale parent metadata. Replay
    // repairs the parent without overwriting the newer complete child image.
    state.nodes.getPtr("/").?.internal_cversion = 0;
    state.nodes.getPtr("/").?.pzxid = 0;
    try applyCreateFields(&state, .{
        .clientId = 1,
        .cxid = 1,
        .zxid = 1,
        .time = 100,
        .type = 19,
    }, "/container", "replacement", &open_acl, .container, ephemeral.container_owner, 1);
    try testing.expectEqual(@as(i32, 1), state.nodes.get("/").?.internal_cversion);
    try testing.expectEqual(@as(i64, 1), state.nodes.get("/").?.pzxid);
    try testing.expectEqualStrings("", state.nodes.get("/container").?.data.?);

    var ttl_body = jute.Writer.init(testing.allocator);
    defer ttl_body.deinit();
    try jute.serialize(&ttl_body, protocol.txn.CreateTTLTxn{
        .path = "/ttl",
        .data = "ttl",
        .acl = &open_acl,
        .parentCVersion = 2,
        .ttl = 5_000,
    });
    var ttl_reader = jute.Reader.init(ttl_body.bytes());
    try applyTransaction(&state, .{
        .clientId = 1,
        .cxid = 2,
        .zxid = 2,
        .time = 200,
        .type = 21,
    }, &ttl_reader);
    const ttl_node = state.nodes.get("/ttl").?;
    try testing.expectEqual(ephemeral.NodeKind.ttl, ttl_node.kind);
    try testing.expectEqual(@as(i64, 5_000), try ephemeral.ttlValue(ttl_node.ephemeral_owner));

    var delete_body = jute.Writer.init(testing.allocator);
    defer delete_body.deinit();
    try jute.serialize(&delete_body, protocol.txn.DeleteTxn{ .path = "/container" });
    var delete_reader = jute.Reader.init(delete_body.bytes());
    try applyTransaction(&state, .{
        .clientId = 0,
        .cxid = 0,
        .zxid = 3,
        .time = 300,
        .type = 20,
    }, &delete_reader);
    try testing.expect(!state.nodes.contains("/container"));
}

test "imports a ZooKeeper snapshot and replays transaction logs" {
    const testing = std.testing;
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    try temporary.dir.createDirPath(testing.io, "source/version-2");
    try temporary.dir.createDirPath(testing.io, "target");
    const root_path = try temporary.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(root_path);
    const source_path = try std.fmt.allocPrint(testing.allocator, "{s}/source", .{root_path});
    defer testing.allocator.free(source_path);
    const target_path = try std.fmt.allocPrint(testing.allocator, "{s}/target", .{root_path});
    defer testing.allocator.free(target_path);

    const session_id: i64 = 0x123456789abcdef;
    var snapshot = jute.Writer.init(testing.allocator);
    defer snapshot.deinit();
    try snapshot.writeInt(snapshot_magic);
    try snapshot.writeInt(persistence_version);
    try snapshot.writeLong(-1);
    try snapshot.writeInt(1);
    try snapshot.writeLong(session_id);
    try snapshot.writeInt(30_000);
    try snapshot.writeInt(0);
    try snapshot.writeString("");
    try snapshot.writeBuffer("");
    try snapshot.writeLong(-1);
    try writePersistedStat(&snapshot, .{ .czxid = 0, .mzxid = 0, .ctime = 0, .mtime = 0, .version = 0, .cversion = 3, .aversion = 0, .ephemeralOwner = 0, .pzxid = 1 });
    try snapshot.writeString("/ephemeral");
    try snapshot.writeBuffer("snapshot");
    try snapshot.writeLong(-1);
    try writePersistedStat(&snapshot, .{ .czxid = 1, .mzxid = 1, .ctime = 100, .mtime = 100, .version = 0, .cversion = 0, .aversion = 0, .ephemeralOwner = session_id, .pzxid = 1 });
    try snapshot.writeString("/container");
    try snapshot.writeBuffer(null);
    try snapshot.writeLong(-1);
    try writePersistedStat(&snapshot, .{ .czxid = 1, .mzxid = 1, .ctime = 100, .mtime = 100, .version = 0, .cversion = 0, .aversion = 0, .ephemeralOwner = ephemeral.container_owner, .pzxid = 1 });
    try snapshot.writeString("/ttl");
    try snapshot.writeBuffer("ttl");
    try snapshot.writeLong(-1);
    try writePersistedStat(&snapshot, .{ .czxid = 1, .mzxid = 1, .ctime = 100, .mtime = 100, .version = 0, .cversion = 0, .aversion = 0, .ephemeralOwner = try ephemeral.ttlOwner(60_000), .pzxid = 1 });
    try snapshot.writeString("/");
    try writeTestSeal(&snapshot);
    try temporary.dir.writeFile(testing.io, .{
        .sub_path = "source/version-2/snapshot.1",
        .data = snapshot.bytes(),
    });

    var payload = jute.Writer.init(testing.allocator);
    defer payload.deinit();
    try jute.serialize(&payload, protocol.txn.TxnHeader{
        .clientId = session_id,
        .cxid = 1,
        .zxid = 2,
        .time = 200,
        .type = 5,
    });
    try jute.serialize(&payload, protocol.txn.SetDataTxn{
        .path = "/ephemeral",
        .data = "replayed",
        .version = 1,
    });
    var log = jute.Writer.init(testing.allocator);
    defer log.deinit();
    try log.writeInt(log_magic);
    try log.writeInt(persistence_version);
    try log.writeLong(0);
    try log.writeLong(@intCast(std.hash.Adler32.hash(payload.bytes())));
    try log.writeBuffer(payload.bytes());
    try log.writeByte(@bitCast(transaction_eor));
    try log.writeLong(0);
    try log.writeInt(0);
    try temporary.dir.writeFile(testing.io, .{
        .sub_path = "source/version-2/log.2",
        .data = log.bytes(),
    });

    try importOnFirstStart(testing.allocator, testing.io, .{
        .source_data_dir = source_path,
        .target_data_dir = target_path,
        .tick_grace_ms = 500,
    });
    // Import is a one-shot operation. Keeping the startup flag must fail once
    // the target directory contains the activated state.
    try testing.expectError(error.TargetDataDirectoryNotEmpty, importOnFirstStart(testing.allocator, testing.io, .{
        .source_data_dir = target_path,
        .target_data_dir = target_path,
        .tick_grace_ms = 500,
    }));
    const state_path = try std.fmt.allocPrint(testing.allocator, "{s}/state.rocksdb", .{target_path});
    defer testing.allocator.free(state_path);
    var store = try rocks_store.RocksStore.open(testing.allocator, state_path);
    defer store.deinit();
    const imported = (try store.getData(testing.allocator, "/ephemeral")).?;
    defer if (imported.data) |value| testing.allocator.free(value);
    try testing.expectEqualStrings("replayed", imported.data.?);
    try testing.expectEqual(@as(i32, 1), imported.stat.version);
    try testing.expectEqual(@as(i64, 0), (try store.exists("/container")).?.ephemeralOwner);
    try testing.expectEqual(@as(i64, 0), (try store.exists("/ttl")).?.ephemeralOwner);
    try testing.expectEqual(@as(i64, 2), (try store.importedSourceZxid()).?);
    const session = (try store.getSession(session_id)).?;
    try testing.expectEqualSlices(u8, &javaSessionPassword(session_id), &session.password);
    try testing.expectEqual(@as(i32, 30_000), session.timeout_ms);
    var created = try store.apply(.{ .create = .{
        .path = "/after-import",
        .data = "new",
        .time_ms = 300,
    } }, 1, 1);
    defer created.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 3), created.stat.?.czxid);
}

fn writePersistedStat(writer: *jute.Writer, stat: protocol.data.StatPersisted) !void {
    try jute.serialize(writer, stat);
}

fn writeTestSeal(writer: *jute.Writer) !void {
    const checksum = std.hash.Adler32.hash(writer.bytes());
    try writer.writeLong(@intCast(checksum));
    try writer.writeString("/");
}

test "first-start import requires an empty target data directory" {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    try temporary.dir.createDirPath(std.testing.io, "target/wal");
    const target_path = try temporary.dir.realPathFileAlloc(std.testing.io, "target", std.testing.allocator);
    defer std.testing.allocator.free(target_path);
    try std.testing.expectError(
        error.TargetDataDirectoryNotEmpty,
        ensureTargetDataDirectoryEmpty(std.testing.io, target_path),
    );

    try temporary.dir.createDirPath(std.testing.io, "stale/state.rocksdb.importing");
    const stale_path = try temporary.dir.realPathFileAlloc(std.testing.io, "stale", std.testing.allocator);
    defer std.testing.allocator.free(stale_path);
    try std.testing.expectError(
        error.TargetDataDirectoryNotEmpty,
        ensureTargetDataDirectoryEmpty(std.testing.io, stale_path),
    );
}

test "Java session password generation matches ZooKeeper" {
    const testing = std.testing;
    try testing.expectEqualSlices(u8, &.{
        0xf7, 0xcd, 0x1b, 0x8b, 0x3f, 0x94, 0x6c, 0x47,
        0x74, 0x25, 0xd4, 0x1b, 0xe3, 0xf6, 0x67, 0xc6,
    }, &javaSessionPassword(0x123456789abcdef));
}

test "persistence filenames use hexadecimal zxids" {
    const testing = std.testing;
    const snapshot = parseCandidate("snapshot.1a.gz", "snapshot", true).?;
    try testing.expectEqual(@as(i64, 0x1a), snapshot.zxid);
    try testing.expectEqual(Compression.gzip, snapshot.compression);
    try testing.expect(parseCandidate("snapshot.bad.ext", "snapshot", true) == null);
    try testing.expect(parseCandidate("log.10.gz", "log", false) == null);
}
