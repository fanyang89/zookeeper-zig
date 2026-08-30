const std = @import("std");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");
const acl = @import("acl.zig");
const data_tree = @import("data_tree.zig");
const rocks_store = @import("rocks_store.zig");

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
    if (try targetImportAlreadyComplete(allocator, io, options.target_data_dir)) return;
    var state = try loadSource(allocator, io, options.source_data_dir, options.source_log_dir);
    defer state.deinit();
    try finalizeState(&state);

    const staging_path = try std.fmt.allocPrint(allocator, "{s}/state.rocksdb.importing", .{options.target_data_dir});
    defer allocator.free(staging_path);
    const final_path = try std.fmt.allocPrint(allocator, "{s}/state.rocksdb", .{options.target_data_dir});
    defer allocator.free(final_path);
    try std.Io.Dir.cwd().deleteTree(io, staging_path);
    errdefer std.Io.Dir.cwd().deleteTree(io, staging_path) catch {};

    var store = try rocks_store.RocksStore.open(allocator, staging_path);
    {
        defer store.deinit();
        const nodes = try materializeNodes(allocator, &state);
        defer allocator.free(nodes);
        const sessions = try materializeSessions(allocator, &state);
        defer allocator.free(sessions);
        try store.installImported(nodes, sessions, options.tick_grace_ms, state.source_zxid);
    }
    const cwd = std.Io.Dir.cwd();
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
        error.UnsupportedSnappySnapshot,
        error.UnsupportedAclScheme,
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
        .snappy => return error.UnsupportedSnappySnapshot,
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
        if (session_id == 0 or timeout_ms <= 0) return error.InvalidSession;
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
        const node = ImportedNode{
            .data = if (maybe_data) |value| try allocator.dupe(u8, value) else null,
            .acl_blob = acl_blob,
            .czxid = try reader.readLong(),
            .mzxid = try reader.readLong(),
            .ctime = try reader.readLong(),
            .mtime = try reader.readLong(),
            .version = try reader.readInt(),
            .internal_cversion = try reader.readInt(),
            .aversion = try reader.readInt(),
            .ephemeral_owner = try reader.readLong(),
            .pzxid = try reader.readLong(),
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
            if (header.clientId == 0 or txn.timeOut <= 0) return error.InvalidSession;
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
        19, 20, 21 => return error.UnsupportedExtendedNodeType,
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
        return applyCreateFields(state, header, legacy.path, legacy.data, legacy.acl, legacy.ephemeral, -1);
    };
    defer jute.deinitDecoded(current, state.allocator);
    if (!hasValidDigestRemainder(reader.remaining())) {
        reader.* = saved;
        const legacy = try jute.deserialize(protocol.txn.CreateTxnV0, reader, state.allocator);
        defer jute.deinitDecoded(legacy, state.allocator);
        return applyCreateFields(state, header, legacy.path, legacy.data, legacy.acl, legacy.ephemeral, -1);
    }
    try applyCreateFields(state, header, current.path, current.data, current.acl, current.ephemeral, current.parentCVersion);
}

fn applyCreateFields(
    state: *ImportState,
    header: protocol.txn.TxnHeader,
    maybe_path: ?[]const u8,
    data: ?[]const u8,
    entries: ?[]const protocol.data.ACL,
    ephemeral: bool,
    parent_cversion: i32,
) !void {
    const path = maybe_path orelse return error.InvalidTransaction;
    if (!data_tree.isValidPath(path) or std.mem.eql(u8, path, "/")) return error.InvalidPath;
    try validateImportedAclSchemes(entries);
    const blob = try acl.normalize(state.allocator, entries, &.{});
    errdefer state.allocator.free(blob);
    if (state.nodes.getPtr(path)) |existing| {
        if (existing.acl_blob) |value| state.allocator.free(value);
        existing.acl_blob = blob;
        return;
    }
    const parent_path = data_tree.parentPath(path) orelse return error.InvalidPath;
    const parent = state.nodes.getPtr(parent_path) orelse return error.MissingParent;
    const next_cversion = if (parent_cversion == -1) parent.internal_cversion +% 1 else parent_cversion;
    if (next_cversion > parent.internal_cversion) {
        parent.internal_cversion = next_cversion;
        parent.pzxid = header.zxid;
    }
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
        .ephemeral_owner = if (ephemeral) header.clientId else 0,
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
    while (nodes.next()) |entry| if (entry.value_ptr.ephemeral_owner == session_id) {
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
            19, 20, 21 => return error.UnsupportedExtendedNodeType,
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
        if (owner == std.math.minInt(i64)) return error.UnsupportedExtendedNodeType;
        if (owner != 0 and !state.sessions.contains(owner) and
            (@as(u64, @bitCast(owner)) >> 56) == 0xff)
        {
            return error.UnsupportedExtendedNodeType;
        }
        if (owner != 0 and !state.sessions.contains(owner)) {
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
        if (entry.value_ptr.ephemeral_owner != 0 and entry.value_ptr.child_count != 0) {
            return error.EphemeralHasChildren;
        }
        const child_count: i32 = std.math.cast(i32, entry.value_ptr.child_count) orelse
            return error.TooManyChildren;
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

fn targetImportAlreadyComplete(
    allocator: std.mem.Allocator,
    io: std.Io,
    target_data_dir: []const u8,
) !bool {
    try std.Io.Dir.cwd().createDirPath(io, target_data_dir);
    const state_path = try std.fmt.allocPrint(allocator, "{s}/state.rocksdb", .{target_data_dir});
    defer allocator.free(state_path);
    const state_exists = blk: {
        std.Io.Dir.cwd().access(io, state_path, .{}) catch |err| switch (err) {
            error.FileNotFound => break :blk false,
            else => return err,
        };
        break :blk true;
    };
    if (state_exists) {
        var store = try rocks_store.RocksStore.open(allocator, state_path);
        defer store.deinit();
        if (try store.importedSourceZxid() != null) return true;
        return error.TargetStateAlreadyExists;
    }
    var target_dir = try openConfiguredDir(io, target_data_dir);
    defer target_dir.close(io);
    var iterator = target_dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (std.mem.eql(u8, entry.name, "state.rocksdb.importing")) continue;
        return error.TargetDataDirectoryNotEmpty;
    }
    return false;
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
    try writePersistedStat(&snapshot, .{ .czxid = 0, .mzxid = 0, .ctime = 0, .mtime = 0, .version = 0, .cversion = 1, .aversion = 0, .ephemeralOwner = 0, .pzxid = 1 });
    try snapshot.writeString("/ephemeral");
    try snapshot.writeBuffer("snapshot");
    try snapshot.writeLong(-1);
    try writePersistedStat(&snapshot, .{ .czxid = 1, .mzxid = 1, .ctime = 100, .mtime = 100, .version = 0, .cversion = 0, .aversion = 0, .ephemeralOwner = session_id, .pzxid = 1 });
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
    // Keeping the startup flag is idempotent after the import marker is durable.
    try importOnFirstStart(testing.allocator, testing.io, .{
        .source_data_dir = source_path,
        .target_data_dir = target_path,
        .tick_grace_ms = 500,
    });
    const state_path = try std.fmt.allocPrint(testing.allocator, "{s}/state.rocksdb", .{target_path});
    defer testing.allocator.free(state_path);
    var store = try rocks_store.RocksStore.open(testing.allocator, state_path);
    defer store.deinit();
    const imported = (try store.getData(testing.allocator, "/ephemeral")).?;
    defer if (imported.data) |value| testing.allocator.free(value);
    try testing.expectEqualStrings("replayed", imported.data.?);
    try testing.expectEqual(@as(i32, 1), imported.stat.version);
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

test "first-start import rejects an existing Raft data directory" {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    try temporary.dir.createDirPath(std.testing.io, "target/wal");
    const target_path = try temporary.dir.realPathFileAlloc(std.testing.io, "target", std.testing.allocator);
    defer std.testing.allocator.free(target_path);
    try std.testing.expectError(
        error.TargetDataDirectoryNotEmpty,
        targetImportAlreadyComplete(std.testing.allocator, std.testing.io, target_path),
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
