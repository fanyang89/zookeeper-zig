// Fuzzer suite for the Apache ZooKeeper data import parser.
//
// The import path parses untrusted upstream snapshot and transaction-log
// files. This suite generates structure-aware valid images with the Jute
// writer, applies deterministic mutations, and feeds the results to the
// byte-level parser entry points exposed by
// `zookeeper.server.zookeeper_import`. Any panic, signal, timeout, or
// allocator leak is a finding. Orchestration lives in `fuzz_engine.zig`.

const std = @import("std");
const zookeeper = @import("zookeeper");
const engine = @import("fuzz_engine.zig");

const import_mod = zookeeper.server.zookeeper_import;
const jute = zookeeper.jute;
const protocol = zookeeper.protocol;
const ephemeral = zookeeper.server.ephemeral;

pub fn main(init: std.process.Init) anyerror!void {
    return engine.mainFor("import", &defs)(init);
}

const defs = [_]engine.TargetDef{
    .{ .name = "snapshot", .build = buildSnapshot, .run = runSnapshot },
    .{ .name = "txnlog", .build = buildTxnlog, .run = runTxnlog },
    .{ .name = "snappy", .build = buildSnappy, .run = runSnappy },
    .{ .name = "gzip", .build = buildGzip, .run = runGzip },
};

// ---------------------------------------------------------------------------
// Input generation
// ---------------------------------------------------------------------------

fn randomSessionId(random: std.Random) i64 {
    const bits = random.intRangeAtMost(u64, 1, std.math.maxInt(i64));
    return @bitCast(bits);
}

fn openAclEntries() []const protocol.data.ACL {
    return &[_]protocol.data.ACL{.{
        .perms = 31,
        .id = .{ .scheme = "world", .id = "anyone" },
    }};
}

fn writeStat(writer: *jute.Writer, czxid: i64, mzxid: i64, owner: i64) !void {
    try jute.serialize(writer, protocol.data.StatPersisted{
        .czxid = czxid,
        .mzxid = mzxid,
        .ctime = 1_000,
        .mtime = 1_000,
        .version = 0,
        .cversion = 0,
        .aversion = 0,
        .ephemeralOwner = owner,
        .pzxid = czxid,
    });
}

fn appendSeal(writer: *jute.Writer) !usize {
    const start = writer.bytes().len;
    const checksum = std.hash.Adler32.hash(writer.bytes());
    try writer.writeLong(@intCast(checksum));
    try writer.writeString("/");
    return start;
}

const SnapshotModel = struct {
    paths: std.ArrayList([]const u8) = .empty,
    sessions: std.ArrayList(i64) = .empty,
    ephemeral_paths: std.ArrayList(struct { path: []const u8, owner: i64 }) = .empty,
    filename_zxid: i64,
};

const SnapshotSeed = struct {
    bytes: std.ArrayList(u8) = .empty,
    payload_spans: std.ArrayList(engine.Span) = .empty,
    seal_offset: ?usize = null,
    filename_zxid: i64 = 0,
};

/// Builds a fully valid snapshot image modeled on the upstream serializer.
fn generateSnapshotSeed(
    arena: std.mem.Allocator,
    random: std.Random,
) !struct { seed: SnapshotSeed, model: SnapshotModel } {
    var seed = SnapshotSeed{};
    var model = SnapshotModel{ .filename_zxid = random.intRangeAtMost(i64, 1, 1_000) };

    var writer = jute.Writer.init(arena);
    defer writer.deinit();

    try writer.writeInt(import_mod.snapshot_magic);
    try writer.writeInt(import_mod.persistence_version);
    try writer.writeLong(-1);

    const session_count = random.intRangeAtMost(usize, 0, 3);
    try writer.writeInt(@intCast(session_count));
    for (0..session_count) |_| {
        const session_id = randomSessionId(random);
        try model.sessions.append(arena, session_id);
        try writer.writeLong(session_id);
        try writer.writeInt(random.intRangeAtMost(i32, 1_000, 60_000));
    }

    // ACL cache with digest and ip identities.
    const acl_count = random.intRangeAtMost(usize, 0, 2);
    try writer.writeInt(@intCast(acl_count));
    for (1..acl_count + 1) |index| {
        try writer.writeLong(@intCast(index));
        try writer.writeInt(1);
        try writer.writeInt(31);
        const scheme = if (index % 2 == 0) "digest" else "ip";
        try writer.writeString(scheme);
        try writer.writeString(if (index % 2 == 0) "user:pass" else "127.0.0.1");
    }

    const node_span_start = writer.bytes().len;

    // Root node uses the empty-string path.
    try writer.writeString("");
    try writer.writeBuffer("");
    try writer.writeLong(if (acl_count == 0) -1 else 1);
    try writeStat(&writer, 0, 0, 0);
    try model.paths.append(arena, "/");

    const child_count = random.intRangeAtMost(usize, 1, 5);
    for (0..child_count) |index| {
        const path = try std.fmt.allocPrint(arena, "/n{d}", .{index});
        try model.paths.append(arena, path);
        const kind_roll = random.intRangeAtMost(u8, 0, 9);
        var owner: i64 = 0;
        if (kind_roll == 1 and model.sessions.items.len > 0) {
            owner = model.sessions.items[random.uintLessThan(usize, model.sessions.items.len)];
            try model.ephemeral_paths.append(arena, .{ .path = path, .owner = owner });
        } else if (kind_roll == 2) {
            owner = ephemeral.container_owner;
        } else if (kind_roll == 3) {
            owner = try ephemeral.ttlOwner(random.intRangeAtMost(i64, 1, 600_000));
        }
        try writer.writeString(path);
        const data_len = random.intRangeAtMost(usize, 0, 24);
        if (data_len == 0) {
            try writer.writeBuffer("");
        } else {
            const data = try arena.alloc(u8, data_len);
            random.bytes(data);
            try writer.writeBuffer(data);
        }
        try writer.writeLong(if (acl_count == 0) -1 else 1);
        const czxid = random.intRangeAtMost(i64, 1, 1_000);
        try writeStat(&writer, czxid, czxid, owner);

        // One grandchild under non-ephemeral nodes.
        if (owner == 0 and random.boolean()) {
            const child_path = try std.fmt.allocPrint(arena, "{s}/c", .{path});
            try model.paths.append(arena, child_path);
            try writer.writeString(child_path);
            try writer.writeBuffer("child");
            try writer.writeLong(-1);
            const child_zxid = random.intRangeAtMost(i64, 1, 1_000);
            try writeStat(&writer, child_zxid, child_zxid, 0);
        }
    }

    // Snapshot terminator and seal.
    try writer.writeString("/");
    seed.seal_offset = try appendSeal(&writer);

    // Optional 21-byte soft digest trailer: long + second seal.
    if (random.boolean()) {
        try writer.writeLong(0);
        seed.seal_offset = try appendSeal(&writer);
    }

    try seed.payload_spans.append(arena, .{ .start = node_span_start, .len = writer.bytes().len - node_span_start });
    seed.filename_zxid = model.filename_zxid;
    try seed.bytes.appendSlice(arena, writer.bytes());
    return .{ .seed = seed, .model = model };
}

fn writeLogEnvelope(arena: std.mem.Allocator, log: *jute.Writer, payload: []const u8, spans: *std.ArrayList(engine.Span)) !void {
    const checksum = std.hash.Adler32.hash(payload);
    const envelope_start = log.bytes().len;
    try log.writeLong(@intCast(checksum));
    try log.writeBuffer(payload);
    try log.writeByte(@bitCast(import_mod.transaction_eor));
    try spans.append(arena, .{ .start = envelope_start + 12, .len = payload.len });
}

fn writeTxnHeader(payload: *jute.Writer, client_id: i64, cxid: i32, zxid: i64, txn_type: i32) !void {
    try jute.serialize(payload, protocol.txn.TxnHeader{
        .clientId = client_id,
        .cxid = cxid,
        .zxid = zxid,
        .time = 1_000,
        .type = txn_type,
    });
}

/// Builds a valid log image referencing paths from `model`.
fn generateLogSeed(arena: std.mem.Allocator, random: std.Random, model: *SnapshotModel, out: *SnapshotSeed) !void {
    var log = jute.Writer.init(arena);
    defer log.deinit();

    try log.writeInt(import_mod.log_magic);
    try log.writeInt(import_mod.persistence_version);
    try log.writeLong(0);

    var next_zxid = model.filename_zxid + 1;
    var created: std.ArrayList([]const u8) = .empty;
    var seq: usize = 0;

    const record_count = random.intRangeAtMost(usize, 1, 8);
    for (0..record_count) |record_index| {
        const kind_roll = random.intRangeAtMost(u8, 0, 9);

        var payload = jute.Writer.init(arena);
        defer payload.deinit();
        const client_id: i64 = if (model.sessions.items.len > 0)
            model.sessions.items[random.uintLessThan(usize, model.sessions.items.len)]
        else
            randomSessionId(random);

        if (kind_roll <= 2) {
            // create, create2, createContainer, or createTTL.
            const path = try std.fmt.allocPrint(arena, "/log{d}", .{seq});
            seq += 1;
            const txn_type: i32 = switch (random.intRangeAtMost(u8, 0, 3)) {
                0 => 1,
                1 => 15,
                2 => 19,
                else => 21,
            };
            try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, txn_type);
            switch (txn_type) {
                19 => try jute.serialize(&payload, protocol.txn.CreateContainerTxn{
                    .path = path,
                    .data = "",
                    .acl = openAclEntries(),
                    .parentCVersion = -1,
                }),
                21 => try jute.serialize(&payload, protocol.txn.CreateTTLTxn{
                    .path = path,
                    .data = "",
                    .acl = openAclEntries(),
                    .parentCVersion = -1,
                    .ttl = random.intRangeAtMost(i64, 1, 600_000),
                }),
                else => try jute.serialize(&payload, protocol.txn.CreateTxn{
                    .path = path,
                    .data = "",
                    .acl = openAclEntries(),
                    .ephemeral = false,
                    .parentCVersion = -1,
                }),
            }
            try created.append(arena, path);
        } else if (kind_roll == 3) {
            const path = if (created.items.len > 0) blk: {
                break :blk created.items[random.uintLessThan(usize, created.items.len)];
            } else "/";
            try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, 2);
            try jute.serialize(&payload, protocol.txn.DeleteTxn{ .path = path });
        } else if (kind_roll == 4) {
            const path = if (model.paths.items.len > 0)
                model.paths.items[random.uintLessThan(usize, model.paths.items.len)]
            else
                "/";
            try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, 5);
            try jute.serialize(&payload, protocol.txn.SetDataTxn{ .path = path, .data = "v", .version = 0 });
        } else if (kind_roll == 5) {
            const path = if (model.paths.items.len > 0)
                model.paths.items[random.uintLessThan(usize, model.paths.items.len)]
            else
                "/";
            try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, 7);
            try jute.serialize(&payload, protocol.txn.SetACLTxn{
                .path = path,
                .acl = openAclEntries(),
                .version = 0,
            });
        } else if (kind_roll == 6) {
            try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, 14);
            var nested_create = jute.Writer.init(arena);
            defer nested_create.deinit();
            try jute.serialize(&nested_create, protocol.txn.CreateTxn{
                .path = "/multin",
                .data = "",
                .acl = openAclEntries(),
                .ephemeral = false,
                .parentCVersion = -1,
            });
            var nested_delete = jute.Writer.init(arena);
            defer nested_delete.deinit();
            try jute.serialize(&nested_delete, protocol.txn.DeleteTxn{ .path = "/n0" });
            const entries = try arena.alloc(protocol.txn.Txn, 2);
            entries[0] = .{ .type = 1, .data = nested_create.bytes() };
            entries[1] = .{ .type = 2, .data = nested_delete.bytes() };
            try jute.serialize(&payload, protocol.txn.MultiTxn{ .txns = entries });
        } else if (kind_roll == 7) {
            try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, -11);
            if (random.boolean() and model.ephemeral_paths.items.len > 0) {
                const victim = model.ephemeral_paths.items[random.uintLessThan(usize, model.ephemeral_paths.items.len)];
                const paths = try arena.alloc(?[]const u8, 1);
                paths[0] = victim.path;
                try jute.serialize(&payload, protocol.txn.CloseSessionTxn{ .paths2Delete = paths });
            }
        } else if (kind_roll == 8) {
            const session_id = randomSessionId(random);
            try writeTxnHeader(&payload, session_id, @intCast(record_index), next_zxid, -10);
            try jute.serialize(&payload, protocol.txn.CreateSessionTxn{ .timeOut = 30_000 });
        } else {
            try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, -1);
            try jute.serialize(&payload, protocol.txn.ErrorTxn{ .err = -101 });
        }
        try writeLogEnvelope(arena, &log, payload.bytes(), &out.payload_spans);
        next_zxid += 1;
    }

    if (random.boolean()) {
        try log.writeLong(0);
        try log.writeInt(0);
    }

    out.filename_zxid = model.filename_zxid + 1;
    try out.bytes.appendSlice(arena, log.bytes());
}

fn generateSnappyChunk(arena: std.mem.Allocator, random: std.Random, out: *std.ArrayList(u8)) !void {
    const data_len = random.intRangeAtMost(usize, 1, 60);
    const data = try arena.alloc(u8, data_len);
    random.bytes(data);

    // Snappy frame: varint uncompressed length + literal tag + bytes.
    var value: u64 = data.len;
    while (true) {
        var byte: u8 = @truncate(value & 0x7f);
        value >>= 7;
        if (value != 0) byte |= 0x80;
        try out.append(arena, byte);
        if (value == 0) break;
    }
    try out.append(arena, @intCast((data.len - 1) << 2));
    try out.appendSlice(arena, data);
}

fn generateSnappySeed(arena: std.mem.Allocator, random: std.Random) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(arena, &[_]u8{ 0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0 });
    try out.appendSlice(arena, &[_]u8{ 0, 0, 0, 1 });
    try out.appendSlice(arena, &[_]u8{ 0, 0, 0, 1 });

    const chunk_count = random.intRangeAtMost(usize, 1, 5);
    for (0..chunk_count) |_| {
        var chunk: std.ArrayList(u8) = .empty;
        try generateSnappyChunk(arena, random, &chunk);
        try out.append(arena, 0);
        try out.append(arena, 0);
        try out.append(arena, @intCast((chunk.items.len >> 8) & 0xff));
        try out.append(arena, @intCast(chunk.items.len & 0xff));
        try out.appendSlice(arena, chunk.items);
    }
    return out.items;
}

fn generateGzipSeed(arena: std.mem.Allocator, random: std.Random) ![]u8 {
    const data_len = random.intRangeAtMost(usize, 0, 3_000);
    const data = try arena.alloc(u8, data_len);
    random.bytes(data);

    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(arena, &[_]u8{ 0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0x03 });

    var offset: usize = 0;
    while (true) {
        const remaining = data.len - offset;
        const chunk_len: usize = @min(remaining, 65_535);
        const last = offset + chunk_len == data.len;
        try out.append(arena, if (last) 1 else 0);
        try out.append(arena, @truncate(chunk_len));
        try out.append(arena, @truncate(chunk_len >> 8));
        const nlen: u16 = ~@as(u16, @truncate(chunk_len));
        try out.append(arena, @truncate(nlen));
        try out.append(arena, @truncate(nlen >> 8));
        try out.appendSlice(arena, data[offset .. offset + chunk_len]);
        offset += chunk_len;
        if (last) break;
    }

    const crc = std.hash.Crc32.hash(data);
    try out.append(arena, @truncate(crc));
    try out.append(arena, @truncate(crc >> 8));
    try out.append(arena, @truncate(crc >> 16));
    try out.append(arena, @truncate(crc >> 24));
    const isize_: u32 = @truncate(data.len);
    try out.append(arena, @truncate(isize_));
    try out.append(arena, @truncate(isize_ >> 8));
    try out.append(arena, @truncate(isize_ >> 16));
    try out.append(arena, @truncate(isize_ >> 24));
    return out.items;
}

// ---------------------------------------------------------------------------
// Target definitions
// ---------------------------------------------------------------------------

fn buildSnapshot(arena: std.mem.Allocator, random: std.Random, donors: []const []const u8) anyerror!engine.Input {
    _ = donors;
    const generated = try generateSnapshotSeed(arena, random);
    return .{
        .bytes = generated.seed.bytes.items,
        .spans = generated.seed.payload_spans.items,
        .seal_offset = generated.seed.seal_offset,
        .aux_i64 = generated.seed.filename_zxid,
    };
}

fn runSnapshot(allocator: std.mem.Allocator, input: engine.Input) anyerror!void {
    var state = try import_mod.parseSnapshotData(allocator, input.bytes, input.aux_i64);
    defer state.deinit();
    try import_mod.finalizeState(&state);
}

fn fixedBaseSnapshot(arena: std.mem.Allocator) !struct { bytes: []u8, zxid: i64 } {
    var prng = std.Random.DefaultPrng.init(0);
    const generated = try generateSnapshotSeed(arena, prng.random());
    return .{ .bytes = generated.seed.bytes.items, .zxid = generated.model.filename_zxid };
}

fn buildTxnlog(arena: std.mem.Allocator, random: std.Random, donors: []const []const u8) anyerror!engine.Input {
    _ = donors;
    const generated = try generateSnapshotSeed(arena, random);
    var model = generated.model;
    var log_seed = SnapshotSeed{};
    try generateLogSeed(arena, random, &model, &log_seed);
    return .{
        .bytes = log_seed.bytes.items,
        .spans = log_seed.payload_spans.items,
        .aux = generated.seed.bytes.items,
        .aux_i64 = generated.seed.filename_zxid,
    };
}

fn runTxnlog(allocator: std.mem.Allocator, input: engine.Input) anyerror!void {
    var base_arena = std.heap.ArenaAllocator.init(allocator);
    defer base_arena.deinit();
    const base = input.aux orelse blk: {
        const fixed = try fixedBaseSnapshot(base_arena.allocator());
        break :blk fixed.bytes;
    };
    const state = import_mod.parseSnapshotData(allocator, base, input.aux_i64) catch return;
    var imported = state;
    defer imported.deinit();
    try import_mod.replayLog(input.bytes, input.aux_i64 + 1, &imported);
    try import_mod.finalizeState(&imported);
}

fn buildSnappy(arena: std.mem.Allocator, random: std.Random, donors: []const []const u8) anyerror!engine.Input {
    _ = donors;
    return .{ .bytes = try generateSnappySeed(arena, random) };
}

fn runSnappy(allocator: std.mem.Allocator, input: engine.Input) anyerror!void {
    const decoded = try import_mod.decompressSnappy(allocator, input.bytes);
    allocator.free(decoded);
}

fn buildGzip(arena: std.mem.Allocator, random: std.Random, donors: []const []const u8) anyerror!engine.Input {
    _ = donors;
    return .{ .bytes = try generateGzipSeed(arena, random) };
}

fn runGzip(allocator: std.mem.Allocator, input: engine.Input) anyerror!void {
    const decoded = try import_mod.decompressGzip(allocator, input.bytes);
    allocator.free(decoded);
}
