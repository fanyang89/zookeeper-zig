// Fuzzer for the Apache ZooKeeper data import parser.
//
// The import path parses untrusted upstream snapshot and transaction-log
// files. This harness generates structure-aware valid inputs, applies
// deterministic mutations, and feeds the results to the byte-level parser
// entry points exposed by `zookeeper.server.zookeeper_import`. Any panic,
// signal, timeout, or allocator leak is a finding.
//
// Usage:
//   zq-fuzz-import [--target T,...] [--iterations N] [--seed S]
//                  [--batch N] [--batch-timeout-ms N]
//                  [--corpus DIR] [--keep-going] [--crash-dir DIR]
//   zq-fuzz-import --replay FILE_OR_DIR...
//   zq-fuzz-import --save-corpus [COUNT] [--corpus DIR]
//
// Targets: snapshot, txnlog, snappy, gzip. The default runs all of them.
// Each input is a pure function of (seed, target, index), so any reported
// index reproduces exactly with the same seed.

const std = @import("std");
const zookeeper = @import("zookeeper");

const import_mod = zookeeper.server.zookeeper_import;
const jute = zookeeper.jute;
const protocol = zookeeper.protocol;
const ephemeral = zookeeper.server.ephemeral;

const default_corpus_root = "tests/fuzz/corpus/import";
const default_iterations_per_target = 20_000;
const default_batch_size = 128;
const default_batch_timeout_ms = 60_000;
const default_input_timeout_ms = 5_000;
const max_input_bytes = 64 * 1024;
const max_saved_bytes = 64 * 1024;
const max_saved_per_target = 100;
const max_donors = 64;

const Target = enum {
    snapshot,
    txnlog,
    snappy,
    gzip,

    fn name(self: Target) []const u8 {
        return @tagName(self);
    }

    fn fromString(value: []const u8) ?Target {
        inline for (@typeInfo(Target).@"enum".fields) |field| {
            if (std.mem.eql(u8, value, field.name)) return @enumFromInt(field.value);
        }
        return null;
    }
};

const all_targets = [_]Target{ .snapshot, .txnlog, .snappy, .gzip };

const Config = struct {
    targets: []const Target,
    iterations: usize,
    seed: u64,
    batch_size: usize,
    batch_timeout_ms: u64,
    input_timeout_ms: u64,
    corpus_root: []const u8,
    crash_dir: []const u8,
    keep_going: bool,
};

const usage =
    \\usage: zq-fuzz-import [options]
    \\
    \\options:
    \\  --target LIST            comma-separated targets (default: all)
    \\  --iterations N           inputs per target (default 20000)
    \\  --seed N                 fuzz PRNG seed (default: random)
    \\  --batch N                inputs per worker process (default 128)
    \\  --batch-timeout-ms N     worker wall-clock budget (default 60000)
    \\  --input-timeout-ms N     per-input budget while bisecting (default 5000)
    \\  --corpus DIR             corpus root (default tests/fuzz/corpus/import)
    \\  --crash-dir DIR          crash output directory (default <corpus>/crashes)
    \\  --keep-going             continue after a finding (default: stop)
    \\  --replay PATH...         run files (or dirs) once in-process, then exit
    \\  --save-corpus [N]        run N inputs per target in-process, saving one
    \\                           input per distinct outcome to the corpus
    \\  --worker T START COUNT S internal: run a deterministic batch
    \\
;

// ---------------------------------------------------------------------------
// Input generation
// ---------------------------------------------------------------------------

const Span = struct { start: usize, len: usize };

/// A generated seed plus the metadata needed for structure-aware mutation.
const Seed = struct {
    bytes: std.ArrayList(u8) = .empty,
    /// Envelope-preserving mutation targets (log record payloads, snapshot
    /// node bodies).
    payload_spans: std.ArrayList(Span) = .empty,
    /// Byte offset of the snapshot seal checksum, when present.
    seal_offset: ?usize = null,
    /// Parsed filename zxid for snapshot inputs.
    filename_zxid: i64 = 0,
    /// Base snapshot for txnlog inputs (generated, never mutated).
    base_snapshot: ?[]const u8 = null,

    fn deinit(self: *Seed, allocator: std.mem.Allocator) void {
        self.bytes.deinit(allocator);
        self.payload_spans.deinit(allocator);
        if (self.base_snapshot) |value| allocator.free(value);
        self.* = undefined;
    }
};

fn randomSessionId(random: std.Random) i64 {
    const bits = random.intRangeAtMost(u64, 1, std.math.maxInt(i64));
    return @bitCast(bits);
}

fn openAclEntries(arena: std.mem.Allocator, random: std.Random) []const protocol.data.ACL {
    _ = arena;
    _ = random;
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

fn appendSeal(writer: *jute.Writer, seed: *Seed) !void {
    const start = writer.bytes().len;
    const checksum = std.hash.Adler32.hash(writer.bytes());
    seed.seal_offset = start;
    try writer.writeLong(@intCast(checksum));
    try writer.writeString("/");
}

const SnapshotModel = struct {
    paths: std.ArrayList([]const u8) = .empty,
    sessions: std.ArrayList(i64) = .empty,
    ephemeral_paths: std.ArrayList(struct { path: []const u8, owner: i64 }) = .empty,
    filename_zxid: i64,

    fn deinit(self: *SnapshotModel, allocator: std.mem.Allocator) void {
        for (self.paths.items) |path| allocator.free(path);
        self.paths.deinit(allocator);
        self.sessions.deinit(allocator);
        self.ephemeral_paths.deinit(allocator);
        self.* = undefined;
    }
};

/// Builds a fully valid snapshot image modeled on the upstream serializer.
fn generateSnapshotSeed(allocator: std.mem.Allocator, random: std.Random) !struct { seed: Seed, model: SnapshotModel } {
    var seed = Seed{};
    errdefer seed.deinit(allocator);
    var model = SnapshotModel{ .filename_zxid = random.intRangeAtMost(i64, 1, 1_000) };
    errdefer model.deinit(allocator);

    var writer = jute.Writer.init(allocator);
    defer writer.deinit();

    try writer.writeInt(import_mod.snapshot_magic);
    try writer.writeInt(import_mod.persistence_version);
    try writer.writeLong(-1);

    const session_count = random.intRangeAtMost(usize, 0, 3);
    try writer.writeInt(@intCast(session_count));
    for (0..session_count) |_| {
        const session_id = randomSessionId(random);
        try model.sessions.append(allocator, session_id);
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
    try model.paths.append(allocator, try allocator.dupe(u8, "/"));

    const child_count = random.intRangeAtMost(usize, 1, 5);
    for (0..child_count) |index| {
        const path = try std.fmt.allocPrint(allocator, "/n{d}", .{index});
        try model.paths.append(allocator, path);
        const kind_roll = random.intRangeAtMost(u8, 0, 9);
        var owner: i64 = 0;
        if (kind_roll == 1 and model.sessions.items.len > 0) {
            owner = model.sessions.items[random.uintLessThan(usize, model.sessions.items.len)];
            try model.ephemeral_paths.append(allocator, .{ .path = path, .owner = owner });
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
            const data = try allocator.alloc(u8, data_len);
            defer allocator.free(data);
            random.bytes(data);
            try writer.writeBuffer(data);
        }
        try writer.writeLong(if (acl_count == 0) -1 else 1);
        const czxid = random.intRangeAtMost(i64, 1, 1_000);
        try writeStat(&writer, czxid, czxid, owner);

        // One grandchild under non-ephemeral nodes.
        if (owner == 0 and random.boolean()) {
            const child_path = try std.fmt.allocPrint(allocator, "{s}/c", .{path});
            try model.paths.append(allocator, child_path);
            try writer.writeString(child_path);
            try writer.writeBuffer("child");
            try writer.writeLong(-1);
            const child_zxid = random.intRangeAtMost(i64, 1, 1_000);
            try writeStat(&writer, child_zxid, child_zxid, 0);
        }
    }

    // Snapshot terminator.
    try writer.writeString("/");
    try appendSeal(&writer, &seed);

    // Optional 21-byte soft digest trailer: long + second seal.
    if (random.boolean()) {
        try writer.writeLong(0);
        const start = writer.bytes().len;
        const checksum = std.hash.Adler32.hash(writer.bytes());
        try writer.writeLong(@intCast(checksum));
        try writer.writeString("/");
        seed.seal_offset = start;
    }

    try seed.payload_spans.append(allocator, .{ .start = node_span_start, .len = writer.bytes().len - node_span_start });
    seed.filename_zxid = model.filename_zxid;
    try seed.bytes.appendSlice(allocator, writer.bytes());
    return .{ .seed = seed, .model = model };
}

const LogRecordKind = enum { create, delete, set_data, multi, close_session, create_session, error_txn, container, ttl, set_acl };

fn writeLogEnvelope(allocator: std.mem.Allocator, log: *jute.Writer, payload: []const u8, seed: *Seed) !void {
    const checksum = std.hash.Adler32.hash(payload);
    const envelope_start = log.bytes().len;
    try log.writeLong(@intCast(checksum));
    try log.writeBuffer(payload);
    try log.writeByte(@bitCast(import_mod.transaction_eor));
    try seed.payload_spans.append(allocator, .{
        .start = envelope_start + 12,
        .len = payload.len,
    });
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
fn generateLogSeed(allocator: std.mem.Allocator, random: std.Random, model: *SnapshotModel) !Seed {
    var seed = Seed{};
    errdefer seed.deinit(allocator);
    var log = jute.Writer.init(allocator);
    defer log.deinit();

    try log.writeInt(import_mod.log_magic);
    try log.writeInt(import_mod.persistence_version);
    try log.writeLong(0);

    var next_zxid = model.filename_zxid + 1;
    var created: std.ArrayList([]const u8) = .empty;
    defer created.deinit(allocator);
    var seq: usize = 0;

    const record_count = random.intRangeAtMost(usize, 1, 8);
    for (0..record_count) |record_index| {
        const kind_roll = random.intRangeAtMost(u8, 0, 9);
        var kind: LogRecordKind = .create;
        if (kind_roll == 1) kind = .delete;
        if (kind_roll == 2) kind = .set_data;
        if (kind_roll == 3) kind = .multi;
        if (kind_roll == 4) kind = .close_session;
        if (kind_roll == 5) kind = .create_session;
        if (kind_roll == 6) kind = .error_txn;
        if (kind_roll == 7) kind = .container;
        if (kind_roll == 8) kind = .ttl;

        var payload = jute.Writer.init(allocator);
        defer payload.deinit();
        const client_id: i64 = if (model.sessions.items.len > 0)
            model.sessions.items[random.uintLessThan(usize, model.sessions.items.len)]
        else
            randomSessionId(random);

        switch (kind) {
            .create, .container, .ttl => {
                const path = try std.fmt.allocPrint(allocator, "/log{d}", .{seq});
                defer allocator.free(path);
                seq += 1;
                try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, if (kind == .create) 1 else if (kind == .container) 19 else 21);
                if (kind == .create) {
                    try jute.serialize(&payload, protocol.txn.CreateTxn{
                        .path = path,
                        .data = "",
                        .acl = openAclEntries(allocator, random),
                        .ephemeral = false,
                        .parentCVersion = -1,
                    });
                } else if (kind == .container) {
                    try jute.serialize(&payload, protocol.txn.CreateContainerTxn{
                        .path = path,
                        .data = "",
                        .acl = openAclEntries(allocator, random),
                        .parentCVersion = -1,
                    });
                } else {
                    try jute.serialize(&payload, protocol.txn.CreateTTLTxn{
                        .path = path,
                        .data = "",
                        .acl = openAclEntries(allocator, random),
                        .parentCVersion = -1,
                        .ttl = random.intRangeAtMost(i64, 1, 600_000),
                    });
                }
                try created.append(allocator, try allocator.dupe(u8, path));
            },
            .delete => {
                const path = if (created.items.len > 0) blk: {
                    const index = random.uintLessThan(usize, created.items.len);
                    break :blk created.items[index];
                } else "/";
                try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, 2);
                try jute.serialize(&payload, protocol.txn.DeleteTxn{ .path = path });
            },
            .set_data => {
                const path = if (model.paths.items.len > 0)
                    model.paths.items[random.uintLessThan(usize, model.paths.items.len)]
                else
                    "/";
                try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, 5);
                try jute.serialize(&payload, protocol.txn.SetDataTxn{ .path = path, .data = "v", .version = 0 });
            },
            .set_acl => {
                const path = if (model.paths.items.len > 0)
                    model.paths.items[random.uintLessThan(usize, model.paths.items.len)]
                else
                    "/";
                try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, 7);
                try jute.serialize(&payload, protocol.txn.SetACLTxn{
                    .path = path,
                    .acl = openAclEntries(allocator, random),
                    .version = 0,
                });
            },
            .multi => {
                try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, 14);
                var nested_create = jute.Writer.init(allocator);
                defer nested_create.deinit();
                try jute.serialize(&nested_create, protocol.txn.CreateTxn{
                    .path = "/multin",
                    .data = "",
                    .acl = openAclEntries(allocator, random),
                    .ephemeral = false,
                    .parentCVersion = -1,
                });
                var nested_delete = jute.Writer.init(allocator);
                defer nested_delete.deinit();
                try jute.serialize(&nested_delete, protocol.txn.DeleteTxn{ .path = "/n0" });
                const entries = try allocator.alloc(protocol.txn.Txn, 2);
                defer allocator.free(entries);
                entries[0] = .{ .type = 1, .data = try allocator.dupe(u8, nested_create.bytes()) };
                entries[1] = .{ .type = 2, .data = try allocator.dupe(u8, nested_delete.bytes()) };
                defer allocator.free(entries[0].data.?);
                defer allocator.free(entries[1].data.?);
                try jute.serialize(&payload, protocol.txn.MultiTxn{ .txns = entries });
            },
            .close_session => {
                try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, -11);
                if (random.boolean() and model.ephemeral_paths.items.len > 0) {
                    const victim = model.ephemeral_paths.items[random.uintLessThan(usize, model.ephemeral_paths.items.len)];
                    const paths = try allocator.alloc(?[]const u8, 1);
                    defer allocator.free(paths);
                    paths[0] = victim.path;
                    try jute.serialize(&payload, protocol.txn.CloseSessionTxn{ .paths2Delete = paths });
                }
            },
            .create_session => {
                const session_id = randomSessionId(random);
                try writeTxnHeader(&payload, session_id, @intCast(record_index), next_zxid, -10);
                try jute.serialize(&payload, protocol.txn.CreateSessionTxn{ .timeOut = 30_000 });
            },
            .error_txn => {
                try writeTxnHeader(&payload, client_id, @intCast(record_index), next_zxid, -1);
                try jute.serialize(&payload, protocol.txn.ErrorTxn{ .err = -101 });
            },
        }
        try writeLogEnvelope(allocator, &log, payload.bytes(), &seed);
        next_zxid += 1;
    }

    if (random.boolean()) {
        try log.writeLong(0);
        try log.writeInt(0);
    }

    seed.filename_zxid = model.filename_zxid + 1;
    try seed.bytes.appendSlice(allocator, log.bytes());
    return seed;
}

fn generateSnappyChunk(allocator: std.mem.Allocator, random: std.Random, out: *std.ArrayList(u8)) !void {
    const data_len = random.intRangeAtMost(usize, 1, 60);
    const data = try allocator.alloc(u8, data_len);
    defer allocator.free(data);
    random.bytes(data);

    // Snappy frame: varint uncompressed length + literal tag + bytes.
    var value: u64 = data.len;
    while (true) {
        var byte: u8 = @truncate(value & 0x7f);
        value >>= 7;
        if (value != 0) byte |= 0x80;
        try out.append(allocator, byte);
        if (value == 0) break;
    }
    try out.append(allocator, @intCast((data.len - 1) << 2));
    try out.appendSlice(allocator, data);
}

fn generateSnappySeed(allocator: std.mem.Allocator, random: std.Random) !Seed {
    var seed = Seed{};
    errdefer seed.deinit(allocator);
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);

    try out.appendSlice(allocator, &[_]u8{ 0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0 });
    try out.appendSlice(allocator, &[_]u8{ 0, 0, 0, 1 });
    try out.appendSlice(allocator, &[_]u8{ 0, 0, 0, 1 });

    const chunk_count = random.intRangeAtMost(usize, 1, 5);
    for (0..chunk_count) |_| {
        var chunk: std.ArrayList(u8) = .empty;
        defer chunk.deinit(allocator);
        try generateSnappyChunk(allocator, random, &chunk);
        try out.append(allocator, 0);
        try out.append(allocator, 0);
        try out.append(allocator, @intCast((chunk.items.len >> 8) & 0xff));
        try out.append(allocator, @intCast(chunk.items.len & 0xff));
        try out.appendSlice(allocator, chunk.items);
    }
    seed.bytes = out;
    return seed;
}

fn generateGzipSeed(allocator: std.mem.Allocator, random: std.Random) !Seed {
    var seed = Seed{};
    errdefer seed.deinit(allocator);

    const data_len = random.intRangeAtMost(usize, 0, 3_000);
    const data = try allocator.alloc(u8, data_len);
    defer allocator.free(data);
    random.bytes(data);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, &[_]u8{ 0x1f, 0x8b, 0x08, 0, 0, 0, 0, 0, 0, 0x03 });

    var offset: usize = 0;
    while (true) {
        const remaining = data.len - offset;
        const chunk_len: usize = @min(remaining, 65_535);
        const last = offset + chunk_len == data.len;
        try out.append(allocator, if (last) 1 else 0);
        try out.append(allocator, @truncate(chunk_len));
        try out.append(allocator, @truncate(chunk_len >> 8));
        const nlen: u16 = ~@as(u16, @truncate(chunk_len));
        try out.append(allocator, @truncate(nlen));
        try out.append(allocator, @truncate(nlen >> 8));
        try out.appendSlice(allocator, data[offset .. offset + chunk_len]);
        offset += chunk_len;
        if (last) break;
    }

    const crc = std.hash.Crc32.hash(data);
    try out.append(allocator, @truncate(crc));
    try out.append(allocator, @truncate(crc >> 8));
    try out.append(allocator, @truncate(crc >> 16));
    try out.append(allocator, @truncate(crc >> 24));
    const isize_: u32 = @truncate(data.len);
    try out.append(allocator, @truncate(isize_));
    try out.append(allocator, @truncate(isize_ >> 8));
    try out.append(allocator, @truncate(isize_ >> 16));
    try out.append(allocator, @truncate(isize_ >> 24));

    seed.bytes = out;
    return seed;
}

// ---------------------------------------------------------------------------
// Mutation
// ---------------------------------------------------------------------------

const interesting_bytes = [_]u8{ 0, 1, 0x7f, 0x80, 0xfe, 0xff };

fn mutate(allocator: std.mem.Allocator, random: std.Random, seed: *Seed, donors: []const []const u8) !void {
    const rounds = random.intRangeAtMost(usize, 0, 4);
    for (0..rounds) |_| {
        if (seed.bytes.items.len == 0) break;
        const roll = random.intRangeAtMost(u8, 0, 9);
        switch (roll) {
            0, 1 => { // Bit flip anywhere.
                const position = random.uintLessThan(usize, seed.bytes.items.len);
                seed.bytes.items[position] ^= @as(u8, 1) << random.intRangeAtMost(u3, 0, 7);
            },
            2 => { // Interesting byte anywhere.
                const position = random.uintLessThan(usize, seed.bytes.items.len);
                seed.bytes.items[position] = interesting_bytes[random.uintLessThan(usize, interesting_bytes.len)];
            },
            3, 4 => { // Envelope-preserving flip inside a payload span.
                if (seed.payload_spans.items.len == 0) continue;
                const span = seed.payload_spans.items[random.uintLessThan(usize, seed.payload_spans.items.len)];
                if (span.len == 0) continue;
                const position = span.start + random.uintLessThan(usize, span.len);
                if (position >= seed.bytes.items.len) continue;
                seed.bytes.items[position] ^= @as(u8, 1) << random.intRangeAtMost(u3, 0, 7);
            },
            5 => { // Truncate.
                seed.bytes.shrinkRetainingCapacity(random.uintAtMost(usize, seed.bytes.items.len));
            },
            6 => { // Extend with garbage.
                const extra = random.intRangeAtMost(usize, 1, 64);
                if (seed.bytes.items.len + extra > max_input_bytes) continue;
                const old = seed.bytes.items.len;
                try seed.bytes.resize(allocator, old + extra);
                random.bytes(seed.bytes.items[old..]);
            },
            7 => { // Splice a donor chunk.
                if (donors.len == 0) continue;
                const donor = donors[random.uintLessThan(usize, donors.len)];
                if (donor.len == 0) continue;
                const chunk_len = random.uintLessThan(usize, @min(donor.len, 256) + 1);
                const donor_start = random.uintLessThan(usize, donor.len - chunk_len + 1);
                const position = random.uintLessThan(usize, seed.bytes.items.len + 1);
                if (seed.bytes.items.len + chunk_len > max_input_bytes) continue;
                try seed.bytes.insertSlice(allocator, position, donor[donor_start .. donor_start + chunk_len]);
            },
            8, 9 => { // Seal-preserving flip: flip a byte, then repair the seal.
                const seal_offset = seed.seal_offset orelse continue;
                if (seal_offset == 0 or seal_offset + 13 > seed.bytes.items.len) continue;
                const position = random.uintLessThan(usize, seal_offset);
                seed.bytes.items[position] ^= @as(u8, 1) << random.intRangeAtMost(u3, 0, 7);
                const checksum = std.hash.Adler32.hash(seed.bytes.items[0..seal_offset]);
                std.mem.writeInt(u64, seed.bytes.items[seal_offset..][0..8], checksum, .big);
            },
            else => {},
        }
    }
}

// ---------------------------------------------------------------------------
// Input execution
// ---------------------------------------------------------------------------

fn runSnapshotInput(allocator: std.mem.Allocator, bytes: []const u8, filename_zxid: i64) !void {
    var state = try import_mod.parseSnapshotData(allocator, bytes, filename_zxid);
    defer state.deinit();
    try import_mod.finalizeState(&state);
}

fn runTxnlogInput(allocator: std.mem.Allocator, seed_bytes: []const u8, base_snapshot: []const u8, base_zxid: i64) !void {
    var state = try import_mod.parseSnapshotData(allocator, base_snapshot, base_zxid);
    defer state.deinit();
    try import_mod.replayLog(seed_bytes, base_zxid + 1, &state);
    try import_mod.finalizeState(&state);
}

fn runTargetInput(allocator: std.mem.Allocator, target: Target, bytes: []const u8, base: ?[]const u8, base_zxid: i64) !void {
    switch (target) {
        .snapshot => try runSnapshotInput(allocator, bytes, base_zxid),
        .txnlog => try runTxnlogInput(allocator, bytes, base.?, base_zxid),
        .snappy => {
            const decoded = try import_mod.decompressSnappy(allocator, bytes);
            allocator.free(decoded);
        },
        .gzip => {
            const decoded = try import_mod.decompressGzip(allocator, bytes);
            allocator.free(decoded);
        },
    }
}

/// Deterministically regenerates input `index` for `target`.
fn buildInput(
    arena: std.mem.Allocator,
    target: Target,
    global_seed: u64,
    index: usize,
    donors: []const []const u8,
) !struct { bytes: []const u8, base: ?[]const u8, base_zxid: i64 } {
    var prng = std.Random.DefaultPrng.init(mix(global_seed, index, @intFromEnum(target)));
    const random = prng.random();

    switch (target) {
        .snapshot => {
            const generated = try generateSnapshotSeed(arena, random);
            var seed = generated.seed;
            var model = generated.model;
            defer model.deinit(arena);
            defer seed.deinit(arena);
            try mutate(arena, random, &seed, donors);
            return .{
                .bytes = try arena.dupe(u8, seed.bytes.items),
                .base = null,
                .base_zxid = seed.filename_zxid,
            };
        },
        .txnlog => {
            const generated = try generateSnapshotSeed(arena, random);
            var model = generated.model;
            defer model.deinit(arena);
            var snapshot_seed = generated.seed;
            defer snapshot_seed.deinit(arena);
            var log_seed = try generateLogSeed(arena, random, &model);
            defer log_seed.deinit(arena);
            try mutate(arena, random, &log_seed, donors);
            return .{
                .bytes = try arena.dupe(u8, log_seed.bytes.items),
                .base = try arena.dupe(u8, snapshot_seed.bytes.items),
                .base_zxid = snapshot_seed.filename_zxid,
            };
        },
        .snappy => {
            var seed = try generateSnappySeed(arena, random);
            defer seed.deinit(arena);
            try mutate(arena, random, &seed, donors);
            return .{ .bytes = try arena.dupe(u8, seed.bytes.items), .base = null, .base_zxid = 1 };
        },
        .gzip => {
            var seed = try generateGzipSeed(arena, random);
            defer seed.deinit(arena);
            try mutate(arena, random, &seed, donors);
            return .{ .bytes = try arena.dupe(u8, seed.bytes.items), .base = null, .base_zxid = 1 };
        },
    }
}

fn mix(a: u64, b: usize, c: u64) u64 {
    var x = a ^ (b +% 0x9e3779b97f4a7c15) ^ (c << 32);
    x ^= x >> 30;
    x *%= 0xbf58476d1ce4e5b9;
    x ^= x >> 27;
    x *%= 0x94d049bb133111eb;
    x ^= x >> 31;
    return x;
}

/// Runs inputs [start, start + count). Crashes on any safety failure; leaks
/// abort the process at allocator deinit. Exits 0 when every input was
/// rejected or accepted cleanly.
fn workerMain(io: std.Io, allocator: std.mem.Allocator, target: Target, start: usize, count: usize, global_seed: u64, donors: []const []const u8) !void {
    for (start..start + count) |index| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const input = buildInput(arena.allocator(), target, global_seed, index, donors) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => continue, // generator failure is not a parser finding
        };
        runTargetInput(allocator, target, input.bytes, input.base, input.base_zxid) catch {};
    }
    _ = io;
}

// ---------------------------------------------------------------------------
// Corpus management
// ---------------------------------------------------------------------------

fn loadCorpus(allocator: std.mem.Allocator, io: std.Io, corpus_root: []const u8, target: Target) !std.ArrayList([]const u8) {
    var donors: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (donors.items) |donor| allocator.free(donor);
        donors.deinit(allocator);
    }
    var names: std.ArrayList([]const u8) = .empty;
    defer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    const target_dir_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ corpus_root, target.name() });
    defer allocator.free(target_dir_path);

    var dir = std.Io.Dir.cwd().openDir(io, target_dir_path, .{ .iterate = true }) catch return donors;
    defer dir.close(io);
    var iterator = dir.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".bin")) continue;
        try names.append(allocator, try allocator.dupe(u8, entry.name));
    }
    std.mem.sort([]const u8, names.items, {}, struct {
        fn lessThan(_: void, left: []const u8, right: []const u8) bool {
            return std.mem.lessThan(u8, left, right);
        }
    }.lessThan);
    for (names.items) |name| {
        if (donors.items.len >= max_donors) break;
        const bytes = dir.readFileAlloc(io, name, allocator, .limited(max_input_bytes)) catch continue;
        try donors.append(allocator, bytes);
    }
    return donors;
}

fn bytesToHex(input: []const u8, output: []u8) void {
    const charset = "0123456789abcdef";
    for (input, 0..) |byte, index| {
        output[index * 2] = charset[byte >> 4];
        output[index * 2 + 1] = charset[byte & 0x0f];
    }
}

fn saveCrash(
    allocator: std.mem.Allocator,
    io: std.Io,
    crash_dir: []const u8,
    target: Target,
    index: usize,
    bytes: []const u8,
    detail: []const u8,
) !void {
    try std.Io.Dir.cwd().createDirPath(io, crash_dir);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha3.Shake256.hash(bytes, &hash, .{});
    var hex: [16]u8 = undefined;
    bytesToHex(hash[0..8], &hex);
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}-{d}-{s}.bin", .{ crash_dir, target.name(), index, hex });
    defer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
    std.debug.print("finding ({s}): saved {s}\nreproduce with: zq-fuzz-import --replay {s}\n", .{ detail, path, path });
}

// ---------------------------------------------------------------------------
// Parent orchestration
// ---------------------------------------------------------------------------

fn spawnWorker(
    allocator: std.mem.Allocator,
    io: std.Io,
    self_exe: []const u8,
    target: Target,
    start: usize,
    count: usize,
    seed: u64,
    timeout_ms: u64,
) !std.process.Child.Term {
    const timeout_seconds = try std.fmt.allocPrint(allocator, "{d}s", .{timeout_ms / 1000});
    defer allocator.free(timeout_seconds);
    const start_arg = try std.fmt.allocPrint(allocator, "{d}", .{start});
    defer allocator.free(start_arg);
    const count_arg = try std.fmt.allocPrint(allocator, "{d}", .{count});
    defer allocator.free(count_arg);
    const seed_arg = try std.fmt.allocPrint(allocator, "{d}", .{seed});
    defer allocator.free(seed_arg);

    var child = try std.process.spawn(io, .{
        .argv = &.{ "timeout", "--kill-after=2s", timeout_seconds, self_exe, "--worker", target.name(), start_arg, count_arg, seed_arg },
    });
    return child.wait(io);
}

fn classifyTerm(term: std.process.Child.Term) ?[]const u8 {
    switch (term) {
        .exited => |code| switch (code) {
            0 => return null,
            124 => return "worker timeout",
            137 => return "worker killed after timeout",
            else => return "worker exited nonzero",
        },
        .signal => return "worker crashed on a signal",
        .stopped => return "worker stopped",
        .unknown => return "worker terminated abnormally",
    }
}

fn bisectBatch(
    allocator: std.mem.Allocator,
    io: std.Io,
    self_exe: []const u8,
    target: Target,
    start: usize,
    count: usize,
    seed: u64,
    input_timeout_ms: u64,
    crash_dir: []const u8,
    donors: []const []const u8,
) !?usize {
    for (start..start + count) |index| {
        const term = try spawnWorker(allocator, io, self_exe, target, index, 1, seed, input_timeout_ms);
        if (classifyTerm(term)) |detail| {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const input = try buildInput(arena.allocator(), target, seed, index, donors);
            try saveCrash(allocator, io, crash_dir, target, index, input.bytes, detail);
            return index;
        }
    }
    return null;
}

fn fuzzTarget(
    allocator: std.mem.Allocator,
    io: std.Io,
    self_exe: []const u8,
    config: Config,
    target: Target,
) !bool {
    var donors_list = try loadCorpus(allocator, io, config.corpus_root, target);
    defer {
        for (donors_list.items) |donor| allocator.free(donor);
        donors_list.deinit(allocator);
    }
    const donors = donors_list.items;

    var start: usize = 0;
    var findings: usize = 0;
    while (start < config.iterations) {
        const count = @min(config.batch_size, config.iterations - start);
        const term = try spawnWorker(allocator, io, self_exe, target, start, count, config.seed, config.batch_timeout_ms);
        if (classifyTerm(term)) |detail| {
            std.debug.print("finding ({s}) in batch {d}..{d} for target {s}; bisecting\n", .{ detail, start, start + count, target.name() });
            _ = try bisectBatch(allocator, io, self_exe, target, start, count, config.seed, config.input_timeout_ms, config.crash_dir, donors);
            findings += 1;
            if (!config.keep_going) return false;
        }
        start += count;
    }
    std.debug.print("target {s}: {d} inputs, {d} findings\n", .{ target.name(), config.iterations, findings });
    return true;
}

// ---------------------------------------------------------------------------
// Replay and corpus saving
// ---------------------------------------------------------------------------

fn replayPaths(allocator: std.mem.Allocator, io: std.Io, paths: []const []const u8) !void {
    var debug_allocator = std.heap.DebugAllocator(.{}){};
    defer {
        const check = debug_allocator.deinit();
        if (check == .leak) std.process.exit(3);
    }
    const leak_checked = debug_allocator.allocator();

    for (paths) |argument| {
        // A replay path encodes its target in the file name: <target>-....bin
        // or <target>/....bin. Plain target names are also accepted directly.
        var base_zxid: i64 = 1;
        var base_snapshot: []const u8 = "";
        var target: ?Target = null;
        inline for (all_targets) |candidate| {
            if (std.mem.indexOf(u8, argument, candidate.name()) != null) target = candidate;
        }
        if (target == null) {
            std.debug.print("skipping {s}: no known target in name\n", .{argument});
            continue;
        }
        const dir_path = std.fs.path.dirname(argument) orelse ".";
        const file_name = std.fs.path.basename(argument);
        var dir = std.Io.Dir.cwd().openDir(io, dir_path, .{}) catch {
            std.debug.print("skipping {s}: cannot open directory\n", .{argument});
            continue;
        };
        defer dir.close(io);
        const bytes = dir.readFileAlloc(io, file_name, allocator, .limited(max_input_bytes * 4)) catch {
            std.debug.print("skipping {s}: unreadable\n", .{argument});
            continue;
        };
        defer allocator.free(bytes);

        if (target.? == .txnlog) {
            // Replay a txnlog input against a minimal valid snapshot built the
            // same way the generator does.
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            var prng = std.Random.DefaultPrng.init(0);
            const generated = try generateSnapshotSeed(arena.allocator(), prng.random());
            base_zxid = generated.model.filename_zxid;
            const owned = try leak_checked.dupe(u8, generated.seed.bytes.items);
            defer leak_checked.free(owned);
            base_snapshot = owned;
            runTargetInput(leak_checked, target.?, bytes, base_snapshot, base_zxid) catch |err| {
                std.debug.print("{s}: rejected with {s}\n", .{ file_name, @errorName(err) });
                continue;
            };
        } else {
            runTargetInput(leak_checked, target.?, bytes, base_snapshot, base_zxid) catch |err| {
                std.debug.print("{s}: rejected with {s}\n", .{ file_name, @errorName(err) });
                continue;
            };
        }
        std.debug.print("{s}: accepted\n", .{file_name});
    }
}

fn saveCorpusMode(allocator: std.mem.Allocator, io: std.Io, config: Config, requested: usize) !void {
    var debug_allocator = std.heap.DebugAllocator(.{}){};
    defer {
        const check = debug_allocator.deinit();
        if (check == .leak) std.process.exit(3);
    }
    const leak_checked = debug_allocator.allocator();

    for (config.targets) |target| {
        var seen: std.StringHashMap(void) = .init(allocator);
        defer {
            var keys = seen.keyIterator();
            while (keys.next()) |key| allocator.free(key.*);
            seen.deinit();
        }
        var donors_list = try loadCorpus(allocator, io, config.corpus_root, target);
        defer {
            for (donors_list.items) |donor| allocator.free(donor);
            donors_list.deinit(allocator);
        }
        const donors = donors_list.items;

        const target_dir = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ config.corpus_root, target.name() });
        defer allocator.free(target_dir);
        try std.Io.Dir.cwd().createDirPath(io, target_dir);

        var saved: usize = 0;
        var index: usize = 0;
        while (index < requested and saved < max_saved_per_target) : (index += 1) {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const input = buildInput(arena.allocator(), target, config.seed, index, donors) catch continue;
            const outcome = if (runTargetInput(leak_checked, target, input.bytes, input.base, input.base_zxid)) |_| "accepted" else |err| @errorName(err);
            const key = try allocator.dupe(u8, outcome);
            if ((try seen.getOrPut(key)).found_existing) {
                allocator.free(key);
                continue;
            }
            if (input.bytes.len > max_saved_bytes) continue;
            var hash: [32]u8 = undefined;
            std.crypto.hash.sha3.Shake256.hash(input.bytes, &hash, .{});
            var hex: [16]u8 = undefined;
            bytesToHex(hash[0..8], &hex);
            const path = try std.fmt.allocPrint(allocator, "{s}/{s}-{s}.bin", .{ target_dir, outcome, hex });
            defer allocator.free(path);
            try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = input.bytes });
            saved += 1;
        }
        std.debug.print("target {s}: saved {d} corpus files from {d} inputs\n", .{ target.name(), saved, index });
    }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const Arguments = struct {
    values: []const []const u8,
    index: usize = 0,

    fn next(self: *Arguments) ?[]const u8 {
        if (self.index >= self.values.len) return null;
        const value = self.values[self.index];
        self.index += 1;
        return value;
    }
};

fn optionValue(arguments: *Arguments) ?[]const u8 {
    return arguments.next();
}

fn parseTargets(allocator: std.mem.Allocator, value: []const u8) ![]const Target {
    var targets: std.ArrayList(Target) = .empty;
    errdefer targets.deinit(allocator);
    var iterator = std.mem.splitScalar(u8, value, ',');
    while (iterator.next()) |name| {
        if (name.len == 0) continue;
        const target = Target.fromString(name) orelse return error.UnknownTarget;
        try targets.append(allocator, target);
    }
    if (targets.items.len == 0) return error.UnknownTarget;
    return targets.toOwnedSlice(allocator);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    const arguments = try init.minimal.args.toSlice(init.arena.allocator());

    var targets: []const Target = &all_targets;
    defer if (targets.ptr != &all_targets) init.arena.allocator().free(targets);
    var iterations: usize = default_iterations_per_target;
    var seed: ?u64 = null;
    var batch_size: usize = default_batch_size;
    var batch_timeout_ms: u64 = default_batch_timeout_ms;
    var input_timeout_ms: u64 = default_input_timeout_ms;
    var corpus_root: []const u8 = default_corpus_root;
    var crash_dir: ?[]const u8 = null;
    var keep_going = false;
    var replay_paths_list: ?[]const []const u8 = null;
    var save_corpus_count: ?usize = null;

    var parser = Arguments{ .values = arguments[1..] };
    while (parser.next()) |argument| {
        if (std.mem.eql(u8, argument, "--target")) {
            const value = optionValue(&parser) orelse return error.InvalidArguments;
            targets = try parseTargets(init.arena.allocator(), value);
        } else if (std.mem.eql(u8, argument, "--iterations")) {
            const value = optionValue(&parser) orelse return error.InvalidArguments;
            iterations = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, argument, "--seed")) {
            const value = optionValue(&parser) orelse return error.InvalidArguments;
            seed = try std.fmt.parseInt(u64, value, 0);
        } else if (std.mem.eql(u8, argument, "--batch")) {
            const value = optionValue(&parser) orelse return error.InvalidArguments;
            batch_size = try std.fmt.parseInt(usize, value, 10);
        } else if (std.mem.eql(u8, argument, "--batch-timeout-ms")) {
            const value = optionValue(&parser) orelse return error.InvalidArguments;
            batch_timeout_ms = try std.fmt.parseInt(u64, value, 0);
        } else if (std.mem.eql(u8, argument, "--input-timeout-ms")) {
            const value = optionValue(&parser) orelse return error.InvalidArguments;
            input_timeout_ms = try std.fmt.parseInt(u64, value, 0);
        } else if (std.mem.eql(u8, argument, "--corpus")) {
            corpus_root = optionValue(&parser) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, argument, "--crash-dir")) {
            crash_dir = optionValue(&parser) orelse return error.InvalidArguments;
        } else if (std.mem.eql(u8, argument, "--keep-going")) {
            keep_going = true;
        } else if (std.mem.eql(u8, argument, "--replay")) {
            const rest = parser.values[parser.index..];
            parser.index = parser.values.len;
            if (rest.len == 0) return error.InvalidArguments;
            replay_paths_list = rest;
        } else if (std.mem.eql(u8, argument, "--save-corpus")) {
            const maybe_count = parser.next();
            if (maybe_count) |value| {
                save_corpus_count = std.fmt.parseInt(usize, value, 10) catch blk: {
                    parser.index -= 1;
                    break :blk null;
                };
            }
            if (save_corpus_count == null) save_corpus_count = 10_000;
        } else if (std.mem.eql(u8, argument, "--worker")) {
            const target_name = optionValue(&parser) orelse return error.InvalidArguments;
            const target = Target.fromString(target_name) orelse return error.InvalidArguments;
            const start = try std.fmt.parseInt(usize, optionValue(&parser) orelse return error.InvalidArguments, 10);
            const count = try std.fmt.parseInt(usize, optionValue(&parser) orelse return error.InvalidArguments, 10);
            const worker_seed = try std.fmt.parseInt(u64, optionValue(&parser) orelse return error.InvalidArguments, 10);
            return workerMain(io, allocator, target, start, count, worker_seed, &.{});
        } else if (std.mem.eql(u8, argument, "--help")) {
            try writeAll(io, usage);
            return;
        } else {
            try writeAll(io, usage);
            return error.InvalidArguments;
        }
    }

    if (replay_paths_list) |paths| {
        try replayPaths(allocator, io, paths);
        return;
    }

    if (save_corpus_count) |count| {
        const config = Config{
            .targets = targets,
            .iterations = iterations,
            .seed = seed orelse 0x5eed_2026,
            .batch_size = batch_size,
            .batch_timeout_ms = batch_timeout_ms,
            .input_timeout_ms = input_timeout_ms,
            .corpus_root = corpus_root,
            .crash_dir = crash_dir orelse try std.fmt.allocPrint(init.arena.allocator(), "{s}/crashes", .{corpus_root}),
            .keep_going = keep_going,
        };
        try saveCorpusMode(allocator, io, config, count);
        return;
    }

    const config = Config{
        .targets = targets,
        .iterations = iterations,
        .seed = seed orelse @as(u64, @bitCast(@as(i64, @truncate(std.Io.Clock.awake.now(io).nanoseconds)))),
        .batch_size = batch_size,
        .batch_timeout_ms = batch_timeout_ms,
        .input_timeout_ms = input_timeout_ms,
        .corpus_root = corpus_root,
        .crash_dir = crash_dir orelse try std.fmt.allocPrint(init.arena.allocator(), "{s}/crashes", .{corpus_root}),
        .keep_going = keep_going,
    };
    std.debug.print("fuzzing {d} targets x {d} inputs, seed {d}\n", .{ targets.len, iterations, config.seed });

    var self_exe_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const self_exe_len = try std.Io.Dir.readLinkAbsolute(io, "/proc/self/exe", &self_exe_buffer);
    const self_exe = self_exe_buffer[0..self_exe_len];

    for (config.targets) |target| {
        if (!try fuzzTarget(allocator, io, self_exe, config, target)) {
            std.debug.print("stopping after findings in target {s}; rerun with --keep-going to continue\n", .{target.name()});
            std.process.exit(1);
        }
    }
    std.debug.print("no findings\n", .{});
}

fn writeAll(io: std.Io, bytes: []const u8) !void {
    var buffer: [1024]u8 = undefined;
    var writer = std.Io.File.stdout().writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}
