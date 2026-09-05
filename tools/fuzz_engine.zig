// Generic fuzzing engine shared by the per-suite harnesses.
//
// The engine owns everything that is not suite specific: deterministic input
// scheduling, structure-aware mutation, worker-process isolation with timeout
// and bisection, corpus persistence, replay, and the command-line interface.
// A suite provides a name and a list of target definitions with `build` and
// `run` functions.
//
// Inputs are a pure function of (seed, target index, input index), so every
// reported finding reproduces exactly. Workers run under `timeout`; panics,
// signals, hangs, and allocator leaks are findings that the parent bisects to
// a single saved input under `<corpus>/<suite>/crashes/`.

const std = @import("std");

pub const max_input_bytes = 64 * 1024;
pub const max_saved_bytes = 64 * 1024;
pub const max_saved_per_target = 100;
pub const max_donors = 64;

pub const default_iterations_per_target = 20_000;
pub const default_batch_size = 128;
pub const default_batch_timeout_ms = 60_000;
pub const default_input_timeout_ms = 5_000;

/// Mutation targets inside a generated input that envelope checks protect.
pub const Span = struct { start: usize, len: usize };

/// A deterministic input plus the metadata the mutator needs.
pub const Input = struct {
    /// Bytes handed to `run`. Mutable so the mutator can work in place.
    bytes: []u8,
    /// Envelope-preserving mutation regions, when the format has them.
    spans: []const Span = &.{},
    /// Offset of a trailing checksum long that the mutator can repair.
    seal_offset: ?usize = null,
    /// Suite-specific auxiliary bytes (for example a base snapshot).
    aux: ?[]const u8 = null,
    /// Suite-specific auxiliary scalar (for example a filename zxid).
    aux_i64: i64 = 0,
};

pub const BuildFn = *const fn (arena: std.mem.Allocator, random: std.Random, donors: []const []const u8) anyerror!Input;
pub const RunFn = *const fn (allocator: std.mem.Allocator, input: Input) anyerror!void;

pub const TargetDef = struct {
    name: []const u8,
    build: BuildFn,
    run: RunFn,
};

pub fn mainFor(comptime suite_name: []const u8, comptime defs: []const TargetDef) fn (std.process.Init) anyerror!void {
    return struct {
        fn main(init: std.process.Init) anyerror!void {
            try run(suite_name, defs, init);
        }
    }.main;
}

const Config = struct {
    targets: []const usize,
    iterations: usize,
    seed: u64,
    batch_size: usize,
    batch_timeout_ms: u64,
    input_timeout_ms: u64,
    corpus_root: []const u8,
    crash_dir: []const u8,
    keep_going: bool,
};

fn usageText(comptime suite: []const u8) []const u8 {
    return "usage: zq-fuzz-" ++ suite ++ " [--target LIST] [--iterations N] [--seed S]\n" ++
        "                 [--batch N] [--batch-timeout-ms N] [--input-timeout-ms N]\n" ++
        "                 [--corpus DIR] [--crash-dir DIR] [--keep-going]\n" ++
        "                 [--replay PATH...]\n" ++
        "                 [--save-corpus [N]]\n" ++
        "                 [--worker TARGET START COUNT SEED]\n";
}

// ---------------------------------------------------------------------------
// Mutation
// ---------------------------------------------------------------------------

const interesting_bytes = [_]u8{ 0, 1, 0x7f, 0x80, 0xfe, 0xff };

fn mutate(allocator: std.mem.Allocator, random: std.Random, input: *Input) !void {
    const rounds = random.intRangeAtMost(usize, 0, 4);
    for (0..rounds) |_| {
        if (input.bytes.len == 0) break;
        const roll = random.intRangeAtMost(u8, 0, 9);
        switch (roll) {
            0, 1 => { // Bit flip anywhere.
                const position = random.uintLessThan(usize, input.bytes.len);
                input.bytes[position] ^= @as(u8, 1) << random.intRangeAtMost(u3, 0, 7);
            },
            2 => { // Interesting byte anywhere.
                const position = random.uintLessThan(usize, input.bytes.len);
                input.bytes[position] = interesting_bytes[random.uintLessThan(usize, interesting_bytes.len)];
            },
            3, 4 => { // Envelope-preserving flip inside a span.
                if (input.spans.len == 0) continue;
                const span = input.spans[random.uintLessThan(usize, input.spans.len)];
                if (span.len == 0) continue;
                const position = span.start + random.uintLessThan(usize, span.len);
                if (position >= input.bytes.len) continue;
                input.bytes[position] ^= @as(u8, 1) << random.intRangeAtMost(u3, 0, 7);
            },
            5 => { // Truncate.
                input.bytes = input.bytes[0..random.uintAtMost(usize, input.bytes.len)];
            },
            6 => { // Extend with garbage.
                const extra = random.intRangeAtMost(usize, 1, 64);
                if (input.bytes.len + extra > max_input_bytes) continue;
                const grown = try allocator.alloc(u8, input.bytes.len + extra);
                @memcpy(grown[0..input.bytes.len], input.bytes);
                random.bytes(grown[input.bytes.len..]);
                input.bytes = grown;
            },
            7 => { // Splice bytes from the same input (no donor needed).
                if (input.bytes.len < 4) continue;
                const chunk_len = random.intRangeAtMost(usize, 1, @min(64, input.bytes.len));
                const source = random.uintLessThan(usize, input.bytes.len - chunk_len + 1);
                const position = random.uintLessThan(usize, input.bytes.len + 1);
                if (input.bytes.len + chunk_len > max_input_bytes) continue;
                const grown = try allocator.alloc(u8, input.bytes.len + chunk_len);
                @memcpy(grown[0..position], input.bytes[0..position]);
                @memcpy(grown[position .. position + chunk_len], input.bytes[source .. source + chunk_len]);
                @memcpy(grown[position + chunk_len ..], input.bytes[position..]);
                input.bytes = grown;
            },
            8, 9 => { // Checksum-preserving flip: flip a byte, repair the seal.
                const seal_offset = input.seal_offset orelse continue;
                if (seal_offset == 0 or seal_offset + 13 > input.bytes.len) continue;
                const position = random.uintLessThan(usize, seal_offset);
                input.bytes[position] ^= @as(u8, 1) << random.intRangeAtMost(u3, 0, 7);
                const checksum = std.hash.Adler32.hash(input.bytes[0..seal_offset]);
                std.mem.writeInt(u64, input.bytes[seal_offset..][0..8], checksum, .big);
            },
            else => {},
        }
    }
}

pub fn mix(a: u64, b: usize, c: u64) u64 {
    var x = a ^ (b +% 0x9e3779b97f4a7c15) ^ (c << 32);
    x ^= x >> 30;
    x *%= 0xbf58476d1ce4e5b9;
    x ^= x >> 27;
    x *%= 0x94d049bb133111eb;
    x ^= x >> 31;
    return x;
}

// ---------------------------------------------------------------------------
// Input execution
// ---------------------------------------------------------------------------

/// Deterministically regenerates input `index` for target `target_index`.
fn buildInput(
    arena: std.mem.Allocator,
    comptime defs: []const TargetDef,
    target_index: usize,
    global_seed: u64,
    index: usize,
    donors: []const []const u8,
) !Input {
    var prng = std.Random.DefaultPrng.init(mix(global_seed, index, target_index));
    const random = prng.random();
    var input = try defs[target_index].build(arena, random, donors);
    mutate(arena, random, &input) catch {
        input.bytes = input.bytes[0..0];
    };
    return input;
}

fn workerMain(allocator: std.mem.Allocator, comptime defs: []const TargetDef, target_index: usize, start: usize, count: usize, global_seed: u64, donors: []const []const u8) !void {
    for (start..start + count) |index| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const input = buildInput(arena.allocator(), defs, target_index, global_seed, index, donors) catch continue;
        defs[target_index].run(allocator, input) catch {};
    }
}

// ---------------------------------------------------------------------------
// Corpus management
// ---------------------------------------------------------------------------

fn loadCorpus(allocator: std.mem.Allocator, io: std.Io, corpus_root: []const u8, target_name: []const u8) !std.ArrayList([]const u8) {
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
    const target_dir_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ corpus_root, target_name });
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
    target_name: []const u8,
    index: usize,
    bytes: []const u8,
    detail: []const u8,
) !void {
    try std.Io.Dir.cwd().createDirPath(io, crash_dir);
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha3.Shake256.hash(bytes, &hash, .{});
    var hex: [16]u8 = undefined;
    bytesToHex(hash[0..8], &hex);
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}-{d}-{s}.bin", .{ crash_dir, target_name, index, hex });
    defer allocator.free(path);
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
    std.debug.print("finding ({s}): saved {s}\nreproduce with: zq-fuzz replay {s}\n", .{ detail, path, path });
}

// ---------------------------------------------------------------------------
// Parent orchestration
// ---------------------------------------------------------------------------

fn spawnWorker(
    allocator: std.mem.Allocator,
    io: std.Io,
    self_exe: []const u8,
    target_name: []const u8,
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
        .argv = &.{ "timeout", "--kill-after=2s", timeout_seconds, self_exe, "--worker", target_name, start_arg, count_arg, seed_arg },
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
    comptime defs: []const TargetDef,
    target_index: usize,
    start: usize,
    count: usize,
    seed: u64,
    input_timeout_ms: u64,
    crash_dir: []const u8,
    donors: []const []const u8,
) !?usize {
    for (start..start + count) |index| {
        const term = try spawnWorker(allocator, io, self_exe, defs[target_index].name, index, 1, seed, input_timeout_ms);
        if (classifyTerm(term)) |detail| {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const input = try buildInput(arena.allocator(), defs, target_index, seed, index, donors);
            try saveCrash(allocator, io, crash_dir, defs[target_index].name, index, input.bytes, detail);
            return index;
        }
    }
    return null;
}

fn fuzzTarget(
    allocator: std.mem.Allocator,
    io: std.Io,
    self_exe: []const u8,
    comptime defs: []const TargetDef,
    config: Config,
    target_index: usize,
) !bool {
    var donors_list = try loadCorpus(allocator, io, config.corpus_root, defs[target_index].name);
    defer {
        for (donors_list.items) |donor| allocator.free(donor);
        donors_list.deinit(allocator);
    }
    const donors = donors_list.items;

    var start: usize = 0;
    var findings: usize = 0;
    while (start < config.iterations) {
        const count = @min(config.batch_size, config.iterations - start);
        const term = try spawnWorker(allocator, io, self_exe, defs[target_index].name, start, count, config.seed, config.batch_timeout_ms);
        if (classifyTerm(term)) |detail| {
            std.debug.print("finding ({s}) in batch {d}..{d} for target {s}; bisecting\n", .{ detail, start, start + count, defs[target_index].name });
            _ = try bisectBatch(allocator, io, self_exe, defs, target_index, start, count, config.seed, config.input_timeout_ms, config.crash_dir, donors);
            findings += 1;
            if (!config.keep_going) return false;
        }
        start += count;
    }
    std.debug.print("target {s}: {d} inputs, {d} findings\n", .{ defs[target_index].name, config.iterations, findings });
    return true;
}

// ---------------------------------------------------------------------------
// Replay and corpus saving
// ---------------------------------------------------------------------------

fn replayPaths(
    allocator: std.mem.Allocator,
    io: std.Io,
    comptime defs: []const TargetDef,
    paths: []const []const u8,
) !void {
    var debug_allocator = std.heap.DebugAllocator(.{}){};
    defer {
        const check = debug_allocator.deinit();
        if (check == .leak) std.process.exit(3);
    }
    const leak_checked = debug_allocator.allocator();

    for (paths) |argument| {
        var target_index: ?usize = null;
        for (defs, 0..) |def, index| {
            if (std.mem.indexOf(u8, argument, def.name) != null) target_index = index;
        }
        if (target_index == null) {
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
        const input = Input{ .bytes = bytes };
        defs[target_index.?].run(leak_checked, input) catch |err| {
            std.debug.print("{s}: rejected with {s}\n", .{ file_name, @errorName(err) });
            continue;
        };
        std.debug.print("{s}: accepted\n", .{file_name});
    }
}

fn saveCorpusMode(
    allocator: std.mem.Allocator,
    io: std.Io,
    comptime defs: []const TargetDef,
    config: Config,
    requested: usize,
) !void {
    var debug_allocator = std.heap.DebugAllocator(.{}){};
    defer {
        const check = debug_allocator.deinit();
        if (check == .leak) std.process.exit(3);
    }
    const leak_checked = debug_allocator.allocator();

    for (config.targets) |target_index| {
        var seen: std.StringHashMap(void) = .init(allocator);
        defer {
            var keys = seen.keyIterator();
            while (keys.next()) |key| allocator.free(key.*);
            seen.deinit();
        }
        var donors_list = try loadCorpus(allocator, io, config.corpus_root, defs[target_index].name);
        defer {
            for (donors_list.items) |donor| allocator.free(donor);
            donors_list.deinit(allocator);
        }
        const donors = donors_list.items;

        const target_dir = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ config.corpus_root, defs[target_index].name });
        defer allocator.free(target_dir);
        try std.Io.Dir.cwd().createDirPath(io, target_dir);

        var saved: usize = 0;
        var index: usize = 0;
        while (index < requested and saved < max_saved_per_target) : (index += 1) {
            var arena = std.heap.ArenaAllocator.init(allocator);
            defer arena.deinit();
            const input = buildInput(arena.allocator(), defs, target_index, config.seed, index, donors) catch continue;
            const outcome = if (defs[target_index].run(leak_checked, input)) |_| "accepted" else |err| @errorName(err);
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
        std.debug.print("target {s}: saved {d} corpus files from {d} inputs\n", .{ defs[target_index].name, saved, index });
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

fn parseTargetList(allocator: std.mem.Allocator, comptime defs: []const TargetDef, value: []const u8) ![]const usize {
    var targets: std.ArrayList(usize) = .empty;
    errdefer targets.deinit(allocator);
    var iterator = std.mem.splitScalar(u8, value, ',');
    while (iterator.next()) |name| {
        if (name.len == 0) continue;
        var matched = false;
        for (defs, 0..) |def, index| {
            if (std.mem.eql(u8, name, def.name)) {
                try targets.append(allocator, index);
                matched = true;
            }
        }
        if (!matched) return error.UnknownTarget;
    }
    if (targets.items.len == 0) return error.UnknownTarget;
    return targets.toOwnedSlice(allocator);
}

fn run(
    comptime suite_name: []const u8,
    comptime defs: []const TargetDef,
    init: std.process.Init,
) !void {
    const allocator = init.gpa;
    const io = init.io;
    const arena = init.arena.allocator();
    const arguments = try init.minimal.args.toSlice(arena);

    var all_targets: [defs.len]usize = undefined;
    for (0..defs.len) |index| all_targets[index] = index;
    var targets: []const usize = &all_targets;
    defer if (targets.ptr != &all_targets) arena.free(targets);
    var iterations: usize = default_iterations_per_target;
    var seed: ?u64 = null;
    var batch_size: usize = default_batch_size;
    var batch_timeout_ms: u64 = default_batch_timeout_ms;
    var input_timeout_ms: u64 = default_input_timeout_ms;
    var corpus_root: []const u8 = try std.fmt.allocPrint(arena, "tests/fuzz/corpus/{s}", .{suite_name});
    var crash_dir: ?[]const u8 = null;
    var keep_going = false;
    var replay_paths_list: ?[]const []const u8 = null;
    var save_corpus_count: ?usize = null;

    var parser = Arguments{ .values = arguments[1..] };
    while (parser.next()) |argument| {
        if (std.mem.eql(u8, argument, "--target")) {
            const value = optionValue(&parser) orelse return error.InvalidArguments;
            targets = try parseTargetList(arena, defs, value);
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
            batch_timeout_ms = try std.fmt.parseInt(u64, value, 10);
        } else if (std.mem.eql(u8, argument, "--input-timeout-ms")) {
            const value = optionValue(&parser) orelse return error.InvalidArguments;
            input_timeout_ms = try std.fmt.parseInt(u64, value, 10);
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
            var target_index: ?usize = null;
            for (defs, 0..) |def, index| {
                if (std.mem.eql(u8, def.name, target_name)) target_index = index;
            }
            const resolved = target_index orelse return error.InvalidArguments;
            const start = try std.fmt.parseInt(usize, optionValue(&parser) orelse return error.InvalidArguments, 10);
            const count = try std.fmt.parseInt(usize, optionValue(&parser) orelse return error.InvalidArguments, 10);
            const worker_seed = try std.fmt.parseInt(u64, optionValue(&parser) orelse return error.InvalidArguments, 0);
            var donors_list = try loadCorpus(allocator, io, corpus_root, defs[resolved].name);
            defer {
                for (donors_list.items) |donor| allocator.free(donor);
                donors_list.deinit(allocator);
            }
            return workerMain(allocator, defs, resolved, start, count, worker_seed, donors_list.items);
        } else if (std.mem.eql(u8, argument, "--help")) {
            try writeAll(io, usageText(suite_name));
            return;
        } else {
            try writeAll(io, usageText(suite_name));
            return error.InvalidArguments;
        }
    }

    if (replay_paths_list) |paths| {
        try replayPaths(allocator, io, defs, paths);
        return;
    }

    const resolved_crash_dir = crash_dir orelse try std.fmt.allocPrint(arena, "{s}/crashes", .{corpus_root});

    if (save_corpus_count) |count| {
        const config = Config{
            .targets = targets,
            .iterations = iterations,
            .seed = seed orelse 0x5eed_2026,
            .batch_size = batch_size,
            .batch_timeout_ms = batch_timeout_ms,
            .input_timeout_ms = input_timeout_ms,
            .corpus_root = corpus_root,
            .crash_dir = resolved_crash_dir,
            .keep_going = keep_going,
        };
        try saveCorpusMode(allocator, io, defs, config, count);
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
        .crash_dir = resolved_crash_dir,
        .keep_going = keep_going,
    };
    std.debug.print("fuzzing {d} targets x {d} inputs, seed {d}\n", .{ targets.len, iterations, config.seed });

    var self_exe_buffer: [std.fs.max_path_bytes]u8 = undefined;
    const self_exe_len = try std.Io.Dir.readLinkAbsolute(io, "/proc/self/exe", &self_exe_buffer);
    const self_exe = self_exe_buffer[0..self_exe_len];

    for (config.targets) |target_index| {
        if (!try fuzzTarget(allocator, io, self_exe, defs, config, target_index)) {
            std.debug.print("stopping after findings in target {s}; rerun with --keep-going to continue\n", .{defs[target_index].name});
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
