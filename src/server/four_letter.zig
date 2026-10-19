// ZooKeeper four-letter-word admin commands.
//
// A client that opens the client port and sends four lowercase ASCII bytes
// as its very first data receives the rendered text response for the named
// command, after which the server closes the connection. Wire protocol
// frames always begin with a zero length byte, so the first byte
// distinguishes commands from protocol traffic without buffering.
//
// Output formats mirror Apache ZooKeeper 3.9.5
// (`org.apache.zookeeper.server.command.*`) so existing operational tooling
// (`echo ruok | nc`, Prometheus exporters that scrape `mntr`) keeps working.
// Commands that require the serving state print `This server is not
// currently serving requests` while the node has no leader.

const std = @import("std");

pub const not_serving = "This server is not currently serving requests";

pub const Command = enum {
    conf,
    cons,
    crst,
    dump,
    envi,
    gtmk,
    isro,
    mntr,
    ruok,
    srst,
    srvr,
    stat,
    stmk,
    wchc,
    wchp,
    wchs,

    pub fn name(self: Command) []const u8 {
        return @tagName(self);
    }
};

pub const command_count = @typeInfo(Command).@"enum".field_names.len;

pub fn commandFromName(text: []const u8) ?Command {
    inline for (@typeInfo(Command).@"enum".field_names) |field| {
        if (std.mem.eql(u8, text, field)) return @field(Command, field);
    }
    return null;
}

/// Mirrors `zookeeper.4lw.commands.whitelist`: `*` enables every command,
/// otherwise a comma-separated list. The upstream default is `srvr` only.
pub const Whitelist = struct {
    allowed: std.EnumSet(Command) = std.EnumSet(Command).initOne(.srvr),

    pub fn parse(text: []const u8) Whitelist {
        var whitelist = Whitelist{ .allowed = .empty };
        var iterator = std.mem.splitScalar(u8, text, ',');
        while (iterator.next()) |token| {
            const trimmed = std.mem.trim(u8, token, " ");
            if (trimmed.len == 0) continue;
            if (std.mem.eql(u8, trimmed, "*")) {
                whitelist.allowed = .full;
                continue;
            }
            if (commandFromName(trimmed)) |command| {
                whitelist.allowed.insert(command);
            }
        }
        return whitelist;
    }

    pub fn allows(self: Whitelist, command: Command) bool {
        return self.allowed.contains(command);
    }
};

/// Rolling request latency in milliseconds. `min`/`avg`/`max` are computed
/// over the most recent samples, matching upstream ServerStats behavior.
fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

pub fn LatencyTracker(comptime history: usize) type {
    return struct {
        const Self = @This();

        mutex: std.atomic.Mutex = .unlocked,
        samples: [history]u32 = @as([history]u32, @splat(0)),
        count: usize = 0,

        pub fn record(self: *Self, latency_ms: u32) void {
            lock(&self.mutex);
            defer self.mutex.unlock();
            self.samples[self.count % history] = latency_ms;
            self.count += 1;
        }

        pub const LatencySnapshot = struct {
            min: u32,
            avg: f64,
            max: u32,
        };

        pub fn snapshot(self: *Self) LatencySnapshot {
            lock(&self.mutex);
            defer self.mutex.unlock();
            const filled = @min(self.count, history);
            if (filled == 0) return .{ .min = 0, .avg = 0, .max = 0 };
            var total: u64 = 0;
            var min: u32 = std.math.maxInt(u32);
            var max: u32 = 0;
            for (self.samples[0..filled]) |sample| {
                total += sample;
                min = @min(min, sample);
                max = @max(max, sample);
            }
            return .{
                .min = min,
                .avg = @as(f64, @floatFromInt(total)) / @as(f64, @floatFromInt(filled)),
                .max = max,
            };
        }

        pub fn reset(self: *Self) void {
            lock(&self.mutex);
            defer self.mutex.unlock();
            self.count = 0;
        }
    };
}

/// Process-wide server counters shared by `srvr`, `stat`, `cons`, `mntr`,
/// and `envi`-adjacent tooling.
pub const ServerCounters = struct {
    packets_received: std.atomic.Value(u64) = .init(0),
    packets_sent: std.atomic.Value(u64) = .init(0),
    connections: std.atomic.Value(i64) = .init(0),
    outstanding_requests: std.atomic.Value(i64) = .init(0),
    latency: LatencyTracker(100) = .{},

    pub fn reset(self: *ServerCounters) void {
        // Latency and packet counters mirror upstream `srst`; the live
        // connection gauge and in-flight requests are structural state and
        // are preserved.
        self.packets_received.store(0, .release);
        self.packets_sent.store(0, .release);
        self.latency.reset();
    }
};

/// One established client connection, snapshotted by the connection thread.
pub const ConnectionInfo = struct {
    remote: []const u8,
    session_id: i64,
    session_timeout_ms: i32,
    established_ms: i64,
    queued: usize,
    received: u64,
    sent: u64,
    last_cxid: i32,
    last_zxid: i64,
    last_op: []const u8,
    last_response_ms: i64,
    last_latency_ms: u32,
    min_latency_ms: u32,
    avg_latency_ms: f64,
    max_latency_ms: u32,
};

pub const SessionInfo = struct {
    id: i64,
    timeout_ms: i32,
};

pub const EphemeralInfo = struct {
    path: []const u8,
    owner: i64,
};

pub const WatchesBySession = struct {
    session_id: i64,
    paths: []const []const u8,
};

pub const WatchesByPath = struct {
    path: []const u8,
    session_ids: []const i64,
};

pub const WatchSummary = struct {
    connections: usize,
    paths: usize,
    total: usize,
};

pub const ConfInfo = struct {
    client_port: u16,
    server_id: u64,
    data_dir: []const u8,
    raft_listen: []const u8,
    raft_advertise: []const u8,
    tick_time_ms: u64,
    min_session_timeout_ms: i32,
    max_session_timeout_ms: i32,
    max_client_connections: i32,
    config: []const u8,
};

/// Everything the renderer needs, gathered by the caller.
pub const Snapshot = struct {
    version: []const u8 = "3.9.5",
    mode: []const u8,
    serving: bool,
    zxid: i64 = 0,
    trace_mask: i64 = 0,
    node_count: usize = 0,
    ephemeral_count: usize = 0,
    approximate_data_size: u64 = 0,
    open_file_descriptors: usize = 0,
    max_file_descriptors: usize = 0,
    followers: ?usize = null,
    synced_followers: ?usize = null,
    pending_syncs: usize = 0,
    last_proposal_size: i64 = -1,
    max_proposal_size: i64 = -1,
    counters: *ServerCounters,
    connections: []const ConnectionInfo = &.{},
    sessions: []const SessionInfo = &.{},
    ephemerals: []const EphemeralInfo = &.{},
    watch_summary: WatchSummary = .{ .connections = 0, .paths = 0, .total = 0 },
    watches_by_session: []const WatchesBySession = &.{},
    watches_by_path: []const WatchesByPath = &.{},
    conf: ConfInfo,
};

pub fn render(
    allocator: std.mem.Allocator,
    snapshot: *Snapshot,
    command: Command,
) ![]u8 {
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);
    const w = &output;
    const S = struct {
        fn line(list: *std.ArrayList(u8), a: std.mem.Allocator, comptime format: []const u8, args: anytype) !void {
            const chunk = try std.fmt.allocPrint(a, format ++ "\n", args);
            defer a.free(chunk);
            try list.appendSlice(a, chunk);
        }

        fn raw(list: *std.ArrayList(u8), a: std.mem.Allocator, value: []const u8) !void {
            try list.appendSlice(a, value);
        }
    };

    const serving = snapshot.serving;
    switch (command) {
        .ruok => try w.appendSlice(allocator, "imok"),
        .isro => try w.appendSlice(allocator, if (serving) "rw" else "null"),
        .gtmk, .stmk => {
            const value = try std.fmt.allocPrint(allocator, "{d}", .{snapshot.trace_mask});
            defer allocator.free(value);
            try w.appendSlice(allocator, value);
        },
        .envi => {
            try S.line(w, allocator, "Environment:", .{});
            try S.line(w, allocator, "os.name={s}", .{@tagName(@import("builtin").os.tag)});
            try S.line(w, allocator, "os.arch={s}", .{@tagName(@import("builtin").cpu.arch)});
            try S.line(w, allocator, "zig.version={s}", .{@import("builtin").zig_version_string});
        },
        .srst => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                try S.line(w, allocator, "Server stats reset.", .{});
            }
        },
        .crst => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                try S.line(w, allocator, "Connection stats reset.", .{});
            }
        },
        .conf => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                try S.line(w, allocator, "clientPort={d}", .{snapshot.conf.client_port});
                try S.line(w, allocator, "secureClientPort=-1", .{});
                try S.line(w, allocator, "dataDir={s}", .{snapshot.conf.data_dir});
                try S.line(w, allocator, "dataLogDir={s}", .{snapshot.conf.data_dir});
                try S.line(w, allocator, "tickTime={d}", .{snapshot.conf.tick_time_ms});
                try S.line(w, allocator, "maxClientCnxns={d}", .{snapshot.conf.max_client_connections});
                try S.line(w, allocator, "minSessionTimeout={d}", .{snapshot.conf.min_session_timeout_ms});
                try S.line(w, allocator, "maxSessionTimeout={d}", .{snapshot.conf.max_session_timeout_ms});
                try S.line(w, allocator, "clientPortListenBacklog=-1", .{});
                try S.line(w, allocator, "serverId={d}", .{snapshot.conf.server_id});
                if (snapshot.conf.config.len > 0) {
                    var member = std.mem.splitScalar(u8, snapshot.conf.config, '\n');
                    while (member.next()) |entry| try S.line(w, allocator, "{s}", .{entry});
                }
            }
        },
        .srvr, .stat => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                try S.line(w, allocator, "Zookeeper version: {s}", .{snapshot.version});
                if (command == .stat) {
                    try S.line(w, allocator, "Clients:", .{});
                    for (snapshot.connections) |connection| {
                        try renderConnectionLine(allocator, w, connection, true);
                        try w.appendSlice(allocator, "\n");
                    }
                    try w.appendSlice(allocator, "\n");
                }
                const latency = snapshot.counters.latency.snapshot();
                try S.line(w, allocator, "Latency min/avg/max: {d}/{d:.3}/{d}", .{ latency.min, latency.avg, latency.max });
                try S.line(w, allocator, "Received: {d}", .{snapshot.counters.packets_received.load(.acquire)});
                try S.line(w, allocator, "Sent: {d}", .{snapshot.counters.packets_sent.load(.acquire)});
                try S.line(w, allocator, "Connections: {d}", .{snapshot.counters.connections.load(.acquire)});
                try S.line(w, allocator, "Outstanding: {d}", .{snapshot.counters.outstanding_requests.load(.acquire)});
                try S.line(w, allocator, "Zxid: 0x{x}", .{@as(u64, @bitCast(snapshot.zxid))});
                try S.line(w, allocator, "Mode: {s}", .{snapshot.mode});
                try S.line(w, allocator, "Node count: {d}", .{snapshot.node_count});
                if (std.mem.eql(u8, snapshot.mode, "leader")) {
                    try S.line(w, allocator, "Proposal sizes last/min/max: -1/-1/-1", .{});
                }
            }
        },
        .cons => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                for (snapshot.connections) |connection| {
                    try renderConnectionLine(allocator, w, connection, false);
                    try w.appendSlice(allocator, "\n");
                }
                try w.appendSlice(allocator, "\n");
            }
        },
        .dump => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                try S.line(w, allocator, "SessionTracker dump:", .{});
                for (snapshot.sessions) |session| {
                    try S.line(w, allocator, "0x{x}\t{d}", .{ @as(u64, @bitCast(session.id)), session.timeout_ms });
                }
                try S.line(w, allocator, "ephemeral nodes dump:", .{});
                for (snapshot.ephemerals) |ephemeral| {
                    try S.line(w, allocator, "{s}0x{x}", .{ ephemeral.path, @as(u64, @bitCast(ephemeral.owner)) });
                }
            }
        },
        .wchs => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                try S.line(w, allocator, "{d} connections watching {d} paths", .{ snapshot.watch_summary.connections, snapshot.watch_summary.paths });
                try S.line(w, allocator, "Total watches:{d}", .{snapshot.watch_summary.total});
            }
        },
        .wchc => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                for (snapshot.watches_by_session) |entry| {
                    try S.line(w, allocator, "0x{x}", .{@as(u64, @bitCast(entry.session_id))});
                    for (entry.paths) |path| {
                        try S.line(w, allocator, "\t{s}", .{path});
                    }
                }
            }
        },
        .wchp => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                for (snapshot.watches_by_path) |entry| {
                    try S.line(w, allocator, "{s}", .{entry.path});
                    for (entry.session_ids) |session_id| {
                        try S.line(w, allocator, "\t0x{x}", .{@as(u64, @bitCast(session_id))});
                    }
                }
            }
        },
        .mntr => {
            if (!serving) {
                try S.raw(w, allocator, not_serving ++ "\n");
            } else {
                const latency = snapshot.counters.latency.snapshot();
                try S.line(w, allocator, "zk_version\t{s}", .{snapshot.version});
                try S.line(w, allocator, "zk_server_state\t{s}", .{snapshot.mode});
                try S.line(w, allocator, "zk_peer_state\t{s} - {s}", .{ snapshot.mode, "connected" });
                try S.line(w, allocator, "zk_avg_latency\t{d}", .{latency.avg});
                try S.line(w, allocator, "zk_max_latency\t{d}", .{latency.max});
                try S.line(w, allocator, "zk_min_latency\t{d}", .{latency.min});
                try S.line(w, allocator, "zk_packets_received\t{d}", .{snapshot.counters.packets_received.load(.acquire)});
                try S.line(w, allocator, "zk_packets_sent\t{d}", .{snapshot.counters.packets_sent.load(.acquire)});
                try S.line(w, allocator, "zk_num_alive_connections\t{d}", .{snapshot.counters.connections.load(.acquire)});
                try S.line(w, allocator, "zk_outstanding_requests\t{d}", .{snapshot.counters.outstanding_requests.load(.acquire)});
                try S.line(w, allocator, "zk_znode_count\t{d}", .{snapshot.node_count});
                try S.line(w, allocator, "zk_watch_count\t{d}", .{snapshot.watch_summary.total});
                try S.line(w, allocator, "zk_ephemerals_count\t{d}", .{snapshot.ephemeral_count});
                try S.line(w, allocator, "zk_approximate_data_size\t{d}", .{snapshot.approximate_data_size});
                try S.line(w, allocator, "zk_open_file_descriptor_count\t{d}", .{snapshot.open_file_descriptors});
                try S.line(w, allocator, "zk_max_file_descriptor_count\t{d}", .{snapshot.max_file_descriptors});
                if (snapshot.followers) |followers| {
                    try S.line(w, allocator, "zk_followers\t{d}", .{followers});
                    try S.line(w, allocator, "zk_synced_followers\t{d}", .{snapshot.synced_followers orelse 0});
                    try S.line(w, allocator, "zk_pending_syncs\t{d}", .{snapshot.pending_syncs});
                }
                try S.line(w, allocator, "zk_last_proposal_size\t{d}", .{snapshot.last_proposal_size});
                try S.line(w, allocator, "zk_max_proposal_size\t{d}", .{snapshot.max_proposal_size});
            }
        },
    }
    return output.toOwnedSlice(allocator);
}

fn renderConnectionLine(
    allocator: std.mem.Allocator,
    output: *std.ArrayList(u8),
    connection: ConnectionInfo,
    brief: bool,
) !void {
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    const S = struct {
        fn append(list: *std.ArrayList(u8), a: std.mem.Allocator, comptime format: []const u8, args: anytype) !void {
            const chunk = try std.fmt.allocPrint(a, format, args);
            defer a.free(chunk);
            try list.appendSlice(a, chunk);
        }
    };
    try S.append(&text, allocator, " {s}[0](queued={d},recved={d},sent={d}", .{
        connection.remote,
        connection.queued,
        connection.received,
        connection.sent,
    });
    if (!brief and connection.session_id != 0) {
        try S.append(&text, allocator, ",sid=0x{x},lop={s},est={d},to={d}", .{
            @as(u64, @bitCast(connection.session_id)),
            connection.last_op,
            connection.established_ms,
            connection.session_timeout_ms,
        });
        if (connection.last_cxid >= 0) {
            try S.append(&text, allocator, ",lcxid=0x{x}", .{@as(u32, @bitCast(connection.last_cxid))});
        }
        try S.append(&text, allocator, ",lzxid=0x{x},lresp={d},llat={d},minlat={d},avglat={d:.3},maxlat={d}", .{
            @as(u64, @bitCast(connection.last_zxid)),
            connection.last_response_ms,
            connection.last_latency_ms,
            connection.min_latency_ms,
            connection.avg_latency_ms,
            connection.max_latency_ms,
        });
    }
    try text.appendSlice(allocator, ")");
    try output.appendSlice(allocator, text.items);
}

/// True when the first bytes of a connection look like a four-letter word
/// rather than a length-prefixed protocol frame.
pub fn looksLikeCommand(first_byte: u8) bool {
    return first_byte >= 'a' and first_byte <= 'z';
}

test "whitelist defaults to srvr and parses lists" {
    const default = Whitelist{};
    try std.testing.expect(default.allows(.srvr));
    try std.testing.expect(!default.allows(.mntr));

    const all = Whitelist.parse("*");
    try std.testing.expect(all.allows(.dump));
    try std.testing.expect(all.allows(.wchp));

    const mixed = Whitelist.parse("srvr, mntr, ruok, unknown");
    try std.testing.expect(mixed.allows(.srvr));
    try std.testing.expect(mixed.allows(.mntr));
    try std.testing.expect(mixed.allows(.ruok));
    try std.testing.expect(!mixed.allows(.stat));
}

test "command lookup accepts exact names only" {
    try std.testing.expectEqual(Command.ruok, commandFromName("ruok").?);
    try std.testing.expectEqual(@as(?Command, null), commandFromName("ruo"));
    try std.testing.expectEqual(@as(?Command, null), commandFromName("ruok "));
}

test "latency tracker computes min avg max over history" {
    var tracker = LatencyTracker(4){};
    tracker.record(5);
    tracker.record(1);
    tracker.record(9);
    var result = tracker.snapshot();
    try std.testing.expectEqual(@as(u32, 1), result.min);
    try std.testing.expectEqual(@as(u32, 9), result.max);
    try std.testing.expectApproxEqAbs(@as(f64, 5.0), result.avg, 0.001);

    tracker.record(3);
    result = tracker.snapshot();
    try std.testing.expectEqual(@as(u32, 1), result.min);
    try std.testing.expectApproxEqAbs(@as(f64, 4.5), result.avg, 0.001);

    tracker.reset();
    result = tracker.snapshot();
    try std.testing.expectEqual(@as(u32, 0), result.min);
    try std.testing.expectEqual(@as(f64, 0.0), result.avg);
}

test "ruok and isro render without trailing newline" {
    var counters = ServerCounters{};
    var snapshot = Snapshot{ .mode = "leader", .serving = true, .counters = &counters, .conf = undefined };
    const ruok = try render(std.testing.allocator, &snapshot, .ruok);
    defer std.testing.allocator.free(ruok);
    try std.testing.expectEqualStrings("imok", ruok);
    const isro = try render(std.testing.allocator, &snapshot, .isro);
    defer std.testing.allocator.free(isro);
    try std.testing.expectEqualStrings("rw", isro);
}

test "gtmk and stmk render the current trace mask" {
    var counters = ServerCounters{};
    var snapshot = Snapshot{
        .mode = "leader",
        .serving = true,
        .trace_mask = 0x1234,
        .counters = &counters,
        .conf = undefined,
    };
    const get_text = try render(std.testing.allocator, &snapshot, .gtmk);
    defer std.testing.allocator.free(get_text);
    try std.testing.expectEqualStrings("4660", get_text);
    const set_text = try render(std.testing.allocator, &snapshot, .stmk);
    defer std.testing.allocator.free(set_text);
    try std.testing.expectEqualStrings("4660", set_text);
}

test "srvr renders upstream field layout" {
    var counters = ServerCounters{};
    counters.packets_received.store(42, .release);
    counters.packets_sent.store(41, .release);
    counters.latency.record(2);
    var snapshot = Snapshot{
        .mode = "leader",
        .serving = true,
        .zxid = 0x100000012,
        .node_count = 7,
        .counters = &counters,
        .conf = undefined,
    };
    const text = try render(std.testing.allocator, &snapshot, .srvr);
    defer std.testing.allocator.free(text);
    try std.testing.expect(std.mem.indexOf(u8, text, "Zookeeper version: 3.9.5\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "Received: 42\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "Sent: 41\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "Zxid: 0x100000012\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "Mode: leader\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "Node count: 7\n") != null);
}

test "non serving commands report not serving" {
    var counters = ServerCounters{};
    var snapshot = Snapshot{ .mode = "looking", .serving = false, .counters = &counters, .conf = undefined };
    const text = try render(std.testing.allocator, &snapshot, .srvr);
    defer std.testing.allocator.free(text);
    try std.testing.expectEqualStrings(not_serving ++ "\n", text);
}

test "stat lists brief client lines" {
    var counters = ServerCounters{};
    const connections = [_]ConnectionInfo{.{
        .remote = "127.0.0.1:53144",
        .session_id = 0,
        .session_timeout_ms = 0,
        .established_ms = 0,
        .queued = 0,
        .received = 1,
        .sent = 0,
        .last_cxid = -1,
        .last_zxid = -1,
        .last_op = "",
        .last_response_ms = 0,
        .last_latency_ms = 0,
        .min_latency_ms = 0,
        .avg_latency_ms = 0,
        .max_latency_ms = 0,
    }};
    var snapshot = Snapshot{
        .mode = "follower",
        .serving = true,
        .counters = &counters,
        .connections = &connections,
        .conf = undefined,
    };
    const text = try render(std.testing.allocator, &snapshot, .stat);
    defer std.testing.allocator.free(text);
    try std.testing.expect(std.mem.indexOf(u8, text, "Clients:\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, " 127.0.0.1:53144[0](queued=0,recved=1,sent=0)\n") != null);
}

test "wchs renders the upstream summary lines" {
    var counters = ServerCounters{};
    var snapshot = Snapshot{
        .mode = "leader",
        .serving = true,
        .counters = &counters,
        .watch_summary = .{ .connections = 2, .paths = 3, .total = 4 },
        .conf = undefined,
    };
    const text = try render(std.testing.allocator, &snapshot, .wchs);
    defer std.testing.allocator.free(text);
    try std.testing.expect(std.mem.indexOf(u8, text, "2 connections watching 3 paths\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "Total watches:4\n") != null);
}

test "mntr emits tab separated zk keys" {
    var counters = ServerCounters{};
    var snapshot = Snapshot{
        .mode = "leader",
        .serving = true,
        .node_count = 12,
        .ephemeral_count = 3,
        .counters = &counters,
        .followers = 2,
        .synced_followers = 2,
        .conf = undefined,
    };
    const text = try render(std.testing.allocator, &snapshot, .mntr);
    defer std.testing.allocator.free(text);
    try std.testing.expect(std.mem.indexOf(u8, text, "zk_server_state\tleader\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "zk_znode_count\t12\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "zk_followers\t2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "zk_ephemerals_count\t3\n") != null);
}
