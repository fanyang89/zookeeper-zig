const std = @import("std");
const linux = std.os.linux;
const raft = @import("raftz");
const command = @import("command.zig");
const config_mod = @import("config.zig");
const data_tree = @import("data_tree.zig");
const distributed_test = @import("distributed_test.zig");
const state_machine = @import("state_machine.zig");

pub const Options = struct {
    tick_interval_ms: u64 = 50,
    election_tick: usize = 20,
    heartbeat_tick: usize = 2,
    proposal_timeout_ticks: u64 = 200,
    read_index_timeout_ticks: u64 = 200,
    snapshot_entries_threshold: u64 = 10_000,
    session_reap_interval_ms: u64 = 500,
};

pub const ProposalResponse = struct {
    allocator: std.mem.Allocator,
    storage: []u8,
    bytes: []u8,

    pub fn deinit(self: *ProposalResponse) void {
        self.allocator.free(self.storage);
        self.* = undefined;
    }
};

pub const Quorum = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    options: Options,
    machine: state_machine.ZooKeeperStateMachine,
    transport: *raft.GrpcLiteTransport,
    raftor: *raft.Raftor,
    driver_thread: std.Thread,
    session_reaper_thread: std.Thread,
    driver_stop: std.atomic.Value(bool) = .init(false),
    session_reaper_stop: std.atomic.Value(bool) = .init(false),
    driver_failed: std.atomic.Value(bool) = .init(false),
    running: bool = false,

    pub fn create(
        allocator: std.mem.Allocator,
        io: std.Io,
        config: *const config_mod.ServerConfig,
        options: Options,
    ) !*Quorum {
        if (options.tick_interval_ms == 0 or options.heartbeat_tick == 0 or
            options.election_tick <= options.heartbeat_tick or
            options.session_reap_interval_ms == 0 or
            options.session_reap_interval_ms > std.math.maxInt(i32)) return error.InvalidConfig;
        const self = try allocator.create(Quorum);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.io = io;
        self.options = options;
        self.driver_stop = .init(false);
        self.session_reaper_stop = .init(false);
        self.driver_failed = .init(false);
        self.running = false;

        try std.Io.Dir.cwd().createDirPath(io, config.data_dir);
        const data_dir = try std.fmt.allocPrintSentinel(allocator, "{s}", .{config.data_dir}, 0);
        defer allocator.free(data_dir);
        _ = try raft.realFileSystem().makeDir(data_dir);
        const state_dir = try std.fmt.allocPrint(allocator, "{s}/state.rocksdb", .{config.data_dir});
        defer allocator.free(state_dir);
        self.machine = try state_machine.ZooKeeperStateMachine.init(allocator, state_dir);
        errdefer self.machine.deinit();

        const max_transport_message = state_machine.max_snapshot_bytes + 1024 * 1024;
        self.transport = try raft.GrpcLiteTransport.create(allocator, .{
            .identity = .{ .cluster_id = config.cluster_id, .node_id = config.node_id },
            .listen_addr = config.raft_listen,
            .stream_limits = .{
                .max_message_size = max_transport_message,
                .max_inbound_buffer_size = max_transport_message + 5,
                .max_outbound_buffer_size = max_transport_message + 5,
            },
            .mailbox_max_bytes = max_transport_message * 2,
        });
        errdefer self.transport.destroy();

        var raft_config: raft.RaftorConfig = .{};
        raft_config.raft.id = config.node_id;
        raft_config.raft.election_tick = options.election_tick;
        raft_config.raft.heartbeat_tick = options.heartbeat_tick;
        raft_config.raft.check_quorum = true;
        raft_config.raft.pre_vote = true;
        raft_config.raft.disable_proposal_forwarding = false;
        raft_config.cluster_id = config.cluster_id;
        raft_config.listen_addr = config.raft_listen;
        raft_config.advertise_addr = config.raft_advertise;
        raft_config.initial_peers = config.peers;
        raft_config.join = config.join;
        raft_config.data_dir = config.data_dir;
        raft_config.tick_interval_ms = options.tick_interval_ms;
        raft_config.proposal_timeout_ticks = options.proposal_timeout_ticks;
        raft_config.read_index_timeout_ticks = options.read_index_timeout_ticks;
        raft_config.snapshot_entries_threshold = options.snapshot_entries_threshold;
        raft_config.checksum_enabled = true;
        self.raftor = try raft.Raftor.createWithTransport(
            allocator,
            raft_config,
            self.machine.stateMachine(),
            self.transport.transport(),
        );
        errdefer self.raftor.destroy();

        self.driver_thread = try std.Thread.spawn(.{}, runDriver, .{self});
        errdefer {
            self.driver_stop.store(true, .release);
            self.raftor.stop();
            self.driver_thread.join();
        }
        self.session_reaper_thread = try std.Thread.spawn(.{}, runSessionReaper, .{self});
        errdefer {
            self.session_reaper_stop.store(true, .release);
            self.session_reaper_thread.join();
        }
        self.running = true;
        return self;
    }

    pub fn shutdown(self: *Quorum) !void {
        if (!self.running) return;
        self.session_reaper_stop.store(true, .release);
        self.session_reaper_thread.join();
        self.driver_stop.store(true, .release);
        self.raftor.stop();
        self.driver_thread.join();
        self.running = false;
        if (self.driver_failed.load(.acquire)) return error.RaftDriverFailed;
    }

    pub fn deinit(self: *Quorum) void {
        std.debug.assert(!self.running);
        self.raftor.destroy();
        self.transport.destroy();
        self.machine.deinit();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn propose(self: *Quorum, mutation: command.Mutation) !ProposalResponse {
        const bytes = try command.encode(self.allocator, mutation);
        defer self.allocator.free(bytes);
        const response_storage = try self.allocator.alloc(u8, try command.resultCapacity(mutation));
        errdefer self.allocator.free(response_storage);
        var waiter = ProposalWaiter.init(self.io, response_storage);
        try self.raftor.propose(bytes, .{
            .ctx = &waiter,
            .function = ProposalWaiter.callback,
        });
        const response_length = try waiter.wait();
        return .{
            .allocator = self.allocator,
            .storage = response_storage,
            .bytes = response_storage[0..response_length],
        };
    }

    pub fn linearizableRead(self: *Quorum) !void {
        var waiter = ReadWaiter.init(self.io);
        try self.raftor.readIndex("", .{
            .ctx = &waiter,
            .function = ReadWaiter.callback,
        });
        try waiter.wait();
    }

    pub fn status(self: *const Quorum) raft.NodeStatus {
        return self.raftor.getStatus();
    }

    fn runDriver(self: *Quorum) void {
        while (!self.driver_stop.load(.acquire)) {
            _ = self.raftor.tick() catch |err| {
                if (!(err == error.ShuttingDown and self.driver_stop.load(.acquire))) {
                    self.driver_failed.store(true, .release);
                    raft.log.err(@src(), "ZooKeeper Raft driver stopped: {s}", .{@errorName(err)});
                }
                break;
            };
            if (self.driver_stop.load(.acquire)) break;
            sleepNanoseconds(self.options.tick_interval_ms * std.time.ns_per_ms);
        }
        if (self.driver_failed.load(.acquire)) self.raftor.stop();
    }

    fn runSessionReaper(self: *Quorum) void {
        var last_tick_ms: ?i64 = null;
        var last_leader_term: u64 = 0;
        while (!self.session_reaper_stop.load(.acquire)) {
            sleepNanoseconds(self.options.session_reap_interval_ms * std.time.ns_per_ms);
            if (self.session_reaper_stop.load(.acquire)) break;
            const leader_status = self.status();
            if (leader_status.role != .leader) {
                last_tick_ms = null;
                last_leader_term = 0;
                continue;
            }
            const monotonic_ms = std.Io.Clock.awake.now(self.io).toMilliseconds();
            if (last_tick_ms == null or last_leader_term != leader_status.term) {
                last_tick_ms = monotonic_ms;
                last_leader_term = leader_status.term;
                continue;
            }
            const elapsed_ms = monotonic_ms - last_tick_ms.?;
            if (elapsed_ms <= 0) continue;
            const bounded_elapsed = @min(
                elapsed_ms,
                @as(i64, @intCast(self.options.session_reap_interval_ms)),
            );
            var tick_response = self.propose(.{ .session_tick = .{
                .leader_term = leader_status.term,
                .elapsed_ms = bounded_elapsed,
                .leader_wall_ms = std.Io.Clock.real.now(self.io).toMilliseconds(),
            } }) catch |err| {
                if (!self.session_reaper_stop.load(.acquire)) {
                    raft.log.warn(@src(), "failed to advance session clock: {s}", .{@errorName(err)});
                }
                continue;
            };
            const tick_result = command.decodeResult(tick_response.bytes) catch {
                tick_response.deinit();
                continue;
            };
            tick_response.deinit();
            const current_status = self.status();
            if (tick_result.code == .ok) last_tick_ms.? += bounded_elapsed;
            if (tick_result.code != .ok or current_status.role != .leader or
                current_status.term != leader_status.term) continue;
            const expired = self.machine.expiredSessions(self.allocator) catch |err| {
                raft.log.warn(@src(), "failed to scan expired sessions: {s}", .{@errorName(err)});
                continue;
            };
            for (expired) |session| {
                if (self.session_reaper_stop.load(.acquire)) break;
                const latest_status = self.status();
                if (latest_status.role != .leader or latest_status.term != leader_status.term) break;
                var response = self.propose(.{ .expire_session = .{
                    .session_id = session.session_id,
                    .expected_expires_at_ms = session.expires_at_ms,
                } }) catch |err| {
                    if (!self.session_reaper_stop.load(.acquire)) {
                        raft.log.warn(@src(), "failed to expire session {}: {s}", .{
                            session.session_id,
                            @errorName(err),
                        });
                    }
                    continue;
                };
                response.deinit();
            }
            self.allocator.free(expired);

            const extended = self.machine.expiredExtendedNodes(self.allocator, 64) catch |err| {
                raft.log.warn(@src(), "failed to scan expired extended nodes: {s}", .{@errorName(err)});
                continue;
            };
            for (extended) |candidate| {
                if (self.session_reaper_stop.load(.acquire)) break;
                const latest_status = self.status();
                if (latest_status.role != .leader or latest_status.term != leader_status.term) break;
                var response = self.propose(.{ .delete_extended = .{
                    .path = candidate.path,
                    .expected_czxid = candidate.czxid,
                    .expected_kind = candidate.kind,
                } }) catch |err| {
                    if (!self.session_reaper_stop.load(.acquire)) {
                        raft.log.warn(@src(), "failed to delete extended node {s}: {s}", .{
                            candidate.path,
                            @errorName(err),
                        });
                    }
                    continue;
                };
                response.deinit();
            }
            for (extended) |candidate| self.allocator.free(candidate.path);
            self.allocator.free(extended);
        }
    }
};

const ProposalWaiter = struct {
    io: std.Io,
    response_storage: []u8,
    done: std.atomic.Value(u32) = .init(0),
    response_length: usize = 0,
    failure: ?raft.Error = null,

    fn init(io: std.Io, response_storage: []u8) ProposalWaiter {
        return .{ .io = io, .response_storage = response_storage };
    }

    fn callback(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *ProposalWaiter = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => |response| {
                if (response.len > self.response_storage.len) {
                    self.failure = error.Fatal;
                } else {
                    @memcpy(self.response_storage[0..response.len], response);
                    self.response_length = response.len;
                }
            },
            .err => |err| self.failure = err,
        }
        self.done.store(1, .release);
        std.Io.futexWake(self.io, u32, &self.done.raw, 1);
    }

    fn wait(self: *ProposalWaiter) !usize {
        while (self.done.load(.acquire) == 0) {
            std.Io.futexWaitUncancelable(self.io, u32, &self.done.raw, 0);
        }
        if (self.failure) |err| return err;
        return self.response_length;
    }
};

const ReadWaiter = struct {
    io: std.Io,
    done: std.atomic.Value(u32) = .init(0),
    failure: ?raft.Error = null,

    fn init(io: std.Io) ReadWaiter {
        return .{ .io = io };
    }

    fn callback(ctx: *anyopaque, result: raft.ReadIndexResult) void {
        const self: *ReadWaiter = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => {},
            .err => |err| self.failure = err,
        }
        self.done.store(1, .release);
        std.Io.futexWake(self.io, u32, &self.done.raw, 1);
    }

    fn wait(self: *ReadWaiter) !void {
        while (self.done.load(.acquire) == 0) {
            std.Io.futexWaitUncancelable(self.io, u32, &self.done.raw, 0);
        }
        if (self.failure) |err| return err;
    }
};

fn sleepNanoseconds(nanoseconds: u64) void {
    var request = linux.timespec{
        .sec = std.math.cast(isize, nanoseconds / std.time.ns_per_s) orelse std.math.maxInt(isize),
        .nsec = @intCast(nanoseconds % std.time.ns_per_s),
    };
    var remaining: linux.timespec = undefined;
    while (true) {
        const rc = linux.nanosleep(&request, &remaining);
        switch (linux.errno(rc)) {
            .SUCCESS => return,
            .INTR => request = remaining,
            else => return,
        }
    }
}

fn reserveLoopbackPort(io: std.Io) !u16 {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try address.listen(io, .{});
    defer server.deinit(io);
    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        server.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    return std.mem.bigToNative(u16, local_address.port);
}

fn waitForLeader(quorum: *Quorum, io: std.Io) !void {
    var attempts: usize = 0;
    while (attempts < 500) : (attempts += 1) {
        if (quorum.status().leader_id != 0) return;
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.LeaderElectionTimeout;
}

test "single-node quorum persists committed state through WAL restart" {
    const testing = std.testing;
    try raft.log.initGlobal(std.heap.smp_allocator, testing.io, false);
    defer raft.log.deinitGlobal(std.heap.smp_allocator);

    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(root_path);
    const raft_port = try reserveLoopbackPort(testing.io);
    const client_port = try reserveLoopbackPort(testing.io);
    const raft_endpoint = try std.fmt.allocPrint(testing.allocator, "127.0.0.1:{}", .{raft_port});
    defer testing.allocator.free(raft_endpoint);
    const client_endpoint = try std.fmt.allocPrint(testing.allocator, "127.0.0.1:{}", .{client_port});
    defer testing.allocator.free(client_endpoint);
    const peer = try std.fmt.allocPrint(testing.allocator, "1={s}", .{raft_endpoint});
    defer testing.allocator.free(peer);
    const arguments = [_][]const u8{
        "--node-id",       "1",
        "--cluster-id",    "0198f54d-5c2a-7000-8000-000000000001",
        "--client-listen", client_endpoint,
        "--raft-listen",   raft_endpoint,
        "--data-dir",      root_path,
        "--peer",          peer,
    };
    var config = try config_mod.parse(testing.allocator, &arguments);
    defer config.deinit();
    const options = Options{
        .tick_interval_ms = 10,
        .election_tick = 5,
        .heartbeat_tick = 1,
        .proposal_timeout_ticks = 100,
        .read_index_timeout_ticks = 100,
        .snapshot_entries_threshold = 1,
        .session_reap_interval_ms = 10,
    };

    {
        const quorum = try Quorum.create(std.heap.smp_allocator, testing.io, &config, options);
        defer quorum.deinit();
        try waitForLeader(quorum, testing.io);
        const password = [_]u8{0x33} ** 16;
        const now_ms = std.Io.Clock.real.now(testing.io).toMilliseconds();
        var opened_session = try quorum.propose(.{ .open_session = .{
            .session_id = 77,
            .password = &password,
            .timeout_ms = 200,
            .tick_grace_ms = 10,
            .generation = 1,
        } });
        const opened_result = try command.decodeResult(opened_session.bytes);
        try testing.expectEqual(data_tree.ErrorCode.ok, opened_result.code);
        opened_session.deinit();
        var ephemeral = try quorum.propose(.{ .create = .{
            .path = "/expires-with-session",
            .data = "temporary",
            .time_ms = now_ms,
            .ephemeral = true,
            .session_id = 77,
            .session_generation = 1,
        } });
        const ephemeral_result = try command.decodeResult(ephemeral.bytes);
        try testing.expectEqual(data_tree.ErrorCode.ok, ephemeral_result.code);
        ephemeral.deinit();
        var expiration_attempts: usize = 0;
        while ((try quorum.machine.getSession(77)) != null and expiration_attempts < 100) : (expiration_attempts += 1) {
            try testing.io.sleep(.fromMilliseconds(10), .awake);
        }
        try testing.expect((try quorum.machine.getSession(77)) == null);
        try testing.expect((try quorum.machine.exists("/expires-with-session")) == null);

        var response = try quorum.propose(.{ .create = .{
            .path = "/durable",
            .data = "value",
            .time_ms = 1,
        } });
        defer response.deinit();
        const result = try command.decodeResult(response.bytes);
        try testing.expectEqual(@as(i32, 0), @intFromEnum(result.code));
        try quorum.linearizableRead();
        var stored = (try quorum.machine.getData(testing.allocator, "/durable")).?;
        defer stored.deinit(testing.allocator);
        try testing.expectEqualStrings("value", stored.data.?);
        try quorum.shutdown();
    }

    {
        const quorum = try Quorum.create(std.heap.smp_allocator, testing.io, &config, options);
        defer quorum.deinit();
        try waitForLeader(quorum, testing.io);
        try quorum.linearizableRead();
        var restored = (try quorum.machine.getData(testing.allocator, "/durable")).?;
        defer restored.deinit(testing.allocator);
        try testing.expectEqualStrings("value", restored.data.?);
        try quorum.shutdown();
    }
}

fn reserveLoopbackPorts(io: std.Io, output: []u16) !void {
    const listeners = try std.heap.smp_allocator.alloc(std.Io.net.Server, output.len);
    defer std.heap.smp_allocator.free(listeners);
    var initialized: usize = 0;
    defer for (listeners[0..initialized]) |*listener| listener.deinit(io);
    for (output, 0..) |*port, index| {
        const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
        listeners[index] = try address.listen(io, .{});
        initialized += 1;
        var local_address: std.posix.sockaddr.in = undefined;
        var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
        if (std.posix.errno(std.posix.system.getsockname(
            listeners[index].socket.handle,
            @ptrCast(&local_address),
            &address_length,
        )) != .SUCCESS) return error.AddressQueryFailed;
        port.* = std.mem.bigToNative(u16, local_address.port);
    }
}

fn waitForClusterLeader(nodes: []const ?*Quorum, io: std.Io) !u64 {
    var attempts: usize = 0;
    while (attempts < 1_000) : (attempts += 1) {
        var elected: u64 = 0;
        var leaders: usize = 0;
        var consistent = true;
        var reports: usize = 0;
        for (nodes) |maybe_node| if (maybe_node) |node| {
            const status = node.status();
            if (status.role == .leader) {
                elected = status.id;
                leaders += 1;
            }
            if (status.leader_id == 0) consistent = false;
            reports += 1;
        };
        if (leaders == 1) {
            for (nodes) |maybe_node| if (maybe_node) |node| {
                if (node.status().leader_id != elected) consistent = false;
            };
        }
        if (consistent and reports >= 2 and leaders == 1) return elected;
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.LeaderElectionTimeout;
}

fn waitForReplicatedPath(node: *Quorum, path: []const u8, io: std.Io) !void {
    var attempts: usize = 0;
    while (attempts < 500) : (attempts += 1) {
        node.linearizableRead() catch {
            try io.sleep(.fromMilliseconds(10), .awake);
            continue;
        };
        if ((try node.machine.exists(path)) != null) return;
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    return error.ReplicationTimeout;
}

test "three-node quorum replicates through follower and survives leader restart" {
    const testing = std.testing;
    try raft.log.initGlobal(std.heap.smp_allocator, testing.io, false);
    defer raft.log.deinitGlobal(std.heap.smp_allocator);

    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(root_path);

    var ports: [6]u16 = undefined;
    try reserveLoopbackPorts(testing.io, &ports);
    var raft_endpoints: [3][]u8 = undefined;
    var client_endpoints: [3][]u8 = undefined;
    var peer_values: [3][]u8 = undefined;
    var data_dirs: [3][]u8 = undefined;
    for (0..3) |index| {
        raft_endpoints[index] = try std.fmt.allocPrint(testing.allocator, "127.0.0.1:{}", .{ports[index]});
        client_endpoints[index] = try std.fmt.allocPrint(testing.allocator, "127.0.0.1:{}", .{ports[index + 3]});
        peer_values[index] = try std.fmt.allocPrint(testing.allocator, "{}={s}", .{ index + 1, raft_endpoints[index] });
        data_dirs[index] = try std.fmt.allocPrint(testing.allocator, "{s}/node{}", .{ root_path, index + 1 });
    }
    defer for (0..3) |index| {
        testing.allocator.free(data_dirs[index]);
        testing.allocator.free(peer_values[index]);
        testing.allocator.free(client_endpoints[index]);
        testing.allocator.free(raft_endpoints[index]);
    };

    var configs: [3]config_mod.ServerConfig = undefined;
    var config_count: usize = 0;
    defer for (configs[0..config_count]) |*config| config.deinit();
    for (0..3) |index| {
        const node_id = try std.fmt.allocPrint(testing.allocator, "{}", .{index + 1});
        defer testing.allocator.free(node_id);
        const arguments = [_][]const u8{
            "--node-id",       node_id,
            "--cluster-id",    "0198f54d-5c2a-7000-8000-000000000003",
            "--client-listen", client_endpoints[index],
            "--raft-listen",   raft_endpoints[index],
            "--data-dir",      data_dirs[index],
            "--peer",          peer_values[0],
            "--peer",          peer_values[1],
            "--peer",          peer_values[2],
        };
        configs[index] = try config_mod.parse(testing.allocator, &arguments);
        config_count += 1;
    }

    const options = Options{
        .tick_interval_ms = 10,
        .election_tick = 10,
        .heartbeat_tick = 1,
        .proposal_timeout_ticks = 300,
        .read_index_timeout_ticks = 300,
        .snapshot_entries_threshold = 2,
        .session_reap_interval_ms = 10,
    };
    var nodes: [3]?*Quorum = .{ null, null, null };
    defer for (&nodes) |*maybe_node| if (maybe_node.*) |node| {
        if (node.running) node.shutdown() catch {};
        node.deinit();
        maybe_node.* = null;
    };
    for (0..3) |index| {
        nodes[index] = try Quorum.create(std.heap.smp_allocator, testing.io, &configs[index], options);
    }

    const first_leader = try waitForClusterLeader(&nodes, testing.io);
    var follower: *Quorum = undefined;
    for (nodes) |maybe_node| if (maybe_node) |node| {
        if (node.status().id != first_leader) {
            follower = node;
            break;
        }
    };
    var first_response = try follower.propose(.{ .create = .{
        .path = "/cluster",
        .data = "v1",
        .time_ms = 1,
    } });
    defer first_response.deinit();
    try testing.expectEqual(@as(i32, 0), @intFromEnum((try command.decodeResult(first_response.bytes)).code));
    for (nodes) |maybe_node| try waitForReplicatedPath(maybe_node.?, "/cluster", testing.io);

    const stopped_index: usize = @intCast(first_leader - 1);
    try nodes[stopped_index].?.shutdown();
    nodes[stopped_index].?.deinit();
    nodes[stopped_index] = null;
    const second_leader = try waitForClusterLeader(&nodes, testing.io);
    try testing.expect(second_leader != first_leader);

    var survivor: *Quorum = undefined;
    for (nodes) |maybe_node| if (maybe_node) |node| {
        if (node.status().id != second_leader) {
            survivor = node;
            break;
        }
    };
    var second_response = try survivor.propose(.{ .create = .{
        .path = "/after-failover",
        .data = "v2",
        .time_ms = 2,
    } });
    defer second_response.deinit();
    try testing.expectEqual(@as(i32, 0), @intFromEnum((try command.decodeResult(second_response.bytes)).code));

    nodes[stopped_index] = try Quorum.create(
        std.heap.smp_allocator,
        testing.io,
        &configs[stopped_index],
        options,
    );
    _ = try waitForClusterLeader(&nodes, testing.io);
    try waitForReplicatedPath(nodes[stopped_index].?, "/after-failover", testing.io);
    var restored = (try nodes[stopped_index].?.machine.getData(
        testing.allocator,
        "/after-failover",
    )).?;
    defer restored.deinit(testing.allocator);
    try testing.expectEqualStrings("v2", restored.data.?);
}

test "deterministic quorum rejects minority writes and stale reads" {
    try distributed_test.testMinoritySafety();
}

test "deterministic quorum fences stale sessions across leader failover" {
    try distributed_test.testSessionFencing();
}

test "deterministic quorum restores lagging application state from snapshot" {
    try distributed_test.testSnapshotCatchUp();
}
