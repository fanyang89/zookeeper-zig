const std = @import("std");
const linux = std.os.linux;
const raft = @import("raftz");
const command = @import("command.zig");
const config_mod = @import("config.zig");
const state_machine = @import("state_machine.zig");

pub const Options = struct {
    tick_interval_ms: u64 = 50,
    election_tick: usize = 20,
    heartbeat_tick: usize = 2,
    proposal_timeout_ticks: u64 = 200,
    read_index_timeout_ticks: u64 = 200,
    snapshot_entries_threshold: u64 = 10_000,
};

pub const ProposalResponse = struct {
    bytes: []u8,

    pub fn deinit(self: *ProposalResponse, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
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
    driver_stop: std.atomic.Value(bool) = .init(false),
    driver_failed: std.atomic.Value(bool) = .init(false),
    running: bool = false,

    pub fn create(
        allocator: std.mem.Allocator,
        io: std.Io,
        config: *const config_mod.ServerConfig,
        options: Options,
    ) !*Quorum {
        if (options.tick_interval_ms == 0 or options.heartbeat_tick == 0 or
            options.election_tick <= options.heartbeat_tick) return error.InvalidConfig;
        const self = try allocator.create(Quorum);
        errdefer allocator.destroy(self);
        self.allocator = allocator;
        self.io = io;
        self.options = options;
        self.driver_stop = .init(false);
        self.driver_failed = .init(false);
        self.running = false;
        self.machine = try state_machine.ZooKeeperStateMachine.init(allocator);
        errdefer self.machine.deinit();

        const data_dir = try std.fmt.allocPrintSentinel(allocator, "{s}", .{config.data_dir}, 0);
        defer allocator.free(data_dir);
        _ = try raft.realFileSystem().makeDir(data_dir);

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
        self.running = true;
        return self;
    }

    pub fn shutdown(self: *Quorum) !void {
        if (!self.running) return;
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
        var waiter = ProposalWaiter.init(self.allocator, self.io);
        try self.raftor.propose(bytes, .{
            .ctx = &waiter,
            .function = ProposalWaiter.callback,
        });
        return .{ .bytes = try waiter.wait() };
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
};

const ProposalWaiter = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    done: std.atomic.Value(u32) = .init(0),
    response: ?[]u8 = null,
    failure: ?raft.Error = null,

    fn init(allocator: std.mem.Allocator, io: std.Io) ProposalWaiter {
        return .{ .allocator = allocator, .io = io };
    }

    fn callback(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *ProposalWaiter = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => |response| self.response = self.allocator.dupe(u8, response) catch blk: {
                self.failure = error.OutOfMemory;
                break :blk null;
            },
            .err => |err| self.failure = err,
        }
        self.done.store(1, .release);
        std.Io.futexWake(self.io, u32, &self.done.raw, 1);
    }

    fn wait(self: *ProposalWaiter) ![]u8 {
        while (self.done.load(.acquire) == 0) {
            std.Io.futexWaitUncancelable(self.io, u32, &self.done.raw, 0);
        }
        if (self.failure) |err| return err;
        return self.response.?;
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
    };

    {
        const quorum = try Quorum.create(std.heap.smp_allocator, testing.io, &config, options);
        defer quorum.deinit();
        try waitForLeader(quorum, testing.io);
        var response = try quorum.propose(.{ .create = .{
            .path = "/durable",
            .data = "value",
            .time_ms = 1,
        } });
        defer response.deinit(std.heap.smp_allocator);
        const result = try command.decodeResult(response.bytes);
        try testing.expectEqual(@as(i32, 0), @intFromEnum(result.code));
        try quorum.linearizableRead();
        var stored = (try quorum.machine.getData(testing.allocator, "/durable")).?;
        defer stored.deinit(testing.allocator);
        try testing.expectEqualStrings("value", stored.data);
        try quorum.shutdown();
    }

    {
        const quorum = try Quorum.create(std.heap.smp_allocator, testing.io, &config, options);
        defer quorum.deinit();
        try waitForLeader(quorum, testing.io);
        try quorum.linearizableRead();
        var restored = (try quorum.machine.getData(testing.allocator, "/durable")).?;
        defer restored.deinit(testing.allocator);
        try testing.expectEqualStrings("value", restored.data);
        try quorum.shutdown();
    }
}
