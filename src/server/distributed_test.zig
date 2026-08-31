const std = @import("std");
const raft = @import("raftz");
const command = @import("command.zig");
const data_tree = @import("data_tree.zig");
const state_machine = @import("state_machine.zig");

const node_count = 3;
const allocator = std.testing.allocator;

const MutationCompletion = struct {
    done: bool = false,
    code: ?data_tree.ErrorCode = null,
    zxid: i64 = 0,
    failure: ?anyerror = null,

    fn callback(ctx: *anyopaque, result: raft.ProposalResult) void {
        const self: *MutationCompletion = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => |bytes| {
                const decoded = command.decodeResult(bytes) catch {
                    self.failure = error.InvalidMutationResult;
                    self.done = true;
                    return;
                };
                self.code = decoded.code;
                self.zxid = decoded.zxid;
            },
            .err => |err| self.failure = err,
        }
        self.done = true;
    }
};

const ReadCompletion = struct {
    done: bool = false,
    failure: ?raft.Error = null,

    fn callback(ctx: *anyopaque, result: raft.ReadIndexResult) void {
        const self: *ReadCompletion = @ptrCast(@alignCast(ctx));
        switch (result) {
            .ok => {},
            .err => |err| self.failure = err,
        }
        self.done = true;
    }
};

const Cluster = struct {
    network: *raft.LoopbackNetwork,
    machines: [node_count]state_machine.ZooKeeperStateMachine = undefined,
    raftors: [node_count]*raft.Raftor = undefined,
    machine_count: usize = 0,
    raftor_count: usize = 0,

    fn create(root_path: []const u8) !*Cluster {
        const self = try allocator.create(Cluster);
        errdefer allocator.destroy(self);
        const network = try raft.LoopbackNetwork.create(allocator);
        self.* = .{ .network = network };
        errdefer self.deinitInitialized();

        var transports: [node_count]*raft.LoopbackTransport = undefined;
        for (0..node_count) |index| {
            transports[index] = try network.createTransport(index + 1);
        }

        var peers: [node_count]raft.Peer = undefined;
        for (&peers, 0..) |*peer, index| peer.* = .{ .id = index + 1 };

        for (0..node_count) |index| {
            const path = try std.fmt.allocPrint(allocator, "{s}/node-{}.rocksdb", .{ root_path, index + 1 });
            defer allocator.free(path);
            self.machines[index] = try state_machine.ZooKeeperStateMachine.init(allocator, path);
            self.machine_count += 1;

            var config = raft.RaftorConfig{};
            config.raft.id = index + 1;
            config.raft.election_tick = 10;
            config.raft.heartbeat_tick = 1;
            config.raft.election_timeout_seed = (index + 1) * 7919;
            config.raft.check_quorum = true;
            config.raft.pre_vote = true;
            config.initial_peers = &peers;
            config.proposal_timeout_ticks = 80;
            config.read_index_timeout_ticks = 80;
            config.snapshot_entries_threshold = 0;
            config.transport_poll_budget = 256;
            self.raftors[index] = try raft.Raftor.createWithTransport(
                allocator,
                config,
                self.machines[index].stateMachine(),
                transports[index].transport(),
            );
            self.raftor_count += 1;
        }
        return self;
    }

    fn destroy(self: *Cluster) void {
        self.deinitInitialized();
        allocator.destroy(self);
    }

    fn deinitInitialized(self: *Cluster) void {
        for (self.raftors[0..self.raftor_count]) |raftor| raftor.destroy();
        self.raftor_count = 0;
        for (self.machines[0..self.machine_count]) |*machine| machine.deinit();
        self.machine_count = 0;
        self.network.destroy();
    }

    fn tick(self: *Cluster) !void {
        for (self.raftors) |raftor| _ = try raftor.tick();
    }

    fn drive(self: *Cluster, rounds: usize) !void {
        for (0..rounds) |_| try self.tick();
    }

    fn waitForLeader(self: *Cluster, excluded_node_id: u64) !usize {
        for (0..200) |_| {
            var leader: ?usize = null;
            var duplicate = false;
            for (self.raftors, 0..) |raftor, index| {
                if (index + 1 == excluded_node_id or !raftor.isLeader()) continue;
                if (leader != null) duplicate = true else leader = index;
            }
            if (!duplicate) if (leader) |index| return index;
            try self.tick();
        }
        return error.LeaderElectionTimeout;
    }

    fn propose(self: *Cluster, node_index: usize, mutation: command.Mutation, completion: *MutationCompletion) !void {
        const bytes = try command.encode(allocator, mutation);
        defer allocator.free(bytes);
        try self.raftors[node_index].propose(bytes, .{
            .ctx = completion,
            .function = MutationCompletion.callback,
        });
    }

    fn waitForMutation(self: *Cluster, completion: *MutationCompletion) !void {
        for (0..200) |_| {
            if (completion.done) return;
            try self.tick();
        }
        return error.MutationTimeout;
    }

    fn proposeAndWait(self: *Cluster, node_index: usize, mutation: command.Mutation) !MutationCompletion {
        var completion = MutationCompletion{};
        try self.propose(node_index, mutation, &completion);
        try self.waitForMutation(&completion);
        return completion;
    }

    fn waitForPath(self: *Cluster, node_index: usize, path: []const u8, present: bool) !void {
        for (0..200) |_| {
            if (((try self.machines[node_index].exists(path)) != null) == present) return;
            try self.tick();
        }
        return error.StateConvergenceTimeout;
    }

    fn expectPathOnAll(self: *Cluster, path: []const u8, present: bool) !void {
        for (0..node_count) |index| {
            try self.waitForPath(index, path, present);
        }
    }
};

fn expectMutationCode(completion: MutationCompletion, expected: data_tree.ErrorCode) !void {
    try std.testing.expect(completion.failure == null);
    try std.testing.expectEqual(expected, completion.code.?);
}

var safety_isolated_node = std.atomic.Value(u64).init(0);

fn dropSafetyPartition(from: u64, to: u64, _: raft.MessageType) bool {
    const isolated = safety_isolated_node.load(.acquire);
    return isolated != 0 and (from == isolated or to == isolated);
}

pub fn testMinoritySafety() !void {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root_path);
    const cluster = try Cluster.create(root_path);
    defer cluster.destroy();
    safety_isolated_node.store(0, .release);
    defer safety_isolated_node.store(0, .release);

    try cluster.raftors[0].campaign();
    const old_leader = try cluster.waitForLeader(0);
    const old_leader_id = old_leader + 1;
    try expectMutationCode(try cluster.proposeAndWait(old_leader, .{ .create = .{
        .path = "/committed-before-partition",
        .data = "stable",
        .time_ms = 1,
    } }), .ok);
    try cluster.expectPathOnAll("/committed-before-partition", true);

    safety_isolated_node.store(old_leader_id, .release);
    cluster.network.drop_filter = dropSafetyPartition;

    var minority_write = MutationCompletion{};
    try cluster.propose(old_leader, .{ .create = .{
        .path = "/minority-write",
        .data = "must-not-commit",
        .time_ms = 2,
    } }, &minority_write);
    var stale_read = ReadCompletion{};
    try cluster.raftors[old_leader].readIndex("isolated-read", .{
        .ctx = &stale_read,
        .function = ReadCompletion.callback,
    });

    try cluster.drive(3);
    try std.testing.expect(!minority_write.done);
    try std.testing.expect(!stale_read.done);
    try std.testing.expect((try cluster.machines[old_leader].exists("/minority-write")) == null);

    const new_leader = try cluster.waitForLeader(old_leader_id);
    try std.testing.expect(new_leader != old_leader);
    try expectMutationCode(try cluster.proposeAndWait(new_leader, .{ .create = .{
        .path = "/majority-write",
        .data = "available",
        .time_ms = 3,
    } }), .ok);

    try cluster.waitForMutation(&minority_write);
    for (0..200) |_| {
        if (stale_read.done) break;
        try cluster.tick();
    }
    try std.testing.expect(stale_read.done);
    try std.testing.expect(minority_write.failure != null);
    try std.testing.expect(stale_read.failure != null);

    safety_isolated_node.store(0, .release);
    cluster.network.drop_filter = null;
    try cluster.expectPathOnAll("/majority-write", true);
    try cluster.expectPathOnAll("/minority-write", false);
}

var session_isolated_node = std.atomic.Value(u64).init(0);

fn dropSessionPartition(from: u64, to: u64, _: raft.MessageType) bool {
    const isolated = session_isolated_node.load(.acquire);
    return isolated != 0 and (from == isolated or to == isolated);
}

pub fn testSessionFencing() !void {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root_path);
    const cluster = try Cluster.create(root_path);
    defer cluster.destroy();
    session_isolated_node.store(0, .release);
    defer session_isolated_node.store(0, .release);

    try cluster.raftors[0].campaign();
    const old_leader = try cluster.waitForLeader(0);
    const password = [_]u8{0x42} ** 16;
    try expectMutationCode(try cluster.proposeAndWait(old_leader, .{ .open_session = .{
        .session_id = 42,
        .password = &password,
        .timeout_ms = 5_000,
        .tick_grace_ms = 500,
        .generation = 1,
    } }), .ok);
    try expectMutationCode(try cluster.proposeAndWait(old_leader, .{ .create = .{
        .path = "/owned-before-failover",
        .data = "ephemeral",
        .time_ms = 1,
        .ephemeral = true,
        .session_id = 42,
        .session_generation = 1,
    } }), .ok);
    try cluster.expectPathOnAll("/owned-before-failover", true);

    const old_leader_id = old_leader + 1;
    session_isolated_node.store(old_leader_id, .release);
    cluster.network.drop_filter = dropSessionPartition;
    const new_leader = try cluster.waitForLeader(old_leader_id);

    try expectMutationCode(try cluster.proposeAndWait(new_leader, .{ .move_session = .{
        .session_id = 42,
        .password = &password,
        .expected_generation = 1,
        .new_generation = 2,
    } }), .ok);
    try expectMutationCode(try cluster.proposeAndWait(new_leader, .{ .create = .{
        .path = "/stale-owner",
        .data = "rejected",
        .time_ms = 2,
        .ephemeral = true,
        .session_id = 42,
        .session_generation = 1,
    } }), .session_moved);
    try expectMutationCode(try cluster.proposeAndWait(new_leader, .{ .create = .{
        .path = "/fresh-owner",
        .data = "accepted",
        .time_ms = 3,
        .ephemeral = true,
        .session_id = 42,
        .session_generation = 2,
    } }), .ok);

    session_isolated_node.store(0, .release);
    cluster.network.drop_filter = null;
    try cluster.expectPathOnAll("/fresh-owner", true);
    try cluster.expectPathOnAll("/stale-owner", false);
    for (0..node_count) |index| {
        try std.testing.expectEqual(@as(u64, 2), (try cluster.machines[index].getSession(42)).?.generation);
    }

    const active_leader = try cluster.waitForLeader(0);
    try expectMutationCode(try cluster.proposeAndWait(active_leader, .{ .close_session = .{
        .session_id = 42,
        .password = &password,
        .generation = 2,
    } }), .ok);
    try cluster.expectPathOnAll("/owned-before-failover", false);
    try cluster.expectPathOnAll("/fresh-owner", false);
    for (0..node_count) |index| {
        try std.testing.expect((try cluster.machines[index].getSession(42)) == null);
    }
}

var snapshot_isolated_node = std.atomic.Value(u64).init(0);

fn dropSnapshotPartition(from: u64, to: u64, _: raft.MessageType) bool {
    const isolated = snapshot_isolated_node.load(.acquire);
    return isolated != 0 and (from == isolated or to == isolated);
}

pub fn testSnapshotCatchUp() !void {
    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(std.testing.io, ".", allocator);
    defer allocator.free(root_path);
    const cluster = try Cluster.create(root_path);
    defer cluster.destroy();
    snapshot_isolated_node.store(0, .release);
    defer snapshot_isolated_node.store(0, .release);

    try cluster.raftors[0].campaign();
    const leader = try cluster.waitForLeader(0);
    var lagging: usize = 0;
    while (lagging == leader) lagging += 1;
    snapshot_isolated_node.store(lagging + 1, .release);
    cluster.network.drop_filter = dropSnapshotPartition;

    try expectMutationCode(try cluster.proposeAndWait(leader, .{ .create = .{
        .path = "/snapshotted",
        .data = "v1",
        .time_ms = 1,
    } }), .ok);
    try expectMutationCode(try cluster.proposeAndWait(leader, .{ .set_data = .{
        .path = "/snapshotted",
        .data = "v2",
        .expected_version = 0,
        .time_ms = 2,
    } }), .ok);
    const password = [_]u8{0x77} ** 16;
    try expectMutationCode(try cluster.proposeAndWait(leader, .{ .open_session = .{
        .session_id = 77,
        .password = &password,
        .timeout_ms = 5_000,
        .tick_grace_ms = 500,
        .generation = 7,
    } }), .ok);
    try expectMutationCode(try cluster.proposeAndWait(leader, .{ .create = .{
        .path = "/snapshotted-ephemeral",
        .data = "owned",
        .time_ms = 3,
        .ephemeral = true,
        .session_id = 77,
        .session_generation = 7,
    } }), .ok);
    try std.testing.expect((try cluster.machines[lagging].exists("/snapshotted")) == null);

    try cluster.raftors[leader].takeSnapshot();
    snapshot_isolated_node.store(0, .release);
    cluster.network.drop_filter = null;
    try cluster.waitForPath(lagging, "/snapshotted-ephemeral", true);

    var restored = (try cluster.machines[lagging].getData(allocator, "/snapshotted")).?;
    defer restored.deinit(allocator);
    try std.testing.expectEqualStrings("v2", restored.data.?);
    try std.testing.expectEqual(@as(i32, 1), restored.stat.version);
    const restored_session = (try cluster.machines[lagging].getSession(77)).?;
    try std.testing.expectEqual(@as(u64, 7), restored_session.generation);
    try std.testing.expectEqualSlices(u8, &password, &restored_session.password);
}
