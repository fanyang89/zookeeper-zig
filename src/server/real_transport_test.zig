const std = @import("std");
const raft = @import("raftz");
const command = @import("command.zig");
const config_mod = @import("config.zig");
const data_tree = @import("data_tree.zig");
const state_machine = @import("state_machine.zig");

const node_count = 3;
const allocator = std.heap.smp_allocator;

const FaultController = struct {
    isolated_node: std.atomic.Value(u64) = .init(0),

    fn isolate(self: *FaultController, node_id: u64) void {
        self.isolated_node.store(node_id, .release);
    }

    fn heal(self: *FaultController) void {
        self.isolated_node.store(0, .release);
    }

    fn shouldDrop(self: *const FaultController, message: raft.Message) bool {
        const isolated = self.isolated_node.load(.acquire);
        return isolated != 0 and (message.from == isolated or message.to == isolated);
    }
};

const FaultTransport = struct {
    allocator: std.mem.Allocator,
    inner: raft.Transport,
    controller: *FaultController,
    callback: ?raft.MessageCallback = null,

    fn init(
        transport_allocator: std.mem.Allocator,
        inner: raft.Transport,
        controller: *FaultController,
    ) FaultTransport {
        return .{
            .allocator = transport_allocator,
            .inner = inner,
            .controller = controller,
        };
    }

    fn cast(ctx: *anyopaque) *FaultTransport {
        return @ptrCast(@alignCast(ctx));
    }

    fn start(ctx: *anyopaque) raft.Error!void {
        return cast(ctx).inner.start();
    }

    fn stop(ctx: *anyopaque) void {
        cast(ctx).inner.stop();
    }

    fn addPeer(ctx: *anyopaque, id: u64, address: []const u8) raft.Error!bool {
        return cast(ctx).inner.addPeer(id, address);
    }

    fn removePeer(ctx: *anyopaque, id: u64) raft.Error!void {
        return cast(ctx).inner.removePeer(id);
    }

    fn send(ctx: *anyopaque, messages: []const raft.Message) raft.Error!void {
        const self = cast(ctx);
        for (messages) |message| {
            if (self.controller.shouldDrop(message)) continue;
            try self.inner.send(&.{message});
        }
    }

    fn setMessageCallback(ctx: *anyopaque, callback: ?raft.MessageCallback) void {
        const self = cast(ctx);
        self.callback = callback;
        self.inner.setMessageCallback(if (callback == null) null else .{
            .ctx = self,
            .function = receive,
        });
    }

    fn receive(ctx: *anyopaque, message: raft.Message) raft.Error!void {
        const self = cast(ctx);
        if (self.controller.shouldDrop(message)) {
            var owned = message;
            owned.deinit(self.allocator);
            return;
        }
        const callback = self.callback orelse {
            var owned = message;
            owned.deinit(self.allocator);
            return;
        };
        return callback.invoke(message);
    }

    fn setPeerEventCallback(ctx: *anyopaque, callback: ?raft.PeerEventCallback) void {
        cast(ctx).inner.setPeerEventCallback(callback);
    }

    fn pollOne(ctx: *anyopaque) raft.Error!bool {
        return cast(ctx).inner.pollOne();
    }

    fn identity(ctx: *anyopaque) raft.TransportIdentity {
        return cast(ctx).inner.identity().?;
    }

    const vtable: raft.Transport.VTable = .{
        .start = start,
        .stop = stop,
        .add_peer = addPeer,
        .remove_peer = removePeer,
        .send = send,
        .set_message_callback = setMessageCallback,
        .set_peer_event_callback = setPeerEventCallback,
        .poll_one = pollOne,
        .identity = identity,
    };

    fn transport(self: *FaultTransport) raft.Transport {
        return .{ .ctx = self, .vtable = &vtable };
    }
};

fn RealCluster(comptime QuorumType: type, comptime OptionsType: type) type {
    return struct {
        const Self = @This();

        io: std.Io,
        configs: *[node_count]config_mod.ServerConfig,
        options: OptionsType,
        controller: *FaultController,
        raw_transports: [node_count]?*raft.GrpcLiteTransport = .{null} ** node_count,
        fault_transports: [node_count]FaultTransport = undefined,
        nodes: [node_count]?*QuorumType = .{null} ** node_count,

        fn create(
            io: std.Io,
            configs: *[node_count]config_mod.ServerConfig,
            options: OptionsType,
            controller: *FaultController,
        ) !*Self {
            const self = try allocator.create(Self);
            self.* = .{
                .io = io,
                .configs = configs,
                .options = options,
                .controller = controller,
            };
            errdefer self.destroy();
            for (0..node_count) |index| try self.startNode(index);
            return self;
        }

        fn destroy(self: *Self) void {
            for (0..node_count) |index| self.stopNode(index) catch {};
            allocator.destroy(self);
        }

        fn createRawTransport(self: *Self, index: usize) !*raft.GrpcLiteTransport {
            const max_transport_message = state_machine.max_snapshot_bytes + 1024 * 1024;
            return raft.GrpcLiteTransport.create(allocator, .{
                .identity = .{
                    .cluster_id = self.configs[index].cluster_id,
                    .node_id = self.configs[index].node_id,
                },
                .listen_addr = self.configs[index].raft_listen,
                .stream_limits = .{
                    .max_message_size = max_transport_message,
                    .max_inbound_buffer_size = max_transport_message + 5,
                    .max_outbound_buffer_size = max_transport_message + 5,
                },
                .mailbox_max_bytes = max_transport_message * 2,
                .reconnect_initial_delay_ns = 2 * std.time.ns_per_ms,
                .reconnect_max_delay_ns = 20 * std.time.ns_per_ms,
                .graceful_shutdown_timeout_ns = 50 * std.time.ns_per_ms,
            });
        }

        fn startNode(self: *Self, index: usize) !void {
            if (self.nodes[index] != null or self.raw_transports[index] != null) {
                return error.NodeAlreadyRunning;
            }
            const raw = try self.createRawTransport(index);
            self.raw_transports[index] = raw;
            errdefer {
                raw.destroy();
                self.raw_transports[index] = null;
            }
            self.fault_transports[index] = FaultTransport.init(
                allocator,
                raw.transport(),
                self.controller,
            );
            self.nodes[index] = try QuorumType.createWithTransport(
                allocator,
                self.io,
                &self.configs[index],
                self.options,
                self.fault_transports[index].transport(),
            );
        }

        fn stopNode(self: *Self, index: usize) !void {
            var shutdown_error: ?anyerror = null;
            if (self.nodes[index]) |node| {
                if (node.running) node.shutdown() catch |err| {
                    shutdown_error = err;
                };
                node.deinit();
                self.nodes[index] = null;
            }
            if (self.raw_transports[index]) |transport| {
                transport.destroy();
                self.raw_transports[index] = null;
            }
            if (shutdown_error) |err| return err;
        }

        fn waitForLeader(self: *Self, excluded_node_id: u64) !usize {
            for (0..1_000) |_| {
                var found: ?usize = null;
                var duplicate = false;
                for (self.nodes, 0..) |maybe_node, index| if (maybe_node) |node| {
                    if (index + 1 == excluded_node_id or node.status().role != .leader) continue;
                    if (found != null) duplicate = true else found = index;
                };
                if (!duplicate) if (found) |index| return index;
                try self.io.sleep(.fromMilliseconds(10), .awake);
            }
            return error.LeaderElectionTimeout;
        }

        fn waitForSingleLeader(self: *Self) !usize {
            for (0..1_000) |_| {
                var found: ?usize = null;
                var duplicate = false;
                for (self.nodes, 0..) |maybe_node, index| if (maybe_node) |node| {
                    if (node.status().role != .leader) continue;
                    if (found != null) duplicate = true else found = index;
                };
                if (!duplicate) if (found) |index| return index;
                try self.io.sleep(.fromMilliseconds(10), .awake);
            }
            return error.LeaderElectionTimeout;
        }

        fn waitForPath(self: *Self, index: usize, path: []const u8, present: bool) !void {
            for (0..500) |_| {
                const node = self.nodes[index] orelse return error.NodeStopped;
                node.linearizableRead() catch {
                    try self.io.sleep(.fromMilliseconds(10), .awake);
                    continue;
                };
                if (((try node.machine.exists(path)) != null) == present) return;
                try self.io.sleep(.fromMilliseconds(10), .awake);
            }
            return error.ReplicationTimeout;
        }

        fn expectPathOnRunningNodes(self: *Self, path: []const u8, present: bool) !void {
            for (0..node_count) |index| {
                if (self.nodes[index] != null) try self.waitForPath(index, path, present);
            }
        }

        fn waitForPeerOpenCount(self: *Self, from: usize, peer_id: u64, minimum: u64) !void {
            for (0..500) |_| {
                if (self.raw_transports[from].?.peerOpenCount(peer_id) >= minimum) return;
                try self.io.sleep(.fromMilliseconds(10), .awake);
            }
            return error.TransportReconnectTimeout;
        }
    };
}

fn reserveLoopbackPorts(io: std.Io, output: []u16) !void {
    const listeners = try allocator.alloc(std.Io.net.Server, output.len);
    defer allocator.free(listeners);
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

fn expectMutationCode(
    node: anytype,
    mutation: command.Mutation,
    expected: data_tree.ErrorCode,
) !void {
    var response = try node.propose(mutation);
    defer response.deinit();
    try std.testing.expectEqual(expected, (try command.decodeResult(response.bytes)).code);
}

pub fn testRealTransportFaults(comptime QuorumType: type, options: anytype) !void {
    const testing = std.testing;
    try raft.log.initGlobal(allocator, testing.io, false);
    defer raft.log.deinitGlobal(allocator);

    var temporary = std.testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const root_path = try temporary.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(root_path);

    var ports: [node_count * 2]u16 = undefined;
    try reserveLoopbackPorts(testing.io, &ports);
    var raft_endpoints: [node_count][]u8 = undefined;
    var client_endpoints: [node_count][]u8 = undefined;
    var peer_values: [node_count][]u8 = undefined;
    var data_dirs: [node_count][]u8 = undefined;
    for (0..node_count) |index| {
        raft_endpoints[index] = try std.fmt.allocPrint(testing.allocator, "127.0.0.1:{}", .{ports[index]});
        client_endpoints[index] = try std.fmt.allocPrint(testing.allocator, "127.0.0.1:{}", .{ports[index + node_count]});
        peer_values[index] = try std.fmt.allocPrint(testing.allocator, "{}={s}", .{ index + 1, raft_endpoints[index] });
        data_dirs[index] = try std.fmt.allocPrint(testing.allocator, "{s}/node{}", .{ root_path, index + 1 });
    }
    defer for (0..node_count) |index| {
        testing.allocator.free(data_dirs[index]);
        testing.allocator.free(peer_values[index]);
        testing.allocator.free(client_endpoints[index]);
        testing.allocator.free(raft_endpoints[index]);
    };

    var configs: [node_count]config_mod.ServerConfig = undefined;
    var config_count: usize = 0;
    defer for (configs[0..config_count]) |*config| config.deinit();
    for (0..node_count) |index| {
        const node_id = try std.fmt.allocPrint(testing.allocator, "{}", .{index + 1});
        defer testing.allocator.free(node_id);
        const arguments = [_][]const u8{
            "--node-id",       node_id,
            "--cluster-id",    "0198f54d-5c2a-7000-8000-000000000004",
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

    var controller = FaultController{};
    const Cluster = RealCluster(QuorumType, @TypeOf(options));
    const cluster = try Cluster.create(testing.io, &configs, options, &controller);
    defer cluster.destroy();

    const old_leader = try cluster.waitForSingleLeader();
    try expectMutationCode(cluster.nodes[old_leader].?, .{ .create = .{
        .path = "/before-real-partition",
        .data = "stable",
        .time_ms = 1,
    } }, .ok);
    try cluster.expectPathOnRunningNodes("/before-real-partition", true);

    controller.isolate(old_leader + 1);
    const minority_result = cluster.nodes[old_leader].?.propose(.{ .create = .{
        .path = "/real-minority-write",
        .data = "must-not-commit",
        .time_ms = 2,
    } });
    if (minority_result) |response_value| {
        var response = response_value;
        response.deinit();
        return error.MinorityWriteCommitted;
    } else |_| {}
    if (cluster.nodes[old_leader].?.linearizableRead()) |_| {
        return error.IsolatedReadCompleted;
    } else |_| {}

    const new_leader = try cluster.waitForLeader(old_leader + 1);
    try expectMutationCode(cluster.nodes[new_leader].?, .{ .create = .{
        .path = "/real-majority-write",
        .data = "available",
        .time_ms = 3,
    } }, .ok);

    controller.heal();
    try cluster.expectPathOnRunningNodes("/real-majority-write", true);
    try cluster.expectPathOnRunningNodes("/real-minority-write", false);
    const stable_leader = try cluster.waitForSingleLeader();

    var restarted: usize = 0;
    while (restarted == stable_leader) restarted += 1;
    const restarted_id = restarted + 1;
    try cluster.waitForPeerOpenCount(stable_leader, restarted_id, 1);
    const open_count_before = cluster.raw_transports[stable_leader].?.peerOpenCount(restarted_id);
    try cluster.stopNode(restarted);

    for (0..6) |index| {
        var path_buffer: [64]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buffer, "/while-follower-down-{}", .{index});
        try expectMutationCode(cluster.nodes[stable_leader].?, .{ .create = .{
            .path = path,
            .data = "durable",
            .time_ms = @intCast(10 + index),
        } }, .ok);
    }

    try cluster.startNode(restarted);
    try cluster.waitForPath(restarted, "/while-follower-down-5", true);
    try cluster.waitForPeerOpenCount(stable_leader, restarted_id, open_count_before + 1);
    try cluster.waitForPath(restarted, "/before-real-partition", true);
    try cluster.waitForPath(restarted, "/real-majority-write", true);
    try cluster.waitForPath(restarted, "/real-minority-write", false);
}
