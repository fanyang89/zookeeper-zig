const std = @import("std");
const raft = @import("raftz");
const zoo_cfg = @import("zoo_cfg.zig");

pub const ServerConfig = struct {
    allocator: std.mem.Allocator,
    node_id: u64,
    cluster_id: raft.ClusterId,
    client_host: []u8,
    client_port: u16,
    client_request_workers: usize,
    client_request_queue_capacity: usize,
    client_max_connections: usize = 0,
    client_handshake_timeout_ms: u32 = 10_000,
    raft_listen: []u8,
    raft_advertise: []u8,
    data_dir: []u8,
    import_zookeeper_data_dir: ?[]u8,
    import_zookeeper_log_dir: ?[]u8,
    peers: []raft.Peer,
    join: bool,
    four_letter_whitelist: []u8,
    reconfig_enabled: bool,
    admin_listen: ?Endpoint,
    admin_max_connections: usize = 64,

    pub fn deinit(self: *ServerConfig) void {
        for (self.peers) |peer| self.allocator.free(peer.context.?);
        self.allocator.free(self.peers);
        self.allocator.free(self.four_letter_whitelist);
        if (self.admin_listen) |endpoint| self.allocator.free(endpoint.host);
        if (self.import_zookeeper_log_dir) |value| self.allocator.free(value);
        if (self.import_zookeeper_data_dir) |value| self.allocator.free(value);
        self.allocator.free(self.data_dir);
        self.allocator.free(self.raft_advertise);
        self.allocator.free(self.raft_listen);
        self.allocator.free(self.client_host);
        self.* = undefined;
    }
};

pub fn parseWithIo(
    allocator: std.mem.Allocator,
    io: std.Io,
    arguments: []const []const u8,
) !ServerConfig {
    var expanded = try zoo_cfg.expand(allocator, io, arguments);
    if (expanded) |*owned| {
        defer owned.deinit();
        return parse(allocator, owned.arguments.items);
    }
    return parse(allocator, arguments);
}

pub fn parse(allocator: std.mem.Allocator, arguments: []const []const u8) !ServerConfig {
    var node_id: ?u64 = null;
    var cluster_id: ?raft.ClusterId = null;
    var client_listen: ?[]const u8 = null;
    var client_max_connections: usize = 0;
    var client_handshake_timeout_ms: u32 = 10_000;
    var client_request_workers: usize = 0;
    var client_request_queue_capacity: usize = 256;
    var raft_listen: ?[]const u8 = null;
    var raft_advertise: ?[]const u8 = null;
    var data_dir: ?[]const u8 = null;
    var import_zookeeper_data_dir: ?[]const u8 = null;
    var import_zookeeper_log_dir: ?[]const u8 = null;
    var join = false;
    var reconfig_enabled = false;
    var admin_listen: ?[]const u8 = null;
    var admin_max_connections: usize = 64;
    var four_letter_whitelist: ?[]const u8 = null;
    var peer_values: std.ArrayList([]const u8) = .empty;
    defer peer_values.deinit(allocator);

    var index: usize = 0;
    while (index < arguments.len) : (index += 1) {
        const argument = arguments[index];
        if (std.mem.eql(u8, argument, "--join")) {
            join = true;
            continue;
        }
        if (std.mem.eql(u8, argument, "--reconfig-enabled")) {
            reconfig_enabled = true;
            continue;
        }
        if (index + 1 == arguments.len) return error.MissingOptionValue;
        const value = arguments[index + 1];
        index += 1;
        if (std.mem.eql(u8, argument, "--node-id")) {
            node_id = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, argument, "--cluster-id")) {
            cluster_id = try parseClusterId(value);
        } else if (std.mem.eql(u8, argument, "--client-listen")) {
            client_listen = value;
        } else if (std.mem.eql(u8, argument, "--client-handshake-timeout-ms")) {
            client_handshake_timeout_ms = try std.fmt.parseUnsigned(u32, value, 10);
        } else if (std.mem.eql(u8, argument, "--client-max-connections")) {
            client_max_connections = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, argument, "--client-request-workers")) {
            client_request_workers = try std.fmt.parseUnsigned(usize, value, 10);
            if (client_request_workers == 0) return error.InvalidClientRequestWorkers;
        } else if (std.mem.eql(u8, argument, "--client-request-queue-capacity")) {
            client_request_queue_capacity = try std.fmt.parseUnsigned(usize, value, 10);
            if (client_request_queue_capacity == 0) return error.InvalidClientRequestQueueCapacity;
        } else if (std.mem.eql(u8, argument, "--raft-listen")) {
            raft_listen = value;
        } else if (std.mem.eql(u8, argument, "--raft-advertise")) {
            raft_advertise = value;
        } else if (std.mem.eql(u8, argument, "--admin-max-connections")) {
            admin_max_connections = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, argument, "--admin-listen")) {
            admin_listen = value;
        } else if (std.mem.eql(u8, argument, "--data-dir")) {
            data_dir = value;
        } else if (std.mem.eql(u8, argument, "--import-zookeeper-data-dir")) {
            import_zookeeper_data_dir = value;
        } else if (std.mem.eql(u8, argument, "--import-zookeeper-log-dir")) {
            import_zookeeper_log_dir = value;
        } else if (std.mem.eql(u8, argument, "--4lw-whitelist")) {
            four_letter_whitelist = value;
        } else if (std.mem.eql(u8, argument, "--peer")) {
            try peer_values.append(allocator, value);
        } else {
            return error.UnknownOption;
        }
    }

    const resolved_node_id = node_id orelse return error.NodeIdRequired;
    if (resolved_node_id == 0) return error.InvalidNodeId;
    const client_endpoint = try parseEndpoint(client_listen orelse return error.ClientListenRequired);
    const resolved_raft_listen = raft_listen orelse return error.RaftListenRequired;
    _ = try parseEndpoint(resolved_raft_listen);
    const resolved_advertise = raft_advertise orelse resolved_raft_listen;
    _ = try parseEndpoint(resolved_advertise);
    const resolved_data_dir = data_dir orelse return error.DataDirRequired;
    if (resolved_data_dir.len == 0) return error.DataDirRequired;

    const client_host = try allocator.dupe(u8, client_endpoint.host);
    errdefer allocator.free(client_host);
    const owned_raft_listen = try allocator.dupe(u8, resolved_raft_listen);
    errdefer allocator.free(owned_raft_listen);
    const owned_advertise = try allocator.dupe(u8, resolved_advertise);
    errdefer allocator.free(owned_advertise);
    const owned_data_dir = try allocator.dupe(u8, resolved_data_dir);
    errdefer allocator.free(owned_data_dir);
    const owned_import_data_dir = if (import_zookeeper_data_dir) |value|
        try allocator.dupe(u8, value)
    else
        null;
    errdefer if (owned_import_data_dir) |value| allocator.free(value);
    const owned_import_log_dir = if (import_zookeeper_log_dir) |value|
        try allocator.dupe(u8, value)
    else
        null;
    errdefer if (owned_import_log_dir) |value| allocator.free(value);
    if (owned_import_log_dir != null and owned_import_data_dir == null) {
        return error.ImportDataDirRequired;
    }
    if (join and owned_import_data_dir != null) return error.ImportNotAllowedForJoin;
    const peers = try allocator.alloc(raft.Peer, peer_values.items.len);
    errdefer allocator.free(peers);
    var initialized: usize = 0;
    errdefer for (peers[0..initialized]) |peer| allocator.free(peer.context.?);
    for (peer_values.items, 0..) |peer_text, peer_index| {
        const parsed = try parsePeer(peer_text);
        if (join and parsed.id == resolved_node_id) return error.JoinPeerMatchesLocalNode;
        if (join and (std.mem.eql(u8, parsed.address, resolved_raft_listen) or
            std.mem.eql(u8, parsed.address, resolved_advertise))) return error.JoinPeerMatchesLocalAddress;
        for (peers[0..initialized]) |existing| {
            if (existing.id == parsed.id) return error.DuplicatePeerId;
            if (std.mem.eql(u8, existing.context.?, parsed.address)) return error.DuplicatePeerAddress;
        }
        peers[peer_index] = .{
            .id = parsed.id,
            .context = try allocator.dupe(u8, parsed.address),
        };
        initialized += 1;
    }
    if (join and peers.len == 0) return error.JoinPeerRequired;
    if (!join) {
        var found_local = false;
        for (peers) |peer| found_local = found_local or peer.id == resolved_node_id;
        if (peers.len != 0 and !found_local) return error.LocalPeerRequired;
    }
    const parsed_admin = if (admin_listen) |listen| try parseEndpoint(listen) else null;
    const owned_admin = if (parsed_admin) |endpoint| Endpoint{
        .host = try allocator.dupe(u8, endpoint.host),
        .port = endpoint.port,
    } else null;
    errdefer if (owned_admin) |endpoint| allocator.free(endpoint.host);

    return .{
        .allocator = allocator,
        .node_id = resolved_node_id,
        .cluster_id = cluster_id orelse return error.ClusterIdRequired,
        .client_host = client_host,
        .client_port = client_endpoint.port,
        .admin_max_connections = admin_max_connections,
        .client_max_connections = client_max_connections,
        .client_handshake_timeout_ms = client_handshake_timeout_ms,
        .client_request_workers = client_request_workers,
        .client_request_queue_capacity = client_request_queue_capacity,
        .raft_listen = owned_raft_listen,
        .raft_advertise = owned_advertise,
        .data_dir = owned_data_dir,
        .import_zookeeper_data_dir = owned_import_data_dir,
        .import_zookeeper_log_dir = owned_import_log_dir,
        .peers = peers,
        .join = join,
        .reconfig_enabled = reconfig_enabled,
        .admin_listen = owned_admin,
        .four_letter_whitelist = try allocator.dupe(u8, four_letter_whitelist orelse "srvr"),
    };
}

pub const Endpoint = struct {
    host: []const u8,
    port: u16,
};

pub fn parseEndpoint(value: []const u8) !Endpoint {
    const separator = std.mem.lastIndexOfScalar(u8, value, ':') orelse return error.InvalidEndpoint;
    if (separator == 0 or separator + 1 == value.len) return error.InvalidEndpoint;
    const port = try std.fmt.parseUnsigned(u16, value[separator + 1 ..], 10);
    if (port == 0) return error.InvalidEndpoint;
    const host = value[0..separator];
    _ = std.Io.net.IpAddress.parseIp4(host, port) catch return error.InvalidEndpoint;
    return .{ .host = host, .port = port };
}

const ParsedPeer = struct {
    id: u64,
    address: []const u8,
};

fn parsePeer(value: []const u8) !ParsedPeer {
    const separator = std.mem.indexOfScalar(u8, value, '=') orelse return error.InvalidPeer;
    if (separator == 0 or separator + 1 == value.len) return error.InvalidPeer;
    const id = try std.fmt.parseUnsigned(u64, value[0..separator], 10);
    if (id == 0) return error.InvalidPeer;
    const address = value[separator + 1 ..];
    _ = try parseEndpoint(address);
    return .{ .id = id, .address = address };
}

fn parseClusterId(value: []const u8) !raft.ClusterId {
    var result: raft.ClusterId = undefined;
    var output_index: usize = 0;
    var high_nibble: ?u8 = null;
    for (value) |byte| {
        if (byte == '-') continue;
        const nibble = std.fmt.charToDigit(byte, 16) catch return error.InvalidClusterId;
        if (high_nibble) |high| {
            if (output_index == result.len) return error.InvalidClusterId;
            result[output_index] = high << 4 | nibble;
            output_index += 1;
            high_nibble = null;
        } else {
            high_nibble = nibble;
        }
    }
    if (output_index != result.len or high_nibble != null or std.mem.allEqual(u8, &result, 0)) {
        return error.InvalidClusterId;
    }
    return result;
}

test "parse a durable three-node quorum configuration" {
    const arguments = [_][]const u8{
        "--node-id",                       "2",
        "--cluster-id",                    "0198f54d-5c2a-7000-8000-000000000001",
        "--client-listen",                 "127.0.0.1:2182",
        "--client-max-connections",        "100",
        "--client-request-workers",        "3",
        "--client-request-queue-capacity", "32",
        "--raft-listen",                   "127.0.0.1:2882",
        "--data-dir",                      "/tmp/zookeeper-zig-2",
        "--import-zookeeper-data-dir",     "/srv/zookeeper/data",
        "--import-zookeeper-log-dir",      "/srv/zookeeper/log",
        "--peer",                          "1=127.0.0.1:2881",
        "--peer",                          "2=127.0.0.1:2882",
        "--peer",                          "3=127.0.0.1:2883",
    };
    var parsed = try parse(std.testing.allocator, &arguments);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 2), parsed.node_id);
    try std.testing.expectEqual(@as(usize, 3), parsed.peers.len);
    try std.testing.expectEqualStrings("127.0.0.1", parsed.client_host);
    try std.testing.expectEqual(@as(u16, 2182), parsed.client_port);
    try std.testing.expectEqual(@as(usize, 64), parsed.admin_max_connections);
    try std.testing.expectEqual(@as(u32, 10_000), parsed.client_handshake_timeout_ms);
    try std.testing.expectEqual(@as(usize, 100), parsed.client_max_connections);
    try std.testing.expectEqual(@as(usize, 3), parsed.client_request_workers);
    try std.testing.expectEqual(@as(usize, 32), parsed.client_request_queue_capacity);
    try std.testing.expectEqualStrings("/srv/zookeeper/data", parsed.import_zookeeper_data_dir.?);
    try std.testing.expectEqualStrings("/srv/zookeeper/log", parsed.import_zookeeper_log_dir.?);
}

test "reject zero client request limits" {
    const worker_arguments = [_][]const u8{
        "--client-request-workers",
        "0",
    };
    try std.testing.expectError(
        error.InvalidClientRequestWorkers,
        parse(std.testing.allocator, &worker_arguments),
    );

    const queue_arguments = [_][]const u8{
        "--client-request-queue-capacity",
        "0",
    };
    try std.testing.expectError(
        error.InvalidClientRequestQueueCapacity,
        parse(std.testing.allocator, &queue_arguments),
    );
}

test "load a standalone zoo.cfg without myid" {
    const testing = std.testing;
    var temporary = testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const data_dir = try temporary.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(data_dir);
    const contents = try std.fmt.allocPrint(testing.allocator,
        \\tickTime=2000
        \\dataDir={s}
        \\clientPort=2181
        \\zookeeper-zig.adminMaxConnections=5
        \\zookeeper-zig.clientHandshakeTimeoutMs=700
        \\zookeeper-zig.clientMaxConnections=12
        \\
    , .{data_dir});
    defer testing.allocator.free(contents);
    try temporary.dir.writeFile(testing.io, .{ .sub_path = "zoo.cfg", .data = contents });
    const config_path = try temporary.dir.realPathFileAlloc(testing.io, "zoo.cfg", testing.allocator);
    defer testing.allocator.free(config_path);

    const arguments = [_][]const u8{ "--config", config_path };
    var parsed = try parseWithIo(testing.allocator, testing.io, &arguments);
    defer parsed.deinit();
    try testing.expectEqual(@as(u64, 1), parsed.node_id);
    try testing.expectEqualStrings("0.0.0.0", parsed.client_host);
    try testing.expectEqual(@as(u16, 2181), parsed.client_port);
    try testing.expectEqualStrings("127.0.0.1:2888", parsed.raft_listen);
    try testing.expectEqualStrings("127.0.0.1:2888", parsed.raft_advertise);
    try testing.expectEqual(@as(usize, 0), parsed.peers.len);
    try testing.expect(!std.mem.allEqual(u8, &parsed.cluster_id, 0));
    try testing.expectEqual(@as(usize, 5), parsed.admin_max_connections);
    try testing.expectEqual(@as(u32, 700), parsed.client_handshake_timeout_ms);
    try testing.expectEqual(@as(usize, 12), parsed.client_max_connections);
    var unlimited = try parseWithIo(testing.allocator, testing.io, &.{
        "--config",                      config_path, "--client-max-connections", "0",
        "--client-handshake-timeout-ms", "0",
    });
    defer unlimited.deinit();
    var admin_unlimited = try parseWithIo(testing.allocator, testing.io, &.{
        "--config", config_path, "--admin-max-connections", "0",
    });
    defer admin_unlimited.deinit();
    try testing.expectEqual(@as(usize, 0), admin_unlimited.admin_max_connections);
    try testing.expectError(error.InvalidCharacter, parse(testing.allocator, &.{
        "--admin-max-connections", "-1",
    }));
    try testing.expectEqual(@as(u32, 0), unlimited.client_handshake_timeout_ms);
    try testing.expectEqual(@as(usize, 0), unlimited.client_max_connections);
    try testing.expectError(error.InvalidCharacter, parse(testing.allocator, &.{
        "--client-max-connections", "-1",
    }));

    const mutable_arguments = [_][]const u8{
        "--config",           config_path,
        "--reconfig-enabled",
    };
    try testing.expectError(
        error.ZooCfgClusterIdRequiredForMutableMembership,
        parseWithIo(testing.allocator, testing.io, &mutable_arguments),
    );
}

test "load a standard three-node zoo.cfg" {
    const testing = std.testing;
    var temporary = testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const data_dir = try temporary.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(data_dir);
    try temporary.dir.writeFile(testing.io, .{ .sub_path = "myid", .data = "2\n" });
    const contents = try std.fmt.allocPrint(testing.allocator,
        \\tickTime=2000
        \\dataDir={s}
        \\clientPortAddress=127.0.0.1
        \\clientPort=2182
        \\initLimit=10
        \\syncLimit=5
        \\server.1=127.0.0.1:2881:3881
        \\server.2=127.0.0.1:2882:3882:participant
        \\server.3=127.0.0.1:2883:3883
        \\4lw.commands.whitelist=ruok,srvr,mntr
        \\reconfigEnabled=true
        \\zookeeper-zig.clusterId=0198f54d-5c2a-7000-8000-000000000002
        \\admin.enableServer=true
        \\admin.serverAddress=127.0.0.1
        \\admin.serverPort=9080
        \\
    , .{data_dir});
    defer testing.allocator.free(contents);
    try temporary.dir.writeFile(testing.io, .{ .sub_path = "zoo.cfg", .data = contents });
    const config_path = try temporary.dir.realPathFileAlloc(testing.io, "zoo.cfg", testing.allocator);
    defer testing.allocator.free(config_path);

    const arguments = [_][]const u8{ "--config", config_path };
    var parsed = try parseWithIo(testing.allocator, testing.io, &arguments);
    defer parsed.deinit();
    try testing.expectEqual(@as(u64, 2), parsed.node_id);
    try testing.expectEqualStrings(data_dir, parsed.data_dir);
    try testing.expectEqualStrings("127.0.0.1", parsed.client_host);
    try testing.expectEqual(@as(u16, 2182), parsed.client_port);
    try testing.expectEqualStrings("127.0.0.1:2882", parsed.raft_listen);
    try testing.expectEqualStrings("127.0.0.1:2882", parsed.raft_advertise);
    try testing.expectEqual(@as(usize, 3), parsed.peers.len);
    try testing.expectEqualStrings("ruok,srvr,mntr", parsed.four_letter_whitelist);
    try testing.expect(parsed.reconfig_enabled);
    try testing.expectEqualStrings("127.0.0.1", parsed.admin_listen.?.host);
    try testing.expectEqual(@as(u16, 9080), parsed.admin_listen.?.port);

    const join_arguments = [_][]const u8{ "--config", config_path, "--join" };
    var joining = try parseWithIo(testing.allocator, testing.io, &join_arguments);
    defer joining.deinit();
    try testing.expect(joining.join);
    try testing.expectEqual(@as(usize, 2), joining.peers.len);
    for (joining.peers) |peer_value| try testing.expect(peer_value.id != 2);

    try temporary.dir.writeFile(testing.io, .{ .sub_path = "myid", .data = "1\n" });
    var peer = try parseWithIo(testing.allocator, testing.io, &arguments);
    defer peer.deinit();
    try testing.expectEqual(@as(u64, 1), peer.node_id);
    try testing.expectEqualSlices(u8, &parsed.cluster_id, &peer.cluster_id);
    try testing.expectEqualStrings("127.0.0.1:2881", peer.raft_listen);
}

test "load dynamic zoo.cfg servers and apply CLI overrides" {
    const testing = std.testing;
    var temporary = testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    const data_dir = try temporary.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(data_dir);
    try temporary.dir.writeFile(testing.io, .{ .sub_path = "myid", .data = "2" });
    const static_contents = try std.fmt.allocPrint(testing.allocator,
        \\dataDir={s}
        \\dynamicConfigFile=dynamic.cfg
        \\reconfigEnabled=true
        \\server.3=127.0.0.1:2883:3883
        \\admin.enableServer=false
        \\admin.serverPort=9080
        \\
    , .{data_dir});
    defer testing.allocator.free(static_contents);
    try temporary.dir.writeFile(testing.io, .{ .sub_path = "zoo.cfg", .data = static_contents });
    try temporary.dir.writeFile(testing.io, .{
        .sub_path = "dynamic.cfg",
        .data =
        \\server.1=127.0.0.1:2881:3881;127.0.0.1:2181
        \\server.2=127.0.0.1:2882:3882;127.0.0.1:2182
        \\version=100000000
        \\
        ,
    });
    const config_path = try temporary.dir.realPathFileAlloc(testing.io, "zoo.cfg", testing.allocator);
    defer testing.allocator.free(config_path);
    const file_arguments = [_][]const u8{
        "--config",     config_path,
        "--cluster-id", "0198f54d-5c2a-7000-8000-000000000098",
    };
    var from_file = try parseWithIo(testing.allocator, testing.io, &file_arguments);
    defer from_file.deinit();
    try testing.expectEqual(@as(usize, 2), from_file.peers.len);
    for (from_file.peers) |peer_value| try testing.expect(peer_value.id != 3);
    try testing.expectEqual(@as(u16, 2182), from_file.client_port);

    const arguments = [_][]const u8{
        "--config",        config_path,
        "--cluster-id",    "0198f54d-5c2a-7000-8000-000000000099",
        "--client-listen", "127.0.0.1:32182",
        "--raft-listen",   "127.0.0.1:42882",
        "--peer",          "2=127.0.0.1:42882",
    };
    var parsed = try parseWithIo(testing.allocator, testing.io, &arguments);
    defer parsed.deinit();
    try testing.expectEqual(@as(u64, 2), parsed.node_id);
    try testing.expectEqual(@as(u16, 32182), parsed.client_port);
    try testing.expectEqualStrings("127.0.0.1:42882", parsed.raft_listen);
    try testing.expectEqualStrings("127.0.0.1:42882", parsed.raft_advertise);
    try testing.expectEqual(@as(usize, 1), parsed.peers.len);
    try testing.expectEqualStrings("127.0.0.1:42882", parsed.peers[0].context.?);
    try testing.expect(parsed.reconfig_enabled);
    try testing.expect(parsed.admin_listen == null);

    const unsafe_peer_override = [_][]const u8{
        "--config", config_path,
        "--peer",   "2=127.0.0.1:42882",
    };
    try testing.expectError(
        error.ZooCfgClusterIdRequiredForMutableMembership,
        parseWithIo(testing.allocator, testing.io, &unsafe_peer_override),
    );
}

test "reject zoo.cfg settings that cannot be safely downgraded" {
    const testing = std.testing;
    var temporary = testing.tmpDir(.{ .iterate = true });
    defer temporary.cleanup();
    try temporary.dir.writeFile(testing.io, .{
        .sub_path = "zoo.cfg",
        .data = "dataDir=/tmp/zk\nclientPort=2181\nsecureClientPort=2281\n",
    });
    const config_path = try temporary.dir.realPathFileAlloc(testing.io, "zoo.cfg", testing.allocator);
    defer testing.allocator.free(config_path);
    const arguments = [_][]const u8{ "--config", config_path };
    try testing.expectError(
        error.UnsupportedZooCfgSecurity,
        parseWithIo(testing.allocator, testing.io, &arguments),
    );

    try temporary.dir.writeFile(testing.io, .{
        .sub_path = "zoo.cfg",
        .data = "dataDir=/tmp/zk\nclientPort=2181\nserver.1=127.0.0.1:2881:3881:observer\n",
    });
    try testing.expectError(
        error.UnsupportedZooCfgObserver,
        parseWithIo(testing.allocator, testing.io, &arguments),
    );

    try temporary.dir.writeFile(testing.io, .{
        .sub_path = "zoo.cfg",
        .data = "dataDir=/tmp/zk\nclientPort=2181\nquorum.auth.serverRequireSasl=true\n",
    });
    try testing.expectError(
        error.UnsupportedZooCfgSecurity,
        parseWithIo(testing.allocator, testing.io, &arguments),
    );

    try temporary.dir.writeFile(testing.io, .{
        .sub_path = "zoo.cfg",
        .data = "dataDir=/tmp/zk\nclientPort=2181\nstandaloneEnabled=false\n",
    });
    try testing.expectError(
        error.ZooCfgEnsembleRequired,
        parseWithIo(testing.allocator, testing.io, &arguments),
    );
}
