const std = @import("std");

const max_config_bytes: usize = 1024 * 1024;
const max_myid_bytes: usize = 128;

pub const ExpandedArguments = struct {
    allocator: std.mem.Allocator,
    arguments: std.ArrayList([]const u8) = .empty,
    owned: std.ArrayList([]u8) = .empty,

    pub fn deinit(self: *ExpandedArguments) void {
        for (self.owned.items) |value| self.allocator.free(value);
        self.owned.deinit(self.allocator);
        self.arguments.deinit(self.allocator);
        self.* = undefined;
    }

    fn append(self: *ExpandedArguments, value: []const u8) !void {
        try self.arguments.append(self.allocator, value);
    }

    fn appendPair(self: *ExpandedArguments, option: []const u8, value: []const u8) !void {
        const copy = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(copy);
        try self.arguments.ensureUnusedCapacity(self.allocator, 2);
        try self.owned.append(self.allocator, copy);
        self.arguments.appendAssumeCapacity(option);
        self.arguments.appendAssumeCapacity(copy);
    }

    fn appendOwnedPair(self: *ExpandedArguments, option: []const u8, value: []u8) !void {
        errdefer self.allocator.free(value);
        try self.arguments.ensureUnusedCapacity(self.allocator, 2);
        try self.owned.append(self.allocator, value);
        self.arguments.appendAssumeCapacity(option);
        self.arguments.appendAssumeCapacity(value);
    }
};

const Properties = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,
    values: std.StringHashMap([]const u8),

    fn load(
        allocator: std.mem.Allocator,
        io: std.Io,
        path: []const u8,
    ) !Properties {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(
            io,
            path,
            allocator,
            .limited(max_config_bytes),
        );
        errdefer allocator.free(bytes);
        var values = std.StringHashMap([]const u8).init(allocator);
        errdefer values.deinit();

        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |raw_line| {
            const line = std.mem.trim(u8, raw_line, " \t\r");
            if (line.len == 0 or line[0] == '#') continue;
            if (line[line.len - 1] == '\\') return error.UnsupportedZooCfgContinuation;
            const separator = std.mem.indexOfAny(u8, line, "=:") orelse
                return error.InvalidZooCfgLine;
            const key = std.mem.trim(u8, line[0..separator], " \t");
            const value = std.mem.trim(u8, line[separator + 1 ..], " \t");
            if (key.len == 0) return error.InvalidZooCfgLine;
            try rejectUnsupportedSecurity(key, value);
            try values.put(key, value);
        }
        return .{ .allocator = allocator, .bytes = bytes, .values = values };
    }

    fn deinit(self: *Properties) void {
        self.values.deinit();
        self.allocator.free(self.bytes);
        self.* = undefined;
    }

    fn get(self: *const Properties, key: []const u8) ?[]const u8 {
        return self.values.get(key);
    }
};

const ParsedServer = struct {
    id: u64,
    host: []const u8,
    quorum_port: u16,
    client: ?ClientEndpoint,
};

const ClientEndpoint = struct {
    host: ?[]const u8,
    port: u16,
};

const Overrides = struct {
    node_id: ?u64 = null,
    join: bool = false,
    reconfig_enabled: bool = false,
    data_dir: ?[]const u8 = null,
    client_listen: bool = false,
    raft_listen: bool = false,
    raft_advertise: bool = false,
    cluster_id: bool = false,
    peers: bool = false,
};

pub fn expand(
    allocator: std.mem.Allocator,
    io: std.Io,
    arguments: []const []const u8,
) !?ExpandedArguments {
    var cli_arguments: std.ArrayList([]const u8) = .empty;
    defer cli_arguments.deinit(allocator);
    var config_path: ?[]const u8 = null;

    var index: usize = 0;
    while (index < arguments.len) {
        const argument = arguments[index];
        if (std.mem.eql(u8, argument, "--config")) {
            if (config_path != null) return error.DuplicateConfigFile;
            if (index + 1 == arguments.len) return error.MissingOptionValue;
            config_path = arguments[index + 1];
            index += 2;
            continue;
        }
        try cli_arguments.append(allocator, argument);
        index += 1;
        if (std.mem.eql(u8, argument, "--join") or
            std.mem.eql(u8, argument, "--reconfig-enabled")) continue;
        if (index == arguments.len) return error.MissingOptionValue;
        try cli_arguments.append(allocator, arguments[index]);
        index += 1;
    }

    const path = config_path orelse return null;
    const overrides = try findOverrides(cli_arguments.items);
    var result = ExpandedArguments{ .allocator = allocator };
    errdefer result.deinit();
    try appendZooCfg(&result, io, path, overrides);
    for (cli_arguments.items) |argument| try result.append(argument);
    return result;
}

fn findOverrides(arguments: []const []const u8) !Overrides {
    var result = Overrides{};
    var index: usize = 0;
    while (index < arguments.len) {
        const option = arguments[index];
        if (std.mem.eql(u8, option, "--join")) {
            result.join = true;
            index += 1;
            continue;
        }
        if (std.mem.eql(u8, option, "--reconfig-enabled")) {
            result.reconfig_enabled = true;
            index += 1;
            continue;
        }
        if (index + 1 == arguments.len) return error.MissingOptionValue;
        const value = arguments[index + 1];
        if (std.mem.eql(u8, option, "--node-id")) {
            result.node_id = try std.fmt.parseUnsigned(u64, value, 10);
        } else if (std.mem.eql(u8, option, "--data-dir")) {
            result.data_dir = value;
        } else if (std.mem.eql(u8, option, "--client-listen")) {
            result.client_listen = true;
        } else if (std.mem.eql(u8, option, "--raft-listen")) {
            result.raft_listen = true;
        } else if (std.mem.eql(u8, option, "--raft-advertise")) {
            result.raft_advertise = true;
        } else if (std.mem.eql(u8, option, "--cluster-id")) {
            result.cluster_id = true;
        } else if (std.mem.eql(u8, option, "--peer")) {
            result.peers = true;
        }
        index += 2;
    }
    return result;
}

fn appendZooCfg(
    result: *ExpandedArguments,
    io: std.Io,
    path: []const u8,
    overrides: Overrides,
) !void {
    var static = try Properties.load(result.allocator, io, path);
    defer static.deinit();

    var dynamic: ?Properties = null;
    defer if (dynamic) |*properties| properties.deinit();
    var dynamic_path: ?[]u8 = null;
    defer if (dynamic_path) |value| result.allocator.free(value);
    if (static.get("dynamicConfigFile")) |configured_path| {
        dynamic_path = if (std.fs.path.isAbsolute(configured_path))
            try result.allocator.dupe(u8, configured_path)
        else
            try std.fs.path.join(result.allocator, &.{
                std.fs.path.dirname(path) orelse ".",
                configured_path,
            });
        dynamic = try Properties.load(result.allocator, io, dynamic_path.?);
    }

    var servers = std.AutoHashMap(u64, ParsedServer).init(result.allocator);
    defer servers.deinit();
    if (dynamic) |*properties| {
        try collectServers(&servers, properties);
    } else {
        try collectServers(&servers, &static);
    }

    var server_ids: std.ArrayList(u64) = .empty;
    defer server_ids.deinit(result.allocator);
    var id_iterator = servers.keyIterator();
    while (id_iterator.next()) |id| try server_ids.append(result.allocator, id.*);
    std.mem.sort(u64, server_ids.items, {}, std.sort.asc(u64));

    const data_dir = static.get("dataDir") orelse overrides.data_dir orelse
        return error.ZooCfgDataDirRequired;
    if (static.get("dataDir")) |value| try result.appendPair("--data-dir", value);

    const node_id = overrides.node_id orelse if (static.get("zookeeper-zig.nodeId")) |value|
        try parsePositive(u64, value, error.InvalidZooCfgNodeId)
    else if (servers.count() == 0 and !overrides.peers)
        1
    else
        try readMyId(result.allocator, io, data_dir);
    if (overrides.node_id == null) {
        try result.appendOwnedPair(
            "--node-id",
            try std.fmt.allocPrint(result.allocator, "{}", .{node_id}),
        );
    }

    const local_server = servers.get(node_id);
    const config_join = if (static.get("zookeeper-zig.join")) |value|
        try parseBoolean(value)
    else
        false;
    const join = overrides.join or config_join;
    const config_reconfig_enabled = if (static.get("reconfigEnabled")) |value|
        try parseBoolean(value)
    else
        false;
    const reconfig_enabled = overrides.reconfig_enabled or config_reconfig_enabled;
    const standalone_enabled = if (static.get("standaloneEnabled")) |value|
        try parseBoolean(value)
    else
        true;
    if (!standalone_enabled and servers.count() == 0 and !overrides.peers) {
        return error.ZooCfgEnsembleRequired;
    }
    if (!overrides.client_listen) {
        const endpoint = try resolveClientEndpoint(&static, local_server);
        try result.appendOwnedPair(
            "--client-listen",
            try std.fmt.allocPrint(result.allocator, "{s}:{}", .{ endpoint.host, endpoint.port }),
        );
    }

    if (!overrides.raft_listen) {
        if (static.get("zookeeper-zig.raftListen")) |value| {
            try result.appendPair("--raft-listen", value);
        } else if (local_server) |server| {
            try result.appendOwnedPair("--raft-listen", try serverAddress(result.allocator, server));
        } else if (servers.count() == 0) {
            try result.appendPair("--raft-listen", "127.0.0.1:2888");
        } else {
            return error.LocalZooCfgServerRequired;
        }
    }
    if (!overrides.raft_advertise) {
        if (static.get("zookeeper-zig.raftAdvertise")) |value| {
            try result.appendPair("--raft-advertise", value);
        } else if (!overrides.raft_listen) {
            if (local_server) |server| {
                try result.appendOwnedPair("--raft-advertise", try serverAddress(result.allocator, server));
            }
        }
    }

    if (!overrides.cluster_id) {
        if (static.get("zookeeper-zig.clusterId")) |value| {
            try result.appendPair("--cluster-id", value);
        } else {
            if (overrides.peers or join or reconfig_enabled or dynamic != null) {
                return error.ZooCfgClusterIdRequiredForMutableMembership;
            }
            try result.appendOwnedPair(
                "--cluster-id",
                try deriveClusterId(result.allocator, data_dir, server_ids.items, &servers),
            );
        }
    }

    if (!overrides.peers) {
        for (server_ids.items) |id| {
            if (join and id == node_id) continue;
            const server = servers.get(id).?;
            const address = try serverAddress(result.allocator, server);
            defer result.allocator.free(address);
            try result.appendOwnedPair(
                "--peer",
                try std.fmt.allocPrint(result.allocator, "{}={s}", .{ id, address }),
            );
        }
    }

    if (static.get("4lw.commands.whitelist")) |value| {
        try result.appendPair("--4lw-whitelist", value);
    }
    if (config_reconfig_enabled and !overrides.reconfig_enabled) {
        try result.append("--reconfig-enabled");
    }
    if (config_join and !overrides.join) try result.append("--join");
    if (static.get("zookeeper-zig.clientHandshakeTimeoutMs")) |value| {
        try result.appendPair("--client-handshake-timeout-ms", value);
    }
    if (static.get("zookeeper-zig.clientMaxConnections")) |value| {
        try result.appendPair("--client-max-connections", value);
    }
    if (static.get("zookeeper-zig.clientRequestWorkers")) |value| {
        try result.appendPair("--client-request-workers", value);
    }
    if (static.get("zookeeper-zig.clientRequestQueueCapacity")) |value| {
        try result.appendPair("--client-request-queue-capacity", value);
    }
    try appendAdminArguments(result, &static);
}

fn collectServers(
    servers: *std.AutoHashMap(u64, ParsedServer),
    properties: *const Properties,
) !void {
    var iterator = properties.values.iterator();
    while (iterator.next()) |entry| {
        const key = entry.key_ptr.*;
        if (!std.mem.startsWith(u8, key, "server.")) continue;
        const id = try parsePositive(u64, key["server.".len..], error.InvalidZooCfgServer);
        const server = try parseServer(id, entry.value_ptr.*);
        try servers.put(id, server);
    }
}

fn parseServer(id: u64, value: []const u8) !ParsedServer {
    const semicolon = std.mem.indexOfScalar(u8, value, ';');
    const quorum = if (semicolon) |offset| value[0..offset] else value;
    const client_text = if (semicolon) |offset| value[offset + 1 ..] else null;

    var parts: [4][]const u8 = undefined;
    var count: usize = 0;
    var iterator = std.mem.splitScalar(u8, quorum, ':');
    while (iterator.next()) |part| {
        if (count == parts.len or part.len == 0) return error.InvalidZooCfgServer;
        parts[count] = part;
        count += 1;
    }
    if (count < 3 or count > 4) return error.InvalidZooCfgServer;
    const quorum_port = try parsePort(parts[1]);
    _ = try parsePort(parts[2]);
    if (count == 4) {
        if (std.mem.eql(u8, parts[3], "observer")) return error.UnsupportedZooCfgObserver;
        if (!std.mem.eql(u8, parts[3], "participant")) return error.InvalidZooCfgServerRole;
    }
    _ = std.Io.net.IpAddress.parseIp4(parts[0], quorum_port) catch
        return error.InvalidZooCfgServerAddress;

    return .{
        .id = id,
        .host = parts[0],
        .quorum_port = quorum_port,
        .client = if (client_text) |text| try parseClientEndpoint(text) else null,
    };
}

fn parseClientEndpoint(value: []const u8) !ClientEndpoint {
    if (std.mem.lastIndexOfScalar(u8, value, ':')) |separator| {
        if (separator == 0 or separator + 1 == value.len) return error.InvalidZooCfgClientEndpoint;
        return .{
            .host = value[0..separator],
            .port = try parsePort(value[separator + 1 ..]),
        };
    }
    return .{ .host = null, .port = try parsePort(value) };
}

fn resolveClientEndpoint(
    properties: *const Properties,
    local_server: ?ParsedServer,
) !struct { host: []const u8, port: u16 } {
    const configured_host = properties.get("clientPortAddress") orelse "0.0.0.0";
    if (local_server) |server| if (server.client) |client| {
        const host = client.host orelse configured_host;
        _ = std.Io.net.IpAddress.parseIp4(host, client.port) catch
            return error.InvalidZooCfgClientEndpoint;
        return .{ .host = host, .port = client.port };
    };
    const port = try parsePort(properties.get("clientPort") orelse
        return error.ZooCfgClientPortRequired);
    _ = std.Io.net.IpAddress.parseIp4(configured_host, port) catch
        return error.InvalidZooCfgClientEndpoint;
    return .{ .host = configured_host, .port = port };
}

fn appendAdminArguments(result: *ExpandedArguments, properties: *const Properties) !void {
    const enabled = if (properties.get("admin.enableServer")) |value|
        try parseBoolean(value)
    else
        properties.get("admin.serverPort") != null or properties.get("admin.serverAddress") != null;
    if (!enabled) return;
    const host = properties.get("admin.serverAddress") orelse "0.0.0.0";
    const port = try parsePort(properties.get("admin.serverPort") orelse "8080");
    try result.appendOwnedPair(
        "--admin-listen",
        try std.fmt.allocPrint(result.allocator, "{s}:{}", .{ host, port }),
    );
}

fn serverAddress(allocator: std.mem.Allocator, server: ParsedServer) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}:{}", .{ server.host, server.quorum_port });
}

fn deriveClusterId(
    allocator: std.mem.Allocator,
    data_dir: []const u8,
    server_ids: []const u64,
    servers: *const std.AutoHashMap(u64, ParsedServer),
) ![]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("zookeeper-zig-cluster-v1\n");
    if (server_ids.len == 0) {
        hasher.update("standalone\n");
        hasher.update(data_dir);
    } else {
        for (server_ids) |id| {
            const server = servers.get(id).?;
            var buffer: [128]u8 = undefined;
            const canonical = try std.fmt.bufPrint(&buffer, "{}={s}:{}\n", .{
                id,
                server.host,
                server.quorum_port,
            });
            hasher.update(canonical);
        }
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const prefix: [16]u8 = digest[0..16].*;
    const encoded = std.fmt.bytesToHex(prefix, .lower);
    return allocator.dupe(u8, &encoded);
}

fn readMyId(allocator: std.mem.Allocator, io: std.Io, data_dir: []const u8) !u64 {
    const path = try std.fs.path.join(allocator, &.{ data_dir, "myid" });
    defer allocator.free(path);
    const bytes = try std.Io.Dir.cwd().readFileAlloc(
        io,
        path,
        allocator,
        .limited(max_myid_bytes),
    );
    defer allocator.free(bytes);
    return parsePositive(
        u64,
        std.mem.trim(u8, bytes, " \t\r\n"),
        error.InvalidZooCfgNodeId,
    );
}

fn parsePort(value: []const u8) !u16 {
    return parsePositive(u16, value, error.InvalidZooCfgPort);
}

fn parsePositive(comptime T: type, value: []const u8, invalid: anyerror) !T {
    const parsed = std.fmt.parseUnsigned(T, value, 10) catch return invalid;
    if (parsed == 0) return invalid;
    return parsed;
}

fn parseBoolean(value: []const u8) !bool {
    if (std.ascii.eqlIgnoreCase(value, "true")) return true;
    if (std.ascii.eqlIgnoreCase(value, "false")) return false;
    return error.InvalidZooCfgBoolean;
}

fn rejectUnsupportedSecurity(key: []const u8, value: []const u8) !void {
    if (std.mem.eql(u8, key, "secureClientPort") or
        std.mem.startsWith(u8, key, "authProvider."))
    {
        return error.UnsupportedZooCfgSecurity;
    }
    if ((std.mem.eql(u8, key, "sslQuorum") or
        std.mem.eql(u8, key, "quorum.auth.enableSasl") or
        std.mem.eql(u8, key, "quorum.auth.serverRequireSasl") or
        std.mem.eql(u8, key, "quorum.auth.learnerRequireSasl")) and
        try parseBoolean(value))
    {
        return error.UnsupportedZooCfgSecurity;
    }
}
