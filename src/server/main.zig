const std = @import("std");
const raft = @import("raftz");
const zookeeper = @import("zookeeper");

const usage =
    \\usage: zookeeper-quorum-server \\
    \\  [--config ZOO.CFG] \\
    \\  --node-id ID \\
    \\  --cluster-id UUID \\
    \\  --client-listen IPV4:PORT \\
    \\  --raft-listen IPV4:PORT \\
    \\  --data-dir PATH \\
    \\  [--raft-advertise IPV4:PORT] \\
    \\  [--client-handshake-timeout-ms MS] \\
    \\  [--client-max-connections COUNT] \\
    \\  [--client-request-workers COUNT] \\
    \\  [--client-request-queue-capacity COUNT] \\
    \\  [--import-zookeeper-data-dir PATH] \\
    \\  [--import-zookeeper-log-dir PATH] \\
    \\  [--peer ID=IPV4:PORT ...] [--join] [--4lw-whitelist LIST] [--reconfig-enabled]
    \\                                              [--admin-listen HOST:PORT] [--admin-max-connections COUNT]
    \\
;

pub fn main(init: std.process.Init) !void {
    try raft.log.initGlobal(init.gpa, init.io, false);
    defer raft.log.deinitGlobal(init.gpa);
    const arguments = try init.minimal.args.toSlice(init.arena.allocator());
    if (arguments.len == 1 or std.mem.eql(u8, arguments[1], "--help")) {
        try writeStdout(init.io, usage);
        return;
    }

    var config = zookeeper.server.config.parseWithIo(init.gpa, init.io, arguments[1..]) catch |err| {
        var buffer: [256]u8 = undefined;
        const message = try std.fmt.bufPrint(&buffer, "invalid server configuration: {s}\n", .{@errorName(err)});
        try writeStderr(init.io, message);
        try writeStderr(init.io, usage);
        return err;
    };
    defer config.deinit();

    if (config.import_zookeeper_data_dir) |source_data_dir| {
        try zookeeper.server.zookeeper_import.importOnFirstStart(init.gpa, init.io, .{
            .source_data_dir = source_data_dir,
            .source_log_dir = config.import_zookeeper_log_dir,
            .target_data_dir = config.data_dir,
        });
    }

    const quorum = try zookeeper.server.Quorum.create(
        std.heap.smp_allocator,
        init.io,
        &config,
        .{
            // raftz's pre-vote path needs an explicit bootstrap campaign
            // when there is no remote voter to stimulate an election.
            .campaign_on_start = !config.join and config.peers.len <= 1,
        },
    );
    defer {
        if (quorum.running) quorum.shutdown() catch {};
        quorum.deinit();
    }
    var server = try zookeeper.server.TcpServer.init(
        std.heap.smp_allocator,
        init.io,
        quorum,
        config.client_host,
        config.client_port,
        .{
            .max_client_connections = config.client_max_connections,
            .handshake_timeout_ms = config.client_handshake_timeout_ms,
            .request_worker_count = config.client_request_workers,
            .request_queue_capacity = config.client_request_queue_capacity,
            .four_letter_whitelist = config.four_letter_whitelist,
            .reconfig_enabled = config.reconfig_enabled,
            .data_dir = config.data_dir,
        },
    );
    defer server.deinit();
    if (config.admin_listen) |endpoint| {
        var admin = try zookeeper.server.admin_server.AdminServer.init(
            std.heap.smp_allocator,
            init.io,
            .{ .ctx = &server, .render = renderAdminCommand },
            endpoint.host,
            endpoint.port,
        );
        admin.max_connections = config.admin_max_connections;
        defer admin.connections.cancel(init.io);
        var tasks: std.Io.Group = .init;
        defer tasks.cancel(init.io);
        const AdminServe = struct {
            fn task(admin_server: *zookeeper.server.admin_server.AdminServer) std.Io.Cancelable!void {
                return admin_server.serve() catch |err| {
                    if (err == error.Canceled) return error.Canceled;
                };
            }
        };
        try tasks.concurrent(init.io, AdminServe.task, .{&admin});
        raft.log.info(
            @src(),
            "ZooKeeper quorum node {} listening for clients on {s}:{} and Raft on {s}, admin on {s}:{}",
            .{ config.node_id, config.client_host, config.client_port, config.raft_listen, endpoint.host, endpoint.port },
        );
        try server.serve();
        return;
    }
    raft.log.info(
        @src(),
        "ZooKeeper quorum node {} listening for clients on {s}:{} and Raft on {s}",
        .{ config.node_id, config.client_host, config.client_port, config.raft_listen },
    );
    try server.serve();
}

fn renderAdminCommand(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    command: zookeeper.server.four_letter.Command,
) anyerror![]u8 {
    const server: *zookeeper.server.TcpServer = @ptrCast(@alignCast(ctx));
    return server.renderFourLetter(allocator, command);
}

fn writeStdout(io: std.Io, bytes: []const u8) !void {
    var buffer: [1024]u8 = undefined;
    var writer = std.Io.File.stdout().writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

fn writeStderr(io: std.Io, bytes: []const u8) !void {
    var buffer: [1024]u8 = undefined;
    var writer = std.Io.File.stderr().writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}
