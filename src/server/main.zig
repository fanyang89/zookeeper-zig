const std = @import("std");
const raft = @import("raftz");
const zookeeper = @import("zookeeper");

const usage =
    \\usage: zookeeper-quorum-server \\
    \\  --node-id ID \\
    \\  --cluster-id UUID \\
    \\  --client-listen IPV4:PORT \\
    \\  --raft-listen IPV4:PORT \\
    \\  --data-dir PATH \\
    \\  [--raft-advertise IPV4:PORT] \\
    \\  [--import-zookeeper-data-dir PATH] \\
    \\  [--import-zookeeper-log-dir PATH] \\
    \\  [--peer ID=IPV4:PORT ...] [--join]
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

    var config = zookeeper.server.config.parse(init.gpa, arguments[1..]) catch |err| {
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
        .{},
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
    );
    raft.log.info(
        @src(),
        "ZooKeeper quorum node {} listening for clients on {s}:{} and Raft on {s}",
        .{ config.node_id, config.client_host, config.client_port, config.raft_listen },
    );
    try server.serve();
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
