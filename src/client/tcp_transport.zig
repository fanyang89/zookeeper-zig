const std = @import("std");
const wire = @import("../wire.zig");

const ConnectRace = union(enum) {
    connect: anyerror!std.Io.net.Stream,
    timer: anyerror!void,
};

pub const TcpTransport = struct {
    stream: std.Io.net.Stream,
    closed: bool = false,

    pub fn connectAddress(
        io: std.Io,
        address: std.Io.net.IpAddress,
        options: std.Io.net.IpAddress.ConnectOptions,
    ) !TcpTransport {
        if (options.timeout == .none) {
            return .{ .stream = try address.connect(io, options) };
        }

        const deadline = options.timeout.toDeadline(io);
        var bounded_options = options;
        // Zig 0.16's threaded netConnect backend does not implement its
        // timeout field. Race the cancelable operation against our deadline.
        bounded_options.timeout = .none;
        var results: [2]ConnectRace = undefined;
        var select = std.Io.Select(ConnectRace).init(io, &results);
        try select.concurrent(.connect, connectAddressTask, .{ address, io, bounded_options });
        select.concurrent(.timer, timeoutTask, .{ deadline, io }) catch |err| {
            cleanupConnectRace(select.cancel(), io);
            return err;
        };
        const result = select.await() catch |err| {
            cleanupConnectRace(select.cancel(), io);
            return err;
        };
        cleanupConnectRace(select.cancel(), io);
        return connectRaceResult(result);
    }

    pub fn connectHost(
        io: std.Io,
        host: []const u8,
        port: u16,
        options: std.Io.net.IpAddress.ConnectOptions,
    ) !TcpTransport {
        const host_name = try std.Io.net.HostName.init(host);
        if (options.timeout == .none) {
            return .{ .stream = try host_name.connect(io, port, options) };
        }

        const deadline = options.timeout.toDeadline(io);
        var bounded_options = options;
        // The outer deadline includes DNS lookup and all address attempts.
        bounded_options.timeout = .none;
        var results: [2]ConnectRace = undefined;
        var select = std.Io.Select(ConnectRace).init(io, &results);
        try select.concurrent(.connect, connectHostTask, .{ host_name, io, port, bounded_options });
        select.concurrent(.timer, timeoutTask, .{ deadline, io }) catch |err| {
            cleanupConnectRace(select.cancel(), io);
            return err;
        };
        const result = select.await() catch |err| {
            cleanupConnectRace(select.cancel(), io);
            return err;
        };
        cleanupConnectRace(select.cancel(), io);
        return connectRaceResult(result);
    }

    pub fn fromStream(stream: std.Io.net.Stream) TcpTransport {
        return .{ .stream = stream };
    }

    pub fn shutdown(self: *TcpTransport, io: std.Io) void {
        if (self.closed) return;
        self.stream.shutdown(io, .both) catch {};
    }

    pub fn close(self: *TcpTransport, io: std.Io) void {
        if (self.closed) return;
        self.stream.close(io);
        self.closed = true;
    }

    pub fn writeFrame(self: *TcpTransport, io: std.Io, frame: []const u8) !void {
        return self.writeFrameTimeout(io, frame, .none);
    }

    pub fn writeFrameTimeout(
        self: *TcpTransport,
        io: std.Io,
        frame: []const u8,
        timeout: std.Io.Timeout,
    ) !void {
        if (self.closed) return error.TransportClosed;
        if (timeout == .none) return self.writeFrameDirect(io, frame);

        const Race = union(enum) {
            write: anyerror!void,
            timer: anyerror!void,
        };
        var results: [2]Race = undefined;
        var select = std.Io.Select(Race).init(io, &results);
        try select.concurrent(.write, writeFrameTask, .{ self, io, frame });
        select.concurrent(.timer, timeoutTask, .{ timeout, io }) catch |err| {
            select.cancelDiscard();
            return err;
        };
        const result = select.await() catch |err| {
            select.cancelDiscard();
            return err;
        };
        select.cancelDiscard();
        switch (result) {
            .write => |write_result| return write_result,
            .timer => |timer_result| {
                try timer_result;
                return error.Timeout;
            },
        }
    }

    /// Reads one complete frame and returns its owned payload without the
    /// four-byte length prefix.
    pub fn readFrameAlloc(
        self: *TcpTransport,
        allocator: std.mem.Allocator,
        io: std.Io,
        max_payload: usize,
    ) ![]u8 {
        return self.readFrameAllocTimeout(allocator, io, max_payload, .none);
    }

    pub fn readFrameAllocTimeout(
        self: *TcpTransport,
        allocator: std.mem.Allocator,
        io: std.Io,
        max_payload: usize,
        timeout: std.Io.Timeout,
    ) ![]u8 {
        if (self.closed) return error.TransportClosed;
        const deadline = timeout.toDeadline(io);

        var header: [4]u8 = undefined;
        try readAllTimeout(self, io, &header, deadline, false);
        const signed_length = std.mem.readInt(i32, &header, .big);
        if (signed_length < 0) return error.InvalidFrameLength;
        const payload_length: usize = @intCast(signed_length);
        if (payload_length > max_payload) return error.FrameTooLarge;

        const payload = try allocator.alloc(u8, payload_length);
        errdefer allocator.free(payload);
        try readAllTimeout(self, io, payload, deadline, true);
        return payload;
    }

    fn writeFrameDirect(self: *TcpTransport, io: std.Io, frame: []const u8) !void {
        var buffer: [0]u8 = .{};
        var stream_writer = self.stream.writer(io, &buffer);
        stream_writer.interface.writeAll(frame) catch |err| switch (err) {
            error.WriteFailed => return stream_writer.err orelse error.Unexpected,
        };
        stream_writer.interface.flush() catch |err| switch (err) {
            error.WriteFailed => return stream_writer.err orelse error.Unexpected,
        };
    }
};

fn connectAddressTask(
    address: std.Io.net.IpAddress,
    io: std.Io,
    options: std.Io.net.IpAddress.ConnectOptions,
) anyerror!std.Io.net.Stream {
    return address.connect(io, options);
}

fn connectHostTask(
    host_name: std.Io.net.HostName,
    io: std.Io,
    port: u16,
    options: std.Io.net.IpAddress.ConnectOptions,
) anyerror!std.Io.net.Stream {
    return host_name.connect(io, port, options);
}

fn connectRaceResult(result: ConnectRace) !TcpTransport {
    return switch (result) {
        .connect => |connect_result| .{ .stream = try connect_result },
        .timer => |timer_result| {
            try timer_result;
            return error.Timeout;
        },
    };
}

fn cleanupConnectRace(result: ?ConnectRace, io: std.Io) void {
    const completed = result orelse return;
    switch (completed) {
        .connect => |connect_result| {
            const stream = connect_result catch return;
            stream.close(io);
        },
        .timer => {},
    }
}

fn writeFrameTask(transport: *TcpTransport, io: std.Io, frame: []const u8) anyerror!void {
    return transport.writeFrameDirect(io, frame);
}

fn timeoutTask(timeout: std.Io.Timeout, io: std.Io) anyerror!void {
    return timeout.sleep(io);
}

fn readAllTimeout(
    transport: *TcpTransport,
    io: std.Io,
    destination: []u8,
    timeout: std.Io.Timeout,
    frame_started: bool,
) !void {
    var offset: usize = 0;
    while (offset < destination.len) {
        const message = transport.stream.socket.receiveTimeout(
            io,
            destination[offset..],
            timeout,
        ) catch |err| {
            if (err == error.Timeout and (frame_started or offset != 0)) {
                return error.PartialFrameTimeout;
            }
            return err;
        };
        if (message.data.len == 0) return error.EndOfStream;
        offset += message.data.len;
    }
}

fn writeFragmented(stream: std.Io.net.Stream, io: std.Io, bytes: []const u8) !void {
    var buffer: [0]u8 = .{};
    var writer = stream.writer(io, &buffer);
    for (bytes) |byte| {
        try writer.interface.writeAll(&.{byte});
        try writer.interface.flush();
    }
}

fn serverFixture(server: *std.Io.net.Server, io: std.Io) !void {
    const stream = try server.accept(io);
    var transport = TcpTransport.fromStream(stream);
    defer transport.close(io);

    const payload = try transport.readFrameAlloc(std.testing.allocator, io, wire.default_max_payload);
    defer std.testing.allocator.free(payload);
    try std.testing.expectEqualStrings("request", payload);

    const response = [_]u8{ 0, 0, 0, 8 } ++ "response".*;
    try writeFragmented(stream, io, &response);
}

test "write timeout race does not run its timer synchronously" {
    const testing = std.testing;
    var io_instance: std.Io.Threaded = .init(testing.allocator, .{
        .async_limit = .nothing,
    });
    defer io_instance.deinit();
    const io = io_instance.io();

    const listen_address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try listen_address.listen(io, .{});
    defer server.deinit(io);

    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        server.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    const port = std.mem.bigToNative(u16, local_address.port);

    var server_future = try io.concurrent(serverFixture, .{ &server, io });
    defer server_future.cancel(io) catch {};

    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    var transport = try TcpTransport.connectAddress(io, address, .{
        .mode = .stream,
        .protocol = .tcp,
    });
    defer transport.close(io);

    const request = [_]u8{ 0, 0, 0, 7 } ++ "request".*;
    const started_ms = std.Io.Clock.awake.now(io).toMilliseconds();
    try transport.writeFrameTimeout(io, &request, .{ .duration = .{
        .raw = .fromSeconds(1),
        .clock = .awake,
    } });
    const elapsed_ms = std.Io.Clock.awake.now(io).toMilliseconds() - started_ms;
    try testing.expect(elapsed_ms < 500);

    const response = try transport.readFrameAllocTimeout(
        testing.allocator,
        io,
        wire.default_max_payload,
        .{ .duration = .{
            .raw = .fromSeconds(1),
            .clock = .awake,
        } },
    );
    defer testing.allocator.free(response);
    try testing.expectEqualStrings("response", response);
    try server_future.await(io);
}

test "TCP transport reads fragmented frames and writes complete frames" {
    const testing = std.testing;
    const listen_address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var server = try listen_address.listen(testing.io, .{});
    defer server.deinit(testing.io);

    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        server.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    const port = std.mem.bigToNative(u16, local_address.port);

    var server_future = try testing.io.concurrent(serverFixture, .{ &server, testing.io });
    defer server_future.cancel(testing.io) catch {};

    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    var transport = try TcpTransport.connectAddress(testing.io, address, .{
        .mode = .stream,
        .protocol = .tcp,
    });
    defer transport.close(testing.io);

    const request = [_]u8{ 0, 0, 0, 7 } ++ "request".*;
    try transport.writeFrame(testing.io, &request);
    const response = try transport.readFrameAllocTimeout(
        testing.allocator,
        testing.io,
        wire.default_max_payload,
        .{ .duration = .{
            .raw = .fromSeconds(1),
            .clock = .awake,
        } },
    );
    defer testing.allocator.free(response);
    try testing.expectEqualStrings("response", response);

    try server_future.await(testing.io);
}
