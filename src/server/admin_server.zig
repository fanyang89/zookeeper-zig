// ZooKeeper AdminServer HTTP endpoints.
//
// Mirrors the upstream `/commands` URL space: `GET /commands` lists the
// registered commands as links, `GET /commands/<name>` executes a command and
// returns upstream-shaped JSON `{"command": ..., "error": null, ...}` by
// default, or the plain four-letter text with `?format=txt`. Field names and
// values come from the rendered command output: tab-separated `mntr` keys and
// `Key: value` lines become JSON fields; decorative lines are omitted, which
// matches upstream behavior for commands whose structured responses carry
// only the standard header (for example `ruok`).
//
// The command renderer is injected so the HTTP layer is testable without a
// live quorum; `TcpServer` provides the real implementation.

const std = @import("std");
const four_letter = @import("four_letter.zig");
const TcpTransport = @import("../client/tcp_transport.zig").TcpTransport;

pub const Provider = struct {
    ctx: *anyopaque,
    render: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator, command: four_letter.Command) anyerror![]u8,
};

pub const Response = struct {
    status: u16,
    content_type: []const u8,
    body: []u8,

    pub fn deinit(self: *Response, allocator: std.mem.Allocator) void {
        allocator.free(self.body);
        self.* = undefined;
    }
};

/// Command names accepted in URLs: every four-letter word plus the upstream
/// admin primary names where they differ.
const aliases = [_]struct { name: []const u8, command: four_letter.Command }{
    .{ .name = "conf", .command = .conf },
    .{ .name = "configuration", .command = .conf },
    .{ .name = "config", .command = .conf },
    .{ .name = "cons", .command = .cons },
    .{ .name = "connections", .command = .cons },
    .{ .name = "crst", .command = .crst },
    .{ .name = "connection_stat_reset", .command = .crst },
    .{ .name = "dump", .command = .dump },
    .{ .name = "envi", .command = .envi },
    .{ .name = "env", .command = .envi },
    .{ .name = "environment", .command = .envi },
    .{ .name = "gtmk", .command = .gtmk },
    .{ .name = "get_trace_mask", .command = .gtmk },
    .{ .name = "isro", .command = .isro },
    .{ .name = "is_read_only", .command = .isro },
    .{ .name = "mntr", .command = .mntr },
    .{ .name = "monitor", .command = .mntr },
    .{ .name = "ruok", .command = .ruok },
    .{ .name = "srst", .command = .srst },
    .{ .name = "stat_reset", .command = .srst },
    .{ .name = "srvr", .command = .srvr },
    .{ .name = "server_stats", .command = .srvr },
    .{ .name = "stat", .command = .stat },
    .{ .name = "stats", .command = .stat },
    .{ .name = "stmk", .command = .stmk },
    .{ .name = "set_trace_mask", .command = .stmk },
    .{ .name = "wchc", .command = .wchc },
    .{ .name = "watch_by_session", .command = .wchc },
    .{ .name = "wchp", .command = .wchp },
    .{ .name = "watch_by_path", .command = .wchp },
    .{ .name = "wchs", .command = .wchs },
    .{ .name = "watch_summary", .command = .wchs },
};

fn commandForName(name: []const u8) ?four_letter.Command {
    for (aliases) |alias| {
        if (std.mem.eql(u8, name, alias.name)) return alias.command;
    }
    return null;
}

const Format = enum { json, txt };

const max_header_bytes = 4096;

fn headerLength(request: []const u8) !?usize {
    if (std.mem.indexOf(u8, request, "\r\n\r\n")) |offset| {
        const length = offset + 4;
        if (length > max_header_bytes) return error.HeadersTooLarge;
        return length;
    }
    if (request.len >= max_header_bytes) return error.HeadersTooLarge;
    return null;
}

/// Handles one HTTP/1.x request with complete, bounded headers. Truncated
/// input must never execute a management command.
pub fn handleRequest(
    allocator: std.mem.Allocator,
    provider: Provider,
    request: []const u8,
) !Response {
    const length = (try headerLength(request)) orelse return error.MalformedRequest;
    var lines = std.mem.splitSequence(u8, request[0..length], "\r\n");
    const request_line = lines.next() orelse return error.MalformedRequest;
    var parts = std.mem.tokenizeScalar(u8, request_line, ' ');
    const method = parts.next() orelse return error.MalformedRequest;
    const target = parts.next() orelse return error.MalformedRequest;
    if (!std.mem.eql(u8, method, "GET") and !std.mem.eql(u8, method, "HEAD")) {
        return error.MethodNotAllowed;
    }

    var path = target;
    var format = Format.json;
    if (std.mem.indexOfScalar(u8, target, '?')) |query_start| {
        path = target[0..query_start];
        var params = std.mem.splitScalar(u8, target[query_start + 1 ..], '&');
        while (params.next()) |param| {
            if (std.mem.eql(u8, param, "format=txt")) format = .txt;
        }
    }

    if (std.mem.eql(u8, path, "/commands")) {
        return .{ .status = 200, .content_type = "text/html; charset=utf-8", .body = try renderCommandList(allocator) };
    }
    if (!std.mem.startsWith(u8, path, "/commands/") or path.len == "/commands/".len) {
        return error.NotFound;
    }
    const name = path["/commands/".len..];
    const command = commandForName(name) orelse {
        const body = try std.fmt.allocPrint(
            allocator,
            "{{\n  \"command\" : \"{s}\",\n  \"error\" : \"Unknown command: {s}\"\n}}",
            .{ name, name },
        );
        return .{ .status = 404, .content_type = "application/json", .body = body };
    };

    const text = try provider.render(provider.ctx, allocator, command);
    defer allocator.free(text);
    if (format == .txt) {
        const body = try allocator.dupe(u8, text);
        return .{ .status = 200, .content_type = "text/plain; charset=utf-8", .body = body };
    }
    return .{
        .status = 200,
        .content_type = "application/json",
        .body = try renderJson(allocator, name, text),
    };
}

fn renderCommandList(allocator: std.mem.Allocator) ![]u8 {
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);
    for (aliases) |alias| {
        try output.appendSlice(allocator, "<a href=\"/commands/");
        try output.appendSlice(allocator, alias.name);
        try output.appendSlice(allocator, "\">");
        try output.appendSlice(allocator, alias.name);
        try output.appendSlice(allocator, "</a><br/>\n");
    }
    return output.toOwnedSlice(allocator);
}

/// Converts rendered command text into the upstream JSON envelope. Lines
/// shaped `key<TAB>value` (mntr) or `Key: value` become fields; other lines
/// are decorative and dropped.
fn renderJson(allocator: std.mem.Allocator, name: []const u8, text: []const u8) ![]u8 {
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);
    try output.appendSlice(allocator, "{\n  \"command\" : ");
    try appendJsonString(allocator, &output, name);
    try output.appendSlice(allocator, ",\n  \"error\" : null");

    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, "\r");
        if (line.len == 0) continue;
        const separator = std.mem.indexOfScalar(u8, line, '\t') orelse blk: {
            const colon = std.mem.indexOf(u8, line, ": ") orelse continue;
            break :blk colon;
        };
        const key = std.mem.trim(u8, line[0..separator], " ");
        const value = std.mem.trim(u8, line[separator + 1 ..], " ");
        if (key.len == 0 or value.len == 0) continue;
        try output.appendSlice(allocator, ",\n  ");
        try appendJsonString(allocator, &output, key);
        try output.appendSlice(allocator, " : ");
        try appendJsonString(allocator, &output, value);
    }
    try output.appendSlice(allocator, "\n}");
    return output.toOwnedSlice(allocator);
}

fn appendJsonString(allocator: std.mem.Allocator, output: *std.ArrayList(u8), value: []const u8) !void {
    try output.append(allocator, '"');
    for (value) |byte| {
        switch (byte) {
            '"' => try output.appendSlice(allocator, "\\\""),
            '\\' => try output.appendSlice(allocator, "\\\\"),
            '\n' => try output.appendSlice(allocator, "\\n"),
            '\r' => try output.appendSlice(allocator, "\\r"),
            '\t' => try output.appendSlice(allocator, "\\t"),
            else => {
                if (byte < 0x20) {
                    const escaped = try std.fmt.allocPrint(allocator, "\\u{x:0>4}", .{byte});
                    defer allocator.free(escaped);
                    try output.appendSlice(allocator, escaped);
                } else {
                    try output.append(allocator, byte);
                }
            },
        }
    }
    try output.append(allocator, '"');
}

pub const AdminServer = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    provider: Provider,
    address: std.Io.net.IpAddress,
    connections: std.Io.Group = .init,
    header_timeout_ms: u32 = 10_000,
    response_timeout_ms: u32 = 10_000,
    max_connections: usize = 64,
    admitted_connections: std.atomic.Value(usize) = .init(0),

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        provider: Provider,
        host: []const u8,
        port: u16,
    ) !AdminServer {
        return .{
            .allocator = allocator,
            .io = io,
            .provider = provider,
            .address = try std.Io.net.IpAddress.parseIp4(host, port),
        };
    }

    pub fn serve(self: *AdminServer) !void {
        var listener = try self.address.listen(self.io, .{ .reuse_address = true });
        defer listener.deinit(self.io);
        while (true) {
            const stream = try listener.accept(self.io);
            if (self.max_connections != 0 and self.admitted_connections.load(.acquire) >= self.max_connections) {
                stream.close(self.io);
                continue;
            }
            _ = self.admitted_connections.fetchAdd(1, .acq_rel);
            self.connections.concurrent(self.io, serveTask, .{ self, stream }) catch |err| {
                stream.close(self.io);
                _ = self.admitted_connections.fetchSub(1, .acq_rel);
                return err;
            };
        }
    }

    fn serveTask(self: *AdminServer, stream: std.Io.net.Stream) std.Io.Cancelable!void {
        defer _ = self.admitted_connections.fetchSub(1, .acq_rel);
        self.serveConnection(stream) catch |err| {
            if (err == error.Canceled) return error.Canceled;
        };
    }

    fn serveConnection(self: *AdminServer, stream: std.Io.net.Stream) !void {
        defer stream.close(self.io);
        // One absolute deadline for the entire header, never reset by a
        // partial read. Reject EOF/overflow before invoking the provider.
        const timeout: std.Io.Timeout = .{ .duration = .{
            .raw = .fromMilliseconds(self.header_timeout_ms),
            .clock = .awake,
        } };
        const deadline = timeout.toDeadline(self.io);
        var buffer: [max_header_bytes]u8 = undefined;
        var length: usize = 0;
        while (try headerLength(buffer[0..length]) == null) {
            const message = try stream.socket.receiveTimeout(self.io, buffer[length..], deadline);
            if (message.data.len == 0) return error.MalformedRequest;
            length += message.data.len;
        }
        const request = buffer[0..length];

        var response = handleRequest(self.allocator, self.provider, request) catch |err| switch (err) {
            error.MethodNotAllowed => Response{
                .status = 405,
                .content_type = "text/plain; charset=utf-8",
                .body = try self.allocator.dupe(u8, "method not allowed\n"),
            },
            error.NotFound => Response{
                .status = 404,
                .content_type = "text/plain; charset=utf-8",
                .body = try self.allocator.dupe(u8, "not found\n"),
            },
            else => Response{
                .status = 500,
                .content_type = "text/plain; charset=utf-8",
                .body = try self.allocator.dupe(u8, "internal error\n"),
            },
        };
        defer response.deinit(self.allocator);

        const head = try std.fmt.allocPrint(
            self.allocator,
            "HTTP/1.1 {d} {s}\r\nContent-Type: {s}\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
            .{ response.status, reasonPhrase(response.status), response.content_type, response.body.len },
        );
        defer self.allocator.free(head);
        var outbound: std.ArrayList(u8) = .empty;
        defer outbound.deinit(self.allocator);
        try outbound.appendSlice(self.allocator, head);
        try outbound.appendSlice(self.allocator, response.body);
        // TcpTransport writes the supplied bytes verbatim; HTTP has already
        // been framed above. Its cancelable write race also handles a peer
        // that stops reading after a partial response.
        var transport = TcpTransport.fromStream(stream);
        try transport.writeFrameTimeout(self.io, outbound.items, .{ .duration = .{
            .raw = .fromMilliseconds(self.response_timeout_ms),
            .clock = .awake,
        } });
    }
};

fn reasonPhrase(status: u16) []const u8 {
    return switch (status) {
        200 => "OK",
        404 => "Not Found",
        405 => "Method Not Allowed",
        else => "Internal Server Error",
    };
}

fn writeAll(io: std.Io, stream: std.Io.net.Stream, bytes: []const u8) !void {
    var buffer: [0]u8 = .{};
    var writer = stream.writer(io, &buffer);
    try writer.interface.writeAll(bytes);
    try writer.interface.flush();
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const StubProvider = struct {
    fn render(ctx: *anyopaque, allocator: std.mem.Allocator, command: four_letter.Command) anyerror![]u8 {
        _ = ctx;
        return switch (command) {
            .dump => blk: {
                const data = try allocator.alloc(u8, 16 * 1024 * 1024);
                @memset(data, 'x');
                break :blk data;
            },
            .ruok => try allocator.dupe(u8, "imok"),
            .mntr => try allocator.dupe(u8, "zk_version\t3.9.5\nzk_server_state\tleader\nzk_znode_count\t7\n"),
            .srvr => try allocator.dupe(u8, "Zookeeper version: 3.9.5\nReceived: 42\nMode: leader\n"),
            else => try allocator.dupe(u8, ""),
        };
    }
};

fn stubProvider() Provider {
    return .{ .ctx = undefined, .render = StubProvider.render };
}

test "command list page links every alias" {
    var response = try handleRequest(std.testing.allocator, stubProvider(), "GET /commands HTTP/1.1\r\nHost: x\r\n\r\n");
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "<a href=\"/commands/monitor\">monitor</a><br/>") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "<a href=\"/commands/ruok\">ruok</a><br/>") != null);
}

test "mntr json parses tab separated fields into the envelope" {
    var response = try handleRequest(std.testing.allocator, stubProvider(), "GET /commands/mntr HTTP/1.1\r\n\r\n");
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expectEqualStrings("application/json", response.content_type);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"command\" : \"mntr\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"zk_version\" : \"3.9.5\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"zk_znode_count\" : \"7\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"error\" : null") != null);
}

test "srvr json accepts primary names and colon separated fields" {
    var response = try handleRequest(std.testing.allocator, stubProvider(), "GET /commands/server_stats HTTP/1.1\r\n\r\n");
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"command\" : \"server_stats\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, response.body, "\"Received\" : \"42\"") != null);
}

test "format txt returns the raw command output" {
    var response = try handleRequest(std.testing.allocator, stubProvider(), "GET /commands/ruok?format=txt HTTP/1.1\r\n\r\n");
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expectEqualStrings("imok", response.body);
}

test "unknown commands and paths answer 404" {
    var unknown = try handleRequest(std.testing.allocator, stubProvider(), "GET /commands/nope HTTP/1.1\r\n\r\n");
    defer unknown.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 404), unknown.status);
    try std.testing.expect(std.mem.indexOf(u8, unknown.body, "Unknown command: nope") != null);

    try std.testing.expectError(
        error.NotFound,
        handleRequest(std.testing.allocator, stubProvider(), "GET /other HTTP/1.1\r\n\r\n"),
    );
    try std.testing.expectError(
        error.MethodNotAllowed,
        handleRequest(std.testing.allocator, stubProvider(), "POST /commands/ruok HTTP/1.1\r\n\r\n"),
    );
}

test "json escapes special characters" {
    var output: std.ArrayList(u8) = .empty;
    defer output.deinit(std.testing.allocator);
    try appendJsonString(std.testing.allocator, &output, "a\"b\\c\nd");
    try std.testing.expectEqualStrings("\"a\\\"b\\\\c\\nd\"", output.items);
}

test "admin server serves real HTTP requests over loopback" {
    const testing = std.testing;
    const port = try reserveTestPort(testing.io);
    var admin = try AdminServer.init(testing.allocator, testing.io, stubProvider(), "127.0.0.1", port);
    defer admin.connections.cancel(testing.io);
    admin.header_timeout_ms = 1_000;
    admin.response_timeout_ms = 100;
    admin.max_connections = 1;

    var tasks: std.Io.Group = .init;
    defer tasks.cancel(testing.io);
    const TestServe = struct {
        fn task(server: *AdminServer) std.Io.Cancelable!void {
            return server.serve() catch |err| {
                if (err == error.Canceled) return error.Canceled;
            };
        }
    };
    try tasks.concurrent(testing.io, TestServe.task, .{&admin});

    // Wait for the listener, then issue a request.
    var attempts: usize = 0;
    var stream: ?std.Io.net.Stream = null;
    while (attempts < 200) : (attempts += 1) {
        const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
        if (address.connect(testing.io, .{ .mode = .stream })) |connected| {
            stream = connected;
            break;
        } else |err| {
            if (err != error.ConnectionRefused) return err;
            try testing.io.sleep(.fromMilliseconds(5), .awake);
        }
    }
    const connected = stream orelse return error.AdminServerDidNotStart;
    defer connected.close(testing.io);

    try writeAll(testing.io, connected, "GET /commands/mntr HTTP/1.1\r\n");
    try writeAll(testing.io, connected, "Host: localhost\r\n\r\n");
    var buffer: [8192]u8 = undefined;
    var length: usize = 0;
    while (length < buffer.len) {
        const message = try connected.socket.receive(testing.io, buffer[length..]);
        if (message.data.len == 0) break;
        length += message.data.len;
    }
    const response = buffer[0..length];
    try testing.expect(std.mem.indexOf(u8, response, "HTTP/1.1 200 OK") != null);
    try testing.expect(std.mem.indexOf(u8, response, "Content-Type: application/json") != null);
    try testing.expect(std.mem.indexOf(u8, response, "\"zk_server_state\" : \"leader\"") != null);

    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", port);
    try waitForAdmitted(&admin, 0);
    const idle = try address.connect(testing.io, .{ .mode = .stream });
    {
        defer idle.close(testing.io);
        try waitForAdmitted(&admin, 1);
        const excess = try address.connect(testing.io, .{ .mode = .stream });
        defer excess.close(testing.io);
        var byte: [1]u8 = undefined;
        const rejected = try excess.socket.receiveTimeout(testing.io, &byte, .{ .duration = .{
            .raw = .fromSeconds(2),
            .clock = .awake,
        } });
        try testing.expectEqual(@as(usize, 0), rejected.data.len);
    }
    try waitForAdmitted(&admin, 0);
    // Keep the receiver open but do not drain the large response. The write
    // deadline must release the slot without relying on peer disconnect.
    const stalled = try address.connect(testing.io, .{ .mode = .stream });
    defer stalled.close(testing.io);
    try writeAll(testing.io, stalled, "GET /commands/dump?format=txt HTTP/1.1\r\n\r\n");
    var prefix: [12]u8 = undefined;
    var received: usize = 0;
    while (received < prefix.len) {
        const first = try stalled.socket.receiveTimeout(testing.io, prefix[received..], .{ .duration = .{
            .raw = .fromSeconds(2),
            .clock = .awake,
        } });
        try testing.expect(first.data.len > 0);
        received += first.data.len;
    }
    try testing.expect(std.mem.startsWith(u8, &prefix, "HTTP/1.1 200"));
    try waitForAdmitted(&admin, 0);
    for ([_][]const u8{ "", "GET /commands/srst HTTP/1.1\r\n", "x" ** max_header_bytes }) |input| {
        try waitForAdmitted(&admin, 0);
        const invalid = try address.connect(testing.io, .{ .mode = .stream });
        defer invalid.close(testing.io);
        if (input.len > 0) try writeAll(testing.io, invalid, input);
        if (input.len > 0 and input.len < max_header_bytes) try invalid.shutdown(testing.io, .send);
        var byte: [1]u8 = undefined;
        const closed = try invalid.socket.receiveTimeout(testing.io, &byte, .{ .duration = .{
            .raw = .fromSeconds(2),
            .clock = .awake,
        } });
        try testing.expectEqual(@as(usize, 0), closed.data.len);
    }
    try waitForAdmitted(&admin, 0);
    const recovered = try address.connect(testing.io, .{ .mode = .stream });
    defer recovered.close(testing.io);
    try writeAll(testing.io, recovered, "GET /commands/ruok?format=txt HTTP/1.1\r\n\r\n");
    length = 0;
    while (length < buffer.len) {
        const message = try recovered.socket.receiveTimeout(testing.io, buffer[length..], .{ .duration = .{
            .raw = .fromSeconds(2),
            .clock = .awake,
        } });
        if (message.data.len == 0) break;
        length += message.data.len;
    }
    try testing.expect(std.mem.startsWith(u8, buffer[0..length], "HTTP/1.1 200"));
    try testing.expect(std.mem.endsWith(u8, buffer[0..length], "\r\n\r\nimok"));
}

fn waitForAdmitted(admin: *AdminServer, expected: usize) !void {
    for (0..1_000) |_| {
        if (admin.admitted_connections.load(.acquire) == expected) return;
        try admin.io.sleep(.fromMilliseconds(5), .awake);
    }
    return error.ConnectionBudgetTimeout;
}

fn reserveTestPort(io: std.Io) !u16 {
    const address = try std.Io.net.IpAddress.parseIp4("127.0.0.1", 0);
    var listener = try address.listen(io, .{});
    defer listener.deinit(io);
    var local_address: std.posix.sockaddr.in = undefined;
    var address_length: std.posix.socklen_t = @sizeOf(std.posix.sockaddr.in);
    if (std.posix.errno(std.posix.system.getsockname(
        listener.socket.handle,
        @ptrCast(&local_address),
        &address_length,
    )) != .SUCCESS) return error.AddressQueryFailed;
    return std.mem.bigToNative(u16, local_address.port);
}

test "admin rejects incomplete and oversized headers before invoking commands" {
    const CountingProvider = struct {
        fn render(ctx: *anyopaque, allocator: std.mem.Allocator, command: four_letter.Command) ![]u8 {
            _ = command;
            const calls: *usize = @ptrCast(@alignCast(ctx));
            calls.* += 1;
            return allocator.dupe(u8, "imok");
        }
    };
    var calls: usize = 0;
    const provider = Provider{ .ctx = &calls, .render = CountingProvider.render };
    try std.testing.expectError(error.MalformedRequest, handleRequest(std.testing.allocator, provider, "GET /commands/srst HTTP/1.1\r\nHost: localhost\r\n"));
    try std.testing.expectError(error.HeadersTooLarge, handleRequest(std.testing.allocator, provider, "GET /commands/srst HTTP/1.1\r\nX: " ++ "a" ** max_header_bytes ++ "\r\n\r\n"));
    try std.testing.expectEqual(@as(usize, 0), calls);
    const prefix = "GET /commands/ruok HTTP/1.1\r\nX: ";
    var response = try handleRequest(std.testing.allocator, provider, prefix ++ "a" ** (max_header_bytes - prefix.len - 4) ++ "\r\n\r\n");
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expectEqual(@as(usize, 1), calls);
}
