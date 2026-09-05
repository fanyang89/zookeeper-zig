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

/// Handles one HTTP/1.x request. `request` holds the raw bytes read from the
/// socket (the request may be truncated by the connection closing; only the
/// request line and `?format=` parameter matter).
pub fn handleRequest(
    allocator: std.mem.Allocator,
    provider: Provider,
    request: []const u8,
) !Response {
    var lines = std.mem.splitSequence(u8, request, "\r\n");
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
            self.connections.concurrent(self.io, serveTask, .{ self, stream }) catch |err| {
                stream.close(self.io);
                return err;
            };
        }
    }

    fn serveTask(self: *AdminServer, stream: std.Io.net.Stream) std.Io.Cancelable!void {
        self.serveConnection(stream) catch |err| {
            if (err == error.Canceled) return error.Canceled;
        };
    }

    fn serveConnection(self: *AdminServer, stream: std.Io.net.Stream) !void {
        defer stream.close(self.io);
        // Read until end of headers; requests are tiny so a single bounded
        // buffer is enough, and HTTP/1.0-style close-delimited requests from
        // curl still work because we only need the request line.
        var buffer: [4096]u8 = undefined;
        var length: usize = 0;
        while (length < buffer.len) {
            const message = try stream.socket.receive(self.io, buffer[length..]);
            if (message.data.len == 0) break;
            length += message.data.len;
            if (std.mem.indexOf(u8, buffer[0..length], "\r\n\r\n") != null) break;
        }
        const request = buffer[0..length];
        if (request.len == 0) return;

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
        try writeAll(self.io, stream, outbound.items);
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

    try writeAll(testing.io, connected, "GET /commands/mntr HTTP/1.1\r\nHost: localhost\r\n\r\n");
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
