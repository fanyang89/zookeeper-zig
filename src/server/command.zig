const std = @import("std");
const jute = @import("../jute.zig");
const data_tree = @import("data_tree.zig");

pub const version: i32 = 1;

pub const Kind = enum(i32) {
    create = 1,
    delete = 2,
    set_data = 5,
};

pub const Mutation = union(Kind) {
    create: struct {
        path: []const u8,
        data: []const u8,
        time_ms: i64,
    },
    delete: struct {
        path: []const u8,
        expected_version: i32,
    },
    set_data: struct {
        path: []const u8,
        data: []const u8,
        expected_version: i32,
        time_ms: i64,
    },
};

pub const ResultView = struct {
    code: data_tree.ErrorCode,
    zxid: i64,
    body: []const u8,
};

pub fn resultCapacity(mutation: Mutation) error{SizeOverflow}!usize {
    return switch (mutation) {
        .create => |value| std.math.add(usize, 84, value.path.len) catch error.SizeOverflow,
        .delete => 12,
        .set_data => 80,
    };
}

pub fn encode(allocator: std.mem.Allocator, mutation: Mutation) ![]u8 {
    var writer = jute.Writer.init(allocator);
    defer writer.deinit();
    try writer.writeInt(version);
    try writer.writeInt(@intFromEnum(mutation));
    switch (mutation) {
        .create => |value| {
            try writer.writeString(value.path);
            try writer.writeBuffer(value.data);
            try writer.writeLong(value.time_ms);
        },
        .delete => |value| {
            try writer.writeString(value.path);
            try writer.writeInt(value.expected_version);
        },
        .set_data => |value| {
            try writer.writeString(value.path);
            try writer.writeBuffer(value.data);
            try writer.writeInt(value.expected_version);
            try writer.writeLong(value.time_ms);
        },
    }
    return allocator.dupe(u8, writer.bytes());
}

pub fn decode(bytes: []const u8) !Mutation {
    var reader = jute.Reader.init(bytes);
    if (try reader.readInt() != version) return error.UnsupportedCommandVersion;
    const kind = checkedEnum(Kind, try reader.readInt()) orelse return error.UnknownCommand;
    const mutation: Mutation = switch (kind) {
        .create => .{ .create = .{
            .path = (try reader.readString()) orelse return error.InvalidCommand,
            .data = (try reader.readBuffer()) orelse return error.InvalidCommand,
            .time_ms = try reader.readLong(),
        } },
        .delete => .{ .delete = .{
            .path = (try reader.readString()) orelse return error.InvalidCommand,
            .expected_version = try reader.readInt(),
        } },
        .set_data => .{ .set_data = .{
            .path = (try reader.readString()) orelse return error.InvalidCommand,
            .data = (try reader.readBuffer()) orelse return error.InvalidCommand,
            .expected_version = try reader.readInt(),
            .time_ms = try reader.readLong(),
        } },
    };
    if (reader.remaining() != 0) return error.InvalidCommand;
    return mutation;
}

pub fn encodeResult(
    allocator: std.mem.Allocator,
    code: data_tree.ErrorCode,
    zxid: i64,
    body: anytype,
) ![]u8 {
    var writer = jute.Writer.init(allocator);
    defer writer.deinit();
    try writer.writeInt(@intFromEnum(code));
    try writer.writeLong(zxid);
    if (@TypeOf(body) != void) try jute.serialize(&writer, body);
    return allocator.dupe(u8, writer.bytes());
}

pub fn decodeResult(bytes: []const u8) !ResultView {
    var reader = jute.Reader.init(bytes);
    const code = checkedEnum(data_tree.ErrorCode, try reader.readInt()) orelse
        return error.UnknownResultCode;
    const zxid = try reader.readLong();
    return .{ .code = code, .zxid = zxid, .body = bytes[reader.consumed()..] };
}

fn checkedEnum(comptime T: type, value: std.meta.Tag(T)) ?T {
    inline for (std.meta.fields(T)) |field| {
        if (field.value == value) return @enumFromInt(value);
    }
    return null;
}

test "mutation commands and results round trip" {
    const testing = std.testing;
    const encoded = try encode(testing.allocator, .{ .set_data = .{
        .path = "/node",
        .data = "value",
        .expected_version = 4,
        .time_ms = 99,
    } });
    defer testing.allocator.free(encoded);
    const decoded = try decode(encoded);
    try testing.expectEqualStrings("/node", decoded.set_data.path);
    try testing.expectEqualStrings("value", decoded.set_data.data);
    try testing.expectEqual(@as(i32, 4), decoded.set_data.expected_version);

    const response = try encodeResult(testing.allocator, .ok, 7, {});
    defer testing.allocator.free(response);
    const result = try decodeResult(response);
    try testing.expectEqual(data_tree.ErrorCode.ok, result.code);
    try testing.expectEqual(@as(i64, 7), result.zxid);
    try testing.expectEqual(@as(usize, 0), result.body.len);
}
