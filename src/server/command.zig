const std = @import("std");
const jute = @import("../jute.zig");
const data_tree = @import("data_tree.zig");

pub const version: i32 = 3;
pub const legacy_version: i32 = 1;

pub const Kind = enum(i32) {
    create = 1,
    delete = 2,
    set_data = 5,
    open_session = 100,
    touch_session = 101,
    close_session = 102,
    expire_session = 103,
    move_session = 104,
    session_tick = 105,
};

pub const Mutation = union(Kind) {
    create: struct {
        path: []const u8,
        data: []const u8,
        time_ms: i64,
        ephemeral: bool = false,
        session_id: i64 = 0,
        session_generation: u64 = 0,
        sequential: bool = false,
    },
    delete: struct {
        path: []const u8,
        expected_version: i32,
        session_id: i64 = 0,
        session_generation: u64 = 0,
    },
    set_data: struct {
        path: []const u8,
        data: []const u8,
        expected_version: i32,
        time_ms: i64,
        session_id: i64 = 0,
        session_generation: u64 = 0,
    },
    open_session: struct {
        session_id: i64,
        password: []const u8,
        timeout_ms: i32,
        tick_grace_ms: i32,
        generation: u64,
    },
    touch_session: struct {
        session_id: i64,
        password: []const u8,
        generation: u64,
    },
    close_session: struct {
        session_id: i64,
        password: []const u8,
        generation: u64,
    },
    expire_session: struct {
        session_id: i64,
        expected_expires_at_ms: i64,
    },
    move_session: struct {
        session_id: i64,
        password: []const u8,
        expected_generation: u64,
        new_generation: u64,
    },
    session_tick: struct {
        leader_term: u64,
        elapsed_ms: i64,
    },
};

pub const ResultView = struct {
    code: data_tree.ErrorCode,
    zxid: i64,
    body: []const u8,
};

pub fn resultCapacity(mutation: Mutation) error{SizeOverflow}!usize {
    return switch (mutation) {
        .create => |value| std.math.add(usize, 94, value.path.len) catch error.SizeOverflow,
        .delete, .open_session, .touch_session, .close_session, .expire_session, .move_session, .session_tick => 12,
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
            try writer.writeBool(value.ephemeral);
            try writer.writeLong(value.session_id);
            try writer.writeLong(@bitCast(value.session_generation));
            try writer.writeBool(value.sequential);
        },
        .delete => |value| {
            try writer.writeString(value.path);
            try writer.writeInt(value.expected_version);
            try writer.writeLong(value.session_id);
            try writer.writeLong(@bitCast(value.session_generation));
        },
        .set_data => |value| {
            try writer.writeString(value.path);
            try writer.writeBuffer(value.data);
            try writer.writeInt(value.expected_version);
            try writer.writeLong(value.time_ms);
            try writer.writeLong(value.session_id);
            try writer.writeLong(@bitCast(value.session_generation));
        },
        .open_session => |value| {
            try writer.writeLong(value.session_id);
            try writer.writeBuffer(value.password);
            try writer.writeInt(value.timeout_ms);
            try writer.writeInt(value.tick_grace_ms);
            try writer.writeLong(@bitCast(value.generation));
        },
        .touch_session => |value| {
            try writer.writeLong(value.session_id);
            try writer.writeBuffer(value.password);
            try writer.writeLong(@bitCast(value.generation));
        },
        .close_session => |value| {
            try writer.writeLong(value.session_id);
            try writer.writeBuffer(value.password);
            try writer.writeLong(@bitCast(value.generation));
        },
        .expire_session => |value| {
            try writer.writeLong(value.session_id);
            try writer.writeLong(value.expected_expires_at_ms);
        },
        .move_session => |value| {
            try writer.writeLong(value.session_id);
            try writer.writeBuffer(value.password);
            try writer.writeLong(@bitCast(value.expected_generation));
            try writer.writeLong(@bitCast(value.new_generation));
        },
        .session_tick => |value| {
            try writer.writeLong(@bitCast(value.leader_term));
            try writer.writeLong(value.elapsed_ms);
        },
    }
    return allocator.dupe(u8, writer.bytes());
}

pub fn decode(bytes: []const u8) !Mutation {
    var reader = jute.Reader.init(bytes);
    const encoded_version = try reader.readInt();
    if (encoded_version < legacy_version or encoded_version > version) {
        return error.UnsupportedCommandVersion;
    }
    const kind = checkedEnum(Kind, try reader.readInt()) orelse return error.UnknownCommand;
    if (encoded_version == legacy_version and @intFromEnum(kind) >= 100) {
        return error.UnsupportedCommandVersion;
    }
    const mutation: Mutation = switch (kind) {
        .create => .{ .create = .{
            .path = (try reader.readString()) orelse return error.InvalidCommand,
            .data = (try reader.readBuffer()) orelse return error.InvalidCommand,
            .time_ms = try reader.readLong(),
            .ephemeral = if (encoded_version >= 2) try reader.readBool() else false,
            .session_id = if (encoded_version >= 2) try reader.readLong() else 0,
            .session_generation = if (encoded_version >= 2) @bitCast(try reader.readLong()) else 0,
            .sequential = if (encoded_version >= 3) try reader.readBool() else false,
        } },
        .delete => .{ .delete = .{
            .path = (try reader.readString()) orelse return error.InvalidCommand,
            .expected_version = try reader.readInt(),
            .session_id = if (encoded_version >= 2) try reader.readLong() else 0,
            .session_generation = if (encoded_version >= 2) @bitCast(try reader.readLong()) else 0,
        } },
        .set_data => .{ .set_data = .{
            .path = (try reader.readString()) orelse return error.InvalidCommand,
            .data = (try reader.readBuffer()) orelse return error.InvalidCommand,
            .expected_version = try reader.readInt(),
            .time_ms = try reader.readLong(),
            .session_id = if (encoded_version >= 2) try reader.readLong() else 0,
            .session_generation = if (encoded_version >= 2) @bitCast(try reader.readLong()) else 0,
        } },
        .open_session => .{ .open_session = .{
            .session_id = try reader.readLong(),
            .password = (try reader.readBuffer()) orelse return error.InvalidCommand,
            .timeout_ms = try reader.readInt(),
            .tick_grace_ms = try reader.readInt(),
            .generation = @bitCast(try reader.readLong()),
        } },
        .touch_session => .{ .touch_session = .{
            .session_id = try reader.readLong(),
            .password = (try reader.readBuffer()) orelse return error.InvalidCommand,
            .generation = @bitCast(try reader.readLong()),
        } },
        .close_session => .{ .close_session = .{
            .session_id = try reader.readLong(),
            .password = (try reader.readBuffer()) orelse return error.InvalidCommand,
            .generation = @bitCast(try reader.readLong()),
        } },
        .expire_session => .{ .expire_session = .{
            .session_id = try reader.readLong(),
            .expected_expires_at_ms = try reader.readLong(),
        } },
        .move_session => .{ .move_session = .{
            .session_id = try reader.readLong(),
            .password = (try reader.readBuffer()) orelse return error.InvalidCommand,
            .expected_generation = @bitCast(try reader.readLong()),
            .new_generation = @bitCast(try reader.readLong()),
        } },
        .session_tick => .{ .session_tick = .{
            .leader_term = @bitCast(try reader.readLong()),
            .elapsed_ms = try reader.readLong(),
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

    const password = [_]u8{0xa5} ** 16;
    const session_encoded = try encode(testing.allocator, .{ .open_session = .{
        .session_id = 42,
        .password = &password,
        .timeout_ms = 5_000,
        .tick_grace_ms = 500,
        .generation = 9_000,
    } });
    defer testing.allocator.free(session_encoded);
    const session_decoded = try decode(session_encoded);
    try testing.expectEqual(@as(i64, 42), session_decoded.open_session.session_id);
    try testing.expectEqualSlices(u8, &password, session_decoded.open_session.password);
    try testing.expectEqual(@as(i32, 5_000), session_decoded.open_session.timeout_ms);
    try testing.expectEqual(@as(i32, 500), session_decoded.open_session.tick_grace_ms);
    try testing.expectEqual(@as(u64, 9_000), session_decoded.open_session.generation);

    var legacy_writer = jute.Writer.init(testing.allocator);
    defer legacy_writer.deinit();
    try legacy_writer.writeInt(legacy_version);
    try legacy_writer.writeInt(@intFromEnum(Kind.create));
    try legacy_writer.writeString("/legacy");
    try legacy_writer.writeBuffer("payload");
    try legacy_writer.writeLong(123);
    const legacy_create = try decode(legacy_writer.bytes());
    try testing.expectEqualStrings("/legacy", legacy_create.create.path);
    try testing.expect(!legacy_create.create.ephemeral);
    try testing.expectEqual(@as(i64, 0), legacy_create.create.session_id);

    var version_two_writer = jute.Writer.init(testing.allocator);
    defer version_two_writer.deinit();
    try version_two_writer.writeInt(2);
    try version_two_writer.writeInt(@intFromEnum(Kind.create));
    try version_two_writer.writeString("/v2");
    try version_two_writer.writeBuffer("payload");
    try version_two_writer.writeLong(456);
    try version_two_writer.writeBool(true);
    try version_two_writer.writeLong(42);
    try version_two_writer.writeLong(7);
    const version_two_create = try decode(version_two_writer.bytes());
    try testing.expect(version_two_create.create.ephemeral);
    try testing.expect(!version_two_create.create.sequential);
    try testing.expectEqual(@as(u64, 7), version_two_create.create.session_generation);

    const response = try encodeResult(testing.allocator, .ok, 7, {});
    defer testing.allocator.free(response);
    const result = try decodeResult(response);
    try testing.expectEqual(data_tree.ErrorCode.ok, result.code);
    try testing.expectEqual(@as(i64, 7), result.zxid);
    try testing.expectEqual(@as(usize, 0), result.body.len);
}
