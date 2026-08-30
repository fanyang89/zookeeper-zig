const std = @import("std");
const binary = @import("binary.zig");

pub const DeserializeError = binary.DecodeError || std.mem.Allocator.Error;
pub const SerializeError = binary.WriteError;

pub fn MapEntry(comptime Key: type, comptime Value: type) type {
    return struct {
        key: Key,
        value: Value,
    };
}

pub fn serialize(writer: *binary.Writer, value: anytype) SerializeError!void {
    try serializeValue(@TypeOf(value), writer, value);
}

pub fn deserialize(
    comptime Record: type,
    reader: *binary.Reader,
    allocator: std.mem.Allocator,
) DeserializeError!Record {
    return deserializeValue(Record, reader, allocator);
}

/// Releases allocations created by `deserialize`. Borrowed byte slices are not
/// released. Do not call this for records assembled by the caller.
pub fn deinitDecoded(value: anytype, allocator: std.mem.Allocator) void {
    deinitValue(@TypeOf(value), value, allocator);
}

fn serializeValue(comptime T: type, writer: *binary.Writer, value: T) SerializeError!void {
    switch (@typeInfo(T)) {
        .bool => try writer.writeBool(value),
        .int => |info| switch (info.bits) {
            8 => try writer.writeByte(@bitCast(value)),
            32 => try writer.writeInt(@bitCast(value)),
            64 => try writer.writeLong(@bitCast(value)),
            else => @compileError("unsupported Jute integer type: " ++ @typeName(T)),
        },
        .float => |info| switch (info.bits) {
            32 => try writer.writeFloat(value),
            64 => try writer.writeDouble(value),
            else => @compileError("unsupported Jute float type: " ++ @typeName(T)),
        },
        .optional => |info| try serializeOptional(info.child, writer, value),
        .pointer => |info| {
            if (info.size != .slice) @compileError("Jute pointers must be slices: " ++ @typeName(T));
            try serializeSlice(T, info.child, writer, value);
        },
        .@"struct" => |info| inline for (info.fields) |field| {
            try serializeValue(field.type, writer, @field(value, field.name));
        },
        else => @compileError("unsupported Jute type: " ++ @typeName(T)),
    }
}

fn serializeOptional(
    comptime Child: type,
    writer: *binary.Writer,
    value: ?Child,
) SerializeError!void {
    switch (@typeInfo(Child)) {
        .pointer => |info| {
            if (info.size != .slice) @compileError("optional Jute pointers must be slices: " ++ @typeName(Child));
            if (value) |slice| {
                try serializeSlice(Child, info.child, writer, slice);
            } else {
                try writer.writeVectorLength(null);
            }
        },
        else => @compileError("only optional slices are supported by Jute reflection: " ++ @typeName(Child)),
    }
}

fn serializeSlice(
    comptime Slice: type,
    comptime Element: type,
    writer: *binary.Writer,
    value: Slice,
) SerializeError!void {
    if (Element == u8) {
        try writer.writeBuffer(value);
        return;
    }

    try writer.writeVectorLength(value.len);
    for (value) |element| try serializeValue(Element, writer, element);
}

fn deserializeValue(
    comptime T: type,
    reader: *binary.Reader,
    allocator: std.mem.Allocator,
) DeserializeError!T {
    return switch (@typeInfo(T)) {
        .bool => try reader.readBool(),
        .int => |info| switch (info.bits) {
            8 => @bitCast(try reader.readByte()),
            32 => @bitCast(try reader.readInt()),
            64 => @bitCast(try reader.readLong()),
            else => @compileError("unsupported Jute integer type: " ++ @typeName(T)),
        },
        .float => |info| switch (info.bits) {
            32 => try reader.readFloat(),
            64 => try reader.readDouble(),
            else => @compileError("unsupported Jute float type: " ++ @typeName(T)),
        },
        .optional => |info| try deserializeOptional(T, info.child, reader, allocator),
        .pointer => |info| blk: {
            if (info.size != .slice) @compileError("Jute pointers must be slices: " ++ @typeName(T));
            break :blk try deserializeRequiredSlice(T, info.child, reader, allocator);
        },
        .@"struct" => blk: {
            var result: T = undefined;
            try deserializeStructFields(T, 0, &result, reader, allocator);
            break :blk result;
        },
        else => @compileError("unsupported Jute type: " ++ @typeName(T)),
    };
}

fn deserializeStructFields(
    comptime Record: type,
    comptime field_index: usize,
    result: *Record,
    reader: *binary.Reader,
    allocator: std.mem.Allocator,
) DeserializeError!void {
    const fields = @typeInfo(Record).@"struct".fields;
    if (field_index == fields.len) return;

    const field = fields[field_index];
    @field(result.*, field.name) = try deserializeValue(field.type, reader, allocator);
    errdefer deinitValue(field.type, @field(result.*, field.name), allocator);
    try deserializeStructFields(Record, field_index + 1, result, reader, allocator);
}

fn deserializeOptional(
    comptime Optional: type,
    comptime Child: type,
    reader: *binary.Reader,
    allocator: std.mem.Allocator,
) DeserializeError!Optional {
    return switch (@typeInfo(Child)) {
        .pointer => |info| blk: {
            if (info.size != .slice) @compileError("optional Jute pointers must be slices: " ++ @typeName(Child));
            if (info.child == u8) break :blk try reader.readBuffer();
            const length = try reader.readVectorLength() orelse break :blk null;
            break :blk try deserializeSlice(Child, info.child, length, reader, allocator);
        },
        else => @compileError("only optional slices are supported by Jute reflection: " ++ @typeName(Child)),
    };
}

fn deserializeRequiredSlice(
    comptime Slice: type,
    comptime Element: type,
    reader: *binary.Reader,
    allocator: std.mem.Allocator,
) DeserializeError!Slice {
    if (Element == u8) return (try reader.readBuffer()) orelse error.InvalidLength;
    const length = (try reader.readVectorLength()) orelse return error.InvalidLength;
    return deserializeSlice(Slice, Element, length, reader, allocator);
}

fn deserializeSlice(
    comptime Slice: type,
    comptime Element: type,
    length: usize,
    reader: *binary.Reader,
    allocator: std.mem.Allocator,
) DeserializeError!Slice {
    const values = try allocator.alloc(Element, length);
    errdefer allocator.free(values);

    var initialized: usize = 0;
    errdefer for (values[0..initialized]) |value| deinitValue(Element, value, allocator);
    while (initialized < values.len) : (initialized += 1) {
        values[initialized] = try deserializeValue(Element, reader, allocator);
    }
    return values;
}

fn deinitValue(comptime T: type, value: T, allocator: std.mem.Allocator) void {
    switch (@typeInfo(T)) {
        .optional => |info| if (value) |child| deinitValue(info.child, child, allocator),
        .pointer => |info| {
            if (info.size != .slice) @compileError("Jute pointers must be slices: " ++ @typeName(T));
            if (info.child == u8) return;
            for (value) |element| deinitValue(info.child, element, allocator);
            allocator.free(value);
        },
        .@"struct" => |info| inline for (info.fields) |field| {
            deinitValue(field.type, @field(value, field.name), allocator);
        },
        else => {},
    }
}

const AllocationChild = struct {
    values: ?[]const i32,
};

const AllocationRecord = struct {
    first: ?[]const i32,
    children: ?[]const AllocationChild,
};

fn decodeAllocationFixture(allocator: std.mem.Allocator) !void {
    const encoded = [_]u8{
        0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x02,
        0x00, 0x00, 0x00, 0x03,
        0x00, 0x00, 0x00, 0x04,
    };
    var reader = binary.Reader.init(&encoded);
    const decoded = try deserialize(AllocationRecord, &reader, allocator);
    defer deinitDecoded(decoded, allocator);
}

test "reflection frees every allocation when allocation fails" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        decodeAllocationFixture,
        .{},
    );
}

test "reflection supports every Jute primitive" {
    const testing = std.testing;
    const Record = struct {
        byte: i8,
        boolean: bool,
        int: i32,
        long: i64,
        float: f32,
        double: f64,
    };
    const expected = Record{
        .byte = -7,
        .boolean = true,
        .int = -42,
        .long = 123_456,
        .float = 1.25,
        .double = -9.5,
    };

    var writer = binary.Writer.init(testing.allocator);
    defer writer.deinit();
    try serialize(&writer, expected);

    var reader = binary.Reader.init(writer.bytes());
    const actual = try deserialize(Record, &reader, testing.allocator);
    try testing.expectEqualDeep(expected, actual);
}

test "reflection serializes and deserializes records in declaration order" {
    const testing = std.testing;
    const Child = struct {
        id: i64,
    };
    const Record = struct {
        xid: i32,
        path: ?[]const u8,
        children: ?[]const Child,
        watch: bool,
    };

    const children = [_]Child{ .{ .id = 7 }, .{ .id = 9 } };
    const expected = Record{
        .xid = 42,
        .path = "/node",
        .children = &children,
        .watch = true,
    };

    var writer = binary.Writer.init(testing.allocator);
    defer writer.deinit();
    try serialize(&writer, expected);

    var reader = binary.Reader.init(writer.bytes());
    const actual = try deserialize(Record, &reader, testing.allocator);
    defer deinitDecoded(actual, testing.allocator);

    try testing.expectEqual(expected.xid, actual.xid);
    try testing.expectEqualStrings(expected.path.?, actual.path.?);
    try testing.expectEqual(@as(usize, 2), actual.children.?.len);
    try testing.expectEqual(@as(i64, 7), actual.children.?[0].id);
    try testing.expectEqual(@as(i64, 9), actual.children.?[1].id);
    try testing.expect(actual.watch);
    try testing.expectEqual(@as(usize, 0), reader.remaining());
}

test "reflection cleans initialized fields when a later field is truncated" {
    const testing = std.testing;
    const Record = struct {
        values: ?[]const i32,
        tail: i64,
    };
    const values = [_]i32{ 1, 2, 3 };

    var writer = binary.Writer.init(testing.allocator);
    defer writer.deinit();
    try serialize(&writer, Record{ .values = &values, .tail = 9 });

    var reader = binary.Reader.init(writer.bytes()[0 .. writer.bytes().len - 1]);
    try testing.expectError(error.EndOfStream, deserialize(Record, &reader, testing.allocator));
}

test "reflection cleans a partially initialized nested vector element" {
    const testing = std.testing;
    const Element = struct {
        values: ?[]const i32,
        tail: i64,
    };
    const Record = struct {
        elements: ?[]const Element,
    };
    const values = [_]i32{ 4, 5 };
    const elements = [_]Element{.{ .values = &values, .tail = 10 }};

    var writer = binary.Writer.init(testing.allocator);
    defer writer.deinit();
    try serialize(&writer, Record{ .elements = &elements });

    var reader = binary.Reader.init(writer.bytes()[0 .. writer.bytes().len - 1]);
    try testing.expectError(error.EndOfStream, deserialize(Record, &reader, testing.allocator));
}

test "reflection preserves null and empty slices" {
    const testing = std.testing;
    const Record = struct {
        missing: ?[]const u8,
        empty: ?[]const i32,
    };
    const empty = [_]i32{};

    var writer = binary.Writer.init(testing.allocator);
    defer writer.deinit();
    try serialize(&writer, Record{ .missing = null, .empty = &empty });

    var reader = binary.Reader.init(writer.bytes());
    const actual = try deserialize(Record, &reader, testing.allocator);
    defer deinitDecoded(actual, testing.allocator);
    try testing.expectEqual(@as(?[]const u8, null), actual.missing);
    try testing.expectEqual(@as(usize, 0), actual.empty.?.len);
}
