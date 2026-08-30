const std = @import("std");

pub const DecodeError = error{
    EndOfStream,
    InvalidLength,
    LengthLimitExceeded,
    CollectionLimitExceeded,
};

pub const WriteError = std.mem.Allocator.Error || error{LengthOverflow};
pub const ReadAllocError = DecodeError || std.mem.Allocator.Error;

pub const Limits = struct {
    max_buffer_size: usize = 0xfffff,
    extra_max_buffer_size: usize = 0xfffff,
    max_collection_elements: usize = 0xfffff,

    pub fn totalBufferSize(self: Limits) usize {
        const max = @as(usize, std.math.maxInt(i32));
        const buffer = @min(self.max_buffer_size, max);
        const extra = @min(self.extra_max_buffer_size, max);
        if (extra > max - buffer) return max;
        return buffer + extra;
    }
};

pub const Writer = struct {
    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8) = .empty,

    pub fn init(allocator: std.mem.Allocator) Writer {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Writer) void {
        self.buffer.deinit(self.allocator);
        self.* = undefined;
    }

    /// The returned slice is invalidated by the next write that reallocates the
    /// buffer and by `deinit`.
    pub fn bytes(self: *const Writer) []const u8 {
        return self.buffer.items;
    }

    pub fn dataSize(self: *const Writer) usize {
        return self.buffer.items.len;
    }

    pub fn ensureTotalCapacityPrecise(self: *Writer, capacity: usize) std.mem.Allocator.Error!void {
        try self.buffer.ensureTotalCapacityPrecise(self.allocator, capacity);
    }

    pub fn toOwnedSlice(self: *Writer) std.mem.Allocator.Error![]u8 {
        return self.buffer.toOwnedSlice(self.allocator);
    }

    pub fn toOwnedSliceAssert(self: *Writer) []u8 {
        return self.buffer.toOwnedSliceAssert();
    }

    pub fn truncate(self: *Writer, length: usize) void {
        std.debug.assert(length <= self.buffer.items.len);
        self.buffer.items.len = length;
    }

    pub fn patchInt(self: *Writer, offset: usize, value: i32) error{InvalidOffset}!void {
        if (offset > self.buffer.items.len or self.buffer.items.len - offset < 4) {
            return error.InvalidOffset;
        }
        std.mem.writeInt(i32, self.buffer.items[offset..][0..4], value, .big);
    }

    pub fn writeByte(self: *Writer, value: i8) std.mem.Allocator.Error!void {
        try self.buffer.append(self.allocator, @bitCast(value));
    }

    pub fn writeBool(self: *Writer, value: bool) std.mem.Allocator.Error!void {
        try self.buffer.append(self.allocator, @intFromBool(value));
    }

    pub fn writeInt(self: *Writer, value: i32) std.mem.Allocator.Error!void {
        var bytes_: [4]u8 = undefined;
        std.mem.writeInt(i32, &bytes_, value, .big);
        try self.buffer.appendSlice(self.allocator, &bytes_);
    }

    pub fn writeBytes(self: *Writer, bytes_: []const u8) std.mem.Allocator.Error!void {
        try self.buffer.appendSlice(self.allocator, bytes_);
    }

    pub fn writeLong(self: *Writer, value: i64) std.mem.Allocator.Error!void {
        var bytes_: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes_, value, .big);
        try self.buffer.appendSlice(self.allocator, &bytes_);
    }

    pub fn writeFloat(self: *Writer, value: f32) std.mem.Allocator.Error!void {
        const bits: i32 = if (value != value)
            @bitCast(@as(u32, 0x7fc00000))
        else
            @bitCast(value);
        try self.writeInt(bits);
    }

    pub fn writeDouble(self: *Writer, value: f64) std.mem.Allocator.Error!void {
        const bits: i64 = if (value != value)
            @bitCast(@as(u64, 0x7ff8000000000000))
        else
            @bitCast(value);
        try self.writeLong(bits);
    }

    /// Writes the supplied Jute `ustring` wire bytes without transcoding.
    pub fn writeString(self: *Writer, value: ?[]const u8) WriteError!void {
        try self.writeNullableBytes(value);
    }

    pub fn writeBuffer(self: *Writer, value: ?[]const u8) WriteError!void {
        try self.writeNullableBytes(value);
    }

    pub fn writeVectorLength(self: *Writer, length: ?usize) WriteError!void {
        if (length) |value| {
            try self.writeLength(value);
        } else {
            try self.writeInt(-1);
        }
    }

    pub fn writeMapLength(self: *Writer, length: usize) WriteError!void {
        try self.writeLength(length);
    }

    pub fn writeRecord(self: *Writer, record: anytype) @TypeOf(record.serialize(self)) {
        return record.serialize(self);
    }

    fn writeNullableBytes(self: *Writer, value: ?[]const u8) WriteError!void {
        if (value) |bytes_| {
            try self.writeLength(bytes_.len);
            try self.buffer.appendSlice(self.allocator, bytes_);
        } else {
            try self.writeInt(-1);
        }
    }

    fn writeLength(self: *Writer, length: usize) WriteError!void {
        if (length > std.math.maxInt(i32)) return error.LengthOverflow;
        try self.writeInt(@intCast(length));
    }
};

pub const Reader = struct {
    input: []const u8,
    position: usize = 0,
    limits: Limits,

    pub fn init(input: []const u8) Reader {
        return initWithLimits(input, .{});
    }

    pub fn initWithLimits(input: []const u8, limits: Limits) Reader {
        return .{ .input = input, .limits = limits };
    }

    pub fn consumed(self: *const Reader) usize {
        return self.position;
    }

    pub fn remaining(self: *const Reader) usize {
        return self.input.len - self.position;
    }

    pub fn readByte(self: *Reader) DecodeError!i8 {
        return @bitCast((try self.take(1))[0]);
    }

    pub fn readBool(self: *Reader) DecodeError!bool {
        return (try self.take(1))[0] != 0;
    }

    pub fn readInt(self: *Reader) DecodeError!i32 {
        const bytes_ = try self.take(4);
        return std.mem.readInt(i32, bytes_[0..4], .big);
    }

    pub fn readLong(self: *Reader) DecodeError!i64 {
        const bytes_ = try self.take(8);
        return std.mem.readInt(i64, bytes_[0..8], .big);
    }

    pub fn readFloat(self: *Reader) DecodeError!f32 {
        return @bitCast(try self.readInt());
    }

    pub fn readDouble(self: *Reader) DecodeError!f64 {
        return @bitCast(try self.readLong());
    }

    /// The returned string bytes borrow from the Reader input. Jute `ustring`
    /// payloads are exposed as wire bytes; this method does not transcode them.
    pub fn readString(self: *Reader) DecodeError!?[]const u8 {
        return self.readNullableBytes();
    }

    /// The returned buffer borrows from the Reader input.
    pub fn readBuffer(self: *Reader) DecodeError!?[]const u8 {
        return self.readNullableBytes();
    }

    /// The caller owns the returned string and must free it with `allocator`.
    /// The Reader position remains consumed if allocation fails.
    pub fn readStringAlloc(self: *Reader, allocator: std.mem.Allocator) ReadAllocError!?[]u8 {
        const value = try self.readString() orelse return null;
        return try allocator.dupe(u8, value);
    }

    /// The caller owns the returned buffer and must free it with `allocator`.
    /// The Reader position remains consumed if allocation fails.
    pub fn readBufferAlloc(self: *Reader, allocator: std.mem.Allocator) ReadAllocError!?[]u8 {
        const value = try self.readBuffer() orelse return null;
        return try allocator.dupe(u8, value);
    }

    pub fn readVectorLength(self: *Reader) DecodeError!?usize {
        const length = try self.readInt();
        if (length == -1) return null;
        return try self.validateCollectionLength(length);
    }

    pub fn readMapLength(self: *Reader) DecodeError!usize {
        return self.validateCollectionLength(try self.readInt());
    }

    pub fn readRecord(self: *Reader, comptime Record: type) @TypeOf(Record.deserialize(self)) {
        return Record.deserialize(self);
    }

    fn readNullableBytes(self: *Reader) DecodeError!?[]const u8 {
        const length = try self.readInt();
        if (length == -1) return null;
        if (length < 0) return error.InvalidLength;

        const value: usize = @intCast(length);
        if (value > self.limits.totalBufferSize()) return error.LengthLimitExceeded;
        return try self.take(value);
    }

    fn validateCollectionLength(self: *const Reader, length: i32) DecodeError!usize {
        if (length < 0) return error.InvalidLength;
        const value: usize = @intCast(length);
        if (value > self.limits.max_collection_elements) return error.CollectionLimitExceeded;
        return value;
    }

    fn take(self: *Reader, length: usize) DecodeError![]const u8 {
        if (length > self.remaining()) return error.EndOfStream;
        const start = self.position;
        self.position += length;
        return self.input[start..self.position];
    }
};

test "binary archive encodes Java-compatible primitive values" {
    const testing = std.testing;
    var writer = Writer.init(testing.allocator);
    defer writer.deinit();

    try writer.writeByte(-2);
    try writer.writeBool(true);
    try writer.writeInt(0x01020304);
    try writer.writeLong(0x0102030405060708);
    try writer.writeFloat(1.5);
    try writer.writeDouble(-2.25);

    const expected = [_]u8{
        0xfe,
        0x01,
        0x01,
        0x02,
        0x03,
        0x04,
        0x01,
        0x02,
        0x03,
        0x04,
        0x05,
        0x06,
        0x07,
        0x08,
        0x3f,
        0xc0,
        0x00,
        0x00,
        0xc0,
        0x02,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
    };
    try testing.expectEqualSlices(u8, &expected, writer.bytes());

    var reader = Reader.init(writer.bytes());
    try testing.expectEqual(@as(i8, -2), try reader.readByte());
    try testing.expect(try reader.readBool());
    try testing.expectEqual(@as(i32, 0x01020304), try reader.readInt());
    try testing.expectEqual(@as(i64, 0x0102030405060708), try reader.readLong());
    try testing.expectEqual(@as(f32, 1.5), try reader.readFloat());
    try testing.expectEqual(@as(f64, -2.25), try reader.readDouble());
    try testing.expectEqual(@as(usize, 0), reader.remaining());
}

test "binary archive canonicalizes NaN like Java DataOutput" {
    const testing = std.testing;
    var writer = Writer.init(testing.allocator);
    defer writer.deinit();

    try writer.writeFloat(@bitCast(@as(u32, 0x7fa12345)));
    try writer.writeDouble(@bitCast(@as(u64, 0x7ff123456789abcd)));

    const expected = [_]u8{
        0x7f, 0xc0, 0x00, 0x00,
        0x7f, 0xf8, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    };
    try testing.expectEqualSlices(u8, &expected, writer.bytes());
}

test "binary archive handles strings buffers and null values" {
    const testing = std.testing;
    var writer = Writer.init(testing.allocator);
    defer writer.deinit();

    try writer.writeString("hello");
    try writer.writeString(null);
    try writer.writeBuffer(&.{ 0x00, 0xff, 0x7f });
    try writer.writeBuffer(null);

    var reader = Reader.init(writer.bytes());
    try testing.expectEqualStrings("hello", (try reader.readString()).?);
    try testing.expectEqual(@as(?[]const u8, null), try reader.readString());
    try testing.expectEqualSlices(u8, &.{ 0x00, 0xff, 0x7f }, (try reader.readBuffer()).?);
    try testing.expectEqual(@as(?[]const u8, null), try reader.readBuffer());
    try testing.expectEqual(writer.dataSize(), reader.consumed());

    var allocating_reader = Reader.init(&.{ 0, 0, 0, 3, 'z', 'i', 'g' });
    const owned = (try allocating_reader.readStringAlloc(testing.allocator)).?;
    defer testing.allocator.free(owned);
    try testing.expectEqualStrings("zig", owned);
}

test "owned writer slice supports spare capacity" {
    const testing = std.testing;
    var writer = Writer.init(testing.allocator);
    defer writer.deinit();

    try writer.ensureTotalCapacityPrecise(64);
    try writer.writeInt(42);
    const owned = try writer.toOwnedSlice();
    defer testing.allocator.free(owned);

    try testing.expectEqualSlices(u8, &.{ 0, 0, 0, 42 }, owned);
}

test "binary archive enforces byte and collection limits" {
    const testing = std.testing;

    var oversized_bytes = Reader.initWithLimits(&.{ 0, 0, 0, 5, 'h', 'e', 'l', 'l', 'o' }, .{
        .max_buffer_size = 2,
        .extra_max_buffer_size = 2,
    });
    try testing.expectError(error.LengthLimitExceeded, oversized_bytes.readString());

    var oversized_vector = Reader.initWithLimits(&.{ 0, 0, 0, 3 }, .{ .max_collection_elements = 2 });
    try testing.expectError(error.CollectionLimitExceeded, oversized_vector.readVectorLength());

    var invalid = Reader.init(&.{ 0xff, 0xff, 0xff, 0xfe });
    try testing.expectError(error.InvalidLength, invalid.readBuffer());

    var invalid_vector = Reader.init(&.{ 0xff, 0xff, 0xff, 0xfe });
    try testing.expectError(error.InvalidLength, invalid_vector.readVectorLength());

    var invalid_map = Reader.init(&.{ 0xff, 0xff, 0xff, 0xff });
    try testing.expectError(error.InvalidLength, invalid_map.readMapLength());

    var truncated = Reader.init(&.{ 0, 0, 0, 4, 1, 2 });
    try testing.expectError(error.EndOfStream, truncated.readBuffer());
}

test "buffer limit addition saturates at the Jute signed length maximum" {
    const testing = std.testing;
    const limits = Limits{
        .max_buffer_size = std.math.maxInt(usize),
        .extra_max_buffer_size = std.math.maxInt(usize),
    };
    try testing.expectEqual(@as(usize, std.math.maxInt(i32)), limits.totalBufferSize());
}

test "binary archive encodes vector and map lengths" {
    const testing = std.testing;
    var writer = Writer.init(testing.allocator);
    defer writer.deinit();

    try writer.writeVectorLength(3);
    try writer.writeVectorLength(null);
    try writer.writeMapLength(2);

    var reader = Reader.init(writer.bytes());
    try testing.expectEqual(@as(?usize, 3), try reader.readVectorLength());
    try testing.expectEqual(@as(?usize, null), try reader.readVectorLength());
    try testing.expectEqual(@as(usize, 2), try reader.readMapLength());
}

test "binary archive supports record serialization" {
    const testing = std.testing;
    const Sample = struct {
        id: i32,
        enabled: bool,

        fn serialize(self: @This(), writer: *Writer) !void {
            try writer.writeInt(self.id);
            try writer.writeBool(self.enabled);
        }

        fn deserialize(reader: *Reader) !@This() {
            return .{
                .id = try reader.readInt(),
                .enabled = try reader.readBool(),
            };
        }
    };

    var writer = Writer.init(testing.allocator);
    defer writer.deinit();
    try writer.writeRecord(Sample{ .id = 42, .enabled = true });

    var reader = Reader.init(writer.bytes());
    const decoded = try reader.readRecord(Sample);
    try testing.expectEqual(@as(i32, 42), decoded.id);
    try testing.expect(decoded.enabled);
}
