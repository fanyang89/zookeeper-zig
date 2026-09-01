const std = @import("std");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");

pub const Kind = enum(i32) {
    create = 1,
    delete = 2,
    set_data = 5,
    check = 13,
    create2 = 15,
    create_container = 19,
    create_ttl = 21,
};

pub const ReadKind = enum(i32) {
    get_data = 4,
    get_children = 8,
};

pub const ReadOperation = union(ReadKind) {
    get_data: protocol.proto.GetDataRequest,
    get_children: protocol.proto.GetChildrenRequest,

    pub fn path(self: ReadOperation) ?[]const u8 {
        return switch (self) {
            inline else => |request| request.path,
        };
    }
};

pub const Operation = union(Kind) {
    create: protocol.proto.CreateRequest,
    delete: protocol.proto.DeleteRequest,
    set_data: protocol.proto.SetDataRequest,
    check: protocol.proto.CheckVersionRequest,
    create2: protocol.proto.CreateRequest,
    create_container: protocol.proto.CreateRequest,
    create_ttl: protocol.proto.CreateTTLRequest,

    pub fn deinit(self: *Operation, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .create => |*value| jute.deinitDecoded(value.*, allocator),
            .delete => |*value| jute.deinitDecoded(value.*, allocator),
            .set_data => |*value| jute.deinitDecoded(value.*, allocator),
            .check => |*value| jute.deinitDecoded(value.*, allocator),
            .create2 => |*value| jute.deinitDecoded(value.*, allocator),
            .create_container => |*value| jute.deinitDecoded(value.*, allocator),
            .create_ttl => |*value| jute.deinitDecoded(value.*, allocator),
        }
        self.* = undefined;
    }
};

pub const RequestIterator = struct {
    reader: jute.Reader,
    finished: bool = false,

    pub fn init(bytes: []const u8) RequestIterator {
        return .{ .reader = jute.Reader.init(bytes) };
    }

    pub fn next(self: *RequestIterator, allocator: std.mem.Allocator) !?Operation {
        if (self.finished) return null;
        const header = try jute.deserialize(protocol.proto.MultiHeader, &self.reader, allocator);
        if (header.done) {
            if (header.type != -1 or header.err != -1 or self.reader.remaining() != 0) {
                return error.InvalidMultiRequest;
            }
            self.finished = true;
            return null;
        }
        if (header.err != -1) return error.InvalidMultiRequest;
        return switch (header.type) {
            @intFromEnum(Kind.create) => .{ .create = try jute.deserialize(
                protocol.proto.CreateRequest,
                &self.reader,
                allocator,
            ) },
            @intFromEnum(Kind.delete) => .{ .delete = try jute.deserialize(
                protocol.proto.DeleteRequest,
                &self.reader,
                allocator,
            ) },
            @intFromEnum(Kind.set_data) => .{ .set_data = try jute.deserialize(
                protocol.proto.SetDataRequest,
                &self.reader,
                allocator,
            ) },
            @intFromEnum(Kind.check) => .{ .check = try jute.deserialize(
                protocol.proto.CheckVersionRequest,
                &self.reader,
                allocator,
            ) },
            @intFromEnum(Kind.create2) => .{ .create2 = try jute.deserialize(
                protocol.proto.CreateRequest,
                &self.reader,
                allocator,
            ) },
            @intFromEnum(Kind.create_container) => .{ .create_container = try jute.deserialize(
                protocol.proto.CreateRequest,
                &self.reader,
                allocator,
            ) },
            @intFromEnum(Kind.create_ttl) => .{ .create_ttl = try jute.deserialize(
                protocol.proto.CreateTTLRequest,
                &self.reader,
                allocator,
            ) },
            else => error.UnsupportedMultiOperation,
        };
    }
};

pub const ReadRequestIterator = struct {
    reader: jute.Reader,
    finished: bool = false,

    pub fn init(bytes: []const u8) ReadRequestIterator {
        return .{ .reader = jute.Reader.init(bytes) };
    }

    pub fn next(self: *ReadRequestIterator) !?ReadOperation {
        if (self.finished) return null;
        const header_type = try self.reader.readInt();
        const done = try self.reader.readBool();
        const header_error = try self.reader.readInt();
        if (done) {
            if (header_type != -1 or header_error != -1 or self.reader.remaining() != 0) {
                return error.InvalidMultiRequest;
            }
            self.finished = true;
            return null;
        }
        if (header_error != -1) return error.InvalidMultiRequest;
        return switch (header_type) {
            @intFromEnum(ReadKind.get_data) => .{ .get_data = .{
                .path = try self.reader.readString(),
                .watch = try self.reader.readBool(),
            } },
            @intFromEnum(ReadKind.get_children) => .{ .get_children = .{
                .path = try self.reader.readString(),
                .watch = try self.reader.readBool(),
            } },
            else => error.UnsupportedMultiOperation,
        };
    }
};

pub fn validateReadRequest(bytes: []const u8) !void {
    var iterator = ReadRequestIterator.init(bytes);
    while (try iterator.next()) |_| {}
}

pub const response_terminator_size: usize = 9;
pub const error_result_size: usize = 13;
pub const get_data_result_base_size: usize = 81;
pub const get_children_result_base_size: usize = 13;

pub fn ensureReadResponseRoom(
    current_size: usize,
    operation_size: usize,
    body_limit: usize,
) !void {
    const with_operation = std.math.add(usize, current_size, operation_size) catch
        return error.MultiReadResponseTooLarge;
    const with_terminator = std.math.add(
        usize,
        with_operation,
        response_terminator_size,
    ) catch return error.MultiReadResponseTooLarge;
    if (with_terminator > body_limit) return error.MultiReadResponseTooLarge;
}

pub fn writeErrorResult(writer: *jute.Writer, code: i32) !void {
    try writeHeader(writer, -1, false, code);
    try jute.serialize(writer, protocol.proto.ErrorResponse{ .err = code });
}

pub fn responseBodyCapacity(bytes: []const u8) !usize {
    var reader = jute.Reader.init(bytes);
    var total: usize = 0;
    while (true) {
        const header_type = try reader.readInt();
        const done = try reader.readBool();
        const header_error = try reader.readInt();
        if (done) {
            if (header_type != -1 or header_error != -1 or reader.remaining() != 0) {
                return error.InvalidMultiRequest;
            }
            return std.math.add(usize, total, 9) catch error.SizeOverflow;
        }
        if (header_error != -1) return error.InvalidMultiRequest;
        const success_size: usize = switch (header_type) {
            @intFromEnum(Kind.create) => try readCreateCapacity(&reader, false, false),
            @intFromEnum(Kind.create2), @intFromEnum(Kind.create_container) => try readCreateCapacity(&reader, true, false),
            @intFromEnum(Kind.create_ttl) => try readCreateCapacity(&reader, true, true),
            @intFromEnum(Kind.delete), @intFromEnum(Kind.check) => blk: {
                _ = (try reader.readString()) orelse return error.InvalidMultiRequest;
                _ = try reader.readInt();
                break :blk 9;
            },
            @intFromEnum(Kind.set_data) => blk: {
                _ = (try reader.readString()) orelse return error.InvalidMultiRequest;
                _ = try reader.readBuffer();
                _ = try reader.readInt();
                break :blk 77;
            },
            else => return error.UnsupportedMultiOperation,
        };
        total = std.math.add(usize, total, @max(success_size, 13)) catch
            return error.SizeOverflow;
    }
}

fn readCreateCapacity(reader: *jute.Reader, include_stat: bool, include_ttl: bool) !usize {
    const path = (try reader.readString()) orelse return error.InvalidMultiRequest;
    _ = try reader.readBuffer();
    try skipAcl(reader);
    const flags = try reader.readInt();
    if (include_ttl) _ = try reader.readLong();
    const resolved_path_length = std.math.add(
        usize,
        path.len,
        if ((flags & 2) != 0) 10 else 0,
    ) catch return error.SizeOverflow;
    return std.math.add(
        usize,
        if (include_stat) 81 else 13,
        resolved_path_length,
    ) catch error.SizeOverflow;
}

fn skipAcl(reader: *jute.Reader) !void {
    const count = try reader.readInt();
    if (count < -1) return error.InvalidMultiRequest;
    var index: i32 = 0;
    while (index < count) : (index += 1) {
        _ = try reader.readInt();
        _ = (try reader.readString()) orelse return error.InvalidMultiRequest;
        _ = (try reader.readString()) orelse return error.InvalidMultiRequest;
    }
}

pub fn writeHeader(writer: *jute.Writer, kind: i32, done: bool, err: i32) !void {
    try jute.serialize(writer, protocol.proto.MultiHeader{
        .type = kind,
        .done = done,
        .err = err,
    });
}

pub fn writeTerminator(writer: *jute.Writer) !void {
    try writeHeader(writer, -1, true, -1);
}

test "multi request iterator parses supported operations" {
    const testing = std.testing;
    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try writeHeader(&writer, @intFromEnum(Kind.create), false, -1);
    try jute.serialize(&writer, protocol.proto.CreateRequest{
        .path = "/node",
        .data = "value",
        .acl = null,
        .flags = 0,
    });
    try writeHeader(&writer, @intFromEnum(Kind.check), false, -1);
    try jute.serialize(&writer, protocol.proto.CheckVersionRequest{
        .path = "/node",
        .version = 0,
    });
    try writeHeader(&writer, @intFromEnum(Kind.create_ttl), false, -1);
    try jute.serialize(&writer, protocol.proto.CreateTTLRequest{
        .path = "/ttl",
        .data = "value",
        .acl = null,
        .flags = 5,
        .ttl = 100,
    });
    try writeTerminator(&writer);
    try testing.expectEqual(@as(usize, 125), try responseBodyCapacity(writer.bytes()));

    var iterator = RequestIterator.init(writer.bytes());
    var create = (try iterator.next(testing.allocator)).?;
    defer create.deinit(testing.allocator);
    try testing.expectEqualStrings("/node", create.create.path.?);
    var check = (try iterator.next(testing.allocator)).?;
    defer check.deinit(testing.allocator);
    try testing.expectEqual(@as(i32, 0), check.check.version);
    var create_ttl = (try iterator.next(testing.allocator)).?;
    defer create_ttl.deinit(testing.allocator);
    try testing.expectEqual(@as(i64, 100), create_ttl.create_ttl.ttl);
    try testing.expect((try iterator.next(testing.allocator)) == null);
}

test "multi request iterator rejects missing terminator" {
    const testing = std.testing;
    var iterator = RequestIterator.init("");
    try testing.expectError(error.EndOfStream, iterator.next(testing.allocator));
}

test "multi read iterator parses getData and getChildren" {
    const testing = std.testing;
    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try writeHeader(&writer, @intFromEnum(ReadKind.get_data), false, -1);
    try jute.serialize(&writer, protocol.proto.GetDataRequest{
        .path = "/data",
        .watch = false,
    });
    try writeHeader(&writer, @intFromEnum(ReadKind.get_children), false, -1);
    try jute.serialize(&writer, protocol.proto.GetChildrenRequest{
        .path = "/parent",
        .watch = false,
    });
    try writeTerminator(&writer);
    try validateReadRequest(writer.bytes());

    var iterator = ReadRequestIterator.init(writer.bytes());
    const get_data = (try iterator.next()).?;
    try testing.expectEqualStrings("/data", get_data.get_data.path.?);
    const get_children = (try iterator.next()).?;
    try testing.expectEqualStrings("/parent", get_children.get_children.path.?);
    try testing.expect((try iterator.next()) == null);
}

test "multi read response size constants match Jute encoding" {
    const testing = std.testing;
    var writer = jute.Writer.init(testing.allocator);
    defer writer.deinit();
    try writeHeader(&writer, @intFromEnum(ReadKind.get_data), false, 0);
    try jute.serialize(&writer, protocol.proto.GetDataResponse{
        .data = "abc",
        .stat = std.mem.zeroes(protocol.data.Stat),
    });
    try testing.expectEqual(get_data_result_base_size + 3, writer.dataSize());

    writer.truncate(0);
    try writeHeader(&writer, @intFromEnum(ReadKind.get_children), false, 0);
    try jute.serialize(&writer, protocol.proto.GetChildrenResponse{
        .children = &.{ "one", "two" },
    });
    try testing.expectEqual(
        get_children_result_base_size + 4 + 3 + 4 + 3,
        writer.dataSize(),
    );
}

test "multi read response budget reserves the terminator" {
    const testing = std.testing;
    try ensureReadResponseRoom(0, error_result_size, error_result_size + response_terminator_size);
    try testing.expectError(
        error.MultiReadResponseTooLarge,
        ensureReadResponseRoom(0, error_result_size, error_result_size + response_terminator_size - 1),
    );
    try testing.expectError(
        error.MultiReadResponseTooLarge,
        ensureReadResponseRoom(std.math.maxInt(usize), 1, std.math.maxInt(usize)),
    );
}

test "multi read iterator rejects write operations and trailing data" {
    const testing = std.testing;
    var write_request = jute.Writer.init(testing.allocator);
    defer write_request.deinit();
    try writeHeader(&write_request, @intFromEnum(Kind.check), false, -1);
    try jute.serialize(&write_request, protocol.proto.CheckVersionRequest{
        .path = "/node",
        .version = 0,
    });
    try writeTerminator(&write_request);
    try testing.expectError(
        error.UnsupportedMultiOperation,
        validateReadRequest(write_request.bytes()),
    );

    var trailing = jute.Writer.init(testing.allocator);
    defer trailing.deinit();
    try writeTerminator(&trailing);
    try trailing.writeByte(0);
    try testing.expectError(error.InvalidMultiRequest, validateReadRequest(trailing.bytes()));
}
