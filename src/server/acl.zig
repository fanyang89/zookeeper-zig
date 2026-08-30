const std = @import("std");
const jute = @import("../jute.zig");
const protocol = @import("../protocol.zig");

pub const read: i32 = 1;
pub const write: i32 = 2;
pub const create: i32 = 4;
pub const delete: i32 = 8;
pub const admin: i32 = 16;
pub const all: i32 = read | write | create | delete | admin;

pub const Identity = struct {
    scheme: []const u8,
    id: []const u8,
};

pub const Entry = struct {
    perms: i32,
    scheme: []const u8,
    id: []const u8,
};

pub const open_unsafe = [_]Entry{.{
    .perms = all,
    .scheme = "world",
    .id = "anyone",
}};

pub fn encode(allocator: std.mem.Allocator, entries: []const Entry) ![]u8 {
    if (entries.len == 0 or entries.len > std.math.maxInt(i32)) return error.InvalidAcl;
    var writer = jute.Writer.init(allocator);
    errdefer writer.deinit();
    try writer.writeInt(@intCast(entries.len));
    for (entries) |entry| {
        if (!isValidEntry(entry)) return error.InvalidAcl;
        try writer.writeInt(entry.perms);
        try writer.writeString(entry.scheme);
        try writer.writeString(entry.id);
    }
    return writer.toOwnedSliceAssert();
}

pub fn encodeIdentities(allocator: std.mem.Allocator, identities: []const Identity) ![]u8 {
    if (identities.len > std.math.maxInt(i32)) return error.TooManyIdentities;
    var writer = jute.Writer.init(allocator);
    errdefer writer.deinit();
    try writer.writeInt(@intCast(identities.len));
    for (identities) |identity| {
        try writer.writeString(identity.scheme);
        try writer.writeString(identity.id);
    }
    return writer.toOwnedSliceAssert();
}

pub fn normalize(
    allocator: std.mem.Allocator,
    entries: ?[]const protocol.data.ACL,
    identities: []const Identity,
) ![]u8 {
    const source = entries orelse return error.InvalidAcl;
    if (source.len == 0) return error.InvalidAcl;
    var normalized: std.ArrayList(Entry) = .empty;
    defer normalized.deinit(allocator);
    for (source) |entry| {
        const scheme = entry.id.scheme orelse return error.InvalidAcl;
        const id = entry.id.id orelse return error.InvalidAcl;
        if (std.mem.eql(u8, scheme, "auth")) {
            if (id.len != 0 or identities.len == 0) return error.InvalidAcl;
            for (identities) |identity| try normalized.append(allocator, .{
                .perms = entry.perms,
                .scheme = identity.scheme,
                .id = identity.id,
            });
        } else {
            try normalized.append(allocator, .{
                .perms = entry.perms,
                .scheme = scheme,
                .id = id,
            });
        }
    }
    return encode(allocator, normalized.items);
}

pub fn allows(blob: ?[]const u8, permission: i32, identities_blob: ?[]const u8) !bool {
    if (blob == null) return true;
    var reader = jute.Reader.init(blob.?);
    const count = try reader.readInt();
    if (count <= 0) return error.InvalidAcl;
    var index: i32 = 0;
    while (index < count) : (index += 1) {
        const perms = try reader.readInt();
        const scheme = (try reader.readString()) orelse return error.InvalidAcl;
        const id = (try reader.readString()) orelse return error.InvalidAcl;
        if ((perms & permission) == 0) continue;
        if (std.mem.eql(u8, scheme, "world") and std.mem.eql(u8, id, "anyone")) return true;
        if (try containsIdentity(identities_blob, scheme, id)) return true;
    }
    if (reader.remaining() != 0) return error.InvalidAcl;
    return false;
}

pub fn validate(blob: []const u8) !void {
    var reader = jute.Reader.init(blob);
    const count = try reader.readInt();
    if (count <= 0) return error.InvalidAcl;
    var index: i32 = 0;
    while (index < count) : (index += 1) {
        const entry = Entry{
            .perms = try reader.readInt(),
            .scheme = (try reader.readString()) orelse return error.InvalidAcl,
            .id = (try reader.readString()) orelse return error.InvalidAcl,
        };
        if (!isValidEntry(entry)) return error.InvalidAcl;
    }
    if (reader.remaining() != 0) return error.InvalidAcl;
}

pub fn decodeViews(
    allocator: std.mem.Allocator,
    blob: ?[]const u8,
) ![]protocol.data.ACL {
    if (blob == null) {
        const entries = try allocator.alloc(protocol.data.ACL, 1);
        entries[0] = .{ .perms = all, .id = .{ .scheme = "world", .id = "anyone" } };
        return entries;
    }
    var reader = jute.Reader.init(blob.?);
    const count = try reader.readInt();
    if (count <= 0) return error.InvalidAcl;
    const entries = try allocator.alloc(protocol.data.ACL, @intCast(count));
    errdefer allocator.free(entries);
    for (entries) |*entry| {
        entry.* = .{
            .perms = try reader.readInt(),
            .id = .{
                .scheme = (try reader.readString()) orelse return error.InvalidAcl,
                .id = (try reader.readString()) orelse return error.InvalidAcl,
            },
        };
    }
    if (reader.remaining() != 0) return error.InvalidAcl;
    return entries;
}

pub fn digestIdentity(allocator: std.mem.Allocator, credentials: []const u8) ![]u8 {
    const colon = std.mem.indexOfScalar(u8, credentials, ':') orelse return error.InvalidCredentials;
    if (colon == 0) return error.InvalidCredentials;
    var digest: [std.crypto.hash.Sha1.digest_length]u8 = undefined;
    std.crypto.hash.Sha1.hash(credentials, &digest, .{});
    const encoded_len = std.base64.standard.Encoder.calcSize(digest.len);
    const result = try allocator.alloc(u8, colon + 1 + encoded_len);
    errdefer allocator.free(result);
    @memcpy(result[0..colon], credentials[0..colon]);
    result[colon] = ':';
    _ = std.base64.standard.Encoder.encode(result[colon + 1 ..], &digest);
    return result;
}

fn containsIdentity(blob: ?[]const u8, scheme: []const u8, id: []const u8) !bool {
    const encoded = blob orelse return false;
    var reader = jute.Reader.init(encoded);
    const count = try reader.readInt();
    if (count < 0) return error.InvalidIdentity;
    var index: i32 = 0;
    while (index < count) : (index += 1) {
        const candidate_scheme = (try reader.readString()) orelse return error.InvalidIdentity;
        const candidate_id = (try reader.readString()) orelse return error.InvalidIdentity;
        if (std.mem.eql(u8, scheme, candidate_scheme) and std.mem.eql(u8, id, candidate_id)) {
            return true;
        }
    }
    if (reader.remaining() != 0) return error.InvalidIdentity;
    return false;
}

fn isValidEntry(entry: Entry) bool {
    if (std.mem.eql(u8, entry.scheme, "world")) return std.mem.eql(u8, entry.id, "anyone");
    if (std.mem.eql(u8, entry.scheme, "digest")) {
        const colon = std.mem.indexOfScalar(u8, entry.id, ':') orelse return false;
        return colon > 0 and colon + 1 < entry.id.len;
    }
    return false;
}

test "zero permission ACL is valid and grants nothing" {
    const testing = std.testing;
    const blob = try encode(testing.allocator, &.{.{
        .perms = 0,
        .scheme = "world",
        .id = "anyone",
    }});
    defer testing.allocator.free(blob);
    try validate(blob);
    try testing.expect(!try allows(blob, read, null));
}

test "digest identity matches ZooKeeper format" {
    const testing = std.testing;
    const identity = try digestIdentity(testing.allocator, "super:secret");
    defer testing.allocator.free(identity);
    try testing.expectEqualStrings("super:lK75jTNcA+U9vtVEw5vB51mj/w4=", identity);
}
