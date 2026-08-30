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
    defer writer.deinit();
    try writer.writeInt(@intCast(entries.len));
    for (entries) |entry| {
        if (!isValidEntry(entry)) return error.InvalidAcl;
        try writer.writeInt(entry.perms);
        try writer.writeString(entry.scheme);
        try writer.writeString(entry.id);
    }
    return allocator.dupe(u8, writer.bytes());
}

pub fn encodeIdentities(allocator: std.mem.Allocator, identities: []const Identity) ![]u8 {
    if (identities.len > std.math.maxInt(i32)) return error.TooManyIdentities;
    var writer = jute.Writer.init(allocator);
    defer writer.deinit();
    try writer.writeInt(@intCast(identities.len));
    for (identities) |identity| {
        try writer.writeString(identity.scheme);
        try writer.writeString(identity.id);
    }
    return allocator.dupe(u8, writer.bytes());
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
        if (std.mem.eql(u8, scheme, "auth")) {
            if (entry.id.id) |id| {
                if (id.len != 0) return error.InvalidAcl;
            }
            var expanded = false;
            for (identities) |identity| {
                if (!isAuthenticatedScheme(identity.scheme)) continue;
                try normalized.append(allocator, .{
                    .perms = entry.perms,
                    .scheme = identity.scheme,
                    .id = identity.id,
                });
                expanded = true;
            }
            if (!expanded) return error.InvalidAcl;
        } else {
            try normalized.append(allocator, .{
                .perms = entry.perms,
                .scheme = scheme,
                .id = entry.id.id orelse return error.InvalidAcl,
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
        if (std.mem.eql(u8, scheme, candidate_scheme) and identityMatches(scheme, candidate_id, id)) {
            return true;
        }
    }
    if (reader.remaining() != 0) return error.InvalidIdentity;
    return false;
}

fn identityMatches(scheme: []const u8, identity: []const u8, acl_id: []const u8) bool {
    if (std.mem.eql(u8, scheme, "ip")) return ipv4Matches(identity, acl_id);
    return std.mem.eql(u8, identity, acl_id);
}

fn isAuthenticatedScheme(scheme: []const u8) bool {
    return std.mem.eql(u8, scheme, "digest");
}

fn isValidEntry(entry: Entry) bool {
    if (std.mem.eql(u8, entry.scheme, "world")) return std.mem.eql(u8, entry.id, "anyone");
    if (std.mem.eql(u8, entry.scheme, "digest")) {
        const colon = std.mem.indexOfScalar(u8, entry.id, ':') orelse return false;
        return colon > 0 and colon + 1 < entry.id.len;
    }
    if (std.mem.eql(u8, entry.scheme, "ip")) return parseIpv4Acl(entry.id) != null;
    return false;
}

const Ipv4Acl = struct {
    address: u32,
    prefix: u6,
};

fn parseIpv4Acl(value: []const u8) ?Ipv4Acl {
    const slash = std.mem.indexOfScalar(u8, value, '/');
    if (slash != null and std.mem.indexOfScalarPos(u8, value, slash.? + 1, '/') != null) return null;
    const address_text = if (slash) |index| value[0..index] else value;
    const address = parseIpv4(address_text) orelse return null;
    const prefix: u6 = if (slash) |index| blk: {
        const text = value[index + 1 ..];
        if (text.len == 0) return null;
        const parsed = parseDecimalByte(text) orelse return null;
        if (parsed > 32) return null;
        break :blk @intCast(parsed);
    } else 32;
    return .{ .address = address, .prefix = prefix };
}

fn parseIpv4(value: []const u8) ?u32 {
    var parts = std.mem.splitScalar(u8, value, '.');
    var address: u32 = 0;
    var count: usize = 0;
    while (parts.next()) |part| {
        if (count == 4 or part.len == 0) return null;
        const octet = parseDecimalByte(part) orelse return null;
        address = (address << 8) | octet;
        count += 1;
    }
    return if (count == 4) address else null;
}

fn parseDecimalByte(value: []const u8) ?u8 {
    if (value.len == 0) return null;
    for (value) |byte| if (byte < '0' or byte > '9') return null;
    return std.fmt.parseUnsigned(u8, value, 10) catch null;
}

fn ipv4Matches(identity: []const u8, acl_id: []const u8) bool {
    const candidate = parseIpv4(identity) orelse return false;
    const rule = parseIpv4Acl(acl_id) orelse return false;
    if (rule.prefix == 0) return true;
    const shift: u5 = @intCast(32 - rule.prefix);
    return (candidate >> shift) == (rule.address >> shift);
}

test "IPv4 ACL matching supports ZooKeeper CIDR masks" {
    const testing = std.testing;
    const blob = try encode(testing.allocator, &.{.{
        .perms = read,
        .scheme = "ip",
        .id = "192.168.1.0/24",
    }});
    defer testing.allocator.free(blob);
    const allowed = try encodeIdentities(testing.allocator, &.{.{
        .scheme = "ip",
        .id = "192.168.1.42",
    }});
    defer testing.allocator.free(allowed);
    const denied = try encodeIdentities(testing.allocator, &.{.{
        .scheme = "ip",
        .id = "192.168.2.42",
    }});
    defer testing.allocator.free(denied);
    try testing.expect(try allows(blob, read, allowed));
    try testing.expect(!try allows(blob, read, denied));
    try testing.expect(!try allows(blob, write, allowed));
    try testing.expect(ipv4Matches("203.0.113.9", "0.0.0.0/0"));
    try testing.expect(ipv4Matches("10.0.15.255", "10.0.0.0/20"));
    try testing.expect(!ipv4Matches("10.0.16.0", "10.0.0.0/20"));
    try testing.expect(ipv4Matches("127.0.0.1", "127.0.0.1"));
    try testing.expect(!ipv4Matches("127.0.0.2", "127.0.0.1/32"));
    try testing.expect(!isValidEntry(.{
        .perms = read,
        .scheme = "ip",
        .id = "192.168.1_0.1/2_4",
    }));
}

test "auth ACL with null id expands to a digest identity" {
    const testing = std.testing;
    const entries = [_]protocol.data.ACL{.{
        .perms = all,
        .id = .{ .scheme = "auth", .id = null },
    }};
    const identities = [_]Identity{.{
        .scheme = "digest",
        .id = "ben:Zx6hEcF2qP6F4I0RFRKZ+YLTswU=",
    }};
    const blob = try normalize(testing.allocator, &entries, &identities);
    defer testing.allocator.free(blob);
    const encoded_identities = try encodeIdentities(testing.allocator, &identities);
    defer testing.allocator.free(encoded_identities);
    try testing.expect(try allows(blob, all, encoded_identities));
}

test "auth ACL expansion excludes IP identities" {
    const testing = std.testing;
    const entries = [_]protocol.data.ACL{.{
        .perms = all,
        .id = .{ .scheme = "auth", .id = "" },
    }};
    const identities = [_]Identity{.{ .scheme = "ip", .id = "127.0.0.1" }};
    try testing.expectError(error.InvalidAcl, normalize(testing.allocator, &entries, &identities));
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
