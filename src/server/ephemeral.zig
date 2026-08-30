const std = @import("std");

pub const NodeKind = enum(u8) {
    persistent = 0,
    ephemeral = 1,
    container = 2,
    ttl = 3,
};

pub const container_owner: i64 = std.math.minInt(i64);
pub const extended_mask: u64 = 0xff00000000000000;
pub const reserved_mask: u64 = 0x00ffff0000000000;
pub const ttl_value_mask: u64 = 0x000000ffffffffff;
pub const max_ttl_ms: i64 = @intCast(ttl_value_mask);
pub const container_unused_timeout_ms: i64 = 5 * 60 * 1000;

pub fn isValidTtl(ttl_ms: i64) bool {
    return ttl_ms > 0 and ttl_ms <= max_ttl_ms;
}

pub fn ttlOwner(ttl_ms: i64) !i64 {
    if (!isValidTtl(ttl_ms)) return error.InvalidTtl;
    return @bitCast(extended_mask | @as(u64, @intCast(ttl_ms)));
}

pub fn ttlValue(owner: i64) !i64 {
    const bits: u64 = @bitCast(owner);
    if ((bits & extended_mask) != extended_mask or (bits & reserved_mask) != 0) {
        return error.InvalidTtlOwner;
    }
    const value: i64 = @intCast(bits & ttl_value_mask);
    if (value <= 0) return error.InvalidTtlOwner;
    return value;
}

pub fn classifyOwner(owner: i64) !NodeKind {
    if (owner == 0) return .persistent;
    if (owner == container_owner) return .container;
    const bits: u64 = @bitCast(owner);
    if ((bits & extended_mask) == extended_mask) {
        _ = try ttlValue(owner);
        return .ttl;
    }
    return .ephemeral;
}

pub fn kindFromByte(value: u8) ?NodeKind {
    return switch (value) {
        0 => .persistent,
        1 => .ephemeral,
        2 => .container,
        3 => .ttl,
        else => null,
    };
}

pub fn validate(kind: NodeKind, owner: i64) !void {
    switch (kind) {
        .persistent => {
            if (owner != 0) return error.InvalidNodeKind;
        },
        .ephemeral => {
            if (owner == 0) return error.InvalidNodeKind;
        },
        .container => {
            if (owner != container_owner) return error.InvalidNodeKind;
        },
        .ttl => {
            _ = ttlValue(owner) catch return error.InvalidNodeKind;
        },
    }
}

pub fn clientOwner(kind: NodeKind, owner: i64) i64 {
    return if (kind == .ephemeral) owner else 0;
}

pub fn permitsChildren(kind: NodeKind) bool {
    return kind != .ephemeral;
}

test "extended owners match ZooKeeper 3.9.5" {
    const testing = std.testing;
    try testing.expectEqual(container_owner, @as(i64, @bitCast(@as(u64, 0x8000000000000000))));
    const owner = try ttlOwner(1234);
    try testing.expectEqual(@as(u64, 0xff000000000004d2), @as(u64, @bitCast(owner)));
    try testing.expectEqual(@as(i64, 1234), try ttlValue(owner));
    try testing.expectEqual(NodeKind.ttl, try classifyOwner(owner));
    try testing.expectEqual(@as(i64, 0), clientOwner(.ttl, owner));
}
