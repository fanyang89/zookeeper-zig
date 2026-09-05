// ZooKeeper dynamic membership configuration (reconfig).
//
// The quorum membership is expressed in the upstream ZooKeeper config-string
// format stored at `/zookeeper/config`: one `server.<id>` entry per line,
// where each entry is `server.<id>=<host>:<port>[:<role>]` with an optional
// upstream-style second port and `;<client address>` suffix that this server
// accepts and ignores. The single port is the Raft address.
//
// `reconfig` requests either apply incremental joins and leaves to the
// current config or replace it with a full member list, guarded by a
// compare-and-set on the config node version (`curConfigId`).

const std = @import("std");

pub const path = "/zookeeper/config";

pub const Member = struct {
    id: u64,
    address: []const u8,
    observer: bool = false,
};

pub const Error = error{
    InvalidConfig,
    InvalidMemberId,
    InvalidMemberAddress,
    DuplicateMemberId,
    EmptyMembership,
    MissingMember,
    LeavingLastParticipant,
    OutOfMemory,
};

/// Parses one `server.<id>=<spec>` entry spec into host, raft port, and role.
fn parseSpec(allocator: std.mem.Allocator, spec: []const u8) Error!struct { address: []const u8, observer: bool } {
    // Drop the upstream client-address suffix: `host:ports:role;clientaddr`.
    const member_part = if (std.mem.indexOfScalar(u8, spec, ';')) |index| spec[0..index] else spec;
    var observer = false;
    var host: ?[]const u8 = null;
    var port: ?u16 = null;
    var index: usize = 0;
    var parts = std.mem.splitScalar(u8, member_part, ':');
    while (parts.next()) |part| : (index += 1) {
        switch (index) {
            0 => host = part,
            1 => port = std.fmt.parseInt(u16, part, 10) catch return Error.InvalidMemberAddress,
            2 => {
                if (std.fmt.parseInt(u16, part, 10)) |_| {
                    // Upstream election port: accepted and ignored; the role
                    // follows as the fourth component.
                } else |_| {
                    observer = try parseRole(part);
                }
            },
            3 => observer = try parseRole(part),
            else => return Error.InvalidMemberAddress,
        }
    }
    const resolved_host = host orelse return Error.InvalidMemberAddress;
    const resolved_port = port orelse return Error.InvalidMemberAddress;
    if (resolved_host.len == 0) return Error.InvalidMemberAddress;
    return .{
        .address = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ resolved_host, resolved_port }),
        .observer = observer,
    };
}

fn parseRole(text: []const u8) Error!bool {
    if (std.mem.eql(u8, text, "participant")) return false;
    if (std.mem.eql(u8, text, "observer")) return true;
    return Error.InvalidConfig;
}

/// Parses a full config string. Entries are separated by newlines or commas.
pub fn parseConfig(allocator: std.mem.Allocator, text: []const u8) Error![]Member {
    var members: std.ArrayList(Member) = .empty;
    errdefer {
        for (members.items) |member| allocator.free(member.address);
        members.deinit(allocator);
    }
    var entries = std.mem.tokenizeAny(u8, text, "\n,");
    while (entries.next()) |raw_entry| {
        const entry = std.mem.trim(u8, raw_entry, " \t");
        if (entry.len == 0) continue;
        const assignment = std.mem.indexOfScalar(u8, entry, '=') orelse return Error.InvalidConfig;
        const id_text = entry[0..assignment];
        const id_name = if (std.mem.startsWith(u8, id_text, "server."))
            id_text["server.".len..]
        else
            id_text;
        const id = std.fmt.parseInt(u64, id_name, 10) catch return Error.InvalidMemberId;
        if (id == 0) return Error.InvalidMemberId;
        for (members.items) |member| {
            if (member.id == id) return Error.DuplicateMemberId;
        }
        const spec = try parseSpec(allocator, entry[assignment + 1 ..]);
        try members.append(allocator, .{ .id = id, .address = spec.address, .observer = spec.observer });
    }
    const result = try members.toOwnedSlice(allocator);
    errdefer allocator.free(result);
    try validate(result);
    return result;
}

pub fn validate(members: []const Member) Error!void {
    if (members.len == 0) return Error.EmptyMembership;
    var participants: usize = 0;
    var observers: usize = 0;
    for (members) |member| {
        if (member.observer) observers += 1 else participants += 1;
    }
    if (participants == 0 and observers > 0) return Error.EmptyMembership;
}

/// Formats members canonically: `server.<id>=<host>:<port>:<role>` lines
/// sorted by id and joined with newlines.
pub fn formatConfig(allocator: std.mem.Allocator, members: []const Member) Error![]u8 {
    const sorted = try allocator.dupe(Member, members);
    defer allocator.free(sorted);
    std.mem.sort(Member, sorted, {}, struct {
        fn lessThan(_: void, left: Member, right: Member) bool {
            return left.id < right.id;
        }
    }.lessThan);
    var output: std.ArrayList(u8) = .empty;
    errdefer output.deinit(allocator);
    for (sorted, 0..) |member, index| {
        if (index > 0) try output.append(allocator, '\n');
        const role: []const u8 = if (member.observer) "observer" else "participant";
        const line = try std.fmt.allocPrint(allocator, "server.{d}={s}:{s}", .{ member.id, member.address, role });
        defer allocator.free(line);
        try output.appendSlice(allocator, line);
    }
    return output.toOwnedSlice(allocator);
}

/// Parses a `leavingServers` list: bare ids or `server.<id>` names.
pub fn parseLeaving(allocator: std.mem.Allocator, text: []const u8) Error![]u64 {
    var ids: std.ArrayList(u64) = .empty;
    errdefer ids.deinit(allocator);
    var entries = std.mem.tokenizeAny(u8, text, "\n,");
    while (entries.next()) |raw_entry| {
        const entry = std.mem.trim(u8, raw_entry, " \t");
        if (entry.len == 0) continue;
        const id_name = if (std.mem.startsWith(u8, entry, "server."))
            entry["server.".len..]
        else
            entry;
        const id = std.fmt.parseInt(u64, id_name, 10) catch return Error.InvalidMemberId;
        try ids.append(allocator, id);
    }
    const result = try ids.toOwnedSlice(allocator);
    errdefer allocator.free(result);
    for (result, 0..) |id, index| {
        if (id == 0) return Error.InvalidMemberId;
        for (result[0..index]) |seen| {
            if (seen == id) return Error.DuplicateMemberId;
        }
    }
    return result;
}

/// Applies incremental joins and leaves to a parsed current membership.
/// Both strings may be null or empty. Join entries use the full
/// `server.<id>=<spec>` form.
pub fn applyChanges(
    allocator: std.mem.Allocator,
    current: []const Member,
    joining_text: ?[]const u8,
    leaving_text: ?[]const u8,
) Error![]Member {
    var members: std.ArrayList(Member) = .empty;
    errdefer {
        for (members.items) |member| allocator.free(member.address);
        members.deinit(allocator);
    }
    const leaving = try parseLeaving(allocator, leaving_text orelse "");
    defer allocator.free(leaving);
    for (current) |member| {
        var removed = false;
        for (leaving) |id| {
            if (id == member.id) removed = true;
        }
        if (removed) continue;
        try members.append(allocator, .{
            .id = member.id,
            .address = try allocator.dupe(u8, member.address),
            .observer = member.observer,
        });
    }

    if (joining_text) |text| {
        if (text.len > 0) {
            const joining = try parseConfig(allocator, text);
            defer {
                for (joining) |member| allocator.free(member.address);
                allocator.free(joining);
            }
            for (joining) |join| {
                var replaced = false;
                for (members.items) |*member| {
                    if (member.id == join.id) {
                        const updated = try allocator.dupe(u8, join.address);
                        allocator.free(member.address);
                        member.address = updated;
                        member.observer = join.observer;
                        replaced = true;
                    }
                }
                if (!replaced) {
                    try members.append(allocator, .{
                        .id = join.id,
                        .address = try allocator.dupe(u8, join.address),
                        .observer = join.observer,
                    });
                }
            }
        }
    }

    const result = try members.toOwnedSlice(allocator);
    errdefer allocator.free(result);
    try validate(result);
    return result;
}

/// One step toward reconciling live Raft membership with a target config.
pub const MembershipOp = union(enum) {
    add: struct { id: u64, address: []const u8, learner: bool },
    remove: struct { id: u64 },
    update_address: struct { id: u64, address: []const u8 },
};

pub const LiveMember = struct {
    id: u64,
    address: []const u8,
};

/// Computes the membership operations needed to move `live` to `target`.
/// Adds and address updates precede removes so the quorum never shrinks
/// before new members are voting. Caller owns the returned ops; addresses
/// alias `live` and `target`.
pub fn diffMembership(
    allocator: std.mem.Allocator,
    live: []const LiveMember,
    target: []const Member,
) Error![]MembershipOp {
    var ops: std.ArrayList(MembershipOp) = .empty;
    errdefer ops.deinit(allocator);
    for (target) |member| {
        var live_address: ?[]const u8 = null;
        for (live) |entry| {
            if (entry.id == member.id) live_address = entry.address;
        }
        if (live_address) |address| {
            if (!std.mem.eql(u8, address, member.address)) {
                try ops.append(allocator, .{ .update_address = .{
                    .id = member.id,
                    .address = member.address,
                } });
            }
            continue;
        }
        try ops.append(allocator, .{ .add = .{
            .id = member.id,
            .address = member.address,
            .learner = member.observer,
        } });
    }
    for (live) |entry| {
        var present = false;
        for (target) |member| {
            if (member.id == entry.id) present = true;
        }
        if (!present) try ops.append(allocator, .{ .remove = .{ .id = entry.id } });
    }
    return ops.toOwnedSlice(allocator);
}

pub fn freeMembers(allocator: std.mem.Allocator, members: []Member) void {
    for (members) |member| allocator.free(member.address);
    allocator.free(members);
}

test "parse accepts upstream three and four part forms" {
    const members = try parseConfig(std.testing.allocator,
        \\server.1=127.0.0.1:2881:participant;0.0.0.0:2181
        \\server.2=127.0.0.1:2882:3882
        \\server.3=127.0.0.1:2883:observer
        \\server.4=127.0.0.1:2884
    );
    defer freeMembers(std.testing.allocator, members);
    try std.testing.expectEqual(@as(usize, 4), members.len);
    try std.testing.expectEqual(@as(u64, 1), members[0].id);
    try std.testing.expectEqualStrings("127.0.0.1:2881", members[0].address);
    try std.testing.expect(!members[0].observer);
    try std.testing.expect(!members[1].observer);
    try std.testing.expect(members[2].observer);
    try std.testing.expect(!members[3].observer);
}

test "parse rejects malformed entries" {
    try std.testing.expectError(Error.InvalidConfig, parseConfig(std.testing.allocator, "1.2.3.4:2888"));
    try std.testing.expectError(Error.InvalidMemberId, parseConfig(std.testing.allocator, "server.x=1.2.3.4:2888"));
    try std.testing.expectError(Error.InvalidMemberAddress, parseConfig(std.testing.allocator, "server.1=host:notaport"));
    try std.testing.expectError(Error.DuplicateMemberId, parseConfig(std.testing.allocator, "server.1=1.2.3.4:1,server.1=1.2.3.4:2"));
    try std.testing.expectError(Error.EmptyMembership, parseConfig(std.testing.allocator, ""));
}

test "format and parse round-trip canonically" {
    const members = [_]Member{
        .{ .id = 2, .address = "10.0.0.2:2882" },
        .{ .id = 1, .address = "10.0.0.1:2881" },
        .{ .id = 3, .address = "10.0.0.3:2883", .observer = true },
    };
    const text = try formatConfig(std.testing.allocator, &members);
    defer std.testing.allocator.free(text);
    try std.testing.expectEqualStrings(
        "server.1=10.0.0.1:2881:participant\n" ++
            "server.2=10.0.0.2:2882:participant\n" ++
            "server.3=10.0.0.3:2883:observer",
        text,
    );
    const parsed = try parseConfig(std.testing.allocator, text);
    defer freeMembers(std.testing.allocator, parsed);
    try std.testing.expectEqual(@as(usize, 3), parsed.len);
    try std.testing.expectEqual(@as(u64, 1), parsed[0].id);
    try std.testing.expect(parsed[2].observer);
}

test "applyChanges adds removes and updates members" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const current = try parseConfig(allocator, "server.1=127.0.0.1:2881,server.2=127.0.0.1:2882");
    const updated = try applyChanges(
        allocator,
        current,
        "server.3=127.0.0.1:2883:participant",
        "2",
    );
    const text = try formatConfig(allocator, updated);
    try std.testing.expectEqualStrings(
        "server.1=127.0.0.1:2881:participant\nserver.3=127.0.0.1:2883:participant",
        text,
    );
}

test "diffMembership orders adds before removes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const allocator = arena.allocator();
    const live = [_]LiveMember{
        .{ .id = 1, .address = "10.0.0.1:2881" },
        .{ .id = 2, .address = "10.0.0.2:2882" },
    };
    const target = try parseConfig(allocator, "server.2=10.0.0.2:2883,server.3=10.0.0.3:2883");
    const ops = try diffMembership(allocator, &live, target);
    try std.testing.expectEqual(@as(usize, 3), ops.len);
    try std.testing.expectEqual(@as(u64, 2), ops[0].update_address.id);
    try std.testing.expectEqual(@as(u64, 3), ops[1].add.id);
    try std.testing.expectEqual(@as(u64, 1), ops[2].remove.id);
}
