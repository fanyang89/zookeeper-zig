const data = @import("data.zig");

pub const LearnerInfo = struct {
    serverid: i64,
    protocolVersion: i32,
    configVersion: i64,
};

pub const QuorumPacket = struct {
    type: i32,
    zxid: i64,
    data: ?[]const u8,
    authinfo: ?[]const data.Id,
};

pub const QuorumAuthPacket = struct {
    magic: i64,
    status: i32,
    token: ?[]const u8,
};
