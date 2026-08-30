pub const Id = struct {
    scheme: ?[]const u8,
    id: ?[]const u8,
};

pub const ACL = struct {
    perms: i32,
    id: Id,
};

pub const Stat = struct {
    czxid: i64,
    mzxid: i64,
    ctime: i64,
    mtime: i64,
    version: i32,
    cversion: i32,
    aversion: i32,
    ephemeralOwner: i64,
    dataLength: i32,
    numChildren: i32,
    pzxid: i64,
};

pub const StatPersisted = struct {
    czxid: i64,
    mzxid: i64,
    ctime: i64,
    mtime: i64,
    version: i32,
    cversion: i32,
    aversion: i32,
    ephemeralOwner: i64,
    pzxid: i64,
};

pub const ClientInfo = struct {
    authScheme: ?[]const u8,
    user: ?[]const u8,
};
