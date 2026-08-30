const data = @import("data.zig");

pub const TxnDigest = struct {
    version: i32,
    treeDigest: i64,
};

pub const TxnHeader = struct {
    clientId: i64,
    cxid: i32,
    zxid: i64,
    time: i64,
    type: i32,
};

pub const CreateTxnV0 = struct {
    path: ?[]const u8,
    data: ?[]const u8,
    acl: ?[]const data.ACL,
    ephemeral: bool,
};

pub const CreateTxn = struct {
    path: ?[]const u8,
    data: ?[]const u8,
    acl: ?[]const data.ACL,
    ephemeral: bool,
    parentCVersion: i32,
};

pub const CreateTTLTxn = struct {
    path: ?[]const u8,
    data: ?[]const u8,
    acl: ?[]const data.ACL,
    parentCVersion: i32,
    ttl: i64,
};

pub const CreateContainerTxn = struct {
    path: ?[]const u8,
    data: ?[]const u8,
    acl: ?[]const data.ACL,
    parentCVersion: i32,
};

pub const DeleteTxn = struct {
    path: ?[]const u8,
};

pub const SetDataTxn = struct {
    path: ?[]const u8,
    data: ?[]const u8,
    version: i32,
};

pub const CheckVersionTxn = struct {
    path: ?[]const u8,
    version: i32,
};

pub const SetACLTxn = struct {
    path: ?[]const u8,
    acl: ?[]const data.ACL,
    version: i32,
};

pub const SetMaxChildrenTxn = struct {
    path: ?[]const u8,
    max: i32,
};

pub const CreateSessionTxn = struct {
    timeOut: i32,
};

pub const CloseSessionTxn = struct {
    paths2Delete: ?[]const ?[]const u8,
};

pub const ErrorTxn = struct {
    err: i32,
};

pub const Txn = struct {
    type: i32,
    data: ?[]const u8,
};

pub const MultiTxn = struct {
    txns: ?[]const Txn,
};
