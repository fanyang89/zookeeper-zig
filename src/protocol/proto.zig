const data = @import("data.zig");

pub const ConnectRequest = struct {
    protocolVersion: i32,
    lastZxidSeen: i64,
    timeOut: i32,
    sessionId: i64,
    passwd: ?[]const u8,
    readOnly: bool,
};

pub const ConnectResponse = struct {
    protocolVersion: i32,
    timeOut: i32,
    sessionId: i64,
    passwd: ?[]const u8,
    readOnly: bool,
};

pub const SetWatches = struct {
    relativeZxid: i64,
    dataWatches: ?[]const ?[]const u8,
    existWatches: ?[]const ?[]const u8,
    childWatches: ?[]const ?[]const u8,
};

pub const SetWatches2 = struct {
    relativeZxid: i64,
    dataWatches: ?[]const ?[]const u8,
    existWatches: ?[]const ?[]const u8,
    childWatches: ?[]const ?[]const u8,
    persistentWatches: ?[]const ?[]const u8,
    persistentRecursiveWatches: ?[]const ?[]const u8,
};

pub const RequestHeader = struct {
    xid: i32,
    type: i32,
};

pub const MultiHeader = struct {
    type: i32,
    done: bool,
    err: i32,
};

pub const AuthPacket = struct {
    type: i32,
    scheme: ?[]const u8,
    auth: ?[]const u8,
};

pub const ReplyHeader = struct {
    xid: i32,
    zxid: i64,
    err: i32,
};

pub const GetDataRequest = struct {
    path: ?[]const u8,
    watch: bool,
};

pub const SetDataRequest = struct {
    path: ?[]const u8,
    data: ?[]const u8,
    version: i32,
};

pub const ReconfigRequest = struct {
    joiningServers: ?[]const u8,
    leavingServers: ?[]const u8,
    newMembers: ?[]const u8,
    curConfigId: i64,
};

pub const SetDataResponse = struct {
    stat: data.Stat,
};

pub const GetSASLRequest = struct {
    token: ?[]const u8,
};

pub const SetSASLRequest = struct {
    token: ?[]const u8,
};

pub const SetSASLResponse = struct {
    token: ?[]const u8,
};

pub const CreateRequest = struct {
    path: ?[]const u8,
    data: ?[]const u8,
    acl: ?[]const data.ACL,
    flags: i32,
};

pub const CreateTTLRequest = struct {
    path: ?[]const u8,
    data: ?[]const u8,
    acl: ?[]const data.ACL,
    flags: i32,
    ttl: i64,
};

pub const DeleteRequest = struct {
    path: ?[]const u8,
    version: i32,
};

pub const GetChildrenRequest = struct {
    path: ?[]const u8,
    watch: bool,
};

pub const GetAllChildrenNumberRequest = struct {
    path: ?[]const u8,
};

pub const GetChildren2Request = struct {
    path: ?[]const u8,
    watch: bool,
};

pub const CheckVersionRequest = struct {
    path: ?[]const u8,
    version: i32,
};

pub const GetMaxChildrenRequest = struct {
    path: ?[]const u8,
};

pub const GetMaxChildrenResponse = struct {
    max: i32,
};

pub const SetMaxChildrenRequest = struct {
    path: ?[]const u8,
    max: i32,
};

pub const SyncRequest = struct {
    path: ?[]const u8,
};

pub const SyncResponse = struct {
    path: ?[]const u8,
};

pub const GetACLRequest = struct {
    path: ?[]const u8,
};

pub const SetACLRequest = struct {
    path: ?[]const u8,
    acl: ?[]const data.ACL,
    version: i32,
};

pub const SetACLResponse = struct {
    stat: data.Stat,
};

pub const AddWatchRequest = struct {
    path: ?[]const u8,
    mode: i32,
};

pub const WatcherEvent = struct {
    type: i32,
    state: i32,
    path: ?[]const u8,
};

pub const ErrorResponse = struct {
    err: i32,
};

pub const CreateResponse = struct {
    path: ?[]const u8,
};

pub const Create2Response = struct {
    path: ?[]const u8,
    stat: data.Stat,
};

pub const ExistsRequest = struct {
    path: ?[]const u8,
    watch: bool,
};

pub const ExistsResponse = struct {
    stat: data.Stat,
};

pub const GetDataResponse = struct {
    data: ?[]const u8,
    stat: data.Stat,
};

pub const GetChildrenResponse = struct {
    children: ?[]const ?[]const u8,
};

pub const GetAllChildrenNumberResponse = struct {
    totalNumber: i32,
};

pub const GetChildren2Response = struct {
    children: ?[]const ?[]const u8,
    stat: data.Stat,
};

pub const GetACLResponse = struct {
    acl: ?[]const data.ACL,
    stat: data.Stat,
};

pub const CheckWatchesRequest = struct {
    path: ?[]const u8,
    type: i32,
};

pub const RemoveWatchesRequest = struct {
    path: ?[]const u8,
    type: i32,
};

pub const GetEphemeralsRequest = struct {
    prefixPath: ?[]const u8,
};

pub const GetEphemeralsResponse = struct {
    ephemerals: ?[]const ?[]const u8,
};

pub const WhoAmIResponse = struct {
    clientInfo: ?[]const data.ClientInfo,
};
