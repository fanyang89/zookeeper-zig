const builtin = @import("builtin");

pub const client = @import("client.zig");
pub const jute = @import("jute.zig");
pub const protocol = @import("protocol.zig");
pub const server = if (builtin.os.tag == .linux)
    @import("server.zig")
else
    @import("server_unsupported.zig");
pub const wire = @import("wire.zig");

test {
    _ = client.BlockingClient;
    _ = jute.Reader;
    _ = jute.Writer;
    _ = protocol.proto.ConnectRequest;
    _ = server;
    _ = wire.OpCode;
}
