//! ThreadSanitizer test root for project-owned concurrent paths.
//!
//! Keep this dependency closure free of C++ libraries. Raft, grpc-lite, and
//! RocksDB have their own upstream concurrency testing boundaries.

const builtin = @import("builtin");

comptime {
    if (builtin.os.tag != .linux) @compileError("ThreadSanitizer tests require Linux");
}

test {
    _ = @import("server/worker_pool.zig");
    _ = @import("server/watch.zig");
    _ = @import("client/async.zig");
    _ = @import("client/tcp_transport.zig");
}
