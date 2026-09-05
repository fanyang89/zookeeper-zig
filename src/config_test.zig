//! Focused test root for zoo.cfg parsing without quorum integration tests.

test "zoo.cfg configuration suite" {
    _ = @import("server/config.zig");
}
