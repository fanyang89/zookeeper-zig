const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const raftz_module = if (target.result.os.tag == .linux)
        b.dependency("raftz", .{
            .target = target,
            .optimize = optimize,
        }).module("raftz")
    else
        null;
    const imports: []const std.Build.Module.Import = if (raftz_module) |module| &.{.{
        .name = "raftz",
        .module = module,
    }} else &.{};

    _ = b.addModule("zookeeper", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = imports,
    });
    _ = b.addModule("jute", .{
        .root_source_file = b.path("src/jute.zig"),
        .target = target,
        .optimize = optimize,
    });

    const test_module = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = imports,
    });
    const tests = b.addTest(.{ .root_module = test_module });

    const check_zig = b.step("check-zig", "Compile native Zig tests without running them");
    check_zig.dependOn(&tests.step);
    const check_jute = b.step("check-jute", "Alias for check-zig");
    check_jute.dependOn(&tests.step);

    const run_tests = b.addRunArtifact(tests);
    const test_zig = b.step("test-zig", "Run native Zig unit tests");
    test_zig.dependOn(&run_tests.step);
    const test_jute = b.step("test-jute", "Alias for test-zig");
    test_jute.dependOn(&run_tests.step);
    const test_step = b.step("test", "Run native Zig unit tests");
    test_step.dependOn(&run_tests.step);
}
