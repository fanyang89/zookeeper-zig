const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const zest = b.dependency("zest", .{});

    const linux_server = target.result.os.tag == .linux;
    const raftz_module = if (linux_server)
        b.dependency("raftz", .{
            .target = target,
            .optimize = optimize,
        }).module("raftz")
    else
        null;
    const rocksdb_dependency = if (linux_server) b.dependency("rocksdb", .{
        .target = target,
        .optimize = optimize,
        .enable_snappy = true,
    }) else null;
    const imports: []const std.Build.Module.Import = if (raftz_module) |raftz| &.{
        .{ .name = "raftz", .module = raftz },
        .{ .name = "rocksdb", .module = rocksdb_dependency.?.module("bindings") },
        .{ .name = "rocksdb_c", .module = rocksdb_dependency.?.module("rocksdb") },
    } else &.{};

    const zookeeper_module = b.addModule("zookeeper", .{
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
    const tests = b.addTest(.{
        .root_module = test_module,
        .test_runner = .{
            .path = zest.path("src/root.zig"),
            .mode = .simple,
        },
    });

    if (raftz_module) |module| {
        const server_module = b.createModule(.{
            .root_source_file = b.path("src/server/main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "zookeeper", .module = zookeeper_module },
                .{ .name = "raftz", .module = module },
            },
        });
        const server = b.addExecutable(.{
            .name = "zookeeper-quorum-server",
            .root_module = server_module,
        });
        b.installArtifact(server);

        const java_interop = b.addSystemCommand(&.{"bash"});
        java_interop.addFileArg(b.path("tests/interop/java/run.sh"));
        java_interop.addArtifactArg(server);
        const test_java_interop = b.step(
            "test-interop-java",
            "Test the server with the official Apache ZooKeeper Java client",
        );
        test_java_interop.dependOn(&java_interop.step);
    }

    const check_zig = b.step("check-zig", "Compile native Zig tests without running them");
    check_zig.dependOn(&tests.step);
    const check_jute = b.step("check-jute", "Alias for check-zig");
    check_jute.dependOn(&tests.step);

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run native Zig unit tests");
    test_step.dependOn(&run_tests.step);
}
