const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const zest = b.dependency("zest", .{});
    const system_rocksdb = b.option(
        bool,
        "system-rocksdb",
        "Link the quorum server against the system RocksDB library",
    ) orelse false;

    const linux_server = target.result.os.tag == .linux;
    const raftz_module = if (linux_server)
        b.dependency("raftz", .{
            .target = target,
            .optimize = optimize,
        }).module("raftz")
    else
        null;
    const rocksdb_modules: ?RocksDBModules = if (linux_server)
        if (system_rocksdb)
            addSystemRocksDB(b, target, optimize)
        else blk: {
            const dependency = b.dependency("rocksdb", .{
                .target = target,
                .optimize = optimize,
                .enable_snappy = true,
            });
            break :blk .{
                .bindings = dependency.module("bindings"),
                .c = dependency.module("rocksdb"),
            };
        }
    else
        null;
    const imports: []const std.Build.Module.Import = if (raftz_module) |raftz| &.{
        .{ .name = "raftz", .module = raftz },
        .{ .name = "rocksdb", .module = rocksdb_modules.?.bindings },
        .{ .name = "rocksdb_c", .module = rocksdb_modules.?.c },
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

        const upstream_server_suite = b.addSystemCommand(&.{"bash"});
        upstream_server_suite.addFileArg(b.path("tests/interop/java/run-upstream-server.sh"));
        upstream_server_suite.addArtifactArg(server);
        const test_upstream_server = b.step(
            "test-upstream-server",
            "Run compatible Apache ZooKeeper server behavior tests",
        );
        test_upstream_server.dependOn(&upstream_server_suite.step);

        const jepsen_smoke = b.addSystemCommand(&.{"bash"});
        jepsen_smoke.addFileArg(b.path("tests/jepsen/run.sh"));
        jepsen_smoke.addArtifactArg(server);
        const test_jepsen_smoke = b.step(
            "test-jepsen-smoke",
            "Run the Jepsen linearizable register smoke test",
        );
        test_jepsen_smoke.dependOn(&jepsen_smoke.step);
    }

    const check_zig = b.step("check-zig", "Compile native Zig tests without running them");
    check_zig.dependOn(&tests.step);
    const check_jute = b.step("check-jute", "Alias for check-zig");
    check_jute.dependOn(&tests.step);

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run native Zig unit tests");
    test_step.dependOn(&run_tests.step);
}

const RocksDBModules = struct {
    bindings: *std.Build.Module,
    c: *std.Build.Module,
};

fn addSystemRocksDB(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) RocksDBModules {
    const dependency = b.dependency("rocksdb", .{
        .target = target,
        .optimize = optimize,
    });
    const header = b.addWriteFiles().add(
        "system-rocksdb.h",
        "#include <rocksdb/c.h>\n",
    );
    const translate_c = b.addTranslateC(.{
        .root_source_file = header,
        .target = target,
        .optimize = optimize,
    });
    const c_module = b.createModule(.{
        .root_source_file = translate_c.getOutput(),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .link_libcpp = true,
    });
    c_module.linkSystemLibrary("rocksdb", .{});
    c_module.linkSystemLibrary("snappy", .{});

    const bindings_module = b.createModule(.{
        .root_source_file = dependency.path("src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });
    bindings_module.addImport("rocksdb", c_module);

    return .{
        .bindings = bindings_module,
        .c = c_module,
    };
}
