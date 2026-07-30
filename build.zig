const std = @import("std");

const version = std.SemanticVersion{ .major = 3, .minor = 9, .patch = 5 };

const common_sources = &.{
    "src/zookeeper.c",
    "src/recordio.c",
    "src/zk_log.c",
    "src/zk_hashtable.c",
    "src/addrvec.c",
    "src/hashtable/hashtable.c",
    "src/hashtable/hashtable_itr.c",
};

const common_flags = &.{
    "-std=gnu11",
    "-fno-strict-aliasing",
};

const Features = struct {
    tls: bool,
    sasl: bool,
    sanitizer: Sanitizer,
    fuzz: bool,
    coverage: bool,
    clang_runtime_dir: ?[]const u8,
};

const Sanitizer = enum {
    none,
    address,
    undefined,
    thread,
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});
    const features = Features{
        .tls = b.option(bool, "tls", "Build OpenSSL support") orelse true,
        .sasl = b.option(bool, "sasl", "Build Cyrus SASL support") orelse true,
        .sanitizer = b.option(Sanitizer, "sanitize", "Enable address, undefined, or thread sanitizer") orelse .none,
        .fuzz = b.option(bool, "fuzz", "Build libFuzzer fuzz targets") orelse false,
        .coverage = b.option(bool, "coverage", "Build with Clang source-based coverage instrumentation") orelse false,
        .clang_runtime_dir = b.option([]const u8, "clang-runtime-dir", "Clang compiler-rt directory (see: clang --print-runtime-dir)"),
    };

    if (target.result.os.tag != .linux) {
        @panic("zookeeper-c currently supports Linux targets only");
    }

    const config_header = addConfigHeader(b);

    if (features.fuzz) {
        addFuzzStep(b, target, optimize, config_header, features);
        return;
    }

    const mt_static = addZooKeeperLibrary(b, target, optimize, config_header, features, true, .static);
    const mt_shared = addZooKeeperLibrary(b, target, optimize, config_header, features, true, .dynamic);
    const st_static = addZooKeeperLibrary(b, target, optimize, config_header, features, false, .static);
    const st_shared = addZooKeeperLibrary(b, target, optimize, config_header, features, false, .dynamic);

    b.installArtifact(mt_static);
    b.installArtifact(mt_shared);
    b.installArtifact(st_static);
    b.installArtifact(st_shared);

    const cli_mt = addTool(b, "cli_mt", "src/cli.c", target, optimize, features, true, mt_static);
    const cli_st = addTool(b, "cli_st", "src/cli.c", target, optimize, features, false, st_static);
    const load_gen = addTool(b, "load_gen", "src/load_gen.c", target, optimize, features, true, mt_static);
    b.installArtifact(cli_mt);
    b.installArtifact(cli_st);
    b.installArtifact(load_gen);

    installHeaders(b);

    const test_step = b.step("test", "Run C client unit tests");
    const unit_mt = addTestExecutable(b, "unit_mt", "tests/unit.c", target, optimize, features, true, mt_static);
    const unit_st = addTestExecutable(b, "unit_st", "tests/unit.c", target, optimize, features, false, st_static);
    test_step.dependOn(&b.addRunArtifact(unit_mt).step);
    test_step.dependOn(&b.addRunArtifact(unit_st).step);

    const e2e_step = b.step("e2e", "Run C client end-to-end tests");
    const e2e_mt = addTestExecutable(b, "e2e_mt", "tests/e2e.c", target, optimize, features, true, mt_static);
    const e2e_st = addTestExecutable(b, "e2e_st", "tests/e2e.c", target, optimize, features, false, st_static);
    const run_e2e_mt = b.addRunArtifact(e2e_mt);
    const run_e2e_st = b.addRunArtifact(e2e_st);
    if (b.args) |args| {
        run_e2e_mt.addArgs(args);
        run_e2e_st.addArgs(args);
    }
    e2e_step.dependOn(&run_e2e_mt.step);
    e2e_step.dependOn(&run_e2e_st.step);

    const chaos_step = b.step("chaos", "Build chaos test executables");
    const chaos_mt = addTestExecutable(b, "chaos_mt", "tests/chaos.c", target, optimize, features, true, mt_static);
    chaos_step.dependOn(&b.addInstallArtifact(chaos_mt, .{}).step);
}

fn addFuzzStep(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    config_header: *std.Build.Step.ConfigHeader,
    features: Features,
) void {
    const fuzz_step = b.step("fuzz", "Build and run libFuzzer targets");
    const mt_static = addZooKeeperLibrary(b, target, optimize, config_header, features, true, .static);

    const targets = .{
        .{ "fuzz_jute", "tests/fuzz/jute.c" },
    };

    inline for (targets) |t| {
        const exe = addFuzzTarget(b, t[0], t[1], target, optimize, features, mt_static);
        const run = b.addRunArtifact(exe);
        run.addArg(b.fmt("tests/fuzz/corpus/{s}", .{t[0]}));
        if (b.args) |args| run.addArgs(args);
        fuzz_step.dependOn(&run.step);
    }
}

fn addFuzzTarget(
    b: *std.Build,
    name: []const u8,
    source: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    features: Features,
    library: *std.Build.Step.Compile,
) *std.Build.Step.Compile {
    const module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    applyInstrumentation(module, features, true);
    addIncludes(b, module);
    module.addCMacro("USE_STATIC_LIB", "1");
    module.addCMacro("THREADED", "1");
    if (features.tls) module.addCMacro("HAVE_OPENSSL_H", "1");
    if (features.sasl) module.addCMacro("HAVE_CYRUS_SASL_H", "1");
    module.addCSourceFile(.{
        .file = b.path(source),
        .flags = cFlags(features.sanitizer, features.fuzz, features.coverage),
    });
    module.linkLibrary(library);
    addSystemLibraries(module, features, true);

    const exe = b.addExecutable(.{ .name = name, .root_module = module });
    if (features.coverage) exe.forceUndefinedSymbol("__llvm_profile_runtime");
    return exe;
}

fn addConfigHeader(b: *std.Build) *std.Build.Step.ConfigHeader {
    return b.addConfigHeader(.{
        .style = .{ .cmake = b.path("vendor/zookeeper-client-c/cmake_config.h.in") },
        .include_path = "config.h",
    }, .{
        .HAVE_ARPA_INET_H = true,
        .HAVE_DLFCN_H = true,
        .HAVE_FCNTL_H = true,
        .HAVE_GENERATED_ZOOKEEPER_JUTE_C = true,
        .HAVE_GENERATED_ZOOKEEPER_JUTE_H = true,
        .HAVE_GETCWD = true,
        .HAVE_GETHOSTBYNAME = true,
        .HAVE_GETHOSTNAME = true,
        .HAVE_GETLOGIN = true,
        .HAVE_GETPWUID_R = true,
        .HAVE_GETTIMEOFDAY = true,
        .HAVE_GETUID = true,
        .HAVE_INTTYPES_H = true,
        .HAVE_LIBRT = true,
        .HAVE_MEMMOVE = true,
        .HAVE_MEMORY_H = true,
        .HAVE_MEMSET = true,
        .HAVE_NETDB_H = true,
        .HAVE_NETINET_IN_H = true,
        .HAVE_POLL = true,
        .HAVE_SOCKET = true,
        .HAVE_STDINT_H = true,
        .HAVE_STDLIB_H = true,
        .HAVE_STRCHR = true,
        .HAVE_STRDUP = true,
        .HAVE_STRERROR = true,
        .HAVE_STRINGS_H = true,
        .HAVE_STRING_H = true,
        .HAVE_STRTOL = true,
        .HAVE_SYS_SOCKET_H = true,
        .HAVE_SYS_STAT_H = true,
        .HAVE_SYS_TIME_H = true,
        .HAVE_SYS_TYPES_H = true,
        .HAVE_SYS_UTSNAME_H = true,
        .HAVE_UNISTD_H = true,
        .ZOO_IPV6_ENABLED = true,
        .SOCK_CLOEXEC_ENABLED = false,
        .PROJECT_NAME = "zookeeper",
        .email = "user@zookeeper.apache.org",
        .description = "zookeeper C client",
        .PROJECT_VERSION = "3.9.5",
    });
}

fn addZooKeeperLibrary(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    config_header: *std.Build.Step.ConfigHeader,
    features: Features,
    threaded: bool,
    linkage: std.builtin.LinkMode,
) *std.Build.Step.Compile {
    const name = if (threaded) "zookeeper_mt" else "zookeeper_st";
    const module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
    });
    configureClientModule(b, module, config_header, features, threaded, linkage == .dynamic);

    return b.addLibrary(.{
        .name = name,
        .linkage = linkage,
        .root_module = module,
        .version = version,
    });
}

fn configureClientModule(
    b: *std.Build,
    module: *std.Build.Module,
    config_header: *std.Build.Step.ConfigHeader,
    features: Features,
    threaded: bool,
    link_system_libraries: bool,
) void {
    applyInstrumentation(module, features, link_system_libraries);
    module.addConfigHeader(config_header);
    addIncludes(b, module);
    module.addCMacro("USE_STATIC_LIB", "1");
    if (threaded) module.addCMacro("THREADED", "1");
    if (features.tls) module.addCMacro("HAVE_OPENSSL_H", "1");
    if (features.sasl) module.addCMacro("HAVE_CYRUS_SASL_H", "1");

    module.addCSourceFiles(.{
        .root = b.path("vendor/zookeeper-client-c"),
        .files = common_sources,
        .flags = cFlags(features.sanitizer, features.fuzz, features.coverage),
    });
    module.addCSourceFile(.{
        .file = b.path("generated/zookeeper.jute.c"),
        .flags = cFlags(features.sanitizer, features.fuzz, features.coverage),
    });
    module.addCSourceFile(.{
        .file = b.path(if (threaded)
            "vendor/zookeeper-client-c/src/mt_adaptor.c"
        else
            "vendor/zookeeper-client-c/src/st_adaptor.c"),
        .flags = cFlags(features.sanitizer, features.fuzz, features.coverage),
    });
    if (features.sasl) {
        module.addCSourceFile(.{
            .file = b.path("vendor/zookeeper-client-c/src/zk_sasl.c"),
            .flags = cFlags(features.sanitizer, features.fuzz, features.coverage),
        });
    }
    if (link_system_libraries) addSystemLibraries(module, features, threaded);
}

fn addTool(
    b: *std.Build,
    name: []const u8,
    source: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    features: Features,
    threaded: bool,
    library: *std.Build.Step.Compile,
) *std.Build.Step.Compile {
    const module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    applyInstrumentation(module, features, true);
    addIncludes(b, module);
    module.addCMacro("USE_STATIC_LIB", "1");
    if (threaded) module.addCMacro("THREADED", "1");
    if (features.tls) module.addCMacro("HAVE_OPENSSL_H", "1");
    if (features.sasl) module.addCMacro("HAVE_CYRUS_SASL_H", "1");
    module.addCSourceFile(.{
        .file = b.path(b.fmt("vendor/zookeeper-client-c/{s}", .{source})),
        .flags = cFlags(features.sanitizer, features.fuzz, features.coverage),
    });
    module.linkLibrary(library);
    addSystemLibraries(module, features, threaded);

    const exe = b.addExecutable(.{ .name = name, .root_module = module });
    if (features.coverage) exe.forceUndefinedSymbol("__llvm_profile_runtime");
    return exe;
}

fn addTestExecutable(
    b: *std.Build,
    name: []const u8,
    source: []const u8,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    features: Features,
    threaded: bool,
    library: *std.Build.Step.Compile,
) *std.Build.Step.Compile {
    const module = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    applyInstrumentation(module, features, true);
    addIncludes(b, module);
    module.addCMacro("USE_STATIC_LIB", "1");
    if (threaded) module.addCMacro("THREADED", "1");
    if (features.tls) module.addCMacro("HAVE_OPENSSL_H", "1");
    if (features.sasl) module.addCMacro("HAVE_CYRUS_SASL_H", "1");
    module.addCSourceFile(.{
        .file = b.path(source),
        .flags = cFlags(features.sanitizer, features.fuzz, features.coverage),
    });
    module.linkLibrary(library);
    addSystemLibraries(module, features, threaded);

    const exe = b.addExecutable(.{ .name = name, .root_module = module });
    if (features.coverage) exe.forceUndefinedSymbol("__llvm_profile_runtime");
    return exe;
}

fn addIncludes(b: *std.Build, module: *std.Build.Module) void {
    module.addIncludePath(b.path("vendor/zookeeper-client-c/include"));
    module.addIncludePath(b.path("vendor/zookeeper-client-c/src"));
    module.addIncludePath(b.path("vendor/zookeeper-client-c/src/hashtable"));
    module.addIncludePath(b.path("generated"));
}

fn applyInstrumentation(module: *std.Build.Module, features: Features, link_runtime: bool) void {
    switch (features.sanitizer) {
        .none => {},
        .address => {
            module.omit_frame_pointer = false;
            if (link_runtime) linkClangRuntime(module, features, "clang_rt.asan");
        },
        .undefined => module.sanitize_c = .full,
        .thread => module.sanitize_thread = true,
    }
    if (features.fuzz and link_runtime)
        linkClangRuntime(module, features, "clang_rt.fuzzer");
    if (features.coverage and link_runtime)
        linkClangRuntime(module, features, "clang_rt.profile");
}

fn linkClangRuntime(module: *std.Build.Module, features: Features, name: []const u8) void {
    const dir = features.clang_runtime_dir orelse
        @panic("-Dclang-runtime-dir is required (run: clang --print-runtime-dir)");
    module.addLibraryPath(.{ .cwd_relative = dir });
    module.linkSystemLibrary(name, .{});
}

fn cFlags(sanitizer: Sanitizer, fuzz: bool, coverage: bool) []const []const u8 {
    if (coverage)
        return &.{ "-std=gnu11", "-fno-strict-aliasing", "-fprofile-instr-generate", "-fcoverage-mapping" };
    if (sanitizer == .address and fuzz)
        return &.{ "-std=gnu11", "-fno-strict-aliasing", "-fsanitize=address", "-fno-omit-frame-pointer", "-fsanitize=fuzzer-no-link" };
    if (sanitizer == .address)
        return &.{ "-std=gnu11", "-fno-strict-aliasing", "-fsanitize=address", "-fno-omit-frame-pointer" };
    if (fuzz)
        return &.{ "-std=gnu11", "-fno-strict-aliasing", "-fsanitize=fuzzer-no-link" };
    return common_flags;
}

fn addSystemLibraries(module: *std.Build.Module, features: Features, threaded: bool) void {
    module.linkSystemLibrary("m", .{});
    module.linkSystemLibrary("rt", .{});
    if (threaded) module.linkSystemLibrary("pthread", .{});
    if (features.tls) {
        module.linkSystemLibrary("ssl", .{});
        module.linkSystemLibrary("crypto", .{});
    }
    if (features.sasl) module.linkSystemLibrary("sasl2", .{});
}

fn installHeaders(b: *std.Build) void {
    const headers = .{
        "proto.h",
        "recordio.h",
        "zookeeper.h",
        "zookeeper_log.h",
        "zookeeper_version.h",
    };
    inline for (headers) |header| {
        b.getInstallStep().dependOn(&b.addInstallHeaderFile(
            b.path("vendor/zookeeper-client-c/include/" ++ header),
            "zookeeper/" ++ header,
        ).step);
    }
    b.getInstallStep().dependOn(&b.addInstallHeaderFile(
        b.path("generated/zookeeper.jute.h"),
        "zookeeper/zookeeper.jute.h",
    ).step);
}
