const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const module = b.addModule("btree", .{
        .root_source_file = b.path("./src/lib.zig"),
        .target = target,
        .optimize = optimize,
    });

    const unit_tests = b.addTest(.{
        .root_module = module,
        .use_llvm = true,
    });

    const library = b.addLibrary(.{
        .name = "btree",
        .root_module = module,
    });
    b.default_step.dependOn(&library.step);
    b.installArtifact(library);

    const run_unit_tests = b.addRunArtifact(unit_tests);
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&run_unit_tests.step);
}
