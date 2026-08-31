const std = @import("std");

pub fn WorkerPool(
    comptime Job: type,
    comptime Context: type,
    comptime handle: *const fn (Context, Job) void,
) type {
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        io: std.Io,
        worker_count: usize,
        buffer: []Job,
        queue: std.Io.Queue(Job),
        workers: std.Io.Group = .init,
        started: bool = false,
        closed: bool = false,
        stopped: bool = false,

        pub fn init(
            allocator: std.mem.Allocator,
            io: std.Io,
            worker_count: usize,
            queue_capacity: usize,
        ) !Self {
            if (worker_count == 0) return error.InvalidWorkerCount;
            if (queue_capacity == 0) return error.InvalidQueueCapacity;
            const buffer = try allocator.alloc(Job, queue_capacity);
            return .{
                .allocator = allocator,
                .io = io,
                .worker_count = worker_count,
                .buffer = buffer,
                .queue = .init(buffer),
            };
        }

        pub fn deinit(self: *Self) void {
            if (!self.stopped) {
                self.close();
                self.awaitDrained() catch {
                    self.workers.cancel(self.io);
                    self.stopped = true;
                };
            }
            self.allocator.free(self.buffer);
            self.* = undefined;
        }

        pub fn start(self: *Self, context: Context) !void {
            if (self.started) return;
            if (self.closed) return error.WorkerPoolClosed;
            errdefer {
                self.close();
                self.workers.cancel(self.io);
                self.stopped = true;
            }
            for (0..self.worker_count) |_| {
                try self.workers.concurrent(self.io, workerMain, .{ self, context });
            }
            self.started = true;
        }

        pub fn submit(self: *Self, job: Job) !void {
            if (!self.started) return error.WorkerPoolNotStarted;
            return self.queue.putOne(self.io, job);
        }

        pub fn trySubmit(self: *Self, job: Job) !bool {
            if (!self.started) return error.WorkerPoolNotStarted;
            return try self.queue.put(self.io, &.{job}, 0) == 1;
        }

        pub fn close(self: *Self) void {
            if (self.closed) return;
            self.queue.close(self.io);
            self.closed = true;
        }

        pub fn awaitDrained(self: *Self) std.Io.Cancelable!void {
            if (self.stopped) return;
            try self.workers.await(self.io);
            self.stopped = true;
        }

        pub fn shutdown(self: *Self) std.Io.Cancelable!void {
            self.close();
            try self.awaitDrained();
        }

        fn workerMain(self: *Self, context: Context) std.Io.Cancelable!void {
            while (true) {
                const job = self.queue.getOne(self.io) catch |err| switch (err) {
                    error.Closed => return,
                    error.Canceled => return error.Canceled,
                };
                handle(context, job);
            }
        }
    };
}

const TestJob = struct {
    id: u8,
    block: bool = false,
};

const TestContext = struct {
    io: std.Io,
    entered: std.Io.Semaphore = .{},
    release: std.Io.Semaphore = .{},
    order: [4]u8 = undefined,
    count: usize = 0,
};

fn handleTestJob(context: *TestContext, job: TestJob) void {
    if (job.block) {
        context.entered.post(context.io);
        context.release.waitUncancelable(context.io);
    }
    context.order[context.count] = job.id;
    context.count += 1;
}

const TestPool = WorkerPool(TestJob, *TestContext, handleTestJob);

const SubmitContext = struct {
    pool: *TestPool,
    io: std.Io,
    entered: *std.Io.Semaphore,
    job: TestJob,
};

fn submitTestJob(context: *SubmitContext) !void {
    context.entered.post(context.io);
    try context.pool.submit(context.job);
}

const DrainContext = struct {
    pool: *TestPool,
    io: std.Io,
    entered: *std.Io.Semaphore,
    done: *std.atomic.Value(bool),
};

fn awaitTestDrain(context: *DrainContext) !void {
    context.entered.post(context.io);
    try context.pool.awaitDrained();
    context.done.store(true, .release);
}

test "worker pool applies backpressure at capacity one" {
    const testing = std.testing;
    var context = TestContext{ .io = testing.io };
    var pool = try TestPool.init(testing.allocator, testing.io, 1, 1);
    defer pool.deinit();
    try pool.start(&context);

    try pool.submit(.{ .id = 1, .block = true });
    try context.entered.wait(testing.io);
    try pool.submit(.{ .id = 2 });
    try testing.expect(!(try pool.trySubmit(.{ .id = 3 })));

    context.release.post(testing.io);
    try pool.submit(.{ .id = 3 });
    try pool.shutdown();
    try testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, context.order[0..context.count]);
}

test "worker pool cancels a producer waiting on a full queue" {
    const testing = std.testing;
    var context = TestContext{ .io = testing.io };
    var pool = try TestPool.init(testing.allocator, testing.io, 1, 1);
    defer pool.deinit();
    try pool.start(&context);

    try pool.submit(.{ .id = 1, .block = true });
    try context.entered.wait(testing.io);
    try pool.submit(.{ .id = 2 });

    var submit_entered: std.Io.Semaphore = .{};
    var submit_context = SubmitContext{
        .pool = &pool,
        .io = testing.io,
        .entered = &submit_entered,
        .job = .{ .id = 3 },
    };
    var submit_future = try testing.io.concurrent(submitTestJob, .{&submit_context});
    try submit_entered.wait(testing.io);
    try testing.expectError(error.Canceled, submit_future.cancel(testing.io));

    context.release.post(testing.io);
    try pool.shutdown();
    try testing.expectEqualSlices(u8, &.{ 1, 2 }, context.order[0..context.count]);
}

test "worker pool shutdown drains queued jobs in order" {
    const testing = std.testing;
    var context = TestContext{ .io = testing.io };
    var pool = try TestPool.init(testing.allocator, testing.io, 1, 2);
    defer pool.deinit();
    try pool.start(&context);

    try pool.submit(.{ .id = 1, .block = true });
    try context.entered.wait(testing.io);
    try pool.submit(.{ .id = 2 });
    try pool.submit(.{ .id = 3 });
    pool.close();
    try testing.expectError(error.Closed, pool.submit(.{ .id = 4 }));

    var drain_entered: std.Io.Semaphore = .{};
    var drain_done = std.atomic.Value(bool).init(false);
    var drain_context = DrainContext{
        .pool = &pool,
        .io = testing.io,
        .entered = &drain_entered,
        .done = &drain_done,
    };
    var drain_future = try testing.io.concurrent(awaitTestDrain, .{&drain_context});
    try drain_entered.wait(testing.io);
    try testing.expect(!drain_done.load(.acquire));

    context.release.post(testing.io);
    try drain_future.await(testing.io);
    try testing.expect(drain_done.load(.acquire));
    try testing.expectEqualSlices(u8, &.{ 1, 2, 3 }, context.order[0..context.count]);
}
