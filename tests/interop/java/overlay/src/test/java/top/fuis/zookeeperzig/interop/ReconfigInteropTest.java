package top.fuis.zookeeperzig.interop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.ReconfigTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

public final class ReconfigInteropTest {
    private static final int SESSION_TIMEOUT_MILLIS = 15_000;
    private static final long OPERATION_TIMEOUT_MILLIS = 30_000;

    @TempDir
    Path testDirectory;

    @Test
    @Timeout(value = 180)
    public void officialAdminReplacesVotingMember() throws Exception {
        replaceVotingMember(false);
    }

    @Test
    @Timeout(value = 180)
    public void testBulkReconfig() throws Exception {
        replaceVotingMember(true);
    }

    @Test
    @Timeout(value = 180)
    public void testFreshObserverDoesNotVote() throws Exception {
        try (ZigQuorumProcess cluster = ZigQuorumProcess.start(
                 System.getProperty("zookeeper.zig.server"), testDirectory, OPERATION_TIMEOUT_MILLIS);
             ZooKeeperAdmin admin = connectAdmin(cluster.nodeConnectString(0))) {
            createEventually(admin, "/before-observer");
            Stat before = new Stat();
            admin.getConfig(false, before);
            String observer = cluster.memberConfig(3).replace(":participant", ":observer");
            List<String> target = Arrays.asList(cluster.memberConfig(0), cluster.memberConfig(1), observer);
            Stat joined = new Stat();
            assertEquals(String.join("\n", target),
                text(admin.reconfigure(null, null, target, before.getVersion(), joined)));
            assertEquals(before.getVersion() + 1, joined.getVersion());
            cluster.stopNode(2);
            cluster.startJoiningNode(3, 0, 1);

            try (ZooKeeper reader = connect(cluster.nodeConnectString(3))) {
                awaitExists(reader, "/before-observer");
                ReconfigTest.testServerHasConfig(reader, target, Collections.singletonList("3"));
                ReconfigTest.testNormalOperation(admin, reader);
            }

            // With only two voters, the live learner must not replace either
            // vote. Check sustained unavailability, not one transient failure.
            cluster.stopNode(1);
            Thread.sleep(5_000);
            assertTrue(cluster.isNodeAlive(3), "observer exited before quorum check");
            assertCannotConnect(cluster.nodeConnectString(0));
            assertTrue(cluster.isNodeAlive(3), "observer exited during quorum check");
            cluster.startNode(1);
            try (ZooKeeper writer = connect(cluster.nodeConnectString(0));
                 ZooKeeper reader = connect(cluster.nodeConnectString(3))) {
                createEventually(writer, "/after-observer-quorum-recovery");
                awaitExists(reader, "/after-observer-quorum-recovery");
                ReconfigTest.testNormalOperation(writer, reader);
                ReconfigTest.testServerHasConfig(reader, target, Collections.singletonList("3"));
            }
        }
    }

    private void replaceVotingMember(boolean bulk) throws Exception {
        String binary = System.getProperty("zookeeper.zig.server");
        try (ZigQuorumProcess cluster = ZigQuorumProcess.start(
                 binary,
                 testDirectory,
                 OPERATION_TIMEOUT_MILLIS)) {
            String nodeOne = cluster.nodeConnectString(0);
            String nodeFourConfig = cluster.memberConfig(3);
            Stat initialStat = new Stat();

            try (ZooKeeper reader = connect(nodeOne)) {
                String initialConfig = text(reader.getData("/zookeeper/config", false, initialStat));
                assertTrue(initialConfig.contains("server.1="), initialConfig);
                assertTrue(initialConfig.contains("server.2="), initialConfig);
                assertTrue(initialConfig.contains("server.3="), initialConfig);
            }

            Stat removedStat = new Stat();
            try (ZooKeeperAdmin admin = connectAdmin(nodeOne)) {
                String removedConfig = text(admin.reconfigure(
                    null,
                    bulk ? null : Collections.singletonList("3"),
                    bulk ? Arrays.asList(cluster.memberConfig(0), cluster.memberConfig(1)) : null,
                    initialStat.getVersion(),
                    removedStat));
                assertTrue(removedConfig.contains("server.1="), removedConfig);
                assertTrue(removedConfig.contains("server.2="), removedConfig);
                assertTrue(!removedConfig.contains("server.3="), removedConfig);
                assertEquals(initialStat.getVersion() + 1, removedStat.getVersion());

                // Both stale and future CAS must leave bytes/stat unchanged,
                // not merely report an error while publishing a new target.
                for (long invalidVersion : new long[] {removedStat.getVersion() - 1L, removedStat.getVersion() + 1L}) {
                    assertBadVersionEventually(admin, nodeOne,
                        bulk ? null : Collections.singletonList(nodeFourConfig),
                        bulk ? Arrays.asList(cluster.memberConfig(0), cluster.memberConfig(1), nodeFourConfig) : null,
                        invalidVersion, removedConfig, removedStat);
                }
            }

            cluster.stopNode(1);
            Thread.sleep(5_000);
            assertTrue(cluster.isNodeAlive(2), "removed node exited before quorum check");
            assertCannotConnect(nodeOne);
            assertTrue(cluster.isNodeAlive(2), "removed node exited during quorum check");
            cluster.startNode(1);

            try (ZooKeeper writer = connect(nodeOne)) {
                createEventually(writer, "/after-removal");
            }

            cluster.stopNode(2);
            Stat joinedStat = new Stat();
            try (ZooKeeperAdmin admin = connectAdmin(nodeOne)) {
                String joinedConfig = text(admin.reconfigure(
                    bulk ? null : Collections.singletonList(nodeFourConfig),
                    null,
                    bulk ? Arrays.asList(cluster.memberConfig(0), cluster.memberConfig(1), nodeFourConfig) : null,
                    removedStat.getVersion(),
                    joinedStat));
                assertTrue(joinedConfig.contains(nodeFourConfig), joinedConfig);
                assertEquals(removedStat.getVersion() + 1, joinedStat.getVersion());
            }
            cluster.startJoiningNode(3, 0, 1);

            try (ZooKeeper verifier = connect(cluster.nodeConnectString(3))) {
                awaitExists(verifier, "/after-removal");
            }

            try (ZooKeeper writer = connect(nodeOne)) {
                cluster.stopNode(1);
                createEventually(writer, "/after-replacement");
            }
            try (ZooKeeper verifier = connect(cluster.nodeConnectString(3));
                 ZooKeeper writer = connect(nodeOne)) {
                awaitExists(verifier, "/after-replacement");
                // Execute the pinned upstream helper and its assertions unchanged.
                ReconfigTest.testNormalOperation(writer, verifier);
                ReconfigTest.testServerHasConfig(verifier,
                    Arrays.asList(cluster.memberConfig(0), cluster.memberConfig(1), nodeFourConfig),
                    Collections.singletonList("3"));
            }
        }
    }

    @Test
    @Timeout(value = 150)
    public void testRemoveOneAsynchronous() throws Exception {
        try (ZigQuorumProcess cluster = ZigQuorumProcess.start(
                 System.getProperty("zookeeper.zig.server"), testDirectory, OPERATION_TIMEOUT_MILLIS);
             ZooKeeperAdmin admin = connectAdmin(cluster.nodeConnectString(0))) {
            Stat before = new Stat();
            admin.getConfig(false, before);
            Object context = new Object();
            CompletableFuture<ReconfigResult> completed = new CompletableFuture<>();
            admin.reconfigure(null, Collections.singletonList("3"), null, before.getVersion(),
                (rc, path, ctx, data, stat) -> completed.complete(new ReconfigResult(rc, path, ctx, data, stat)),
                context);
            ReconfigResult result = completed.get(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            assertEquals(KeeperException.Code.OK.intValue(), result.rc);
            assertEquals("/zookeeper/config", result.path);
            assertSame(context, result.context, "callback context must be preserved");
            assertNotNull(result.stat);
            String removed = text(result.data);
            assertEquals(cluster.memberConfig(0) + "\n" + cluster.memberConfig(1), removed);
            assertEquals(before.getVersion() + 1, result.stat.getVersion());
            assertConfigUnchanged(admin, removed, result.stat);

            CompletableFuture<ReconfigResult> rejected = new CompletableFuture<>();
            admin.reconfigure(Collections.singletonList(cluster.memberConfig(3)), null, null, before.getVersion(),
                (rc, path, ctx, data, stat) -> rejected.complete(new ReconfigResult(rc, path, ctx, data, stat)),
                context);
            ReconfigResult stale = rejected.get(OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            assertEquals(KeeperException.Code.BADVERSION.intValue(), stale.rc);
            assertEquals("/zookeeper/config", stale.path);
            assertSame(context, stale.context);
            assertNull(stale.data);
            assertNull(stale.stat);
            assertConfigUnchanged(admin, removed, result.stat);
            createEventually(admin, "/after-async-removal");
            cluster.stopNode(1);
            Thread.sleep(5_000);
            assertTrue(cluster.isNodeAlive(2), "removed node exited before async quorum check");
            assertCannotConnect(cluster.nodeConnectString(0));
            assertTrue(cluster.isNodeAlive(2), "removed node exited during async quorum check");
            cluster.startNode(1);
            try (ZooKeeper writer = connect(cluster.nodeConnectString(0));
                 ZooKeeper reader = connect(cluster.nodeConnectString(1))) {
                ReconfigTest.testNormalOperation(writer, reader);
            }
        }
    }

    private static final class ReconfigResult {
        private final int rc;
        private final String path;
        private final Object context;
        private final byte[] data;
        private final Stat stat;

        private ReconfigResult(int rc, String path, Object context, byte[] data, Stat stat) {
            this.rc = rc;
            this.path = path;
            this.context = context;
            this.data = data;
            this.stat = stat;
        }
    }

    @Test
    @Timeout(value = 120)
    public void testRoleChangeRejectedWithoutConfigMutation() throws Exception {
        try (ZigQuorumProcess cluster = ZigQuorumProcess.start(
                 System.getProperty("zookeeper.zig.server"),
                 testDirectory,
                 OPERATION_TIMEOUT_MILLIS);
             ZooKeeperAdmin admin = connectAdmin(cluster.nodeConnectString(0))) {
            Stat before = new Stat();
            String config = text(admin.getConfig(false, before));
            String observer = cluster.memberConfig(2).replace(":participant", ":observer");
            List<String> unchanged = Arrays.asList(
                cluster.memberConfig(0), cluster.memberConfig(1), cluster.memberConfig(2));
            List<String> bulk = Arrays.asList(
                cluster.memberConfig(0), cluster.memberConfig(1), observer);
            // Include an unchanged participant: the existing incremental parser
            // rejects an observer-only joining list even before role validation.
            List<String> incremental = Arrays.asList(cluster.memberConfig(0), observer);
            assertThrows(KeeperException.BadArgumentsException.class,
                () -> admin.reconfigure(incremental, null, null, -1, new Stat()));
            assertConfigUnchanged(admin, config, before);
            assertThrows(KeeperException.BadArgumentsException.class,
                () -> admin.reconfigure(null, null, bulk, before.getVersion(), new Stat()));
            assertConfigUnchanged(admin, config, before);

            Stat accepted = new Stat();
            assertEquals(config, text(admin.reconfigure(null, null, unchanged, before.getVersion(), accepted)));
            assertEquals(before.getVersion() + 1, accepted.getVersion());
            // Node 3 must still vote: with node 2 down, nodes 1+3 can commit.
            cluster.stopNode(1);
            createEventually(admin, "/after-rejected-role-change");
        }
    }

    @Test
    @Timeout(value = 120)
    public void testPublicConfigPoisonCannotChangeMembership() throws Exception {
        try (ZigQuorumProcess cluster = ZigQuorumProcess.start(
                 System.getProperty("zookeeper.zig.server"), testDirectory, OPERATION_TIMEOUT_MILLIS);
             ZooKeeperAdmin admin = connectAdmin(cluster.nodeConnectString(0))) {
            String baseline = text(admin.getConfig(false, new Stat()));
            List<String> members = Arrays.asList(
                cluster.memberConfig(0), cluster.memberConfig(1), cluster.memberConfig(2));
            List<String> demoted = Arrays.asList(cluster.memberConfig(0), cluster.memberConfig(1),
                cluster.memberConfig(2).replace(":participant", ":observer"));
            for (boolean useMulti : new boolean[] {false, true}) {
                for (String poison : Arrays.asList(String.join("\n", demoted), "not-a-config")) {
                    byte[] data = poison.getBytes(StandardCharsets.UTF_8);
                    if (useMulti) {
                        admin.multi(Collections.singletonList(Op.setData("/zookeeper/config", data, -1)));
                    } else {
                        admin.setData("/zookeeper/config", data, -1);
                    }
                    Stat poisoned = new Stat();
                    assertEquals(poison, text(admin.getConfig(false, poisoned)));
                    assertThrows(KeeperException.BadArgumentsException.class,
                        () -> admin.reconfigure(null, null, demoted, poisoned.getVersion(), new Stat()));
                    assertConfigUnchanged(admin, poison, poisoned);
                    Stat repaired = new Stat();
                    assertEquals(baseline,
                        text(admin.reconfigure(null, null, members, poisoned.getVersion(), repaired)));
                    assertEquals(poisoned.getVersion() + 1, repaired.getVersion());
                    assertConfigUnchanged(admin, baseline, repaired);
                }
            }
            admin.delete("/zookeeper/config", -1);
            assertNull(admin.exists("/zookeeper/config", false));
            assertThrows(KeeperException.BadArgumentsException.class,
                () -> admin.reconfigure(null, null, demoted, -1, new Stat()));
            assertNull(admin.exists("/zookeeper/config", false), "rejection must not recreate public node");
            Stat repaired = new Stat();
            assertEquals(baseline, text(admin.reconfigure(null, null, members, -1, repaired)));
            assertEquals(0, repaired.getVersion());
            // A poisoned/deleted public node did not demote voter 3.
            cluster.stopNode(1);
            createEventually(admin, "/after-public-config-repair");
        }
    }

    private static void assertBadVersionEventually(
        ZooKeeperAdmin admin, String endpoint, List<String> joining, List<String> newMembers,
        long invalidVersion, String expectedConfig, Stat expectedStat) throws Exception {
        // Capture the target/version and baseline once. Positive reconfigs do
        // not use this helper. Async waits bound even a stalled client request.
        assertBadVersionEventually(endpoint, invalidVersion,
            TimeUnit.MILLISECONDS.toNanos(OPERATION_TIMEOUT_MILLIS), System::nanoTime,
            remaining -> {
                CompletableFuture<ReconfigResult> completed = new CompletableFuture<>();
                admin.reconfigure(joining, null, newMembers, invalidVersion,
                    (rc, path, ctx, data, stat) -> completed.complete(new ReconfigResult(rc, path, ctx, data, stat)), null);
                requireOk(completed.get(remaining, TimeUnit.NANOSECONDS));
            },
            remaining -> {
                CompletableFuture<ReconfigResult> completed = new CompletableFuture<>();
                admin.getConfig(false,
                    (rc, path, ctx, data, stat) -> completed.complete(new ReconfigResult(rc, path, ctx, data, stat)), null);
                ReconfigResult result = completed.get(remaining, TimeUnit.NANOSECONDS);
                requireOk(result);
                assertArrayEquals(expectedConfig.getBytes(StandardCharsets.UTF_8), result.data,
                    "negative CAS changed config bytes");
                assertEquals(expectedStat, result.stat, "negative CAS changed config Stat");
            });
    }

    private static void requireOk(ReconfigResult result) throws KeeperException {
        if (result.rc != KeeperException.Code.OK.intValue()) {
            throw KeeperException.create(KeeperException.Code.get(result.rc));
        }
    }

    @FunctionalInterface
    private interface BoundedAction {
        void run(long remainingNanos) throws Exception;
    }

    private static void assertBadVersionEventually(
        String endpoint, long invalidVersion, long timeoutNanos, LongSupplier clock,
        BoundedAction request, BoundedAction verifyConfig) throws Exception {
        final long deadline = clock.getAsLong() + timeoutNanos;
        for (int attempt = 1; ; attempt++) {
            String context = "negative CAS invalidVersion=" + invalidVersion
                + " attempt=" + attempt + " endpoint=" + endpoint;
            boolean rejected = false;
            try {
                request.run(remainingNanos(deadline, clock, context));
                throw new AssertionError(context + " unexpectedly succeeded");
            } catch (KeeperException.BadVersionException expected) {
                rejected = true;
            } catch (KeeperException.ConnectionLossException transientFailure) {
                // An ambiguous reply never counts as the expected rejection.
            } catch (Exception unexpected) {
                throw new AssertionError(context + " unexpected request error", unexpected);
            }
            // Check the same baseline after any recoverable read, including
            // after ambiguous replies. Read retries never resubmit the CAS.
            for (int readAttempt = 1; ; readAttempt++) {
                String readContext = context + " readAttempt=" + readAttempt;
                try {
                    verifyConfig.run(remainingNanos(deadline, clock, readContext));
                    remainingNanos(deadline, clock, readContext);
                    break;
                } catch (KeeperException.ConnectionLossException transientFailure) {
                    pauseBeforeRetry(deadline, clock, readContext);
                } catch (Exception | AssertionError unexpected) {
                    throw new AssertionError(readContext + " config verification failed", unexpected);
                }
            }
            if (rejected) return;
            pauseBeforeRetry(deadline, clock, context);
        }
    }

    private static long remainingNanos(long deadline, LongSupplier clock, String context) {
        long remaining = deadline - clock.getAsLong();
        if (remaining <= 0) throw new AssertionError(context + " deadline expired");
        return remaining;
    }

    private static void pauseBeforeRetry(long deadline, LongSupplier clock, String context) throws InterruptedException {
        TimeUnit.NANOSECONDS.sleep(Math.min(TimeUnit.MILLISECONDS.toNanos(100),
            remainingNanos(deadline, clock, context)));
    }

    @Test
    @Timeout(value = 5)
    public void testNegativeCasRetriesOnlyConnectionLoss() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        AtomicInteger reads = new AtomicInteger();
        assertBadVersionEventually("scripted:1", 7, TimeUnit.SECONDS.toNanos(1), () -> 0,
            remaining -> {
                if (requests.incrementAndGet() == 1) throw new KeeperException.ConnectionLossException();
                throw new KeeperException.BadVersionException();
            },
            remaining -> {
                if (reads.incrementAndGet() == 1) throw new KeeperException.ConnectionLossException();
            });
        assertEquals(2, requests.get());
        assertEquals(3, reads.get());
    }

    @Test
    @Timeout(value = 5)
    public void testNegativeCasFailsOnSuccessOrUnexpectedError() {
        for (boolean succeeds : new boolean[] {false, true}) {
            AssertionError failure = assertThrows(AssertionError.class,
                () -> assertBadVersionEventually("scripted:2", 9, 10, () -> 0,
                    remaining -> {
                        if (!succeeds) throw new KeeperException.BadArgumentsException();
                    }, remaining -> { throw new AssertionError("must not read after request failure"); }));
            assertTrue(failure.getMessage().contains("invalidVersion=9 attempt=1 endpoint=scripted:2"));
            assertTrue(failure.getMessage().contains(succeeds ? "unexpectedly succeeded" : "unexpected request error"));
        }
    }

    @Test
    @Timeout(value = 5)
    public void testNegativeCasConnectionLossExpiresWithoutResettingDeadline() {
        for (boolean readFails : new boolean[] {false, true}) {
            AtomicLong now = new AtomicLong();
            AtomicInteger requests = new AtomicInteger();
            AssertionError failure = assertThrows(AssertionError.class,
                () -> assertBadVersionEventually("scripted:3", 11, 10, now::get,
                    remaining -> {
                        requests.incrementAndGet();
                        if (readFails) throw new KeeperException.BadVersionException();
                        now.addAndGet(6);
                        throw new KeeperException.ConnectionLossException();
                    }, remaining -> {
                        if (readFails) {
                            now.addAndGet(6);
                            throw new KeeperException.ConnectionLossException();
                        }
                    }));
            int expectedAttempts = readFails ? 1 : 2;
            assertEquals(expectedAttempts, requests.get());
            assertTrue(failure.getMessage().contains("invalidVersion=11 attempt=" + expectedAttempts + " endpoint=scripted:3"));
            Throwable detail = failure.getCause() == null ? failure : failure.getCause();
            assertTrue(detail.getMessage().contains("deadline expired"));
        }
    }

    @Test
    @Timeout(value = 5)
    public void testNegativeCasFailsOnMutationOrUnexpectedReadError() {
        for (boolean mutation : new boolean[] {false, true}) {
            AssertionError failure = assertThrows(AssertionError.class,
                () -> assertBadVersionEventually("scripted:4", 13, 10, () -> 0,
                    remaining -> { throw new KeeperException.BadVersionException(); },
                    remaining -> {
                        if (mutation) throw new AssertionError("config Stat changed");
                        throw new KeeperException.NoNodeException();
                    }));
            assertTrue(failure.getMessage().contains("invalidVersion=13 attempt=1 endpoint=scripted:4"));
            assertTrue(failure.getMessage().contains("config verification failed"));
        }
    }

    private static void assertConfigUnchanged(ZooKeeper client, String expected, Stat before) throws Exception {
        Stat after = new Stat();
        assertEquals(expected, text(client.getConfig(false, after)));
        assertEquals(before, after, "config stat must not change on rejection");
    }

    private static ZooKeeper connect(String connectString) throws Exception {
        ConnectedWatcher watcher = new ConnectedWatcher();
        ZooKeeper client = new ZooKeeper(connectString, SESSION_TIMEOUT_MILLIS, watcher);
        try {
            watcher.awaitConnected();
            return client;
        } catch (Exception error) {
            client.close();
            throw error;
        }
    }

    private static ZooKeeperAdmin connectAdmin(String connectString) throws Exception {
        ConnectedWatcher watcher = new ConnectedWatcher();
        ZooKeeperAdmin client = new ZooKeeperAdmin(
            connectString,
            SESSION_TIMEOUT_MILLIS,
            watcher);
        try {
            watcher.awaitConnected();
            return client;
        } catch (Exception error) {
            client.close();
            throw error;
        }
    }

    private static void assertCannotConnect(String connectString) throws Exception {
        ConnectedWatcher watcher = new ConnectedWatcher();
        try (ZooKeeper client = new ZooKeeper(
                 connectString,
                 SESSION_TIMEOUT_MILLIS,
                 watcher)) {
            assertTrue(
                !watcher.await(15_000),
                "removed member still formed a quorum for a new session");
        }
    }

    private static void createEventually(ZooKeeper client, String path) throws Exception {
        long deadline = System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(OPERATION_TIMEOUT_MILLIS);
        KeeperException lastError = null;
        while (System.nanoTime() < deadline) {
            try {
                client.create(
                    path,
                    path.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
                return;
            } catch (KeeperException.NodeExistsException ignored) {
                return;
            } catch (KeeperException error) {
                lastError = error;
                Thread.sleep(100);
            }
        }
        throw new AssertionError("timed out creating " + path, lastError);
    }

    private static void awaitExists(ZooKeeper client, String path) throws Exception {
        long deadline = System.nanoTime()
            + TimeUnit.MILLISECONDS.toNanos(OPERATION_TIMEOUT_MILLIS);
        KeeperException lastError = null;
        while (System.nanoTime() < deadline) {
            try {
                if (client.exists(path, false) != null) {
                    return;
                }
            } catch (KeeperException error) {
                lastError = error;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("timed out waiting for " + path, lastError);
    }

    private static String text(byte[] bytes) {
        assertNotNull(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final class ConnectedWatcher implements Watcher {
        private final CountDownLatch connected = new CountDownLatch(1);

        @Override
        public void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        }

        private boolean await(long timeoutMillis) throws InterruptedException {
            return connected.await(timeoutMillis, TimeUnit.MILLISECONDS);
        }

        private void awaitConnected() throws InterruptedException {
            assertTrue(
                await(OPERATION_TIMEOUT_MILLIS),
                "timed out waiting for ZooKeeper connection");
        }
    }
}
