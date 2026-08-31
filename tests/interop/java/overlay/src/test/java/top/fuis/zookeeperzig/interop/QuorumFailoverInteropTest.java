package top.fuis.zookeeperzig.interop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

public final class QuorumFailoverInteropTest {
    private static final int SESSION_TIMEOUT_MILLIS = 15_000;
    private static final long OPERATION_TIMEOUT_MILLIS = 30_000;

    @TempDir
    Path testDirectory;

    @Test
    @Timeout(value = 180)
    public void officialClientPreservesSessionAcrossRollingFailures() throws Exception {
        String binary = System.getProperty("zookeeper.zig.server");
        try (ZigQuorumProcess cluster = ZigQuorumProcess.start(
                 binary,
                 testDirectory,
                 OPERATION_TIMEOUT_MILLIS)) {
            SessionWatcher watcher = new SessionWatcher();
            ZooKeeper client = new ZooKeeper(
                cluster.connectString(),
                SESSION_TIMEOUT_MILLIS,
                watcher);
            String ephemeral = "/quorum-ephemeral";
            try {
                watcher.awaitConnected(1, OPERATION_TIMEOUT_MILLIS);
                long sessionId = client.getSessionId();
                byte[] stable = "stable".getBytes(StandardCharsets.UTF_8);
                byte[] credentials = "quorum:secret".getBytes(StandardCharsets.UTF_8);

                client.addAuthInfo("digest", credentials);
                client.create(
                    "/quorum-root",
                    stable,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
                client.create(
                    ephemeral,
                    new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
                client.create(
                    "/quorum-secure",
                    stable,
                    ZooDefs.Ids.CREATOR_ALL_ACL,
                    CreateMode.PERSISTENT);

                for (int index = 0; index < 3; index++) {
                    cluster.stopNode(index);
                    awaitOperational(client, "/quorum-root", OPERATION_TIMEOUT_MILLIS);
                    assertEquals(sessionId, client.getSessionId());
                    assertNotNull(client.exists(ephemeral, false));
                    assertArrayEquals(stable, awaitData(
                        client,
                        "/quorum-secure",
                        OPERATION_TIMEOUT_MILLIS));

                    String marker = "/during-node-" + (index + 1) + "-restart";
                    createEventually(client, marker, OPERATION_TIMEOUT_MILLIS);
                    cluster.startNode(index);
                    verifyNodeCaughtUp(cluster.nodeConnectString(index), marker);
                }

                assertTrue(watcher.disconnectedCount() > 0, "client never observed a server disconnect");
                assertTrue(watcher.connectedCount() > 1, "client never established a replacement connection");
            } finally {
                client.close();
            }

            SessionWatcher observerWatcher = new SessionWatcher();
            try (ZooKeeper observer = new ZooKeeper(
                     cluster.connectString(),
                     SESSION_TIMEOUT_MILLIS,
                     observerWatcher)) {
                observerWatcher.awaitConnected(1, OPERATION_TIMEOUT_MILLIS);
                awaitMissing(observer, ephemeral, OPERATION_TIMEOUT_MILLIS);
            }
        }
    }

    private static void verifyNodeCaughtUp(String connectString, String marker) throws Exception {
        SessionWatcher watcher = new SessionWatcher();
        try (ZooKeeper verifier = new ZooKeeper(
                 connectString,
                 SESSION_TIMEOUT_MILLIS,
                 watcher)) {
            watcher.awaitConnected(1, OPERATION_TIMEOUT_MILLIS);
            awaitOperational(verifier, marker, OPERATION_TIMEOUT_MILLIS);
            assertNotNull(verifier.exists(marker, false));
        }
    }

    private static void createEventually(ZooKeeper client, String path, long timeoutMillis)
        throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            try {
                if (client.exists(path, false) != null) {
                    return;
                }
                client.create(
                    path,
                    path.getBytes(StandardCharsets.UTF_8),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
                return;
            } catch (KeeperException error) {
                if (error.code() == KeeperException.Code.NODEEXISTS) {
                    return;
                }
                if (!isTransient(error.code())) {
                    throw error;
                }
            }
            Thread.sleep(50);
        }
        assertNotNull(client.exists(path, false), "create did not complete: " + path);
    }

    private static byte[] awaitData(ZooKeeper client, String path, long timeoutMillis)
        throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            try {
                return client.getData(path, false, null);
            } catch (KeeperException error) {
                if (!isTransient(error.code())) {
                    throw error;
                }
            }
            Thread.sleep(50);
        }
        return client.getData(path, false, null);
    }

    private static void awaitOperational(ZooKeeper client, String path, long timeoutMillis)
        throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            try {
                if (client.exists(path, false) != null) {
                    return;
                }
            } catch (KeeperException error) {
                if (!isTransient(error.code())) {
                    throw error;
                }
            }
            Thread.sleep(50);
        }
        assertNotNull(client.exists(path, false), "cluster did not become operational");
    }

    private static void awaitMissing(ZooKeeper client, String path, long timeoutMillis)
        throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            try {
                if (client.exists(path, false) == null) {
                    return;
                }
            } catch (KeeperException error) {
                if (!isTransient(error.code())) {
                    throw error;
                }
            }
            Thread.sleep(50);
        }
        assertNull(client.exists(path, false), "ephemeral node survived session close");
    }

    private static boolean isTransient(KeeperException.Code code) {
        return code == KeeperException.Code.CONNECTIONLOSS
            || code == KeeperException.Code.OPERATIONTIMEOUT
            || code == KeeperException.Code.SESSIONMOVED;
    }

    private static final class SessionWatcher implements Watcher {
        private int connectedCount;
        private int disconnectedCount;

        @Override
        public synchronized void process(WatchedEvent event) {
            if (event.getState() == Event.KeeperState.SyncConnected) {
                connectedCount++;
            } else if (event.getState() == Event.KeeperState.Disconnected) {
                disconnectedCount++;
            }
            notifyAll();
        }

        synchronized void awaitConnected(int minimumCount, long timeoutMillis)
            throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (connectedCount < minimumCount) {
                long remainingNanos = deadline - System.nanoTime();
                if (remainingNanos <= 0) {
                    throw new AssertionError("ZooKeeper client did not connect");
                }
                TimeUnit.NANOSECONDS.timedWait(this, remainingNanos);
            }
        }

        synchronized int connectedCount() {
            return connectedCount;
        }

        synchronized int disconnectedCount() {
            return disconnectedCount;
        }
    }
}
