package top.fuis.zookeeperzig.interop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.admin.ZooKeeperAdmin;
import org.apache.zookeeper.data.Stat;
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
                    Collections.singletonList("3"),
                    null,
                    initialStat.getVersion(),
                    removedStat));
                assertTrue(removedConfig.contains("server.1="), removedConfig);
                assertTrue(removedConfig.contains("server.2="), removedConfig);
                assertTrue(!removedConfig.contains("server.3="), removedConfig);
                assertEquals(initialStat.getVersion() + 1, removedStat.getVersion());

                assertThrows(
                    KeeperException.BadVersionException.class,
                    () -> admin.reconfigure(
                        Collections.singletonList(nodeFourConfig),
                        null,
                        null,
                        removedStat.getVersion() + 1L,
                        new Stat()));
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
                    Collections.singletonList(nodeFourConfig),
                    null,
                    null,
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
            try (ZooKeeper verifier = connect(cluster.nodeConnectString(3))) {
                awaitExists(verifier, "/after-replacement");
            }
        }
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
