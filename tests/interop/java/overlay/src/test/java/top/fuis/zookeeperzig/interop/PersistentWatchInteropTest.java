package top.fuis.zookeeperzig.interop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PersistentWatchInteropTest extends ClientBase {
    private static final long EVENT_TIMEOUT_SECONDS = 30;

    private TestableZooKeeper zk;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        zk = createClient();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        if (zk != null) {
            zk.close();
        }
        super.tearDown();
    }

    @Test
    public void testPersistentWatchAndRemoval() throws Exception {
        RecordingWatcher watcher = new RecordingWatcher();
        zk.addWatch("/persistent", watcher, AddWatchMode.PERSISTENT);
        zk.create("/persistent", bytes("zero"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEvent(watcher, Watcher.Event.EventType.NodeCreated, "/persistent");
        zk.setData("/persistent", bytes("one"), -1);
        assertEvent(watcher, Watcher.Event.EventType.NodeDataChanged, "/persistent");
        zk.create("/persistent/child", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEvent(watcher, Watcher.Event.EventType.NodeChildrenChanged, "/persistent");
        zk.delete("/persistent/child", -1);
        assertEvent(watcher, Watcher.Event.EventType.NodeChildrenChanged, "/persistent");
        zk.delete("/persistent", -1);
        assertEvent(watcher, Watcher.Event.EventType.NodeDeleted, "/persistent");
        zk.create("/persistent", bytes("again"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEvent(watcher, Watcher.Event.EventType.NodeCreated, "/persistent");

        zk.removeWatches("/persistent", watcher, Watcher.WatcherType.Persistent, false);
        assertEvent(watcher, Watcher.Event.EventType.PersistentWatchRemoved, "/persistent");
        zk.setData("/persistent", bytes("ignored"), -1);
        assertNoEvent(watcher);
        assertThrows(
            KeeperException.NoWatcherException.class,
            () -> zk.removeAllWatches("/persistent", Watcher.WatcherType.Persistent, false));
    }

    @Test
    public void testPersistentRecursiveWatch() throws Exception {
        RecordingWatcher watcher = new RecordingWatcher();
        zk.addWatch("/recursive", watcher, AddWatchMode.PERSISTENT_RECURSIVE);
        zk.create("/recursive", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEvent(watcher, Watcher.Event.EventType.NodeCreated, "/recursive");
        zk.create("/recursive/child", bytes("zero"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEvent(watcher, Watcher.Event.EventType.NodeCreated, "/recursive/child");
        zk.setData("/recursive/child", bytes("one"), -1);
        assertEvent(watcher, Watcher.Event.EventType.NodeDataChanged, "/recursive/child");
        zk.delete("/recursive/child", -1);
        assertEvent(watcher, Watcher.Event.EventType.NodeDeleted, "/recursive/child");
        assertNoEvent(watcher);

        zk.removeAllWatches("/recursive", Watcher.WatcherType.PersistentRecursive, false);
        assertEvent(watcher, Watcher.Event.EventType.PersistentWatchRemoved, "/recursive");
        zk.create("/recursive/after", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertNoEvent(watcher);
    }

    @Test
    public void testMultiDeleteUsesDeletedNodeAcl() throws Exception {
        RecordingWatcher watcher = new RecordingWatcher();
        List<ACL> noReadAcl = Collections.singletonList(new ACL(
            ZooDefs.Perms.ALL & ~ZooDefs.Perms.READ,
            ZooDefs.Ids.ANYONE_ID_UNSAFE));
        zk.create("/multi-acl", bytes("value"), noReadAcl, CreateMode.PERSISTENT);
        zk.addWatch("/multi-acl", watcher, AddWatchMode.PERSISTENT);
        zk.multi(Collections.singletonList(Op.delete("/multi-acl", -1)));
        assertNoEvent(watcher);
    }

    @Test
    public void testAnyRemovalCoversEveryWatchMode() throws Exception {
        RecordingWatcher dataWatcher = new RecordingWatcher();
        RecordingWatcher childWatcher = new RecordingWatcher();
        RecordingWatcher persistentWatcher = new RecordingWatcher();
        zk.create("/remove-any", bytes("value"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertNotNull(zk.getData("/remove-any", dataWatcher, null));
        assertNotNull(zk.getChildren("/remove-any", childWatcher));
        zk.addWatch("/remove-any", persistentWatcher, AddWatchMode.PERSISTENT);

        zk.removeAllWatches("/remove-any", Watcher.WatcherType.Any, false);
        assertEvent(dataWatcher, Watcher.Event.EventType.DataWatchRemoved, "/remove-any");
        assertEvent(childWatcher, Watcher.Event.EventType.ChildWatchRemoved, "/remove-any");
        assertEvent(persistentWatcher, Watcher.Event.EventType.PersistentWatchRemoved, "/remove-any");
        zk.setData("/remove-any", bytes("ignored"), -1);
        assertNoEvent(dataWatcher);
        assertNoEvent(childWatcher);
        assertNoEvent(persistentWatcher);
    }

    @Test
    public void testRecursiveWatchUsesChrootRelativePath() throws Exception {
        RecordingWatcher watcher = new RecordingWatcher();
        zk.create("/watch-chroot", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        TestableZooKeeper chrootClient = createClient(hostPort + "/watch-chroot");
        try {
            chrootClient.addWatch("/", watcher, AddWatchMode.PERSISTENT_RECURSIVE);
            zk.create("/watch-chroot/child", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertEvent(watcher, Watcher.Event.EventType.NodeCreated, "/child");
        } finally {
            chrootClient.close();
        }
    }

    @Test
    public void testPersistentWatchRestoredAfterDisconnectedMutation() throws Exception {
        RecordingWatcher watcher = new RecordingWatcher();
        TestableZooKeeper writer = createClient();
        try {
            zk.create("/restored-persistent", bytes("before"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.addWatch("/restored-persistent", watcher, AddWatchMode.PERSISTENT);
            assertTrue(zk.pauseCnxn(1_000));
            waitForExists(zk, "/restored-persistent");
            writer.setData("/restored-persistent", bytes("after"), -1);
            assertEvent(watcher, Watcher.Event.EventType.NodeDataChanged, "/restored-persistent");
        } finally {
            writer.close();
        }
    }

    private static void waitForExists(TestableZooKeeper client, String path) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(EVENT_TIMEOUT_SECONDS);
        KeeperException.ConnectionLossException lastError = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                assertNotNull(client.exists(path, false));
                return;
            } catch (KeeperException.ConnectionLossException error) {
                lastError = error;
                Thread.sleep(100);
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw new AssertionError("timed out reconnecting");
    }

    private static void assertEvent(
        RecordingWatcher watcher,
        Watcher.Event.EventType type,
        String path) throws Exception {
        WatchedEvent event = watcher.events.poll(EVENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(event, "timed out waiting for " + type + " on " + path);
        assertEquals(type, event.getType());
        assertEquals(Watcher.Event.KeeperState.SyncConnected, event.getState());
        assertEquals(path, event.getPath());
    }

    private static void assertNoEvent(RecordingWatcher watcher) throws Exception {
        WatchedEvent event = watcher.events.poll(500, TimeUnit.MILLISECONDS);
        if (event != null) {
            throw new AssertionError("unexpected watch event: " + event);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class RecordingWatcher implements Watcher {
        private final BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() != Watcher.Event.EventType.None) {
                events.add(event);
            }
        }
    }
}
