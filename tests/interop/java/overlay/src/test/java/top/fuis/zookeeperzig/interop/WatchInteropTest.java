package top.fuis.zookeeperzig.interop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WatchInteropTest extends ClientBase {
    private static final long EVENT_TIMEOUT_SECONDS = 30;

    private final RecordingWatcher watcher = new RecordingWatcher();
    private TestableZooKeeper zk;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();
        zk = new TestableZooKeeper(hostPort, CONNECTION_TIMEOUT, watcher);
        assertEquals(
            Watcher.Event.KeeperState.SyncConnected,
            watcher.states.poll(EVENT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
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
    public void testCoreOneShotWatches() throws Exception {
        zk.create("/data", bytes("zero"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertArrayEquals(bytes("zero"), zk.getData("/data", watcher, null));
        zk.setData("/data", bytes("one"), -1);
        assertEvent(Watcher.Event.EventType.NodeDataChanged, "/data");
        zk.setData("/data", bytes("two"), -1);
        assertNull(watcher.events.poll(300, TimeUnit.MILLISECONDS));

        assertNull(zk.exists("/missing", watcher));
        zk.create("/missing", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEvent(Watcher.Event.EventType.NodeCreated, "/missing");

        zk.create("/parent", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertTrue(zk.getChildren("/parent", watcher).isEmpty());
        zk.create("/parent/child", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        assertEvent(Watcher.Event.EventType.NodeChildrenChanged, "/parent");

        zk.create("/deleted", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.exists("/deleted", watcher);
        zk.getChildren("/deleted", watcher);
        zk.delete("/deleted", -1);
        assertEvent(Watcher.Event.EventType.NodeDeleted, "/deleted");
        assertNull(watcher.events.poll(300, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testMultiAndSessionCloseWatches() throws Exception {
        RecordingWatcher multiWatcher = new RecordingWatcher();
        assertNull(zk.exists("/multi-watch", multiWatcher));
        assertTrue(zk.getChildren("/", multiWatcher).isEmpty());
        zk.multi(java.util.Arrays.asList(
            Op.create("/multi-watch", bytes("created"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.setData("/multi-watch", bytes("updated"), -1)));
        assertEvent(multiWatcher, Watcher.Event.EventType.NodeCreated, "/multi-watch");
        assertEvent(multiWatcher, Watcher.Event.EventType.NodeChildrenChanged, "/");
        assertNull(multiWatcher.events.poll(300, TimeUnit.MILLISECONDS));

        TestableZooKeeper owner = createClient();
        try {
            owner.create("/ephemeral-watch", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            assertTrue(zk.exists("/ephemeral-watch", multiWatcher) != null);
            owner.close();
            assertEvent(multiWatcher, Watcher.Event.EventType.NodeDeleted, "/ephemeral-watch");
        } finally {
            owner.close();
        }
    }

    @Test
    public void testWatchRestoredAfterDisconnectedMutation() throws Exception {
        TestableZooKeeper writer = createClient();
        try {
            zk.create("/reconnect", bytes("before"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            assertArrayEquals(bytes("before"), zk.getData("/reconnect", watcher, null));

            assertTrue(zk.pauseCnxn(1_000));
            writer.setData("/reconnect", bytes("after"), -1);
            assertEvent(Watcher.Event.EventType.NodeDataChanged, "/reconnect");
        } finally {
            writer.close();
        }
    }

    private void assertEvent(Watcher.Event.EventType type, String path) throws Exception {
        assertEvent(watcher, type, path);
    }

    private void assertEvent(
        RecordingWatcher source,
        Watcher.Event.EventType type,
        String path) throws Exception {
        WatchedEvent event = source.events.poll(EVENT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertNotNull(event, "timed out waiting for " + type + " on " + path);
        assertEquals(type, event.getType());
        assertEquals(Watcher.Event.KeeperState.SyncConnected, event.getState());
        assertEquals(path, event.getPath());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class RecordingWatcher implements Watcher {
        private final BlockingQueue<WatchedEvent> events = new LinkedBlockingQueue<>();
        private final BlockingQueue<Watcher.Event.KeeperState> states = new LinkedBlockingQueue<>();

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Watcher.Event.EventType.None) {
                states.add(event.getState());
            } else {
                events.add(event);
            }
        }
    }
}
