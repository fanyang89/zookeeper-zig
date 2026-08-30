package top.fuis.zookeeperzig.interop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExtendedTypesInteropTest extends ClientBase {
    private TestableZooKeeper zk;

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        System.setProperty("zookeeper.extendedTypesEnabled", "true");
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
        System.clearProperty("zookeeper.extendedTypesEnabled");
    }

    @Test
    public void testContainerAndTtlLifecycle() throws Exception {
        Stat containerStat = new Stat();
        zk.create(
            "/container",
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.CONTAINER,
            containerStat);
        assertEquals(0, containerStat.getEphemeralOwner());
        zk.create(
            "/container/child",
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        zk.delete("/container/child", -1);
        awaitMissing("/container", 10_000);

        Stat ttlStat = new Stat();
        zk.create(
            "/ttl",
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT_WITH_TTL,
            ttlStat,
            250);
        assertEquals(0, ttlStat.getEphemeralOwner());
        awaitMissing("/ttl", 10_000);

        String sequential = zk.create(
            "/ttl-",
            new byte[0],
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL,
            new Stat(),
            250);
        assertTrue(sequential.matches("/ttl-[0-9]{10}"), sequential);
        awaitMissing(sequential, 10_000);
    }

    private void awaitMissing(String path, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (zk.exists(path, false) == null) {
                return;
            }
            Thread.sleep(50);
        }
        assertNull(zk.exists(path, false), "node was not cleaned up: " + path);
    }
}
