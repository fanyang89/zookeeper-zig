package top.fuis.zookeeperzig.interop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.CreateOptions;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.OpResult;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MultiInteropTest extends ClientBase {
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
    public void testAtomicCreateSetAndCheck() throws Exception {
        List<OpResult> results = zk.multi(Arrays.asList(
            Op.create("/multi", bytes("first"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.setData("/multi", bytes("second"), 0),
            Op.check("/multi", 1)));

        assertTrue(results.get(0) instanceof OpResult.CreateResult);
        assertTrue(results.get(1) instanceof OpResult.SetDataResult);
        OpResult.SetDataResult setData = (OpResult.SetDataResult) results.get(1);
        assertEquals(1, setData.getStat().getVersion());
        assertTrue(results.get(2) instanceof OpResult.CheckResult);
        assertArrayEquals(bytes("second"), zk.getData("/multi", false, null));
    }

    @Test
    public void testRollbackResults() throws Exception {
        zk.create("/kept", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        KeeperException failure = assertThrows(KeeperException.class, () -> zk.multi(Arrays.asList(
            Op.create("/rolled-back", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT),
            Op.setData("/missing", bytes("failure"), -1),
            Op.delete("/kept", -1))));

        assertEquals(KeeperException.Code.NONODE, failure.code());
        List<OpResult> results = failure.getResults();
        assertEquals(3, results.size());
        for (OpResult result : results) {
            assertTrue(result instanceof OpResult.ErrorResult);
        }
        assertEquals(
            KeeperException.Code.OK.intValue(),
            ((OpResult.ErrorResult) results.get(0)).getErr());
        assertEquals(
            KeeperException.Code.NONODE.intValue(),
            ((OpResult.ErrorResult) results.get(1)).getErr());
        assertEquals(
            KeeperException.Code.RUNTIMEINCONSISTENCY.intValue(),
            ((OpResult.ErrorResult) results.get(2)).getErr());
        assertNull(zk.exists("/rolled-back", false));
        assertNotNull(zk.exists("/kept", false));
    }

    @Test
    public void testExtendedCreates() throws Exception {
        CreateOptions create2 = CreateOptions
            .newBuilder(ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
            .build();
        List<OpResult> results = zk.multi(Arrays.asList(
            Op.create("/create2", new byte[0], create2),
            Op.create("/container", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.CONTAINER),
            Op.create(
                "/ttl",
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_WITH_TTL,
                60_000)));

        for (OpResult result : results) {
            assertTrue(result instanceof OpResult.CreateResult);
            assertNotNull(((OpResult.CreateResult) result).getStat());
        }
        assertNotNull(zk.exists("/create2", false));
        assertNotNull(zk.exists("/container", false));
        assertNotNull(zk.exists("/ttl", false));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
