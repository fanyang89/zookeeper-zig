package top.fuis.zookeeperzig.interop;

import static org.apache.zookeeper.client.FourLetterWordMain.send4LetterWord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

public final class FourLetterInteropTest {
    private static final long START_TIMEOUT_MILLIS = 30_000;

    @TempDir
    Path testDirectory;

    @Test
    @Timeout(value = 120)
    public void upstreamClientExercisesEveryWhitelistedCommand() throws Exception {
        int clientPort = reservePort();
        try (ZigServerProcess server = ZigServerProcess.startForFourLetterWords(
                 System.getProperty("zookeeper.zig.server"),
                 1,
                 clientPort,
                 testDirectory.resolve("all").toFile(),
                 START_TIMEOUT_MILLIS,
                 "*")) {
            assertEquals("imok", request(clientPort, "ruok").trim());
            assertEquals("rw", request(clientPort, "isro").trim());

            String serverStats = request(clientPort, "srvr");
            assertTrue(serverStats.contains("Zookeeper version:"), serverStats);
            assertTrue(serverStats.contains("Mode: standalone"), serverStats);
            assertTrue(serverStats.contains("Node count:"), serverStats);

            String monitor = request(clientPort, "mntr");
            assertTrue(monitor.contains("zk_version\t"), monitor);
            assertTrue(monitor.contains("zk_server_state\tstandalone"), monitor);
            assertTrue(monitor.contains("zk_num_alive_connections\t"), monitor);

            String config = request(clientPort, "conf");
            assertTrue(config.contains("clientPort=" + clientPort), config);
            assertTrue(config.contains("secureClientPort=-1"), config);
            assertTrue(config.contains("dataDir="), config);

            assertTrue(request(clientPort, "envi").contains("Environment:"));
            assertTrue(request(clientPort, "wchs").contains("connections watching"));
            assertTrue(request(clientPort, "stat").contains("Zookeeper version:"));
            assertTrue(request(clientPort, "dump").contains("SessionTracker dump:"));
            assertTrue(request(clientPort, "srst").contains("Server stats reset."));
            assertTrue(request(clientPort, "crst").contains("Connection stats reset."));

            String connections = request(clientPort, "cons");
            String watchesByConnection = request(clientPort, "wchc");
            String watchesByPath = request(clientPort, "wchp");
            assertTrue(connections.trim().isEmpty(), connections);
            assertTrue(watchesByConnection.trim().isEmpty(), watchesByConnection);
            assertTrue(watchesByPath.trim().isEmpty(), watchesByPath);

            long formerMask = Long.parseLong(request(clientPort, "gtmk").trim());
            assertTrue(formerMask >= 0);
            assertEquals("4660", request(clientPort, setTraceMask(0x1234)).trim());
            assertEquals("4660", request(clientPort, "gtmk").trim());
            assertEquals(
                Long.toString(formerMask),
                request(clientPort, setTraceMask(formerMask)).trim());
        }
    }

    @Test
    @Timeout(value = 120)
    public void upstreamClientObservesWhitelistRejection() throws Exception {
        int clientPort = reservePort();
        try (ZigServerProcess server = ZigServerProcess.startForFourLetterWords(
                 System.getProperty("zookeeper.zig.server"),
                 2,
                 clientPort,
                 testDirectory.resolve("restricted").toFile(),
                 START_TIMEOUT_MILLIS,
                 "srvr")) {
            String rejected = request(clientPort, "ruok");
            assertTrue(rejected.contains("not in the whitelist"), rejected);
            assertTrue(request(clientPort, "srvr").contains("Zookeeper version:"));
        }
    }

    private static String request(int port, String command) throws Exception {
        return send4LetterWord("127.0.0.1", port, command);
    }

    private static String setTraceMask(long value) {
        byte[] request = ByteBuffer.allocate(12)
            .put("stmk".getBytes(StandardCharsets.US_ASCII))
            .putLong(value)
            .array();
        return new String(request, StandardCharsets.ISO_8859_1);
    }

    private static int reservePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(false);
            return socket.getLocalPort();
        }
    }
}
