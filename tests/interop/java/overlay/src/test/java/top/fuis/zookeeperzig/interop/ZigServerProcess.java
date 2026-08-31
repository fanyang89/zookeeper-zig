package top.fuis.zookeeperzig.interop;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public final class ZigServerProcess implements AutoCloseable {
    private static final String CLUSTER_ID = "0198f54d-5c2a-7000-8000-000000000001";

    private final Process process;
    private final File logFile;

    private ZigServerProcess(Process process, File logFile) {
        this.process = process;
        this.logFile = logFile;
    }

    public static ZigServerProcess start(
        String binary,
        int serverId,
        int clientPort,
        File testDirectory,
        long timeoutMillis) throws Exception {
        int raftPort = reservePort();
        File dataDirectory = new File(testDirectory, "zig-data");
        if (!dataDirectory.isDirectory() && !dataDirectory.mkdirs()) {
            throw new IOException("failed to create Zig data directory: " + dataDirectory);
        }
        File logFile = new File(testDirectory, "zig-server.log");
        Process process = new ProcessBuilder(
            binary,
            "--node-id", Integer.toString(serverId),
            "--cluster-id", CLUSTER_ID,
            "--client-listen", "127.0.0.1:" + clientPort,
            "--raft-listen", "127.0.0.1:" + raftPort,
            "--data-dir", dataDirectory.getAbsolutePath(),
            "--peer", serverId + "=127.0.0.1:" + raftPort)
            .redirectErrorStream(true)
            .redirectOutput(logFile)
            .start();
        ZigServerProcess server = new ZigServerProcess(process, logFile);
        try {
            server.awaitReady(clientPort, timeoutMillis);
            return server;
        } catch (Exception error) {
            process.destroyForcibly();
            throw error;
        }
    }

    private static int reservePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(false);
            return socket.getLocalPort();
        }
    }

    private void awaitReady(int port, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new IOException("Zig server exited during startup:\n" + readLog());
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port), 250);
                break;
            } catch (IOException ignored) {
                Thread.sleep(100);
            }
        }

        CountDownLatch connected = new CountDownLatch(1);
        ZooKeeper client = new ZooKeeper("127.0.0.1:" + port, (int) timeoutMillis, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connected.countDown();
            }
        });
        try {
            if (!connected.await(timeoutMillis, TimeUnit.MILLISECONDS)) {
                throw new IOException("Zig server did not accept a ZooKeeper session:\n" + readLog());
            }
            client.exists("/", false);
        } finally {
            client.close();
        }
    }

    private String readLog() throws IOException {
        if (!logFile.isFile()) {
            return "<no server log>";
        }
        return new String(Files.readAllBytes(logFile.toPath()), StandardCharsets.UTF_8);
    }

    @Override
    public void close() throws Exception {
        if (Boolean.getBoolean("zookeeper.zig.printServerLog")) {
            System.err.println("--- Zig server log: " + logFile + " ---");
            System.err.println(readLog());
        }
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                throw new IOException("failed to stop Zig server; log: " + logFile);
            }
        }
    }
}
