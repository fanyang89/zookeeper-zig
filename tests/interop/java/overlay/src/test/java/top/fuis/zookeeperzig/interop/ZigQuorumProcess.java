package top.fuis.zookeeperzig.interop;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class ZigQuorumProcess implements AutoCloseable {
    private static final String CLUSTER_ID = "0198f54d-5c2a-7000-8000-000000000002";
    private static final int NODE_COUNT = 3;

    private final String binary;
    private final long timeoutMillis;
    private final Node[] nodes;

    private static final class Node {
        private final int id;
        private final int clientPort;
        private final int raftPort;
        private final Path dataDirectory;
        private final File logFile;
        private Process process;

        private Node(int id, int clientPort, int raftPort, Path dataDirectory, File logFile) {
            this.id = id;
            this.clientPort = clientPort;
            this.raftPort = raftPort;
            this.dataDirectory = dataDirectory;
            this.logFile = logFile;
        }
    }

    private ZigQuorumProcess(String binary, long timeoutMillis, Node[] nodes) {
        this.binary = binary;
        this.timeoutMillis = timeoutMillis;
        this.nodes = nodes;
    }

    public static ZigQuorumProcess start(String binary, Path testDirectory, long timeoutMillis)
        throws Exception {
        if (binary == null || binary.isEmpty()) {
            throw new IllegalArgumentException("zookeeper.zig.server must name the Zig server binary");
        }
        int[] ports = reservePorts(NODE_COUNT * 2);
        Node[] nodes = new Node[NODE_COUNT];
        for (int index = 0; index < NODE_COUNT; index++) {
            Path dataDirectory = testDirectory.resolve("node-" + (index + 1));
            Files.createDirectories(dataDirectory);
            nodes[index] = new Node(
                index + 1,
                ports[index],
                ports[index + NODE_COUNT],
                dataDirectory,
                testDirectory.resolve("node-" + (index + 1) + ".log").toFile());
        }

        ZigQuorumProcess cluster = new ZigQuorumProcess(binary, timeoutMillis, nodes);
        try {
            for (int index = 0; index < NODE_COUNT; index++) {
                cluster.startNode(index);
            }
            return cluster;
        } catch (Exception error) {
            try {
                cluster.close();
            } catch (Exception cleanupError) {
                error.addSuppressed(cleanupError);
            }
            throw error;
        }
    }

    public String connectString() {
        StringBuilder result = new StringBuilder();
        for (Node node : nodes) {
            if (result.length() != 0) {
                result.append(',');
            }
            result.append("127.0.0.1:").append(node.clientPort);
        }
        return result.toString();
    }

    public String nodeConnectString(int index) {
        return "127.0.0.1:" + node(index).clientPort;
    }

    public synchronized void startNode(int index) throws Exception {
        Node node = node(index);
        if (node.process != null && node.process.isAlive()) {
            throw new IllegalStateException("node is already running: " + node.id);
        }
        List<String> command = new ArrayList<>();
        command.add(binary);
        command.add("--node-id");
        command.add(Integer.toString(node.id));
        command.add("--cluster-id");
        command.add(CLUSTER_ID);
        command.add("--client-listen");
        command.add("127.0.0.1:" + node.clientPort);
        command.add("--raft-listen");
        command.add("127.0.0.1:" + node.raftPort);
        command.add("--data-dir");
        command.add(node.dataDirectory.toAbsolutePath().toString());
        for (Node peer : nodes) {
            command.add("--peer");
            command.add(peer.id + "=127.0.0.1:" + peer.raftPort);
        }
        node.process = new ProcessBuilder(command)
            .redirectErrorStream(true)
            .redirectOutput(ProcessBuilder.Redirect.appendTo(node.logFile))
            .start();
        try {
            awaitReady(node);
        } catch (Exception error) {
            try {
                stopNode(index);
            } catch (Exception cleanupError) {
                error.addSuppressed(cleanupError);
            }
            throw error;
        }
    }

    public synchronized void stopNode(int index) throws Exception {
        Node node = node(index);
        Process process = node.process;
        node.process = null;
        if (process == null) {
            return;
        }
        process.destroy();
        if (!process.waitFor(10, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                throw new IOException("failed to stop Zig quorum node " + node.id);
            }
        }
    }

    private void awaitReady(Node node) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (node.process == null || !node.process.isAlive()) {
                throw new IOException(
                    "Zig quorum node " + node.id + " exited during startup:\n" + readLog(node));
            }
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", node.clientPort), 250);
                return;
            } catch (IOException ignored) {
                Thread.sleep(50);
            }
        }
        throw new IOException(
            "Zig quorum node " + node.id + " did not listen in time:\n" + readLog(node));
    }

    private static int[] reservePorts(int count) throws IOException {
        ServerSocket[] sockets = new ServerSocket[count];
        int[] ports = new int[count];
        try {
            for (int index = 0; index < count; index++) {
                sockets[index] = new ServerSocket(0);
                sockets[index].setReuseAddress(false);
                ports[index] = sockets[index].getLocalPort();
            }
            return ports;
        } finally {
            for (ServerSocket socket : sockets) {
                if (socket != null) {
                    socket.close();
                }
            }
        }
    }

    private Node node(int index) {
        if (index < 0 || index >= nodes.length) {
            throw new IndexOutOfBoundsException("invalid node index: " + index);
        }
        return nodes[index];
    }

    private String readLog(Node node) throws IOException {
        if (!node.logFile.isFile()) {
            return "<no server log>";
        }
        return new String(Files.readAllBytes(node.logFile.toPath()), StandardCharsets.UTF_8);
    }

    private String readAllLogs() throws IOException {
        StringBuilder result = new StringBuilder();
        for (Node node : nodes) {
            result.append("--- Zig quorum node ").append(node.id).append(" ---\n");
            result.append(readLog(node));
            if (result.length() == 0 || result.charAt(result.length() - 1) != '\n') {
                result.append('\n');
            }
        }
        return result.toString();
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        for (int index = nodes.length - 1; index >= 0; index--) {
            try {
                stopNode(index);
            } catch (Exception error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        }
        if (Boolean.getBoolean("zookeeper.zig.printServerLog")) {
            System.err.println(readAllLogs());
        }
        if (failure != null) {
            throw failure;
        }
    }
}
