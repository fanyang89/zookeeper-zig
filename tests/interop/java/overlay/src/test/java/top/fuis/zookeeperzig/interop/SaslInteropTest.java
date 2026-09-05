package top.fuis.zookeeperzig.interop;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.TestableZooKeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ClientInfo;
import org.apache.zookeeper.proto.AuthPacket;
import org.apache.zookeeper.proto.ConnectRequest;
import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.GetDataRequest;
import org.apache.zookeeper.proto.GetDataResponse;
import org.apache.zookeeper.proto.GetSASLRequest;
import org.apache.zookeeper.proto.ReplyHeader;
import org.apache.zookeeper.proto.RequestHeader;
import org.apache.zookeeper.proto.SetSASLResponse;
import org.apache.zookeeper.proto.WatcherEvent;
import org.apache.zookeeper.proto.WhoAmIResponse;
import org.apache.zookeeper.test.ClientBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Official 3.9.5 Jute records, without pretending to negotiate a SASL mechanism. */
public class SaslInteropTest extends ClientBase {
    @Test
    @Timeout(120)
    public void testNoMechanismDoesNotAuthenticateOrDisturbDigestAndWatches() throws Exception {
        byte[] credentials = "user:password".getBytes(StandardCharsets.UTF_8);
        byte[] secret = "private".getBytes(StandardCharsets.UTF_8);
        try (TestableZooKeeper owner = createClient(); RawClient client = new RawClient()) {
            owner.addAuthInfo("digest", credentials);
            owner.create("/secure", secret, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            owner.create("/watched", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            List<ClientInfo> initial = client.identities(1);
            assertEquals(1, initial.size());
            assertEquals("ip", initial.get(0).getAuthScheme());

            client.request(2, ZooDefs.OpCode.getData, new GetDataRequest("/watched", true),
                Code.OK, new GetDataResponse());
            for (byte[] token : new byte[][] {new byte[0], credentials, new byte[0]}) {
                client.sasl(3, token);
                assertEquals(initial, client.identities(4));
                client.request(5, ZooDefs.OpCode.getData, new GetDataRequest("/secure", false),
                    Code.NOAUTH, null);
            }
            client.request(6, ZooDefs.OpCode.create,
                new CreateRequest("/not-authenticated", new byte[0], ZooDefs.Ids.CREATOR_ALL_ACL, 0),
                Code.INVALIDACL, null);
            assertNull(owner.exists("/not-authenticated", false));

            client.request(-4, ZooDefs.OpCode.auth, new AuthPacket(0, "digest", credentials), Code.OK, null);
            List<ClientInfo> authenticated = client.identities(7);
            assertEquals(2, authenticated.size());
            assertTrue(authenticated.stream().anyMatch(id -> "digest".equals(id.getAuthScheme())));
            client.sasl(8, credentials);
            assertEquals(authenticated, client.identities(9));
            GetDataResponse data = new GetDataResponse();
            client.request(10, ZooDefs.OpCode.getData, new GetDataRequest("/secure", false), Code.OK, data);
            assertArrayEquals(secret, data.getData());
            client.request(-2, ZooDefs.OpCode.ping, null, Code.OK, null);

            owner.setData("/watched", secret, -1);
            WatcherEvent event = new WatcherEvent();
            client.reply(-1, Code.OK, event);
            assertEquals("/watched", event.getPath());
            assertEquals(3, event.getType()); // NodeDataChanged
            client.request(11, ZooDefs.OpCode.closeSession, null, Code.OK, null);
        }
    }

    @Test
    @Timeout(120)
    public void testNullMalformedAndTruncatedSaslRecordsCloseConnection() throws Exception {
        byte[][] bodies = {
            {}, {0, 0, 0}, {0, 0, 0, 2, 1},
            {-1, -1, -1, -1}, // null is not the valid empty initial DIGEST-MD5 token
            {-1, -1, -1, -2}, // invalid negative length
            {0x7f, -1, -1, -1}, // oversized declared token, without allocating it
            {0, 0, 0, 1, 1, 2} // trailing data
        };
        for (byte[] body : bodies) {
            try (RawClient client = new RawClient()) {
                byte[] header = encode(new RequestHeader(1, ZooDefs.OpCode.sasl));
                client.out.writeInt(header.length + body.length);
                client.out.write(header);
                client.out.write(body);
                client.out.flush();
                client.expectClosed();
            }
        }
        // A bad connection must not take down the listener or poison other sessions.
        try (RawClient client = new RawClient()) {
            client.sasl(1, new byte[0]);
            client.request(-2, ZooDefs.OpCode.ping, null, Code.OK, null);
        }
    }

    @Test
    @Timeout(120)
    public void testOversizedAndTruncatedSaslFramesCloseConnection() throws Exception {
        try (RawClient client = new RawClient()) {
            client.out.writeInt(Integer.MAX_VALUE);
            client.out.flush();
            client.expectClosed();
        }
        try (RawClient client = new RawClient()) {
            byte[] header = encode(new RequestHeader(1, ZooDefs.OpCode.sasl));
            client.out.writeInt(header.length + 5);
            client.out.write(header);
            client.out.writeInt(1); // promised one token byte is never sent
            client.out.flush();
            client.socket.shutdownOutput();
            client.expectClosed();
        }
        try (RawClient client = new RawClient()) {
            client.sasl(1, new byte[0]);
        }
    }

    private static byte[] encode(Record... records) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        BinaryOutputArchive archive = BinaryOutputArchive.getArchive(bytes);
        for (Record record : records) {
            if (record != null) {
                record.serialize(archive, "record");
            }
        }
        return bytes.toByteArray();
    }

    private final class RawClient implements AutoCloseable {
        final Socket socket = new Socket();
        final DataInputStream in;
        final DataOutputStream out;

        RawClient() throws Exception {
            String[] endpoint = hostPort.split(":");
            socket.connect(new InetSocketAddress(endpoint[0], Integer.parseInt(endpoint[1])), CONNECTION_TIMEOUT);
            socket.setSoTimeout(CONNECTION_TIMEOUT);
            in = new DataInputStream(socket.getInputStream());
            out = new DataOutputStream(socket.getOutputStream());
            send(new ConnectRequest(0, 0, CONNECTION_TIMEOUT, 0, new byte[16], false));
            ConnectResponse response = new ConnectResponse();
            response.deserialize(BinaryInputArchive.getArchive(new ByteArrayInputStream(frame())), "connect");
            assertTrue(response.getSessionId() != 0);
            assertTrue(response.getTimeOut() > 0);
        }

        void send(Record... records) throws Exception {
            byte[] bytes = encode(records);
            out.writeInt(bytes.length);
            out.write(bytes);
            out.flush();
        }

        byte[] frame() throws Exception {
            int length = in.readInt();
            assertTrue(length >= 0 && length <= 0xfffff, "bounded reply length: " + length);
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return bytes;
        }

        void request(int xid, int opcode, Record body, Code code, Record response) throws Exception {
            send(new RequestHeader(xid, opcode), body);
            reply(xid, code, response);
        }

        void reply(int xid, Code code, Record response) throws Exception {
            ByteArrayInputStream bytes = new ByteArrayInputStream(frame());
            BinaryInputArchive archive = BinaryInputArchive.getArchive(bytes);
            ReplyHeader header = new ReplyHeader();
            header.deserialize(archive, "header");
            assertEquals(xid, header.getXid());
            assertEquals(code.intValue(), header.getErr());
            if (response != null) {
                response.deserialize(archive, "response");
            }
            assertEquals(0, bytes.available());
        }

        void sasl(int xid, byte[] token) throws Exception {
            send(new RequestHeader(xid, ZooDefs.OpCode.sasl), new GetSASLRequest(token));
            byte[] bytes = frame();
            BinaryInputArchive archive = BinaryInputArchive.getArchive(new ByteArrayInputStream(bytes));
            ReplyHeader header = new ReplyHeader();
            header.deserialize(archive, "header");
            assertEquals(xid, header.getXid());
            assertEquals(Code.OK.intValue(), header.getErr()); // pre-FAN-6 returns UNIMPLEMENTED
            assertEquals(0L, header.getZxid());
            SetSASLResponse response = new SetSASLResponse();
            response.deserialize(archive, "response");
            assertNull(response.getToken());
            assertEquals(20, bytes.length);
            assertEquals(-1, ByteBuffer.wrap(bytes, 16, 4).getInt()); // null, not empty (0)
        }

        List<ClientInfo> identities(int xid) throws Exception {
            WhoAmIResponse response = new WhoAmIResponse();
            request(xid, ZooDefs.OpCode.whoAmI, null, Code.OK, response);
            return response.getClientInfo();
        }

        void expectClosed() throws Exception {
            try {
                assertEquals(-1, in.read(), "invalid SASL must close without a reply");
            } catch (SocketException reset) {
                // A TCP reset, like EOF, is a safe rejection. Read timeouts are not accepted.
            }
        }

        @Override
        public void close() throws Exception {
            socket.close();
        }
    }
}
