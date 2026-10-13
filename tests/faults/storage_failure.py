#!/usr/bin/env python3
"""Linux subprocess storage-failure and same-directory recovery regression."""

import contextlib
import pathlib
import resource
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time


class ConnectionLoss(Exception):
    pass


def buffer(value):
    return struct.pack(">i", len(value)) + value


def read_exact(sock, size):
    result = bytearray()
    while len(result) < size:
        chunk = sock.recv(size - len(result))
        if not chunk:
            raise EOFError("server closed the connection")
        result.extend(chunk)
    return bytes(result)


def exchange(sock, payload):
    sock.sendall(buffer(payload))
    size = struct.unpack(">i", read_exact(sock, 4))[0]
    if not 0 <= size <= 1024 * 1024:
        raise AssertionError(f"invalid response length: {size}")
    return read_exact(sock, size)


def connect(port):
    sock = socket.create_connection(("127.0.0.1", port), timeout=2)
    try:
        response = exchange(sock, struct.pack(">iqiq", 0, 0, 15000, 0)
                            + buffer(bytes(16)) + b"\x00")
        if len(response) < 16 or struct.unpack_from(">q", response, 8)[0] == 0:
            raise AssertionError("session negotiation failed")
        return sock
    except BaseException:
        sock.close()
        raise


def request(sock, xid, opcode, body):
    response = exchange(sock, struct.pack(">ii", xid, opcode) + body)
    reply_xid, _, error = struct.unpack_from(">iqi", response)
    if reply_xid == xid and error == -4:
        raise ConnectionLoss(f"request {xid}: ConnectionLoss")
    if (reply_xid, error) != (xid, 0):
        raise AssertionError(f"request {xid}: xid={reply_xid}, error={error}")
    return response[16:]


def create(sock, xid, path, data):
    acl = struct.pack(">ii", 1, 31) + buffer(b"world") + buffer(b"anyone")
    body = buffer(path) + buffer(data) + acl + struct.pack(">i", 0)
    result = request(sock, xid, 1, body)
    assert result == buffer(path), result


def assert_data(sock, xid, path, expected):
    result = request(sock, xid, 4, buffer(path) + b"\x00")
    size = struct.unpack_from(">i", result)[0]
    assert size == len(expected) and result[4:4 + size] == expected, result


def ports(count=3):
    with contextlib.ExitStack() as stack:
        listeners = [stack.enter_context(socket.socket()) for _ in range(count)]
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in listeners]


def ignore_file_size_signal():
    # Exercise the application's EFBIG error path, not a SIGXFSZ termination.
    signal.signal(signal.SIGXFSZ, signal.SIG_IGN)


@contextlib.contextmanager
def server(binary, directory, client_port, raft_port, admin_port, *, node_id=1, peers=None, extra_args=()):
    command = [binary, "--node-id", str(node_id), "--cluster-id",
               "0198f54d-5c2a-7000-8000-000000000005",
               "--client-listen", f"127.0.0.1:{client_port}",
               "--raft-listen", f"127.0.0.1:{raft_port}",
               "--data-dir", directory]
    for peer_id, port in (peers if peers is not None else [(node_id, raft_port)]):
        command += ["--peer", f"{peer_id}=127.0.0.1:{port}"]
    if admin_port is not None:
        command += ["--admin-listen", f"127.0.0.1:{admin_port}"]
    command.extend(extra_args)
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE, preexec_fn=ignore_file_size_signal)
    try:
        yield process
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            _, logs = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            _, logs = process.communicate(timeout=10)
        if logs:
            sys.stderr.write(logs.decode(errors="replace"))


def await_client(process, port):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(f"server exited during startup: {process.returncode}")
        try:
            return connect(port)
        except (OSError, EOFError):
            time.sleep(0.05)
    raise AssertionError("session startup deadline exceeded")


def assert_closed(port):
    try:
        sock = socket.create_connection(("127.0.0.1", port), timeout=1)
    except ConnectionRefusedError:
        return
    sock.close()
    raise AssertionError(f"port {port} is still accepting connections")


def scenario(binary, directory, with_admin):
    client_port, raft_port, admin_port = ports()
    admin_port = admin_port if with_admin else None
    with server(binary, directory, client_port, raft_port, admin_port) as process:
        with await_client(process, client_port) as client:
            create(client, 1, b"/durable", b"acknowledged-before-fault")
            assert_data(client, 2, b"/durable", b"acknowledged-before-fault")
            with contextlib.ExitStack() as idle:
                idle.enter_context(socket.create_connection(("127.0.0.1", client_port), timeout=2))
                if admin_port is not None:
                    idle.enter_context(socket.create_connection(("127.0.0.1", admin_port), timeout=2))
                # Applies only to this child. Future regular-file writes beyond
                # byte 1 fail with EFBIG; pipes remain writable for diagnostics.
                resource.prlimit(process.pid, resource.RLIMIT_FSIZE, (1, 1))
                try:
                    create(client, 3, b"/during-fault", b"must-not-acknowledge")
                except (OSError, EOFError, ConnectionLoss):
                    pass
                else:
                    raise AssertionError("write unexpectedly acknowledged under storage fault")
                _, logs = process.communicate(timeout=15)
                text = logs.decode(errors="replace")
                assert process.returncode > 0, f"expected error exit, got {process.returncode}"
                assert "ZooKeeper Raft driver stopped: WalWriteFailed" in text, text
                assert "RaftDriverFailed" in text, text
        assert_closed(client_port)
        if admin_port is not None:
            assert_closed(admin_port)
    # Model an external supervisor starting a new unrestricted process. Reuse
    # the entire data directory and identity; do not remove or repair files.
    with server(binary, directory, client_port, raft_port, admin_port) as process:
        with await_client(process, client_port) as client:
            assert_data(client, 1, b"/durable", b"acknowledged-before-fault")
            create(client, 2, b"/after-restart", b"writable-again")
            assert_data(client, 3, b"/after-restart", b"writable-again")
    print(f"PASS storage failure, error exit, and recovery (admin={with_admin})")


def main():
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    for with_admin in (False, True):
        with tempfile.TemporaryDirectory(prefix="zookeeper-storage-fault-") as directory:
            scenario(binary, directory, with_admin)


if __name__ == "__main__":
    main()
