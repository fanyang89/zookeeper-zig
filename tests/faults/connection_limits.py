#!/usr/bin/env python3
"""Verify admission before handshake and slot recovery on disconnect."""

import pathlib
import socket
import sys
import tempfile
import time

from storage_failure import assert_data, await_client, create, ports, server


def status(port):
    with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
        sock.sendall(b"ruok")
        return sock.recv(4)


def await_status(port):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            if status(port) == b"imok":
                return
        except ConnectionResetError:
            pass
        time.sleep(0.02)
    raise AssertionError("connection slot was not released")


def await_idle(port):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        sock = socket.create_connection(("127.0.0.1", port), timeout=2)
        try:
            sock.sendall(b"\x00")  # Incomplete frame header: no handshake.
            sock.settimeout(0.1)
            assert sock.recv(1) == b"", "unexpected pre-handshake response"
        except socket.timeout:
            sock.settimeout(2)
            return sock
        except ConnectionResetError:
            pass
        except BaseException:
            sock.close()
            raise
        sock.close()
        time.sleep(0.02)
    raise AssertionError("idle connection admission deadline exceeded")


def scenario(binary, directory):
    client_port, raft_port, _ = ports()
    with server(binary, directory, client_port, raft_port, None,
                extra_args=["--client-max-connections", "1", "--4lw-whitelist", "ruok"]) as process:
        with await_client(process, client_port) as client:
            create(client, 1, b"/limit", b"still-serving")
            # One established client exhausts the budget. TCP establishment
            # itself may succeed; the accepted socket must be closed promptly.
            for _ in range(5):
                try:
                    assert status(client_port) == b"", "over-limit 4lw was served"
                except ConnectionResetError:
                    pass
            assert_data(client, 2, b"/limit", b"still-serving")
        await_status(client_port)
        # No handshake is required to occupy the sole slot. EOF and malformed
        # frames must both release it for later clients.
        for opening in (b"", b"\xff\xff\xff"):
            with await_idle(client_port) as idle:
                for _ in range(3):
                    try:
                        assert status(client_port) == b"", "idle socket bypassed budget"
                    except ConnectionResetError:
                        pass
                if opening:
                    idle.sendall(opening)
                    assert idle.recv(1) == b"", "oversized frame was not rejected"
            await_status(client_port)
        with await_client(process, client_port) as client:
            assert_data(client, 1, b"/limit", b"still-serving")
    print("PASS connection cap: established/idle/4lw rejection and slot recovery")


if __name__ == "__main__":
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="zookeeper-connection-limit-") as directory:
        scenario(binary, directory)
