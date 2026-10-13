#!/usr/bin/env python3
"""Absolute opening-input deadlines and connection-budget recovery."""

import pathlib
import select
import socket
import struct
import sys
import tempfile
import time

from connection_limits import await_status
from storage_failure import assert_data, await_client, create, ports, server


def admitted_socket(port):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        started = time.monotonic()
        sock = socket.create_connection(("127.0.0.1", port), timeout=2)
        try:
            # A completed 4lw reply can precede its task releasing the slot.
            # Retry only admission rejection, before sending any test input.
            sock.settimeout(0.1)
            assert sock.recv(1) == b"", "unexpected response without input"
        except socket.timeout:
            sock.settimeout(2)
            return sock, started
        except ConnectionResetError:
            pass
        except BaseException:
            sock.close()
            raise
        sock.close()
        time.sleep(0.02)
    raise AssertionError("pre-handshake admission deadline exceeded")


def expires(port, fragments, interval=0.2):
    sock, started = admitted_socket(port)
    with sock:
        index = 0
        next_send = started
        # A one-second absolute budget must expire well before a deadline
        # restarted after the fourth header byte at 0.6 seconds would expire.
        deadline = started + 1.5
        while time.monotonic() < deadline:
            now = time.monotonic()
            if index < len(fragments) and now >= next_send:
                try:
                    sock.sendall(fragments[index])
                except ConnectionResetError:
                    assert now - started >= 0.5, "connection rejected before timeout"
                    return
                index += 1
                next_send += interval
            if select.select([sock], [], [], 0.02)[0]:
                try:
                    response = sock.recv(1)
                except ConnectionResetError:
                    response = b""
                assert response == b"", response
                assert time.monotonic() - started >= 0.5, "connection rejected before timeout"
                return
    raise AssertionError("opening deadline was not enforced or was reset by input")


def scenario(binary, directory):
    client_port, raft_port, _ = ports()
    with server(binary, directory, client_port, raft_port, None, extra_args=[
            "--client-max-connections", "1", "--client-handshake-timeout-ms", "1000",
            "--4lw-whitelist", "ruok,stmk"]) as process:
        with await_client(process, client_port) as client:
            create(client, 1, b"/timeout", b"established-session")
            time.sleep(1.2)
            assert_data(client, 2, b"/timeout", b"established-session")
        for fragments in (
                [],                                  # No opening bytes.
                [b"\x00"],                          # Partial length header.
                [b"\x00", b"\x00", b"\x00", b"\x2d"] + [b"\x00"] * 8,
                [b"s", b"t", b"m", b"k"] + [b"\x00"] * 7):
            await_status(client_port)
            expires(client_port, fragments)
            await_status(client_port)
        # A complete stmk command still works before its deadline.
        sock, _ = admitted_socket(client_port)
        with sock:
            sock.sendall(b"stmk" + struct.pack(">q", 42))
            response = bytearray()
            while chunk := sock.recv(128):
                response.extend(chunk)
            assert bytes(response).strip() == b"42", response
        with await_client(process, client_port) as client:
            assert_data(client, 1, b"/timeout", b"established-session")
    print("PASS opening deadline: idle/partial/drip/stmk, slot recovery, established session")


if __name__ == "__main__":
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="zookeeper-handshake-timeout-") as directory:
        scenario(binary, directory)
