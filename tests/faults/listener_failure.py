#!/usr/bin/env python3
"""Configured listener failures must not leave a partially serving process."""

import pathlib
import socket
import sys
import tempfile

from storage_failure import assert_closed, assert_data, await_client, create, ports, server


def scenario(binary, directory, occupied):
    client_port, raft_port, admin_port = ports()
    conflict_port = admin_port if occupied == "admin" else client_port
    other_port = client_port if occupied == "admin" else admin_port
    with socket.socket() as conflict:
        conflict.bind(("127.0.0.1", conflict_port))
        conflict.listen()
        with server(binary, directory, client_port, raft_port, admin_port) as process:
            _, logs = process.communicate(timeout=15)
            text = logs.decode(errors="replace")
            assert process.returncode > 0, f"expected unsuccessful exit, got {process.returncode}"
            assert "AddressInUse" in text, text
        assert_closed(other_port)
        assert_closed(raft_port)
        # The server must not have closed the unrelated port owner.
        with socket.create_connection(("127.0.0.1", conflict_port), timeout=2):
            pass
    # Releasing the conflict is sufficient: no data deletion or identity change.
    with server(binary, directory, client_port, raft_port, admin_port) as process:
        with await_client(process, client_port) as client:
            create(client, 1, b"/listeners-ready", b"both-listeners")
            assert_data(client, 2, b"/listeners-ready", b"both-listeners")
        with socket.create_connection(("127.0.0.1", admin_port), timeout=2) as admin:
            admin.sendall(b"GET /commands/ruok?format=txt HTTP/1.1\r\n\r\n")
            response = bytearray()
            while data := admin.recv(4096):
                response.extend(data)
            assert response.startswith(b"HTTP/1.1 200 "), response
            assert response.endswith(b"\r\n\r\nimok"), response
    print(f"PASS {occupied} bind conflict: unsuccessful exit, sibling cleanup, restart")


if __name__ == "__main__":
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    for occupied in ("admin", "client"):
        with tempfile.TemporaryDirectory(prefix="zookeeper-listener-failure-") as directory:
            scenario(binary, directory, occupied)
