#!/usr/bin/env python3
"""Cold full-data-directory backup and isolated single-node restore drill."""

import hashlib
import pathlib
import shutil
import struct
import sys
import tempfile

from storage_failure import (assert_data, await_client, buffer, create, exchange,
                             ports, request, server)


def checksums(directory):
    return {str(path.relative_to(directory)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(directory.rglob("*")) if path.is_file()}


def sequential(client, xid):
    acl = struct.pack(">ii", 1, 31) + buffer(b"world") + buffer(b"anyone")
    result = request(client, xid, 1, buffer(b"/tree/seq-") + buffer(b"sequence")
                     + acl + struct.pack(">i", 2))
    size = struct.unpack_from(">i", result)[0]
    assert size > 0 and len(result) == size + 4, result
    return result[4:]


def node_image(client, xid, path):
    # Include the complete Stat, not just payload bytes.
    return request(client, xid, 4, buffer(path) + b"\x00")


def absent(client, xid, path):
    result = exchange(client, struct.pack(">ii", xid, 3) + buffer(path) + b"\x00")
    reply_xid, _, error = struct.unpack_from(">iqi", result)
    assert (reply_xid, error) == (xid, -101), result


def scenario(binary, root):
    source = root / "source"
    backup = root / "backup"
    restored = root / "restored"
    client_port, raft_port, _ = ports()
    with server(binary, str(source), client_port, raft_port, None) as process:
        with await_client(process, client_port) as client:
            create(client, 1, b"/tree", b"parent")
            create(client, 2, b"/tree/value", b"initial")
            request(client, 3, 5, buffer(b"/tree/value") + buffer(b"at-backup")
                    + struct.pack(">i", 0))
            first_sequence = sequential(client, 4)
            acl = (struct.pack(">ii", 2, 31) + buffer(b"world") + buffer(b"anyone")
                   + struct.pack(">i", 1) + buffer(b"ip") + buffer(b"127.0.0.1"))
            request(client, 5, 7, buffer(b"/tree/value") + acl + struct.pack(">i", -1))
            saved_value = node_image(client, 6, b"/tree/value")
            saved_parent = node_image(client, 7, b"/tree")
            saved_acl = request(client, 8, 6, buffer(b"/tree/value"))
            assert saved_acl.startswith(acl), saved_acl
            saved_config = node_image(client, 9, b"/zookeeper/config")
    # The context manager has waited for process exit before any file is copied.
    # Copy WAL, snapshots, RocksDB, and metadata together, not just SST files.
    shutil.copytree(source, backup)
    manifest = checksums(backup)
    assert manifest and checksums(source) == manifest

    # Demonstrate the backup's recovery point using acknowledged later changes.
    with server(binary, str(source), client_port, raft_port, None) as process:
        with await_client(process, client_port) as client:
            request(client, 1, 5, buffer(b"/tree/value") + buffer(b"after-backup")
                    + struct.pack(">i", 1))
            create(client, 2, b"/after-backup", b"not-in-backup")
            next_sequence = sequential(client, 3)
            assert next_sequence != first_sequence
            assert_data(client, 4, b"/tree/value", b"after-backup")
    # Make the original path unavailable. Never run the restored identity
    # concurrently with the original instance.
    source.rename(root / "retired-source")
    assert checksums(backup) == manifest
    shutil.copytree(backup, restored)
    assert checksums(restored) == manifest
    with server(binary, str(restored), client_port, raft_port, None) as process:
        with await_client(process, client_port) as client:
            assert node_image(client, 1, b"/tree/value") == saved_value
            assert node_image(client, 2, b"/tree") == saved_parent
            assert request(client, 3, 6, buffer(b"/tree/value")) == saved_acl
            assert node_image(client, 4, b"/zookeeper/config") == saved_config
            assert_data(client, 5, first_sequence, b"sequence")
            absent(client, 6, b"/after-backup")
            absent(client, 7, next_sequence)
            # Sequence allocation resumes at the backup's state, not the newer
            # source state. This is rollback, not an online clone or migration.
            assert sequential(client, 8) == next_sequence
            create(client, 9, b"/restored-write", b"accepted")
            assert_data(client, 10, b"/restored-write", b"accepted")
    assert checksums(backup) == manifest, "restore modified the backup artifact"
    print("PASS cold backup: data/Stat/ACL/config, recovery point, sequences, new writes")


def main():
    binary = str(pathlib.Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory(prefix="zookeeper-backup-restore-") as directory:
        scenario(binary, pathlib.Path(directory))


if __name__ == "__main__":
    main()
