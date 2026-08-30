# Importing Apache ZooKeeper data

The quorum server can initialize a new node from an Apache ZooKeeper 3.9.5
`dataDir`. Import runs before Raft, the session reaper, and client listeners
start.

```sh
zig-out/bin/zookeeper-quorum-server \
  --node-id 1 \
  --cluster-id 0198f54d-5c2a-7000-8000-000000000001 \
  --client-listen 127.0.0.1:2181 \
  --raft-listen 127.0.0.1:2881 \
  --data-dir ./data/node1 \
  --import-zookeeper-data-dir /srv/zookeeper/data \
  --import-zookeeper-log-dir /srv/zookeeper/log \
  --peer 1=127.0.0.1:2881 \
  --peer 2=127.0.0.1:2882 \
  --peer 3=127.0.0.1:2883
```

`--import-zookeeper-log-dir` is optional. Logs are read from the data directory
when it is omitted. Each source path may name either the directory containing
`version-2` or `version-2` itself.

## Cluster procedure

1. Stop writes to the Apache ZooKeeper ensemble and copy an immutable snapshot
   of its data and log directories.
2. Start with empty target data directories. Import refuses to replace an
   existing non-imported RocksDB state.
3. Pass the same source copy and import arguments to every initial voting
   member. Never import only one member, and do not use import options with
   `--join`.
4. After successful import, the arguments may remain configured. A durable
   import marker makes subsequent starts idempotent.

The importer selects the newest valid snapshot and replays later transaction
logs. It validates file headers, snapshot seals, log Adler-32 checksums, record
lengths, `0x42` transaction markers, paths, ACL references, and tree structure.
The imported RocksDB is built in `state.rocksdb.importing` and activated with a
single rename only after validation succeeds.

Sessions and ordinary ephemeral nodes are retained. Because Apache ZooKeeper
does not persist remaining lease time, each surviving session receives one
fresh negotiated lease. The original 16-byte password is regenerated with
ZooKeeper's `java.util.Random(sessionId ^ 0xB3415C00)` algorithm, allowing an
existing client to resume with its saved credentials.

## Supported source state

- Plain and gzip-compressed snapshots
- Persistent and ordinary ephemeral nodes
- Nullable node data
- ZooKeeper ACLs using schemes supported by this server
- Session create/close records
- Create, create2, delete, setData, setACL, reconfig, and multi log records
- Legacy create and close-session transaction layouts

The importer intentionally rejects rather than weakens or changes unsupported
state:

- Xerial Snappy snapshots
- Container and TTL nodes or transactions
- ACL schemes unavailable in this server, such as SASL and X.509
- Corrupt or truncated logs
- Snapshot-only recovery when no valid snapshot exists

Each input file and the decompressed snapshot are currently limited to 256 MiB.
ZooKeeper source zxids are preserved in node stats but are not reused as Raft
indexes. New Raft history starts from index 1 over the imported baseline, while
client-visible zxids use the imported source zxid as an offset so they remain
strictly greater than imported zxids.
