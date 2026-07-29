# Generated Jute Sources

`zookeeper.jute.c` and `zookeeper.jute.h` were generated from the included
`zookeeper.jute` schema using the Jute compiler from Apache ZooKeeper 3.9.5.
The upstream release does not ship these generated files.

Reproduction command from the root of a full ZooKeeper 3.9.5 source tree:

```sh
mkdir -p zookeeper-client/zookeeper-client-c/generated
mvn -pl zookeeper-jute generate-sources -DskipTests
```

The generated files retain their Apache license headers. They are kept outside
`vendor/zookeeper-client-c` so vendor verification remains exact.

SHA-256 checksums:

```text
1b39859417e3ebad1fe3c67bb6d1cae5a5916a0754c3f7859de2a5b343b1e05a  zookeeper.jute
00fca1b509458b39826d1ddc4c92322a7fb1b5e851edc6774e9d24b9437f6e0c  zookeeper.jute.c
44de453278f09de752d8dcb10f796a9b1eaddd118cff57b669355e55c20b1157  zookeeper.jute.h
```
