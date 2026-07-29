# Vendored Apache ZooKeeper C Client

`zookeeper-client-c/` is copied byte-for-byte from the Apache ZooKeeper 3.9.5
source release. It intentionally retains the upstream build files, tests,
licenses, and C/C++ source files even when this project does not use them.

- Release: Apache ZooKeeper 3.9.5
- Git tag: `release-3.9.5`
- Git commit: `293c895a8d966a3ecb92872be4a1daf87d725da2`
- Source archive: `apache-zookeeper-3.9.5.tar.gz`
- Archive URL: <https://archive.apache.org/dist/zookeeper/zookeeper-3.9.5/apache-zookeeper-3.9.5.tar.gz>
- Archive SHA-512: see `vendor.lock`
- Archive path: `apache-zookeeper-3.9.5/zookeeper-client/zookeeper-client-c`

The release does not contain the generated Jute C files required by the C
client. Those files are kept separately in `/generated`; see its README for
their provenance.

Run `mise run vendor:verify` to compare this directory with the release.
