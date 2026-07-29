# ZooKeeper C Client built with Zig

This project builds the unmodified Apache ZooKeeper 3.9.5 C client with Zig.
It targets Linux and produces both threaded (`zookeeper_mt`) and single-threaded
(`zookeeper_st`) libraries.

The client sources are local and builds do not fetch dependencies. See
`vendor/UPSTREAM.md` for source provenance.

## Requirements

- mise 2026.7.13 or newer
- Zig 0.16.0 (installed by mise)
- OpenSSL development files for TLS builds
- Cyrus SASL development files for SASL builds

## Quick start

```sh
mise install
mise run build
mise run test
```

More test and instrumentation tasks will be listed by `mise tasks`.
