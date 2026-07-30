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
- Cyrus SASL DIGEST-MD5 mechanism for SASL end-to-end tests
- Docker with Compose for end-to-end tests
- OpenSSL and JDK `keytool` for TLS end-to-end tests
- Clang with compiler-rt for address sanitizer tests

## Quick start

```sh
mise install
mise run build
mise run test
mise run test-asan
mise run test-ubsan
mise run test-tsan
mise run e2e
mise run e2e-sasl
mise run e2e-tls
mise run e2e-quorum
```

More test and instrumentation tasks will be listed by `mise tasks`.
