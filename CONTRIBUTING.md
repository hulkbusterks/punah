# Contributing

Punah is currently an experimental personal project. Design discussion and
carefully scoped changes are welcome, but interfaces and on-disk formats may
still evolve before the first stable release.

Before submitting a change, run:

```console
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Changes to persistent formats, wire protocols, identity derivation, durability,
compaction, or unsafe boundaries must update the corresponding design document
and include failure-path tests. Performance changes should include reproducible
commands and should distinguish development measurements from production
claims.
