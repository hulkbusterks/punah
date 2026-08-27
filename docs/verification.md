# Verification

Punah separates fast pull-request checks, deterministic crash testing, and
longer dynamic analysis. Performance numbers are never used as hosted-runner CI
gates because shared runners are too noisy for that purpose.

## Local and pull-request checks

The repository pins Rust 1.97.1 in `rust-toolchain.toml`. The same commands run
in CI:

```sh
cargo fmt --all -- --check
cargo fmt --all --manifest-path fuzz/Cargo.toml -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --all-features --no-deps --locked
cargo test --workspace --all-targets --all-features --locked
cargo test --workspace --release --locked
cargo check --manifest-path fuzz/Cargo.toml --locked
```

Debug tests run on Linux and macOS; optimized tests run on Linux. Workflows have
read-only repository permission, pin third-party actions by commit, and cancel
superseded runs. Dependabot proposes monthly Cargo and workflow updates, while
lockfiles keep ordinary builds deterministic.

## Crash consistency

Normal stable tests inject failures before the first media effect and after
record-body, first-flush, commit-marker, and final-flush effects. `CrashDevice`
keeps separate volatile and durable images: writes change volatility, Flush
copies the complete volatile image to durable state, and power loss discards
volatility plus queued work. Singleton and cohort tests exhaust every operation-
effect boundary with FIFO and reversed completion delivery, reopen only durable
bytes, and separately prove acknowledged LocalDurable operations survive a
later loss. A final Flush may take effect while its acknowledgement is lost, so
recovery can legitimately contain unacknowledged records.

This is an atomic two-image contract model: complete writes take effect and a
successful Flush promotes the whole image. It does not model torn sectors,
partial failed-Flush persistence, controller reordering, or prove SPDK/FUA
semantics. Cohort tests also exhaust queue capacity and verify fail-stop behavior
after ambiguous accepted I/O.

Metadata tests interrupt immutable-object and dual-root publication. Filesystem
tests reopen Stores across rotation, tombstones, active suffixes, strict sealed
boundaries, compaction, and corrupt-root handling. They now explicitly exercise
the two namespace/publication sides of rotation and compaction: a durable new
segment before root publication remains an orphan and is never reused, while a
new compaction root before old-input deletion recovers only replacement
membership. Resumable-compaction coverage also copies a live record, performs a
new foreground overwrite between slices, publishes the replacement, and
reopens to prove the exact relocation fence cannot redirect the newer version.
Graceful job cancellation is separately checked to remove its private output,
while the crash-window test deliberately skips destructors and retains it as an
orphan under the old root.
Publication interleaving tests block the helper before active-file sync, append
a foreground suffix, release publication, and reopen to prove the captured
prefix plus suffix scan is complete. A second test fills the active segment and
proves rotation rendezvous installs the pending root before topology changes. A
missing captured active path makes the helper fail and verifies the Store
rejects all later reads and mutations rather than guessing authority.
These real-file states complement rather than overclaim the
two-image byte model. Injecting `CrashDevice` below the complete Store still
requires separating filesystem namespace/metadata authority from Store's
current concrete `FileDevice`; torn sectors and real filesystem crash ordering
remain Linux fault-injection work.

## Fuzzing

The detached `fuzz` workspace keeps `libfuzzer-sys` out of production crates.
It has two initial targets:

- `decode_untrusted` feeds arbitrary bytes to durable and wire decoders, commit
  validators, aligned extent-v2 recovery, and dual-root selection. It asserts
  owned and borrowed record decoding agree exactly.
- `roundtrip_formats` constructs bounded valid records, protocol v1/v2 frames,
  aligned extents, manifests, journal entries, and superblocks. It checks
  length calculation, uncommitted-to-committed transitions, and round-trip
  equality.

The scheduled workflow runs each target for three minutes with a 64 KiB input
limit. A local longer run is:

```sh
cargo install cargo-fuzz --version 0.13.2 --locked
cargo +nightly-2026-08-20 fuzz run decode_untrusted -- -max_len=65536
cargo +nightly-2026-08-20 fuzz run roundtrip_formats -- -max_len=65536
```

Crash artifacts are ignored; minimized cases that represent stable bugs should
be copied into a checked-in corpus and ordinary regression test.

## Scheduled dynamic checks

The weekly dynamic-analysis workflow runs strict-provenance Miri over pure safe
codecs/state machines, AddressSanitizer over the device/engine/Store path, and a
workspace LLVM coverage summary. Miri intentionally excludes `MmapRaw` and OS
I/O; ASan covers that storage boundary on Linux. Golden hex fixtures pin record
v1, aligned-extent v2 headers, manifest, journal, and superblock bytes so an
accidental format drift fails ordinary tests. Coverage is diagnostic and never
substitutes for crash-boundary or parser-state assertions.
AddressSanitizer should cover device, engine, and Store filesystem tests on
Linux. Coverage may be uploaded as an informational artifact, but no global
percentage replaces parser-state, crash-boundary, or recovery-invariant tests.
