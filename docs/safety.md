# Safety policy

Unsafe Rust is denied by default at workspace level. The current code has two
narrow local opt-ins. `punah-device/src/mapped.rs` establishes an operating-system
mapping with `MmapRaw` and wraps raw reads in bounds-checked copies to owned
memory. `punah-device/src/uring.rs` pushes an SQE containing raw pointers while
retaining the owning request buffer until its correlated CQE; its `Drop` path
waits for outstanding work and deliberately leaks buffers if teardown cannot
prove the kernel released them. Neither module exposes a raw pointer or
file-backed Rust reference.
Future FFI, DMA, hugepage, or lock-free-ring crates may opt in only after their
safety boundary is documented.

Every unsafe block must state the invariants required for memory validity,
aliasing, alignment, thread ownership, and lifetime. Raw pointers may not escape
the owning module through a safe API. The current mapping invariants are detailed
in [Mapped segment safety](mapped-segment-safety.md).

Submission transfers ownership of a device buffer to its request. Completion
returns that ownership. A buffer cannot return to its pool while a kernel or
foreign device may still access it. Queue pairs are owned by one worker and are
not `Send` or `Sync` unless both the backend and scheduler prove that safe.

## Verification status

| Check | Current status |
|---|---|
| Format, strict Clippy, strict rustdoc | Required by pull-request CI |
| Debug tests | Required on Linux and macOS |
| Optimized tests | Required on Linux |
| Decoder and format round-trip fuzzing | Two checked-in targets; scheduled weekly |
| Deterministic write/flush/marker crash boundaries | Separate volatile/durable images cover every singleton/cohort operation-effect boundary, including before the first effect, with FIFO and reversed completion delivery |
| Filesystem restart and compaction recovery | Covered by Store integration tests |
| Miri on pure safe crates | Weekly pinned-nightly workflow; mappings remain out of scope |
| Address sanitizer on mapping/device/store tests | Weekly pinned-nightly Linux workflow |
| Golden durable-format byte fixtures | Checked in for record v1, aligned extent headers, manifest, journal, and superblock |
| LLVM line/region coverage | Weekly workspace summary; diagnostic only |

Generic line coverage is not a substitute for crash-boundary and parser-state
coverage. Minimized fuzz failures should become normal regression tests when the
failure represents a stable invariant.
