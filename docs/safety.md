# Safety policy

Unsafe Rust is forbidden by default at workspace level. Future FFI crates may
override that lint locally only after documenting their safety boundary.

Permitted unsafe areas are limited to:

- SPDK and DPDK FFI
- DMA and hugepage allocation
- memory mapping
- carefully reviewed lock-free rings

Every unsafe block must state the invariants required for memory validity,
aliasing, alignment, thread ownership, and lifetime. Raw pointers may not escape
the owning module through a safe API.

Submission transfers ownership of a DMA buffer to the device request.
Completion returns that ownership. A buffer cannot return to its pool while a
foreign device may still access it.

Queue pairs are owned by one pinned worker and are not `Send` or `Sync` unless
the underlying API and Punah's scheduling model prove that safe.

Required verification includes layout assertions, golden FFI tests, fuzzing of
record/protocol decoders, crash testing, sanitizers where applicable, and Miri
for portable safe components.
