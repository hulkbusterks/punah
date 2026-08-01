# Code organization and functional core

## Direction of dependencies

Dependencies point inward toward stable domain types:

```text
types <- record <- engine <- server/control plane
             ^         ^
             |         |
          devices and transports (effectful adapters)
```

Domain crates must not depend on networking, storage hardware, process-global
configuration, or a particular executor.

## Module responsibilities

- `lib.rs` is a small public façade containing module declarations and curated
  re-exports. It does not accumulate implementation logic.
- A module owns one concept such as record coding, indexing, recovery, leases,
  segment lifecycle, or a device backend.
- Hardware-specific implementations live below explicit adapter modules and do
  not leak foreign types into domain APIs.
- Tests live beside the invariant they prove. Cross-component behavior belongs
  in workspace-level integration tests once a server exists.

A file approaching roughly 300 lines prompts a responsibility review; this is
a signal, not an automatic requirement to create tiny files.

## Functional core, imperative shell

Punah is a systems program, so mutation is necessary. We keep it deliberate:

- Parsing, validation, hashing, state-transition decisions, compaction
  selection, and recovery rules are pure functions whenever practical.
- Domain inputs are immutable values. A transformation returns a new value or
  an explicit result rather than mutating unrelated global state.
- Mutable ownership is localized to index shards, lease tables, queues, segment
  writers, and device adapters.
- Time, randomness, sequence allocation, and hardware completion are passed in
  as inputs instead of read implicitly inside domain logic.
- Errors are values with domain meaning; expected failures do not panic.

The portable runtime currently has one OS owner thread per storage shard; these
are the same topology dimension, not independent `shard_count` and
`worker_threads` knobs. The default is the largest power of two no greater than
the host's available parallelism. Linux polling deployments will pin these
owners explicitly.
- Panics are restricted to programmer invariants and are documented.

This is not an attempt to imitate a purely functional language on the hot path.
Owned mutable buffers and per-core state are appropriate when they make
lifetimes, allocation, and performance clearer.

## State machines

Asynchronous storage and networking use explicit enums for states and events.
Invalid transitions should be unrepresentable where practical and rejected
otherwise. Submission transfers buffer ownership; completion returns it.
If submission is rejected before the device accepts ownership, the complete
request and buffer are returned to the caller for retry or cancellation.

## Public API discipline

- Newtypes distinguish identifiers, offsets, lengths, generations, hashes, and
  tokens when mixing them would be dangerous.
- Persistent and wire-format types are not exposed as memory-layout promises.
- Crate exports are curated; implementation modules stay private by default.
- Compatibility is promised only after a format or API is explicitly marked
  stable.
