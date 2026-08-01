# Exact operation identity

Punah identifies reusable computation with an exact, versioned operation
descriptor. It does not infer semantic equivalence and it does not represent or
schedule a workflow DAG.

The version-1 descriptor includes:

- an operation kind;
- an implementation, provider, or model identifier;
- ordered immutable dependency `ArtifactId`s;
- opaque canonical argument bytes;
- fixed-size environment and schema fingerprints;
- optional named fields whose byte names are strictly sorted and unique.

Every variable component is length-framed in a deterministic binary encoding.
The descriptor begins with a format magic and version, and its `OperationId` is
the full BLAKE3-256 digest of those canonical bytes. Strict field, collection,
and total-size limits bound server work before an identity is accepted.

## Client responsibility

The client SDK owns canonicalization within its application domain. For
example, an SDK must decide how JSON numbers, map ordering, model revisions,
tool versions, compiler flags, and relevant environment state become stable
bytes. Punah will not normalize JSON, reorder dependencies, guess omitted
defaults, or decide that two different prompts are equivalent.

Clients should include every input capable of changing a correct result and
exclude incidental state that cannot affect it. Too little state risks an
incorrect cache hit; too much state only reduces reuse. Domain SDKs should pin
their canonicalization rules with known vectors and place the rule/version in
the schema fingerprint.

The server receives the canonical descriptor and a claimed `OperationId`, then
recomputes and verifies the ID. A mismatch is rejected. This keeps the server
independent of changing model, agent, compiler, and tool schemas while ensuring
that an untrusted or buggy client cannot publish a result under a different
descriptor's identity.

## Deliberate boundary

Operation identity is a storage/cache mechanism. Workflow planning, semantic
matching, GPU scheduling, and predictive placement belong to an orchestrator or
an optional advisor. Punah remains correct when no workflow information exists.
