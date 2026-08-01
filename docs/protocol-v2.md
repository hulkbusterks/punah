# Punah binary protocol v2

Protocol v2 carries exact artifact, operation, and lease mechanisms. It does
not carry workflow plans or make orchestration decisions. Redgo-compatible v1
remains unchanged and can coexist on a listener because the first byte is the
protocol version.

## Frame layout

All integers are unsigned big-endian values. The fixed header is 32 bytes.

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 1 | version | `2` |
| 1 | 1 | opcode | operation listed below |
| 2 | 1 | status | `None` for requests; explicit outcome for responses |
| 3 | 1 | flags | opcode-specific, initially zero unless specified |
| 4 | 2 | reserved | must be zero; nonzero frames are rejected |
| 6 | 2 | header length | `32`; permits explicit future framing versions |
| 8 | 8 | request ID | opaque correlation ID copied into the response |
| 16 | 4 | key length | maximum 1 MiB |
| 20 | 4 | value length | maximum 64 MiB |
| 24 | 4 | TTL seconds | opcode-specific; zero means no requested expiry |
| 28 | 4 | CRC32-C | header bytes 0..28, then key, then value |
| 32 | key length | key | borrowed identity/descriptor bytes |
| ... | value length | value | borrowed payload bytes |

CRC32-C uses the shared `punah-checksum` implementation with hardware dispatch
where supported and a portable fallback. It detects accidental corruption; it
is not an authentication mechanism. TLS or QUIC must authenticate untrusted
network traffic.

The decoder is incremental but bounded. Before a complete header it requests
exactly 32 bytes. After parsing a valid bounded header it reports the exact
total length required. It consumes one frame and leaves trailing bytes for the
next frame.

## Opcodes

| Value | Name | Intended mechanism |
|---:|---|---|
| `0x20` | `ArtifactGet` | retrieve an exact immutable artifact |
| `0x21` | `ArtifactPut` | publish an immutable artifact |
| `0x22` | `OperationResolve` | resolve an exact operation ID to its result |
| `0x23` | `LeaseAcquire` | acquire computation ownership and fencing token |
| `0x24` | `LeaseRenew` | extend ownership held by a fencing token |
| `0x25` | `LeaseRelease` | relinquish ownership |
| `0x26` | `ResultPublish` | publish a result under a valid fencing token |

## Payload schemas

Requests set `status=None` and `flags=0`. Responses repeat the opcode and
`request_id` with an explicit status. Validation is bounded and does not read a
clock.

| Operation | Request key | Request value | TTL |
|---|---|---|---|
| `ArtifactGet` | `ArtifactId` (32 bytes) | empty | zero |
| `ArtifactPut` | claimed `ArtifactId` (32 bytes) | artifact bytes | zero |
| `OperationResolve` | `OperationId` (32 bytes) | empty | zero |
| `LeaseAcquire` | `OperationId` (32 bytes) | empty | positive lease seconds |
| `LeaseRenew` | `OperationId` (32 bytes) | `LeaseToken` (16 bytes) | positive lease seconds |
| `LeaseRelease` | `OperationId` (32 bytes) | `LeaseToken` (16 bytes) | zero |
| `ResultPublish` | `OperationId` (32 bytes) | token (16), result `ArtifactId` (32), result bytes | optional result TTL |

Artifact and result publication recompute BLAKE3-256 and reject a mismatched
claimed identity. Lease grant and renewal responses carry the 16-byte token;
their TTL reports remaining seconds rounded up. A held response has no token.

Artifacts and operation results have distinct stable physical keys:
`0x01 || ArtifactId` and `0x02 || OperationId`. These internal tags prevent
equal 32-byte values from aliasing across identity classes and never appear in
logical protocol keys.

The TCP connection layer reads the first version byte, then reads the selected
version's complete fixed header and bounded body before decoding. It therefore
tolerates arbitrary TCP fragmentation and can carry v1 and v2 frames on the
same connection without guessing from a partially consumed header. Transport
only selects framing; opcode validation, identity checks, clock use, and lease
semantics remain in the service layer.

## Status values

| Value | Name | Meaning |
|---:|---|---|
| `0x00` | `None` | request, no response outcome asserted |
| `0x01` | `Hit` | exact object/result found |
| `0x02` | `Miss` | exact object/result absent |
| `0x03` | `Stored` | publication accepted |
| `0x04` | `Deleted` | deletion/release completed where applicable |
| `0x05` | `LeaseGranted` | caller owns the returned fencing token |
| `0x06` | `LeaseHeld` | another caller currently owns the lease |
| `0x07` | `Conflict` | immutable value or fencing precondition conflicts |
| `0x08` | `Backpressure` | bounded service capacity is temporarily exhausted |
| `0x09` | `Invalid` | request shape or field is invalid |
| `0xff` | `Error` | internal failure without a more precise outcome |

Explicit `Hit` and `Miss` remove v1's ambiguity between an absent object and a
present zero-length value. Unknown opcodes, statuses, header lengths, and
nonzero reserved fields are rejected rather than guessed.

## Compatibility and evolution

V1 retains its exact 16-byte Redgo layout, opcodes, unsigned 24-bit TTL, and
payload-only checksum. V2 is a separate Rust module and does not change the v1
API. Future compatible additions should allocate flag bits or define
opcode-specific payload schemas. A change to the fixed header or checksum
coverage requires a new protocol version, not reinterpretation of reserved
bytes.
