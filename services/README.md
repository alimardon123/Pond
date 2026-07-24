# services/

Cross-cutting services built on the kernel.

These are not Lenses (they don't interpret bytes). They are services
that sit between the kernel and the Lenses, providing capabilities the
kernel deliberately doesn't have.

## What's here

| Service | Purpose | Algebra |
|---|---|---|
| `transport/` | Compression + encryption + checksumming | §17 Transport Algebra |
| `schema/` | Versioned schema registry | §18 Schema Evolution Algebra |
| `replication/` | Primary-secondary replication + 2PC coordinator | §16 Replication Algebra |

## transport/

The Transport Layer sits between the Kernel (raw bytes) and the Lens
(interpreted state). It handles:
- Compression (zstd in production; zlib in reference)
- Encryption (AES-GCM in production; XOR in reference)
- Checksumming (AES-GCM tags)
- Block index for range reads
- Envelope encryption (master key wraps DEKs)

**Files:**
- `transport.py` — reference implementation (zlib + XOR, for clarity)
- `transport_production.py` — production (zstd + AES-GCM + per-block random nonces)

**Layer order (A10):** compress → encrypt → checksum.

## schema/

The Schema Registry is a thin layer over the Names substrate. Per SE7:
"Schema Registry is a Naming convention. No new substrate, no new axiom."

**Files:**
- `schema_registry.py` — versioned schemas, backward/forward compatibility,
  migration (v_old → v_new via decode + re-encode)

Schemas are stored as blobs, referenced by `__schema/{name}/v{version}`.
Schema evolution is Parquet-native (missing columns → NULL).

## replication/

The Replication Coordinator implements the Replication Algebra (§16)
plus the A7 escape hatch (cross-Collection atomicity via 2PC).

**Files:**
- `replication_coordinator.py` — two coordinators:
  - `PrimarySecondaryCoordinator`: single-writer per Ref, tombstone
    barrier (G6), failover contract.
  - `TwoPhaseCommitCoordinator`: cross-Collection atomicity via 2PC,
    using only kernel primitives (no kernel changes).

## Usage

Each service is independent. Import what you need:

```python
# Transport
from services.transport.transport_production import ProductionTransportLayer

# Schema Registry
from services.schema.schema_registry import SchemaRegistry

# Replication
from services.replication.replication_coordinator import PrimarySecondaryCoordinator
```

## Dependencies

- `pond-core/` (kernel)
- `transport/`: `zstandard`, `cryptography` (production only)
- `schema/`: stdlib only
- `replication/`: stdlib only
