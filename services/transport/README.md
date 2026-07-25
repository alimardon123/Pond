# services/transport/

The **Transport Layer** — compression + encryption + checksumming.

## What it is

Sits between the Kernel (raw bytes) and the Lens (interpreted state).
The Lens sees plaintext, uncompressed bytes. The Kernel stores
encrypted, compressed bytes with a block index for range reads.

Layer order (A10): **compress → encrypt → checksum**.

Implements the Transport Algebra (TR1–TR6):
- TR1  Dedup is broken under encryption (accepted trade-off)
- TR2  Dictionary is a content-addressed sidecar
- TR3  Transport sits below the Lens, above the Kernel
- TR4  Transport is optional per Collection
- TR5  Transport operates per-blob, not per-byte
- TR6  Block index is a Physical Structure (rebuildable)

## Files

| File | Class | Purpose |
|---|---|---|
| `transport.py` | `TransportLayer`, `KeyStore` | **Reference impl** — zlib + XOR. For test clarity. |
| `transport_production.py` | `ProductionTransportLayer` | **Production impl** — zstd + AES-GCM + per-block random nonces + envelope encryption (master key wraps DEKs). |

Both modules share the same API and the same algebra. Swap reference
for production by changing one import.

## Architecture

```
Lens (sees plaintext, uncompressed bytes)
    ↓  TransportLayer.write(blob, key)
Kernel (stores AES-GCM-encrypted, zstd-compressed bytes + block index)
    ↑  TransportLayer.read(ref, key)
Lens (sees plaintext, uncompressed bytes)
```

Transport depends only on `pond-core` (per `REPO_ORGANIZATION.md` §7).
It is NOT a kernel extension — it is a library that wraps kernel I/O.

## Usage

```python
from services.transport.transport_production import ProductionTransportLayer
from services.transport.transport import KeyStore

ks = KeyStore(master_key=...)
t  = ProductionTransportLayer(kernel, ks)
ref = t.write("my_collection", b"plaintext blob")
data = t.read("my_collection", ref)
```

## Dependencies

- `pond-core/` (kernel)
- Reference: stdlib only (`zlib`, `hashlib`)
- Production: `zstandard`, `cryptography`
