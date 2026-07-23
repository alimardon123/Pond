# tla/

TLA+ formal specification of the Pond kernel.

## What it is

A formal specification of the kernel's three primitives (Write, Read,
Ref) plus tombstone operation, checked with the TLC model checker.

## Files

- `PondKernel.tla` — the TLA+ specification. Defines:
  - The kernel state (blobSet, refMap)
  - Three primitives (Write, Read, Ref) + Tombstone
  - 6 invariants (TypeInvariant, A1_Immutability, A2_ContentAddressing,
    A4_ReferentialIntegrity, C0_BlobImmutability, C2_SingleRefAtomicity)
- `PondKernel.cfg` — TLC model configuration (small finite sets for
  model checking: 3 byte values, 4 hashes, 2 names)
- `README.md` — how to run the model checker

## Result

TLC verifies all 6 invariants hold across all 56 reachable states:

```
Model checking completed. No error has been found.
623 states generated, 56 distinct states found, 0 states left on queue.
```

This proves the kernel axioms are **consistent** (not contradictory).
It does NOT prove the architecture is *correct* — that requires
external validation (see `docs/WHERE_POND_FAILS.md`).

## Running

The TLA+ tools are not bundled (4.3MB JAR). Download:

```bash
curl -sL -o tla/tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
cd tla
java -cp tla2tools.jar tlc2.TLC -config PondKernel.cfg PondKernel
```

See `README.md` for details.
