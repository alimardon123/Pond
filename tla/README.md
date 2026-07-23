# Pond TLA+ Specification

This directory contains the TLA+ specification of the Pond kernel,
used in Phase N.2 to formally verify the kernel axioms.

## Files

- `PondKernel.tla` — the TLA+ specification of the kernel's three
  primitives (Write, Read, Ref) and six invariants.
- `PondKernel.cfg` — TLC model configuration (small finite sets for
  model checking).

## Running the model checker

The TLA+ tools (TLC) are not bundled with this repo. Download the
JAR from the TLA+ release page:

```bash
curl -sL -o tla2tools.jar \
  https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar
```

Then run TLC:

```bash
java -cp tla2tools.jar tlc2.TLC -config PondKernel.cfg PondKernel
```

Expected output (Phase N.2 result):

```
Model checking completed. No error has been found.
623 states generated, 56 distinct states found, 0 states left on queue.
```

## What is proven

Six invariants hold in all 56 reachable states:

- `TypeInvariant` — blobSet and refMap are well-typed
- `A1_Immutability` — blobSet is append-only
- `A2_ContentAddressing` — Hash is injective
- `A4_ReferentialIntegrity` — every non-tombstone ref points to a written blob
- `C0_BlobImmutability` — Read(Write(b)) = b
- `C2_SingleRefAtomicity` — refMap[n] is a single value, never a "mix"

See `POND_PHASE_N_REPORT.md` for the full Phase N report.
