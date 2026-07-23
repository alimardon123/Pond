# scripts/

Test suites, hazard simulators, benchmarks, and differential tests.

## What's here

### Verification (Phase L) — 536 checks

| File | Purpose | Checks |
|---|---|---|
| `phase_l_property_tests.py` | Property tests for all kernel axioms (A1-A10) and algebra laws | 491 |
| `phase_l_hazard_simulator.py` | Hazard simulator (read-after-write lag, partition, disk corruption, etc.) | 3 self-tests |
| `phase_l_differential_git.py` | Differential tests vs real Git | 45 |

### Proofs and untested laws (Phase N) — 33 checks

| File | Purpose | Checks |
|---|---|---|
| `phase_n_untested_laws.py` | Tests for M1-M4' (merge) + W1-W5 (workspace) | 23 |
| `phase_n_additional_hazards.py` | Partition + disk corruption hazards | 10 |

### Remaining laws and hazards (Phase O) — 61 checks

| File | Purpose | Checks |
|---|---|---|
| `phase_o_remaining_laws.py` | MAN3, RR3/4, G2/4/5, REP2/4/5/6/8/9, TR4/5, SE1/2/3/4/7 | 48 |
| `phase_o_remaining_hazards.py` | Byzantine, hash collision, replay, concurrent compaction+replication | 13 |

### Real differential tests (Phase P) — 16 checks

| File | Purpose | Checks |
|---|---|---|
| `phase_p_real_differentials.py` | Real Dolt + Iceberg differential tests | 16 |

### Benchmarks (Phase Q)

| File | Purpose |
|---|---|
| `phase_q_benchmarks.py` | Head-to-head vs Git, Dolt, Iceberg (7 operations × 4 systems) |

## Total: 646 checks, all passing

## Running

```bash
# All verification tests
python scripts/phase_l_property_tests.py
python scripts/phase_l_differential_git.py

# Proofs and untested laws
python scripts/phase_n_untested_laws.py
python scripts/phase_n_additional_hazards.py

# Remaining laws and hazards
python scripts/phase_o_remaining_laws.py
python scripts/phase_o_remaining_hazards.py

# Real Dolt + Iceberg differentials (requires dolt binary + duckdb + pyiceberg)
PATH="/home/z/bin:$PATH" python scripts/phase_p_real_differentials.py

# Benchmarks (requires dolt binary + duckdb + pyarrow)
PATH="/home/z/bin:$PATH" python scripts/phase_q_benchmarks.py
```

## Dependencies

- `pond-core/`, `pond-sdk/`, `services/transport/`
- Phase P + Q: `dolt` binary, `duckdb`, `pyiceberg`, `pyarrow`
