# scripts/

Verification scripts: property tests, hazard simulators, benchmarks, and
differential tests.

## What's here

### Active verification scripts (referenced by `tests/test_all.py`)

| File | Purpose | Checks |
|---|---|---|
| `phase_l_property_tests.py` | Property tests for all kernel axioms (A1-A10) and algebra laws | 491 |
| `phase_l_hazard_simulator.py` | Hazard simulator (read-after-write lag, partition, disk corruption, etc.) | 3 self-tests |
| `phase_l_differential_git.py` | Differential tests vs real Git | 45 |
| `phase_n_untested_laws.py` | Tests for M1-M4' (merge) + W1-W5 (workspace) | 23 |
| `phase_n_additional_hazards.py` | Partition + disk corruption hazards | 10 |
| `phase_o_remaining_laws.py` | MAN3, RR3/4, G2/4/5, REP2/4/5/6/8/9, TR4/5, SE1/2/3/4/7 | 48 |
| `phase_o_remaining_hazards.py` | Byzantine, hash collision, replay, concurrent compaction+replication | 13 |
| `verify_knowledge_graph.py` | Verifies every active file is documented in `KNOWLEDGE_GRAPH.md` | — |
| `benchmark_decode_paths.py` | Rust vs Python decode path benchmark | — |

### Shared config

| File | Purpose |
|---|---|
| `_r2_config.py` | Cloudflare R2 credentials helper (reads from env) |
| `r2_demo_history.json` | Demo data for R2 benchmarks |

### Archived

One-off scripts (50+ files: old benchmarks, R2 demos, Phase L/N/O/P/Q
tests, ad-hoc tests) have been moved to `archive/scripts/`. They are
preserved for reference but not part of the active test suite.

## Running

```bash
# All verification tests (via pytest)
pytest tests/test_all.py -v

# Individual scripts
python scripts/phase_l_property_tests.py
python scripts/phase_l_differential_git.py
python scripts/verify_knowledge_graph.py
```

## Total active checks: 633+, all passing
