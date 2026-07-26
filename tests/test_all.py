#!/usr/bin/env python3
"""
Pond Test Suite — single pytest entry point

Runs ALL tests: property tests, differential tests, hazard tests,
lab tracks, architecture laws, and lens laws.

Usage:
    pytest tests/test_all.py -v
    # or just:
    python -m pytest tests/test_all.py -v
"""

import os
import sys
import subprocess

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run_script(script_path):
    """Run a Python script as a subprocess and return (success, output)."""
    full_path = os.path.join(REPO_ROOT, script_path)
    result = subprocess.run(
        [sys.executable, full_path],
        capture_output=True, text=True, timeout=300, cwd=REPO_ROOT,
    )
    return result.returncode == 0, result.stdout + result.stderr


def test_property_tests():
    ok, output = _run_script("scripts/phase_l_property_tests.py")
    assert ok, f"Property tests failed:\n{output[-500:]}"

def test_git_differential():
    ok, output = _run_script("scripts/phase_l_differential_git.py")
    assert ok, f"Git differential tests failed:\n{output[-500:]}"

def test_hazard_simulator():
    ok, output = _run_script("scripts/phase_l_hazard_simulator.py")
    assert ok, f"Hazard simulator failed:\n{output[-500:]}"

def test_untested_laws():
    ok, output = _run_script("scripts/phase_n_untested_laws.py")
    assert ok, f"Untested laws failed:\n{output[-500:]}"

def test_additional_hazards():
    ok, output = _run_script("scripts/phase_n_additional_hazards.py")
    assert ok, f"Additional hazards failed:\n{output[-500:]}"

def test_remaining_laws():
    ok, output = _run_script("scripts/phase_o_remaining_laws.py")
    assert ok, f"Remaining laws failed:\n{output[-500:]}"

def test_remaining_hazards():
    ok, output = _run_script("scripts/phase_o_remaining_hazards.py")
    assert ok, f"Remaining hazards failed:\n{output[-500:]}"

def test_architecture_laws():
    ok, output = _run_script("tests/architecture/architecture_laws.py")
    assert ok, f"Architecture laws failed:\n{output[-500:]}"

def test_lakehouse():
    ok, output = _run_script("lenses/lakehouse/lakehouse_lens.py")
    assert ok, f"Lakehouse tests failed:\n{output[-500:]}"

def test_feature_store_lens():
    ok, output = _run_script("pond-labs/lenses/feature_store_lens.py")
    assert ok, f"Feature Store Lens failed:\n{output[-500:]}"

def test_interop_demo():
    ok, output = _run_script("pond-labs/demos/interop_demo.py")
    assert ok, f"Interop demo failed:\n{output[-500:]}"

def test_loc_benchmark():
    ok, output = _run_script("pond-labs/benchmarks/loc_benchmark.py")
    assert ok, f"LOC benchmark failed:\n{output[-500:]}"

def test_track1_compat():
    ok, output = _run_script("pond-labs/tracks/track1_compat_matrix.py")
    assert ok, f"Track 1 failed:\n{output[-500:]}"

def test_track2_index_portability():
    ok, output = _run_script("pond-labs/tracks/track2_index_portability.py")
    assert ok, f"Track 2 failed:\n{output[-500:]}"

def test_track7_reverse():
    ok, output = _run_script("pond-labs/tracks/track7_reverse_composability.py")
    assert ok, f"Track 7 failed:\n{output[-500:]}"

def test_track8_storage_independence():
    ok, output = _run_script("pond-labs/tracks/track8_storage_independence.py")
    assert ok, f"Track 8 failed:\n{output[-500:]}"

def test_track9_production_lakehouse():
    ok, output = _run_script("pond-labs/tracks/track9_production_lakehouse.py")
    assert ok, f"Track 9 failed:\n{output[-500:]}"

def test_pruning():
    ok, output = _run_script("tests/integration/test_pruning.py")
    assert ok, f"Pruning tests failed:\n{output[-500:]}"

def test_lakehouse_pruning():
    ok, output = _run_script("tests/integration/test_lakehouse_pruning.py")
    assert ok, f"Lakehouse pruning tests failed:\n{output[-500:]}"

def test_column_chunk_pruning_benchmark():
    ok, output = _run_script("pond-labs/benchmarks/column_chunk_pruning_benchmark.py")
    assert ok, f"Column-chunk pruning benchmark failed:\n{output[-500:]}"

def test_column_chunk_storage():
    ok, output = _run_script("tests/integration/test_column_chunk_storage.py")
    assert ok, f"Column-chunk storage tests failed:\n{output[-500:]}"

def test_column_chunk_storage_benchmark():
    ok, output = _run_script("pond-labs/benchmarks/column_chunk_storage_benchmark.py")
    assert ok, f"Column-chunk storage benchmark failed:\n{output[-500:]}"

def test_encoded_pruning():
    ok, output = _run_script("tests/integration/test_encoded_pruning.py")
    assert ok, f"Encoded pruning tests failed:\n{output[-500:]}"

def test_encoded_pruning_benchmark():
    ok, output = _run_script("pond-labs/benchmarks/encoded_pruning_benchmark.py")
    assert ok, f"Encoded pruning benchmark failed:\n{output[-500:]}"

def test_kv_pruning_and_projection():
    ok, output = _run_script("tests/integration/test_kv_pruning_and_projection.py")
    assert ok, f"KV pruning + projection tests failed:\n{output[-500:]}"

def test_collection_metadata():
    ok, output = _run_script("tests/integration/test_collection_metadata.py")
    assert ok, f"Collection metadata tests failed:\n{output[-500:]}"

def test_index_modes():
    ok, output = _run_script("tests/integration/test_index_modes.py")
    assert ok, f"Index modes tests failed:\n{output[-500:]}"

def test_transport_production():
    ok, output = _run_script("services/transport/transport_production.py")
    assert ok, f"Transport tests failed:\n{output[-500:]}"

def test_schema_registry():
    ok, output = _run_script("services/schema/schema_registry.py")
    assert ok, f"Schema Registry failed:\n{output[-500:]}"

def test_replication_coordinator():
    ok, output = _run_script("services/replication/replication_coordinator.py")
    assert ok, f"Replication Coordinator failed:\n{output[-500:]}"

def test_knowledge_graph_coverage():
    ok, output = _run_script("scripts/verify_knowledge_graph.py")
    assert ok, f"KG coverage check failed:\n{output[-500:]}"
