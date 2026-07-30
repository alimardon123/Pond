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


def test_loc_benchmark():
    ok, output = _run_script("pond-labs/benchmarks/loc_benchmark.py")
    assert ok, f"LOC benchmark failed:\n{output[-500:]}"




def test_bitpack_compression_benchmark():
    ok, output = _run_script("pond-labs/benchmarks/bitpack_compression_benchmark.py")
    assert ok, f"Bitpack compression benchmark failed:\n{output[-500:]}"



def test_pond_config():
    ok, output = _run_script("tests/integration/test_pond_config.py")
    assert ok, f"Pond config tests failed:\n{output[-500:]}"



def test_polars_adapter_demo():
    ok, output = _run_script("pond-labs/demos/polars_adapter_demo.py")
    assert ok, f"Polars adapter demo failed:\n{output[-500:]}"


def test_streaming_lens_demo():
    ok, output = _run_script("pond-labs/demos/streaming_lens_demo.py")
    assert ok, f"Streaming lens demo failed:\n{output[-500:]}"





def test_schema_registry():
    ok, output = _run_script("services/schema/schema_registry.py")
    assert ok, f"Schema Registry failed:\n{output[-500:]}"

def test_replication_coordinator():
    ok, output = _run_script("services/replication/replication_coordinator.py")
    assert ok, f"Replication Coordinator failed:\n{output[-500:]}"

def test_knowledge_graph_coverage():
    ok, output = _run_script("scripts/verify_knowledge_graph.py")
    assert ok, f"KG coverage check failed:\n{output[-500:]}"
