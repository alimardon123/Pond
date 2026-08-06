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


def test_rust_python_roundtrip():
    """Verify the pond_rust PyO3 module (built from pond-rust/ workspace)
    can encode + decode PND2 blobs end-to-end from Python."""
    import os, sys
    rust_so = os.path.join(REPO_ROOT, "pond-rust", "target", "release",
                           "pond_rust.so")
    if not os.path.exists(rust_so):
        import pytest
        pytest.skip(f"pond_rust.so not built — run pond-rust/build.sh")
    sys.path.insert(0, os.path.dirname(rust_so))
    try:
        import pond_rust
        cols = [("id", [1, 2, 3, 4, 5]),
                ("name", ["alice", "bob", "carol", "dave", "eve"]),
                ("score", [1.5, 2.5, 3.5, 4.5, 5.5])]
        result = pond_rust.encode(cols, 5)
        assert result["blob"][:4] == b"PND2", "encode should produce PND2 magic"
        decoded = pond_rust.decode(result["blob"])
        assert decoded["id"] == [1, 2, 3, 4, 5]
        assert decoded["name"] == ["alice", "bob", "carol", "dave", "eve"]
        assert decoded["score"] == [1.5, 2.5, 3.5, 4.5, 5.5]
        # Projection pushdown
        proj = pond_rust.decode(result["blob"], columns=["id"])
        assert list(proj.keys()) == ["id"]
        # Predicate pushdown
        filt = pond_rust.decode(result["blob"], predicates=[("id", ">", 2)])
        assert filt["id"] == [3, 4, 5]
        assert filt["name"] == ["carol", "dave", "eve"]
    finally:
        sys.path.pop(0)


def test_rust_c_abi():
    """Verify the pond-core C ABI works end-to-end from a C program.
    Skips if cargo or cc is unavailable."""
    import os, shutil, subprocess
    # cargo may be in ~/.cargo/bin (not on PATH in some environments)
    cargo_bin = shutil.which("cargo")
    if cargo_bin is None:
        cargo_candidate = os.path.expanduser("~/.cargo/bin/cargo")
        if os.path.exists(cargo_candidate):
            cargo_bin = cargo_candidate
    if cargo_bin is None or not shutil.which("cc"):
        import pytest
        pytest.skip("cargo or cc not available — skipping C ABI test")
    rust_dir = os.path.join(REPO_ROOT, "pond-rust")
    static_lib = os.path.join(rust_dir, "target", "release", "libpond_core.a")
    if not os.path.exists(static_lib):
        # Build it
        subprocess.run([cargo_bin, "build", "--release", "-p", "pond_core"],
                       cwd=rust_dir, check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)
    test_bin = os.path.join(rust_dir, "target", "test_c_abi")
    test_src = os.path.join(rust_dir, "tests", "test_c_abi.c")
    # Compile: link the static lib directly (avoids pulling libpython via .so)
    cc_cmd = ["cc", test_src, "-I", os.path.join(rust_dir, "pond-core"),
              static_lib, "-lpthread", "-ldl", "-lm", "-o", test_bin]
    result = subprocess.run(cc_cmd, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, f"cc failed:\n{result.stderr}"
    # Run
    result = subprocess.run([test_bin], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, \
        f"C ABI test failed:\n{result.stdout}\n{result.stderr}"
    assert "ALL C ABI TESTS PASSED" in result.stdout, \
        f"C ABI test missing success marker:\n{result.stdout}"


def test_go_sdk():
    """Verify the Go SDK (sdk-go/) builds and its tests pass.
    Skips if Go or cargo is unavailable."""
    import os, shutil, subprocess
    # Locate go binary (may be in ~/.local/go/bin)
    go_bin = shutil.which("go")
    if go_bin is None:
        go_candidate = os.path.expanduser("~/.local/go/bin/go")
        if os.path.exists(go_candidate):
            go_bin = go_candidate
    cargo_bin = shutil.which("cargo")
    if cargo_bin is None:
        cargo_candidate = os.path.expanduser("~/.cargo/bin/cargo")
        if os.path.exists(cargo_candidate):
            cargo_bin = cargo_candidate
    if go_bin is None or cargo_bin is None:
        import pytest
        pytest.skip("go or cargo not available — skipping Go SDK test")

    rust_dir = os.path.join(REPO_ROOT, "pond-rust")
    sdk_go_dir = os.path.join(REPO_ROOT, "sdk-go")

    # Ensure libpond_core.a is built
    static_lib = os.path.join(rust_dir, "target", "release", "libpond_core.a")
    if not os.path.exists(static_lib):
        subprocess.run([cargo_bin, "build", "--release", "-p", "pond_core"],
                       cwd=rust_dir, check=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=600)

    # Ensure Python test blobs exist (the Go test decodes them for cross-lang compat)
    blob_dir = os.path.join(rust_dir, "tests", "test_blobs")
    if not os.path.isdir(blob_dir) or len(os.listdir(blob_dir)) == 0:
        # Generate them
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.join(REPO_ROOT, "pond-sdk") + ":" + \
                            os.path.join(rust_dir, "target", "release")
        subprocess.run(["python3",
                        os.path.join(rust_dir, "tests", "generate_test_blobs.py")],
                       cwd=REPO_ROOT, check=True, env=env, timeout=120,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    # Run `go test ./...` in sdk-go/
    env = dict(os.environ)
    env["PATH"] = os.path.dirname(go_bin) + ":" + env.get("PATH", "")
    result = subprocess.run([go_bin, "test", "-v", "./..."],
                            cwd=sdk_go_dir, capture_output=True, text=True,
                            env=env, timeout=300)
    assert result.returncode == 0, \
        f"go test failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
    assert "PASS" in result.stdout and "FAIL" not in result.stdout, \
        f"go test reported failures:\n{result.stdout}"
