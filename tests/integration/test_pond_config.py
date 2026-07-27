#!/usr/bin/env python3
"""
Test: PondConfig — persistent pruning + encoding settings.

Verifies:
  1. Default values match expectations
  2. Save/load round-trip preserves all settings
  3. should_prune() respects auto/true/false + force
  4. get_encoding_hints() respects auto_select
  5. Validation rejects invalid values
  6. load_for_kernel() finds .pond/config

Run:
    python tests/integration/test_pond_config.py
"""

from __future__ import annotations

import os
import sys
import shutil
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from pond_config import PondConfig


def test_defaults():
    """Default values match expectations."""
    print("=" * 60)
    print("PondConfig: defaults")
    print("=" * 60)
    config = PondConfig()
    assert config.pruning_enabled == "auto"
    assert config.pruning_force is False
    assert config.encoding_auto_select is True
    assert config.encoding_default == "raw"
    assert config.chunk_size == 1000
    assert config.row_group_size == 10000
    assert config.bitpack_max_bitwidth == 32
    print(f"  [OK] Defaults: {config}")


def test_save_load_roundtrip():
    """Save/load round-trip preserves all settings."""
    print("\n" + "=" * 60)
    print("PondConfig: save/load round-trip")
    print("=" * 60)
    config = PondConfig()
    config.pruning_enabled = "true"
    config.pruning_force = True
    config.encoding_auto_select = False
    config.encoding_default = "bitpack"
    config.chunk_size = 2000
    config.row_group_size = 5000
    config.bitpack_max_bitwidth = 16

    tmpdir = tempfile.mkdtemp(prefix="pond_config_")
    try:
        config_path = os.path.join(tmpdir, ".pond", "config")
        config.save(config_path)
        assert os.path.exists(config_path), "Config file not created"
        print(f"  [OK] Saved to {config_path}")

        loaded = PondConfig.load(config_path)
        assert loaded.pruning_enabled == "true"
        assert loaded.pruning_force is True
        assert loaded.encoding_auto_select is False
        assert loaded.encoding_default == "bitpack"
        assert loaded.chunk_size == 2000
        assert loaded.row_group_size == 5000
        assert loaded.bitpack_max_bitwidth == 16
        print(f"  [OK] Loaded: {loaded}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_should_prune():
    """should_prune respects auto/true/false + force."""
    print("\n" + "=" * 60)
    print("PondConfig: should_prune")
    print("=" * 60)

    config = PondConfig()

    # auto: enable on object store, disable on local
    config.pruning_enabled = "auto"
    assert config.should_prune(is_object_store=True) is True
    assert config.should_prune(is_object_store=False) is False
    print(f"  [OK] auto: object_store=True → prune, local → no prune")

    # true: always prune
    config.pruning_enabled = "true"
    assert config.should_prune(is_object_store=True) is True
    assert config.should_prune(is_object_store=False) is True
    print(f"  [OK] true: always prune")

    # false: never prune
    config.pruning_enabled = "false"
    assert config.should_prune(is_object_store=True) is False
    assert config.should_prune(is_object_store=False) is False
    print(f"  [OK] false: never prune")

    # force: overrides everything
    config.pruning_enabled = "false"
    config.pruning_force = True
    assert config.should_prune(is_object_store=False) is True
    print(f"  [OK] force=True: overrides 'false'")


def test_encoding_hints():
    """get_encoding_hints respects auto_select."""
    print("\n" + "=" * 60)
    print("PondConfig: encoding hints")
    print("=" * 60)

    config = PondConfig()
    cols = ["age", "name", "region"]

    # auto_select=True → empty hints (let encoder pick)
    config.encoding_auto_select = True
    hints = config.get_encoding_hints(cols)
    assert hints == {}
    print(f"  [OK] auto_select=True → empty hints")

    # auto_select=False → use default for all columns
    config.encoding_auto_select = False
    config.encoding_default = "dict"
    hints = config.get_encoding_hints(cols)
    assert hints == {"age": "dict", "name": "dict", "region": "dict"}
    print(f"  [OK] auto_select=False → dict for all: {hints}")


def test_validation():
    """Validation rejects invalid values."""
    print("\n" + "=" * 60)
    print("PondConfig: validation")
    print("=" * 60)

    config = PondConfig()
    try:
        config.pruning_enabled = "invalid"
        assert False, "Should have raised"
    except ValueError:
        print(f"  [OK] Invalid pruning_enabled rejected")

    try:
        config.encoding_default = "invalid"
        assert False, "Should have raised"
    except ValueError:
        print(f"  [OK] Invalid encoding_default rejected")

    try:
        config.chunk_size = 0
        assert False, "Should have raised"
    except ValueError:
        print(f"  [OK] chunk_size=0 rejected")

    try:
        config.bitpack_max_bitwidth = 128
        assert False, "Should have raised"
    except ValueError:
        print(f"  [OK] bitpack_max_bitwidth=128 rejected")


def test_load_for_kernel():
    """load_for_kernel finds .pond/config in base_dir."""
    print("\n" + "=" * 60)
    print("PondConfig: load_for_kernel")
    print("=" * 60)

    tmpdir = tempfile.mkdtemp(prefix="pond_kernel_config_")
    try:
        # No config → defaults
        config = PondConfig.load_for_kernel(tmpdir)
        assert config.chunk_size == 1000
        print(f"  [OK] No .pond/config → defaults: {config}")

        # Create config
        config.chunk_size = 5000
        config.save(os.path.join(tmpdir, ".pond", "config"))

        # Load it
        loaded = PondConfig.load_for_kernel(tmpdir)
        assert loaded.chunk_size == 5000
        print(f"  [OK] .pond/config found: {loaded}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    test_defaults()
    test_save_load_roundtrip()
    test_should_prune()
    test_encoding_hints()
    test_validation()
    test_load_for_kernel()
    print("\n" + "=" * 60)
    print("ALL POND_CONFIG TESTS PASSED")
    print("=" * 60)
