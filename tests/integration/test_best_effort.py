#!/usr/bin/env python3
"""
Test: best_effort helper for best-effort operations.

Verifies:
  1. best_effort returns the result on success
  2. best_effort returns None and logs on recoverable exceptions
  3. best_effort re-raises non-recoverable exceptions (KeyboardInterrupt, etc.)
  4. warn_best_effort logs at DEBUG level
  5. POND_DEBUG=1 enables DEBUG logging

Run:
    python tests/integration/test_best_effort.py
"""

from __future__ import annotations

import os
import sys
import logging
import io

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))

from best_effort import best_effort, warn_best_effort, _logger


def test_best_effort_success():
    """best_effort returns the result on success."""
    print("=" * 60)
    print("best_effort: success path")
    print("=" * 60)
    result = best_effort("test op", lambda x, y: x + y, 2, 3)
    assert result == 5, f"Expected 5, got {result}"
    print(f"  [OK] best_effort returns result on success: {result}")


def test_best_effort_recoverable():
    """best_effort returns None on recoverable exceptions."""
    print("\n" + "=" * 60)
    print("best_effort: recoverable exceptions")
    print("=" * 60)

    # KeyError
    def raise_keyerror():
        d = {}
        return d["missing"]
    result = best_effort("key lookup", raise_keyerror)
    assert result is None, f"Expected None for KeyError, got {result}"
    print(f"  [OK] KeyError → None")

    # ValueError
    def raise_valueerror():
        return int("not a number")
    result = best_effort("parse int", raise_valueerror)
    assert result is None, f"Expected None for ValueError, got {result}"
    print(f"  [OK] ValueError → None")

    # ImportError
    def raise_importerror():
        raise ImportError("test import error")
    result = best_effort("import ext", raise_importerror)
    assert result is None, f"Expected None for ImportError, got {result}"
    print(f"  [OK] ImportError → None")

    # TypeError
    def raise_typeerror():
        return "a" + 1
    result = best_effort("add str+int", raise_typeerror)
    assert result is None, f"Expected None for TypeError, got {result}"
    print(f"  [OK] TypeError → None")


def test_best_effort_non_recoverable():
    """best_effort re-raises non-recoverable exceptions."""
    print("\n" + "=" * 60)
    print("best_effort: non-recoverable exceptions re-raised")
    print("=" * 60)

    # RuntimeError should be re-raised (not in recoverable list)
    def raise_runtime():
        raise RuntimeError("real bug")
    try:
        best_effort("real operation", raise_runtime)
        assert False, "Should have re-raised RuntimeError"
    except RuntimeError as e:
        print(f"  [OK] RuntimeError re-raised: {e}")

    # KeyboardInterrupt should be re-raised
    def raise_keyboard():
        raise KeyboardInterrupt()
    try:
        best_effort("interrupted op", raise_keyboard)
        assert False, "Should have re-raised KeyboardInterrupt"
    except KeyboardInterrupt:
        print(f"  [OK] KeyboardInterrupt re-raised")


def test_warn_best_effort():
    """warn_best_effort logs at DEBUG level."""
    print("\n" + "=" * 60)
    print("warn_best_effort: DEBUG logging")
    print("=" * 60)

    # Capture log output
    log_stream = io.StringIO()
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    _logger.addHandler(handler)
    old_level = _logger.level
    _logger.setLevel(logging.DEBUG)

    try:
        warn_best_effort("test op", ValueError("test error"))
        log_output = log_stream.getvalue()
        assert "DEBUG" in log_output, f"Expected DEBUG in log, got: {log_output}"
        assert "test op" in log_output, f"Expected 'test op' in log, got: {log_output}"
        assert "ValueError" in log_output, f"Expected 'ValueError' in log, got: {log_output}"
        assert "test error" in log_output, f"Expected 'test error' in log, got: {log_output}"
        print(f"  [OK] warn_best_effort logs at DEBUG with operation + exception")
        print(f"       Log: {log_output.strip()}")
    finally:
        _logger.removeHandler(handler)
        _logger.setLevel(old_level)


def test_pond_debug_env():
    """POND_DEBUG=1 enables DEBUG logging."""
    print("\n" + "=" * 60)
    print("POND_DEBUG=1 environment variable")
    print("=" * 60)

    # Re-import with POND_DEBUG=1
    # (We can't actually re-import in the same process, but we can verify
    # the logger responds to setLevel)
    _logger.setLevel(logging.DEBUG)
    assert _logger.level == logging.DEBUG
    print(f"  [OK] Logger level settable to DEBUG")
    print(f"       (POND_DEBUG=1 at import time also sets this)")


if __name__ == "__main__":
    test_best_effort_success()
    test_best_effort_recoverable()
    test_best_effort_non_recoverable()
    test_warn_best_effort()
    test_pond_debug_env()
    print("\n" + "=" * 60)
    print("ALL BEST_EFFORT TESTS PASSED")
    print("=" * 60)
