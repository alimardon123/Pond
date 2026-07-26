"""
Best-effort operation logging.

A tiny helper for "best-effort" operations — things that should succeed
but where failure should NOT crash the caller (e.g., zone-map computation
on a Parquet column that doesn't support min/max). The previous pattern
was `except Exception: pass`, which silently swallowed real bugs and
made "why is my lakehouse slow?" impossible to debug.

This module provides:
  - best_effort(operation: str, fn: Callable, *args, **kwargs) -> Any
    Runs fn(*args, **kwargs). On specific recoverable exceptions
    (AttributeError, KeyError, TypeError, ValueError, ImportError),
    logs a warning at DEBUG level via the stdlib `logging` module and
    returns None. On other exceptions (e.g., KeyboardInterrupt,
    MemoryError), re-raises.

  - warn_best_effort(operation: str, exc: Exception) -> None
    Logs a best-effort warning. Useful when a caller already has the
    exception and wants to log it without re-raising.

The warnings are emitted via the stdlib `logging` module under the
logger name "pond.best_effort". By default these are silent (DEBUG
level); users can enable them with:

    import logging
    logging.getLogger("pond.best_effort").setLevel(logging.DEBUG)

Or via the POND_DEBUG=1 environment variable, which enables DEBUG-level
logging for all Pond loggers.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, TypeVar

T = TypeVar("T")

# Module-level logger. Users can configure it via:
#   logging.getLogger("pond.best_effort").setLevel(logging.DEBUG)
_logger = logging.getLogger("pond.best_effort")

# Enable DEBUG-level logging if POND_DEBUG=1 is set in the environment.
if os.environ.get("POND_DEBUG", "") == "1":
    _logger.setLevel(logging.DEBUG)
    if not _logger.handlers:
        _handler = logging.StreamHandler()
        _handler.setFormatter(logging.Formatter(
            "[pond] %(levelname)s %(message)s"))
        _logger.addHandler(_handler)

# Exception types that are "expected" failures for best-effort operations.
# These are caught and logged; everything else is re-raised.
_RECOVERABLE_EXCEPTIONS = (
    AttributeError,    # missing attribute / method
    KeyError,          # missing key in dict / column not found
    TypeError,         # wrong type for operation
    ValueError,        # invalid value (e.g., can't compute min/max)
    ImportError,       # optional dependency not installed
    ArithmeticError,   # numeric issues (OverflowError, ZeroDivisionError)
)


def best_effort(operation: str, fn: Callable[..., T], *args, **kwargs) -> T | None:
    """Run fn(*args, **kwargs) as a best-effort operation.

    On a recoverable exception (AttributeError, KeyError, TypeError,
    ValueError, ImportError, ArithmeticError), logs a DEBUG warning
    and returns None. On other exceptions, re-raises.

    Args:
        operation: short description of the operation, for the log message.
            Example: "compute zone map for users.rg/999"
        fn: the callable to run
        *args, **kwargs: passed to fn

    Returns:
        The return value of fn(*args, **kwargs), or None if a recoverable
        exception was caught.
    """
    try:
        return fn(*args, **kwargs)
    except _RECOVERABLE_EXCEPTIONS as exc:
        warn_best_effort(operation, exc)
        return None


def warn_best_effort(operation: str, exc: Exception) -> None:
    """Log a best-effort warning.

    Args:
        operation: short description of the operation that failed
        exc: the exception that was caught
    """
    _logger.debug("best-effort '%s' failed: %s: %s",
                  operation, type(exc).__name__, exc)
