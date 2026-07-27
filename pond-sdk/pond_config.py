"""
Pond configuration — persistent pruning + encoding settings.

A .pond/config file lives at the root of a Pond repository and
configures the pruning/encoding behavior for all collections. This
lets users tune Pond for their workload without changing code.

Settings:
  [pruning]
  enabled = auto          # auto | true | false
                          # auto: enable on object stores (S3, NFS),
                          #        disable on local disk
  force = false           # override auto-detection

  [encoding]
  auto_select = true      # automatically pick the best encoding per column
  default = raw           # default encoding when auto_select fails:
                          # raw | rle | dict | bitpack

  [column_chunks]
  chunk_size = 1000       # rows per column chunk
  row_group_size = 10000  # rows per row group

  [bitpack]
  max_bitwidth = 32       # fall back to raw if values need more than 32 bits

Usage:
    # Create a config
    from pond_config import PondConfig
    config = PondConfig()
    config.pruning_enabled = "auto"
    config.chunk_size = 2000
    config.save("/path/to/repo/.pond/config")

    # Load a config
    config = PondConfig.load("/path/to/repo/.pond/config")

    # Use it
    if config.should_prune(is_object_store=True):
        use_pruning = True

The config file is JSON (human-readable, easy to edit). It lives at
.pond/config relative to the kernel's base_dir.

GENERIC: works for any workload. The config doesn't reference any
specific lens or format — it tunes the shared pruning/encoding
infrastructure that all lenses use.
"""

from __future__ import annotations

import json
import os
from typing import Optional, Any


class PondConfig:
    """Persistent configuration for Pond's pruning + encoding settings.

    Attributes:
        pruning_enabled: "auto" | "true" | "false"
        pruning_force: bool — override auto-detection
        encoding_auto_select: bool — automatically pick best encoding
        encoding_default: str — fallback encoding ("raw", "rle", "dict", "bitpack")
        chunk_size: int — rows per column chunk
        row_group_size: int — rows per row group
        bitpack_max_bitwidth: int — fall back to raw above this
    """

    DEFAULTS = {
        "pruning_enabled": "auto",
        "pruning_force": False,
        "encoding_auto_select": True,
        "encoding_default": "raw",
        "chunk_size": 1000,
        "row_group_size": 10000,
        "bitpack_max_bitwidth": 32,
    }

    def __init__(self):
        for key, val in self.DEFAULTS.items():
            setattr(self, key, val)

    @property
    def pruning_enabled(self) -> str:
        return self._pruning_enabled

    @pruning_enabled.setter
    def pruning_enabled(self, value: str):
        if value not in ("auto", "true", "false"):
            raise ValueError(f"pruning_enabled must be 'auto', 'true', or 'false', got {value!r}")
        self._pruning_enabled = value

    @property
    def pruning_force(self) -> bool:
        return self._pruning_force

    @pruning_force.setter
    def pruning_force(self, value: bool):
        self._pruning_force = bool(value)

    @property
    def encoding_auto_select(self) -> bool:
        return self._encoding_auto_select

    @encoding_auto_select.setter
    def encoding_auto_select(self, value: bool):
        self._encoding_auto_select = bool(value)

    @property
    def encoding_default(self) -> str:
        return self._encoding_default

    @encoding_default.setter
    def encoding_default(self, value: str):
        if value not in ("raw", "rle", "dict", "bitpack"):
            raise ValueError(f"encoding_default must be 'raw', 'rle', 'dict', or 'bitpack', got {value!r}")
        self._encoding_default = value

    @property
    def chunk_size(self) -> int:
        return self._chunk_size

    @chunk_size.setter
    def chunk_size(self, value: int):
        if value < 1:
            raise ValueError(f"chunk_size must be >= 1, got {value}")
        self._chunk_size = int(value)

    @property
    def row_group_size(self) -> int:
        return self._row_group_size

    @row_group_size.setter
    def row_group_size(self, value: int):
        if value < 1:
            raise ValueError(f"row_group_size must be >= 1, got {value}")
        self._row_group_size = int(value)

    @property
    def bitpack_max_bitwidth(self) -> int:
        return self._bitpack_max_bitwidth

    @bitpack_max_bitwidth.setter
    def bitpack_max_bitwidth(self, value: int):
        if value < 1 or value > 64:
            raise ValueError(f"bitpack_max_bitwidth must be 1-64, got {value}")
        self._bitpack_max_bitwidth = int(value)

    # Use __dict__ to store the actual values (with _ prefix)
    def __init__(self):
        self._pruning_enabled = self.DEFAULTS["pruning_enabled"]
        self._pruning_force = self.DEFAULTS["pruning_force"]
        self._encoding_auto_select = self.DEFAULTS["encoding_auto_select"]
        self._encoding_default = self.DEFAULTS["encoding_default"]
        self._chunk_size = self.DEFAULTS["chunk_size"]
        self._row_group_size = self.DEFAULTS["row_group_size"]
        self._bitpack_max_bitwidth = self.DEFAULTS["bitpack_max_bitwidth"]

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON storage."""
        return {
            "pruning": {
                "enabled": self._pruning_enabled,
                "force": self._pruning_force,
            },
            "encoding": {
                "auto_select": self._encoding_auto_select,
                "default": self._encoding_default,
            },
            "column_chunks": {
                "chunk_size": self._chunk_size,
                "row_group_size": self._row_group_size,
            },
            "bitpack": {
                "max_bitwidth": self._bitpack_max_bitwidth,
            },
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PondConfig":
        """Deserialize from a dict."""
        config = cls()
        pruning = d.get("pruning", {})
        if "enabled" in pruning:
            config.pruning_enabled = pruning["enabled"]
        if "force" in pruning:
            config.pruning_force = pruning["force"]
        encoding = d.get("encoding", {})
        if "auto_select" in encoding:
            config.encoding_auto_select = encoding["auto_select"]
        if "default" in encoding:
            config.encoding_default = encoding["default"]
        cc = d.get("column_chunks", {})
        if "chunk_size" in cc:
            config.chunk_size = cc["chunk_size"]
        if "row_group_size" in cc:
            config.row_group_size = cc["row_group_size"]
        bp = d.get("bitpack", {})
        if "max_bitwidth" in bp:
            config.bitpack_max_bitwidth = bp["max_bitwidth"]
        return config

    def save(self, path: str) -> None:
        """Save config to a JSON file (typically .pond/config)."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2, sort_keys=True)

    @classmethod
    def load(cls, path: str) -> "PondConfig":
        """Load config from a JSON file. Returns defaults if file doesn't exist."""
        if not os.path.exists(path):
            return cls()  # defaults
        with open(path) as f:
            return cls.from_dict(json.load(f))

    @classmethod
    def load_for_kernel(cls, base_dir: str) -> "PondConfig":
        """Load config for a kernel with the given base_dir.

        Looks for .pond/config in the base_dir. Returns defaults if not found.
        """
        return cls.load(os.path.join(base_dir, ".pond", "config"))

    def should_prune(self, is_object_store: bool = False) -> bool:
        """Decide whether to enable pruning based on config + storage type.

        Args:
            is_object_store: True if the kernel is backed by an object
                store (S3, GCS, NFS). False for local disk.

        Returns:
            True if pruning should be enabled.
        """
        if self._pruning_force:
            return True
        if self._pruning_enabled == "true":
            return True
        if self._pruning_enabled == "false":
            return False
        # "auto" — enable on object stores, disable on local disk
        return is_object_store

    def get_encoding_hints(self, columns: list[str]) -> dict[str, str]:
        """Get encoding hints for a set of columns.

        If auto_select is True, returns {} (let the encoder pick).
        If auto_select is False, returns {col: default_encoding} for all columns.
        """
        if self._encoding_auto_select:
            return {}
        return {col: self._encoding_default for col in columns}

    def __repr__(self) -> str:
        return (f"PondConfig(pruning={self._pruning_enabled}, "
                f"encoding={'auto' if self._encoding_auto_select else self._encoding_default}, "
                f"chunk_size={self._chunk_size}, "
                f"row_group_size={self._row_group_size})")
