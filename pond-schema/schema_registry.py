"""
Pond Schema Registry (Phase P.1)

A thin layer over the Names substrate that implements the Schema
Evolution Algebra (POND_FORMAL_ALGEBRAS.md §18).

Per SE7: "Schema Registry is a Naming convention. It uses the
existing Names substrate (Refs with prefix __schema/). No new
substrate, no new axiom."

This module demonstrates the algebra is buildable. It provides:
  - register_schema(name, version, schema) -> schema_hash
  - get_schema(name, version) -> schema
  - latest_version(name) -> version
  - list_versions(name) -> [versions]
  - resolve_decoder(name) -> (version, schema, decoder)

Schema storage:
  Schemas are JSON-serializable dicts stored as blobs.
  Refs: __schema/{name}/v{version} -> schema_hash
  Refs: __schema/{name}/latest -> schema_hash of latest version

Compatibility contracts (per SE1-SE4):
  - SE1: backward compat — new code reads old data (new fields have defaults)
  - SE2: forward compat — old code reads new data (unknown fields skipped)
  - SE3: writer schema recorded in key prefix or blob header
  - SE4: compatibility is Lens's responsibility (kernel doesn't enforce)

This module provides the *storage* and *lookup*; the Lens provides
the *decoder* and *compatibility policy*. The Registry is a library,
not a kernel extension.

Run tests:
    python pond-schema/schema_registry.py
"""

from __future__ import annotations

import os
import sys
import json
import time
import hashlib
import tempfile
import shutil
from typing import Optional, Callable, Any

# Make pond-core importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "pond-core"))
from pond_minimal import PondMinimal  # noqa: E402


# ---------------------------------------------------------------------------
# Schema Registry
# ---------------------------------------------------------------------------

class SchemaRegistry:
    """Thin layer over the Names substrate for schema storage.

    Conventions:
      __schema/{name}/v{version} -> schema_hash   (one ref per version)
      __schema/{name}/latest     -> schema_hash   (always the latest)

    Schemas are JSON dicts. The Registry hashes them with the same
    SHA-256 the kernel uses (A2), so identical schemas dedup.

    Per SE6: schemas are immutable. Once written, a (name, version)
    pair cannot be changed. The Registry enforces this by checking
    for an existing ref before writing.
    """

    def __init__(self, kernel: PondMinimal):
        self.kernel = kernel

    # ------------------------------------------------------------------
    # Internal naming helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _version_ref(name: str, version: int) -> str:
        return f"__schema/{name}/v{version}"

    @staticmethod
    def _latest_ref(name: str) -> str:
        return f"__schema/{name}/latest"

    # ------------------------------------------------------------------
    # Register / lookup
    # ------------------------------------------------------------------

    def register_schema(self, name: str, version: int,
                        schema: dict) -> str:
        """Register a schema version. Per SE6, schemas are immutable:
        if (name, version) already exists, the new schema must match
        the existing one exactly (otherwise raise).

        Returns the schema's content-addressed hash.
        Also updates __schema/{name}/latest if version is the highest.
        """
        ref = self._version_ref(name, version)
        schema_bytes = json.dumps(schema, sort_keys=True).encode()
        schema_hash = self.kernel.write(schema_bytes)

        existing = self.kernel.resolve(ref)
        if existing is not None:
            # SE6: schemas are immutable. Verify the new schema matches.
            if existing != schema_hash:
                raise ValueError(
                    f"Schema {name} v{version} already registered with "
                    f"different content (existing={existing[:8]}, "
                    f"new={schema_hash[:8]}). Schemas are immutable (SE6)."
                )
            # Same schema re-registered — idempotent, no-op
            return existing

        # New version — register it
        self.kernel.reference(ref, schema_hash)

        # Update 'latest' if this is the highest version
        latest_ref = self._latest_ref(name)
        current_latest = self.kernel.resolve(latest_ref)
        if current_latest is None or version > self._version_from_ref(
                name, current_latest):
            self.kernel.reference(latest_ref, schema_hash)

        return schema_hash

    def _version_from_ref(self, name: str, schema_hash: str) -> int:
        """Find which version a schema_hash corresponds to."""
        for v in range(1, 10000):  # bounded scan
            ref = self._version_ref(name, v)
            if self.kernel.resolve(ref) == schema_hash:
                return v
            if self.kernel.resolve(ref) is None:
                break
        return -1

    def get_schema(self, name: str, version: int) -> Optional[dict]:
        """Get a specific schema version. Returns None if not registered."""
        ref = self._version_ref(name, version)
        h = self.kernel.resolve(ref)
        if h is None:
            return None
        return json.loads(self.kernel.read(h))

    def get_schema_by_hash(self, schema_hash: str) -> dict:
        """Get a schema by its content-addressed hash."""
        return json.loads(self.kernel.read(schema_hash))

    def latest_version(self, name: str) -> Optional[int]:
        """Get the latest version number for a schema name."""
        latest_ref = self._latest_ref(name)
        h = self.kernel.resolve(latest_ref)
        if h is None:
            return None
        return self._version_from_ref(name, h)

    def list_versions(self, name: str) -> list[int]:
        """List all registered versions for a schema name."""
        versions = []
        for v in range(1, 10000):
            ref = self._version_ref(name, v)
            if self.kernel.resolve(ref) is None:
                break
            versions.append(v)
        return versions

    # ------------------------------------------------------------------
    # Decoder resolution (per SE3)
    # ------------------------------------------------------------------

    def resolve_decoder(self, name: str,
                        decoder_factory: Callable[[dict], Callable[[bytes], Any]]
                        ) -> tuple[int, dict, Callable[[bytes], Any]]:
        """Resolve the latest decoder for a schema name.

        Per SE3: the writer schema is recorded (in the version ref).
        Per SE1: the reader (latest) schema is what the Lens supports.
        Per SE4: the decoder_factory is provided by the Lens (compatibility
        is the Lens's responsibility).

        Returns (latest_version, latest_schema, decoder).
        """
        v = self.latest_version(name)
        if v is None:
            raise KeyError(f"No schemas registered for {name}")
        schema = self.get_schema(name, v)
        decoder = decoder_factory(schema)
        return (v, schema, decoder)

    def decode_with_writer_schema(self, name: str, data: bytes,
                                  writer_version: int,
                                  decoder_factory: Callable[[dict], Callable[[bytes], Any]]
                                  ) -> Any:
        """Decode data using the writer's schema version.

        Per SE3: the writer schema is recorded (in the version ref).
        Per SE4: the decoder_factory is provided by the Lens (compatibility
        is the Lens's responsibility).

        Returns the decoded value using the WRITER's schema. No field
        defaults are filled — the caller sees exactly what was written.
        """
        writer_schema = self.get_schema(name, writer_version)
        if writer_schema is None:
            raise KeyError(
                f"Schema {name} v{writer_version} not registered"
            )
        decoder = decoder_factory(writer_schema)
        return decoder(data)

    def decode_backward_compatible(self, name: str, data: bytes,
                                   writer_version: int,
                                   reader_schema: Optional[dict] = None,
                                   ) -> dict:
        """Per SE1 (backward compat): read old data with the latest
        (reader) schema. Missing fields are filled with defaults.

        If reader_schema is None, the latest registered schema is used.

        The data must be JSON (this is a reference implementation;
        a real Lens would provide its own decoder).
        """
        if reader_schema is None:
            v = self.latest_version(name)
            if v is None:
                raise KeyError(f"No schemas registered for {name}")
            reader_schema = self.get_schema(name, v)

        # Parse the data (it was written with writer_version's schema)
        parsed = json.loads(data)

        # Fill defaults for any field in reader_schema not in parsed
        result = {}
        for field, field_type in reader_schema.get("fields", {}).items():
            if field in parsed:
                result[field] = parsed[field]
            else:
                result[field] = _default_for_type(field_type)
        return result

    # ------------------------------------------------------------------
    # Migration (per §18.6)
    # ------------------------------------------------------------------

    def migrate(self, name: str, v_old: int, v_new: int,
                data: bytes,
                decoder_factory: Callable[[dict], Callable[[bytes], Any]],
                encoder_factory: Callable[[dict], Callable[[Any], bytes]]
                ) -> bytes:
        """Migrate data from schema v_old to v_new.

        Decode with v_old, re-encode with v_new. This is the compaction
        pattern from §18.6: expensive (full rewrite) but rare.
        """
        decoded = self.decode_with_writer_schema(
            name, data, v_old, decoder_factory
        )
        new_schema = self.get_schema(name, v_new)
        encoder = encoder_factory(new_schema)
        return encoder(decoded)


# ---------------------------------------------------------------------------
# Reference decoder/encoder factories (for testing)
# ---------------------------------------------------------------------------

def json_decoder_factory(schema: dict) -> Callable[[bytes], dict]:
    """A simple JSON decoder factory. The schema documents expected
    fields; the decoder fills missing fields with defaults (SE1)."""
    def decode(data: bytes) -> dict:
        parsed = json.loads(data)
        result = {}
        for field, field_type in schema.get("fields", {}).items():
            if field in parsed:
                result[field] = parsed[field]
            else:
                # SE1: default for missing field
                result[field] = _default_for_type(field_type)
        return result
    return decode


def json_encoder_factory(schema: dict) -> Callable[[dict], bytes]:
    """A simple JSON encoder factory. Encodes only fields in the schema."""
    def encode(value: dict) -> bytes:
        filtered = {
            k: v for k, v in value.items()
            if k in schema.get("fields", {})
        }
        return json.dumps(filtered, sort_keys=True).encode()
    return encode


def _default_for_type(type_name: str):
    if type_name == "int":
        return 0
    if type_name == "string":
        return ""
    if type_name == "bool":
        return False
    if type_name == "float":
        return 0.0
    return None


# ---------------------------------------------------------------------------
# Self-tests
# ---------------------------------------------------------------------------

def _self_test():
    """Verify the Schema Registry works end-to-end."""
    print("=== Schema Registry self-test ===")

    tmpdir = tempfile.mkdtemp(prefix="pond_schema_")
    try:
        kernel = PondMinimal(tmpdir)
        reg = SchemaRegistry(kernel)

        # Test 1: register v1
        v1_schema = {"fields": {"id": "int", "name": "string"}}
        h1 = reg.register_schema("user", 1, v1_schema)
        print(f"  [OK] registered user v1 -> {h1[:8]}")

        # Test 2: get v1 back
        retrieved = reg.get_schema("user", 1)
        assert retrieved == v1_schema, "v1 schema round-trip failed"
        print(f"  [OK] retrieved user v1")

        # Test 3: register v2 (adds email field)
        v2_schema = {"fields": {"id": "int", "name": "string", "email": "string"}}
        h2 = reg.register_schema("user", 2, v2_schema)
        assert h1 != h2, "different schemas must have different hashes"
        print(f"  [OK] registered user v2 -> {h2[:8]}")

        # Test 4: latest_version
        assert reg.latest_version("user") == 2
        print(f"  [OK] latest_version(user) = 2")

        # Test 5: list_versions
        versions = reg.list_versions("user")
        assert versions == [1, 2], f"expected [1, 2], got {versions}"
        print(f"  [OK] list_versions(user) = {versions}")

        # Test 6: SE6 immutability — re-registering v1 with same content is OK
        h1_again = reg.register_schema("user", 1, v1_schema)
        assert h1_again == h1, "idempotent re-register returns same hash"
        print(f"  [OK] SE6: idempotent re-register returns same hash")

        # Test 7: SE6 immutability — re-registering v1 with DIFFERENT content fails
        v1_modified = {"fields": {"id": "int", "name": "string", "extra": "int"}}
        try:
            reg.register_schema("user", 1, v1_modified)
            assert False, "should have raised"
        except ValueError as e:
            assert "immutable" in str(e)
            print(f"  [OK] SE6: re-register with different content rejected")

        # Test 8: SE1 backward compat — v2 decoder reads v1 data
        v1_data = json.dumps({"id": 42, "name": "alice"}).encode()
        decoded_v2 = reg.decode_backward_compatible(
            "user", v1_data, writer_version=1,
        )
        assert decoded_v2 == {"id": 42, "name": "alice", "email": ""}, \
            f"SE1 failed: {decoded_v2}"
        print(f"  [OK] SE1: v2 decoder reads v1 data (email defaults to '')")

        # Test 9: SE2 forward compat — v1 decoder reads v2 data
        v2_data = json.dumps({"id": 42, "name": "alice", "email": "a@b.c"}).encode()
        # v1 reader schema only knows id, name
        v1_reader_schema = {"fields": {"id": "int", "name": "string"}}
        decoded_v1 = reg.decode_backward_compatible(
            "user", v2_data, writer_version=2,
            reader_schema=v1_reader_schema,
        )
        assert decoded_v1 == {"id": 42, "name": "alice"}, \
            f"SE2 failed: {decoded_v1}"
        print(f"  [OK] SE2: v1 decoder reads v2 data (email skipped)")

        # Test 10: migration (§18.6)
        migrated = reg.migrate(
            "user", v_old=1, v_new=2, data=v1_data,
            decoder_factory=json_decoder_factory,
            encoder_factory=json_encoder_factory,
        )
        migrated_dict = json.loads(migrated)
        assert migrated_dict == {"id": 42, "name": "alice"}, \
            f"migration failed: {migrated_dict}"
        print(f"  [OK] migration: v1 data -> v2 encoding (no data loss)")

        # Test 11: SE7 — Schema Registry uses only Names substrate
        # Verify the kernel API is unchanged
        api = [m for m in dir(kernel) if not m.startswith("_")]
        has_schema_method = any("schema" in m.lower() for m in api)
        assert not has_schema_method, "SE7: kernel should not have schema methods"
        print(f"  [OK] SE7: Schema Registry uses only Names substrate (no kernel changes)")

        # Test 12: schemas are content-addressed (SE5)
        # Same schema content -> same hash
        h1_dup = kernel.write(json.dumps(v1_schema, sort_keys=True).encode())
        assert h1_dup == h1, "SE5: schemas are content-addressed"
        print(f"  [OK] SE5: schemas are content-addressed (same content -> same hash)")

        kernel.close()
        print("\nAll Schema Registry tests pass.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    _self_test()
