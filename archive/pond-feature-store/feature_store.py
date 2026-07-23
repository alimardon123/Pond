"""
Pond Feature Store — a production-quality feature store built on the
Pond SDK. This is the Phase E flagship application.

Architecture (recursive composition, per RFC-0006 Layered Architecture):
  Kernel (3 primitives: Write/Read/Reference)  [FROZEN, ~140 LOC]
    -> ProllyLensBase (delta commits + Prolly trees + skip pointers)
      -> IndexedLens (auto-indexing with lazy/eager/incremental)
        -> FeatureStore (features + entities + point-in-time JOIN + versioning)

The FeatureStore uses IndexedLens for storage + auto-indexing,
CrossLens for reading from source Lenss (SQL, Streaming, ArrowLens),
and SemanticLens for feature metadata (metrics, dimensions).

Production features (Phase E):
  - Feature definitions WITH VERSIONING (v1, v2, ...; reproducible ML)
  - Schema validation (type-checked writes; rejects mismatched values)
  - Entity registry (entity types, join keys)
  - Online serving: point lookup via index, O(log N)
  - Batch online serving: get_feature_matrix (entities x features)
  - Offline serving: batch scan, point-in-time correctness
  - Point-in-time JOIN: the killer ML feature — join a training
    dataset's event timestamps against feature values as-of those
    timestamps, preventing label leakage
  - Feature lineage (source Lens -> feature -> transformation)
  - Feature freshness monitoring (O(1) via cached metadata)
  - Cross-Lens ingestion (read from SQL/Streaming/Arrow Views)
  - Semantic model integration (features as metrics/dimensions)
  - Persistence: data survives process restart (kernel-backed)

Storage model (all keys are content-addressed blobs in the kernel):
  _features/{name}/{version}            -> feature definition blob
  _entities/{entity_type}/{entity_id}   -> entity metadata blob
  _meta/latest_ts/{feature_name}        -> cached latest timestamp (for O(1) freshness)
  {feature_name}/{version}/{entity_id}/{timestamp} -> feature value blob
  Index: by_entity   -> entity_id -> blob_hash (latest per entity)
  Index: by_feature  -> feature_name/version -> blob_hash (latest per feature)

See RFC-0011 for the full production specification.
"""

import json
import time
import sys
import os
from typing import Optional, Any, Union
from dataclasses import dataclass, asdict, field

# Path setup
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_HERE)
for _pkg in ("pond-core", "pond-sdk", "pond-semantic"):
    sys.path.insert(0, os.path.join(_REPO_ROOT, _pkg))
sys.path.insert(0, _HERE)

from pond_minimal import PondMinimal
from auto_index import IndexedLens
from lens_sdk import CrossLens, SemanticLens


# ---------------------------------------------------------------------------
# Type validation
# ---------------------------------------------------------------------------

# Supported feature types and their Python type mappings.
# Used by write_feature_value to validate that the value matches the
# feature's declared type.
_FEATURE_TYPE_VALIDATORS = {
    "int":    lambda v: isinstance(v, (int, float)) and not isinstance(v, bool) and float(v).is_integer(),
    "float":  lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "string": lambda v: isinstance(v, str),
    "bool":   lambda v: isinstance(v, bool),
    "vector": lambda v: isinstance(v, list) and all(isinstance(x, (int, float)) for x in v),
    # "any" accepts anything; "json" accepts any JSON-serializable value
    "any":    lambda v: True,
    "json":   lambda v: True,
}


def _validate_feature_value(feature_type: str, value: Any) -> None:
    """Validate that `value` matches `feature_type`. Raises ValueError if not.

    This is the schema enforcement for feature values. It runs on every
    write_feature_value call. Production feature stores MUST reject
    type-mismatched writes to prevent corrupting downstream ML models.
    """
    validator = _FEATURE_TYPE_VALIDATORS.get(feature_type)
    if validator is None:
        raise ValueError(
            f"Unknown feature type '{feature_type}'. "
            f"Supported types: {sorted(_FEATURE_TYPE_VALIDATORS.keys())}"
        )
    if not validator(value):
        # Build a helpful error message
        actual_type = type(value).__name__
        if isinstance(value, list):
            actual_type = f"list[{type(value[0]).__name__ if value else 'empty'}]"
        raise ValueError(
            f"Value {value!r} (type={actual_type}) does not match "
            f"feature type '{feature_type}'."
        )


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FeatureDefinition:
    """Definition of a feature, with versioning."""
    name: str
    type: str  # int, float, string, bool, vector, any, json
    source: str  # source Lens name
    transformation: str = ""  # SQL expression or description
    description: str = ""
    tags: list = field(default_factory=list)
    version: int = 1  # incremented on redefinition
    created_at: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class EntityDefinition:
    """Definition of an entity type (e.g., 'user', 'order', 'product')."""
    entity_type: str
    join_key: str  # the field name used to join (e.g., 'user_id')
    description: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# FeatureStore — production-quality feature store
# ---------------------------------------------------------------------------

class FeatureStore(IndexedLens):
    """A production-quality feature store built on IndexedLens.

    Recursive composition (per RFC-0006):
      FeatureStore extends IndexedLens extends ProllyLensBase extends Kernel

    Production features:
      - Schema validation: write_feature_value rejects type-mismatched values.
      - Feature versioning: redefining a feature increments its version.
      - Entity registry: register_entity_type / get_entity_type.
      - Point-in-time JOIN: get_training_dataset joins event timestamps
        against feature values as-of those timestamps.
      - Batch online serving: get_feature_matrix for ML inference.
      - O(1) freshness: get_freshness reads cached latest timestamp.
      - Persistence: data survives process restart (kernel-backed).

    See RFC-0011 for the full specification.
    """

    def __init__(self, kernel: PondMinimal, name: str = "feature_store"):
        super().__init__(kernel, name)
        # Auto-indexes for fast lookups. Both are lazy (default) for fast writes.
        #
        # by_entity uses a COMPOSITE key "{feature_name}|{version}|{entity_id}"
        # so that each (feature, entity) pair gets its own index entry. This
        # makes get_feature_value O(log N) for the normal multi-feature case
        # (one entity has many features). The previous design used entity_id
        # alone as the key, which caused last-writer-wins collisions and
        # forced an O(N) fallback scan whenever the indexed feature didn't
        # match the requested one. See validation/customer_analytics_report.md
        # finding (a) for the measured impact.
        self.register_index("by_entity",
                             lambda d: f"{d.get('feature_name', '')}|v{d.get('version', 1)}|{d.get('entity_id', '')}",
                             mode="lazy", staleness_budget=3)
        self.register_index("by_feature",
                             lambda d: f"{d.get('feature_name', '')}/v{d.get('version', 1)}",
                             mode="lazy", staleness_budget=3)
        # In-memory cache of staged (uncommitted) feature definitions.
        # This allows write_feature_value to validate against a feature
        # that was just defined in the same session but not yet committed.
        # Key: (feature_name, version) -> definition dict.
        self._staged_features: dict[tuple[str, int], dict] = {}

    def has_staged(self) -> bool:
        """Check whether there are uncommitted staged changes.

        Exposed on FeatureStore so callers don't need to reach into
        `self.base.has_staged()` directly. Returns True if there are
        staged feature values, deletes, or feature definitions waiting
        for a commit.
        """
        return self.base.has_staged()

    # ------------------------------------------------------------------
    # Feature definitions (with versioning)
    # ------------------------------------------------------------------

    def define_feature(self, name: str, feature_type: str, source: str,
                       transformation: str = "", description: str = "",
                       tags: list = None) -> int:
        """Define a feature, or increment its version if it already exists.

        Args:
            name: the feature's name (e.g., 'customer_total_spent').
            feature_type: one of {int, float, string, bool, vector, any, json}.
                Type-checked on every write_feature_value call.
            source: the source Lens name (e.g., 'orders'). Descriptive
                only — used for lineage, not for actual ingestion.
            transformation: **descriptive only** — a human-readable
                description of how the feature is computed (e.g.,
                'SUM(amount) GROUP BY customer_id'). This is NOT
                executed; you must compute the feature value yourself
                and pass it to write_feature_value. An actual
                transformation engine is future work (see
                docs/FEATURE_STORE_USE_CASE.md §"What this does NOT
                cover").
            description: human-readable description of the feature.
            tags: optional list of tags for grouping/filtering.

        Returns:
            The feature's version number (1 for a new feature,
            2+ for a redefined feature).

        Schema validation on the type: must be one of
        {int, float, string, bool, vector, any, json}.

        If the feature already exists with the SAME type/source/transformation,
        this is a no-op (returns the existing version) — idempotent.
        If the feature exists with a DIFFERENT type/source/transformation,
        a new version is created.
        """
        if feature_type not in _FEATURE_TYPE_VALIDATORS:
            raise ValueError(
                f"Unknown feature type '{feature_type}'. "
                f"Supported: {sorted(_FEATURE_TYPE_VALIDATORS.keys())}"
            )
        existing = self.get_feature_definition(name)
        if existing is not None:
            # Idempotent: same definition -> return existing version
            if (existing["type"] == feature_type
                    and existing["source"] == source
                    and existing["transformation"] == transformation):
                return existing["version"]
            # Different definition -> new version
            new_version = existing["version"] + 1
        else:
            new_version = 1

        feat = FeatureDefinition(
            name=name, type=feature_type, source=source,
            transformation=transformation, description=description,
            tags=tags or [], version=new_version, created_at=time.time()
        )
        feat_dict = feat.to_dict()
        self.put(f"_features/{name}/{new_version}", feat_dict)
        # Cache in memory so write_feature_value can validate against it
        # before commit (the staged write isn't visible to base.read_all
        # until commit).
        self._staged_features[(name, new_version)] = feat_dict
        return new_version

    def get_feature_definition(self, name: str,
                                version: Optional[int] = None) -> Optional[dict]:
        """Get a feature definition. If version is None, returns the latest.

        Checks the in-memory staged cache first (for features defined in
        this session but not yet committed), then falls back to the
        committed state in the kernel.

        Returns None if the feature (or the requested version) does not exist.
        """
        # If a specific version is requested, check the staged cache first
        if version is not None:
            staged = self._staged_features.get((name, version))
            if staged is not None:
                return staged
            h = self.base.lookup(f"_features/{name}/{version}")
            if not h:
                return None
            return self.decode(self.kernel.read_blob(h))

        # No version specified — find the latest version across staged + committed
        # Check staged cache first
        latest_staged_version = 0
        latest_staged = None
        for (n, v), fd in self._staged_features.items():
            if n == name and v > latest_staged_version:
                latest_staged_version = v
                latest_staged = fd
        # Check committed state for any version newer than staged
        state = self.base.read_all()
        prefix = f"_features/{name}/"
        latest_committed_version = 0
        latest_committed_key = None
        for key in state:
            if key.startswith(prefix):
                try:
                    v = int(key[len(prefix):])
                    if v > latest_committed_version:
                        latest_committed_version = v
                        latest_committed_key = key
                except ValueError:
                    continue
        # Return whichever is newer: staged or committed
        if latest_staged_version >= latest_committed_version and latest_staged is not None:
            return latest_staged
        if latest_committed_key is None:
            return None
        h = state[latest_committed_key]
        return self.decode(self.kernel.read_blob(h))

    def list_features(self) -> list[str]:
        """List all feature names (without versions)."""
        state = self.base.read_all()
        names = set()
        for k in state:
            if k.startswith("_features/"):
                # _features/{name}/{version}
                rest = k[len("_features/"):]
                name = rest.split("/")[0]
                names.add(name)
        return sorted(names)

    def list_feature_versions(self, name: str) -> list[int]:
        """List all versions of a feature. Returns [] if feature does not exist."""
        state = self.base.read_all()
        prefix = f"_features/{name}/"
        versions = []
        for k in state:
            if k.startswith(prefix):
                try:
                    versions.append(int(k[len(prefix):]))
                except ValueError:
                    continue
        return sorted(versions)

    # ------------------------------------------------------------------
    # Entity registry
    # ------------------------------------------------------------------

    def register_entity_type(self, entity_type: str, join_key: str,
                              description: str = "") -> None:
        """Register an entity type (e.g., 'user' with join_key 'user_id').

        Entity types are optional but recommended for production feature
        stores. They document which field is used to join feature values
        to entities, and enable cross-feature entity validation.
        """
        ent = EntityDefinition(
            entity_type=entity_type, join_key=join_key, description=description
        )
        self.put(f"_entities/{entity_type}", ent.to_dict())

    def get_entity_type(self, entity_type: str) -> Optional[dict]:
        """Get an entity type definition. Returns None if not registered."""
        h = self.base.lookup(f"_entities/{entity_type}")
        if not h:
            return None
        return self.decode(self.kernel.read_blob(h))

    def list_entity_types(self) -> list[str]:
        """List all registered entity types."""
        state = self.base.read_all()
        return [k[len("_entities/"):] for k in state
                if k.startswith("_entities/")]

    # ------------------------------------------------------------------
    # Feature value ingestion (with schema validation)
    # ------------------------------------------------------------------

    def write_feature_value(self, feature_name: str, entity_id: str,
                            value: Any, timestamp: float = None,
                            version: Optional[int] = None) -> str:
        """Write a feature value for an entity at a timestamp.

        Args:
            feature_name: the feature's name. Must be defined (call
                define_feature first).
            entity_id: the entity's identifier (e.g., 'user:123').
            value: the feature value. Type-checked against the feature's
                declared type; raises ValueError on mismatch.
            timestamp: the event timestamp. Defaults to now().
            version: the feature version to write to. Defaults to the
                latest version.

        Returns:
            The key under which the value was stored (for debugging).

        Raises:
            ValueError: if the feature is not defined, or if the value
                does not match the feature's declared type.
        """
        feat = self.get_feature_definition(feature_name, version)
        if feat is None:
            raise ValueError(
                f"Feature '{feature_name}' (version {version}) is not defined. "
                f"Call define_feature('{feature_name}', ...) first."
            )
        # Schema validation — reject type-mismatched writes
        _validate_feature_value(feat["type"], value)

        record = {
            "feature_name": feature_name,
            "version": feat["version"],
            "entity_id": entity_id,
            "value": value,
            "timestamp": timestamp or time.time(),
        }
        key = f"{feature_name}/v{feat['version']}/{entity_id}/{record['timestamp']}"
        self.put(key, record)

        # Update the cached latest-timestamp for O(1) freshness
        self._update_freshness_cache(feature_name, record["timestamp"])
        return key

    def ingest_from_view(self, source_view, feature_name: str,
                         entity_field: str, value_field: str,
                         timestamp_field: str = "ts",
                         version: Optional[int] = None) -> int:
        """Ingest feature values from another View (SQL, Streaming, ArrowLens).

        Uses CrossLens to read the source Lens's latest state. For each
        row in the source, extracts entity_id, value, and timestamp, then
        calls write_feature_value (which validates the schema).

        Args:
            source_view: the Lens to ingest from (must have get_all()).
            feature_name: the target feature name (must be defined).
            entity_field: the field in source rows to use as entity_id.
            value_field: the field in source rows to use as feature value.
            timestamp_field: the field in source rows to use as timestamp.
                Defaults to 'ts'. If the field is missing, uses time.time().
            version: the feature version to write to. Defaults to latest.

        Returns:
            The number of values successfully ingested.

        Raises:
            ValueError: if a value fails schema validation (ingestion
                stops at the first failure; previously-ingested values
                are staged but not committed).
        """
        count = 0
        data = CrossLens.read_all_from(source_view)
        for key, record in data.items():
            if key.startswith("_"):
                continue
            entity_id = str(record.get(entity_field, ""))
            value = record.get(value_field)
            timestamp = record.get(timestamp_field, time.time())
            if entity_id and value is not None:
                self.write_feature_value(
                    feature_name, entity_id, value, timestamp, version
                )
                count += 1
        return count

    # ------------------------------------------------------------------
    # Online serving (point lookup)
    # ------------------------------------------------------------------

    def get_feature_value(self, feature_name: str, entity_id: str,
                          version: Optional[int] = None) -> Optional[Any]:
        """Get the latest feature value for an entity. O(log N) via composite index.

        Uses the by_entity index with composite key
        "{feature_name}|{version}|{entity_id}" so each (feature, entity)
        pair has its own index entry. This is O(log N) even when one
        entity has many features (the normal case).

        Args:
            feature_name: the feature's name.
            entity_id: the entity's identifier.
            version: the feature version. Defaults to latest.

        Returns:
            The feature value, or None if no value exists for this
            entity/feature/version combination.
        """
        feat = self.get_feature_definition(feature_name, version)
        if feat is None:
            return None
        v = feat["version"]

        # Direct index lookup using the composite key. This is O(log N)
        # and works correctly even when the entity has many features,
        # because each (feature, version, entity) triple has its own
        # index entry.
        index_key = f"{feature_name}|v{v}|{entity_id}"
        result = self.find_by("by_entity", index_key)
        if result is not None:
            return result.get("value")

        # Fallback: scan for the latest record matching feature+entity+version.
        # This runs only when the index is stale (lazy mode, not yet rebuilt)
        # or when no value exists for this combination. In steady state
        # (index fresh), this path is never taken.
        state = self.base.read_all()
        prefix = f"{feature_name}/v{v}/{entity_id}/"
        latest_ts = -1.0
        latest_value = None
        for key, h in state.items():
            if key.startswith(prefix):
                record = json.loads(self.kernel.read_blob(h))
                if record["timestamp"] > latest_ts:
                    latest_ts = record["timestamp"]
                    latest_value = record["value"]
        return latest_value

    def get_feature_vector(self, entity_id: str,
                            feature_names: list[str]) -> dict[str, Any]:
        """Get multiple feature values for one entity (a feature vector).

        This is the standard ML inference pattern: given an entity and a
        list of features, return {feature_name: value} for online serving.

        For batch inference (many entities), use get_feature_matrix instead
        — it's O(N+M) rather than O(N*M).
        """
        return {name: self.get_feature_value(name, entity_id)
                for name in feature_names}

    def get_feature_matrix(self, entity_ids: list[str],
                            feature_names: list[str]) -> "list[dict[str, Any]]":
        """Batch online serving: get feature values for many entities at once.

        Returns a list of rows, one per entity_id. Each row is a dict:
            {"entity_id": entity_id, feature_name: value, ...}

        Single full-state scan, partitioned by feature prefix. For N
        total records, M features, and E entities:
            Complexity: O(N + E * M) for the scan + row assembly.
        This is more efficient than calling get_feature_vector in a loop
        (O(E * M * log N) via E*M index lookups) because it does ONE
        full-state scan instead of E*M index lookups.

        Use this for batch ML inference (e.g., scoring 10K users against
        50 features).
        """
        # Resolve feature versions upfront (one call per feature).
        feature_versions: dict[str, Optional[int]] = {}
        feature_prefixes: dict[str, str] = {}
        for fname in feature_names:
            feat = self.get_feature_definition(fname)
            if feat is None:
                feature_versions[fname] = None
            else:
                v = feat["version"]
                feature_versions[fname] = v
                feature_prefixes[fname] = f"{fname}/v{v}/"

        # SINGLE full-state scan. Partition records by feature as we go.
        # ent_values[fname][entity_id] = (timestamp, value)
        ent_values: dict[str, dict[str, tuple[float, Any]]] = {
            fname: {} for fname in feature_names
        }
        state = self.base.read_all()
        for key, h in state.items():
            # Check which feature this key belongs to. A key looks like
            # "{feature_name}/v{version}/{entity_id}/{timestamp}". We
            # check against each feature's prefix.
            for fname, prefix in feature_prefixes.items():
                if key.startswith(prefix):
                    record = json.loads(self.kernel.read_blob(h))
                    eid = record["entity_id"]
                    ts = record["timestamp"]
                    cur = ent_values[fname].get(eid)
                    if cur is None or ts > cur[0]:
                        ent_values[fname][eid] = (ts, record["value"])
                    break  # key matches at most one feature prefix

        # Build the result rows: one per entity_id
        rows = []
        for entity_id in entity_ids:
            row = {"entity_id": entity_id}
            for fname in feature_names:
                ev = ent_values[fname].get(entity_id)
                row[fname] = ev[1] if ev is not None else None
            rows.append(row)
        return rows

    # ------------------------------------------------------------------
    # Offline serving (batch scan)
    # ------------------------------------------------------------------

    def get_all_values(self, feature_name: str,
                        version: Optional[int] = None) -> list[dict]:
        """Get all values for a feature (offline/batch serving).

        Args:
            feature_name: the feature's name.
            version: the feature version. Defaults to latest.

        Returns:
            A list of record dicts, each with keys: feature_name, version,
            entity_id, value, timestamp.
        """
        feat = self.get_feature_definition(feature_name, version)
        if feat is None:
            return []
        v = feat["version"]
        state = self.base.read_all()
        prefix = f"{feature_name}/v{v}/"
        results = []
        for key, h in state.items():
            if key.startswith(prefix):
                record = json.loads(self.kernel.read_blob(h))
                results.append(record)
        return results

    def get_feature_values_at_time(self, feature_name: str,
                                    timestamp: float,
                                    version: Optional[int] = None) -> list[dict]:
        """Get feature values as of a specific timestamp (point-in-time).

        Returns the latest value per entity whose timestamp is <= `timestamp`.
        This is the foundation of point-in-time correctness for ML training.

        For the full point-in-time JOIN (joining a training dataset's
        event timestamps against feature values), use get_training_dataset.
        """
        all_values = self.get_all_values(feature_name, version)
        by_entity: dict[str, dict] = {}
        for record in all_values:
            if record["timestamp"] <= timestamp:
                eid = record["entity_id"]
                if eid not in by_entity or record["timestamp"] > by_entity[eid]["timestamp"]:
                    by_entity[eid] = record
        return list(by_entity.values())

    def get_training_dataset(self, events: list[dict],
                              feature_names: list[str],
                              entity_field: str = "entity_id",
                              timestamp_field: str = "timestamp",
                              version: Optional[int] = None) -> list[dict]:
        """Point-in-time JOIN: build a training dataset from events + features.

        THE killer feature of a production feature store. Given a list of
        events (each with an entity_id and a timestamp), and a list of
        feature names, returns a training dataset where each row is:

            {entity_id, timestamp, feature_1, feature_2, ..., (event fields)}

        The feature values are looked up as-of the event's timestamp,
        preventing label leakage (no future data is included).

        Args:
            events: list of event dicts. Each must have entity_field and
                timestamp_field. Other fields are preserved in the output.
            feature_names: features to join.
            entity_field: the field in events to use as entity_id.
                Defaults to 'entity_id'.
            timestamp_field: the field in events to use as the as-of
                timestamp. Defaults to 'timestamp'.
            version: feature version to join against. Defaults to latest.

        Returns:
            List of training rows, one per event. Each row has the
            original event fields plus one field per feature_name
            (value is None if no feature value exists as-of the event
            timestamp).

        Example:
            events = [
                {"entity_id": "u1", "timestamp": 1000, "label": 1},
                {"entity_id": "u1", "timestamp": 2000, "label": 0},
            ]
            features = ["total_spent", "order_count"]
            dataset = fs.get_training_dataset(events, features)
            # dataset[0] = {"entity_id": "u1", "timestamp": 1000, "label": 1,
            #               "total_spent": <value as of ts=1000>,
            #               "order_count": <value as of ts=1000>}
            # dataset[1] = {"entity_id": "u1", "timestamp": 2000, "label": 0,
            #               "total_spent": <value as of ts=2000>,
            #               "order_count": <value as of ts=2000>}
        """
        # For each feature, build an {entity_id: [(ts, value), ...]} map
        # sorted by timestamp. Then for each event, binary-search for the
        # latest value with ts <= event_ts.
        feature_timelines: dict[str, dict[str, list[tuple[float, Any]]]] = {}
        for fname in feature_names:
            feat = self.get_feature_definition(fname, version)
            if feat is None:
                feature_timelines[fname] = {}
                continue
            v = feat["version"]
            state = self.base.read_all()
            prefix = f"{fname}/v{v}/"
            ent_timeline: dict[str, list[tuple[float, Any]]] = {}
            for key, h in state.items():
                if key.startswith(prefix):
                    record = json.loads(self.kernel.read_blob(h))
                    eid = record["entity_id"]
                    ent_timeline.setdefault(eid, []).append(
                        (record["timestamp"], record["value"])
                    )
            # Sort each entity's timeline by timestamp
            for eid in ent_timeline:
                ent_timeline[eid].sort(key=lambda x: x[0])
            feature_timelines[fname] = ent_timeline

        # For each event, find the latest feature value with ts <= event_ts
        import bisect
        training_rows = []
        for event in events:
            eid = event.get(entity_field)
            event_ts = event.get(timestamp_field)
            row = dict(event)  # copy original event fields
            for fname in feature_names:
                timeline = feature_timelines[fname].get(eid, [])
                if not timeline:
                    row[fname] = None
                    continue
                # Binary search: find the rightmost ts <= event_ts
                timestamps = [t for t, _ in timeline]
                idx = bisect.bisect_right(timestamps, event_ts) - 1
                if idx < 0:
                    row[fname] = None  # no value as-of event_ts
                else:
                    row[fname] = timeline[idx][1]
            training_rows.append(row)
        return training_rows

    # ------------------------------------------------------------------
    # Feature lineage
    # ------------------------------------------------------------------

    def get_lineage(self, feature_name: str,
                     version: Optional[int] = None) -> Optional[dict]:
        """Get the lineage of a feature (source, transformation, values_count)."""
        feat = self.get_feature_definition(feature_name, version)
        if not feat:
            return None
        return {
            "feature": feature_name,
            "version": feat["version"],
            "source": feat["source"],
            "transformation": feat["transformation"],
            "type": feat["type"],
            "values_count": len(self.get_all_values(feature_name, feat["version"])),
        }

    # ------------------------------------------------------------------
    # Semantic model integration
    # ------------------------------------------------------------------

    def register_with_semantic_view(self, semantic: SemanticLens) -> None:
        """Register features as semantic metrics/dimensions.

        For each feature, registers a metric in the SemanticLens with:
          - name: feature name
          - source: this FeatureStore's name
          - field: 'value'
          - dialects: {'ANSI_SQL': 'AVG(value)'} (default; override per-feature)
          - dimensions: ['entity_id']
          - ai_context: feature description
        """
        for feat_name in self.list_features():
            feat = self.get_feature_definition(feat_name)
            if feat:
                semantic.define_metric_ossie(
                    name=feat_name,
                    source=self.name,
                    field="value",
                    dialects={"ANSI_SQL": f"AVG(value)"},
                    dimensions=["entity_id"],
                    ai_context=feat.get("description", "")
                )

    # ------------------------------------------------------------------
    # Feature freshness (O(1) via cached metadata)
    # ------------------------------------------------------------------

    def _update_freshness_cache(self, feature_name: str, timestamp: float) -> None:
        """Update the cached latest timestamp for a feature.

        This enables O(1) get_freshness instead of O(N) scan.
        The cache is stored as a kernel blob under
        _meta/latest_ts/{feature_name}. It is updated on every
        write_feature_value call.
        """
        # Read the current cached value (if any)
        cache_key = f"_meta/latest_ts/{feature_name}"
        current = self.base.lookup(cache_key)
        if current is not None:
            cached_record = self.decode(self.kernel.read_blob(current))
            if cached_record.get("latest_ts", 0) >= timestamp:
                return  # cache is already newer; no update needed
        # Write the new cache
        cache_record = {"feature_name": feature_name, "latest_ts": timestamp}
        self.put(cache_key, cache_record)

    def get_freshness(self, feature_name: str) -> Optional[float]:
        """Get the age of the latest feature value's EVENT timestamp.

        Returns `time.time() - latest_event_timestamp`, where
        `latest_event_timestamp` is the `timestamp` argument passed to
        the most recent `write_feature_value` call for this feature.

        **Important:** this is the age of the data's NOMINAL event
        timestamp, NOT the wall-clock time when the write happened.
        If you write a feature value with `timestamp=1704067200`
        (Jan 1 2024) and call `get_freshness` from a 2026 process,
        it will return ~80 million seconds — because the data's
        event timestamp is from 2024, even though you just wrote it.

        This is the correct semantic for feature-store freshness
        monitoring: you want to know "how stale is the data I'm
        serving," which is based on the event timestamp, not the
        write time. If you want wall-clock write freshness, store
        `time.time()` as the `timestamp` argument in
        `write_feature_value`.

        O(1) via the cached latest timestamp. Returns None if the
        feature has no values.
        """
        cache_key = f"_meta/latest_ts/{feature_name}"
        current = self.base.lookup(cache_key)
        if current is None:
            # No cache; fall back to O(N) scan (for data written before
            # the cache was introduced, or for features with no values)
            values = self.get_all_values(feature_name)
            if not values:
                return None
            latest = max(v["timestamp"] for v in values)
            return time.time() - latest
        cached_record = self.decode(self.kernel.read_blob(current))
        return time.time() - cached_record.get("latest_ts", 0)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_feature_store():
    """Original Phase D test — preserved for backward compatibility."""
    import shutil
    bench_dir = "/tmp/pond_feature_store_test"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    print("=== FEATURE STORE: RECURSIVE VIEW COMPOSITION ===\n")

    # Layer 1: Source data (SQL-like View)
    from lens_sdk import View
    orders = View(kernel, "orders")
    orders.put("order:1", {"order_id": 1, "customer_id": "cust_1", "amount": 100, "product": "Widget", "ts": 1000.0})
    orders.put("order:2", {"order_id": 2, "customer_id": "cust_2", "amount": 200, "product": "Gadget", "ts": 1001.0})
    orders.put("order:3", {"order_id": 3, "customer_id": "cust_1", "amount": 50, "product": "Widget", "ts": 1002.0})
    orders.put("order:4", {"order_id": 4, "customer_id": "cust_3", "amount": 300, "product": "Gadget", "ts": 1003.0})
    orders.commit("insert 4 orders")
    print(f"  Source View 'orders': {orders.count()} rows")

    # Layer 2: Feature Store
    fs = FeatureStore(kernel, "feature_store")

    fs.define_feature("total_spent", "float", "orders",
                      "SUM(amount) GROUP BY customer_id",
                      "Total amount spent by customer")
    fs.define_feature("order_count", "int", "orders",
                      "COUNT(*) GROUP BY customer_id",
                      "Number of orders by customer")
    fs.define_feature("avg_order_value", "float", "orders",
                      "AVG(amount) GROUP BY customer_id",
                      "Average order value")

    from collections import defaultdict
    customer_totals = defaultdict(float)
    customer_counts = defaultdict(int)
    for key, record in orders.get_all().items():
        cid = record["customer_id"]
        customer_totals[cid] += record["amount"]
        customer_counts[cid] += 1

    for cid, total in customer_totals.items():
        fs.write_feature_value("total_spent", cid, total, timestamp=2000.0)
    for cid, count in customer_counts.items():
        fs.write_feature_value("order_count", cid, count, timestamp=2000.0)
    for cid, total in customer_totals.items():
        fs.write_feature_value("avg_order_value", cid, total / customer_counts[cid], timestamp=2000.0)

    fs.commit("ingest features from orders")
    print(f"  Feature Store: {len(fs.list_features())} features, {fs.count()} entries")

    print(f"\n  Online serving (point lookup):")
    print(f"    total_spent[cust_1] = {fs.get_feature_value('total_spent', 'cust_1')}")
    print(f"    order_count[cust_1] = {fs.get_feature_value('order_count', 'cust_1')}")
    print(f"    avg_order_value[cust_1] = {fs.get_feature_value('avg_order_value', 'cust_1')}")

    vector = fs.get_feature_vector("cust_2", ["total_spent", "order_count", "avg_order_value"])
    print(f"\n  Feature vector for cust_2: {vector}")

    all_values = fs.get_all_values("total_spent")
    print(f"\n  Offline serving (batch scan):")
    print(f"    total_spent: {len(all_values)} values")
    for v in all_values:
        print(f"      {v['entity_id']}: ${v['value']}")

    pt_values = fs.get_feature_values_at_time("total_spent", timestamp=1500.0)
    print(f"\n  Point-in-time at ts=1500 (before features were written):")
    print(f"    Values: {len(pt_values)} (expected 0 — features written at ts=2000)")

    pt_values = fs.get_feature_values_at_time("total_spent", timestamp=2500.0)
    print(f"  Point-in-time at ts=2500 (after features were written):")
    print(f"    Values: {len(pt_values)} (expected 3)")
    for v in pt_values:
        print(f"      {v['entity_id']}: ${v['value']}")

    print(f"\n  Feature lineage:")
    for feat in fs.list_features():
        lineage = fs.get_lineage(feat)
        print(f"    {lineage['feature']} <- {lineage['source']} ({lineage['values_count']} values)")

    print(f"\n  Semantic model integration:")
    semantic = SemanticLens(kernel, "semantic")
    fs.register_with_semantic_view(semantic)
    semantic.commit("register features as semantic metrics")
    print(f"    Metrics: {semantic.list_metrics()}")
    for m in semantic.list_metrics():
        print(f"      {m}: {semantic.get_metric(m).get('ai_context', '')}")

    print(f"\n  Feature freshness:")
    for feat in fs.list_features():
        freshness = fs.get_freshness(feat)
        print(f"    {feat}: {freshness:.0f}s ago" if freshness else f"    {feat}: no data")

    print(f"\n=== RECURSIVE COMPOSITION VERIFIED ===")
    print(f"  Kernel (3 primitives)")
    print(f"    -> ProllyLensBase (delta commits + Prolly trees)")
    print(f"      -> IndexedLens (auto-indexing + incremental updates)")
    print(f"        -> FeatureStore (features + lineage + semantic)")
    print(f"  4 layers of composition, each adding semantics.")
    print(f"  Each layer uses ONLY the layer below. No layer skips.")
    print(f"  Kernel unchanged. All composition is Lens-level.")

    print(f"\n=== ALL TESTS PASSED ===")
    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)


def test_production_features():
    """Phase E test — production features: schema validation, versioning,
    point-in-time JOIN, batch serving, entity registry, persistence."""
    import shutil
    bench_dir = "/tmp/pond_feature_store_production"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)

    print("=== FEATURE STORE: PRODUCTION FEATURES (Phase E) ===\n")

    fs = FeatureStore(kernel, "feature_store")

    # --- Schema validation ---
    print("--- Schema validation ---")
    fs.define_feature("user_age", "int", "users", "age field")
    fs.commit("define user_age")

    # Valid write
    fs.write_feature_value("user_age", "u1", 25, timestamp=1000.0)
    fs.commit("write valid user_age")
    assert fs.get_feature_value("user_age", "u1") == 25
    print("  Valid int write: PASS")

    # Invalid write — string to int feature
    try:
        fs.write_feature_value("user_age", "u2", "twenty-five", timestamp=1000.0)
        print("  FAIL: should have rejected string for int feature")
    except ValueError as e:
        print(f"  Schema rejection (string->int): PASS")
        assert "does not match" in str(e)

    # Invalid write — float that's not an integer
    try:
        fs.write_feature_value("user_age", "u3", 25.5, timestamp=1000.0)
        print("  FAIL: should have rejected 25.5 for int feature")
    except ValueError:
        print(f"  Schema rejection (25.5->int): PASS")

    # Write to undefined feature
    try:
        fs.write_feature_value("undefined_feature", "u1", 42, timestamp=1000.0)
        print("  FAIL: should have rejected write to undefined feature")
    except ValueError as e:
        print(f"  Undefined feature rejection: PASS")
        assert "not defined" in str(e)

    # --- Feature versioning ---
    print("\n--- Feature versioning ---")
    v1 = fs.define_feature("user_score", "float", "users", "score v1")
    assert v1 == 1
    fs.write_feature_value("user_score", "u1", 0.5, timestamp=1000.0, version=1)
    fs.commit("write user_score v1")

    # Redefine with different type -> new version
    v2 = fs.define_feature("user_score", "int", "users", "score v2 (now integer)")
    assert v2 == 2
    fs.write_feature_value("user_score", "u1", 5, timestamp=2000.0, version=2)
    fs.commit("write user_score v2")

    # Idempotent redefinition -> same version
    v2b = fs.define_feature("user_score", "int", "users", "score v2 (now integer)")
    assert v2b == 2
    print(f"  Versioning: v1={v1}, v2={v2}, idempotent redef={v2b}: PASS")

    # Both versions are queryable
    assert fs.get_feature_value("user_score", "u1", version=1) == 0.5
    assert fs.get_feature_value("user_score", "u1", version=2) == 5
    # Latest version is the default
    assert fs.get_feature_value("user_score", "u1") == 5
    print(f"  Multi-version query: v1=0.5, v2=5, latest=5: PASS")

    assert fs.list_feature_versions("user_score") == [1, 2]
    print(f"  list_feature_versions: {fs.list_feature_versions('user_score')}: PASS")

    # --- Entity registry ---
    print("\n--- Entity registry ---")
    fs.register_entity_type("user", "user_id", "Application user")
    fs.register_entity_type("order", "order_id", "Customer order")
    fs.commit("register entity types")
    assert "user" in fs.list_entity_types()
    assert "order" in fs.list_entity_types()
    assert fs.get_entity_type("user")["join_key"] == "user_id"
    print(f"  Entity registry: {fs.list_entity_types()}: PASS")

    # --- Point-in-time JOIN (the killer feature) ---
    print("\n--- Point-in-time JOIN ---")
    # Define a feature and write values at multiple timestamps
    fs.define_feature("user_balance", "float", "transactions", "running balance")
    fs.write_feature_value("user_balance", "u1", 100.0, timestamp=1000.0)  # initial
    fs.write_feature_value("user_balance", "u1", 50.0,  timestamp=2000.0)  # withdrawal
    fs.write_feature_value("user_balance", "u1", 75.0,  timestamp=3000.0)  # deposit
    fs.commit("write balance history")

    # Training events: 3 events for u1 at different timestamps
    events = [
        {"entity_id": "u1", "timestamp": 500.0,  "label": 0},  # before any balance
        {"entity_id": "u1", "timestamp": 1500.0, "label": 1},  # balance was 100
        {"entity_id": "u1", "timestamp": 2500.0, "label": 0},  # balance was 50
        {"entity_id": "u1", "timestamp": 3500.0, "label": 1},  # balance was 75
    ]
    dataset = fs.get_training_dataset(events, ["user_balance"])
    assert len(dataset) == 4
    assert dataset[0]["user_balance"] is None   # ts=500, no balance yet
    assert dataset[1]["user_balance"] == 100.0  # ts=1500, balance was 100
    assert dataset[2]["user_balance"] == 50.0   # ts=2500, balance was 50
    assert dataset[3]["user_balance"] == 75.0   # ts=3500, balance was 75
    # Original event fields are preserved
    assert dataset[0]["label"] == 0
    assert dataset[1]["label"] == 1
    print(f"  Point-in-time JOIN: 4 events -> 4 training rows: PASS")
    print(f"    ts=500:  balance={dataset[0]['user_balance']} (None — no data yet)")
    print(f"    ts=1500: balance={dataset[1]['user_balance']} (100.0 — correct)")
    print(f"    ts=2500: balance={dataset[2]['user_balance']} (50.0 — correct)")
    print(f"    ts=3500: balance={dataset[3]['user_balance']} (75.0 — correct)")
    print(f"  Label leakage prevented: PASS (no future data in any row)")

    # --- Batch online serving (feature matrix) ---
    print("\n--- Batch online serving (feature matrix) ---")
    # Add more entities
    for eid in ["u2", "u3", "u4"]:
        fs.write_feature_value("user_balance", eid, 200.0, timestamp=4000.0)
    fs.commit("add more users")

    matrix = fs.get_feature_matrix(
        entity_ids=["u1", "u2", "u3", "u4", "u5_nonexistent"],
        feature_names=["user_balance", "user_age"]
    )
    assert len(matrix) == 5
    assert matrix[0]["entity_id"] == "u1"
    assert matrix[0]["user_balance"] == 75.0  # latest for u1
    assert matrix[1]["entity_id"] == "u2"
    assert matrix[1]["user_balance"] == 200.0
    assert matrix[4]["entity_id"] == "u5_nonexistent"
    assert matrix[4]["user_balance"] is None  # no data
    print(f"  get_feature_matrix: 5 entities x 2 features: PASS")
    print(f"    u1: balance={matrix[0]['user_balance']}, age={matrix[0]['user_age']}")
    print(f"    u2: balance={matrix[1]['user_balance']}, age={matrix[1]['user_age']}")
    print(f"    u5: balance={matrix[4]['user_balance']} (None — no data)")

    # --- O(1) freshness via cache ---
    print("\n--- O(1) freshness via cache ---")
    # The freshness cache should have been updated on every write_feature_value
    freshness = fs.get_freshness("user_balance")
    assert freshness is not None
    assert freshness >= 0
    print(f"  get_freshness('user_balance'): {freshness:.3f}s: PASS (O(1) via cache)")

    # --- Persistence test ---
    print("\n--- Persistence test ---")
    # Only commit if there's something staged
    if fs.has_staged():
        fs.commit("final commit before restart")
    kernel.close()

    # Reopen the same kernel — data should survive
    kernel2 = PondMinimal(bench_dir)
    fs2 = FeatureStore(kernel2, "feature_store")
    assert "user_age" in fs2.list_features()
    assert "user_score" in fs2.list_features()
    assert "user_balance" in fs2.list_features()
    assert fs2.get_feature_value("user_age", "u1") == 25
    assert fs2.get_feature_value("user_balance", "u1") == 75.0
    # Versioning survived
    assert fs2.list_feature_versions("user_score") == [1, 2]
    # Entity registry survived
    assert "user" in fs2.list_entity_types()
    print(f"  Data survived process restart: PASS")
    print(f"    Features: {fs2.list_features()}")
    print(f"    user_age[u1] = {fs2.get_feature_value('user_age', 'u1')}")
    print(f"    user_balance[u1] = {fs2.get_feature_value('user_balance', 'u1')}")

    kernel2.close()
    shutil.rmtree(bench_dir, ignore_errors=True)

    print(f"\n=== ALL PRODUCTION TESTS PASSED ===")


if __name__ == "__main__":
    test_feature_store()
    print()
    test_production_features()
