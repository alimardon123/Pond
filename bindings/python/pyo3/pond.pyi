"""Type stubs for the `pond` Python module (PyO3 bindings).

These stubs mirror the PyO3-exposed API in
`bindings/python/pyo3/src/lib.rs`. They are intended for IDE auto-
completion, type-checking (mypy / pyright), and as a precise machine-
readable description of the Python-facing surface.

Notes:
  - Methods that accept arbitrary Python values use `Any` from `typing`.
  - Methods that return JSON-serialisable structures use `dict[str, Any]`
    or `list[Any]` — the actual shape is documented per-method.
  - `where` parameters accept either a SQL string ("age >= 18 AND city =
    'NYC'") or a dict ({'age': ('>', 25)}). Both forms are accepted by
    the Rust side; the stub uses `Union[str, dict[str, Any]]` to reflect
    that.
  - `Optional[T]` is used for parameters that default to `None`.
  - Tuples are used for fixed-arity return values (e.g., commit history
    entries are (hash, message, index) triples).
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union

# ---------------------------------------------------------------------------
# Type aliases for return types
# ---------------------------------------------------------------------------

# A columnar result: {column_name: [values]}.
ColumnarResult = dict[str, list[Any]]

# A predicate tuple: (column_name, operator, value).
Predicate = tuple[str, str, Any]

# A commit history entry: (commit_hash, message, index).
HistoryEntry = tuple[str, str, int]

# A shard entry returned by read_with_shards: (shard_name, data_bytes).
ShardEntry = tuple[str, bytes]

# An index search result entry: (distance, vector_id).
IndexSearchHit = tuple[float, str]

# Column spec for write_rows: (name, list_of_values).
ColumnSpec = tuple[str, list[Any]]

# Stats entry returned by encode(): (name, vtype, min, max, null_count).
EncodeStatsEntry = tuple[str, int, Any, Any, int]

# Encode result: {'blob': bytes, 'stats': [EncodeStatsEntry, ...]}.
EncodeResult = dict[str, Any]

# Where expression: either a SQL string or a dict of {column: (op, value)}.
WhereExpr = Union[str, dict[str, Any]]

# Merge action: 'update' | 'delete' | 'skip', or a dict mapping action → condition.
MergeAction = Union[str, dict[str, str]]

# Merge result: {'matched': N, 'updated': N, 'deleted': N, 'inserted': N, 'skipped': N}.
MergeResult = dict[str, int]

# GC stats: {'live': N, 'dead': N, 'dead_size_bytes': N}.
GcStats = dict[str, int]

# Vacuum result: {'deleted': N, 'preserved': N, 'dry_run': bool}.
VacuumResult = dict[str, Union[int, bool]]

# Optimize result: {'collections_optimized': N, 'shards_compacted': N, 'manifests_flattened': N}.
OptimizeResult = dict[str, int]

# A row dict (JSON object).
Row = dict[str, Any]


# ---------------------------------------------------------------------------
# Storage — the main Pond storage handle
# ---------------------------------------------------------------------------

class Storage:
    """A Pond storage handle backed by the Rust UnifiedStorage.

    Provides the same operations as the Python `PondStorage` class, but
    all logic runs in Rust (no Python reference kernel needed).

    Auto-detects the storage type from the `location` argument:
      - `Storage('/var/lib/pond')` → local filesystem
      - `Storage('s3://bucket/prefix?...')` → S3-compatible storage
      - `Storage('.')` → local filesystem (current directory)
    """

    def __new__(
        cls,
        location: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: Optional[str] = None,
        endpoint: Optional[str] = None,
    ) -> "Storage":
        """Open a storage handle at `location`.

        For S3, credentials default to the AWS env vars
        (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN).
        """
        ...

    # --- Local FS / S3 constructors ---

    @staticmethod
    def from_s3(url: str) -> "Storage":
        """Create a Storage backed by S3-compatible storage.

        Equivalent to `Storage('s3://...')` — kept for explicit clarity.
        """
        ...

    # --- Raw bytes read/write ---

    def write(self, collection: str, data: bytes, message: str) -> str:
        """Write raw bytes to a collection on the active branch.

        Returns the commit hash (hex string).
        """
        ...

    def read(self, collection: str) -> bytes:
        """Read the raw bytes of a collection's HEAD on the active branch."""
        ...

    # --- Structured row operations ---

    def write_rows(
        self,
        collection: str,
        columns: list[ColumnSpec],
        message: str,
        crdt: bool = True,
        where: Optional[WhereExpr] = None,
    ) -> str:
        """Write structured columns as a PND2 blob with column stats.

        Auto-adds `_rowid` + `_version` columns when `crdt=True` (default).
        Returns the commit hash.
        """
        ...

    def update_rows(
        self,
        collection: str,
        updates: dict[str, Any],
        where: Optional[WhereExpr] = None,
        key_col: Optional[str] = None,
        crdt: bool = True,
    ) -> int:
        """Update rows matching `where` — like SQL `UPDATE ... WHERE`.

        Returns the number of rows updated.
        """
        ...

    def delete_rows(
        self,
        collection: str,
        where: Optional[WhereExpr] = None,
        key_col: Optional[str] = None,
        crdt: bool = True,
    ) -> int:
        """Delete rows matching `where` — like SQL `DELETE FROM ... WHERE`.

        Returns the number of rows deleted.
        """
        ...

    def merge_rows(
        self,
        collection: str,
        rows: list[Row],
        on: Optional[Union[str, list[Union[str, tuple[str, str]]]]] = None,
        key_col: Optional[str] = None,
        crdt: bool = True,
        where: Optional[WhereExpr] = None,
        on_match: Optional[MergeAction] = None,
        on_miss: Optional[MergeAction] = None,
        on_miss_target: Optional[MergeAction] = None,
    ) -> MergeResult:
        """Merge rows into a collection — SQL MERGE with multi-action + multi-key.

        Returns a dict with keys: matched, updated, deleted, inserted, skipped.
        """
        ...

    def sql(self, sql: str) -> ColumnarResult:
        """Execute a SQL statement (SELECT / INSERT / UPDATE / DELETE / MERGE).

        For SELECT: returns a dict of {column: [values]}.
        For mutations: returns a dict with status/counts.
        """
        ...

    def read_rows(
        self,
        collection: str,
        columns: Optional[list[str]] = None,
        predicates: Optional[list[Predicate]] = None,
    ) -> ColumnarResult:
        """Read structured columns with optional pruning and projection.

        Returns a dict of {column_name: [values]}.
        """
        ...

    # --- Branch / merge / history ---

    def branch(self, collection: str, branch_name: str) -> str:
        """Create a new branch from the active branch.

        Returns the commit hash the branch was created at.
        """
        ...

    def checkout(self, collection: str, branch_name: str) -> None:
        """Switch the active branch (must already exist)."""
        ...

    def checkout_new(self, collection: str, branch_name: str) -> None:
        """Create a new branch and switch to it (like `git checkout -b`)."""
        ...

    def merge(
        self,
        collection: str,
        source: str,
        target: Optional[str] = None,
        message: str = "merge",
    ) -> str:
        """Merge `source` into `target` (default: active branch).

        Returns the merge commit hash.
        """
        ...

    def history(self, collection: str, limit: int = 20) -> list[HistoryEntry]:
        """Show commit history for a collection (newest first).

        Returns a list of (commit_hash, message, index) tuples.
        """
        ...

    def ls(self) -> list[str]:
        """List all collection names (sorted, deduplicated)."""
        ...

    def undo(self, collection: str, steps: int = 1) -> str:
        """Undo the last `steps` commits.

        Returns the new HEAD commit hash.
        """
        ...

    def revert(self, collection: str, commit_hash: str) -> None:
        """Revert the active branch to a specific commit hash."""
        ...

    def get_active_branch(self, collection: str) -> str:
        """Get the active branch name (defaults to 'main')."""
        ...

    def set_active_branch(self, collection: str, branch_name: str) -> None:
        """Set the active branch (in-memory only — not persisted)."""
        ...

    # --- Index operations ---

    def build_index(
        self,
        collection: str,
        index_name: str,
        index_type: str,
        config: Optional[dict[str, Any]] = None,
    ) -> str:
        """Build an index on a collection ('simple', 'ivf', or 'hnsw').

        Returns the index blob hash.
        """
        ...

    def lookup_index(
        self, collection: str, index_name: str, index_key: str
    ) -> Optional[str]:
        """Look up a single key in a 'simple' index.

        Returns the matching rowid, or None if not found.
        """
        ...

    def search_index(
        self,
        collection: str,
        index_type: str,
        query: list[float],
        k: int = 10,
        n_probe: int = 10,
        ef: int = 50,
    ) -> list[IndexSearchHit]:
        """Search an index (IVF or HNSW) for the k nearest neighbors.

        Returns a list of (distance, vector_id) tuples.
        """
        ...

    def drop_index(self, collection: str, index_name: str) -> bool:
        """Drop an index. Returns True if it existed and was dropped."""
        ...

    def list_indexes(self, collection: str) -> list[str]:
        """List all active index names for a collection."""
        ...

    # --- GC / Vacuum ---

    def gc_stats(self, compute_size: bool = False) -> GcStats:
        """Analyze reachability and return GC stats (read-only).

        Returns {'live': N, 'dead': N, 'dead_size_bytes': N}.
        """
        ...

    def vacuum(
        self, preserve_days: int = 0, dry_run: bool = False
    ) -> VacuumResult:
        """Delete unreachable blobs with time-travel safety.

        Returns {'deleted': N, 'preserved': N, 'dry_run': bool}.
        """
        ...

    # --- CRDT shards ---

    def append_shard(
        self, collection: str, shard_name: str, data: bytes
    ) -> str:
        """Append a CRDT shard (raw bytes) to the active branch.

        Returns the shard blob hash.
        """
        ...

    def upsert_shard(
        self,
        collection: str,
        shard_name: str,
        rows: list[Row],
        key_col: Optional[str] = None,
    ) -> str:
        """Upsert rows as a CRDT shard with _rowid + _version.

        Returns the shard blob hash.
        """
        ...

    def delete_shard(
        self,
        collection: str,
        shard_name: str,
        rowids: list[str],
        key_col: Optional[str] = None,
    ) -> str:
        """Delete rows by writing a tombstone shard.

        Returns the tombstone shard blob hash.
        """
        ...

    def read_with_shards(self, collection: str) -> list[ShardEntry]:
        """Read HEAD + all live shards (CRDT read path).

        Returns a list of (shard_name, data_bytes) tuples. The first
        element is HEAD (name='__head__'), followed by all shards.
        """
        ...

    def shard_count(self, collection: str) -> int:
        """Count the number of live shards for a collection's active branch."""
        ...

    def compact_shards(self, collection: str) -> int:
        """Compact shards — merge all into HEAD and clear the shard list.

        Returns the number of shards compacted.
        """
        ...

    # --- Atomic publication (transactions) ---

    def begin_tx(self) -> str:
        """Begin a transaction. Returns a transaction ID."""
        ...

    def commit_tx(self, tx_id: str, message: str) -> str:
        """Commit a transaction. Returns the commit hash."""
        ...

    def abort_tx(self, tx_id: str) -> str:
        """Abort a transaction (currently a no-op). Returns the tx_id."""
        ...

    def is_tx_committed(self, tx_id: str) -> bool:
        """Check if a transaction has been committed."""
        ...

    # --- Optimize ---

    def optimize(self, collection: Optional[str] = None) -> OptimizeResult:
        """Optimize storage — compact shards + flatten delta manifests.

        If `collection` is None, optimizes ALL collections.
        Returns {'collections_optimized': N, 'shards_compacted': N, 'manifests_flattened': N}.
        """
        ...

    # --- Media upload / download ---

    def upload(
        self,
        collection: str,
        name: str,
        data: bytes,
        mime_type: Optional[str] = None,
        **kwargs: Any,
    ) -> Row:
        """Upload a file into a structured table with metadata.

        Returns the inserted row as a dict.
        """
        ...

    def download(
        self,
        collection: str,
        name: Optional[str] = None,
        where: Optional[WhereExpr] = None,
    ) -> Optional[bytes]:
        """Download a file's bytes by name or WHERE clause.

        Returns the file content as bytes, or None if not found.
        """
        ...

    # --- Semantic layers ---

    def layer(
        self,
        name: str,
        adapters: Optional[list[str]] = None,
        enable_reflection: bool = False,
    ) -> "SemanticLayer":
        """Get a semantic layer handle (creates the layer if it doesn't exist)."""
        ...

    def layers(self) -> list[str]:
        """List all semantic layer names."""
        ...

    # --- UDF pushdown ---

    def register_udf(self, name: str, func: Callable[..., Any]) -> None:
        """Register a Python UDF for SQL WHERE pushdown."""
        ...

    def unregister_udf(self, name: str) -> bool:
        """Unregister a UDF by name. Returns True if it was found."""
        ...

    def list_udfs(self) -> list[str]:
        """List all registered UDF names (sorted)."""
        ...

    # --- Row-Level Security (RLS) ---

    def set_rls_policy(self, collection: str, tenant_id: str) -> None:
        """Set an RLS policy: write_rows auto-adds _tenant, read_rows filters by it."""
        ...

    def get_rls_policy(self, collection: str) -> Optional[str]:
        """Get the RLS tenant_id for a collection, or None if no policy is set."""
        ...

    def clear_rls_policy(self, collection: str) -> bool:
        """Clear the RLS policy. Returns True if a policy was cleared."""
        ...


# ---------------------------------------------------------------------------
# SemanticLayer — handle for cross-collection semantic layer operations
# ---------------------------------------------------------------------------

class SemanticLayer:
    """A handle to a semantic layer.

    Get one via: `m = s.layer('sales')`.
    """

    def add_datasets(self, datasets: list[str]) -> None:
        """Add multiple datasets (collection names) to the layer."""
        ...

    def add_metrics(self, metrics: dict[str, str]) -> None:
        """Add multiple metrics: {metric_name: expression}.

        Example: {'revenue': 'SUM(orders.amount)', 'count': 'COUNT(orders.id)'}
        """
        ...

    def add_dimensions(
        self, dimensions: dict[str, tuple[str, str, str]]
    ) -> None:
        """Add multiple dimensions: {dim_name: (collection, field, data_type)}."""
        ...

    def add_relationships(
        self, relationships: dict[str, tuple[str, str, str]]
    ) -> None:
        """Add multiple relationships: {rel_name: (from, to, condition)}."""
        ...

    def info(self) -> dict[str, Any]:
        """Get a full overview of the layer.

        Returns a dict with: name, adapters, datasets, metrics, dimensions,
        relationships, reflection_enabled.
        """
        ...

    def datasets(self) -> list[str]:
        """List datasets in this layer."""
        ...

    def metrics(self) -> list[str]:
        """List metrics in this layer."""
        ...

    def dimensions(self) -> list[str]:
        """List dimensions in this layer."""
        ...

    def relationships(self) -> list[str]:
        """List relationships in this layer."""
        ...

    def adapters(self) -> list[str]:
        """List the adapters currently enabled on this layer."""
        ...

    def add_adapter(self, adapter: str) -> None:
        """Add an adapter to this layer. Idempotent."""
        ...

    def remove_adapter(self, adapter: str) -> bool:
        """Remove an adapter. Returns True if it was present."""
        ...

    def export(self, adapter: Optional[str] = None) -> dict[str, Any]:
        """Export the layer in a specific adapter format.

        If `adapter` is None, uses the first adapter in the layer's list.
        """
        ...

    def enable_reflection(self) -> None:
        """Enable reflection on this layer."""
        ...

    def disable_reflection(self) -> None:
        """Disable reflection on this layer."""
        ...


# ---------------------------------------------------------------------------
# Module-level functions
# ---------------------------------------------------------------------------

def decode(
    blob_bytes: bytes,
    columns: Optional[list[str]] = None,
    predicates: Optional[list[Predicate]] = None,
) -> Optional[dict[str, Any]]:
    """Decode a PND2 blob into a dict of {column_name: list_of_values}.

    Optionally project columns and apply row-level predicate pushdown.
    Returns None if the blob is not a valid PND2 blob.
    """
    ...


def encode(
    columns: list[ColumnSpec],
    n_rows: int,
) -> Optional[EncodeResult]:
    """Encode a list of (name, values) columns into a PND2 blob.

    Returns a dict with 'blob' (bytes) and 'stats' (list of tuples), or
    None if the columns can't be encoded (e.g., unsupported types).
    """
    ...


# ---------------------------------------------------------------------------
# __all__
# ---------------------------------------------------------------------------

__all__: list[str] = [
    "Storage",
    "SemanticLayer",
    "decode",
    "encode",
]
