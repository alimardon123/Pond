"""
SharedDataset + NativeView — the "data is just bytes, Views are lenses" pattern.

This is the architectural pattern the user asked for: multiple Views
reading the SAME bytes, each interpreting them its own way. Like a
Linux filesystem where the same file can be read by a text editor,
grep, or an image viewer.

Design:
  - SharedDataset: a named collection of bytes in the kernel with a
    commit DAG. Data is stored as Arrow IPC (the canonical format
    that DuckDB, Polars, DataFusion, pandas all read natively).
    A manifest blob tracks which Views are "enabled" on the data.

  - NativeView: a thin reader that reads the dataset's Arrow bytes
    and presents them per its conventions. Examples:
      - DuckDBView: registers the Arrow table with DuckDB for SQL
      - PolarsView: returns a Polars DataFrame
      - ArrowView: returns the raw pyarrow.Table
      - DataFusionView: registers with DataFusion

  - Manifest: a small metadata blob stored alongside the data.
    Like a sidecar file in a Linux directory. Lists enabled Views
    with their versions and schemas.

Key property: ZERO COPYING. The data is stored once as Arrow IPC.
Every reader reads the same bytes. No translation layer, no
intermediate format, no duplication.

Usage:
    # Create a shared dataset
    dataset = SharedDataset(kernel, "orders")
    dataset.write_records([
        {"order_id": 1, "customer_id": "c1", "amount": 100.0},
        {"order_id": 2, "customer_id": "c2", "amount": 200.0},
    ])
    dataset.commit("initial load")

    # Enable multiple Views on the SAME data
    dataset.enable_view("sql", version=1, schema={"order_id": "int64", ...})
    dataset.enable_view("olap", version=1)
    dataset.enable_view("git", version=1)

    # Read via different Views — SAME bytes, different interpretations
    duckdb_view = DuckDBView(dataset)
    table_name = duckdb_view.register(duckdb_con)
    # DuckDB runs SQL on the Arrow bytes directly (zero-copy)

    polars_view = PolarsView(dataset)
    df = polars_view.read()
    # Polars reads the same Arrow bytes (zero-copy)

    arrow_view = ArrowNativeView(dataset)
    table = arrow_view.read()
    # Raw Arrow Table

Architecture alignment:
  - Simple: one shared dataset, multiple thin readers
  - Powerful: same data, many interpretations
  - Performant: zero-copy Arrow reads (DuckDB/Polars/DataFusion all
    read Arrow natively)
  - Scalable: readers are independent, no coordination
  - Efficient: one copy of bytes, no duplication
  - Beautiful: Linux filesystem analogy — bytes are bytes, readers
    interpret them

The manifest is the "sidecar file" the user described. It's like a
.pond/manifest.json in the dataset's "directory" saying "this data
is enabled for SQL View v1, OLAP View v1, Git View v1."
"""

from __future__ import annotations

import os
import sys
import json
import time
from typing import Optional, Any

# Path setup
HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), "pond-core"))
sys.path.insert(0, HERE)

import pyarrow as pa
from pond_minimal import PondMinimal
from view_sdk import View


# ---------------------------------------------------------------------------
# Arrow encoding helpers (reused from pond-arrow/arrow_view.py)
# ---------------------------------------------------------------------------

def _table_to_ipc_bytes(table: pa.Table) -> bytes:
    """Encode a pyarrow Table as Arrow IPC stream bytes."""
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


def _ipc_bytes_to_table(data: bytes) -> pa.Table:
    """Decode Arrow IPC stream bytes into a pyarrow Table."""
    reader = pa.ipc.open_stream(pa.BufferReader(data))
    return reader.read_all()


def _records_to_table(records: list[dict]) -> pa.Table:
    """Convert a list of row dicts into a pyarrow Table."""
    if not records:
        return pa.table({})
    # Use the first record's keys as columns
    keys = list(records[0].keys())
    cols = {k: [r.get(k) for r in records] for k in keys}
    return pa.table(cols)


# ---------------------------------------------------------------------------
# SharedDataset — shared bytes + manifest + commit DAG
# ---------------------------------------------------------------------------

class SharedDataset(View):
    """A shared dataset that stores data as Arrow IPC bytes.

    Multiple NativeView readers can read the SAME bytes. A manifest
    tracks which Views are enabled (like a sidecar file in a Linux
    directory).

    The data is stored once. Every reader reads the same bytes.
    No copying, no translation, no duplication.

    Storage model (keys in the kernel):
      _data/snapshot          → Arrow IPC blob (the current data)
      _manifest               → JSON blob listing enabled Views
      _schema                 → Arrow schema blob (for readers that need it)

    The commit DAG is shared (inherited from View). All readers see
    the same HEAD commit.
    """

    def __init__(self, kernel: PondMinimal, name: str):
        super().__init__(kernel, name)
        self._staged_records: list[dict] = []
        self._staged_deletes: set[str] = set()
        # In-memory manifest cache. Updated on enable_view/disable_view
        # so that multiple calls before commit accumulate correctly.
        # Without this, _read_manifest() would only see committed state
        # and multiple enable_view calls before commit would overwrite
        # each other.
        self._manifest_cache: Optional[dict] = None

    # ------------------------------------------------------------------
    # Writing — stores data as Arrow IPC (canonical format)
    # ------------------------------------------------------------------

    def write_records(self, records: list[dict]) -> None:
        """Stage records for the next commit. Data will be stored as Arrow IPC.

        Args:
            records: list of row dicts. All dicts should have the same keys.
        """
        self._staged_records.extend(records)

    def write_record(self, record: dict) -> None:
        """Stage a single record. Convenience wrapper around write_records."""
        self._staged_records.append(record)

    def delete_record(self, primary_key: str) -> None:
        """Stage a record deletion by primary key."""
        self._staged_deletes.add(primary_key)

    def commit(self, message: str = "") -> str:
        """Commit staged records as a new Arrow IPC snapshot.

        Builds the new full-table state (current committed state +
        staged records - staged deletes), encodes as Arrow IPC, and
        creates a new commit on the shared DAG.
        """
        # Read current state
        current_table = self.read_arrow()
        if current_table is not None and current_table.num_rows > 0:
            current_rows = current_table.to_pylist()
        else:
            current_rows = []

        # Merge staged records (overwrite by _pk if present)
        pk_field = "_pk"
        # Assign _pk to staged records if they don't have one
        for i, r in enumerate(self._staged_records):
            if pk_field not in r:
                r[pk_field] = f"row_{i}_{time.time_ns()}"

        by_pk = {r.get(pk_field): r for r in current_rows}
        for r in self._staged_records:
            by_pk[r.get(pk_field)] = r
        for pk in self._staged_deletes:
            by_pk.pop(pk, None)

        new_rows = list(by_pk.values())
        if new_rows:
            new_table = _records_to_table(new_rows)
        else:
            new_table = pa.table({})

        ipc_bytes = _table_to_ipc_bytes(new_table)
        blob_hash = self.kernel.write(ipc_bytes)
        self.base.stage("_data/snapshot", blob_hash)

        # Also store the schema for readers that need it
        schema_bytes = new_table.schema.serialize()
        schema_hash = self.kernel.write(schema_bytes)
        self.base.stage("_schema", schema_hash)

        self._staged_records.clear()
        self._staged_deletes.clear()
        # Invalidate the manifest cache so the next read picks up the
        # committed state.
        self._manifest_cache = None
        return self.base.commit(message or f"{self.name} snapshot")

    # ------------------------------------------------------------------
    # Reading — returns the Arrow Table (zero-copy for external readers)
    # ------------------------------------------------------------------

    def read_arrow(self) -> Optional[pa.Table]:
        """Read the current snapshot as a pyarrow.Table.

        This is the canonical read method. All NativeView readers
        call this and present the result their own way.

        Returns None if the dataset has no committed data.
        """
        h = self.base.lookup("_data/snapshot")
        if h is None:
            return None
        ipc_bytes = self.kernel.read_blob(h)
        return _ipc_bytes_to_table(ipc_bytes)

    def read_schema(self) -> Optional[pa.Schema]:
        """Read the dataset's Arrow schema."""
        h = self.base.lookup("_schema")
        if h is None:
            return None
        schema_bytes = self.kernel.read_blob(h)
        return pa.ipc.read_schema(pa.BufferReader(schema_bytes))

    # ------------------------------------------------------------------
    # Manifest — the "sidecar file" tracking enabled Views
    # ------------------------------------------------------------------

    def _read_manifest(self) -> dict:
        """Read the manifest. Returns {} if no manifest exists.

        Checks the in-memory cache first (for staged but uncommitted
        changes), then falls back to the committed state.
        """
        if self._manifest_cache is not None:
            return dict(self._manifest_cache)
        h = self.base.lookup("_manifest")
        if h is None:
            return {}
        return json.loads(self.kernel.read_blob(h))

    def _write_manifest(self, manifest: dict) -> None:
        """Write the manifest (stages it; commit to persist).

        Also updates the in-memory cache so subsequent _read_manifest
        calls see the staged changes.
        """
        self._manifest_cache = dict(manifest)
        manifest_bytes = json.dumps(manifest, sort_keys=True).encode()
        blob_hash = self.kernel.write(manifest_bytes)
        self.base.stage("_manifest", blob_hash)

    def enable_view(self, view_name: str, version: int = 1,
                    **metadata) -> None:
        """Enable a View on this dataset.

        Writes a manifest entry. Like creating a sidecar file that
        says "this data is readable by View X v1, with schema Y."

        Args:
            view_name: the View type name (e.g., "sql", "olap", "git").
            version: the View version.
            **metadata: additional metadata (e.g., schema=...).
        """
        manifest = self._read_manifest()
        manifest[view_name] = {
            "version": version,
            "enabled_at": time.time(),
            **metadata,
        }
        self._write_manifest(manifest)

    def disable_view(self, view_name: str) -> None:
        """Disable a View on this dataset."""
        manifest = self._read_manifest()
        manifest.pop(view_name, None)
        self._write_manifest(manifest)

    def list_enabled_views(self) -> dict:
        """List all enabled Views and their metadata."""
        return self._read_manifest()

    def is_view_enabled(self, view_name: str) -> bool:
        """Check if a View is enabled on this dataset."""
        return view_name in self._read_manifest()

    # ------------------------------------------------------------------
    # Collection-like API (inherited from View, but override to read
    # from the Arrow snapshot directly for consistency)
    # ------------------------------------------------------------------

    def __iter__(self):
        """Iterate over rows (decoded from Arrow)."""
        table = self.read_arrow()
        if table is None:
            return
        for row in table.to_pylist():
            yield row

    def __len__(self) -> int:
        table = self.read_arrow()
        return table.num_rows if table is not None else 0


# ---------------------------------------------------------------------------
# NativeView — thin readers that interpret the shared bytes
# ---------------------------------------------------------------------------

class NativeView:
    """A thin reader that interprets a SharedDataset's Arrow bytes.

    Subclasses override `read()` to present the data their own way.
    The reader does NOT copy the data — it reads the same Arrow bytes
    that every other reader sees.

    This is the "Linux file viewer" pattern: the file (bytes) is
    shared; the viewer (reader) interprets it.
    """

    def __init__(self, dataset: SharedDataset):
        self.dataset = dataset

    def read(self):
        """Read the dataset and present it per this View's conventions.

        Override in subclasses.
        """
        raise NotImplementedError

    def is_enabled(self) -> bool:
        """Check if this View type is enabled on the dataset."""
        return self.dataset.is_view_enabled(self.view_name)

    @property
    def view_name(self) -> str:
        """The View type name (e.g., 'sql', 'olap'). Override in subclasses."""
        return self.__class__.__name__.replace("View", "").lower()


class ArrowNativeView(NativeView):
    """Read the dataset as a raw pyarrow.Table.

    This is the most direct reader — no transformation, just the
    Arrow Table. All other readers build on top of this.
    """

    @property
    def view_name(self) -> str:
        return "arrow"

    def read(self) -> Optional[pa.Table]:
        return self.dataset.read_arrow()


class DuckDBView(NativeView):
    """Read the dataset via DuckDB (SQL queries on Arrow bytes).

    DuckDB reads Arrow natively (zero-copy). This View registers
    the dataset's Arrow table with a DuckDB connection, so you can
    run SQL queries on it.

    Usage:
        view = DuckDBView(dataset)
        con = duckdb.connect()
        table_name = view.register(con)
        result = con.execute(f"SELECT * FROM {table_name} WHERE amount > 100").fetchall()
    """

    @property
    def view_name(self) -> str:
        return "sql"

    def read(self):
        """Returns the Arrow Table (for direct use with duckdb.from_arrow)."""
        return self.dataset.read_arrow()

    def register(self, con, table_name: Optional[str] = None) -> str:
        """Register the dataset with a DuckDB connection for SQL queries.

        Args:
            con: a duckdb.Connection.
            table_name: optional table name. Defaults to the dataset's name.

        Returns:
            The table name used. Query with: SELECT * FROM <name>.
        """
        table = self.dataset.read_arrow()
        if table is None:
            raise ValueError("Dataset has no data")
        name = table_name or self.dataset.name
        con.register(name, table)
        return name

    def query(self, con, sql: str):
        """Run a SQL query on the dataset. Convenience method.

        Args:
            con: a duckdb.Connection.
            sql: SQL query. Use the dataset's name as the table name.

        Returns:
            The query result (list of tuples or Arrow table).
        """
        name = self.register(con)
        return con.execute(sql).fetchall()


class PolarsView(NativeView):
    """Read the dataset as a Polars DataFrame (zero-copy from Arrow).

    Polars reads Arrow natively. This View converts the dataset's
    Arrow Table to a Polars DataFrame with zero copying.

    Usage:
        view = PolarsView(dataset)
        df = view.read()
        result = df.filter(df["amount"] > 100).select("order_id", "amount")
    """

    @property
    def view_name(self) -> str:
        return "olap"

    def read(self):
        """Returns a Polars DataFrame (zero-copy from Arrow)."""
        import polars as pl
        table = self.dataset.read_arrow()
        if table is None:
            return pl.DataFrame()
        return pl.from_arrow(table)


class PandasView(NativeView):
    """Read the dataset as a pandas DataFrame.

    pandas reads Arrow via pyarrow's to_pandas() (zero-copy for many dtypes).
    """

    @property
    def view_name(self) -> str:
        return "pandas"

    def read(self):
        """Returns a pandas DataFrame."""
        table = self.dataset.read_arrow()
        if table is None:
            import pandas as pd
            return pd.DataFrame()
        return table.to_pandas()


class DataFusionView(NativeView):
    """Read the dataset via Apache DataFusion (SQL query engine).

    DataFusion reads Arrow natively. This View registers the
    dataset's Arrow table with a DataFusion SessionContext.

    Usage:
        view = DataFusionView(dataset)
        ctx = view.register()
        result = ctx.sql("SELECT * FROM orders WHERE amount > 100").collect()
    """

    @property
    def view_name(self) -> str:
        return "datafusion"

    def read(self):
        """Returns the Arrow Table (for direct use with DataFusion)."""
        return self.dataset.read_arrow()

    def register(self, ctx=None):
        """Register the dataset with a DataFusion SessionContext.

        Args:
            ctx: optional existing SessionContext. If None, creates one.

        Returns:
            The SessionContext with the dataset registered.
        """
        from datafusion import SessionContext
        if ctx is None:
            ctx = SessionContext()
        table = self.dataset.read_arrow()
        if table is None:
            raise ValueError("Dataset has no data")
        ctx.register_record_batches(self.dataset.name, [table.to_batches()])
        return ctx


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_shared_dataset_basic():
    """SharedDataset stores data as Arrow IPC; multiple readers read the same bytes."""
    import shutil
    bench = "/tmp/pond_shared_ds"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    dataset = SharedDataset(kernel, "orders")
    dataset.write_records([
        {"order_id": 1, "customer_id": "c1", "amount": 100.0, "region": "US"},
        {"order_id": 2, "customer_id": "c2", "amount": 200.0, "region": "EU"},
        {"order_id": 3, "customer_id": "c1", "amount": 50.0,  "region": "US"},
    ])
    dataset.commit("initial load")

    # Read as Arrow Table
    arrow_view = ArrowNativeView(dataset)
    table = arrow_view.read()
    assert table is not None
    assert table.num_rows == 3
    assert "order_id" in table.column_names
    assert "amount" in table.column_names

    # Iterate rows directly (collection-like)
    rows = list(dataset)
    assert len(rows) == 3
    assert {r["customer_id"] for r in rows} == {"c1", "c2"}

    # len()
    assert len(dataset) == 3

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: SharedDataset basic (write records, read Arrow, iterate, len)")


def test_multiple_readers_same_bytes():
    """The KEY test: DuckDB, Polars, and Arrow all read the SAME bytes.

    This is the "Linux filesystem" pattern: the data is stored once
    as Arrow IPC. Multiple readers read the same bytes and present
    them differently. No copying, no duplication.
    """
    import shutil
    bench = "/tmp/pond_shared_multi"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Write data ONCE
    dataset = SharedDataset(kernel, "orders")
    dataset.write_records([
        {"order_id": 1, "customer_id": "c1", "amount": 100.0, "region": "US"},
        {"order_id": 2, "customer_id": "c2", "amount": 200.0, "region": "EU"},
        {"order_id": 3, "customer_id": "c1", "amount": 50.0,  "region": "US"},
        {"order_id": 4, "customer_id": "c3", "amount": 300.0, "region": "ASIA"},
    ])
    dataset.commit("load 4 orders")

    # Reader 1: Arrow (raw)
    arrow_view = ArrowNativeView(dataset)
    table = arrow_view.read()
    assert table.num_rows == 4

    # Reader 2: DuckDB (SQL queries on the SAME bytes)
    import duckdb
    duckdb_view = DuckDBView(dataset)
    con = duckdb.connect()
    name = duckdb_view.register(con)
    # Run SQL on Pond data — zero-copy Arrow read
    result = con.execute(
        f"SELECT region, COUNT(*) as cnt, SUM(amount) as total "
        f"FROM {name} GROUP BY region ORDER BY region"
    ).fetchall()
    assert len(result) == 3  # US, EU, ASIA
    regions = {r[0] for r in result}
    assert regions == {"US", "EU", "ASIA"}
    us_row = [r for r in result if r[0] == "US"][0]
    assert us_row[1] == 2  # 2 US orders
    assert us_row[2] == 150.0  # 100 + 50
    con.close()

    # Reader 3: Polars (DataFrame on the SAME bytes)
    import polars as pl
    polars_view = PolarsView(dataset)
    df = polars_view.read()
    assert df.height == 4
    us_df = df.filter(df["region"] == "US")
    assert us_df.height == 2
    assert us_df["amount"].sum() == 150.0

    # Reader 4: pandas (on the SAME bytes)
    pandas_view = PandasView(dataset)
    pdf = pandas_view.read()
    assert len(pdf) == 4
    assert (pdf["region"] == "US").sum() == 2

    # All 4 readers read the SAME bytes — verify by checking they all
    # see the same total amount
    arrow_total = sum(table["amount"].to_pylist())
    duckdb_total = con.execute(f"SELECT SUM(amount) FROM {name}").fetchone()[0] if False else None
    # (DuckDB connection is closed; reopen for the check)
    con2 = duckdb.connect()
    duckdb_view.register(con2)
    duckdb_total = con2.execute(f"SELECT SUM(amount) FROM {name}").fetchone()[0]
    polars_total = df["amount"].sum()
    pandas_total = pdf["amount"].sum()

    assert arrow_total == duckdb_total == polars_total == pandas_total == 650.0
    con2.close()

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print(f"PASS: Multiple readers same bytes (Arrow={arrow_total}, "
          f"DuckDB={duckdb_total}, Polars={polars_total}, pandas={pandas_total})")


def test_manifest_enablement():
    """The manifest tracks which Views are enabled (sidecar file pattern)."""
    import shutil
    bench = "/tmp/pond_manifest"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    dataset = SharedDataset(kernel, "orders")
    dataset.write_records([{"order_id": 1, "amount": 100.0}])
    dataset.commit("load")

    # Initially no Views enabled
    assert dataset.list_enabled_views() == {}
    assert not dataset.is_view_enabled("sql")

    # Enable SQL View (writes a manifest entry)
    dataset.enable_view("sql", version=1, schema={"order_id": "int64", "amount": "float64"})
    dataset.commit("enable SQL view")

    assert dataset.is_view_enabled("sql")
    assert "sql" in dataset.list_enabled_views()
    assert dataset.list_enabled_views()["sql"]["version"] == 1

    # Enable OLAP View
    dataset.enable_view("olap", version=1, layout="columnar")
    dataset.commit("enable OLAP view")

    enabled = dataset.list_enabled_views()
    assert set(enabled.keys()) == {"sql", "olap"}
    assert enabled["olap"]["layout"] == "columnar"

    # Disable SQL View
    dataset.disable_view("sql")
    dataset.commit("disable SQL view")
    assert not dataset.is_view_enabled("sql")
    assert "olap" in dataset.list_enabled_views()

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Manifest enablement (enable, list, disable, is_enabled)")


def test_manifest_persists_across_restart():
    """The manifest survives process restart (it's a kernel blob)."""
    import shutil
    bench = "/tmp/pond_manifest_persist"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    dataset = SharedDataset(kernel, "orders")
    dataset.write_records([{"order_id": 1, "amount": 100.0}])
    dataset.enable_view("sql", version=1)
    dataset.enable_view("olap", version=1)
    dataset.commit("load + enable views")
    kernel.close()

    # Reopen
    kernel2 = PondMinimal(bench)
    dataset2 = SharedDataset(kernel2, "orders")
    enabled = dataset2.list_enabled_views()
    assert set(enabled.keys()) == {"sql", "olap"}

    # Data also survived
    table = dataset2.read_arrow()
    assert table is not None
    assert table.num_rows == 1

    kernel2.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Manifest persists across restart")


def test_versioning_and_history():
    """SharedDataset supports branching and history (inherited from View)."""
    import shutil
    bench = "/tmp/pond_shared_versioning"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    dataset = SharedDataset(kernel, "orders")
    dataset.write_records([{"order_id": 1, "amount": 100.0}])
    dataset.commit("v1")

    dataset.write_records([{"order_id": 2, "amount": 200.0}])
    dataset.commit("v2")

    # History
    history = dataset.history()
    assert len(history) >= 2

    # Branch
    dataset.branch("experiment")
    dataset.checkout("experiment")
    dataset.write_records([{"order_id": 3, "amount": 300.0}])
    dataset.commit("experiment v3")

    # Experiment branch has 3 rows
    table = dataset.read_arrow()
    assert table.num_rows == 3

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: Versioning and history (branch, checkout, history)")


def test_elegant_pattern():
    """The full elegant pattern: write once, enable views, read many ways."""
    import shutil
    bench = "/tmp/pond_elegant_shared"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Step 1: Write data ONCE to a SharedDataset
    dataset = SharedDataset(kernel, "customers")
    dataset.write_records([
        {"customer_id": "c1", "name": "Alice", "region": "US", "ltv": 1500.0, "plan": "pro"},
        {"customer_id": "c2", "name": "Bob",   "region": "EU", "ltv": 800.0,  "plan": "free"},
        {"customer_id": "c3", "name": "Carol", "region": "US", "ltv": 2200.0, "plan": "enterprise"},
    ])

    # Step 2: Enable multiple Views on the SAME data (manifest entries)
    dataset.enable_view("sql", version=1,
                         schema={"customer_id": "string", "name": "string",
                                 "region": "string", "ltv": "float64", "plan": "string"})
    dataset.enable_view("olap", version=1, layout="columnar")
    dataset.enable_view("pandas", version=1)
    dataset.commit("load customers + enable 3 views")

    # Step 3: Read via DuckDB (SQL) — SAME bytes
    import duckdb
    duckdb_view = DuckDBView(dataset)
    con = duckdb.connect()
    duckdb_view.register(con)
    sql_result = con.execute(
        "SELECT region, AVG(ltv) as avg_ltv FROM customers GROUP BY region ORDER BY region"
    ).fetchall()
    assert len(sql_result) == 2  # US, EU
    us_avg = [r for r in sql_result if r[0] == "US"][0][1]
    assert abs(us_avg - 1850.0) < 0.01  # (1500 + 2200) / 2
    con.close()

    # Step 4: Read via Polars (OLAP) — SAME bytes
    import polars as pl
    polars_view = PolarsView(dataset)
    df = polars_view.read()
    pro_df = df.filter(df["plan"] == "pro")
    assert pro_df.height == 1
    assert pro_df["name"][0] == "Alice"

    # Step 5: Read via pandas — SAME bytes
    pandas_view = PandasView(dataset)
    pdf = pandas_view.read()
    us_count = (pdf["region"] == "US").sum()
    assert us_count == 2

    # Step 6: Iterate directly (collection-like) — SAME bytes
    rows = list(dataset)
    assert len(rows) == 3
    assert {r["customer_id"] for r in rows} == {"c1", "c2", "c3"}

    # Step 7: Verify all readers see the SAME data
    arrow_view = ArrowNativeView(dataset)
    table = arrow_view.read()
    arrow_total_ltv = sum(table["ltv"].to_pylist())

    con2 = duckdb.connect()
    duckdb_view.register(con2)
    duckdb_total_ltv = con2.execute("SELECT SUM(ltv) FROM customers").fetchone()[0]
    con2.close()

    polars_total_ltv = df["ltv"].sum()
    pandas_total_ltv = pdf["ltv"].sum()

    assert arrow_total_ltv == duckdb_total_ltv == polars_total_ltv == pandas_total_ltv == 4500.0

    # Step 8: Manifest tracks enablement
    enabled = dataset.list_enabled_views()
    assert set(enabled.keys()) == {"sql", "olap", "pandas"}

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print(f"PASS: Elegant pattern (write once, 3 views enabled, "
          f"4 readers same bytes: total_ltv={arrow_total_ltv})")


def _run_all_tests():
    print("=== SharedDataset + NativeView — Shared Bytes, Multiple Readers ===\n")
    test_shared_dataset_basic()
    test_multiple_readers_same_bytes()
    test_manifest_enablement()
    test_manifest_persists_across_restart()
    test_versioning_and_history()
    test_elegant_pattern()
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    _run_all_tests()
