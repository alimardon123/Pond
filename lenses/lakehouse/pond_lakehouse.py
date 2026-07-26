"""
PondLakehouse — the DuckDB-backed lakehouse façade over LakehouseLens.

This is the flagship application that tests whether Pond's Lens algebra
covers real workloads. It composes:

  - PondMinimal kernel (storage)
  - LakehouseLens (interprets bytes as Parquet row groups)
  - DuckDB (query engine, registered against Pond tables)

The lens (lakehouse_lens.py) is PyArrow-only and does NOT require DuckDB.
This module is the only place DuckDB is needed. A user who wants to
write Parquet row groups and do time-travel without ever running SQL
can use LakehouseLens directly and skip this module entirely.

Predicate + projection pushdown:
  When pruning is enabled and the SQL contains a WHERE clause, query()
  extracts predicates via sql_pushdown.py and calls
  read_with_encoded_pruning (which cascades down to column-chunk →
  row-group → full read based on the collection's storage mode).

Object-store-aware pruning:
  If use_pruning is None (default), pruning is auto-enabled when the
  kernel is backed by object storage (S3, GCS, etc.) — network RTT
  savings dwarf Python overhead. For local disk, pruning defaults to
  off (DuckDB native scan is faster for local data).
"""

from __future__ import annotations

import os
import sys
import tempfile
import shutil
from typing import Optional

# Make pond-core, pond-sdk, and the lakehouse lens importable
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "pond-sdk"))
sys.path.insert(0, SCRIPT_DIR)

# DuckDB is required for this façade only (not for the lens itself)
try:
    import duckdb
except ImportError:
    raise ImportError(
        "DuckDB is required for pond_lakehouse (the SQL façade). "
        "Install with: pip install duckdb. "
        "If you only need Parquet read/write without SQL, use "
        "LakehouseLens directly — it does not require DuckDB."
    )

# PyArrow for the Parquet/Arrow interchange
try:
    import pyarrow as pa
except ImportError:
    raise ImportError(
        "PyArrow is required for pond_lakehouse. "
        "Install with: pip install pyarrow"
    )

from kernel import PondMinimal  # noqa: E402
from lakehouse_lens import (  # noqa: E402
    LakehouseLens, DEFAULT_RANGE_ROW_GROUP_SIZE,
)
from sql_pushdown import extract_predicates, extract_columns  # noqa: E402


class PondLakehouse:
    """A lightweight lakehouse: Pond kernel + LakehouseLens + DuckDB.

    This is the flagship. It provides:
      - CREATE TABLE
      - INSERT
      - SELECT (via DuckDB, with pushdown to Parquet)
      - Time travel (AS OF commit hash)
      - Branching (dev/test branches on tables)
      - Merge (union merge for now)
      - Schema evolution (Parquet-native)
      - Range read/write for operational workloads
    """

    def __init__(self, base_dir: str, force_pruning: Optional[bool] = None):
        """Create a PondLakehouse.

        Args:
            base_dir: filesystem path or object store URL for the kernel.
            force_pruning: override auto-detection for predicate pushdown.
                None = auto (S3→on, local→off)
                True = always prune
                False = never prune
        """
        self.kernel = PondMinimal(base_dir)
        self.lens = LakehouseLens(self.kernel)
        self._force_pruning = force_pruning
        self.duckdb = duckdb.connect()

    def create_table(self, name: str, data: pa.Table) -> str:
        return self.lens.create_table(name, data)

    def insert(self, name: str, data: pa.Table) -> str:
        return self.lens.insert(name, data)

    def range_write(self, name: str, data: pa.Table, key_col: str,
                    row_group_size: int = DEFAULT_RANGE_ROW_GROUP_SIZE) -> str:
        """Range write: store data as row groups in the ProllyTreeIndex.
        See LakehouseLens.range_write for details."""
        return self.lens.range_write(name, data, key_col, row_group_size)

    def range_read(self, name: str,
                   start_key: Optional[str] = None,
                   end_key: Optional[str] = None) -> pa.Table:
        """Range read: scan a key range from the ProllyTreeIndex.
        See LakehouseLens.range_read for details."""
        return self.lens.range_read(name, start_key, end_key)

    def range_point_lookup(self, name: str, key: str) -> Optional[pa.Table]:
        """Point lookup: O(log N) via the ProllyTreeIndex."""
        return self.lens.range_point_lookup(name, key)

    def query(self, sql: str, table_name: Optional[str] = None,
              use_pruning: Optional[bool] = None) -> pa.Table:
        """Run a SQL query against a Pond-hosted table.

        If table_name is provided, the table is registered with DuckDB
        as a named relation. The SQL can then reference it by name.

        PREDICATE + PROJECTION PUSHDOWN:
        When pruning is enabled and the SQL contains a WHERE clause with
        simple column-op-value predicates, the query method automatically:
          1. Extracts WHERE predicates from the SQL
          2. Uses read_with_encoded_pruning (fastest available path) to
             skip non-matching row groups / column chunks
          3. Uses read_columns for projection pushdown (only needed columns)
          4. Registers the pruned+projected table with DuckDB
          5. Executes the SQL on the reduced dataset

        OBJECT-STORE-AWARE PRUNING:
        If use_pruning is None (default), pruning is auto-enabled when
        the kernel is backed by object storage (S3, GCS, etc.) — network
        RTT savings dwarf Python overhead. For local disk, pruning defaults
        to off (DuckDB native scan is faster for local data).
        Pass use_pruning=True/False to override.

        Args:
            sql: the SQL query string
            table_name: name of the table to register
            use_pruning: None=auto (object store→on, local→off),
                True=force on, False=force off.
        """
        if table_name:
            # Auto-decide pruning based on storage type or force_pruning override
            if use_pruning is None:
                if self._force_pruning is not None:
                    use_pruning = self._force_pruning
                else:
                    try:
                        from collection_metadata import CollectionMetadata
                        meta = CollectionMetadata(self.kernel)
                        use_pruning = meta.should_prune()
                    except Exception:
                        use_pruning = False  # default: no pruning if detection fails

            if use_pruning:
                table = self._read_with_pushdown(sql, table_name)
            else:
                table = self.lens.read_table(table_name)
            self.duckdb.register(table_name, table)
        return self.duckdb.execute(sql).to_arrow_table()

    def _read_with_pushdown(self, sql: str, table_name: str) -> pa.Table:
        """Read a table with predicate + projection pushdown.

        Extracts WHERE predicates and SELECT columns from the SQL, then
        uses the best available pruning read path:
          1. read_with_encoded_pruning (FastLanes-style) — fastest
          2. read_with_column_chunk_pruning — per-column-chunk I/O
          3. read_with_pruning — row-group pruning only
          4. read_table — full read (fallback)

        Each path falls back to the next if the storage mode is not
        available for this collection (e.g., legacy range_write data
        uses path 3 or 4; range_write_encoded data uses path 1).

        Falls back to full read_table if predicate extraction fails.
        """
        try:
            # Extract predicates from WHERE clause
            predicates = extract_predicates(sql)

            # Extract projected columns from SELECT clause
            columns = extract_columns(sql)

            if predicates:
                # Try the fastest path first; each path falls back
                # internally if the storage mode is not available.
                # read_with_encoded_pruning falls back to
                # read_with_column_chunk_pruning which falls back to
                # read_with_pruning which falls back to read_table.
                table = self.lens.read_with_encoded_pruning(
                    table_name,
                    predicates=predicates,
                    row_filter=None,  # let DuckDB evaluate the predicate
                )
                # Apply projection on the pruned result.
                # MUST include WHERE columns so DuckDB can evaluate the filter.
                if columns and columns != ["*"]:
                    # Add predicate columns to the projection
                    pred_cols = [p[0] for p in predicates]
                    all_cols = list(set(columns + pred_cols))
                    available = [c for c in all_cols if c in table.column_names]
                    if available:
                        table = table.select(available)
                return table
            elif columns and columns != ["*"]:
                # No WHERE but projection pushdown
                return self.lens.read_columns(table_name, columns)
            else:
                # No pushdown possible — full read
                return self.lens.read_table(table_name)
        except (ImportError, KeyError, AttributeError):
            # Missing extension or column — fall back to full read
            return self.lens.read_table(table_name)
        except Exception:
            # Any other failure in pushdown → fall back to full read.
            # (Catches parser bugs, predicate evaluation errors, etc.)
            return self.lens.read_table(table_name)

    def query_at(self, sql: str, table_name: str, commit_hash: str) -> pa.Table:
        """Time travel: query a table at a specific commit."""
        table = self.lens.read_table(table_name, commit_hash)
        # Register with a temp name to avoid clobbering the live table
        temp_name = f"{table_name}_at_{commit_hash[:8]}"
        self.duckdb.register(temp_name, table)
        # Replace table_name with temp_name in the SQL (simple substitution)
        sql_at = sql.replace(table_name, temp_name)
        return self.duckdb.execute(sql_at).to_arrow_table()

    def branch(self, table_name: str, branch_name: str) -> str:
        return self.lens.branch(table_name, branch_name)

    def commit_to_branch(self, table_name: str, branch_name: str,
                         data: pa.Table) -> str:
        return self.lens.commit_to_branch(table_name, branch_name, data)

    def merge_branch(self, table_name: str, branch_name: str) -> str:
        return self.lens.merge_branch(table_name, branch_name)

    def history(self, table_name: str) -> list[dict]:
        return self.lens.history(table_name)

    def close(self):
        self.duckdb.close()
        self.kernel.close()


# ---------------------------------------------------------------------------
# Self-tests (kept here for backward compat with `python pond_lakehouse.py`)
# TODO: M25 — move these to tests/integration/test_lakehouse_flagship.py
# ---------------------------------------------------------------------------

def _self_test():
    """Verify the PondLakehouse flagship works end-to-end."""
    print("=== Pond Lakehouse self-test ===")

    tmpdir = tempfile.mkdtemp(prefix="pond_lakehouse_")
    try:
        lh = PondLakehouse(tmpdir)

        # Test 1: create a table and query it
        users = pa.table({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "age": [30, 25, 35],
        })
        lh.create_table("users", users)
        result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
        assert result.column("cnt")[0].as_py() == 3, f"expected 3, got {result.column('cnt')[0]}"
        print(f"  [OK] create table + SELECT COUNT(*)")

        # Test 2: insert and re-query
        new_users = pa.table({
            "id": [4, 5],
            "name": ["dave", "eve"],
            "age": [40, 28],
        })
        lh.insert("users", new_users)
        result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
        assert result.column("cnt")[0].as_py() == 5, f"expected 5, got {result.column('cnt')[0]}"
        print(f"  [OK] insert + SELECT COUNT(*)")

        # Test 3: filter query (age > 30 → carol, dave)
        result = lh.query(
            "SELECT name FROM users WHERE age > 30 ORDER BY name",
            table_name="users",
        )
        names = [r.as_py() for r in result.column("name")]
        assert names == ["carol", "dave"], f"expected ['carol', 'dave'], got {names}"
        print(f"  [OK] SELECT with WHERE + ORDER BY")

        # Test 4: time travel — query at the original commit (3 rows)
        history = lh.history("users")
        original_commit = history[-1]["hash"]
        result = lh.query_at(
            "SELECT COUNT(*) AS cnt FROM users",
            table_name="users",
            commit_hash=original_commit,
        )
        assert result.column("cnt")[0].as_py() == 3, \
            f"time travel: expected 3 rows at original commit, got {result.column('cnt')[0]}"
        print(f"  [OK] time travel: query at original commit returns 3 rows")

        # Test 5: branching
        lh.branch("users", "dev")
        dev_users = pa.table({
            "id": [6],
            "name": ["frank"],
            "age": [50],
        })
        lh.commit_to_branch("users", "dev", dev_users)
        result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
        assert result.column("cnt")[0].as_py() == 5, \
            "main HEAD unchanged after dev branch commit"
        print(f"  [OK] branch: dev branch commit doesn't affect main HEAD")

        # Test 6: merge dev into main (union merge — dups from common ancestor)
        lh.merge_branch("users", "dev")
        result = lh.query("SELECT COUNT(*) AS cnt FROM users", table_name="users")
        cnt = result.column("cnt")[0].as_py()
        assert cnt == 11, \
            f"union merge: expected 11 rows (5 main + 6 dev, with dups), got {cnt}"
        result = lh.query(
            "SELECT COUNT(*) AS cnt FROM users WHERE name = 'frank'",
            table_name="users",
        )
        assert result.column("cnt")[0].as_py() == 1, "frank appears once after merge"
        print(f"  [OK] merge: 11 rows after union merge (frank included; dups from common ancestor)")

        # Test 7: history shows merge commit with 2 parents
        history = lh.history("users")
        latest = history[0]
        assert latest["second_parent"] is not None, "merge commit has second_parent"
        print(f"  [OK] history: merge commit has 2 parents")

        # Test 8: schema evolution — add a column
        users_v2 = pa.table({
            "id": [7],
            "name": ["grace"],
            "age": [45],
            "email": ["grace@example.com"],  # new column
        })
        lh.insert("users", users_v2)
        result = lh.query(
            "SELECT name, email FROM users WHERE email IS NOT NULL",
            table_name="users",
        )
        emails = [r.as_py() for r in result.column("email")]
        assert emails == ["grace@example.com"], f"expected ['grace@example.com'], got {emails}"
        print(f"  [OK] schema evolution: new column 'email' added; old rows have NULL")

        # Test 9: aggregation query
        result = lh.query(
            "SELECT COUNT(*) AS cnt, AVG(age) AS avg_age, MIN(age) AS min_age, MAX(age) AS max_age FROM users",
            table_name="users",
        )
        cnt = result.column("cnt")[0].as_py()
        assert cnt == 12, f"aggregation: expected 12 rows, got {cnt}"
        print(f"  [OK] aggregation: COUNT/AVG/MIN/MAX over 12 rows")

        # Test 10: JOIN two tables
        orders = pa.table({
            "order_id": [1, 2, 3],
            "user_id": [1, 2, 1],
            "amount": [100.0, 200.0, 150.0],
        })
        lh.create_table("orders", orders)
        lh.duckdb.register("users", lh.lens.read_table("users"))
        lh.duckdb.register("orders", lh.lens.read_table("orders"))
        result = lh.query("""
            SELECT u.name, SUM(o.amount) AS total
            FROM users u
            JOIN orders o ON u.id = o.user_id
            GROUP BY u.name
            ORDER BY total DESC
        """)
        names = [r.as_py() for r in result.column("name")]
        assert "alice" in names, "JOIN: alice has orders"
        print(f"  [OK] JOIN: users ⋈ orders on user_id, grouped by name")

        # ---------------------------------------------------------------
        # Test 11-14: Range read/write on top of the ProllyTreeIndex
        # ---------------------------------------------------------------

        events = pa.table({
            "event_id": [f"e{i:04d}" for i in range(100)],
            "user_id": [i % 10 for i in range(100)],
            "amount": [float(i) for i in range(100)],
        })
        lh.range_write("events", events, key_col="event_id", row_group_size=25)
        print(f"  [OK] range_write: 100 rows in 4 row groups")

        all_rows = lh.range_read("events")
        assert all_rows.num_rows == 100, \
            f"range_read all: expected 100 rows, got {all_rows.num_rows}"
        print(f"  [OK] range_read all: 100 rows")

        range_result = lh.range_read("events", "e0050", "e0080")
        assert range_result.num_rows == 50, \
            f"range_read [e0050,e0080]: expected 50 rows (2 row groups), got {range_result.num_rows}"
        print(f"  [OK] range_read [e0050,e0080]: 50 rows (2 row groups; caller filters exact rows)")

        point_result = lh.range_point_lookup("events", "e0042")
        assert point_result is not None, "point lookup should find a row group"
        assert point_result.num_rows == 25, \
            f"point lookup: expected row group of 25 rows, got {point_result.num_rows}"
        lh.duckdb.register("point_result", point_result)
        exact = lh.duckdb.execute(
            "SELECT event_id, amount FROM point_result WHERE event_id = 'e0042'"
        ).to_arrow_table()
        assert exact.num_rows == 1, f"exact filter: expected 1 row, got {exact.num_rows}"
        assert exact.column("amount")[0].as_py() == 42.0
        print(f"  [OK] range_point_lookup('e0042') + DuckDB filter: O(log N) tree lookup + 1 row")

        lh.close()
        print("\nAll Pond Lakehouse tests pass.")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _benchmark():
    """Quick benchmark: compare PondLakehouse vs native DuckDB+Parquet."""
    print("\n=== PondLakehouse vs native DuckDB+Parquet benchmark ===")
    tmpdir = tempfile.mkdtemp(prefix="pond_lh_bench_")
    try:
        import time as _time

        n_rows = 10_000
        ids = list(range(n_rows))
        names = [f"user_{i}" for i in range(n_rows)]
        ages = [20 + (i % 50) for i in range(n_rows)]
        data = pa.table({"id": ids, "name": names, "age": ages})

        # PondLakehouse
        lh = PondLakehouse(os.path.join(tmpdir, "pond"))
        t0 = _time.perf_counter()
        lh.create_table("users", data)
        t_create_pond = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = lh.query("SELECT COUNT(*) FROM users", table_name="users")
        t_count_pond = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = lh.query("SELECT AVG(age) FROM users", table_name="users")
        t_avg_pond = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = lh.query("SELECT name FROM users WHERE age > 50", table_name="users")
        t_filter_pond = _time.perf_counter() - t0

        lh.close()

        # Native DuckDB + Parquet
        native_dir = os.path.join(tmpdir, "native")
        os.makedirs(native_dir)
        con = duckdb.connect(os.path.join(native_dir, "native.db"))
        con.execute("INSTALL parquet; LOAD parquet;")
        parquet_path = os.path.join(native_dir, "users.parquet")

        t0 = _time.perf_counter()
        import pyarrow.parquet as pq
        pq.write_table(data, parquet_path)
        t_create_native = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = con.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_path}')").fetchone()
        t_count_native = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = con.execute(f"SELECT AVG(age) FROM read_parquet('{parquet_path}')").fetchone()
        t_avg_native = _time.perf_counter() - t0

        t0 = _time.perf_counter()
        result = con.execute(f"SELECT name FROM read_parquet('{parquet_path}') WHERE age > 50").fetchall()
        t_filter_native = _time.perf_counter() - t0

        con.close()

        print(f"\n  Operation         | PondLakehouse | Native DuckDB+Parquet")
        print(f"  ------------------|---------------|----------------------")
        print(f"  create (10K rows) | {t_create_pond*1000:.1f}ms        | {t_create_native*1000:.1f}ms")
        print(f"  COUNT(*)          | {t_count_pond*1000:.1f}ms         | {t_count_native*1000:.1f}ms")
        print(f"  AVG(age)          | {t_avg_pond*1000:.1f}ms         | {t_avg_native*1000:.1f}ms")
        print(f"  filter + scan     | {t_filter_pond*1000:.1f}ms         | {t_filter_native*1000:.1f}ms")

        print(f"\n  Overhead of Pond layer (create): {((t_create_pond/t_create_native - 1) * 100):.0f}%")
        print(f"  Overhead of Pond layer (count):  {((t_count_pond/t_count_native - 1) * 100):.0f}%")
        print(f"  Overhead of Pond layer (avg):    {((t_avg_pond/t_avg_native - 1) * 100):.0f}%")
        print(f"  Overhead of Pond layer (filter): {((t_filter_pond/t_filter_native - 1) * 100):.0f}%")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    _self_test()
    _benchmark()
