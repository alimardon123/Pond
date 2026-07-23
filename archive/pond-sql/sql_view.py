"""
Reworked SQL View on ProllyViewBase.

Uses Prolly trees for O(log N) point lookups, bounded delta journal
for O(1) commits, and View-level indexes for fast secondary lookups.

Features:
  - CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, ALTER TABLE
  - Point lookups via ProllyTree.lookup() — O(log N)
  - Full scans via ProllyTree.read_all() — O(N/chunk_size)
  - Secondary indexes via ProllyViewBase.build_index()
  - Time travel, branching, merge, history
  - All on the 3-primitive kernel (Write/Read/Reference)
"""

import json, time, sys, os
from typing import Optional, Any

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "pond-sdk"))
from pond_minimal import PondMinimal
from prolly_view import ProllyViewBase


class SQLView:
    """SQL database on Pond. Uses ProllyViewBase for all storage."""

    def __init__(self, kernel: PondMinimal, db_name: str = "db"):
        self.kernel = kernel
        self.db_name = db_name
        self.base = ProllyViewBase(kernel, db_name)

    def create_table(self, table_name: str, columns: dict, primary_key: str = "id"):
        schema = {"table": table_name, "columns": columns, "primary_key": primary_key}
        h = self.kernel.write(json.dumps(schema, sort_keys=True).encode())
        self.base.stage(f"_schema/{table_name}", h)
        self.base.commit(f"CREATE TABLE {table_name}")

    def get_schema(self, table_name):
        h = self.base.lookup(f"_schema/{table_name}")
        return json.loads(self.kernel.read_blob(h)) if h else None

    def insert(self, table_name, row):
        schema = self.get_schema(table_name)
        if not schema: raise ValueError(f"Table '{table_name}' does not exist")
        pk = schema["primary_key"]
        if pk not in row: raise ValueError(f"Missing primary key '{pk}'")
        h = self.kernel.write(json.dumps(row, sort_keys=True).encode())
        self.base.stage(f"{table_name}/{row[pk]}", h)

    def insert_batch(self, table_name, rows):
        for r in rows: self.insert(table_name, r)

    def commit(self, message=""):
        return self.base.commit(message or "SQL commit")

    def select_one(self, table_name, pk_value):
        """O(log N) point lookup via Prolly tree."""
        h = self.base.lookup(f"{table_name}/{pk_value}")
        return json.loads(self.kernel.read_blob(h)) if h else None

    def select_all(self, table_name):
        """O(N/chunk_size) full scan."""
        state = self.base.read_all()
        prefix = f"{table_name}/"
        rows = []
        for k, h in state.items():
            if k.startswith(prefix) and not k.startswith("_schema/"):
                rows.append(json.loads(self.kernel.read_blob(h)))
        return rows

    def select_where(self, table_name, column, value):
        return [r for r in self.select_all(table_name) if r.get(column) == value]

    def update(self, table_name, pk_value, updates):
        row = self.select_one(table_name, pk_value)
        if not row: raise ValueError(f"Row pk={pk_value} not found")
        row.update(updates)
        h = self.kernel.write(json.dumps(row, sort_keys=True).encode())
        self.base.stage(f"{table_name}/{pk_value}", h)

    def delete(self, table_name, pk_value):
        self.base.stage_delete(f"{table_name}/{pk_value}")

    def alter_table_add_column(self, table_name, col, col_type="TEXT"):
        schema = self.get_schema(table_name)
        if not schema: raise ValueError(f"Table '{table_name}' does not exist")
        schema["columns"][col] = col_type
        h = self.kernel.write(json.dumps(schema, sort_keys=True).encode())
        self.base.stage(f"_schema/{table_name}", h)
        self.base.commit(f"ALTER TABLE {table_name} ADD {col}")

    def create_index(self, table_name, column):
        """Build a secondary index on a column. O(N) build, O(log N) lookup."""
        def extractor(blob_hash):
            row = json.loads(self.kernel.read_blob(blob_hash))
            return str(row.get(column, ""))
        self.base.build_index(f"{table_name}_{column}", extractor)

    def lookup_by_index(self, table_name, column, value):
        """O(log N) lookup via secondary index."""
        pk = self.base.lookup_by_index(f"{table_name}_{column}", str(value))
        if not pk: return None
        return json.loads(self.kernel.read_blob(pk))

    def history(self, limit=20): return self.base.history(limit)
    def branch(self, name): return self.base.branch(name)
    def checkout(self, name): self.base.checkout(name)
    def list_branches(self): return self.base.list_branches()
    def merge(self, name): return self.base.merge(name)
    def undo(self, steps=1): return self.base.undo(steps)
    def list_tables(self):
        return sorted(k[len("_schema/"):] for k in self.base.read_all() if k.startswith("_schema/"))
    def count_rows(self, table_name):
        prefix = f"{table_name}/"
        return sum(1 for k in self.base.read_all() if k.startswith(prefix) and not k.startswith("_schema/"))
