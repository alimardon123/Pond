import json, time, sys, os
from typing import Optional, Any

sys.path.insert(0, "/home/z/my-project/pond_repo/prototype")
sys.path.insert(0, "/home/z/my-project/pond_repo/libraries")
from pond_minimal import PondMinimal
from delta_view import DeltaViewBase

class SQLLens:
    def __init__(self, kernel, db_name="db"):
        self.kernel = kernel
        self.db_name = db_name
        self.base = DeltaViewBase(kernel, db_name)

    def create_table(self, table_name, columns, primary_key="id"):
        schema = {"table": table_name, "columns": columns, "primary_key": primary_key, "created_at": time.time()}
        schema_bytes = json.dumps(schema, sort_keys=True).encode()
        schema_hash = self.kernel.write(schema_bytes)
        self.base.stage(f"_schema/{table_name}", schema_hash)
        self.base.commit(f"CREATE TABLE {table_name}")

    def get_schema(self, table_name):
        h = self.base.lookup(f"_schema/{table_name}")
        if not h: return None
        return json.loads(self.kernel.read_blob(h))

    def alter_table_add_column(self, table_name, column_name, column_type="TEXT"):
        schema = self.get_schema(table_name)
        if not schema: raise ValueError(f"Table '{table_name}' does not exist")
        schema["columns"][column_name] = column_type
        schema_bytes = json.dumps(schema, sort_keys=True).encode()
        schema_hash = self.kernel.write(schema_bytes)
        self.base.stage(f"_schema/{table_name}", schema_hash)
        self.base.commit(f"ALTER TABLE {table_name} ADD COLUMN {column_name}")

    def insert(self, table_name, row):
        schema = self.get_schema(table_name)
        if not schema: raise ValueError(f"Table '{table_name}' does not exist")
        pk = schema["primary_key"]
        if pk not in row: raise ValueError(f"Missing primary key '{pk}' in row")
        pk_value = str(row[pk])
        key = f"{table_name}/{pk_value}"
        row_bytes = json.dumps(row, sort_keys=True).encode()
        row_hash = self.kernel.write(row_bytes)
        self.base.stage(key, row_hash)

    def insert_batch(self, table_name, rows):
        for row in rows: self.insert(table_name, row)

    def commit(self, message=""):
        return self.base.commit(message or "SQL commit")

    def select_one(self, table_name, pk_value):
        key = f"{table_name}/{str(pk_value)}"
        h = self.base.lookup(key)
        if not h: return None
        return json.loads(self.kernel.read_blob(h))

    def select_all(self, table_name):
        state = self.base.read_all()
        prefix = f"{table_name}/"
        rows = []
        for key, h in state.items():
            if key.startswith(prefix) and not key.startswith("_schema/"):
                rows.append(json.loads(self.kernel.read_blob(h)))
        return rows

    def select_where(self, table_name, column, value):
        return [r for r in self.select_all(table_name) if r.get(column) == value]

    def update(self, table_name, pk_value, updates):
        row = self.select_one(table_name, pk_value)
        if not row: raise ValueError(f"Row with pk={pk_value} not found in '{table_name}'")
        row.update(updates)
        row_bytes = json.dumps(row, sort_keys=True).encode()
        row_hash = self.kernel.write(row_bytes)
        schema = self.get_schema(table_name)
        pk = schema["primary_key"]
        key = f"{table_name}/{str(row[pk])}"
        self.base.stage(key, row_hash)

    def delete(self, table_name, pk_value):
        self.base.stage_delete(f"{table_name}/{str(pk_value)}")

    def history(self, limit=20):
        return self.base.history(limit)

    def branch(self, name):
        return self.base.branch(name)

    def checkout(self, name):
        self.base.checkout(name)

    def list_branches(self):
        return self.base.list_branches()

    def merge(self, branch_name):
        return self.base.merge(branch_name, f"merge '{branch_name}'")

    def undo(self, steps=1):
        return self.base.undo(steps)

    def list_tables(self):
        state = self.base.read_all()
        return sorted(k[len("_schema/"):] for k in state if k.startswith("_schema/"))

    def count_rows(self, table_name):
        state = self.base.read_all()
        prefix = f"{table_name}/"
        return sum(1 for k in state if k.startswith(prefix) and not k.startswith("_schema/"))
