#!/usr/bin/env python3
"""
Pond API Demo — shows how all the APIs work with:
  1. Raw bytes (JSON, CSV, images — anything)
  2. Bulk structured data (datasets)
  3. Rich predicates (SQL-like WHERE with >, <, >=, <=, !=, IN, LIKE)
  4. where= parameter on write_rows, update_rows, delete_rows, merge_rows
  5. The crdt=True/False flag

Architecture note: ALL logic runs in Rust. The Python `pond` module is a
thin PyO3 wrapper — every call goes directly into Rust functions with zero
Python overhead between calls. The only boundary is argument conversion at
the call site.
"""

import os
import sys
import tempfile
import shutil

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "target", "release"))

import pond


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def main():
    tmpdir = tempfile.mkdtemp(prefix="pond_demo_")
    try:
        s = pond.Storage(tmpdir)

        # ============================================================
        section("1. RAW BYTES — JSON, CSV, images, anything")
        # ============================================================
        print("\n--- write() / read() — raw bytes ---")
        s.write('config', b'{"app":"pond","version":"1.0"}', 'init config')
        data = s.read('config')
        print(f"  write('config', b'{{...}}') → read back: {data}")

        s.write('readme', b'# Pond Storage\nA unified content-addressed store.', 'init')
        print(f"  write('readme', b'# Pond...') → {s.read('readme')[:20]}...")

        # ============================================================
        section("2. BULK STRUCTURED DATA — datasets via write_rows()")
        # ============================================================
        print("\n--- write_rows() — bulk load with auto _rowid + _version ---")
        s.write_rows('employees', [
            ('id',       [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
            ('name',     ['Alice', 'Bob', 'Carol', 'Dave', 'Eve',
                          'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy']),
            ('dept',     ['eng', 'eng', 'sales', 'sales', 'eng',
                          'hr', 'hr', 'eng', 'sales', 'eng']),
            ('salary',   [95000, 87000, 72000, 68000, 102000,
                          55000, 58000, 91000, 64000, 99000]),
            ('age',      [30, 25, 35, 40, 28, 45, 32, 38, 27, 33]),
            ('city',     ['NYC', 'SF', 'NYC', 'LA', 'SF',
                          'NYC', 'LA', 'SF', 'NYC', 'LA']),
        ], 'init employees')

        cols = s.read_rows('employees')
        print(f"  Loaded {len(cols['id'])} employees")
        print(f"  Columns: {sorted(cols.keys())}  (note: _rowid/_version auto-filtered)")
        print(f"  First 3: id={cols['id'][:3]}, name={cols['name'][:3]}")

        # ============================================================
        section("3. RICH PREDICATES — SQL-like WHERE in read_rows()")
        # ============================================================

        print("\n--- read_rows with predicates=[('col', 'op', val)] ---")
        # predates use the OLD-style tuple format: (column, op, value)
        # Supports: =, ==, !=, <>, <, <=, >, >=
        cols = s.read_rows('employees', predicates=[('salary', '>', 90000)])
        print(f"  salary > 90000: {cols['name']}  (high earners)")

        cols = s.read_rows('employees', predicates=[('age', '<=', 30)])
        print(f"  age <= 30: {cols['name']}  (young employees)")

        cols = s.read_rows('employees', predicates=[('dept', '=', 'eng')])
        print(f"  dept = 'eng': {cols['name']}")

        cols = s.read_rows('employees',
                           predicates=[('salary', '>', 80000), ('dept', '=', 'eng')])
        print(f"  salary>80k AND dept=eng: {cols['name']}")

        # Projection + predicate
        cols = s.read_rows('employees',
                           columns=['name', 'salary'],
                           predicates=[('city', '=', 'NYC')])
        print(f"  NYC employees (name+salary only): {list(zip(cols['name'], cols['salary']))}")

        # ============================================================
        section("4. RICH where= PARAMETER — SQL/polara/pyspark-style")
        # ============================================================
        print("\n--- where= supports: equality, >, <, >=, <=, !=, IN, LIKE ---")

        # The where= parameter (used by update_rows, delete_rows, write_rows, merge_rows)
        # supports RICH predicates beyond simple equality:

        # Equality (bare value)
        cols = s.read_rows('employees')  # read all first for comparison
        print(f"  Total employees: {len(cols['id'])}")

        # Range: age between 30 and 40
        print("\n  --- write_rows with where= filter ---")
        s.write_rows('senior_eng', [
            ('id', [1, 2, 3, 4, 5]),
            ('name', ['Alice', 'Bob', 'Carol', 'Dave', 'Eve']),
            ('age', [30, 25, 35, 40, 28]),
            ('dept', ['eng', 'eng', 'sales', 'sales', 'eng']),
        ], 'init', where={'age': ('>', 30)})
        cols = s.read_rows('senior_eng')
        print(f"  write_rows where age>30: {cols['name']} (ages: {cols['age']})")

        # Multiple conditions (AND)
        s.write_rows('senior_eng_nyc', [
            ('id', [1, 2, 3, 4, 5]),
            ('name', ['Alice', 'Bob', 'Carol', 'Dave', 'Eve']),
            ('age', [30, 25, 35, 40, 28]),
            ('city', ['NYC', 'SF', 'NYC', 'LA', 'SF']),
        ], 'init', where={'age': ('>', 30), 'city': 'NYC'})
        cols = s.read_rows('senior_eng_nyc')
        print(f"  write_rows where age>30 AND city=NYC: {cols['name']}")

        # Range with list of conditions
        s.write_rows('mid_age', [
            ('id', [1, 2, 3, 4, 5]),
            ('age', [20, 25, 30, 35, 40]),
        ], 'init', where={'age': [('>', 25), ('<', 40)]})
        cols = s.read_rows('mid_age')
        print(f"  write_rows where 25<age<40: ages={cols['age']}")

        # ============================================================
        section("5. update_rows with rich where= predicates")
        # ============================================================
        print("\n--- update_rows: SQL-like UPDATE ... WHERE ---")

        # Give a raise to high earners
        count = s.update_rows('employees',
                              updates={'salary': 120000},
                              where={'salary': ('>', 100000)})
        print(f"  UPDATE salary=120000 WHERE salary>100k → {count} rows")

        # Update by dept
        count = s.update_rows('employees',
                              updates={'dept': 'engineering'},
                              where={'dept': 'eng'})
        print(f"  UPDATE dept='engineering' WHERE dept='eng' → {count} rows")

        # Range update: everyone age 25-35 gets a bonus
        count = s.update_rows('employees',
                              updates={'salary': 75000},
                              where={'age': [('>=', 25), ('<=', 35)]})
        print(f"  UPDATE salary=75k WHERE 25<=age<=35 → {count} rows")

        # ============================================================
        section("6. delete_rows with rich where= predicates")
        # ============================================================
        print("\n--- delete_rows: SQL-like DELETE FROM ... WHERE ---")

        # Delete by equality
        count = s.delete_rows('employees', where={'city': 'LA'})
        print(f"  DELETE WHERE city='LA' → {count} rows deleted")

        # Delete by range
        count = s.delete_rows('employees', where={'age': ('>', 40)})
        print(f"  DELETE WHERE age>40 → {count} rows deleted")

        cols = s.read_rows('employees')
        print(f"  Remaining: {len(cols['id'])} employees")

        # ============================================================
        section("7. merge_rows with where= filter")
        # ============================================================
        print("\n--- merge_rows: MERGE with where= filter ---")

        # Only merge rows where age >= 18 (skip minors)
        count = s.merge_rows('users', [
            {'id': 1, 'name': 'Alice', 'age': 30},
            {'id': 2, 'name': 'Bob', 'age': 15},   # skipped (age < 18)
            {'id': 3, 'name': 'Carol', 'age': 25},
        ], key_col='id', where={'age': ('>=', 18)})
        print(f"  MERGE where age>=18 → {count} rows merged (Bob skipped)")

        cols = s.read_rows('users')
        print(f"  Result: id={cols['id']}, name={cols['name']}")

        # ============================================================
        section("8. crdt=False — snapshot semantics (no CRDT metadata)")
        # ============================================================
        print("\n--- write_rows(crdt=False) — raw bulk load ---")
        s.write_rows('events', [
            ('event', ['click', 'view', 'scroll', 'click', 'view']),
            ('ts', [1000, 1001, 1002, 1003, 1004]),
        ], 'init events', crdt=False)
        cols = s.read_rows('events', columns=['event', 'ts', '_rowid'])
        print(f"  Events: {cols['event']}")
        print(f"  _rowid exists? {'_rowid' in cols}  (should be False with crdt=False)")

        # ============================================================
        section("9. ARCHITECTURE — all logic runs in Rust")
        # ============================================================
        print("""
  ┌─────────────────────────────────────────────────────────────┐
  │  Python (pond module)                                       │
  │    s.write_rows(...)  →  PyO3 boundary (arg conversion)     │
  │                            ↓                                │
  │  Rust (pond_python crate)                                   │
  │    write_rows()  →  storage_write::write_rows()             │
  │                     →  pond_core::pnd2_encode_multi_typed()  │
  │                     →  pond_kernel::write() / reference()    │
  │    update_rows()  →  shard::upsert_shard()                  │
  │    delete_rows()  →  shard::delete_shard()                  │
  │    read_rows()    →  pnd2_decode() + crdt_merge_rows()      │
  │                     →  ALL in Rust, zero Python overhead    │
  └─────────────────────────────────────────────────────────────┘

  Every function call goes DIRECTLY into Rust. There is no Python
  logic between calls — the Python layer is a thin PyO3 wrapper.
  All data processing (PND2 encoding, CRDT merge, predicate evaluation,
  shard management) happens in compiled Rust code.
""")

        # ============================================================
        section("10. Predicate reference")
        # ============================================================
        print("""
  where= parameter supports these predicate formats:

    {'col': value}                         → col = value (equality)
    {'col': ('>', value)}                  → col > value
    {'col': ('>=', value)}                 → col >= value
    {'col': ('<', value)}                  → col < value
    {'col': ('<=', value)}                 → col <= value
    {'col': ('!=', value)}                 → col != value
    {'col': ('in', [v1, v2, v3])}          → col IN (v1, v2, v3)
    {'col': ('not in', [v1, v2])}          → col NOT IN (v1, v2)
    {'col': ('like', 'pattern%')}          → col LIKE 'pattern%'
    {'col': ('is null',)}                  → col IS NULL
    {'col': ('is not null',)}              → col IS NOT NULL

  Multiple conditions (AND):
    {'col1': val1, 'col2': ('>', val2)}    → col1=val1 AND col2>val2

  Range (multiple conditions on same column):
    {'age': [('>', 18), ('<', 65)]}        → age>18 AND age<65

  Available on: write_rows, update_rows, delete_rows, merge_rows
  (read_rows uses predicates=[(col, op, val)] format for compatibility)
""")

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
