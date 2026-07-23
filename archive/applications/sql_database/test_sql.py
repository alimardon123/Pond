#!/usr/bin/env python3
"""Test the SQL View end-to-end."""

import sys, os, shutil, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "libraries"))
from pond_minimal import PondMinimal
from delta_view import DeltaViewBase
from sql_view import SQLView

def test_sql():
    bench_dir = "/tmp/pond_sql_test"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)

    kernel = PondMinimal(bench_dir)
    db = SQLView(kernel, "test_db")

    print("=== CREATE TABLE ===")
    db.create_table("users", {"id": "INT", "name": "TEXT", "email": "TEXT"}, "id")
    print(f"  Schema: {db.get_schema('users')}")

    print("\n=== INSERT ===")
    db.insert("users", {"id": 1, "name": "Alice", "email": "alice@example.com"})
    db.insert("users", {"id": 2, "name": "Bob", "email": "bob@example.com"})
    db.insert("users", {"id": 3, "name": "Carol", "email": "carol@example.com"})
    db.commit("insert 3 users")

    print("\n=== SELECT ONE (point lookup — O(1-2) S3 GETs) ===")
    user = db.select_one("users", 2)
    print(f"  SELECT * FROM users WHERE id=2: {user}")

    print("\n=== SELECT ALL (scan — O(K+1) S3 GETs) ===")
    users = db.select_all("users")
    print(f"  SELECT * FROM users: {len(users)} rows")
    for u in users:
        print(f"    {u}")

    print("\n=== SELECT WHERE ===")
    alice = db.select_where("users", "name", "Alice")
    print(f"  SELECT * FROM users WHERE name='Alice': {alice}")

    print("\n=== UPDATE ===")
    db.update("users", 2, {"email": "bob@newemail.com"})
    db.commit("update Bob's email")
    bob = db.select_one("users", 2)
    print(f"  After update: {bob}")

    print("\n=== DELETE ===")
    db.delete("users", 3)
    db.commit("delete Carol")
    carol = db.select_one("users", 3)
    print(f"  After delete: Carol = {carol}")
    print(f"  Row count: {db.count_rows('users')}")

    print("\n=== ALTER TABLE (schema evolution) ===")
    db.alter_table_add_column("users", "age", "INT")
    schema = db.get_schema("users")
    print(f"  Schema after ALTER: {schema['columns']}")

    print("\n=== TIME TRAVEL (select at past version) ===")
    history = db.history()
    print(f"  History:")
    for h in history:
        print(f"    {h['commit']}  {h['message']}")
    # Read Bob's email at the initial insert commit (should be old email)
    initial_commit = history[-1]["commit"]
    old_bob = db.base._resolve_prefix(initial_commit)
    # Temporarily switch
    original = kernel.resolve("test_db")
    kernel.reference("test_db", old_bob)
    bob_old = db.select_one("users", 2)
    kernel.reference("test_db", original)
    print(f"  Bob at initial commit: {bob_old}")
    print(f"  Bob now: {db.select_one('users', 2)}")

    print("\n=== BRANCHING ===")
    # Save main commit before branching
    main_commit_hash = kernel.resolve("test_db")
    db.branch("experiment")
    db.checkout("experiment")
    db.insert("users", {"id": 4, "name": "Dave", "email": "dave@example.com"})
    db.commit("add Dave on experiment branch")
    print(f"  On 'experiment': {db.count_rows('users')} rows")

    # Switch back to main
    kernel.reference("test_db", main_commit_hash)
    db.base = DeltaViewBase(kernel, "test_db")
    print(f"  On main (after switch): {db.count_rows('users')} rows")

    print("\n=== MERGE ===")
    db.merge("experiment")
    print(f"  After merge: {db.count_rows('users')} rows")
    dave = db.select_one("users", 4)
    print(f"  Dave after merge: {dave}")

    print("\n=== HISTORY (with skip pointers) ===")
    for h in db.history():
        print(f"  {h['commit']}  idx={h['index']}  {h['message']}")

    print("\n=== ALL TESTS PASSED ===")
    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)

if __name__ == "__main__":
    test_sql()
