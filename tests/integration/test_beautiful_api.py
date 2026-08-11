#!/usr/bin/env python3
"""
Test: Beautiful Layered API — write_rows, update_rows, delete_rows, merge_rows.

Verifies the unified write model:
  - write_rows: bulk load, auto-adds _rowid + _version
  - update_rows: SQL-like UPDATE ... WHERE
  - delete_rows: SQL-like DELETE FROM ... WHERE (filter-based, not just rowids)
  - merge_rows: SQL-like MERGE / INSERT ON CONFLICT
  - All with optional crdt=True flag (default: True)
  - _rowid, _version, _deleted always used internally

Also verifies:
  - Branch merge works correctly with CRDT data
  - read_rows auto-filters _rowid/_version/_deleted
"""

import os
import sys
import shutil
import tempfile

# Add the build output to the path
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "target", "release"))

import pond


def _fresh_storage():
    """Create a fresh Storage in a temp dir."""
    tmpdir = tempfile.mkdtemp(prefix="pond_api_")
    s = pond.Storage(tmpdir)
    return s, tmpdir


def test_write_rows_auto_crdt():
    """write_rows auto-adds _rowid + _version by default."""
    print("\n=== Test 1: write_rows auto-adds CRDT metadata ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3]),
            ('name', ['alice', 'bob', 'carol']),
        ], 'init')

        # Read back — _rowid/_version should be filtered by default
        cols = s.read_rows('users')
        assert set(cols.keys()) == {'id', 'name'}, f"Got: {sorted(cols.keys())}"
        assert cols['id'] == [1, 2, 3]
        assert cols['name'] == ['alice', 'bob', 'carol']
        print(f"  [OK] Default read filters _rowid/_version: {sorted(cols.keys())}")

        # Explicitly request _rowid + _version
        cols = s.read_rows('users', columns=['id', '_rowid', '_version'])
        assert '_rowid' in cols
        assert '_version' in cols
        assert len(cols['_rowid']) == 3
        assert len(cols['_version']) == 3
        print(f"  [OK] Explicit columns= shows _rowid: {cols['_rowid'][0][:8]}...")

        s_opt = pond.Storage  # for static reference
        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_write_rows_no_crdt():
    """write_rows with crdt=False skips _rowid/_version."""
    print("\n=== Test 2: write_rows(crdt=False) ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('logs', [
            ('event', ['click', 'view', 'scroll']),
        ], 'init', crdt=False)

        cols = s.read_rows('logs', columns=['event', '_rowid', '_version'])
        assert cols['event'] == ['click', 'view', 'scroll']
        # _rowid / _version should NOT exist
        assert '_rowid' not in cols, f"_rowid should not exist with crdt=False: {cols.keys()}"
        assert '_version' not in cols, f"_version should not exist with crdt=False: {cols.keys()}"
        print(f"  [OK] crdt=False → no _rowid/_version: {sorted(cols.keys())}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_update_rows_with_filter():
    """update_rows with WHERE filter — SQL-like UPDATE."""
    print("\n=== Test 3: update_rows with WHERE filter ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3, 4]),
            ('name', ['alice', 'bob', 'carol', 'dave']),
            ('city', ['NYC', 'LA', 'NYC', 'SF']),
            ('status', ['active', 'active', 'active', 'active']),
        ], 'init')

        # UPDATE users SET status='inactive' WHERE city='NYC'
        count = s.update_rows('users',
                              updates={'status': 'inactive'},
                              where="city = 'NYC'")
        assert count == 2, f"Expected 2 updates, got {count}"
        print(f"  [OK] Updated {count} rows (city='NYC')")

        # Verify
        cols = s.read_rows('users')
        # NYC users (alice, carol) should be inactive
        nyc_indices = [i for i, c in enumerate(cols['city']) if c == 'NYC']
        for i in nyc_indices:
            assert cols['status'][i] == 'inactive', f"Row {i} should be inactive"
        # Non-NYC users should still be active
        non_nyc = [i for i, c in enumerate(cols['city']) if c != 'NYC']
        for i in non_nyc:
            assert cols['status'][i] == 'active', f"Row {i} should still be active"
        print(f"  [OK] NYC users inactive, others active")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_delete_rows_with_filter():
    """delete_rows with WHERE filter — SQL-like DELETE."""
    print("\n=== Test 4: delete_rows with WHERE filter ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3, 4]),
            ('name', ['alice', 'bob', 'carol', 'dave']),
            ('status', ['active', 'inactive', 'active', 'inactive']),
        ], 'init')

        # DELETE FROM users WHERE status='inactive'
        count = s.delete_rows('users', where="status = 'inactive'")
        assert count == 2, f"Expected 2 deletes, got {count}"
        print(f"  [OK] Deleted {count} rows (status='inactive')")

        # Verify — only active users remain
        cols = s.read_rows('users')
        assert len(cols['id']) == 2, f"Expected 2 remaining, got {len(cols['id'])}"
        for s_val in cols['status']:
            assert s_val == 'active', f"Should only have active users, got {s_val}"
        print(f"  [OK] {len(cols['id'])} active users remain")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_delete_all_rows():
    """delete_rows with no WHERE — delete all."""
    print("\n=== Test 5: delete_rows (all) ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3]),
            ('name', ['a', 'b', 'c']),
        ], 'init')

        count = s.delete_rows('users')  # no where = delete all
        assert count == 3, f"Expected 3 deletes, got {count}"
        print(f"  [OK] Deleted all {count} rows")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_merge_rows_upsert():
    """merge_rows — insert + update by key."""
    print("\n=== Test 6: merge_rows (upsert) ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2]),
            ('name', ['alice', 'bob']),
        ], 'init')

        # Merge: update id=1, insert id=3
        result = s.merge_rows('users', [
            {'id': 1, 'name': 'ALICE_UPDATED'},
            {'id': 3, 'name': 'carol_new'},
        ], on='id')
        assert result['matched'] + result['inserted'] == 2, f"Expected 2 processed, got {result}"
        print(f"  [OK] Merged: {result}")

        # Verify
        cols = s.read_rows('users')
        ids = cols['id']
        names = cols['name']
        # id=1 should be updated
        idx1 = ids.index(1)
        assert names[idx1] == 'ALICE_UPDATED', f"id=1 should be updated: {names[idx1]}"
        # id=2 should be unchanged
        idx2 = ids.index(2)
        assert names[idx2] == 'bob', f"id=2 should be unchanged: {names[idx2]}"
        # id=3 should be inserted
        idx3 = ids.index(3)
        assert names[idx3] == 'carol_new', f"id=3 should be inserted: {names[idx3]}"
        print(f"  [OK] id=1 updated, id=2 unchanged, id=3 inserted")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_update_rows_no_crdt():
    """update_rows with crdt=False — rewrite HEAD."""
    print("\n=== Test 7: update_rows(crdt=False) ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3]),
            ('name', ['a', 'b', 'c']),
            ('city', ['NYC', 'LA', 'NYC']),
        ], 'init')

        count = s.update_rows('users',
                              updates={'name': 'UPDATED'},
                              where="city = 'NYC'",
                              crdt=False)
        assert count == 2, f"Expected 2 updates, got {count}"
        print(f"  [OK] Updated {count} rows (crdt=False, HEAD rewrite)")

        cols = s.read_rows('users')
        nyc_names = [cols['name'][i] for i, c in enumerate(cols['city']) if c == 'NYC']
        assert all(n == 'UPDATED' for n in nyc_names), f"NYC names should be UPDATED: {nyc_names}"
        print(f"  [OK] NYC rows updated via HEAD rewrite")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_branch_merge_with_crdt():
    """Branch merge works correctly with CRDT data."""
    print("\n=== Test 8: branch merge with CRDT data ===")
    s, tmpdir = _fresh_storage()
    try:
        # Bulk load on main
        s.write_rows('users', [
            ('id', [1, 2, 3]),
            ('name', ['alice', 'bob', 'carol']),
        ], 'init')

        # Branch dev
        s.branch('users', 'dev')
        s.checkout('users', 'dev')

        # Update on dev
        s.update_rows('users',
                      updates={'name': 'ALICE_DEV'},
                      where="id = 1",
                      key_col='id')

        # Add a new row on dev
        s.merge_rows('users', [{'id': 4, 'name': 'dave_dev'}], on='id')

        # Back to main
        s.checkout('users', 'main')

        # Merge dev into main
        result = s.merge('users', source='dev', target='main', message='merge dev')

        # Verify the merged result
        cols = s.read_rows('users')
        ids = cols['id']
        names = cols['name']

        # Should have 4 rows: 1 (updated), 2, 3 (unchanged), 4 (new)
        assert len(ids) == 4, f"Expected 4 rows after merge, got {len(ids)}"
        assert set(ids) == {1, 2, 3, 4}, f"Expected ids {{1,2,3,4}}, got {set(ids)}"

        # id=1 should reflect the dev update
        idx1 = ids.index(1)
        assert names[idx1] == 'ALICE_DEV', f"id=1 should be ALICE_DEV: {names[idx1]}"
        print(f"  [OK] Merge: {len(ids)} rows, id=1 updated to ALICE_DEV")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def main():
    tests = [
        test_write_rows_auto_crdt,
        test_write_rows_no_crdt,
        test_update_rows_with_filter,
        test_delete_rows_with_filter,
        test_delete_all_rows,
        test_merge_rows_upsert,
        test_update_rows_no_crdt,
        test_branch_merge_with_crdt,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f"  [FAIL] {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            import traceback
            print(f"  [ERROR] {t.__name__}: {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{'='*60}")
    print(f"Beautiful API tests: {passed} passed, {failed} failed")
    print(f"{'='*60}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
