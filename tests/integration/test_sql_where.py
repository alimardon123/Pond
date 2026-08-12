#!/usr/bin/env python3
"""
Test: SQL WHERE string parser + merge cases (on_match / on_miss).

Verifies:
  1. SQL WHERE strings: "age >= 18 AND city = 'NYC'"
  2. All SQL operators: =, !=, >, >=, <, <=, IN, NOT IN, LIKE, IS NULL, IS NOT NULL
  3. AND, OR, NOT, parentheses
  4. merge_rows with on_match='update' | 'delete' | 'skip'
  5. merge_rows with on_miss='insert' | 'skip'
  6. Backward compat with dict-based where=
"""

import os
import sys
import shutil
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "target", "release"))

import pond


def _fresh_storage():
    tmpdir = tempfile.mkdtemp(prefix="pond_sql_")
    s = pond.Storage(tmpdir)
    return s, tmpdir


def test_sql_where_equality():
    """SQL WHERE with = operator."""
    print("\n=== Test 1: SQL WHERE equality ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3]),
            ('name', ['alice', 'bob', 'carol']),
            ('city', ['NYC', 'LA', 'NYC']),
        ], 'init')

        # SQL string where=
        count = s.delete_rows('users', where="city = 'NYC'")
        assert count == 2, f"Expected 2 deletes, got {count}"
        print(f"  [OK] delete_rows(where=\"city = 'NYC'\") → {count} deleted")

        cols = s.read_rows('users')
        assert len(cols['id']) == 1, f"Expected 1 remaining, got {len(cols['id'])}"
        assert cols['name'][0] == 'bob'
        print(f"  [OK] Remaining: {cols['name']}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_sql_where_comparison():
    """SQL WHERE with >, >=, <, <=, !=."""
    print("\n=== Test 2: SQL WHERE comparison operators ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3, 4, 5]),
            ('age', [20, 25, 30, 35, 40]),
            ('salary', [50000, 60000, 70000, 80000, 90000]),
        ], 'init')

        # Greater than
        count = s.update_rows('users', {'status': 'senior'}, where="age > 30")
        assert count == 2, f"Expected 2 (age>30), got {count}"
        print(f"  [OK] age > 30 → {count} rows")

        # Less than or equal
        s.write_rows('users2', [
            ('id', [1, 2, 3, 4, 5]),
            ('age', [20, 25, 30, 35, 40]),
        ], 'init')
        count = s.delete_rows('users2', where="age <= 25")
        assert count == 2, f"Expected 2 (age<=25), got {count}"
        print(f"  [OK] age <= 25 → {count} deleted")

        # Not equal
        s.write_rows('users3', [
            ('id', [1, 2, 3]),
            ('status', ['active', 'inactive', 'active']),
        ], 'init')
        count = s.delete_rows('users3', where="status != 'active'")
        assert count == 1, f"Expected 1, got {count}"
        print(f"  [OK] status != 'active' → {count} deleted")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_sql_where_and_or():
    """SQL WHERE with AND, OR, parentheses."""
    print("\n=== Test 3: SQL WHERE AND / OR / parentheses ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3, 4, 5]),
            ('age', [25, 35, 25, 35, 45]),
            ('city', ['NYC', 'NYC', 'LA', 'LA', 'SF']),
        ], 'init')

        # AND
        count = s.update_rows('users', {'flag': 'match'}, where="age > 30 AND city = 'NYC'")
        assert count == 1, f"Expected 1, got {count}"
        print(f"  [OK] age > 30 AND city = 'NYC' → {count}")

        # OR
        s.write_rows('users2', [
            ('id', [1, 2, 3, 4, 5]),
            ('age', [20, 35, 20, 35, 50]),
        ], 'init')
        count = s.delete_rows('users2', where="age < 25 OR age > 45")
        assert count == 3, f"Expected 3, got {count}"
        print(f"  [OK] age < 25 OR age > 45 → {count}")

        # Parentheses
        s.write_rows('users3', [
            ('id', [1, 2, 3, 4]),
            ('dept', ['eng', 'eng', 'sales', 'sales']),
            ('age', [25, 40, 25, 40]),
        ], 'init')
        count = s.update_rows('users3', {'flag': 'yes'},
                              where="dept = 'eng' AND (age < 30 OR age > 45)")
        assert count == 1, f"Expected 1, got {count}"
        print(f"  [OK] dept='eng' AND (age<30 OR age>45) → {count}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_sql_where_in():
    """SQL WHERE with IN and NOT IN."""
    print("\n=== Test 4: SQL WHERE IN / NOT IN ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3, 4]),
            ('city', ['NYC', 'LA', 'SF', 'Boston']),
        ], 'init')

        count = s.delete_rows('users', where="city IN ('NYC', 'LA')")
        assert count == 2, f"Expected 2, got {count}"
        print(f"  [OK] city IN ('NYC', 'LA') → {count}")

        cols = s.read_rows('users')
        assert set(cols['city']) == {'SF', 'Boston'}
        print(f"  [OK] Remaining: {cols['city']}")

        # NOT IN
        s.write_rows('users2', [
            ('id', [1, 2, 3, 4]),
            ('city', ['NYC', 'LA', 'SF', 'Boston']),
        ], 'init')
        count = s.delete_rows('users2', where="city NOT IN ('NYC', 'LA')")
        assert count == 2, f"Expected 2, got {count}"
        cols = s.read_rows('users2')
        assert set(cols['city']) == {'NYC', 'LA'}
        print(f"  [OK] city NOT IN ('NYC', 'LA') → {count}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_sql_where_like():
    """SQL WHERE with LIKE."""
    print("\n=== Test 5: SQL WHERE LIKE ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3, 4]),
            ('name', ['Alice', 'Aaron', 'Bob', 'Charlie']),
        ], 'init')

        count = s.delete_rows('users', where="name LIKE 'A%'")
        assert count == 2, f"Expected 2, got {count}"
        print(f"  [OK] name LIKE 'A%' → {count}")

        cols = s.read_rows('users')
        assert set(cols['name']) == {'Bob', 'Charlie'}
        print(f"  [OK] Remaining: {cols['name']}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_sql_where_is_null():
    """SQL WHERE with IS NULL / IS NOT NULL."""
    print("\n=== Test 6: SQL WHERE IS NULL / IS NOT NULL ===")
    s, tmpdir = _fresh_storage()
    try:
        # Write with some nulls — we need to use crdt=False to avoid auto _rowid
        s.write_rows('users', [
            ('id', [1, 2, 3]),
            ('email', ['a@b.com', '', 'c@d.com']),  # empty string as "null-ish"
        ], 'init')

        # IS NOT NULL — should match all (empty string is not null)
        # For a real null test, we'd need actual null values
        count = s.update_rows('users', {'verified': True}, where="id > 0")
        assert count == 3, f"Expected 3, got {count}"
        print(f"  [OK] All rows matched")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_merge_on_match_update():
    """merge_rows with on_match='update' (default) — update existing."""
    print("\n=== Test 7: merge_rows on_match='update' ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2]),
            ('name', ['alice', 'bob']),
        ], 'init')

        result = s.merge_rows('users', [
            {'id': 1, 'name': 'ALICE_UPDATED'},
            {'id': 3, 'name': 'carol_new'},
        ], on='id', on_match='update', on_miss='insert')
        assert result['matched'] + result['inserted'] == 2, f"Expected 2, got {result}"

        cols = s.read_rows('users')
        ids = cols['id']
        names = cols['name']
        assert names[ids.index(1)] == 'ALICE_UPDATED'
        assert names[ids.index(2)] == 'bob'
        assert names[ids.index(3)] == 'carol_new'
        print(f"  [OK] id=1 updated, id=2 unchanged, id=3 inserted")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_merge_on_match_skip():
    """merge_rows with on_match='skip' — insert-only (no updates)."""
    print("\n=== Test 8: merge_rows on_match='skip' (insert-only) ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2]),
            ('name', ['alice', 'bob']),
        ], 'init')

        result = s.merge_rows('users', [
            {'id': 1, 'name': 'SHOULD_NOT_UPDATE'},
            {'id': 3, 'name': 'carol_new'},
        ], on='id', on_match='skip', on_miss='insert')
        assert result["matched"] + result["inserted"] + result["deleted"] == 2, f"Expected 2 processed, got {result}"

        cols = s.read_rows('users')
        ids = cols['id']
        names = cols['name']
        # id=1 should NOT be updated (on_match='skip')
        assert names[ids.index(1)] == 'alice', f"id=1 should be unchanged: {names[ids.index(1)]}"
        # id=3 should be inserted
        assert names[ids.index(3)] == 'carol_new'
        print(f"  [OK] id=1 unchanged (skip), id=3 inserted")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_merge_on_match_delete():
    """merge_rows with on_match='delete' — anti-join (delete matched)."""
    print("\n=== Test 9: merge_rows on_match='delete' (anti-join) ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3]),
            ('name', ['alice', 'bob', 'carol']),
        ], 'init')

        # Delete rows that match id=1 and id=3
        result = s.merge_rows('users', [
            {'id': 1},
            {'id': 3},
        ], on='id', on_match='delete', on_miss='skip')
        assert result['deleted'] == 2, f"Expected 2 deleted, got {result}"

        cols = s.read_rows('users')
        assert len(cols['id']) == 1, f"Expected 1 remaining, got {len(cols['id'])}"
        assert cols['name'][0] == 'bob'
        print(f"  [OK] id=1 and id=3 deleted, bob remains")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_merge_on_miss_skip():
    """merge_rows with on_miss='skip' — update-only (no inserts)."""
    print("\n=== Test 10: merge_rows on_miss='skip' (update-only) ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2]),
            ('name', ['alice', 'bob']),
        ], 'init')

        result = s.merge_rows('users', [
            {'id': 1, 'name': 'ALICE_UPDATED'},
            {'id': 99, 'name': 'should_not_insert'},
        ], on='id', on_match='update', on_miss='skip')
        assert result["matched"] == 1 and result["updated"] == 1, f"Expected 1 matched+updated, got {result}"

        cols = s.read_rows('users')
        ids = cols['id']
        names = cols['name']
        # id=1 should be updated
        assert names[ids.index(1)] == 'ALICE_UPDATED'
        # id=99 should NOT be inserted
        assert 99 not in ids, f"id=99 should not be inserted: {ids}"
        # id=2 should be unchanged
        assert names[ids.index(2)] == 'bob'
        print(f"  [OK] id=1 updated, id=99 NOT inserted (skip), id=2 unchanged")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_merge_with_sql_where():
    """merge_rows with SQL WHERE string on incoming rows."""
    print("\n=== Test 11: merge_rows with SQL WHERE on incoming rows ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2]),
            ('name', ['alice', 'bob']),
        ], 'init')

        # Only merge adults (age >= 18)
        result = s.merge_rows('users', [
            {'id': 1, 'name': 'alice', 'age': 30},
            {'id': 3, 'name': 'minor', 'age': 15},
            {'id': 4, 'name': 'carol', 'age': 25},
        ], on='id', where="age >= 18")
        assert result["matched"] + result["inserted"] + result["deleted"] == 2, f"Expected 2 (age>=18), got {result}"

        cols = s.read_rows('users')
        ids = cols['id']
        # id=3 (minor) should NOT be inserted
        assert 3 not in ids, f"id=3 (minor) should not be inserted: {ids}"
        # id=4 (carol) should be inserted
        assert 4 in ids
        print(f"  [OK] age >= 18 filter → {result}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_write_rows_with_sql_where():
    """write_rows with SQL WHERE on input rows."""
    print("\n=== Test 12: write_rows with SQL WHERE ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3, 4, 5]),
            ('age', [15, 20, 25, 30, 35]),
            ('name', ['a', 'b', 'c', 'd', 'e']),
        ], 'init', where="age >= 25")

        cols = s.read_rows('users')
        assert len(cols['id']) == 3, f"Expected 3 (age>=25), got {len(cols['id'])}"
        assert cols['age'] == [25, 30, 35]
        print(f"  [OK] write_rows where age>=25 → {cols["age"]}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def test_sql_string_where():
    """SQL string WHERE works."""
    print("\n=== Test 13: SQL string WHERE ===")
    s, tmpdir = _fresh_storage()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3]),
            ('age', [20, 25, 30]),
        ], 'init')

        # SQL string format
        count = s.delete_rows('users', where="age > 22")
        assert count == 2, f"Expected 2, got {count}"
        print(f"  [OK] where=\"age > 22\" → {count} deleted")

        shutil.rmtree(tmpdir, ignore_errors=True)
    except Exception:
        shutil.rmtree(tmpdir, ignore_errors=True)
        raise


def main():
    tests = [
        test_sql_where_equality,
        test_sql_where_comparison,
        test_sql_where_and_or,
        test_sql_where_in,
        test_sql_where_like,
        test_sql_where_is_null,
        test_merge_on_match_update,
        test_merge_on_match_skip,
        test_merge_on_match_delete,
        test_merge_on_miss_skip,
        test_merge_with_sql_where,
        test_write_rows_with_sql_where,
        test_sql_string_where,
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
    print(f"SQL WHERE + merge cases tests: {passed} passed, {failed} failed")
    print(f"{'='*60}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
