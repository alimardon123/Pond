#!/usr/bin/env python3
"""
Test: SET * + on_miss_target + column mapping + multi-action merge.

Verifies the full SQL MERGE semantics:
  - SET * (copy all source cols + override)
  - SET without * (only update listed cols)
  - on_miss_target (WHEN NOT MATCHED BY SOURCE)
  - t./s. column mapping (different names per side)
  - Multi-action with conditional WHERE on both t. and s.
  - Static values in SET clause
"""

import os
import sys
import shutil
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "target", "release"))

import pond


def _fresh():
    tmpdir = tempfile.mkdtemp(prefix="pond_set_")
    return pond.Storage(tmpdir), tmpdir


def test_set_star_copy_all_plus_override():
    """SET *, t.col = val → copy all source cols, then override specific ones."""
    print("\n=== Test 1: SET * (copy all + override) ===")
    s, tmpdir = _fresh()
    try:
        s.write_rows('users', [
            ('id', [1, 2]),
            ('name', ['alice', 'bob']),
            ('age', [30, 25]),
            ('city', ['NYC', 'LA']),
        ], 'init')

        # Source has all columns + we override city with static value
        result = s.merge_rows('users', [
            {'id': 1, 'name': 'ALICE', 'age': 31, 'city': 'Boston'},
        ], on='t.id = s.id',
           on_match="UPDATE SET *, t.city = 'Seattle'")

        cols = s.read_rows('users')
        idx = cols['id'].index(1)
        assert cols['name'][idx] == 'ALICE', f"name should be ALICE: {cols['name'][idx]}"
        assert cols['age'][idx] == 31, f"age should be 31: {cols['age'][idx]}"
        assert cols['city'][idx] == 'Seattle', f"city should be Seattle: {cols['city'][idx]}"
        print(f"  [OK] All source cols copied + city overridden to 'Seattle'")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_set_explicit_only_listed():
    """SET t.col = val (no *) → only update listed cols, keep rest from target."""
    print("\n=== Test 2: SET explicit (only listed cols) ===")
    s, tmpdir = _fresh()
    try:
        s.write_rows('users', [
            ('id', [1, 2]),
            ('name', ['alice', 'bob']),
            ('age', [30, 25]),
            ('city', ['NYC', 'LA']),
        ], 'init')

        # Only update name + age, keep city from target
        result = s.merge_rows('users', [
            {'id': 1, 'name': 'ALICE', 'age': 31, 'city': 'Boston'},
        ], on='t.id = s.id',
           on_match="UPDATE SET t.name = s.name, t.age = s.age")

        cols = s.read_rows('users')
        idx = cols['id'].index(1)
        assert cols['name'][idx] == 'ALICE', f"name should be ALICE: {cols['name'][idx]}"
        assert cols['age'][idx] == 31, f"age should be 31: {cols['age'][idx]}"
        assert cols['city'][idx] == 'NYC', f"city should stay NYC: {cols['city'][idx]}"
        print(f"  [OK] Only name+age updated, city preserved from target")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_set_column_mapping():
    """SET t.target_col = s.source_col → map different column names."""
    print("\n=== Test 3: SET column mapping (different names) ===")
    s, tmpdir = _fresh()
    try:
        s.write_rows('accounts', [
            ('id', [1, 2]),
            ('full_name', ['alice', 'bob']),
            ('balance', [100, 200]),
        ], 'init')

        # Source has uid + display_name, target has id + full_name
        result = s.merge_rows('accounts', [
            {'uid': 1, 'display_name': 'ALICE', 'amount': 999},
        ], on='t.id = s.uid',
           on_match="UPDATE SET t.full_name = s.display_name, t.balance = s.amount")

        cols = s.read_rows('accounts')
        idx = cols['id'].index(1)
        assert cols['full_name'][idx] == 'ALICE', f"full_name should be ALICE: {cols['full_name'][idx]}"
        assert cols['balance'][idx] == 999, f"balance should be 999: {cols['balance'][idx]}"
        print(f"  [OK] Column mapping: s.display_name → t.full_name, s.amount → t.balance")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_set_static_values():
    """SET t.col = 'static', t.col2 = 999 → set to static values."""
    print("\n=== Test 4: SET static values ===")
    s, tmpdir = _fresh()
    try:
        s.write_rows('products', [
            ('id', [1, 2, 3]),
            ('name', ['Widget', 'Gadget', 'Doohickey']),
            ('price', [10.0, 20.0, 30.0]),
            ('in_stock', [1, 1, 1]),
        ], 'init')

        result = s.merge_rows('products', [
            {'id': 2},
        ], on='t.id = s.id',
           on_match="UPDATE SET t.price = 0, t.in_stock = false")

        cols = s.read_rows('products')
        idx = cols['id'].index(2)
        assert cols['price'][idx] == 0, f"price should be 0: {cols['price'][idx]}"
        assert cols['in_stock'][idx] == False or cols['in_stock'][idx] == 0, f"in_stock should be false/0: {cols['in_stock'][idx]}"
        # Other products unchanged
        idx1 = cols['id'].index(1)
        assert cols['price'][idx1] == 10.0
        print(f"  [OK] Static values: price=0, in_stock=false")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_on_miss_target_delete():
    """on_miss_target DELETE → delete target rows not in source."""
    print("\n=== Test 5: on_miss_target DELETE ===")
    s, tmpdir = _fresh()
    try:
        s.write_rows('users', [
            ('id', [1, 2, 3, 4]),
            ('status', ['active', 'inactive', 'active', 'inactive']),
        ], 'init')

        # Source only has id=1 and id=3. id=2 and id=4 are unmatched targets.
        # Delete unmatched targets with status='inactive'
        result = s.merge_rows('users', [
            {'id': 1},
            {'id': 3},
        ], on='t.id = s.id',
           on_match='UPDATE',
           on_miss_target="DELETE WHERE t.status = 'inactive'")

        cols = s.read_rows('users')
        ids = cols['id']
        assert 2 not in ids, f"id=2 should be deleted: {ids}"
        assert 4 not in ids, f"id=4 should be deleted: {ids}"
        assert 1 in ids and 3 in ids, f"id=1,3 should remain: {ids}"
        print(f"  [OK] Unmatched inactive targets deleted: remaining={ids}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_multi_action_with_set_and_where():
    """Multi-action: UPDATE WHERE + SET + DELETE WHERE on both t. and s."""
    print("\n=== Test 6: Full multi-action with SET + WHERE ===")
    s, tmpdir = _fresh()
    try:
        s.write_rows('inventory', [
            ('id', [1, 2, 3]),
            ('qty', [50, 5, 100]),
            ('status', ['stocked', 'low', 'stocked']),
        ], 'init')

        result = s.merge_rows('inventory', [
            {'id': 2, 'new_qty': 100, 'remove': False},
            {'id': 3, 'new_qty': 0, 'remove': True},
            {'id': 5, 'new_qty': 50, 'remove': False},
        ], on='t.id = s.id',
           on_match="UPDATE WHERE t.status = 'low' SET t.qty = s.new_qty, t.status = 'stocked'; "
                    "DELETE WHERE s.remove = true",
           on_miss="INSERT WHERE s.new_qty > 0")

        cols = s.read_rows('inventory')
        ids = cols['id']

        # id=2: was low → updated to qty=100, status=stocked
        idx2 = ids.index(2)
        assert cols['qty'][idx2] == 100, f"id=2 qty should be 100: {cols['qty'][idx2]}"
        assert cols['status'][idx2] == 'stocked', f"id=2 status should be stocked: {cols['status'][idx2]}"

        # id=3: remove=True → deleted
        assert 3 not in ids, f"id=3 should be deleted: {ids}"

        # id=5: not matched → inserted (new_qty=50 > 0)
        assert 5 in ids, f"id=5 should be inserted: {ids}"

        print(f"  [OK] Multi-action: id=2 updated, id=3 deleted, id=5 inserted")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_insert_with_set_mapping():
    """INSERT with SET → column mapping for new rows."""
    print("\n=== Test 7: INSERT with SET column mapping ===")
    s, tmpdir = _fresh()
    try:
        s.write_rows('customers', [
            ('customer_id', [1]),
            ('name', ['existing']),
            ('email', ['old@test.com']),
        ], 'init')

        # Source has cid + mail + full_name → map to customer_id + email + name
        result = s.merge_rows('customers', [
            {'cid': 2, 'mail': 'new@test.com', 'full_name': 'New Customer'},
        ], on='t.customer_id = s.cid',
           on_match='UPDATE',
           on_miss="INSERT SET t.customer_id = s.cid, t.email = s.mail, t.name = s.full_name")

        cols = s.read_rows('customers')
        idx2 = cols['customer_id'].index(2)
        assert cols['name'][idx2] == 'New Customer', f"name should be New Customer: {cols['name'][idx2]}"
        assert cols['email'][idx2] == 'new@test.com', f"email should be new@test.com: {cols['email'][idx2]}"
        print(f"  [OK] INSERT with column mapping: cid→customer_id, mail→email, full_name→name")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    tests = [
        test_set_star_copy_all_plus_override,
        test_set_explicit_only_listed,
        test_set_column_mapping,
        test_set_static_values,
        test_on_miss_target_delete,
        test_multi_action_with_set_and_where,
        test_insert_with_set_mapping,
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
    print(f"SET * + on_miss_target tests: {passed} passed, {failed} failed")
    print(f"{'='*60}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
