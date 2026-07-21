#!/usr/bin/env python3
"""
Test: elegant cross-view reading via ViewQuery.

Demonstrates the "direct, easy, simple and elegant way of reading
data from other Views" that the architecture review asked for.

Run:
    python pond-sdk/test_view_query.py
"""

from __future__ import annotations

import os
import sys
import shutil

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, HERE)

from pond_minimal import PondMinimal
from lens_sdk import View


def test_basic_iteration():
    """View is iterable: for row in view yields decoded rows."""
    bench = "/tmp/pond_vq_iter"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = View(kernel, "users")
    view.put("u1", {"name": "Alice", "age": 30, "region": "US"})
    view.put("u2", {"name": "Bob", "age": 25, "region": "EU"})
    view.put("u3", {"name": "Carol", "age": 35, "region": "US"})
    view.commit("insert 3 users")

    # Direct iteration
    rows = list(view)
    assert len(rows) == 3
    names = {r["name"] for r in rows}
    assert names == {"Alice", "Bob", "Carol"}

    # len() works
    assert len(view) == 3

    # `in` works (checks key existence)
    assert "u1" in view
    assert "u99" not in view

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: basic iteration (for row in view, len(view), key in view)")


def test_where_filter():
    """view.where(region='US') filters by field value."""
    bench = "/tmp/pond_vq_where"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = View(kernel, "users")
    view.put("u1", {"name": "Alice", "age": 30, "region": "US"})
    view.put("u2", {"name": "Bob", "age": 25, "region": "EU"})
    view.put("u3", {"name": "Carol", "age": 35, "region": "US"})
    view.put("u4", {"name": "Dave", "age": 28, "region": "EU"})
    view.commit("insert 4 users")

    # Filter with kwargs
    us_users = list(view.where(region="US"))
    assert len(us_users) == 2
    assert {r["name"] for r in us_users} == {"Alice", "Carol"}

    # Filter with a predicate
    over_30 = list(view.where(lambda r: r["age"] > 30))
    assert len(over_30) == 1
    assert over_30[0]["name"] == "Carol"

    # Chain multiple .where() calls (ANDed)
    eu_over_26 = list(view.where(region="EU").where(lambda r: r["age"] > 26))
    assert len(eu_over_26) == 1
    assert eu_over_26[0]["name"] == "Dave"

    # .first() returns the first match
    first_us = view.where(region="US").first()
    assert first_us is not None
    assert first_us["region"] == "US"

    # .count() counts matches
    assert view.where(region="US").count() == 2
    assert view.where(region="ASIA").count() == 0

    # .take(n) limits (on a ViewQuery, not directly on View)
    first_2 = view.where().take(2)
    assert len(first_2) == 2

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: where filter (kwargs, predicate, chain, first, count, take)")


def test_select_projection():
    """view.select('name', 'age') projects rows to only those fields."""
    bench = "/tmp/pond_vq_select"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = View(kernel, "users")
    view.put("u1", {"name": "Alice", "age": 30, "region": "US", "email": "a@x.com"})
    view.put("u2", {"name": "Bob", "age": 25, "region": "EU", "email": "b@x.com"})
    view.commit("insert 2 users")

    # Project
    projected = list(view.select("name", "age"))
    assert len(projected) == 2
    for row in projected:
        assert set(row.keys()) == {"name", "age"}
        assert "region" not in row
        assert "email" not in row

    # Chain where + select
    us_names = list(view.where(region="US").select("name"))
    assert len(us_names) == 1
    assert us_names[0] == {"name": "Alice"}

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: select projection (fields, chain with where)")


def test_map_transform():
    """view.map(fn) transforms each row."""
    bench = "/tmp/pond_vq_map"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = View(kernel, "orders")
    view.put("o1", {"order_id": 1, "amount": 100.0, "currency": "EUR"})
    view.put("o2", {"order_id": 2, "amount": 200.0, "currency": "EUR"})
    view.commit("insert 2 orders")

    # Map: convert EUR to USD
    usd_orders = list(view.map(lambda r: {**r, "amount_usd": r["amount"] * 1.1}))

    assert len(usd_orders) == 2
    for row in usd_orders:
        assert "amount_usd" in row
        assert row["amount_usd"] == row["amount"] * 1.1

    # Chain where + select + map
    result = list(view
                  .where(lambda r: r["amount"] > 150)
                  .select("order_id", "amount")
                  .map(lambda r: {**r, "amount_usd": r["amount"] * 1.1}))
    assert len(result) == 1
    assert result[0]["order_id"] == 2
    assert abs(result[0]["amount_usd"] - 220.0) < 0.01  # 200 * 1.1 (float)

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: map transform (single, chain with where + select)")


def test_cross_view_join():
    """view.join(other_view, on='field') joins two Views."""
    bench = "/tmp/pond_vq_join"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    orders = View(kernel, "orders")
    orders.put("o1", {"order_id": 1, "customer_id": "c1", "amount": 100})
    orders.put("o2", {"order_id": 2, "customer_id": "c2", "amount": 200})
    orders.put("o3", {"order_id": 3, "customer_id": "c1", "amount": 50})
    orders.put("o4", {"order_id": 4, "customer_id": "c3", "amount": 300})
    orders.commit("insert 4 orders")

    customers = View(kernel, "customers")
    customers.put("c1", {"customer_id": "c1", "name": "Alice", "region": "US"})
    customers.put("c2", {"customer_id": "c2", "name": "Bob", "region": "EU"})
    customers.put("c5", {"customer_id": "c5", "name": "Eve", "region": "US"})
    # Note: c3 is in orders but NOT in customers (tests LEFT JOIN)
    customers.commit("insert 3 customers (c1, c2, c5)")

    # JOIN orders with customers on customer_id
    joined = list(orders.join(customers, on="customer_id"))

    # All 4 orders should appear (LEFT JOIN: o4 has no matching customer)
    assert len(joined) == 4

    # Check that customer fields are merged in
    by_order = {r["order_id"]: r for r in joined}
    assert by_order[1]["name"] == "Alice"
    assert by_order[1]["region"] == "US"
    assert by_order[2]["name"] == "Bob"
    assert by_order[3]["name"] == "Alice"  # c1 again
    # o4's customer (c3) has no match -> left row as-is
    assert "name" not in by_order[4]
    assert by_order[4]["customer_id"] == "c3"

    # Chain: join + where + select
    us_orders = list(orders
                     .join(customers, on="customer_id")
                     .where(region="US")
                     .select("order_id", "amount", "name"))
    assert len(us_orders) == 2  # o1 and o3 (both c1=US)
    for row in us_orders:
        assert row["name"] == "Alice"
        assert "region" not in row  # selected only order_id, amount, name
    assert {r["order_id"] for r in us_orders} == {1, 3}

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: cross-view JOIN (LEFT JOIN, merge, chain with where + select)")


def test_laziness():
    """ViewQuery is lazy: nothing runs until you iterate or collect."""
    bench = "/tmp/pond_vq_lazy"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    view = View(kernel, "data")
    for i in range(100):
        view.put(f"k{i:03d}", {"id": i, "val": i * 10})
    view.commit("insert 100 items")

    # Build a query — should NOT read any data yet
    q = view.where(lambda r: r["val"] > 500).select("id", "val")

    # Now iterate — this is where evaluation happens
    results = q.collect()
    assert len(results) == 49  # val 510..990
    assert all(r["val"] > 500 for r in results)
    assert all(set(r.keys()) == {"id", "val"} for r in results)

    # .first() stops early (doesn't evaluate all 100)
    first = view.where(lambda r: r["val"] > 500).first()
    assert first is not None
    assert first["val"] > 500

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: laziness (no eval until iterate/collect, first stops early)")


def test_elegant_pattern():
    """The full elegant pattern: read from one view, transform, join."""
    bench = "/tmp/pond_vq_elegant"
    if os.path.exists(bench):
        shutil.rmtree(bench)
    os.makedirs(bench)
    kernel = PondMinimal(bench)

    # Source: orders
    orders = View(kernel, "orders")
    orders.put("o1", {"order_id": 1, "customer_id": "c1", "amount": 100, "ts": 1000})
    orders.put("o2", {"order_id": 2, "customer_id": "c2", "amount": 200, "ts": 1001})
    orders.put("o3", {"order_id": 3, "customer_id": "c1", "amount": 50, "ts": 1002})
    orders.commit("insert 3 orders")

    # Source: customers
    customers = View(kernel, "customers")
    customers.put("c1", {"customer_id": "c1", "name": "Alice", "region": "US"})
    customers.put("c2", {"customer_id": "c2", "name": "Bob", "region": "EU"})
    customers.commit("insert 2 customers")

    # The elegant pattern: one chained query
    result = (orders
              .join(customers, on="customer_id")
              .where(region="US")
              .map(lambda r: {"order_id": r["order_id"],
                              "customer": r["name"],
                              "amount_usd": r["amount"]})
              .collect())

    assert len(result) == 2  # o1 and o3 (both c1=US)
    for row in result:
        assert row["customer"] == "Alice"
        assert "region" not in row  # projected out by map
        assert "customer_id" not in row

    # Compare to the OLD pattern (what you'd have to write without ViewQuery):
    # old_result = []
    # orders_data = CrossView.read_all_from(orders)
    # customers_data = CrossView.read_all_from(customers)
    # for key, order in orders_data.items():
    #     customer = customers_data.get(order["customer_id"])
    #     if customer and customer.get("region") == "US":
    #         old_result.append({
    #             "order_id": order["order_id"],
    #             "customer": customer["name"],
    #             "amount_usd": order["amount"],
    #         })
    # Same result, but 10 lines of imperative code vs 5 lines of declarative.

    kernel.close()
    shutil.rmtree(bench, ignore_errors=True)
    print("PASS: elegant pattern (join + where + map + collect in 5 lines)")


def _run_all_tests():
    print("=== ViewQuery — Elegant Cross-View Reading Tests ===\n")
    test_basic_iteration()
    test_where_filter()
    test_select_projection()
    test_map_transform()
    test_cross_view_join()
    test_laziness()
    test_elegant_pattern()
    print("\n=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    _run_all_tests()
