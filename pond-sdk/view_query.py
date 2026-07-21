"""
ViewQuery — a lazy, composable query API for Pond Views.

Makes a View feel like a collection: iterable, filterable, joinable.
This is the "direct, easy, simple and elegant way of reading data"
that the architecture review asked for.

Design:
  - LAZY: nothing is read from the View until you iterate or .collect().
    This allows a future execution engine (SQL, Polars, DataFusion) to
    push down filters and projections to the kernel level.
  - COMPOSABLE: .where().select().map().join() chain naturally.
  - DICT-LIKE: View.__iter__ yields decoded rows; View.__len__ returns count().

Usage:
    # Iterate rows directly
    for row in orders:
        print(row)

    # Filter with kwargs
    for row in orders.where(region="US"):
        ...

    # Filter with a predicate
    for row in orders.where(lambda r: r["amount"] > 100):
        ...

    # Project
    for row in orders.select("order_id", "amount"):
        ...

    # Chain
    us_orders = (orders
                 .where(region="US")
                 .select("order_id", "amount")
                 .map(lambda r: {**r, "amount_usd": r["amount"]}))

    # Cross-view JOIN
    for row in orders.join(customers, on="customer_id"):
        print(row["order_id"], row["customer_name"])

    # Collect to list
    results = orders.where(region="US").collect()

This module does NOT add kernel features. It is a pure SDK-layer
convenience built on the existing View.get/View.keys API.
"""

from __future__ import annotations

from typing import Any, Optional, Callable, Union, Iterable


# ---------------------------------------------------------------------------
# ViewQuery — lazy, composable query over a View
# ---------------------------------------------------------------------------

class ViewQuery:
    """A lazy query over a View (or another ViewQuery).

    Queries are constructed by chaining .where(), .select(), .map(),
    .join(). Nothing is read from the View until you iterate the query
    or call .collect().

    The laziness is deliberate: it allows a future execution engine to
    push down filters and projections to the kernel level, avoiding
    unnecessary data transfer. Today the evaluation is Python-level
    (iterate keys, decode, filter), but the API is designed so a
    future optimizer could rewrite the plan.
    """

    def __init__(self, source: Union['ViewQuery', Any],
                 predicates: Optional[list] = None,
                 projection: Optional[list[str]] = None,
                 mapper: Optional[Callable] = None):
        # source: a View (has .keys() and .get()), a ViewQuery, or any
        # iterable of dicts.
        self._source = source
        self._predicates = predicates or []
        self._projection = projection
        self._mapper = mapper

    # ------------------------------------------------------------------
    # Internal: yield raw rows from the source
    # ------------------------------------------------------------------

    def _source_rows(self):
        """Yield raw (unfiltered, unprojected) rows from the source."""
        source = self._source
        # Duck-typing: a View has .keys() and .get()
        if hasattr(source, 'keys') and hasattr(source, 'get'):
            for key in source.keys():
                row = source.get(key)
                if row is not None:
                    yield row
        elif isinstance(source, ViewQuery):
            yield from source
        elif isinstance(source, JoinedQuery):
            yield from source
        else:
            # Assume it's an iterable of dicts
            yield from source

    # ------------------------------------------------------------------
    # Iteration — this is where the lazy evaluation actually happens
    # ------------------------------------------------------------------

    def __iter__(self):
        for row in self._source_rows():
            # Apply predicates (ANDed)
            if self._predicates:
                if not all(pred(row) for pred in self._predicates):
                    continue
            # Apply projection
            if self._projection is not None:
                row = {k: row.get(k) for k in self._projection}
            # Apply mapper
            if self._mapper is not None:
                row = self._mapper(row)
            yield row

    # ------------------------------------------------------------------
    # Combinators — each returns a NEW ViewQuery (lazy, no evaluation)
    # ------------------------------------------------------------------

    def where(self, predicate: Optional[Callable] = None,
              **kwargs) -> 'ViewQuery':
        """Filter rows.

        Pass either:
          - a callable: where(lambda r: r["amount"] > 100)
          - kwargs: where(region="US", plan="pro")
          - nothing: where() — returns an unfiltered query (useful as a
            chain starter for .take(), .count(), .first(), etc.)

        Multiple .where() calls are ANDed together.
        """
        if predicate is None and not kwargs:
            # No filter — return an unfiltered query (chain starter)
            return ViewQuery(self._source, self._predicates,
                             self._projection, self._mapper)
        if predicate is not None and kwargs:
            raise TypeError("Pass either a callable or kwargs, not both")
        if kwargs:
            pred = lambda r: all(r.get(k) == v for k, v in kwargs.items())
        elif callable(predicate):
            pred = predicate
        elif isinstance(predicate, dict):
            pred = lambda r: all(r.get(k) == v for k, v in predicate.items())
        else:
            raise TypeError(f"where() expects a callable or kwargs, got {type(predicate)}")
        return ViewQuery(self._source, self._predicates + [pred],
                         self._projection, self._mapper)

    def select(self, *fields: str) -> 'ViewQuery':
        """Project each row to only these fields."""
        return ViewQuery(self._source, self._predicates,
                         list(fields), self._mapper)

    def map(self, fn: Callable) -> 'ViewQuery':
        """Transform each row via fn(row) -> new_row."""
        if self._mapper is not None:
            old = self._mapper
            new_mapper = lambda r: fn(old(r))
        else:
            new_mapper = fn
        return ViewQuery(self._source, self._predicates,
                         self._projection, new_mapper)

    def join(self, other, on: str) -> 'JoinedQuery':
        """LEFT JOIN with another View or ViewQuery.

        For each row in this query, finds the matching row in `other`
        where other[on] == this[on]. Merges the two rows (right side
        wins on field conflicts). Rows with no match are yielded as-is
        (LEFT JOIN semantics).

        The right side is materialized into memory (eager). The left
        side is streamed (lazy).

        Args:
            other: a View, ViewQuery, or any iterable of dicts.
            on: the field name to join on.

        Returns:
            A JoinedQuery that yields merged rows when iterated.
        """
        # Build lookup from right side (eager)
        lookup: dict[Any, dict] = {}
        if hasattr(other, 'keys') and hasattr(other, 'get'):
            # It's a View — iterate its rows
            for key in other.keys():
                row = other.get(key)
                if row is not None:
                    join_val = row.get(on)
                    if join_val is not None:
                        lookup[join_val] = row
        elif hasattr(other, '__iter__'):
            # It's a ViewQuery, JoinedQuery, or iterable
            for row in other:
                join_val = row.get(on) if isinstance(row, dict) else None
                if join_val is not None:
                    lookup[join_val] = row
        else:
            raise TypeError(f"join() expects a View or iterable, got {type(other)}")
        return JoinedQuery(self, lookup, on)

    # ------------------------------------------------------------------
    # Terminal operations — force evaluation
    # ------------------------------------------------------------------

    def collect(self) -> list:
        """Eagerly evaluate and return a list of rows."""
        return list(self)

    def count(self) -> int:
        """Count matching rows (forces evaluation)."""
        return sum(1 for _ in self)

    def first(self) -> Optional[dict]:
        """Return the first matching row, or None."""
        for row in self:
            return row
        return None

    def take(self, n: int) -> list:
        """Return the first n matching rows."""
        result = []
        for row in self:
            result.append(row)
            if len(result) >= n:
                break
        return result


# ---------------------------------------------------------------------------
# JoinedQuery — result of a JOIN
# ---------------------------------------------------------------------------

class JoinedQuery:
    """Result of a JOIN. Iterates the left query, merging matching right rows.

    Supports further chaining (.where, .select, .map) via ViewQuery
    adaptation. The join is a LEFT JOIN: left rows with no match are
    yielded as-is (no right fields added).
    """

    def __init__(self, left: ViewQuery, right_lookup: dict, on: str):
        self._left = left
        self._right_lookup = right_lookup
        self._on = on

    def __iter__(self):
        for left_row in self._left:
            join_val = left_row.get(self._on) if isinstance(left_row, dict) else None
            if join_val is not None and join_val in self._right_lookup:
                right_row = self._right_lookup[join_val]
                # Merge: right wins on conflicts
                yield {**left_row, **right_row}
            else:
                yield left_row

    def collect(self) -> list:
        return list(self)

    def count(self) -> int:
        return sum(1 for _ in self)

    def first(self) -> Optional[dict]:
        for row in self:
            return row
        return None

    # Allow further chaining on the joined result
    def where(self, predicate=None, **kwargs) -> ViewQuery:
        return ViewQuery(self).where(predicate, **kwargs)

    def select(self, *fields) -> ViewQuery:
        return ViewQuery(self).select(*fields)

    def map(self, fn: Callable) -> ViewQuery:
        return ViewQuery(self).map(fn)

    def join(self, other, on: str) -> 'JoinedQuery':
        return ViewQuery(self).join(other, on)
