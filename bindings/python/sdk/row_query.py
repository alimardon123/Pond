"""
LensQuery — a lazy, composable ROW-LEVEL query API for ANY iterable Pond Lens.

NOTE: This is NOT "the query method for data in Pond." It is a lazy,
composable query BUILDER for iterating, filtering, projecting, and
joining ROWS from any source that exposes keys()/get() (or any iterable
of dicts). For SQL queries over tabular data, use LakehouseLens.query().
For point lookups, use KeyValueLens.get() or LakehouseLens.range_point_lookup().

Makes a Lens feel like a collection: iterable, filterable, joinable.
This is the "direct, easy, simple and elegant way of reading data"
that the architecture review asked for.

GENERIC: works on any object that exposes `keys() -> iterable[str]`
and `get(key) -> dict | None`. This includes KeyValueLens (and its
subclasses like KeylessLens, SemanticLens, CollectionIndexer). It also
works on any plain iterable of dicts (lists, generators, etc.) —
useful for testing and for lenses that expose row iteration via a
different API.

Design:
  - LAZY: nothing is read from the source until you iterate or .collect().
    This allows a future execution engine (SQL, Polars, DataFusion) to
    push down filters and projections to the kernel level.
  - COMPOSABLE: .where().select().map().join() chain naturally.
  - DICT-LIKE: source.rows() yields decoded rows; len(source) returns count.

Usage:
    # Iterate rows directly (source is any lens with keys()/get())
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

    # Cross-lens JOIN (both sides must be iterable)
    for row in orders.join(customers, on="customer_id"):
        print(row["order_id"], row["customer_name"])

    # Collect to list
    results = orders.where(region="US").collect()

This module does NOT add kernel features. It is a pure SDK-layer
convenience built on the source's keys()/get() API (or plain iteration).
"""

from __future__ import annotations

from typing import Any, Optional, Callable, Union, Iterable


# ---------------------------------------------------------------------------
# LensQuery — lazy, composable query over a Lens
# ---------------------------------------------------------------------------

class LensQuery:
    """A lazy query over a Lens (or another LensQuery).

    Queries are constructed by chaining .where(), .select(), .map(),
    .join(). Nothing is read from the Lens until you iterate the query
    or call .collect().

    The laziness is deliberate: it allows a future execution engine to
    push down filters and projections to the kernel level, avoiding
    unnecessary data transfer. Today the evaluation is Python-level
    (iterate keys, decode, filter), but the API is designed so a
    future optimizer could rewrite the plan.
    """

    def __init__(self, source: Union['LensQuery', Any],
                 predicates: Optional[list] = None,
                 projection: Optional[list[str]] = None,
                 mapper: Optional[Callable] = None):
        # source: a Lens (has .keys() and .get()), a LensQuery, or any
        # iterable of dicts.
        self._source = source
        self._predicates = predicates or []
        self._projection = projection
        self._mapper = mapper

    # ------------------------------------------------------------------
    # Internal: yield raw rows from the source
    # ------------------------------------------------------------------

    def _source_rows(self):
        """Yield raw (unfiltered, unprojected) rows from the source.

        GENERIC: works on any source that either:
          - exposes .keys() and .get() (KeyValueLens and subclasses), OR
          - is iterable (LensQuery, JoinedQuery, list, generator, etc.)
        """
        source = self._source
        # Duck-typing: any lens with .keys() and .get()
        if hasattr(source, 'keys') and hasattr(source, 'get'):
            for key in source.keys():
                row = source.get(key)
                if row is not None:
                    yield row
        elif isinstance(source, (LensQuery, JoinedQuery)):
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
    # Combinators — each returns a NEW LensQuery (lazy, no evaluation)
    # ------------------------------------------------------------------

    def where(self, predicate: Optional[Callable] = None,
              **kwargs) -> 'LensQuery':
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
            return LensQuery(self._source, self._predicates,
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
        return LensQuery(self._source, self._predicates + [pred],
                         self._projection, self._mapper)

    def select(self, *fields: str) -> 'LensQuery':
        """Project each row to only these fields."""
        return LensQuery(self._source, self._predicates,
                         list(fields), self._mapper)

    def map(self, fn: Callable) -> 'LensQuery':
        """Transform each row via fn(row) -> new_row."""
        if self._mapper is not None:
            old = self._mapper
            new_mapper = lambda r: fn(old(r))
        else:
            new_mapper = fn
        return LensQuery(self._source, self._predicates,
                         self._projection, new_mapper)

    def join(self, other, on: str) -> 'JoinedQuery':
        """LEFT JOIN with another iterable source.

        For each row in this query, finds the matching row in `other`
        where other[on] == this[on]. Merges the two rows (right side
        wins on field conflicts). Rows with no match are yielded as-is
        (LEFT JOIN semantics).

        The right side is materialized into memory (eager). The left
        side is streamed (lazy).

        Args:
            other: any source with keys()/get() (KeyValueLens and
                subclasses), or any iterable of dicts (LensQuery,
                JoinedQuery, list, generator).
            on: the field name to join on.

        Returns:
            A JoinedQuery that yields merged rows when iterated.
        """
        # Build lookup from right side (eager)
        lookup: dict[Any, dict] = {}
        if hasattr(other, 'keys') and hasattr(other, 'get'):
            # It's a lens with keys()/get() — iterate its rows
            for key in other.keys():
                row = other.get(key)
                if row is not None:
                    join_val = row.get(on)
                    if join_val is not None:
                        lookup[join_val] = row
        elif hasattr(other, '__iter__'):
            # It's a LensQuery, JoinedQuery, or iterable of dicts
            for row in other:
                join_val = row.get(on) if isinstance(row, dict) else None
                if join_val is not None:
                    lookup[join_val] = row
        else:
            raise TypeError(f"join() expects an iterable source, got {type(other)}")
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

    Supports further chaining (.where, .select, .map) via LensQuery
    adaptation. The join is a LEFT JOIN: left rows with no match are
    yielded as-is (no right fields added).
    """

    def __init__(self, left: LensQuery, right_lookup: dict, on: str):
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
    def where(self, predicate=None, **kwargs) -> LensQuery:
        return LensQuery(self).where(predicate, **kwargs)

    def select(self, *fields) -> LensQuery:
        return LensQuery(self).select(*fields)

    def map(self, fn: Callable) -> LensQuery:
        return LensQuery(self).map(fn)

    def join(self, other, on: str) -> 'JoinedQuery':
        return LensQuery(self).join(other, on)

# Backward-compatible aliases
ViewQuery = LensQuery  # backward-compatible alias
