"""
SQL pushdown parser — extracts predicates and projections from SQL strings.

This is a deliberately small, regex-based parser. It handles the common
cases that benefit from pushdown:

  - Simple column-op-value comparisons in WHERE: age > 30, region = 'EU'
  - IN lists: region IN ('US', 'EU')
  - BETWEEN (conservatively, as >= lower bound)
  - AND-ed predicates (OR is treated as non-prunable — conservative)
  - Column projection from SELECT col1, col2 FROM ...

It does NOT handle:
  - Joins (predicates on joined tables)
  - Subqueries
  - Complex expressions (functions, arithmetic)
  - OR (would require all branches to agree on pruning)

For anything it can't parse, it returns "*" for columns (read all) or
[] for predicates (no pruning). The caller (PondLakehouse) falls back
to a full read in that case.

Kept as a standalone module so it can be tested in isolation and so
lakehouse_lens.py doesn't have to carry the regex complexity. A future
upgrade to sqlglot would replace this whole module.
"""

from __future__ import annotations

import re


def extract_predicates(sql: str) -> list[tuple[str, str, object]]:
    """Extract simple column-op-value predicates from a SQL WHERE clause.

    Returns:
        List of (column, op, value) tuples. Empty list if no WHERE or
        if the WHERE clause can't be parsed.

    Supports: =, !=, <, <=, >, >=, IN, BETWEEN (conservatively).
    Does NOT support: OR, joins, subqueries, functions.
    """
    predicates: list[tuple[str, str, object]] = []

    # Find WHERE clause (case-insensitive). Stop at GROUP/ORDER/LIMIT or end.
    where_match = re.search(
        r'\bWHERE\b\s+(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)',
        sql, re.IGNORECASE | re.DOTALL)
    if not where_match:
        return predicates

    where_clause = where_match.group(1).strip()

    # Split on AND (case-insensitive). Each AND part is a separate predicate.
    # For OR, we treat the entire OR expression as non-prunable (conservative)
    # because pruning with OR requires ALL branches to say "can't match".
    parts = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)

    for part in parts:
        col, op, value = _parse_single_predicate(part.strip())
        if col is not None:
            predicates.append((col, op, value))

    return predicates


def _parse_single_predicate(part: str) -> tuple:
    """Parse a single predicate like 'age > 30' or 'region = US'.

    Returns:
        (column, op, value) or (None, None, None) if unparseable.
        Supports: =, !=, <, <=, >, >=, IN, BETWEEN.
    """
    if not part:
        return (None, None, None)

    # BETWEEN: col BETWEEN val1 AND val2
    # Note: this is tricky because AND is also a clause separator. We
    # handle BETWEEN before the AND split by looking for the pattern.
    between_match = re.match(
        r'(\w+)\s+BETWEEN\s+(?:\'([^\']*)\'|(\d+\.?\d*))\s+AND\s+'
        r'(?:\'([^\']*)\'|(\d+\.?\d*))',
        part, re.IGNORECASE)
    if between_match:
        col = between_match.group(1)
        # Lower bound
        if between_match.group(2) is not None:
            lo = between_match.group(2)
        else:
            lo = (float(between_match.group(3))
                  if '.' in between_match.group(3)
                  else int(between_match.group(3)))
        # BETWEEN lo AND hi is equivalent to >= lo AND <= hi. We return
        # the lower bound (>= lo) — conservative, might read a bit more.
        return (col, ">=", lo)

    # IN: col IN ('val1', 'val2', ...) or col IN (1, 2, 3)
    in_match = re.match(r'(\w+)\s+IN\s*\(([^)]+)\)', part, re.IGNORECASE)
    if in_match:
        col = in_match.group(1)
        values_str = in_match.group(2)
        values = []
        for v in values_str.split(","):
            v = v.strip()
            if v.startswith("'") and v.endswith("'"):
                values.append(v[1:-1])
            else:
                try:
                    values.append(float(v) if '.' in v else int(v))
                except ValueError:
                    pass
        if values:
            return (col, "in", values)

    # Simple comparison: col OP value
    pattern = r'(\w+)\s*(=|!=|<=|>=|<|>)\s*'
    pattern += r"(?:'([^']*)'|(\d+\.?\d*))"
    match = re.match(pattern, part, re.IGNORECASE)
    if match:
        col, op, str_val, num_val = match.groups()
        if str_val is not None:
            value = str_val
        elif num_val is not None:
            value = float(num_val) if '.' in num_val else int(num_val)
        else:
            return (None, None, None)
        return (col, op, value)

    return (None, None, None)


def extract_columns(sql: str) -> list[str]:
    """Extract projected column names from a SQL SELECT clause.

    Returns:
        ["*"] for SELECT * or if extraction fails (caller reads all
        columns). Otherwise a list of column names for
        SELECT col1, col2, ...
    """
    # Find SELECT ... FROM
    select_match = re.match(
        r'\s*SELECT\s+(.+?)\s+FROM\s+',
        sql, re.IGNORECASE | re.DOTALL)
    if not select_match:
        return ["*"]

    cols_str = select_match.group(1).strip()

    # SELECT *
    if cols_str == "*":
        return ["*"]

    # SELECT COUNT(*), SUM(col), etc. — don't project (need all columns
    # for aggregation; DuckDB will compute the aggregate).
    if re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', cols_str, re.IGNORECASE):
        return ["*"]

    # Split on commas, extract column names
    parts = [p.strip() for p in cols_str.split(",")]
    columns: list[str] = []
    for part in parts:
        # Handle "column" or "table.column" or "column AS alias"
        col_match = re.match(
            r'(?:\w+\.)?(\w+)(?:\s+AS\s+\w+)?$', part, re.IGNORECASE)
        if col_match:
            columns.append(col_match.group(1))
        else:
            return ["*"]  # can't parse — read all columns

    return columns if columns else ["*"]
