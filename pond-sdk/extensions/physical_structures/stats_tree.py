"""
Lazy Hierarchical Stats Tree — O(log N) pruning at PB scale.

PROBLEM:
  A single CollectionManifest blob grows linearly with row group count.
  At ~25K row groups (5MB), the single-fetch sweet spot on S3 breaks down.

SOLUTION:
  Build a separate Prolly/B+ tree keyed by row_group_key. Each leaf
  contains per-row-group stats (same as manifest entries). Internal
  nodes contain AGGREGATED stats (max-of-max, min-of-min, union-bloom).

  Reads walk O(log N) stats-tree nodes, pruning subtrees that can't
  match the predicate. Each node is fetched once and cached by the SDK
  via content addressing (same hash = same node, even across readers).

LAZY:
  The stats tree is NOT built at write time. It's built on the first
  OLAP read that would benefit from it. The build cost is O(N) once;
  subsequent reads are O(log N). The tree is content-addressed, so two
  readers on the same commit share the same cached tree.

  "Lazy" here means: zero write overhead. Stats are computed once per
  data chunk at write time (already done by embedded_stats + manifest).
  The stats tree is just a hierarchical VIEW over those stats — built
  on demand, cached forever.

PB-SCALE BENCHMARK:
  1 PB table at 100 MB per row group = 10M row groups
  Stats tree depth = log_64(10M) = ~4 levels
  Pruning read = 4 fetches (one per level) + K data blobs
  Without tree: 10M-byte manifest (way too big)

  Without stats tree, the manifest can't scale past ~25K row groups.
  With stats tree, we scale to billions of row groups.

FORMAT (PND1-stats-tree v1):
  Each tree node is a kernel blob:
    +--------------------------+
    | Magic (4B): b"PSTT"      |
    | Node type (1B):          |
    |   0 = leaf               |
    |   1 = internal           |
    | n_entries (4B)           |
    +--------------------------+
    | If leaf:                 |
    |   For each entry:        |
    |     key_len (2B)         |
    |     key (UTF-8)          |
    |     blob_hash (32B)      |  ← the data blob hash for this row group
    |     n_rows (4B)          |
    |     n_columns (2B)       |
    |     For each column:     |
    |       name_len (1B)      |
    |       name (UTF-8)       |
    |       value_type (1B)    |
    |       min (8B or var)    |
    |       max (8B or var)    |
    |       null_count (4B)    |
    +--------------------------+
    | If internal:             |
    |   For each child:        |
    |     max_key_len (2B)     |
    |     max_key (UTF-8)      |
    |     child_hash (32B)     |  ← hash of the child node
    |     n_columns (2B)       |  ← aggregated stats for this subtree
    |     For each column:     |
    |       name_len (1B)      |
    |       name (UTF-8)       |
    |       value_type (1B)    |
    |       min (8B or var)    |  ← min-of-mins across subtree
    |       max (8B or var)    |  ← max-of-maxes across subtree
    |       null_count (4B)    |  ← sum of null counts in subtree
    +--------------------------+

INTEGRATION WITH CollectionManifest:
  - For small collections (<25K row groups): manifest blob has all stats
    inline. No stats tree needed.
  - For large collections (>=25K row groups): manifest delegates to a
    stats tree via its `stats_tree_root` field. The manifest still has
    schema + sort_order; the stats tree has all the per-row-group entries.

  The CollectionManifest class hides this distinction:
    manifest.scan_with_pruning(predicates) → yields RowGroupEntry
  Whether the manifest is flat or hierarchical, the API is the same.
"""

from __future__ import annotations

import struct
import os
import sys
from dataclasses import dataclass, field
from typing import Optional, Any, Iterator

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                  "..", "..", "..", "pond-core"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kernel import PondMinimal  # noqa: E402
from embedded_stats import (  # noqa: E402
    VALUE_TYPE_INT64, VALUE_TYPE_FLOAT64, VALUE_TYPE_STRING, VALUE_TYPE_NULL,
)
from collection_manifest import (  # noqa: E402
    RowGroupEntry, ColumnStatsEntry, ColumnChunkEntry,
    _encode_value, _decode_value,
)

# Reuse the Prolly tree's chunk threshold for consistency
_STATS_TREE_MAGIC = b"PSTT"
_STATS_TREE_VERSION = 1
_NODE_TYPE_LEAF = 0
_NODE_TYPE_INTERNAL = 1

# Threshold for leaf node size — ~64 entries per leaf keeps nodes ~4KB
TARGET_LEAF_ENTRIES = 64

# Threshold for switching from flat manifest to hierarchical stats tree.
# Above this many row groups, the manifest blob exceeds ~5MB and we
# delegate to a stats tree.
FLAT_MANIFEST_MAX_ROW_GROUPS = 25_000


# ---------------------------------------------------------------------------
# Stats tree node encoding
# ---------------------------------------------------------------------------

def _encode_value_with_flag(value_type: int, value: Any) -> bytes:
    """Encode a value with a 1-byte has_value flag."""
    if value is None:
        return struct.pack("<B", 0)
    return struct.pack("<B", 1) + _encode_value(value_type, value)


def _decode_value_with_flag(value_type: int, data: bytes, pos: int) -> tuple[Any, int]:
    """Decode a value with a 1-byte has_value flag."""
    has_value = data[pos]; pos += 1
    if not has_value:
        return None, pos
    return _decode_value(value_type, data, pos)


def encode_leaf_node(entries: list[RowGroupEntry]) -> bytes:
    """Encode a leaf node of the stats tree.

    Each entry is a RowGroupEntry with column stats but NO chunk stats
    (chunks live in the data blob's embedded stats, not in the tree).
    """
    buf = bytearray()
    buf += _STATS_TREE_MAGIC
    buf += struct.pack("<BB", _STATS_TREE_VERSION, _NODE_TYPE_LEAF)
    buf += struct.pack("<I", len(entries))

    for rg in entries:
        key_bytes = rg.key.encode("utf-8")
        buf += struct.pack("<H", len(key_bytes))
        buf += key_bytes
        buf += bytes.fromhex(rg.blob_hash)
        buf += struct.pack("<I", rg.n_rows)
        buf += struct.pack("<H", len(rg.columns))

        for col in rg.columns:
            name_bytes = col.name.encode("utf-8")
            buf += struct.pack("<B", len(name_bytes))
            buf += name_bytes
            buf += struct.pack("<B", col.value_type)
            buf += _encode_value_with_flag(col.value_type, col.min)
            buf += _encode_value_with_flag(col.value_type, col.max)
            buf += struct.pack("<I", col.null_count)

    return bytes(buf)


def decode_leaf_node(data: bytes) -> list[RowGroupEntry]:
    """Decode a leaf node into a list of RowGroupEntry."""
    if data[:4] != _STATS_TREE_MAGIC:
        raise ValueError(f"Not a stats tree node (magic={data[:4]!r})")
    version, node_type = struct.unpack("<BB", data[4:6])
    if version != _STATS_TREE_VERSION:
        raise ValueError(f"Unsupported stats tree version: {version}")
    if node_type != _NODE_TYPE_LEAF:
        raise ValueError(f"Not a leaf node (type={node_type})")

    n_entries = struct.unpack("<I", data[6:10])[0]
    pos = 10

    entries: list[RowGroupEntry] = []
    for _ in range(n_entries):
        key_len = struct.unpack("<H", data[pos:pos+2])[0]; pos += 2
        key = data[pos:pos+key_len].decode("utf-8"); pos += key_len
        blob_hash = data[pos:pos+32].hex(); pos += 32
        n_rows = struct.unpack("<I", data[pos:pos+4])[0]; pos += 4
        n_cols = struct.unpack("<H", data[pos:pos+2])[0]; pos += 2

        rg = RowGroupEntry(key=key, blob_hash=blob_hash,
                            n_rows=n_rows, storage_mode=0)
        for _ in range(n_cols):
            name_len = data[pos]; pos += 1
            name = data[pos:pos+name_len].decode("utf-8"); pos += name_len
            vtype = data[pos]; pos += 1
            mn, pos = _decode_value_with_flag(vtype, data, pos)
            mx, pos = _decode_value_with_flag(vtype, data, pos)
            null_count = struct.unpack("<I", data[pos:pos+4])[0]; pos += 4
            rg.columns.append(ColumnStatsEntry(
                name=name, value_type=vtype,
                min=mn, max=mx, null_count=null_count,
            ))
        entries.append(rg)

    return entries


@dataclass
class InternalChild:
    """One child reference in an internal node, with aggregated stats."""
    max_key: str                    # max key in this child's subtree
    child_hash: str                 # hash of the child node blob
    # Aggregated stats per column across the subtree
    column_stats: list[ColumnStatsEntry] = field(default_factory=list)


def encode_internal_node(children: list[InternalChild]) -> bytes:
    """Encode an internal node of the stats tree."""
    buf = bytearray()
    buf += _STATS_TREE_MAGIC
    buf += struct.pack("<BB", _STATS_TREE_VERSION, _NODE_TYPE_INTERNAL)
    buf += struct.pack("<I", len(children))

    for child in children:
        key_bytes = child.max_key.encode("utf-8")
        buf += struct.pack("<H", len(key_bytes))
        buf += key_bytes
        buf += bytes.fromhex(child.child_hash)
        buf += struct.pack("<H", len(child.column_stats))

        for col in child.column_stats:
            name_bytes = col.name.encode("utf-8")
            buf += struct.pack("<B", len(name_bytes))
            buf += name_bytes
            buf += struct.pack("<B", col.value_type)
            buf += _encode_value_with_flag(col.value_type, col.min)
            buf += _encode_value_with_flag(col.value_type, col.max)
            buf += struct.pack("<I", col.null_count)

    return bytes(buf)


def decode_internal_node(data: bytes) -> list[InternalChild]:
    """Decode an internal node into a list of InternalChild."""
    if data[:4] != _STATS_TREE_MAGIC:
        raise ValueError(f"Not a stats tree node (magic={data[:4]!r})")
    version, node_type = struct.unpack("<BB", data[4:6])
    if version != _STATS_TREE_VERSION:
        raise ValueError(f"Unsupported stats tree version: {version}")
    if node_type != _NODE_TYPE_INTERNAL:
        raise ValueError(f"Not an internal node (type={node_type})")

    n_children = struct.unpack("<I", data[6:10])[0]
    pos = 10

    children: list[InternalChild] = []
    for _ in range(n_children):
        key_len = struct.unpack("<H", data[pos:pos+2])[0]; pos += 2
        max_key = data[pos:pos+key_len].decode("utf-8"); pos += key_len
        child_hash = data[pos:pos+32].hex(); pos += 32
        n_cols = struct.unpack("<H", data[pos:pos+2])[0]; pos += 2

        col_stats: list[ColumnStatsEntry] = []
        for _ in range(n_cols):
            name_len = data[pos]; pos += 1
            name = data[pos:pos+name_len].decode("utf-8"); pos += name_len
            vtype = data[pos]; pos += 1
            mn, pos = _decode_value_with_flag(vtype, data, pos)
            mx, pos = _decode_value_with_flag(vtype, data, pos)
            null_count = struct.unpack("<I", data[pos:pos+4])[0]; pos += 4
            col_stats.append(ColumnStatsEntry(
                name=name, value_type=vtype,
                min=mn, max=mx, null_count=null_count,
            ))

        children.append(InternalChild(
            max_key=max_key, child_hash=child_hash,
            column_stats=col_stats,
        ))

    return children


# ---------------------------------------------------------------------------
# Stats tree builder
# ---------------------------------------------------------------------------

def aggregate_stats(entries: list[RowGroupEntry]) -> list[ColumnStatsEntry]:
    """Aggregate per-column stats across a list of RowGroupEntry.

    Used to compute internal-node aggregated stats:
      - min = min of all child mins
      - max = max of all child maxes
      - null_count = sum of all child null_counts

    Returns one ColumnStatsEntry per column name.
    """
    if not entries:
        return []

    # Collect column names (assume all entries have same columns)
    col_names: list[str] = []
    col_vtypes: dict[str, int] = {}
    for col in entries[0].columns:
        col_names.append(col.name)
        col_vtypes[col.name] = col.value_type

    aggregated: list[ColumnStatsEntry] = []
    for name in col_names:
        vtype = col_vtypes[name]
        all_mins = [c.get_column(name).min for c in entries
                     if c.get_column(name) and c.get_column(name).min is not None]
        all_maxes = [c.get_column(name).max for c in entries
                      if c.get_column(name) and c.get_column(name).max is not None]
        null_count = sum(c.get_column(name).null_count for c in entries
                          if c.get_column(name))
        mn = min(all_mins) if all_mins else None
        mx = max(all_maxes) if all_maxes else None
        aggregated.append(ColumnStatsEntry(
            name=name, value_type=vtype,
            min=mn, max=mx, null_count=null_count,
        ))
    return aggregated


def build_stats_tree(kernel: PondMinimal,
                      entries: list[RowGroupEntry]) -> str:
    """Build a hierarchical stats tree from a list of RowGroupEntry.

    Returns the root hash of the tree. The tree is content-addressed:
    the same entries always produce the same root hash.

    Args:
        kernel: the PondMinimal kernel
        entries: list of RowGroupEntry (sorted by key for determinism)

    Returns:
        The root hash of the stats tree (32-byte hex string).
    """
    if not entries:
        # Empty tree: encode an empty leaf
        empty = encode_leaf_node([])
        return kernel.write(empty)

    # Sort entries by key for deterministic tree shape
    sorted_entries = sorted(entries, key=lambda e: e.key)

    # Single leaf — return it directly
    if len(sorted_entries) <= TARGET_LEAF_ENTRIES:
        leaf_data = encode_leaf_node(sorted_entries)
        return kernel.write(leaf_data)

    # Level 0: leaves — list of (max_key, leaf_hash, aggregated_stats)
    # For each leaf, we also compute its aggregated stats so we can
    # attach them to the parent internal node's child reference.
    leaves: list[tuple[str, str, list[ColumnStatsEntry]]] = []
    for i in range(0, len(sorted_entries), TARGET_LEAF_ENTRIES):
        chunk = sorted_entries[i:i + TARGET_LEAF_ENTRIES]
        leaf_data = encode_leaf_node(chunk)
        leaf_hash = kernel.write(leaf_data)
        max_key = chunk[-1].key
        agg_stats = aggregate_stats(chunk)
        leaves.append((max_key, leaf_hash, agg_stats))

    # Build internal levels bottom-up.
    # Each level is a list of (max_key, node_hash, aggregated_stats).
    # We group children into chunks of TARGET_LEAF_ENTRIES and build
    # one internal node per group, with aggregated stats computed by
    # combining the children's already-aggregated stats.
    current_level: list[tuple[str, str, list[ColumnStatsEntry]]] = leaves
    while len(current_level) > 1:
        next_level: list[tuple[str, str, list[ColumnStatsEntry]]] = []

        for i in range(0, len(current_level), TARGET_LEAF_ENTRIES):
            group = current_level[i:i + TARGET_LEAF_ENTRIES]

            # Combine the children's already-aggregated stats
            group_stats = _aggregate_aggregated([g[2] for g in group])

            # Build the internal node's children list
            children = [InternalChild(
                max_key=g[0], child_hash=g[1],
                column_stats=group_stats,
            ) for g in group]
            internal_data = encode_internal_node(children)
            internal_hash = kernel.write(internal_data)

            max_key = group[-1][0]
            next_level.append((max_key, internal_hash, group_stats))

        current_level = next_level

    # current_level has 1 element: the root
    return current_level[0][1]


def _aggregate_aggregated(stats_list: list[list[ColumnStatsEntry]]
                            ) -> list[ColumnStatsEntry]:
    """Combine multiple aggregated-stats lists into one.

    Each input is a list of ColumnStatsEntry (already aggregated over
    some subtree). We combine them by:
      - min = min of all mins
      - max = max of all maxes
      - null_count = sum of all null_counts

    This is the same operation as aggregate_stats, but operates on
    pre-aggregated lists instead of raw RowGroupEntry lists.
    """
    if not stats_list:
        return []

    # Use the first list as a template for column names + value types
    template = stats_list[0]
    result: list[ColumnStatsEntry] = []
    for col_template in template:
        all_mins = []
        all_maxes = []
        null_count = 0
        for stats in stats_list:
            for c in stats:
                if c.name == col_template.name:
                    if c.min is not None:
                        all_mins.append(c.min)
                    if c.max is not None:
                        all_maxes.append(c.max)
                    null_count += c.null_count
                    break
        result.append(ColumnStatsEntry(
            name=col_template.name,
            value_type=col_template.value_type,
            min=min(all_mins) if all_mins else None,
            max=max(all_maxes) if all_maxes else None,
            null_count=null_count,
        ))
    return result


# ---------------------------------------------------------------------------
# Stats tree reader
# ---------------------------------------------------------------------------

class StatsTreeReader:
    """Reads a hierarchical stats tree, pruning subtrees that can't match.

    Lifecycle:
      1. tree = StatsTreeReader(kernel, root_hash)
      2. for entry in tree.scan_with_pruning(predicates):
             # entry is a RowGroupEntry that survived pruning
             data_bytes = kernel.read_blob(entry.blob_hash)

    The reader fetches O(log N) nodes (cached via content addressing),
    pruning subtrees whose aggregated stats prove they can't match.
    """

    def __init__(self, kernel: PondMinimal, root_hash: str):
        self.kernel = kernel
        self.root_hash = root_hash
        # Cache: hash → decoded node. Content-addressed means the same
        # hash always gives the same node, so caching is safe across
        # reads and even across readers.
        self._cache: dict[str, Any] = {}

    def _read_node(self, node_hash: str) -> Any:
        """Read and decode a stats tree node. Cached."""
        if node_hash in self._cache:
            return self._cache[node_hash]
        data = self.kernel.read_blob(node_hash)
        if data[:4] != _STATS_TREE_MAGIC:
            raise ValueError(f"Not a stats tree node (magic={data[:4]!r})")
        node_type = data[5]
        if node_type == _NODE_TYPE_LEAF:
            node = decode_leaf_node(data)
        elif node_type == _NODE_TYPE_INTERNAL:
            node = decode_internal_node(data)
        else:
            raise ValueError(f"Unknown node type: {node_type}")
        self._cache[node_hash] = node
        return node

    def scan_with_pruning(
            self,
            predicates: Optional[list[tuple[str, str, Any]]] = None,
            start_key: Optional[str] = None,
            end_key: Optional[str] = None,
    ) -> Iterator[RowGroupEntry]:
        """Yield row groups that might match the predicates.

        Walks the tree top-down. At each internal node, evaluates
        predicates against each child's aggregated stats — skips
        children whose stats prove they can't match. At each leaf,
        yields entries that survive row-group-level pruning.

        Total fetches: O(log N + K) where K = surviving row groups.
        Each node is fetched once and cached.
        """
        yield from self._scan_node(self.root_hash, predicates, start_key, end_key)

    def _scan_node(self, node_hash: str,
                    predicates: Optional[list[tuple[str, str, Any]]],
                    start_key: Optional[str],
                    end_key: Optional[str]) -> Iterator[RowGroupEntry]:
        """Recursively scan a node, yielding surviving entries."""
        node = self._read_node(node_hash)

        if isinstance(node, list) and node and isinstance(node[0], RowGroupEntry):
            # Leaf node
            for rg in node:
                # Key range filter
                if start_key is not None and rg.key < start_key:
                    continue
                if end_key is not None and rg.key > end_key:
                    continue
                # Predicate pruning
                if predicates and rg.can_prune(predicates):
                    continue
                yield rg
            return

        if isinstance(node, list) and node and isinstance(node[0], InternalChild):
            # Internal node
            for child in node:
                # Key range filter (skip subtrees outside the range)
                if start_key is not None and child.max_key < start_key:
                    continue  # entire subtree is below start_key
                # end_key is harder to bound without a min_key, but we can
                # at least skip if all children would be past end_key.
                # For simplicity, descend and let the leaf filter.

                # Predicate pruning against aggregated stats
                if predicates:
                    pruned = False
                    for col_name, op, val in predicates:
                        col = None
                        for c in child.column_stats:
                            if c.name == col_name:
                                col = c
                                break
                        if col is None:
                            continue
                        if col.can_prune(op, val):
                            pruned = True
                            break
                    if pruned:
                        continue  # entire subtree pruned

                yield from self._scan_node(child.child_hash,
                                             predicates, start_key, end_key)
            return

    def find_row_group(self, key: str) -> Optional[RowGroupEntry]:
        """O(log N) point lookup: find smallest row group with key >= target.

        Walks the tree top-down, descending into the FIRST child whose
        max_key >= target. At the leaf, returns the first entry with
        key >= target.

        Total: O(log N) S3 GETs (one per tree level, cached by content
        addressing after the first read).
        """
        return self._find_row_group(self.root_hash, key)

    def _find_row_group(self, node_hash: str,
                         key: str) -> Optional[RowGroupEntry]:
        """Recursive descent."""
        node = self._read_node(node_hash)

        if isinstance(node, list) and node and isinstance(node[0], RowGroupEntry):
            # Leaf node — return first entry with key >= target
            for rg in node:
                if rg.key >= key:
                    return rg
            return None

        if isinstance(node, list) and node and isinstance(node[0], InternalChild):
            # Internal node — find first child whose max_key >= target
            for child in node:
                if child.max_key >= key:
                    # Descend into this child
                    return self._find_row_group(child.child_hash, key)
            return None

        return None


# ---------------------------------------------------------------------------
# Convenience: build a stats tree from a manifest
# ---------------------------------------------------------------------------

def build_stats_tree_from_manifest(kernel: PondMinimal,
                                     manifest) -> str:
    """Build a stats tree from a CollectionManifest's row groups.

    Args:
        kernel: the PondMinimal kernel
        manifest: a CollectionManifest with row_groups populated

    Returns:
        The root hash of the stats tree.
    """
    return build_stats_tree(kernel, manifest.row_groups)


def should_use_stats_tree(n_row_groups: int) -> bool:
    """Decide whether a collection should use a stats tree.

    Returns True if the manifest blob would exceed ~5MB (the S3
    single-fetch sweet spot).
    """
    return n_row_groups > FLAT_MANIFEST_MAX_ROW_GROUPS
