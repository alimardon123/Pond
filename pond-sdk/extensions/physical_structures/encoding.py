"""
Encoding-aware compute — FastLanes-style structural encodings for Pond.

The big idea (borrowed from FastLanes / Vortex):
  Instead of always decoding a chunk blob to PyArrow then filtering,
  apply predicates DIRECTLY on the encoded bytes for select encodings.
  Skip the decode step entirely for pruned chunks.

Supported encodings (initial set, all storage-friendly and simple):

  1. RLE (Run-Length Encoding) — great for low-cardinality / sorted cols
     Layout: [run_value, run_length, run_value, run_length, ...]
     Predicate eval: walk runs, yield (run_start, run_end) for surviving runs

  2. Dictionary — great for strings / categoricals
     Layout: dict_codes (int array) + dict_values (unique values)
     Predicate eval: scan dict_values once to find matching codes,
     then yield positions where dict_codes[pos] in matching_codes

  3. Bitpack — great for small-range integers
     Layout: bitwidth (1 byte) + packed ints
     Predicate eval: unpack lazily, only positions where predicate matches

  4. Raw (passthrough) — for high-cardinality / heterogeneous data
     Layout: original Parquet bytes
     Predicate eval: falls back to decode + filter

Each encoded chunk blob has a small HEADER:
  magic: 4 bytes ("PND1")
  encoding: 1 byte (0=raw, 1=rle, 2=dict, 3=bitpack)
  n_rows: 4 bytes (uint32)
  payload: encoding-specific

The header is what enables encoding-aware compute: a reader can peek
at the header, decide whether to evaluate the predicate on the encoded
form, and only decode if necessary.

GENERIC: works with any lens that produces column data. The current
implementation uses PyArrow for input, but the encoding format is
lens-agnostic — any lens that produces (column_name, values) pairs
can use it.

Usage (write side — lens does this at write time):
    from encoding import ColumnEncoding, encode_column
    encoded_bytes, encoding_meta = encode_column(values, hint="auto")
    blob_hash = kernel.write(encoded_bytes)

Usage (read side — encoding-aware pruning reader):
    from encoding import EncodingHeader, eval_predicate_encoded
    header = EncodingHeader.from_bytes(blob_bytes[:9])
    if header.encoding == EncodingHeader.RLE:
        matches = eval_predicate_encoded(blob_bytes, op=">=", value=30)
        # matches = list of (start, end) row ranges that survive
        # decode only those ranges, or yield row indices for further filter
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass, field
from typing import Optional, Any, Iterable, Union


# ---------------------------------------------------------------------------
# Encoding constants
# ---------------------------------------------------------------------------

class ColumnEncoding:
    """Encoding type identifiers."""
    RAW = 0       # passthrough — Parquet bytes
    RLE = 1       # run-length encoding (great for low-cardinality / sorted)
    DICT = 2      # dictionary encoding (great for strings/categoricals)
    BITPACK = 3   # bitpacked integers (great for small-range ints)

    @classmethod
    def name(cls, code: int) -> str:
        return {0: "raw", 1: "rle", 2: "dict", 3: "bitpack"}.get(code, "?")

    @classmethod
    def choose(cls, values: list, hint: str = "auto") -> int:
        """Pick an encoding based on data characteristics.

        Args:
            values: list of values (will scan a sample)
            hint: "auto" (default), "rle", "dict", "bitpack", "raw"

        Returns:
            Encoding code (RAW/RLE/DICT/BITPACK).
        """
        if hint != "auto":
            return {"raw": cls.RAW, "rle": cls.RLE,
                    "dict": cls.DICT, "bitpack": cls.BITPACK}.get(hint, cls.RAW)

        if not values:
            return cls.RAW

        # Heuristics
        n = len(values)
        unique = set(values)
        cardinality = len(unique)

        # Low cardinality → dictionary
        if cardinality / n < 0.1 and cardinality < 1000:
            return cls.DICT

        # Sorted / mostly-sorted runs → RLE
        if cls._is_run_heavy(values):
            return cls.RLE

        # Small-range integers → bitpack
        if all(isinstance(v, int) for v in values[:100]):
            try:
                vmin = min(values)
                vmax = max(values)
                if vmax - vmin < 2**16:  # fits in 16 bits
                    return cls.BITPACK
            except Exception:
                pass

        # Default — passthrough
        return cls.RAW

    @staticmethod
    def _is_run_heavy(values: list) -> bool:
        """True if the values have many consecutive repeats (RLE-friendly)."""
        if len(values) < 10:
            return False
        # Sample up to 1000 values
        sample = values[:1000] if len(values) > 1000 else values
        transitions = sum(1 for i in range(1, len(sample))
                          if sample[i] != sample[i - 1])
        # RLE-friendly if transitions < 10% of sample size
        return transitions < len(sample) * 0.1


# ---------------------------------------------------------------------------
# Encoding header — prepended to every encoded chunk blob
# ---------------------------------------------------------------------------

# Header format: magic(4) + encoding(1) + n_rows(4) = 9 bytes
_HEADER_FORMAT = "<4sBI"
_HEADER_SIZE = struct.calcsize(_HEADER_FORMAT)
_HEADER_MAGIC = b"PND1"


class EncodingHeader:
    """Header prepended to every encoded chunk blob.

    Format (9 bytes total):
      magic:     4 bytes  ("PND1")
      encoding:  1 byte   (0=raw, 1=rle, 2=dict, 3=bitpack)
      n_rows:    4 bytes  (uint32, little-endian)
    """
    SIZE = _HEADER_SIZE

    def __init__(self, encoding: int, n_rows: int):
        self.encoding = encoding
        self.n_rows = n_rows

    def to_bytes(self) -> bytes:
        return struct.pack(_HEADER_FORMAT, _HEADER_MAGIC,
                           self.encoding, self.n_rows)

    @classmethod
    def from_bytes(cls, b: bytes) -> "EncodingHeader":
        if len(b) < _HEADER_SIZE:
            raise ValueError(f"Header too short: {len(b)} < {_HEADER_SIZE}")
        magic, encoding, n_rows = struct.unpack(_HEADER_FORMAT, b[:_HEADER_SIZE])
        if magic != _HEADER_MAGIC:
            raise ValueError(f"Bad magic: {magic!r} (expected {_HEADER_MAGIC!r})")
        return cls(encoding=encoding, n_rows=n_rows)

    def __repr__(self):
        return (f"EncodingHeader(encoding={ColumnEncoding.name(self.encoding)}, "
                f"n_rows={self.n_rows})")


# ---------------------------------------------------------------------------
# Per-encoding encoders
# ---------------------------------------------------------------------------

def encode_raw(values: list) -> tuple[bytes, dict]:
    """Raw encoding — stores values as a JSON list (no compression).

    This is the fallback when no structural encoding applies. The
    advantage over Parquet is that we control the format and can
    short-circuit decode for predicate evaluation.
    """
    payload = json.dumps(values, default=str).encode()
    n_rows = len(values)
    header = EncodingHeader(ColumnEncoding.RAW, n_rows).to_bytes()
    meta = {"encoding": "raw", "n_rows": n_rows, "payload_size": len(payload)}
    return header + payload, meta


def encode_rle(values: list) -> tuple[bytes, dict]:
    """Run-length encoding — [value, run_length] pairs.

    Great for low-cardinality / sorted columns. For a column with N
    consecutive identical values, RLE stores 2 numbers instead of N.

    Layout: [value, run_length, value, run_length, ...] as JSON list
    """
    if not values:
        runs = []
    else:
        runs = []
        current = values[0]
        count = 1
        for v in values[1:]:
            if v == current:
                count += 1
            else:
                runs.append([current, count])
                current = v
                count = 1
        runs.append([current, count])

    payload = json.dumps(runs, default=str).encode()
    n_rows = len(values)
    header = EncodingHeader(ColumnEncoding.RLE, n_rows).to_bytes()
    meta = {
        "encoding": "rle",
        "n_rows": n_rows,
        "n_runs": len(runs),
        "compression_ratio": n_rows / max(len(runs), 1),
        "payload_size": len(payload),
    }
    return header + payload, meta


def encode_dict(values: list) -> tuple[bytes, dict]:
    """Dictionary encoding — dict_values + dict_codes.

    Great for strings / categoricals. Stores unique values in a
    dictionary and replaces each value with its code (small int).

    Layout: JSON dict {"dict": [...], "codes": [...]}
    """
    if not values:
        payload = json.dumps({"dict": [], "codes": []}).encode()
        n_rows = 0
        meta = {"encoding": "dict", "n_rows": 0, "n_unique": 0,
                "payload_size": len(payload)}
        return EncodingHeader(ColumnEncoding.DICT, 0).to_bytes() + payload, meta

    # Build dictionary
    unique: list = []
    code_map: dict = {}
    for v in values:
        if v not in code_map:
            code_map[v] = len(unique)
            unique.append(v)
    codes = [code_map[v] for v in values]

    payload = json.dumps({"dict": unique, "codes": codes}, default=str).encode()
    n_rows = len(values)
    header = EncodingHeader(ColumnEncoding.DICT, n_rows).to_bytes()
    meta = {
        "encoding": "dict",
        "n_rows": n_rows,
        "n_unique": len(unique),
        "cardinality_ratio": len(unique) / n_rows,
        "payload_size": len(payload),
    }
    return header + payload, meta


def encode_bitpack(values: list) -> tuple[bytes, dict]:
    """Bitpack encoding — pack small-range integers into minimal bits.

    Great for small-range integers. Computes bitwidth = ceil(log2(range+1)),
    packs each value (offset to non-negative) into that many bits.

    Layout: bitwidth(1) + offset(8) + packed bits as JSON list
    (Using JSON for portability; a real implementation would use raw bytes.)
    """
    if not values:
        payload = json.dumps({"bitwidth": 0, "offset": 0, "packed": []}).encode()
        meta = {"encoding": "bitpack", "n_rows": 0, "bitwidth": 0,
                "payload_size": len(payload)}
        return EncodingHeader(ColumnEncoding.BITPACK, 0).to_bytes() + payload, meta

    vmin = min(values)
    vmax = max(values)
    offset = vmin
    range_val = vmax - vmin
    if range_val == 0:
        bitwidth = 1
    else:
        bitwidth = max(1, (range_val + 1).bit_length())

    # Offset values to non-negative and pack
    offset_vals = [v - offset for v in values]
    # Pack into integers (group bitwidth bits at a time)
    # For simplicity, store as a list of offset values; bitpacking is
    # conceptual — the win is that we can compute min/max/range in O(1).
    payload = json.dumps({
        "bitwidth": bitwidth,
        "offset": offset,
        "min": vmin,
        "max": vmax,
        "packed": offset_vals,
    }).encode()

    n_rows = len(values)
    header = EncodingHeader(ColumnEncoding.BITPACK, n_rows).to_bytes()
    meta = {
        "encoding": "bitpack",
        "n_rows": n_rows,
        "bitwidth": bitwidth,
        "min": vmin,
        "max": vmax,
        "payload_size": len(payload),
    }
    return header + payload, meta


# ---------------------------------------------------------------------------
# Master encoder — picks encoding and dispatches
# ---------------------------------------------------------------------------

_ENCODERS = {
    ColumnEncoding.RAW: encode_raw,
    ColumnEncoding.RLE: encode_rle,
    ColumnEncoding.DICT: encode_dict,
    ColumnEncoding.BITPACK: encode_bitpack,
}


def encode_column(values: list, hint: str = "auto"
                  ) -> tuple[bytes, dict]:
    """Encode a column's values using the best (or hinted) encoding.

    Args:
        values: list of column values
        hint: "auto" (default), "rle", "dict", "bitpack", "raw"

    Returns:
        Tuple of (encoded_bytes, meta_dict).
        encoded_bytes = header(9) + payload
        meta_dict includes encoding name, n_rows, encoding-specific stats
    """
    encoding = ColumnEncoding.choose(values, hint=hint)
    encoder = _ENCODERS[encoding]
    return encoder(values)


# ---------------------------------------------------------------------------
# Encoded predicate evaluation — skip decode for select encodings
# ---------------------------------------------------------------------------

def eval_predicate_encoded(blob_bytes: bytes, column: str,
                            op: str, value: Any
                            ) -> Optional[tuple[list[tuple[int, int]], dict]]:
    """Evaluate a predicate on the ENCODED form of a chunk blob.

    Returns None if the encoding does NOT support direct predicate
    evaluation (caller should fall back to decode + filter).

    Returns (surviving_ranges, meta) if the encoding DOES support it:
      surviving_ranges: list of (start_row, end_row) tuples (end exclusive)
                        representing row ranges that MIGHT match.
                        Empty list = no rows can match (chunk fully pruned).
      meta: dict with encoding name and any eval-specific stats

    Args:
        blob_bytes: the encoded chunk blob (header + payload)
        column: column name (for context, not used in eval)
        op: comparison operator (=, !=, <, <=, >, >=, in)
        value: comparison value (or list for "in")
    """
    header = EncodingHeader.from_bytes(blob_bytes[:EncodingHeader.SIZE])
    payload = blob_bytes[EncodingHeader.SIZE:]

    if header.encoding == ColumnEncoding.RLE:
        return _eval_rle(payload, op, value)
    elif header.encoding == ColumnEncoding.DICT:
        return _eval_dict(payload, op, value)
    elif header.encoding == ColumnEncoding.BITPACK:
        return _eval_bitpack(payload, op, value)
    else:
        # RAW encoding — no shortcut, caller must decode
        return None


def _eval_rle(payload: bytes, op: str, value: Any
              ) -> tuple[list[tuple[int, int]], dict]:
    """Evaluate predicate on RLE-encoded data.

    Walks runs; for each run, checks if run_value matches the predicate.
    If yes, yields (run_start, run_start + run_length) as a surviving range.
    """
    runs = json.loads(payload)
    surviving = []
    pos = 0
    for run_value, run_length in runs:
        if _value_matches(run_value, op, value):
            surviving.append((pos, pos + run_length))
        pos += run_length

    return surviving, {
        "encoding": "rle",
        "n_runs": len(runs),
        "n_surviving_runs": len(surviving),
        "n_surviving_rows": sum(e - s for s, e in surviving),
    }


def _eval_dict(payload: bytes, op: str, value: Any
               ) -> tuple[list[tuple[int, int]], dict]:
    """Evaluate predicate on dictionary-encoded data.

    Scans dict_values once to find matching codes, then scans codes
    array to find row positions where codes[pos] in matching_codes.
    Returns surviving_ranges as a list of (start, end) tuples.
    """
    data = json.loads(payload)
    dict_values = data["dict"]
    codes = data["codes"]

    # Find matching codes
    matching_codes = set()
    for code, dv in enumerate(dict_values):
        if _value_matches(dv, op, value):
            matching_codes.add(code)

    if not matching_codes:
        return [], {"encoding": "dict", "n_unique": len(dict_values),
                    "n_surviving_rows": 0}

    # Find row positions where codes[pos] in matching_codes
    # Coalesce consecutive positions into ranges
    surviving = []
    range_start = None
    for pos, code in enumerate(codes):
        if code in matching_codes:
            if range_start is None:
                range_start = pos
        else:
            if range_start is not None:
                surviving.append((range_start, pos))
                range_start = None
    if range_start is not None:
        surviving.append((range_start, len(codes)))

    return surviving, {
        "encoding": "dict",
        "n_unique": len(dict_values),
        "n_matching_codes": len(matching_codes),
        "n_surviving_ranges": len(surviving),
        "n_surviving_rows": sum(e - s for s, e in surviving),
    }


def _eval_bitpack(payload: bytes, op: str, value: Any
                  ) -> tuple[list[tuple[int, int]], dict]:
    """Evaluate predicate on bitpack-encoded data.

    Bitpack stores min/max in the payload header. If the predicate
    can be fully pruned by min/max (e.g., value > max → no matches),
    return []. Otherwise, return [(0, n_rows)] — caller must decode.
    """
    data = json.loads(payload)
    vmin = data["min"]
    vmax = data["max"]

    # Try to fully prune using min/max
    if op == ">" and vmax <= value:
        return [], {"encoding": "bitpack", "pruned_by_minmax": True,
                    "n_surviving_rows": 0}
    if op == ">=" and vmax < value:
        return [], {"encoding": "bitpack", "pruned_by_minmax": True,
                    "n_surviving_rows": 0}
    if op == "<" and vmin >= value:
        return [], {"encoding": "bitpack", "pruned_by_minmax": True,
                    "n_surviving_rows": 0}
    if op == "<=" and vmin > value:
        return [], {"encoding": "bitpack", "pruned_by_minmax": True,
                    "n_surviving_rows": 0}
    if op == "=" and (value < vmin or value > vmax):
        return [], {"encoding": "bitpack", "pruned_by_minmax": True,
                    "n_surviving_rows": 0}

    # Can't fully prune — caller must decode and filter
    # But we can return (0, n_rows) so the caller knows the chunk survived
    return [(0, data.get("n_rows", len(data.get("packed", []))))], {
        "encoding": "bitpack",
        "pruned_by_minmax": False,
        "min": vmin, "max": vmax,
        "n_surviving_rows": data.get("n_rows", len(data.get("packed", []))),
    }


def _value_matches(val: Any, op: str, value: Any) -> bool:
    """Check if val matches the predicate (op, value)."""
    try:
        if op == "=":
            return val == value
        elif op == "!=":
            return val != value
        elif op == "<":
            return val < value
        elif op == "<=":
            return val <= value
        elif op == ">":
            return val > value
        elif op == ">=":
            return val >= value
        elif op == "in":
            return val in value
    except (TypeError, ValueError):
        return False
    return False


# ---------------------------------------------------------------------------
# Decoders — for when encoded predicate eval is not possible
# ---------------------------------------------------------------------------

def decode_column(blob_bytes: bytes) -> list:
    """Decode an encoded chunk blob back to a list of values.

    Used as a fallback when eval_predicate_encoded returns None or
    when the caller needs the actual values (e.g., for projection).
    """
    header = EncodingHeader.from_bytes(blob_bytes[:EncodingHeader.SIZE])
    payload = blob_bytes[EncodingHeader.SIZE:]

    if header.encoding == ColumnEncoding.RAW:
        return json.loads(payload)
    elif header.encoding == ColumnEncoding.RLE:
        runs = json.loads(payload)
        result = []
        for value, length in runs:
            result.extend([value] * length)
        return result
    elif header.encoding == ColumnEncoding.DICT:
        data = json.loads(payload)
        dict_values = data["dict"]
        codes = data["codes"]
        return [dict_values[c] for c in codes]
    elif header.encoding == ColumnEncoding.BITPACK:
        data = json.loads(payload)
        offset = data["offset"]
        packed = data["packed"]
        return [v + offset for v in packed]
    else:
        raise ValueError(f"Unknown encoding: {header.encoding}")


def decode_surviving_values(blob_bytes: bytes,
                             surviving_ranges: list[tuple[int, int]]) -> list:
    """Decode only the values in surviving_ranges from an encoded chunk blob.

    This is the FastLanes-style optimization: instead of decoding the entire
    chunk to a list and then slicing, we walk the encoded form and yield
    only the values that fall in surviving_ranges. For RLE, we walk runs
    and only materialize the ones that overlap a surviving range. For
    DICT, we walk codes and only materialize the ones in a surviving range.

    For BITPACK and RAW, we still decode the whole chunk and slice (no
    shortcut — BITPACK's win is in min/max pruning at the chunk level,
    not in selective decode).

    Args:
        blob_bytes: the encoded chunk blob (header + payload)
        surviving_ranges: list of (start, end) row ranges (end exclusive)

    Returns:
        List of decoded values from the surviving ranges, in order.
    """
    if not surviving_ranges:
        return []

    header = EncodingHeader.from_bytes(blob_bytes[:EncodingHeader.SIZE])
    payload = blob_bytes[EncodingHeader.SIZE:]

    if header.encoding == ColumnEncoding.RLE:
        # Walk runs; for each run, check if it overlaps any surviving range.
        # If yes, materialize only the overlapping rows.
        runs = json.loads(payload)
        result = []
        pos = 0
        ranges_iter = iter(surviving_ranges)
        try:
            cur_start, cur_end = next(ranges_iter)
        except StopIteration:
            return []

        for run_value, run_length in runs:
            run_end = pos + run_length
            # Skip surviving ranges that end before this run starts
            while cur_end <= pos:
                try:
                    cur_start, cur_end = next(ranges_iter)
                except StopIteration:
                    return result
            # Process all surviving ranges that overlap this run
            while cur_start < run_end:
                if cur_end <= pos:
                    try:
                        cur_start, cur_end = next(ranges_iter)
                    except StopIteration:
                        return result
                    continue
                # Compute overlap
                overlap_start = max(cur_start, pos)
                overlap_end = min(cur_end, run_end)
                if overlap_end > overlap_start:
                    offset = overlap_start - pos
                    n = overlap_end - overlap_start
                    result.extend([run_value] * n)
                if cur_end <= run_end:
                    try:
                        cur_start, cur_end = next(ranges_iter)
                    except StopIteration:
                        return result
                else:
                    break  # next run will handle the rest
            pos = run_end
        return result

    elif header.encoding == ColumnEncoding.DICT:
        # Walk codes; for each code, check if its position is in any
        # surviving range. If yes, materialize dict_values[code].
        data = json.loads(payload)
        dict_values = data["dict"]
        codes = data["codes"]
        result = []
        ranges_iter = iter(surviving_ranges)
        try:
            cur_start, cur_end = next(ranges_iter)
        except StopIteration:
            return []

        for pos, code in enumerate(codes):
            # Advance past ranges that end before this position
            while cur_end <= pos:
                try:
                    cur_start, cur_end = next(ranges_iter)
                except StopIteration:
                    return result
            if cur_start <= pos < cur_end:
                result.append(dict_values[code])
        return result

    else:
        # BITPACK or RAW — no shortcut, decode and slice
        all_values = decode_column(blob_bytes)
        result = []
        for s, e in surviving_ranges:
            result.extend(all_values[s:e])
        return result
