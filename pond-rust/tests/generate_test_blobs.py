#!/usr/bin/env python3
"""
Generate PND2 test blobs for the C ABI test.

Produces binary files in pond-rust/tests/test_blobs/:
  - i64_raw.bin       : single INT64 column, RAW encoding
  - f64_raw.bin       : single FLOAT64 column, RAW encoding
  - str_raw.bin       : single STRING column, RAW encoding
  - bin_raw.bin       : single BINARY column, RAW encoding
  - i64_rle.bin       : single INT64 column, RLE encoding
  - str_dict.bin      : single STRING column, DICT encoding
  - i64_bitpack.bin   : single INT64 column, BITPACK encoding

These blobs are loaded by tests/test_c_abi.c to verify the C ABI can
decode all encodings produced by the Python encoder.
"""
import os
import sys
import struct

# Make pond-sdk importable
HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO_ROOT, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO_ROOT, "pond-rust", "target", "release"))

from extensions.physical_structures.unified_storage import PND2, ColumnSource  # noqa: E402


class ListColumnSource(ColumnSource):
    """Trivial ColumnSource backed by a list of (name, values) tuples."""

    def __init__(self, columns):
        self._columns = dict(columns)
        self._col_names = [name for name, _ in columns]
        self._n_rows = len(columns[0][1]) if columns else 0

    def num_rows(self):
        return self._n_rows

    def column_names(self):
        return list(self._col_names)

    def column_slice(self, name, start, end):
        return self._columns[name][start:end]

    def column_stats(self, name):
        from extensions.physical_structures.column_source import compute_list_stats
        return compute_list_stats(self._columns[name])


def main():
    out_dir = os.path.join(HERE, "test_blobs")
    os.makedirs(out_dir, exist_ok=True)

    # 1. INT64 RAW
    src = ListColumnSource([("v", [1, 2, 3, 100, -50, 999999, 0, -1])])
    blob, _ = PND2.encode(src, encoding_hints={"v": "raw"}, compress=False)
    with open(os.path.join(out_dir, "i64_raw.bin"), "wb") as f:
        f.write(blob)
    print(f"i64_raw.bin: {len(blob)} bytes")

    # 2. FLOAT64 RAW
    src = ListColumnSource([("v", [1.5, 2.5, 3.5, -0.5, 99.99, 0.0, -1.0, 1e10])])
    blob, _ = PND2.encode(src, encoding_hints={"v": "raw"}, compress=False)
    with open(os.path.join(out_dir, "f64_raw.bin"), "wb") as f:
        f.write(blob)
    print(f"f64_raw.bin: {len(blob)} bytes")

    # 3. STRING RAW
    src = ListColumnSource([("v", ["alice", "bob", "carol", "dave", "eve"])])
    blob, _ = PND2.encode(src, encoding_hints={"v": "raw"}, compress=False)
    with open(os.path.join(out_dir, "str_raw.bin"), "wb") as f:
        f.write(blob)
    print(f"str_raw.bin: {len(blob)} bytes")

    # 4. INT64 RLE  (long runs of repeated values)
    rle_values = [10] * 50 + [20] * 30 + [10] * 20
    src = ListColumnSource([("v", rle_values)])
    blob, _ = PND2.encode(src, encoding_hints={"v": "rle"}, compress=False)
    with open(os.path.join(out_dir, "i64_rle.bin"), "wb") as f:
        f.write(blob)
    print(f"i64_rle.bin: {len(blob)} bytes ({len(rle_values)} rows, 3 runs)")

    # 5. STRING DICT  (low-cardinality strings)
    dict_values = ["a", "b", "c", "a", "b", "c", "a", "b", "c", "a"]
    src = ListColumnSource([("v", dict_values)])
    blob, _ = PND2.encode(src, encoding_hints={"v": "dict"}, compress=False)
    with open(os.path.join(out_dir, "str_dict.bin"), "wb") as f:
        f.write(blob)
    print(f"str_dict.bin: {len(blob)} bytes ({len(dict_values)} rows, 3 unique)")

    # 6. INT64 BITPACK (small-range integers — perfect for bitpacking)
    bitpack_values = list(range(0, 100)) + list(range(0, 100))  # 0..99 twice
    src = ListColumnSource([("v", bitpack_values)])
    blob, _ = PND2.encode(src, encoding_hints={"v": "bitpack"}, compress=False)
    with open(os.path.join(out_dir, "i64_bitpack.bin"), "wb") as f:
        f.write(blob)
    print(f"i64_bitpack.bin: {len(blob)} bytes ({len(bitpack_values)} rows, "
          f"range 0..99)")

    # 7. BINARY RAW  (Python encoder handles BINARY via the slow path)
    bin_values = [b"\x00\x01\x02", b"\xff\xee\xdd", b"", b"\xab\xcd", b"\x00" * 8]
    src = ListColumnSource([("v", bin_values)])
    blob, _ = PND2.encode(src, encoding_hints={"v": "raw"}, compress=False)
    with open(os.path.join(out_dir, "bin_raw.bin"), "wb") as f:
        f.write(blob)
    print(f"bin_raw.bin: {len(blob)} bytes ({len(bin_values)} rows)")

    print(f"\nGenerated {len(os.listdir(out_dir))} blob files in {out_dir}")


if __name__ == "__main__":
    main()
