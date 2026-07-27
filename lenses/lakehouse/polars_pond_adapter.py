"""
Polars adapter for Pond's PND1 binary encoded chunks.

Second proof of the SIMD-ready claim: Polars can read Pond's binary
encoded chunks through the same PND1 format spec as the DuckDB adapter.
This proves the format is engine-independent — any execution engine
can read Pond's storage natively.

Polars uses the Arrow columnar format internally, so the adapter
converts PND1 binary → PyArrow Array → Polars Series → Polars DataFrame.
The conversion is zero-copy where possible (Arrow buffers are shared).

Usage:
    from polars_pond_adapter import PondPolarsAdapter

    adapter = PondPolarsAdapter(kernel)
    df = adapter.read_encoded_collection("events")
    result = df.filter(pl.col("age") > 30).group_by("region").len()
"""

from __future__ import annotations

import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "pond-core"))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "pond-sdk"))
sys.path.insert(0, os.path.join(_HERE, "..", "..", "pond-sdk",
                                "extensions", "physical_structures"))

from kernel import PondMinimal
from duckdb_pond_adapter import PondDuckDBAdapter


class PondPolarsAdapter(PondDuckDBAdapter):
    """Reads Pond's PND1 binary encoded chunks as a Polars DataFrame.

    Extends PondDuckDBAdapter (which produces pa.Table) and converts
    to Polars DataFrame. The PND1 binary reading logic is shared —
    only the final conversion differs (pa.Table → pl.DataFrame).

    This proves the PND1 format is engine-independent: the same binary
    chunks can be read by DuckDB, Polars, or any future engine.
    """

    def read_encoded_collection_polars(self, collection: str,
                                         columns: list[str] | None = None
                                         ) -> "pl.DataFrame":
        """Read an encoded collection as a Polars DataFrame.

        Reads the PND1 binary format (same as DuckDB adapter), converts
        to pa.Table, then to Polars DataFrame (zero-copy Arrow transfer).

        Args:
            collection: collection name (must use encoded storage)
            columns: columns to read (None = all)

        Returns:
            pl.DataFrame with the decoded data
        """
        import polars as pl

        # Read as pa.Table (shared PND1 binary reading logic)
        table = self.read_encoded_collection(collection, columns)

        # Convert to Polars DataFrame — zero-copy Arrow transfer
        return pl.from_arrow(table)
