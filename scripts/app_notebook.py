#!/usr/bin/env python3
"""Jupyter Notebook application on Pond — full notebook with cells + attachments.

This demonstrates Pond hosting a COMPLETE Jupyter notebook workload:
  - Code cells (STRING source)
  - Markdown cells (STRING source)
  - Output cells (STRING + BINARY for images)
  - Attachments (BINARY — PNG, JPEG, etc.)
  - Cell metadata (execution count, tags)
  - Version control (commit, branch, revert)

Compares to the original Jupyter notebook format (.ipynb = JSON):
  - .ipynb: single JSON file, no versioning, no concurrent editing
  - Pond:   per-cell storage, versioned, concurrent editing via shards

Usage:
    python scripts/app_notebook.py
"""
import sys, os, json, time
from typing import Optional

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))

from object_store_native_kernel import make_object_store_native_kernel
from pond_storage import PondStorage


class NotebookApp:
    """A Jupyter notebook application backed by Pond storage.

    Each notebook is a Pond collection. Each cell is a row with:
      - cell_id: sequential INT64
      - cell_type: STRING ("code", "markdown", "raw")
      - source: STRING (the cell content)
      - attachment: BINARY (optional — images, files)
      - attachment_type: STRING ("image/png", "application/pdf", etc.)
      - execution_count: INT64 (for code cells)
      - metadata: STRING (JSON — tags, slide info, etc.)
      - output: STRING (execution output)
      - output_image: BINARY (image output — plots, etc.)
    """

    def __init__(self, storage: PondStorage):
        self.storage = storage

    def create_notebook(self, name: str) -> str:
        """Create a new empty notebook (collection)."""
        # Write an empty collection with the notebook schema
        rows = [{
            "cell_id": 0,
            "cell_type": "markdown",
            "source": "# New Notebook\n\nClick to edit.",
            "attachment": None,
            "attachment_type": "",
            "execution_count": 0,
            "metadata": "{}",
            "output": "",
            "output_image": None,
        }]
        commit = self.storage.write(name, rows, key_col="cell_id",
                                      row_group_size=100,
                                      message=f"Create notebook '{name}'")
        # Compact so the init cell is in HEAD with proper schema
        self.storage.stamp_collection_metadata(
            name, lens_type="notebook", key_col="cell_id",
            schema_hint={
                "cell_id": "int64", "cell_type": "string", "source": "string",
                "attachment": "bytes", "attachment_type": "string",
                "execution_count": "int64", "metadata": "string",
                "output": "string", "output_image": "bytes",
            })
        return commit

    def add_cell(self, notebook: str, cell_type: str = "code",
                 source: str = "", attachment: bytes = None,
                 attachment_type: str = "",
                 metadata: dict = None) -> int:
        """Add a cell to the notebook. Returns the cell_id."""
        # Get current cell count to determine next cell_id
        # Use read_with_shards to see HEAD + all shards
        rows = self.storage.read_with_shards(notebook)
        next_id = max((r.get("cell_id", 0) for r in rows), default=0) + 1

        new_cell = {
            "cell_id": next_id,
            "cell_type": cell_type,
            "source": source,
            "attachment": attachment,
            "attachment_type": attachment_type,
            "execution_count": 0,
            "metadata": json.dumps(metadata or {}),
            "output": "",
            "output_image": None,
        }
        # Use upsert_shard so cells get _rowid + _version (enables updates)
        self.storage.upsert_shard(notebook, [new_cell], key_col="cell_id",
                                    row_group_size=100)
        self.storage._unified._invalidate_manifest_cache(notebook)
        return next_id

    def get_cell(self, notebook: str, cell_id: int) -> Optional[dict]:
        """Get a specific cell by ID."""
        rows = self.storage.read_with_shards(notebook)
        for row in rows:
            if row.get("cell_id") == cell_id:
                return row
        return None

    def update_cell(self, notebook: str, cell_id: int,
                    source: str = None, output: str = None,
                    output_image: bytes = None,
                    execution_count: int = None,
                    metadata: dict = None):
        """Update a cell's content/output."""
        # Read the current cell to get _rowid (for CRDT update)
        cell = self.get_cell(notebook, cell_id)
        if cell is None:
            raise ValueError(f"Cell {cell_id} not found in notebook '{notebook}'")

        updated = dict(cell)
        if source is not None:
            updated["source"] = source
        if output is not None:
            updated["output"] = output
        if output_image is not None:
            updated["output_image"] = output_image
        if execution_count is not None:
            updated["execution_count"] = execution_count
        if metadata is not None:
            updated["metadata"] = json.dumps(metadata)

        # Upsert (CRDT update — keeps _rowid, bumps _version)
        self.storage.upsert_shard(notebook, [updated], key_col="cell_id",
                                    row_group_size=100)
        # Invalidate cache so the next get_cell sees the update
        self.storage._unified._invalidate_manifest_cache(notebook)

    def delete_cell(self, notebook: str, cell_id: int):
        """Delete a cell from the notebook (tombstone)."""
        cell = self.get_cell(notebook, cell_id)
        if cell is None:
            return
        rowid = cell.get("_rowid")
        if rowid:
            self.storage.delete_shard(notebook, [rowid], key_col="cell_id",
                                        row_group_size=100)

    def list_cells(self, notebook: str) -> list[dict]:
        """List all cells in the notebook (sorted by cell_id)."""
        rows = self.storage.read_with_shards(notebook)
        rows.sort(key=lambda r: r.get("cell_id", 0))
        return rows

    def execute_cell(self, notebook: str, cell_id: int) -> str:
        """Execute a code cell (simulated — stores output)."""
        cell = self.get_cell(notebook, cell_id)
        if cell is None:
            raise ValueError(f"Cell {cell_id} not found")
        if cell.get("cell_type") != "code":
            raise ValueError(f"Cell {cell_id} is not a code cell")

        source = cell.get("source", "")
        # Simulate execution
        try:
            # In a real app, this would exec the Python code
            output = f"Executed: {source[:50]}..."
            exec_count = cell.get("execution_count", 0) + 1
            self.update_cell(notebook, cell_id,
                              output=output,
                              execution_count=exec_count)
            return output
        except Exception as e:
            output = f"Error: {e}"
            self.update_cell(notebook, cell_id, output=output)
            return output

    def add_attachment(self, notebook: str, cell_id: int,
                       data: bytes, attachment_type: str = "image/png"):
        """Add a binary attachment to a cell (image, PDF, etc.)."""
        cell = self.get_cell(notebook, cell_id)
        if cell is None:
            raise ValueError(f"Cell {cell_id} not found")
        updated = dict(cell)
        updated["attachment"] = data
        updated["attachment_type"] = attachment_type
        self.storage.upsert_shard(notebook, [updated], key_col="cell_id",
                                    row_group_size=100)
        # Invalidate cache so the next get_cell sees the update
        self.storage._unified._invalidate_manifest_cache(notebook)

    def get_attachment(self, notebook: str, cell_id: int) -> Optional[bytes]:
        """Get the binary attachment for a cell."""
        cell = self.get_cell(notebook, cell_id)
        if cell is None:
            return None
        return cell.get("attachment")

    def export_ipynb(self, notebook: str) -> dict:
        """Export the notebook as a .ipynb JSON structure (Jupyter-compatible)."""
        cells = self.list_cells(notebook)
        ipynb_cells = []
        for cell in cells:
            cell_type = cell.get("cell_type", "code")
            source = cell.get("source", "")
            entry = {
                "cell_type": cell_type,
                "metadata": json.loads(cell.get("metadata", "{}")),
                "source": source.splitlines(keepends=True),
            }
            if cell_type == "code":
                entry["execution_count"] = cell.get("execution_count", 0)
                output = cell.get("output", "")
                if output:
                    entry["outputs"] = [{
                        "output_type": "stream",
                        "name": "stdout",
                        "text": output.splitlines(keepends=True),
                    }]
                else:
                    entry["outputs"] = []
            ipynb_cells.append(entry)

        return {
            "nbformat": 4,
            "nbformat_minor": 5,
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python",
                    "name": "python3",
                },
                "language_info": {"name": "python", "version": "3.12"},
            },
            "cells": ipynb_cells,
        }

    def commit_notebook(self, notebook: str, message: str = "") -> str:
        """Commit the notebook (compact shards into HEAD)."""
        return self.storage.compact_shards(notebook)

    def history(self, notebook: str) -> list[dict]:
        """Get the notebook's version history."""
        return self.storage.history(notebook)

    def revert_to_version(self, notebook: str, commit_hash: str):
        """Revert the notebook to a specific version."""
        return self.storage.revert(notebook, commit_hash)


def demo():
    """Full demo: create notebook, add cells, execute, add attachments, version control."""
    print("=" * 70)
    print("POND NOTEBOOK APP — Full Jupyter notebook on Pond storage")
    print("=" * 70)

    kernel, _ = make_object_store_native_kernel()
    storage = PondStorage(kernel)
    app = NotebookApp(storage)

    # 1. Create notebook
    print("\n1. Create notebook 'analysis'")
    app.create_notebook("analysis")
    print("   Created empty notebook")

    # 2. Add cells
    print("\n2. Add cells")
    app.add_cell("analysis", "markdown", "# Data Analysis\n\nThis notebook analyzes sales data.")
    app.add_cell("analysis", "code", "import pandas as pd\ndf = pd.read_csv('sales.csv')\nprint(df.head())")
    app.add_cell("analysis", "code", "df.groupby('region').sum().plot()")
    app.add_cell("analysis", "markdown", "## Results\n\nThe analysis shows strong growth in Q4.")
    print("   Added 4 cells (2 markdown, 2 code)")

    # 3. Execute cells
    print("\n3. Execute code cells")
    app.execute_cell("analysis", 2)  # cell_id 2 (first code cell)
    app.execute_cell("analysis", 3)  # cell_id 3 (second code cell)
    cell = app.get_cell("analysis", 2)
    print(f"   Cell 2 executed: output='{cell.get('output', '')[:40]}...', exec_count={cell.get('execution_count')}")

    # 4. Add attachment (image)
    print("\n4. Add binary attachment (PNG image)")
    fake_png = b'\x89PNG\r\n\x1a\n' + b'\x00' * 100 + b'fake_image_data_for_plot'
    app.add_attachment("analysis", 3, fake_png, "image/png")
    attachment = app.get_attachment("analysis", 3)
    print(f"   Attachment stored: {len(attachment)} bytes, type=image/png")

    # 5. List all cells
    print("\n5. List all cells")
    cells = app.list_cells("analysis")
    for cell in cells:
        ctype = cell.get("cell_type", "?")
        source = cell.get("source", "")[:50]
        has_att = " [attachment]" if cell.get("attachment") else ""
        exec_n = cell.get("execution_count", 0)
        exec_str = f" (exec:{exec_n})" if ctype == "code" and exec_n > 0 else ""
        print(f"   [{cell.get('cell_id')}] {ctype}: {source}...{has_att}{exec_str}")

    # 6. Update a cell
    print("\n6. Update cell 2 (add more code)")
    app.update_cell("analysis", 2,
                    source="import pandas as pd\ndf = pd.read_csv('sales.csv')\nprint(df.describe())\nprint(df.shape)",
                    output="Executed: import pandas as pd...")
    cell = app.get_cell("analysis", 2)
    print(f"   Updated: source length = {len(cell.get('source', ''))}")

    # 7. Delete a cell
    print("\n7. Delete cell 4 (markdown)")
    app.delete_cell("analysis", 4)
    cells = app.list_cells("analysis")
    print(f"   Remaining cells: {len(cells)} (was 5)")

    # 8. Compact + version control
    print("\n8. Commit notebook (compact shards)")
    app.commit_notebook("analysis")
    hist = app.history("analysis")
    print(f"   History: {len(hist)} commits")
    for c in hist:
        print(f"     {c['hash'][:8]} {c.get('message', '')[:40]}")

    # 9. Export as .ipynb
    print("\n9. Export as .ipynb JSON")
    ipynb = app.export_ipynb("analysis")
    print(f"   Exported: {ipynb['nbformat']}.{ipynb['nbformat_minor']}, "
          f"{len(ipynb['cells'])} cells")
    # Verify structure
    assert ipynb["nbformat"] == 4
    assert len(ipynb["cells"]) == 4  # 5 original - 1 deleted

    # 10. Test revert
    if len(hist) >= 2:
        print(f"\n10. Revert to first commit")
        first_commit = hist[-1]["hash"]
        app.revert_to_version("analysis", first_commit)
        cells = app.list_cells("analysis")
        print(f"    After revert: {len(cells)} cells (should be 1 — original)")

    # 11. Cross-lens: read notebook as a table (Lakehouse lens can read it)
    print("\n11. Cross-lens: read notebook via PondStorage.read()")
    rows = storage.read("analysis")
    print(f"    PondStorage sees: {len(rows)} rows, columns: {list(rows[0].keys()) if rows else 'none'}")

    print("\n" + "=" * 70)
    print("COMPARISON: Pond Notebook vs Traditional Jupyter (.ipynb)")
    print("=" * 70)
    print("""
| Feature              | .ipynb (JSON file)        | Pond Notebook                |
|----------------------|---------------------------|------------------------------|
| Storage              | Single JSON file          | Content-addressed blobs      |
| Versioning           | External (git)            | Built-in (commit/branch)     |
| Concurrent editing   | No (file lock)            | Yes (CRDT shards)            |
| Attachments          | Inline base64 (bloats)    | Separate BINARY blobs        |
| Cell-level updates   | Rewrite entire file       | Upsert single cell (shard)   |
| Time-travel          | No                        | Yes (revert to any commit)   |
| PB-scale             | No (single file)          | Yes (sharded, manifest)      |
| Cross-lens access    | No                        | Yes (any lens can read)      |
| GC/vacuum            | Manual                    | Built-in (vacuum)            |
| Branching            | No                        | Yes (experiment branches)    |
""")


if __name__ == "__main__":
    demo()
