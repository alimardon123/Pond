"""
View Compression Study — strip each View to its irreducible translation layer.

Per the architecture review:
  > Take every View. Delete everything possible. How many lines remain?
  > If every View bottoms out around 100-200 lines: good.
  > If one View bottoms out at 2500 lines: you found friction. That is a kernel finding.

Method:
  For each View, identify the IRREDUCIBLE translation layer — the minimum
  code needed to translate between the workload's native language and the
  kernel's 3 primitives (Write, Read, Reference).

  Strip away:
    - Convenience methods (read_file, list_names, etc.)
    - Caching
    - Error handling
    - Documentation
    - Optimizations

  Keep only:
    - The serialization logic (workload -> bytes)
    - The deserialization logic (bytes -> workload)
    - The DAG pattern (how Trees/Commits are structured)
    - The kernel calls (Write/Read/Reference)

  Measure lines of code. Compare across Views. Large differences = friction.

Outcome vocabulary:
  - Kernel issue: View is large because the kernel is missing something
  - View issue: View is large because of poor design
  - Acceptable: View is large because the workload is genuinely complex
"""

import os
import sys
import re

VIEWS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "prototype")


def count_meaningful_lines(filepath: str) -> dict:
    """Count lines, excluding blanks, comments, and docstrings."""
    with open(filepath) as f:
        content = f.read()

    lines = content.split("\n")
    total = len(lines)

    # Strip docstrings (triple-quoted)
    content_no_docstrings = re.sub(r'""".*?"""', '', content, flags=re.DOTALL)
    content_no_docstrings = re.sub(r"'''.*?'''", '', content_no_docstrings, flags=re.DOTALL)

    lines = content_no_docstrings.split("\n")
    meaningful = 0
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            continue
        if stripped.startswith('"""') or stripped.endswith('"""'):
            continue
        if stripped.startswith("'''") or stripped.endswith("'''"):
            continue
        meaningful += 1

    return {"total": total, "meaningful": meaningful}


def analyze_view(name: str, filepath: str, responsibilities: list[str]) -> dict:
    """Analyze a View's size and complexity."""
    counts = count_meaningful_lines(filepath)
    return {
        "name": name,
        "file": os.path.basename(filepath),
        "total_lines": counts["total"],
        "meaningful_lines": counts["meaningful"],
        "responsibilities": responsibilities,
        "irreducible_estimate": len(responsibilities) * 15,  # ~15 lines per responsibility
    }


def main():
    print("=" * 76)
    print("  View Compression Study")
    print("  Goal: strip each View to irreducible translation. Compare sizes.")
    print("  Large differences = friction = kernel finding.")
    print("=" * 76)
    print()

    views = [
        analyze_view(
            "SQLView",
            os.path.join(VIEWS_DIR, "views_minimal.py"),
            [
                "Schema tracking (pa.Schema)",
                "Batch buffering (RecordBatch -> Arrow IPC)",
                "Arrow IPC -> Parquet conversion",
                "Tree pattern (inherit parent + add blob)",
                "Commit pattern (tree + parent + message)",
                "Read: walk tree, concat Parquet tables",
            ],
        ),
        analyze_view(
            "VectorView",
            os.path.join(VIEWS_DIR, "views_minimal.py"),
            [
                "Vector buffering (list of floats)",
                "Serialization (struct.pack floats)",
                "Tree pattern",
                "Commit pattern",
                "Read: walk tree, deserialize floats",
                "Linear scan for nearest-neighbor search",
            ],
        ),
        analyze_view(
            "StreamView",
            os.path.join(VIEWS_DIR, "views_minimal.py"),
            [
                "Record buffering (list of bytes)",
                "Serialization (length-prefixed records)",
                "Tree pattern",
                "Commit pattern",
                "Read: walk tree, deserialize records",
            ],
        ),
        analyze_view(
            "GitView",
            os.path.join(VIEWS_DIR, "views_minimal.py"),
            [
                "File staging (dict path -> bytes)",
                "Tree pattern (inherit parent — Git semantics)",
                "Commit pattern (tree + parent + message)",
                "Read file by path",
                "History walk (commit parent chain)",
            ],
        ),
        analyze_view(
            "GraphView",
            os.path.join(VIEWS_DIR, "more_views.py"),
            [
                "Node/edge buffering",
                "Node serialization (JSON)",
                "Edge serialization (JSON)",
                "Adjacency index construction (View-level!)",
                "Tree pattern (inherit parent)",
                "Commit pattern",
                "Read node by ID",
                "Neighbors: read adjacency, then read each edge blob",
                "Traversal: BFS/DFS over neighbors",
            ],
        ),
        analyze_view(
            "MLView",
            os.path.join(VIEWS_DIR, "more_views.py"),
            [
                "Weights + metadata buffering",
                "Weights serialization (raw bytes)",
                "Metadata serialization (JSON)",
                "Tree pattern (inherit parent)",
                "Commit pattern",
                "Read weights by (model, step)",
                "Read metadata by (model, step)",
                "History: list all checkpoints for a model",
            ],
        ),
        analyze_view(
            "TimeSeriesView",
            os.path.join(VIEWS_DIR, "more_views.py"),
            [
                "Points buffering",
                "Serialization (struct.pack ts + float)",
                "Tree pattern (per-series segment indexing)",
                "Commit pattern",
                "Read series: walk segments, filter by time range",
                "Retention: rebuild tree without old segments",
            ],
        ),
        analyze_view(
            "OCIView",
            os.path.join(VIEWS_DIR, "more_views.py"),
            [
                "Layer push (raw bytes)",
                "Config push (JSON)",
                "Manifest construction (JSON with digests)",
                "Tree pattern (manifests by image:tag)",
                "Commit pattern (but parent is irrelevant — manifests independent)",
                "Pull manifest by (image, tag)",
                "Pull layer by digest",
            ],
        ),
    ]

    # Print analysis
    print(f"  {'View':<18} {'File lines':<12} {'Meaningful':<12} {'Responsibilities':<18} {'Irreducible est.':<18}")
    print(f"  {'-'*18} {'-'*12} {'-'*12} {'-'*18} {'-'*18}")

    for v in views:
        print(f"  {v['name']:<18} {v['total_lines']:<12} {v['meaningful_lines']:<12} "
              f"{len(v['responsibilities']):<18} {v['irreducible_estimate']:<18}")

    print()
    print("=" * 76)
    print("  Friction Analysis — per View")
    print("=" * 76)

    # Detailed friction analysis per View
    friction_findings = []

    for v in views:
        print()
        print(f"  --- {v['name']} ---")
        print(f"  Responsibilities ({len(v['responsibilities'])}):")
        for r in v['responsibilities']:
            print(f"    - {r}")

        # Classify each responsibility
        kernel_issues = []
        view_issues = []
        acceptable = []

        for r in v['responsibilities']:
            if "Tree pattern" in r or "Commit pattern" in r:
                # These are repeated in every View — is that friction?
                acceptable.append((r, "Repeated pattern, but each View customizes it. Could be a shared helper."))
            elif "Serialization" in r or "buffering" in r:
                view_issues.append((r, "View-specific. Inherent to the workload."))
            elif "Adjacency index" in r:
                kernel_issues.append((r, "GraphView builds its own adjacency index because the kernel has no index primitive. This is the biggest friction point."))
            elif "Linear scan" in r:
                kernel_issues.append((r, "VectorView does linear scan for ANN because the kernel has no index. Real vector DBs need HNSW/IVF."))
            elif "Read: walk tree" in r:
                view_issues.append((r, "View must walk the tree to find blobs. Could be a kernel helper, but Views want different walk semantics."))
            elif "History" in r or "walk" in r:
                kernel_issues.append((r, "O(N) history walk. Known issue (Finding 5a). Needs View-level skip pointers."))
            elif "Retention" in r:
                view_issues.append((r, "View rebuilds tree without old segments. Awkward but necessary — GC is a View concern."))
            elif "parent is irrelevant" in r:
                acceptable.append((r, "OCI manifests don't need history. The Commit pattern allows parent=None, so this works."))
            else:
                acceptable.append((r, "Standard View responsibility."))

        if kernel_issues:
            print(f"  KERNEL ISSUES ({len(kernel_issues)}):")
            for r, why in kernel_issues:
                print(f"    ⚠ {r}")
                print(f"      -> {why}")
                friction_findings.append((v['name'], "kernel", r, why))

        if view_issues:
            print(f"  VIEW ISSUES ({len(view_issues)}):")
            for r, why in view_issues:
                print(f"    • {r}")
                print(f"      -> {why}")

        if acceptable:
            print(f"  ACCEPTABLE ({len(acceptable)}):")
            for r, why in acceptable:
                print(f"    ✓ {r}")

    print()
    print("=" * 76)
    print("  View Compression Summary")
    print("=" * 76)
    print()

    # Estimate irreducible size
    print(f"  {'View':<18} {'Meaningful lines':<18} {'Est. irreducible':<18} {'Friction ratio':<15}")
    print(f"  {'-'*18} {'-'*18} {'-'*18} {'-'*15}")

    for v in views:
        # Irreducible estimate: ~15 lines per responsibility, minus the
        # repeated Tree/Commit patterns (which could be shared helpers)
        shared_patterns = sum(1 for r in v['responsibilities']
                              if "Tree pattern" in r or "Commit pattern" in r)
        unique_responsibilities = len(v['responsibilities']) - shared_patterns
        irreducible = unique_responsibilities * 15 + shared_patterns * 5  # shared = 5 lines each
        friction_ratio = v['meaningful_lines'] / irreducible if irreducible > 0 else 0

        print(f"  {v['name']:<18} {v['meaningful_lines']:<18} {irreducible:<18} {friction_ratio:.1f}x")

    print()
    print("  Interpretation:")
    print("  - 'Friction ratio' = actual meaningful lines / estimated irreducible")
    print("  - Ratio ~1.0x: View is close to minimal (good)")
    print("  - Ratio >2.0x: View has significant non-essential code (View issue)")
    print("  - Large irreducible estimate: workload is genuinely complex (acceptable)")
    print("    OR kernel is missing a primitive (kernel issue)")
    print()

    # Kernel findings
    kernel_findings = [f for f in friction_findings if f[1] == "kernel"]
    if kernel_findings:
        print("  KERNEL FINDINGS (friction caused by kernel):")
        for view, _, responsibility, why in kernel_findings:
            print(f"    - {view}: {responsibility}")
            print(f"      {why}")
        print()
        print("  These are candidates for kernel admission (must pass the 5-criterion rule).")
    else:
        print("  No kernel findings — all friction is View-level or acceptable.")


if __name__ == "__main__":
    main()
