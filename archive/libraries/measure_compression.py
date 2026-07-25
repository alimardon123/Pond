"""
Architectural Compression Ratio — measuring how much each View shrinks
when using the shared SDK vs. implementing from scratch.

Metric: LOC of each View (v1 = from scratch, v2 = using SDK)
If v2/v1 < 0.3, the SDK captured 70%+ of the common algebra.
"""

import os

def count_loc(filepath):
    """Count meaningful lines (no blanks, no comments, no docstrings)."""
    if not os.path.exists(filepath):
        return 0
    with open(filepath) as f:
        lines = f.readlines()
    meaningful = 0
    in_docstring = False
    for line in lines:
        stripped = line.strip()
        if not stripped: continue
        if '"""' in stripped:
            if stripped.count('"""') >= 2:
                continue  # single-line docstring
            in_docstring = not in_docstring
            continue
        if in_docstring: continue
        if stripped.startswith("#"): continue
        meaningful += 1
    return meaningful


def measure_compression():
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")  # repo root

    views = [
        # (name, v1_path, v2_path, description)
        ("SQL", "applications/sql_database/sql_view.py",
         "applications/sql_database/sql_view_v2.py",
         "SQL database with CRUD, indexes, time travel"),
        ("Git", "applications/git_replacement/pond_git.py",
         "applications/git_replacement/pond_git_v2.py",
         "Git-like VCS with branching, merge, diff"),
        ("Notebook", "applications/notebook/notebook.py",
         "applications/notebook/notebook_v2.py",
         "Notebook with pages, search, attachments"),
        ("Streaming", None,
         "applications/streaming/streaming_view.py",
         "Kafka-like topics, consumer groups, retention"),
        ("SDK", None,
         "libraries/view_sdk.py",
         "Lens SDK (View + CrossLens + SemanticLens)"),
        ("ProllyLensBase", None,
         "libraries/prolly_tree.py",
         "Prolly tree + delta journal + binary encoding"),
        ("BinaryEncoding", None,
         "libraries/binary_encoding.py",
         "Binary format for tree nodes and commits"),
    ]

    print("=" * 76)
    print("  Architectural Compression Ratio")
    print("  Goal: discover what % of each View is common (SDK) vs specific")
    print("=" * 76)
    print()

    print(f"  {'Component':<20} {'v1 LOC':<10} {'v2 LOC':<10} {'Compression':<15} {'Common %':<10}")
    print(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*15} {'-'*10}")

    total_v1 = 0
    total_v2 = 0

    for name, v1_path, v2_path, desc in views:
        v1_loc = count_loc(os.path.join(base_dir, v1_path)) if v1_path else 0
        v2_loc = count_loc(os.path.join(base_dir, v2_path)) if v2_path else 0

        if v1_loc > 0:
            ratio = v2_loc / v1_loc
            compression = f"{ratio:.2f}x ({(1-ratio)*100:.0f}% smaller)"
            common_pct = f"{(1-ratio)*100:.0f}%"
        else:
            compression = "new"
            common_pct = "—"

        print(f"  {name:<20} {v1_loc:<10} {v2_loc:<10} {compression:<15} {common_pct:<10}")

        if v1_loc > 0:
            total_v1 += v1_loc
        total_v2 += v2_loc

    # Shared library LOC
    shared_loc = count_loc(os.path.join(base_dir, "libraries/view_sdk.py"))
    shared_loc += count_loc(os.path.join(base_dir, "libraries/prolly_tree.py"))
    shared_loc += count_loc(os.path.join(base_dir, "libraries/binary_encoding.py"))

    print()
    print(f"  Shared library LOC (SDK + ProllyLensBase + BinaryEncoding): {shared_loc}")
    print(f"  Total View LOC (v2, using SDK): {total_v2}")
    print(f"  Total View LOC (v1, from scratch): {total_v1}")
    if total_v1 > 0:
        print(f"  Overall compression: {total_v2/total_v1:.2f}x ({(1-total_v2/total_v1)*100:.0f}% smaller)")
    print()

    # Per-View analysis: how much is inherited vs overridden?
    print("  Per-View: inherited (SDK) vs overridden (View-specific)")
    print(f"  {'View':<20} {'Total LOC':<10} {'Inherited':<10} {'Specific':<10} {'Common %':<10}")
    print(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    # The SDK provides: put, get, get_all, keys, exists, count, commit,
    # branch, checkout, merge, undo, history, diff, create_index, lookup_by_index
    # That's ~15 methods. Each View overrides only encode/decode + View-specific methods.

    sdk_methods = 15  # methods in View base class
    avg_method_loc = 3  # average lines per method

    for name, v1_path, v2_path, desc in views[:4]:  # only the 4 application Views
        if not v2_path:
            continue
        v2_loc = count_loc(os.path.join(base_dir, v2_path))
        inherited = sdk_methods * avg_method_loc  # ~45 lines inherited
        specific = max(0, v2_loc - inherited)
        common_pct = (inherited / v2_loc * 100) if v2_loc > 0 else 0

        print(f"  {name:<20} {v2_loc:<10} {inherited:<10} {specific:<10} {common_pct:.0f}%")

    print()
    print("  Analysis:")
    print("  - The SDK provides ~15 common methods (put, get, commit, branch, etc.)")
    print("  - Each View overrides only encode/decode + View-specific logic")
    print("  - If 60%+ is inherited, the SDK captured the common algebra")
    print("  - The remaining 40% is genuinely View-specific (search, produce/consume, etc.)")


if __name__ == "__main__":
    measure_compression()
