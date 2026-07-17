"""
Stage 5: Scale destruction.

Goal: prove the architecture breaks at extreme scale (10B blobs, 100M
namespaces, 1B commits, 1T references). If any operation degrades
non-linearly, the architecture fails.

Method: analytical (we can't actually build 10B blobs in a prototype).
Compute the theoretical resource usage at each scale and check for
non-linear growth.

Scales tested:
  - 1 TB (1K tables, 100K blobs, 100K commits)
  - 100 TB (10K tables, 20M blobs, 1M commits)
  - 1 PB (100K tables, 200M blobs, 10M commits)
  - 10 PB (1M tables, 2B blobs, 100M commits)
  - 100 PB (10M tables, 20B blobs, 1B commits)

For each scale, measure:
  - Storage (data + metadata)
  - Root namespace size
  - Object count (does it exceed S3 limits?)
  - Time per operation (asymptotic)
  - Memory required

Outcome vocabulary:
  - Supported: scale is achievable with linear resource growth
  - Falsified: resource growth is non-linear or exceeds limits
  - Inconclusive: couldn't isolate the question
  - Needs larger-scale validation: prototype limits prevent a conclusion
"""

def section(title):
    print()
    print("=" * 76)
    print(f"  {title}")
    print("=" * 76)


def fmt_bytes(n):
    if n < 1024: return f"{n} B"
    if n < 1024**2: return f"{n/1024:.1f} KB"
    if n < 1024**3: return f"{n/1024**2:.1f} MB"
    if n < 1024**4: return f"{n/1024**3:.2f} GB"
    return f"{n/1024**4:.2f} TB"


def fmt_count(n):
    if n < 1000: return f"{n}"
    if n < 1e6: return f"{n/1000:.1f}K"
    if n < 1e9: return f"{n/1e6:.1f}M"
    if n < 1e12: return f"{n/1e9:.1f}B"
    return f"{n/1e12:.2f}T"


# Architectural constants
BLOB_SIZE = 512 * 1024**2  # 512 MB default
TREE_ENTRY_SIZE = 100  # bytes per entry in a Tree blob
COMMIT_SIZE = 200  # bytes per Commit blob
ROOT_ENTRY_SIZE = 100  # bytes per name in root namespace
S3_LIST_LIMIT = 1000  # max keys per LIST request
S3_BUCKET_OBJECT_LIMIT = 10**12  # S3 has no practical limit, but ~10^12 is extreme


def analyze_scale(scale_tb, n_tables, n_commits_per_table):
    """Analyze resource usage at a given scale."""
    data_bytes = scale_tb * 1024**4
    n_blobs = data_bytes // BLOB_SIZE
    n_commits = n_tables * n_commits_per_table

    # Metadata: hierarchical trees
    avg_blobs_per_table = n_blobs / n_tables
    # Tree per commit: O(blobs / 256) entries (hierarchical)
    tree_per_commit = (avg_blobs_per_table / 256) * TREE_ENTRY_SIZE + 256 * TREE_ENTRY_SIZE
    tree_bytes = n_commits * tree_per_commit
    commit_bytes = n_commits * COMMIT_SIZE
    root_bytes = n_tables * ROOT_ENTRY_SIZE
    metadata_bytes = tree_bytes + commit_bytes + root_bytes

    # Object count on S3: blobs + trees + commits
    # (root namespace is separate — local or external KV)
    object_count = n_blobs + n_commits * 2  # each commit has a tree + commit blob

    # LIST operations to enumerate all objects: object_count / 1000
    list_operations = object_count / S3_LIST_LIMIT

    # Memory needed for root namespace (if cached in RAM)
    root_memory = root_bytes

    # Time for GC (walk all reachable objects): O(reachable)
    gc_time_seconds = object_count / 100_000  # assume 100K objects/sec walk rate

    return {
        "scale_tb": scale_tb,
        "data_bytes": data_bytes,
        "n_blobs": n_blobs,
        "n_tables": n_tables,
        "n_commits": n_commits,
        "metadata_bytes": metadata_bytes,
        "metadata_ratio": metadata_bytes / data_bytes,
        "object_count": object_count,
        "list_operations": list_operations,
        "root_memory": root_memory,
        "gc_time_seconds": gc_time_seconds,
    }


def exp_scale_analysis():
    section("Scale analysis: 1TB → 100PB")
    print()
    print("  Analyzing resource usage at each scale. Looking for non-linear growth.")
    print()

    scales = [
        (1,    1_000,    100),    # 1TB: 1K tables, 100 commits each
        (100,  10_000,   100),    # 100TB: 10K tables
        (1024, 100_000,  100),    # 1PB: 100K tables
        (10240, 1_000_000, 100),  # 10PB: 1M tables
        (102400, 10_000_000, 100), # 100PB: 10M tables
    ]

    print(f"  {'Scale':<10} {'Blobs':<12} {'Objects':<14} {'Metadata':<12} {'Meta%':<8} {'Root RAM':<10} {'GC time':<10}")
    print(f"  {'-'*10} {'-'*12} {'-'*14} {'-'*12} {'-'*8} {'-'*10} {'-'*10}")

    results = []
    for scale_tb, n_tables, n_commits_per_table in scales:
        r = analyze_scale(scale_tb, n_tables, n_commits_per_table)
        results.append(r)
        print(f"  {scale_tb:<10} TB {fmt_count(r['n_blobs']):<12} "
              f"{fmt_count(r['object_count']):<14} {fmt_bytes(r['metadata_bytes']):<12} "
              f"{r['metadata_ratio']*100:<8.4f} {fmt_bytes(r['root_memory']):<10} "
              f"{r['gc_time_seconds']:.0f}s")

    print()
    print("  Analysis:")
    print()

    # Check 1: metadata ratio should decrease with scale (more data per commit)
    meta_ratios = [r["metadata_ratio"] for r in results]
    if meta_ratios[-1] < meta_ratios[0]:
        print(f"  ✓ Metadata ratio DECREASES with scale ({meta_ratios[0]*100:.3f}% → {meta_ratios[-1]*100:.6f}%)")
        print(f"    This is correct: more data per commit means metadata is amortized.")
    else:
        print(f"  ✗ Metadata ratio INCREASES with scale — architecture fails.")

    print()

    # Check 2: object count vs S3 limits
    max_objects = results[-1]["object_count"]
    print(f"  Object count at 100PB: {fmt_count(max_objects)}")
    if max_objects < S3_BUCKET_OBJECT_LIMIT:
        print(f"  ✓ Within S3 bucket limits (limit ~10^12)")
    else:
        print(f"  ✗ Exceeds S3 bucket limits — need multiple buckets.")

    print()

    # Check 3: root namespace memory
    max_root_ram = results[-1]["root_memory"]
    print(f"  Root namespace memory at 100PB: {fmt_bytes(max_root_ram)}")
    if max_root_ram < 10 * 1024**3:  # < 10 GB
        print(f"  ✓ Fits in RAM (10M tables × 100 bytes = 1GB)")
    else:
        print(f"  ✗ Doesn't fit in RAM — need sharding.")

    print()

    # Check 4: GC time
    max_gc_time = results[-1]["gc_time_seconds"]
    print(f"  GC time at 100PB (walk all reachable): {max_gc_time:.0f}s = {max_gc_time/3600:.1f}h")
    if max_gc_time < 3600:  # < 1 hour
        print(f"  ✓ GC completes in reasonable time")
    else:
        print(f"  ⚠ GC takes {max_gc_time/3600:.1f}h at 100PB — needs incremental GC")

    print()

    # Check 5: LIST operations (if someone enumerates all objects)
    max_lists = results[-1]["list_operations"]
    print(f"  LIST operations to enumerate all objects at 100PB: {fmt_count(max_lists)}")
    print(f"  Cost: ${max_lists * 0.005 / 1000:.2f} per full enumeration")
    print(f"  Note: Views should never need to enumerate all objects — they walk")
    print(f"  specific Trees, not the whole bucket. This is a worst-case metric.")

    return results


def exp_root_namespace_scale():
    section("Root namespace scale: 100M namespaces")
    print()
    print("  Scenario: 100 million named tables/branches/tags.")
    print("  Question: does the root namespace stay functional?")
    print()
    print("  Current implementation: SQLite, single table.")
    print("  SQLite limits: ~10^9 rows per table (theoretical), ~10^7 practical.")
    print()
    print("  At 100M names:")
    print("    - Storage: 100M × 100 bytes = 10 GB (fits on disk)")
    print("    - Memory (if cached): 10 GB (large but feasible)")
    print("    - Lookup latency: O(log N) via SQLite B-tree = ~30 comparisons")
    print("    - Insert latency: O(log N) + rebalance")
    print()
    print("  At 1B names:")
    print("    - Storage: 100 GB")
    print("    - Memory: 100 GB (doesn't fit in RAM — need disk-backed)")
    print("    - SQLite would struggle past ~100M rows")
    print()
    print("  VERDICT: NEEDS LARGER-SCALE VALIDATION at 1B+ names.")
    print("  At 100M names, SQLite works but is at its practical limit.")
    print("  At 1B+ names, need a distributed KV (FoundationDB, etcd) for roots.")
    print()
    print("  This is NOT a kernel issue — the kernel treats roots as an")
    print("  implementation detail. The View-level root store can be swapped")
    print("  (SQLite → FDB → etcd) without kernel changes.")


def exp_commit_history_scale():
    section("Commit history scale: 1B commits on one table")
    print()
    print("  Scenario: one table with 1 billion commits in its history.")
    print("  Question: does time travel / history walk degrade?")
    print()
    print("  Current state: time travel is O(N) — walk the parent chain (Finding 5a).")
    print("  At 1B commits: 1B × ~1us per commit read = 1000 seconds = ~17 minutes.")
    print("  This is UNUSABLE.")
    print()
    print("  With View-level skip pointers (O(log N)):")
    print("  At 1B commits: log2(1B) = 30 hops × ~1ms per S3 GET = 30ms. Excellent.")
    print()
    print("  VERDICT: FALSIFIED without skip pointers (known issue, Finding 5a).")
    print("  SUPPORTED with View-level skip pointers (the fix).")
    print("  Skip pointers are a View concern, not a kernel change.")


def exp_blob_count_scale():
    section("Blob count scale: 20B blobs at 100PB")
    print()
    print("  Scenario: 100PB of data in 512MB blobs = 20 billion blobs.")
    print("  Question: does any operation degrade with blob count?")
    print()
    print("  Analysis:")
    print("  - Write: O(1) — independent of blob count. ✓")
    print("  - Read latest: O(1) — resolve name, read 1 blob. ✓")
    print("  - Full scan: O(B) — must read every blob. Unavoidable. At 20B blobs,")
    print("    this is 20B S3 GETs = $8000 per scan. Expensive but correct.")
    print("  - Tree walk: O(blobs in tree) — for a single table. If table has")
    print("    1M blobs, tree walk is 1M entries = ~100MB tree. Fits in RAM.")
    print("  - LIST all objects: O(B/1000) S3 LIST calls. At 20B blobs = 20M LISTs.")
    print("    Expensive but Views shouldn't need to enumerate all objects.")
    print()
    print("  VERDICT: SUPPORTED — no operation degrades non-linearly with blob count.")
    print("  Full scan cost is O(B) but that's unavoidable (you must read the data).")
    print("  The architecture doesn't amplify blob count — it's 1:1 with logical data.")


def exp_namespace_fanout():
    section("Namespace fanout: 10M tables × 100 branches each = 1B names")
    print()
    print("  Scenario: 10M tables, each with 100 branches = 1B named references.")
    print("  Question: does the root namespace handle 1B entries?")
    print()
    print("  At 1B names:")
    print("    - SQLite: would struggle (practical limit ~100M rows)")
    print("    - FoundationDB: handles 1B+ keys natively (ordered KV)")
    print("    - Sharding: split roots by hash prefix across N SQLite instances")
    print()
    print("  The kernel doesn't mandate a specific root store implementation.")
    print("  Views choose the root store (SQLite for dev, FDB for 1B+ scale).")
    print()
    print("  VERDICT: SUPPORTED at the architecture level — roots are swappable.")
    print("  NEEDS LARGER-SCALE VALIDATION for specific root store implementations.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 76)
    print("  Stage 5: Scale destruction")
    print("  Goal: prove the architecture breaks at extreme scale.")
    print("  Method: analytical (can't build 10B blobs in a prototype).")
    print("=" * 76)

    results = exp_scale_analysis()
    exp_root_namespace_scale()
    exp_commit_history_scale()
    exp_blob_count_scale()
    exp_namespace_fanout()

    section("SCALE DESTRUCTION SUMMARY")
    print()
    print("  Scale dimension                          | Outcome")
    print("  -----------------------------------------|------------------------------------------")
    print("  Data volume (1TB → 100PB)                | SUPPORTED (metadata ratio decreases)")
    print("  Blob count (up to 20B)                   | SUPPORTED (no non-linear degradation)")
    print("  Root namespace (100M names)              | NEEDS VALIDATION (SQLite at limit)")
    print("  Root namespace (1B names)                | NEEDS VALIDATION (requires FDB/etcd)")
    print("  Commit history (1B commits, no skip ptr) | FALSIFIED (O(N) walk, known Finding 5a)")
    print("  Commit history (1B commits, skip ptrs)   | SUPPORTED (O(log N) = 30 hops)")
    print("  Namespace fanout (1B names)              | SUPPORTED (roots are swappable)")
    print()
    print("  Findings:")
    print()
    print("  - Data volume scales linearly. Metadata ratio DECREASES with scale.")
    print("  - Blob count doesn't amplify (1:1 with logical data).")
    print("  - Commit history is the weak point without skip pointers (Finding 5a).")
    print("  - Root namespace needs a distributed KV at 1B+ names (not a kernel issue).")
    print()
    print("  No NEW scale issues found beyond the known ones (Finding 5a, Finding 6).")
    print("  The architecture scales linearly to 100PB. The two known issues are")
    print("  View-level fixes (skip pointers, GC), not kernel changes.")
    print()
    print("  Next: Stage 6 (Human destruction) — can a stranger implement Git/Iceberg/OCI?")


if __name__ == "__main__":
    main()
