"""Comprehensive performance benchmark — ALL Pond workloads.

Reports wall-clock time, GET/PUT counts, and throughput for every
operation Pond supports. Run on LocalFS (the dev/test backend).

Workloads:
  1. Bulk write (1000, 10K, 100K rows)
  2. Append shard (single-writer warm, multi-writer concurrent)
  3. Point lookup (cold, warm, at scale)
  4. Range scan (full, predicate-pruned at 1%/10%/50% selectivity)
  5. Branch + merge
  6. Time travel (read old commit)
  7. ACID transaction (1, 2, 5 collections)
  8. Compaction (manifest-level, row-level)
  9. GC + vacuum
  10. Concurrent readers during writes
  11. Streaming (write_stream / read_stream)
  12. KV (get / put / keys)
  13. Vector (insert / search)

Run:
  python scripts/benchmark_full.py
"""
import os, sys, time, tempfile, shutil, threading, statistics

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, os.path.join(REPO, "pond-core"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk"))
sys.path.insert(0, os.path.join(REPO, "pond-sdk", "extensions", "physical_structures"))
sys.path.insert(0, os.path.join(REPO, "lenses", "keyvalue"))
sys.path.insert(0, os.path.join(REPO, "lenses", "streaming"))
sys.path.insert(0, os.path.join(REPO, "lenses", "vector"))


def _make_kernel(tmpdir):
    from local_fs_object_store import LocalFSObjectStore
    from object_store_native_kernel import ObjectStoreNativeKernel
    store = LocalFSObjectStore(tmpdir)
    return ObjectStoreNativeKernel(store), store


def _reset(kernel, store):
    kernel.reset_stats()
    store.reset_stats()
    kernel._root_ref_cache = None
    kernel._root_ref_hash = None


def _stats(kernel, store):
    return {
        "gets": store.stats["gets"],
        "puts": store.stats["puts"],
        "bytes_read": store.stats["bytes_read"],
        "bytes_written": store.stats["bytes_written"],
    }


def _ms(t):
    return f"{t * 1000:.2f}ms"


def _fmt_bytes(n):
    if n < 1024:
        return f"{n}B"
    elif n < 1024 * 1024:
        return f"{n / 1024:.1f}KB"
    elif n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f}MB"
    else:
        return f"{n / (1024 * 1024 * 1024):.2f}GB"


def bench_bulk_write():
    """1. Bulk write at 3 scales."""
    print("\n--- 1. Bulk Write ---")
    print(f"  {'Scale':<12} {'Time':>10} {'Rows/s':>12} {'PUTs':>8} {'Written':>10}")
    print(f"  {'-'*12} {'-'*10} {'-'*12} {'-'*8} {'-'*10}")

    for n_rows in [1000, 10000, 100000]:
        tmpdir = tempfile.mkdtemp(prefix=f"pond_bw_{n_rows}_")
        try:
            kernel, store = _make_kernel(tmpdir)
            from pond_storage import PondStorage
            s = PondStorage(kernel)

            rows = [{"id": i, "name": f"user_{i}", "age": i % 100} for i in range(n_rows)]
            _reset(kernel, store)
            t0 = time.perf_counter()
            s.write("bench", rows, key_col="id", row_group_size=1000)
            elapsed = time.perf_counter() - t0
            st = _stats(kernel, store)

            print(f"  {n_rows:<12} {_ms(elapsed):>10} {n_rows/elapsed:>12.0f} {st['puts']:>8} {_fmt_bytes(st['bytes_written']):>10}")
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception as e:
            print(f"  {n_rows:<12} FAIL: {e}")
            shutil.rmtree(tmpdir, ignore_errors=True)


def bench_append_shard():
    """2. Append shard (single-writer warm, multi-writer concurrent)."""
    print("\n--- 2. Append Shard ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_append_")
    try:
        kernel, store = _make_kernel(tmpdir)
        from pond_storage import PondStorage
        s = PondStorage(kernel)
        s.write("bench", [{"id": 0, "v": "init"}], key_col="id")

        # Warm appends (cached)
        N = 100
        _reset(kernel, store)
        t0 = time.perf_counter()
        for i in range(N):
            s.append_shard("bench", [{"id": i + 1, "v": f"v{i}"}], key_col="id")
        elapsed = time.perf_counter() - t0
        st = _stats(kernel, store)
        print(f"  Warm appends ({N}):     {_ms(elapsed)} total, {_ms(elapsed/N)}/op, {N/elapsed:.0f} ops/s, {st['puts']} PUTs")

        # Concurrent appends (5 writers × 20 appends)
        N_WRITERS = 5
        APPENDS_PER = 20
        errors = []
        _reset(kernel, store)

        def writer(wid):
            try:
                ws = PondStorage(kernel)
                for i in range(APPENDS_PER):
                    ws.append_shard("bench",
                        [{"id": wid * 1000 + i + 1, "v": f"w{wid}_{i}"}],
                        key_col="id")
            except Exception as e:
                errors.append(e)

        t0 = time.perf_counter()
        threads = [threading.Thread(target=writer, args=(w,)) for w in range(N_WRITERS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.perf_counter() - t0
        st = _stats(kernel, store)
        total_appends = N_WRITERS * APPENDS_PER

        rows = s.read_with_shards("bench")
        print(f"  Concurrent ({N_WRITERS}w×{APPENDS_PER}): {_ms(elapsed)} total, {_ms(elapsed/total_appends)}/op, {total_appends/elapsed:.0f} ops/s, {len(rows)} rows merged, {len(errors)} errors")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_point_lookup():
    """3. Point lookup (cold, warm, at scale)."""
    print("\n--- 3. Point Lookup ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_pl_")
    try:
        kernel, store = _make_kernel(tmpdir)
        from pond_storage import PondStorage
        s = PondStorage(kernel)
        s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(10000)],
                key_col="id", row_group_size=1000)

        # Cold lookup
        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        s._unified._head_cache.clear()
        t0 = time.perf_counter()
        row = s.point_lookup("bench", key="5000")
        cold = time.perf_counter() - t0
        st_cold = _stats(kernel, store)

        # Warm lookup (cached)
        _reset(kernel, store)
        t0 = time.perf_counter()
        row = s.point_lookup("bench", key="5001")
        warm = time.perf_counter() - t0
        st_warm = _stats(kernel, store)

        # Batch of 100 lookups (warm)
        _reset(kernel, store)
        t0 = time.perf_counter()
        for i in range(100):
            s.point_lookup("bench", key=str(i * 100))
        batch = time.perf_counter() - t0
        st_batch = _stats(kernel, store)

        print(f"  Cold lookup:       {_ms(cold)}, {st_cold['gets']} GETs")
        print(f"  Warm lookup:       {_ms(warm)}, {st_warm['gets']} GETs")
        print(f"  100 lookups:       {_ms(batch)}, {_ms(batch/100)}/op, {100/batch:.0f} ops/s, {st_batch['gets']} total GETs")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_range_scan():
    """4. Range scan (full, predicate-pruned at 1%/10%/50% selectivity)."""
    print("\n--- 4. Range Scan ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_rs_")
    try:
        kernel, store = _make_kernel(tmpdir)
        from pond_storage import PondStorage
        s = PondStorage(kernel)
        s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(10000)],
                key_col="id", row_group_size=100)

        # Full scan
        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        t0 = time.perf_counter()
        rows = s.read("bench")
        full = time.perf_counter() - t0
        st = _stats(kernel, store)
        print(f"  Full scan (10000 rows): {_ms(full)}, {len(rows)} rows, {st['gets']} GETs, {_fmt_bytes(st['bytes_read'])} read")

        # Pruned reads at different selectivities
        print(f"  {'Selectivity':<14} {'Time':>10} {'Rows':>8} {'GETs':>8} {'Read':>10}")
        print(f"  {'-'*14} {'-'*10} {'-'*8} {'-'*8} {'-'*10}")
        for label, pred, expected in [
            ("1% (id>9900)", ("id", ">", 9900), 99),
            ("10% (id>9000)", ("id", ">", 9000), 999),
            ("50% (id>5000)", ("id", ">", 5000), 4999),
        ]:
            _reset(kernel, store)
            s._unified._manifest_cache.clear()
            t0 = time.perf_counter()
            rows = s.read("bench", predicates=[pred])
            elapsed = time.perf_counter() - t0
            st = _stats(kernel, store)
            ok = "✓" if len(rows) == expected else f"✗({len(rows)})"
            print(f"  {label:<14} {_ms(elapsed):>10} {len(rows):>8} {st['gets']:>8} {_fmt_bytes(st['bytes_read']):>10} {ok}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_branch_merge():
    """5. Branch + merge."""
    print("\n--- 5. Branch + Merge ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_bm_")
    try:
        kernel, store = _make_kernel(tmpdir)
        from pond_storage import PondStorage
        s = PondStorage(kernel)
        s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(1000)],
                key_col="id", row_group_size=100)

        # Branch
        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        t0 = time.perf_counter()
        s.branch("bench", "dev")
        branch_t = time.perf_counter() - t0
        st = _stats(kernel, store)
        print(f"  Branch:        {_ms(branch_t)}, {st['puts']} PUTs (O(1) ref copy)")

        # Append on dev
        s.checkout("bench", "dev")
        s.append_shard("bench", [{"id": 1000 + i, "v": f"dev{i}"} for i in range(100)],
                        key_col="id", row_group_size=100)

        # Merge
        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        t0 = time.perf_counter()
        s.merge("bench", "dev")
        merge_t = time.perf_counter() - t0
        st = _stats(kernel, store)
        rows = s.read("bench")
        print(f"  Merge:         {_ms(merge_t)}, {st['puts']} PUTs, {st['gets']} GETs, {len(rows)} rows after merge")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_time_travel():
    """6. Time travel (read old commit)."""
    print("\n--- 6. Time Travel ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_tt_")
    try:
        kernel, store = _make_kernel(tmpdir)
        from pond_storage import PondStorage
        s = PondStorage(kernel)

        # Write version 1
        s.write("tt", [{"id": i, "v": 1} for i in range(100)], key_col="id", row_group_size=100)
        v1_manifest = kernel.resolve("collections/tt/branches/main/manifest")

        # Append version 2
        s.append("tt", [{"id": 100 + i, "v": 2} for i in range(100)], key_col="id")

        # Read current (v2)
        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        t0 = time.perf_counter()
        rows_current = s.read("tt")
        current_t = time.perf_counter() - t0

        # Time travel to v1 (via UnifiedStorage directly — manifest_hash)
        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        t0 = time.perf_counter()
        rows_v1 = s._unified.read("tt", manifest_hash=v1_manifest)
        tt_t = time.perf_counter() - t0
        st = _stats(kernel, store)

        print(f"  Current (v2, 200 rows):  {_ms(current_t)}")
        print(f"  Time travel (v1, 100 rows): {_ms(tt_t)}, {st['gets']} GETs, {len(rows_v1)} rows")
        ok = "✓" if len(rows_current) == 200 and len(rows_v1) == 100 else "✗"
        print(f"  Correctness: {ok}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_acid():
    """7. ACID transaction (1, 2, 5 collections)."""
    print("\n--- 7. ACID Transactions ---")
    print(f"  {'Collections':<14} {'Per tx':>10} {'Per coll':>10} {'Tx/s':>10} {'PUTs/tx':>10}")
    print(f"  {'-'*14} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")

    for n_coll in [1, 2, 5]:
        tmpdir = tempfile.mkdtemp(prefix=f"pond_acid_{n_coll}_")
        try:
            kernel, store = _make_kernel(tmpdir)
            from pond_storage import PondStorage
            s = PondStorage(kernel)
            for c in range(n_coll):
                s.write(f"coll{c}", [{"id": 0, "v": "init"}], key_col="id")

            N = 50
            _reset(kernel, store)
            t0 = time.perf_counter()
            for i in range(N):
                tx = s.begin_tx()
                for c in range(n_coll):
                    s.append_shard(f"coll{c}", [{"id": i + 1, "v": f"v{i}"}],
                                    key_col="id", tx_id=tx)
                s.commit_tx(tx)
            elapsed = time.perf_counter() - t0
            st = _stats(kernel, store)

            print(f"  {n_coll:<14} {_ms(elapsed/N):>10} {_ms(elapsed/(N*n_coll)):>10} {N/elapsed:>10.0f} {st['puts']//N:>10}")
            shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception as e:
            print(f"  {n_coll:<14} FAIL: {e}")
            shutil.rmtree(tmpdir, ignore_errors=True)


def bench_compaction():
    """8. Compaction (manifest-level, row-level)."""
    print("\n--- 8. Compaction ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_compact_")
    try:
        kernel, store = _make_kernel(tmpdir)
        from pond_storage import PondStorage
        s = PondStorage(kernel)

        # Manifest-level (insert-only)
        s.write("insert_only", [{"id": i, "v": f"v{i}"} for i in range(1000)],
                key_col="id", row_group_size=100)
        for i in range(5):
            s.append_shard("insert_only",
                [{"id": 1000 + i * 100 + j, "v": f"s{i}_{j}"} for j in range(100)],
                key_col="id", row_group_size=100)

        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        s._unified._head_cache.clear()
        t0 = time.perf_counter()
        s.compact_shards("insert_only")
        manifest_t = time.perf_counter() - t0
        st = _stats(kernel, store)
        print(f"  Manifest-level (5 shards, 1500 rows): {_ms(manifest_t)}, {st['gets']} GETs (zero data blob I/O)")

        # Row-level (upsert)
        s.write("upsert_test", [{"id": i, "v": f"v{i}"} for i in range(100)],
                key_col="id", row_group_size=100)
        rows = s.read_with_shards("upsert_test")
        s.upsert_shard("upsert_test", rows, key_col="id", row_group_size=100)

        _reset(kernel, store)
        s._unified._manifest_cache.clear()
        s._unified._head_cache.clear()
        t0 = time.perf_counter()
        s.compact_shards("upsert_test")
        row_t = time.perf_counter() - t0
        st = _stats(kernel, store)
        print(f"  Row-level (1 upsert shard, 100 rows):  {_ms(row_t)}, {st['gets']} GETs (decodes data for CRDT merge)")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_gc_vacuum():
    """9. GC + vacuum."""
    print("\n--- 9. GC + Vacuum ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_gc_")
    try:
        kernel, store = _make_kernel(tmpdir)
        from pond_storage import PondStorage
        s = PondStorage(kernel)
        s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(1000)],
                key_col="id", row_group_size=100)

        # Create unreachable blobs via append + compact
        for i in range(3):
            s.append_shard("bench", [{"id": 1000 + i * 100 + j, "v": f"s{i}_{j}"} for j in range(100)],
                            key_col="id", row_group_size=100)
        s.compact_shards("bench")

        blobs_before = len(store.list_all_blob_hashes())

        _reset(kernel, store)
        t0 = time.perf_counter()
        gc_stats = s.gc()
        gc_t = time.perf_counter() - t0

        s.vacuum()
        vacuum_t = time.perf_counter() - t0
        blobs_after = len(store.list_all_blob_hashes())

        print(f"  GC:           {_ms(gc_t)}, {gc_stats.get('dead', 0)} dead blobs found")
        print(f"  Vacuum:       {_ms(vacuum_t)} (total with GC)")
        print(f"  Blobs:        {blobs_before} → {blobs_after} (reclaimed {blobs_before - blobs_after})")

        # Verify data still intact
        rows = s.read("bench")
        ok = "✓" if len(rows) == 1300 else f"✗ ({len(rows)})"
        print(f"  Data intact:  {ok} (1300 rows)")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_concurrent_reads():
    """10. Concurrent readers during writes."""
    print("\n--- 10. Concurrent Reads During Writes ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_cr_")
    try:
        kernel, store = _make_kernel(tmpdir)
        from pond_storage import PondStorage
        s = PondStorage(kernel)
        s.write("bench", [{"id": i, "v": f"v{i}"} for i in range(100)],
                key_col="id", row_group_size=100)

        read_counts = []
        write_done = threading.Event()
        errors = []

        def reader():
            try:
                rs = PondStorage(kernel)
                count = 0
                while not write_done.is_set():
                    rows = rs.read_with_shards("bench")
                    count += 1
                read_counts.append(count)
            except Exception as e:
                errors.append(e)

        # 1 writer, 5 readers for 2 seconds
        readers = [threading.Thread(target=reader) for _ in range(5)]
        for t in readers:
            t.start()

        t0 = time.perf_counter()
        for i in range(50):
            s.append_shard("bench", [{"id": 100 + i, "v": f"new{i}"}], key_col="id")
            time.sleep(0.01)  # 10ms between appends
        write_done.set()
        for t in readers:
            t.join()
        elapsed = time.perf_counter() - t0

        total_reads = sum(read_counts)
        print(f"  50 appends + 5 concurrent readers ({_ms(elapsed)} total):")
        print(f"    Writes:      50 appends")
        print(f"    Reads:       {total_reads} total ({total_reads/elapsed:.0f} reads/s)")
        print(f"    Errors:      {len(errors)}")
        ok = "✓" if not errors else f"✗ ({len(errors)} errors)"
        print(f"    Correctness: {ok}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_streaming():
    """11. Streaming (write_stream / read_stream)."""
    print("\n--- 11. Streaming ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_str_")
    try:
        from streaming_lens import StreamingLens
        kernel, store = _make_kernel(tmpdir)
        lens = StreamingLens(kernel)

        # Write stream
        data = b"x" * (1024 * 100)  # 100KB
        _reset(kernel, store)
        t0 = time.perf_counter()
        lens.write_stream("stream1", data, segment_size=1024 * 10)  # 10KB segments
        write_t = time.perf_counter() - t0
        st = _stats(kernel, store)

        # Read stream
        _reset(kernel, store)
        t0 = time.perf_counter()
        read_data = lens.read_stream("stream1")
        read_t = time.perf_counter() - t0
        st_r = _stats(kernel, store)

        ok = "✓" if read_data == data else "✗"
        print(f"  Write 100KB stream: {_ms(write_t)}, {st['puts']} PUTs")
        print(f"  Read 100KB stream:  {_ms(read_t)}, {st_r['gets']} GETs")
        print(f"  Correctness:        {ok}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_keyvalue():
    """12. KV (get / put / keys)."""
    print("\n--- 12. KeyValue ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_kv_")
    try:
        from keyvalue_lens import KeyValueLens
        kernel, store = _make_kernel(tmpdir)
        lens = KeyValueLens(kernel, "kv")

        # Put 1000 keys
        _reset(kernel, store)
        t0 = time.perf_counter()
        for i in range(1000):
            lens.put(f"key{i:04d}", {"value": f"val{i}"})
        lens.commit("1000 keys")
        put_t = time.perf_counter() - t0
        st = _stats(kernel, store)

        # Get 100 keys (warm)
        _reset(kernel, store)
        t0 = time.perf_counter()
        for i in range(100):
            v = lens.get(f"key{i:05d}" if i < 10 else f"key{i:04d}")
        get_t = time.perf_counter() - t0
        st_g = _stats(kernel, store)

        keys = lens.keys()
        print(f"  Put 1000 keys:  {_ms(put_t)}, {1000/put_t:.0f} puts/s, {st['puts']} PUTs")
        print(f"  Get 100 keys:   {_ms(get_t)}, {100/get_t:.0f} gets/s, {st_g['gets']} GETs")
        print(f"  Total keys:     {len(keys)}")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def bench_vector():
    """13. Vector (insert / search)."""
    print("\n--- 13. Vector ---")
    tmpdir = tempfile.mkdtemp(prefix="pond_vec_")
    try:
        from vector_lens import VectorLens
        kernel, store = _make_kernel(tmpdir)
        lens = VectorLens(kernel)

        # Insert 1000 vectors (10-dim) via insert() + commit()
        import random
        random.seed(42)

        _reset(kernel, store)
        t0 = time.perf_counter()
        for i in range(1000):
            vec = [random.random() for _ in range(10)]
            lens.insert("vecs", f"vec{i}", vec)
        lens.commit("vecs")
        insert_t = time.perf_counter() - t0
        st = _stats(kernel, store)

        # Search
        query = [0.5] * 10
        _reset(kernel, store)
        lens._unified_storage._manifest_cache.clear()
        t0 = time.perf_counter()
        results = lens.search("vecs", query, k=10)
        search_t = time.perf_counter() - t0
        st_s = _stats(kernel, store)

        print(f"  Insert 1000 vectors (10-dim): {_ms(insert_t)}, {st['puts']} PUTs")
        print(f"  Search (k=10):                {_ms(search_t)}, {st_s['gets']} GETs, {len(results)} results")

        shutil.rmtree(tmpdir, ignore_errors=True)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def main():
    print("=" * 70)
    print("  Comprehensive Performance Benchmark (LocalFS)")
    print("  " + time.strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 70)

    bench_bulk_write()
    bench_append_shard()
    bench_point_lookup()
    bench_range_scan()
    bench_branch_merge()
    bench_time_travel()
    bench_acid()
    bench_compaction()
    bench_gc_vacuum()
    bench_concurrent_reads()
    bench_streaming()
    bench_keyvalue()
    bench_vector()

    print(f"\n{'=' * 70}")
    print("  Benchmark complete.")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
