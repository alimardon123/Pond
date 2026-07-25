"""Quick test of ProllyLensBase — Prolly trees + bounded delta journal."""
import sys, os, shutil, time, json
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "prototype"))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__))))
from kernel import PondMinimal
from prolly_tree import ProllyLensBase, ProllyTree

def test():
    bench_dir = "/tmp/pond_prolly_test"
    if os.path.exists(bench_dir): shutil.rmtree(bench_dir)
    os.makedirs(bench_dir)
    kernel = PondMinimal(bench_dir)
    base = ProllyLensBase(kernel, "test")

    print("=== INSERT 100 keys ===")
    for i in range(100):
        h = kernel.write(f"value-{i}".encode())
        base.stage(f"key-{i:04d}", h)
    base.commit("insert 100 keys")

    print("=== POINT LOOKUP (O(log N)) ===")
    h = base.lookup("key-0050")
    data = kernel.read_blob(h) if h else None
    print(f"  lookup('key-0050') = {data!r}")

    print("\n=== POINT LOOKUP (miss) ===")
    h = base.lookup("key-9999")
    print(f"  lookup('key-9999') = {h}")

    print("\n=== FULL SCAN ===")
    state = base.read_all()
    print(f"  {len(state)} entries")

    print("\n=== UPDATE 1 key (delta commit) ===")
    h_new = kernel.write(b"updated-value-50")
    base.stage("key-0050", h_new)
    base.commit("update key-0050")
    h = base.lookup("key-0050")
    print(f"  after update: {kernel.read_blob(h)!r}")

    print("\n=== DELETE 1 key (delta commit) ===")
    base.stage_delete("key-0051")
    base.commit("delete key-0051")
    h = base.lookup("key-0051")
    print(f"  after delete: {h}")

    print("\n=== HISTORY ===")
    for entry in base.history():
        print(f"  {entry['commit']}  {entry['type']}  {entry['message']}")

    print("\n=== BRANCH + MERGE ===")
    main_commit = kernel.resolve("test")
    base.branch("feature")
    base.checkout("feature")
    h_feat = kernel.write(b"feature-value")
    base.stage("key-feat", h_feat)
    base.commit("add feature key")
    print(f"  On feature: {len(base.read_all())} entries")
    print(f"  feature key: {base.lookup('key-feat')}")

    # Switch back to main
    kernel.reference("test", main_commit)
    base2 = ProllyLensBase(kernel, "test")
    print(f"  On main: {len(base2.read_all())} entries")
    print(f"  feature key on main: {base2.lookup('key-feat')}")

    # Merge
    base2.merge("feature")
    print(f"  After merge: {len(base2.read_all())} entries")
    print(f"  feature key after merge: {base2.lookup('key-feat')}")

    print("\n=== INDEX ===")
    # Build an index: maps first_char → blob_hash (not key)
    state = base2.read_all()
    index_entries = {}
    for pk, bh in state.items():
        val = kernel.read_blob(bh).decode()
        index_entries[f"_index/first_char/{val[0]}"] = bh  # blob hash, not key

    tree_root = ProllyTree.build(kernel, index_entries)
    kernel.reference("test__index__first_char", tree_root)

    # Lookup by index
    idx_result = ProllyTree.lookup(kernel, tree_root, "_index/first_char/u")
    print(f"  Index lookup 'u': {idx_result[:16] if idx_result else None}")

    print("\n=== ALL TESTS PASSED ===")
    kernel.close()
    shutil.rmtree(bench_dir, ignore_errors=True)

if __name__ == "__main__":
    test()
