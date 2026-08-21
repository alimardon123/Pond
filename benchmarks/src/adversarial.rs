// Can a writer who chooses keys degrade the tree?
//
// Boundaries are decided by a hash of the key, and keys are application data.
// A writer who wants shallow chunks can search for keys whose fingerprint
// lands on a boundary and insert only those, driving chunk sizes to the
// minimum — which drives fanout to the minimum and depth up.
use pond_index::{fingerprint, ChunkConfig, MemStore, NodeStore, Tree};

fn main() {
    let cfg = ChunkConfig::default();
    let target = 20_000usize;

    // Mine keys whose fingerprint is a boundary at any chunk position.
    let t = std::time::Instant::now();
    let mut mined: Vec<Vec<u8>> = Vec::new();
    let mut tried = 0u64;
    let mut i = 0u64;
    while mined.len() < target {
        let k = format!("k{:020}", i);
        i += 1;
        tried += 1;
        // position 2 is the first at which a boundary is allowed
        if cfg.is_boundary(fingerprint(k.as_bytes()), 2) {
            mined.push(k.into_bytes());
        }
    }
    let mine_time = t.elapsed();
    mined.sort();

    let store = MemStore::new();
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        mined.into_iter().map(|k| (k, vec![b'x'; 100])).collect();
    let tree = Tree::build_sorted(&store, entries, cfg);

    // Honest baseline: the same number of ordinary keys.
    let store2 = MemStore::new();
    let normal: Vec<(Vec<u8>, Vec<u8>)> = (0..target as u64)
        .map(|i| (format!("user:{:012}", i).into_bytes(), vec![b'x'; 100]))
        .collect();
    let tree2 = Tree::build_sorted(&store2, normal, cfg);

    println!(
        "mined {} boundary keys from {} candidates in {:.1}s ({:.0} keys/s)",
        target,
        tried,
        mine_time.as_secs_f64(),
        target as f64 / mine_time.as_secs_f64()
    );
    println!(
        "  adversarial: depth {}, {} nodes",
        tree.depth(&store),
        store.len()
    );
    println!(
        "  honest     : depth {}, {} nodes",
        tree2.depth(&store2),
        store2.len()
    );
}
