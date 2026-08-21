// adversarial.rs — can a writer who chooses keys degrade the tree?
//
// Chunk boundaries are decided by a hash of the key, and keys are application
// data. So a writer can search for keys whose fingerprint lands on a boundary
// and insert only those, driving every chunk to the smallest size the
// configuration permits — and with it the fanout, and therefore the depth and
// the object count.
//
// This measures the attack against the honest baseline. The defence is the
// chunk floor: below it, no key can end a chunk however long it is searched
// for, so fanout has a hard lower bound that does not depend on the data.
//
//   cargo run --release -p pond_bench --bin adversarial

use pond_index::{fingerprint, ChunkConfig, MemStore, Tree};

/// How many keys to mine. Enough to show the shape; the effect grows with n.
const KEYS: usize = 5_000;

fn main() {
    let cfg = ChunkConfig::default();

    // The attacker's best move is to end a chunk at the earliest position the
    // configuration allows. Searching for a key that ends one *earlier* is
    // futile — that is what the floor buys.
    let earliest = cfg.min_entries;
    println!(
        "target {} entries/chunk, floor {} — no key can end a chunk before the floor",
        cfg.target_entries, earliest
    );

    let t = std::time::Instant::now();
    let mut mined: Vec<Vec<u8>> = Vec::with_capacity(KEYS);
    let mut tried = 0u64;
    let mut i = 0u64;
    let mut buf = String::with_capacity(24);
    while mined.len() < KEYS {
        use std::fmt::Write;
        buf.clear();
        let _ = write!(buf, "k{:020}", i);
        i += 1;
        tried += 1;
        if cfg.is_boundary(fingerprint(buf.as_bytes()), earliest) {
            mined.push(buf.as_bytes().to_vec());
        }
    }
    let mine_time = t.elapsed();
    mined.sort();

    let adversarial = build(&mined, cfg);
    let honest: Vec<Vec<u8>> = (0..KEYS as u64)
        .map(|i| format!("user:{:012}", i).into_bytes())
        .collect();
    let honest = build(&honest, cfg);

    println!(
        "mined {} boundary keys from {} candidates in {:.1}s ({:.0} keys/s)",
        KEYS,
        tried,
        mine_time.as_secs_f64(),
        KEYS as f64 / mine_time.as_secs_f64().max(1e-9)
    );
    println!("  adversarial: depth {}, {} nodes", adversarial.1, adversarial.0);
    println!("  honest     : depth {}, {} nodes", honest.1, honest.0);
    println!(
        "  ratio      : {:.1}x the objects, {:+} levels",
        adversarial.0 as f64 / honest.0.max(1) as f64,
        adversarial.1 as i64 - honest.1 as i64
    );
}

/// Build a tree from keys and return (node count, depth).
fn build(keys: &[Vec<u8>], cfg: ChunkConfig) -> (usize, usize) {
    let store = MemStore::new();
    let entries: Vec<(Vec<u8>, Vec<u8>)> =
        keys.iter().map(|k| (k.clone(), vec![b'x'; 100])).collect();
    let tree = Tree::build_sorted(&store, entries, cfg);
    (store.len(), tree.depth(&store))
}
