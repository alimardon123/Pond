// nodesize.rs — how big should an index node be?
//
// On a local disk this is the classic B-tree question and the answer is "about
// a page". On object storage the cost function is different enough to invert
// it, and the difference is worth measuring rather than assuming:
//
//   * A GET is billed per request, not per byte, and a ranged GET is billed
//     the same as a full one. Reading 200 KB costs exactly what reading 4 KB
//     costs.
//   * Latency is roughly a fixed base plus a per-byte term, and the base
//     dominates at these sizes — so a bigger node is nearly free to read.
//   * Depth is the number of *dependent* round trips, and dependent round
//     trips cannot be parallelised: each level names the next.
//
// So the read side wants the largest node that still transfers quickly, and
// what it is really buying is a lower depth.
//
// The write side pushes back, but less than it does on disk: an insert
// rewrites `depth` nodes, so bigger nodes cost more *bytes* per write while
// costing the same *number* of requests. Bytes are the cheap axis here.
//
// This prints both sides at several scales so the trade can be read off
// instead of argued about.

use pond_index::{ChunkConfig, MemStore, Tree};

fn main() {
    let scales = [100_000u64, 1_000_000, 4_000_000];
    let targets = [512u32, 2048, 8192];

    println!("| entries | target | depth | nodes | avg node bytes | bytes rewritten per insert |");
    println!("|---|---|---|---|---|---|");

    for n in scales {
        for target in targets {
            let store = MemStore::new();
            let cfg = ChunkConfig::with_target(target);
            let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..n)
                .map(|i| (format!("user:{:012}", i).into_bytes(), vec![b'x'; 100]))
                .collect();
            let tree = Tree::build_sorted(&store, entries, cfg);

            let nodes = store.len() as u64;
            let avg = store.bytes_written() / nodes.max(1);
            let depth = tree.depth(&store);

            // An insert rewrites its leaf and every ancestor.
            let per_insert = avg * depth as u64;

            println!(
                "| {} | {} | {} | {} | {} | {} |",
                n, target, depth, nodes, avg, per_insert
            );
        }
    }
}
