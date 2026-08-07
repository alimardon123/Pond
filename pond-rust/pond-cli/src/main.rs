// Pond CLI — the `pond` command
//
// REIMPLEMENTS the correct Python UnifiedStorage logic in Rust:
//   - Commit format: {"parent", "second_parent", "manifest", "message", "timestamp", "index"}
//   - Active branch: IN-MEMORY (not persisted as a ref), defaults to "main"
//   - Branch ref: collections/{name}/_branches/{branch}/commit
//   - Manifest ref: collections/{name}/_branches/{branch}/manifest
//   - Merge: writes a merge commit with two parents (not just fast-forward)
//
// Mirrors the Python PondStorage/UnifiedStorage API:
//   write, read, branch, checkout, checkout -b, merge (source→target),
//   list-branches, history, undo, revert, ls, cat, gc, version

use clap::{Parser, Subcommand};
use pond_kernel::PondKernel;
use std::io::{self, Read as IoRead, Write as IoWrite};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Parser)]
#[command(name = "pond")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Content-addressed storage with branching and time-travel")]
struct Cli {
    #[arg(long, env = "POND_ROOT", global = true)]
    root: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init { #[arg(default_value = ".")] path: PathBuf },
    Write { collection: String, #[arg(group = "input")] file: Option<String>,
            #[arg(long, group = "input")] json: Option<String>,
            #[arg(long, group = "input")] bytes: bool,
            #[arg(short, long)] message: Option<String> },
    Read { name_or_hash: String, #[arg(short, long)] output: Option<String> },
    Branch { collection: String, branch_name: String },
    Checkout { collection: String, branch_name: String,
               #[arg(short = 'b', long = "new")] new: bool },
    Merge { collection: String, source_branch: String,
            #[arg(short, long)] into: Option<String>,
            #[arg(short, long)] message: Option<String> },
    Branches { collection: String },
    History { collection: String, #[arg(short, long, default_value = "20")] limit: usize },
    Undo { collection: String, #[arg(default_value = "1")] steps: usize },
    Revert { collection: String, commit_hash: String },
    Ls,
    Cat { hash: String },
    Version,
}

// ---------------------------------------------------------------------------
// Ref namespace helpers — match Python UnifiedStorage conventions exactly
// ---------------------------------------------------------------------------

/// Branch commit ref: collections/{name}/_branches/{branch}/commit
fn branch_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/commit", collection, branch)
}

/// Manifest ref: collections/{name}/_branches/{branch}/manifest
fn manifest_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/manifest", collection, branch)
}

// ---------------------------------------------------------------------------
// Commit format — matches Python _write_commit_blob exactly
// ---------------------------------------------------------------------------

/// A commit blob. Stored as JSON. Matches the Python format:
///   {"parent": "hash_or_null", "second_parent": "hash_or_null",
///    "manifest": "manifest_hash", "message": "...",
///    "timestamp": 1234567890.123, "index": 0}
fn make_commit_json(
    parent: Option<&str>,
    second_parent: Option<&str>,
    manifest: &str,
    message: &str,
    index: usize,
) -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64()).unwrap_or(0.0);
    format!(
        r#"{{"parent":{},"second_parent":{},"manifest":"{}","message":"{}","timestamp":{},"index":{}}}"#,
        parent.map(|p| format!("\"{}\"", p)).unwrap_or_else(|| "null".to_string()),
        second_parent.map(|p| format!("\"{}\"", p)).unwrap_or_else(|| "null".to_string()),
        manifest,
        message.replace('\\', "\\\\").replace('"', "\\\""),
        ts,
        index,
    )
}

/// Parse a commit blob (JSON) and extract fields.
#[derive(Debug)]
struct Commit {
    parent: Option<String>,
    second_parent: Option<String>,
    manifest: String,
    message: String,
    timestamp: f64,
    index: usize,
}

fn parse_commit(data: &[u8]) -> Option<Commit> {
    let s = String::from_utf8_lossy(data);
    Some(Commit {
        parent: extract_field(&s, "parent"),
        second_parent: extract_field(&s, "second_parent"),
        manifest: extract_field(&s, "manifest").unwrap_or_default(),
        message: extract_field(&s, "message").unwrap_or_default(),
        timestamp: extract_field(&s, "timestamp")
            .and_then(|v| v.parse().ok()).unwrap_or(0.0),
        index: extract_field(&s, "index")
            .and_then(|v| v.parse().ok()).unwrap_or(0),
    })
}

// ---------------------------------------------------------------------------
// CLI state — active branch is IN-MEMORY (matches Python _active_branches)
// ---------------------------------------------------------------------------

/// The active branch for each collection. Persisted to .pond/HEAD
/// (like .git/HEAD) so the CLI can maintain state across invocations.
/// The Python implementation keeps this in-memory (_active_branches dict)
/// because it's a long-running process. The CLI needs persistence.
fn get_active_branch(kernel: &PondKernel, collection: &str) -> String {
    // Try reading from .pond/HEAD
    // The kernel doesn't expose its base_dir, so we use the store's
    // list_paths to find the HEAD file. Actually, the HEAD file is at
    // the root level — we can read it via the kernel's resolve.
    //
    // We store active branch as: _active_branch/{collection} → blob containing branch name
    // This uses the kernel's own ref mechanism (no separate file).
    let ref_name = format!("_active_branch/{}", collection);
    if let Some(hash) = kernel.resolve(&ref_name) {
        if let Ok(data) = kernel.read_blob(&hash) {
            return String::from_utf8_lossy(&data).to_string();
        }
    }
    "main".to_string()
}

/// Set the active branch for a collection (persisted via kernel refs).
fn set_active_branch(kernel: &PondKernel, collection: &str, branch: &str) {
    let ref_name = format!("_active_branch/{}", collection);
    if let Ok(h) = kernel.write(branch.as_bytes()) {
        let _ = kernel.reference(&ref_name, &h);
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { path } => {
            let base = cli.root.unwrap_or(path);
            cmd_init(&base);
        }
        Commands::Version => {
            println!("pond {}", env!("CARGO_PKG_VERSION"));
        }
        cmd => {
            let root = cli.root.unwrap_or_else(|| PathBuf::from("."));
            let kernel = match PondKernel::new_local(&root) {
                Ok(k) => k,
                Err(e) => {
                    eprintln!("Error: failed to open kernel at {}: {}", root.display(), e);
                    eprintln!("Hint: run 'pond init' first");
                    std::process::exit(1);
                }
            };
            match cmd {
                Commands::Write { collection, file, json, bytes, message } => {
                    cmd_write(&kernel, &collection, file, json, bytes, message);
                }
                Commands::Read { name_or_hash, output } => {
                    cmd_read(&kernel, &name_or_hash, output);
                }
                Commands::Branch { collection, branch_name } => {
                    cmd_branch(&kernel, &collection, &branch_name);
                }
                Commands::Checkout { collection, branch_name, new } => {
                    cmd_checkout(&kernel, &collection, &branch_name, new);
                }
                Commands::Merge { collection, source_branch, into, message } => {
                    cmd_merge(&kernel, &collection, &source_branch, into, message);
                }
                Commands::Branches { collection } => {
                    cmd_branches(&kernel, &collection);
                }
                Commands::History { collection, limit } => {
                    cmd_history(&kernel, &collection, limit);
                }
                Commands::Undo { collection, steps } => {
                    cmd_undo(&kernel, &collection, steps);
                }
                Commands::Revert { collection, commit_hash } => {
                    cmd_revert(&kernel, &collection, &commit_hash);
                }
                Commands::Ls => { cmd_ls(&kernel); }
                Commands::Cat { hash } => { cmd_cat(&kernel, &hash); }
                _ => unreachable!(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

fn cmd_init(path: &PathBuf) {
    let blobs_dir = path.join("blobs");
    match fs::create_dir_all(&blobs_dir) {
        Ok(()) => println!("Initialized empty Pond repository in {}", path.display()),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_write(kernel: &PondKernel, collection: &str, file: Option<String>,
             json: Option<String>, bytes: bool, message: Option<String>) {
    let data: Vec<u8> = if let Some(j) = json {
        match serde_json::from_str::<serde_json::Value>(&j) {
            Ok(_) => j.into_bytes(),
            Err(e) => { eprintln!("Error: invalid JSON: {}", e); std::process::exit(1); }
        }
    } else if bytes || file.as_deref() == Some("-") {
        let mut buf = Vec::new();
        io::stdin().read_to_end(&mut buf).unwrap();
        buf
    } else if let Some(path) = file {
        std::fs::read(&path).unwrap_or_else(|e| {
            eprintln!("Error: failed to read {}: {}", path, e); std::process::exit(1);
        })
    } else {
        eprintln!("Error: no input provided. Use <file>, --json, or --bytes");
        std::process::exit(1);
    };

    // Write the data blob
    let data_hash = kernel.write(&data).unwrap_or_else(|e| {
        eprintln!("Error: {}", e); std::process::exit(1);
    });

    // Write a commit blob matching the Python format:
    // {"parent": ..., "second_parent": null, "manifest": data_hash,
    //  "message": "...", "timestamp": ..., "index": N}
    let active_branch = get_active_branch(kernel, collection);
    let parent = kernel.resolve(&branch_ref(collection, &active_branch));
    let index = parent.as_ref()
        .and_then(|p| kernel.read_blob(p).ok())
        .and_then(|d| parse_commit(&d))
        .map(|c| c.index + 1)
        .unwrap_or(0);
    let commit_json = make_commit_json(
        parent.as_deref(), None, &data_hash,
        &message.unwrap_or_default(), index);
    let commit_hash = kernel.write(commit_json.as_bytes()).unwrap_or_else(|e| {
        eprintln!("Error: {}", e); std::process::exit(1);
    });

    // Update the active branch's commit ref AND manifest ref
    // (matches Python _write_commit_blob which updates both)
    kernel.reference(&branch_ref(collection, &active_branch), &commit_hash).unwrap();
    kernel.reference(&manifest_ref(collection, &active_branch), &data_hash).unwrap();
    // Also set the flat ref for backward compat
    let _ = kernel.reference(collection, &commit_hash);
    println!("{}\t{}", &commit_hash[..12], collection);
}

fn cmd_read(kernel: &PondKernel, name_or_hash: &str, output: Option<String>) {
    let active_branch = get_active_branch(kernel, name_or_hash);
    let commit_ref = branch_ref(name_or_hash, &active_branch);
    let data = if let Some(commit_hash) = kernel.resolve(&commit_ref) {
        // Read the commit blob, extract the manifest (= data hash for v0.1),
        // read the data
        match kernel.read_blob(&commit_hash) {
            Ok(commit_data) => {
                if let Some(commit) = parse_commit(&commit_data) {
                    kernel.read_blob(&commit.manifest)
                } else {
                    // Old-style: commit IS the data
                    Ok(commit_data)
                }
            }
            Err(e) => Err(e),
        }
    } else {
        kernel.read(name_or_hash)
    };
    match data {
        Ok(data) => {
            if let Some(path) = output {
                std::fs::write(&path, &data).unwrap();
            } else {
                io::stdout().write_all(&data).unwrap();
            }
        }
        Err(e) => { eprintln!("Error: '{}': {}", name_or_hash, e); std::process::exit(1); }
    }
}

fn cmd_branch(kernel: &PondKernel, collection: &str, branch_name: &str) {
    // Matches Python branch(): copy BOTH commit ref AND manifest ref
    let active = get_active_branch(kernel, collection);
    let source_commit = kernel.resolve(&branch_ref(collection, &active))
        .unwrap_or_else(|| {
            eprintln!("Error: collection '{}' has no commits", collection);
            std::process::exit(1);
        });
    kernel.reference(&branch_ref(collection, branch_name), &source_commit).unwrap();
    // Also copy the manifest ref (matches Python)
    if let Some(source_manifest) = kernel.resolve(&manifest_ref(collection, &active)) {
        kernel.reference(&manifest_ref(collection, branch_name), &source_manifest).unwrap();
    }
    println!("Created branch '{}' at {}", branch_name, &source_commit[..12]);
}

fn cmd_checkout(kernel: &PondKernel, collection: &str, branch_name: &str, new: bool) {
    if new {
        cmd_branch(kernel, collection, branch_name);
    } else {
        if kernel.resolve(&branch_ref(collection, branch_name)).is_none() {
            eprintln!("Error: branch '{}' not found", branch_name);
            std::process::exit(1);
        }
    }
    // Persist the active branch (CLI needs state across invocations)
    set_active_branch(kernel, collection, branch_name);
    println!("Switched to branch '{}'", branch_name);
}

fn cmd_merge(kernel: &PondKernel, collection: &str, source_branch: &str,
             into: Option<String>, message: Option<String>) {
    let target_branch = into.unwrap_or_else(|| get_active_branch(kernel, collection));

    let source_commit = kernel.resolve(&branch_ref(collection, source_branch))
        .unwrap_or_else(|| {
            eprintln!("Error: source branch '{}' not found", source_branch);
            std::process::exit(1);
        });
    let target_commit = kernel.resolve(&branch_ref(collection, &target_branch))
        .unwrap_or_else(|| {
            eprintln!("Error: target branch '{}' not found", target_branch);
            std::process::exit(1);
        });

    // Get source manifest
    let source_manifest = kernel.resolve(&manifest_ref(collection, source_branch))
        .unwrap_or_default();

    // Write a MERGE COMMIT with two parents (matches Python merge)
    let target_index = kernel.read_blob(&target_commit).ok()
        .and_then(|d| parse_commit(&d))
        .map(|c| c.index + 1)
        .unwrap_or(0);
    let merge_json = make_commit_json(
        Some(&target_commit),     // parent = target (the branch being merged INTO)
        Some(&source_commit),     // second_parent = source (the branch being merged FROM)
        &source_manifest,         // manifest = source's manifest (fast-forward: source wins)
        &message.unwrap_or_else(|| format!("Merge '{}' into '{}'", source_branch, target_branch)),
        target_index,
    );
    let merge_hash = kernel.write(merge_json.as_bytes()).unwrap();

    // Point target branch at the merge commit
    kernel.reference(&branch_ref(collection, &target_branch), &merge_hash).unwrap();
    kernel.reference(&manifest_ref(collection, &target_branch), &source_manifest).unwrap();
    if target_branch == get_active_branch(kernel, collection) {
        let _ = kernel.reference(collection, &merge_hash);
    }
    println!("Merge commit {} ('{}' → '{}')", &merge_hash[..12], source_branch, target_branch);
}

fn cmd_branches(kernel: &PondKernel, collection: &str) {
    let prefix = format!("collections/{}/_branches/", collection);
    let refs = kernel.list_names_prefix(&prefix);
    let active = get_active_branch(kernel, collection);
    if refs.is_empty() {
        if kernel.resolve(collection).is_some() {
            println!("* main");
        } else {
            println!("(no branches)");
        }
        return;
    }
    for ref_path in refs {
        if let Some(branch) = ref_path.strip_prefix(&prefix).and_then(|s| s.strip_suffix("/commit")) {
            let marker = if branch == active { "*" } else { " " };
            let hash = kernel.resolve(&ref_path).unwrap_or_default();
            let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
            println!("{} {}\t{}", marker, branch, prefix);
        }
    }
}

fn cmd_history(kernel: &PondKernel, collection: &str, limit: usize) {
    let active = get_active_branch(kernel, collection);
    let mut current = kernel.resolve(&branch_ref(collection, &active))
        .or_else(|| kernel.resolve(collection));
    let mut count = 0;
    while let Some(commit_hash) = current {
        if count >= limit { break; }
        match kernel.read_blob(&commit_hash) {
            Ok(commit_data) => {
                if let Some(commit) = parse_commit(&commit_data) {
                    let merge_marker = if commit.second_parent.is_some() { " (merge)" } else { "" };
                    println!("{}\t{}{}", &commit_hash[..12], commit.message, merge_marker);
                    count += 1;
                    current = commit.parent;
                } else {
                    println!("{}\t(data, no commit metadata)", &commit_hash[..12]);
                    break;
                }
            }
            Err(_) => { println!("{}\t(data)", &commit_hash[..12]); break; }
        }
    }
    if count == 0 { println!("(no commits)"); }
}

fn cmd_undo(kernel: &PondKernel, collection: &str, steps: usize) {
    let active = get_active_branch(kernel, collection);
    let mut current = kernel.resolve(&branch_ref(collection, &active));
    for _ in 0..steps {
        current = current.as_ref()
            .and_then(|h| kernel.read_blob(h).ok())
            .and_then(|d| parse_commit(&d))
            .and_then(|c| c.parent);
    }
    match current {
        Some(target_hash) => {
            kernel.reference(&branch_ref(collection, &active), &target_hash).unwrap();
            // Sync manifest ref (matches Python _sync_branch_manifest_to_head)
            if let Some(commit) = kernel.read_blob(&target_hash).ok().and_then(|d| parse_commit(&d)) {
                if !commit.manifest.is_empty() {
                    kernel.reference(&manifest_ref(collection, &active), &commit.manifest).unwrap();
                }
            }
            println!("Undo {} → now at {}", steps, &target_hash[..12]);
        }
        None => {
            eprintln!("Error: cannot undo {} steps", steps);
            std::process::exit(1);
        }
    }
}

fn cmd_revert(kernel: &PondKernel, collection: &str, commit_hash: &str) {
    let active = get_active_branch(kernel, collection);
    if kernel.read_blob(commit_hash).is_err() {
        eprintln!("Error: commit '{}' not found", commit_hash);
        std::process::exit(1);
    }
    kernel.reference(&branch_ref(collection, &active), commit_hash).unwrap();
    // Sync manifest ref
    if let Some(commit) = kernel.read_blob(commit_hash).ok().and_then(|d| parse_commit(&d)) {
        if !commit.manifest.is_empty() {
            kernel.reference(&manifest_ref(collection, &active), &commit.manifest).unwrap();
        }
    }
    println!("Reverted to {}", &commit_hash[..12]);
}

fn cmd_ls(kernel: &PondKernel) {
    let names = kernel.list_names();
    if names.is_empty() { println!("(no collections)"); return; }
    let mut collections: Vec<String> = names.iter()
        .filter(|n| n.starts_with("collections/"))
        .filter_map(|n| n.strip_prefix("collections/").and_then(|s| s.split('/').next()))
        .map(|s| s.to_string())
        .collect();
    for n in &names {
        if !n.starts_with("collections/") && !n.contains('/') {
            collections.push(n.clone());
        }
    }
    collections.sort(); collections.dedup();
    for name in collections {
        let active = get_active_branch(kernel, &name);
        let hash = kernel.resolve(&branch_ref(&name, &active))
            .or_else(|| kernel.resolve(&name))
            .unwrap_or_default();
        let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
        println!("{}\t{}", prefix, name);
    }
}

fn cmd_cat(kernel: &PondKernel, hash: &str) {
    match kernel.read_blob(hash) {
        Ok(data) => { io::stdout().write_all(&data).unwrap(); return; }
        Err(_) if hash.len() < 64 => {
            let matches = kernel.list_blobs_prefix(hash);
            if matches.len() == 1 {
                kernel.read_blob(&matches[0]).map(|d| { io::stdout().write_all(&d).unwrap(); }).ok();
                return;
            } else if matches.is_empty() {
                eprintln!("Error: no blob with prefix '{}'", hash);
            } else {
                eprintln!("Error: ambiguous prefix '{}'", hash);
            }
            std::process::exit(1);
        }
        Err(e) => { eprintln!("Error: '{}': {}", hash, e); std::process::exit(1); }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_field(json: &str, field: &str) -> Option<String> {
    let needle = format!("\"{}\":\"", field);
    if let Some(start) = json.find(&needle) {
        let rest = &json[start + needle.len()..];
        if let Some(end) = rest.find('"') { return Some(rest[..end].to_string()); }
    }
    let null_needle = format!("\"{}\":null", field);
    if json.contains(&null_needle) { return None; }
    // Also try numeric values (timestamp, index)
    let num_needle = format!("\"{}\":", field);
    if let Some(start) = json.find(&num_needle) {
        let rest = &json[start + num_needle.len()..];
        let num_str: String = rest.chars().take_while(|c| c.is_ascii_digit() || *c == '.').collect();
        if !num_str.is_empty() { return Some(num_str); }
    }
    None
}

use std::fs;
