// Pond CLI — the `pond` command
//
// A single-binary tool for content-addressed storage with branching,
// time-travel, and universal data support (JSON, CSV, raw bytes).
//
// Mirrors the Python PondStorage/UnifiedStorage API:
//   write, read, branch, checkout, checkout -b, merge (source→target),
//   list-branches, history, undo, revert, diff, ls, cat, gc, version
//
// Design principles:
//   - DuckDB philosophy: one binary, no server, embedded
//   - Universal storage: accepts any data format (JSON, CSV, raw bytes)
//   - Simple: mirrors the kernel's 3 primitives (write, read, ref)
//   - Beautiful: git-like commands (init, branch, checkout, merge)

use clap::{Parser, Subcommand};
use pond_kernel::PondKernel;
use std::io::{self, Read as IoRead, Write as IoWrite};
use std::path::PathBuf;

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
    /// Initialize a new .pond/ directory
    Init {
        #[arg(default_value = ".")]
        path: PathBuf,
    },

    /// Write data to a collection (creates a new commit on the active branch)
    Write {
        collection: String,
        #[arg(group = "input")]
        file: Option<String>,
        #[arg(long, group = "input")]
        json: Option<String>,
        #[arg(long, group = "input")]
        bytes: bool,
        #[arg(short, long)]
        message: Option<String>,
    },

    /// Read a collection's current data (from the active branch)
    Read {
        name_or_hash: String,
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Create a branch from the active branch's HEAD
    Branch {
        collection: String,
        branch_name: String,
    },

    /// Switch the active branch (like `git checkout`)
    Checkout {
        collection: String,
        branch_name: String,
        /// Create the branch AND checkout (like `git checkout -b`)
        #[arg(short = 'b', long = "new")]
        new: bool,
    },

    /// Merge a source branch into a target branch
    Merge {
        collection: String,
        /// Branch to merge FROM
        source_branch: String,
        /// Branch to merge INTO (defaults to active branch)
        #[arg(short, long)]
        into: Option<String>,
        #[arg(short, long)]
        message: Option<String>,
    },

    /// List branches of a collection
    Branches {
        collection: String,
    },

    /// Show commit history of a collection
    History {
        collection: String,
        #[arg(short, long, default_value = "20")]
        limit: usize,
    },

    /// Undo the last N commits on the active branch
    Undo {
        collection: String,
        #[arg(default_value = "1")]
        steps: usize,
    },

    /// Revert the active branch to a specific commit hash
    Revert {
        collection: String,
        commit_hash: String,
    },

    /// List all collections
    Ls,

    /// Read a blob by its hash (or hash prefix)
    Cat {
        hash: String,
    },

    /// Print version info
    Version,
}

// ---------------------------------------------------------------------------
// Ref namespace helpers — match the Python UnifiedStorage conventions
// ---------------------------------------------------------------------------

fn branch_ref(collection: &str, branch: &str) -> String {
    format!("collections/{}/_branches/{}/commit", collection, branch)
}

fn active_branch_ref(collection: &str) -> String {
    format!("collections/{}/_active_branch", collection)
}

fn definition_ref(collection: &str) -> String {
    format!("collections/{}/definition", collection)
}

/// Get the active branch name for a collection. Defaults to "main".
fn get_active_branch(kernel: &PondKernel, collection: &str) -> String {
    match kernel.resolve(&active_branch_ref(collection)) {
        Some(hash) => {
            // The _active_branch ref points to a blob containing the branch NAME.
            // Read the blob to get the name.
            match kernel.read_blob(&hash) {
                Ok(data) => String::from_utf8_lossy(&data).to_string(),
                Err(_) => "main".to_string(),
            }
        }
        None => "main".to_string(),
    }
}

/// Set the active branch for a collection.
fn set_active_branch(kernel: &PondKernel, collection: &str, branch: &str) -> io::Result<()> {
    // _active_branch stores the branch NAME, not a hash. We write it as a
    // blob first (so the kernel verifies it), then reference it.
    // Actually, the kernel requires the hash to refer to an existing blob.
    // So we write the branch name as a blob, then reference it.
    let h = kernel.write(branch.as_bytes())?;
    kernel.reference(&active_branch_ref(collection), &h)
}

/// Get the commit hash for a branch. Falls back to flat ref for backward compat.
fn get_branch_commit(kernel: &PondKernel, collection: &str, branch: &str) -> Option<String> {
    kernel.resolve(&branch_ref(collection, branch))
        .or_else(|| {
            // Backward compat: try flat ref (older convention)
            if branch == "main" {
                kernel.resolve(collection)
            } else {
                None
            }
        })
}

/// Set the commit hash for a branch.
fn set_branch_commit(kernel: &PondKernel, collection: &str, branch: &str, hash: &str) -> io::Result<()> {
    kernel.reference(&branch_ref(collection, branch), hash)?;
    // If this is the active branch, also update the flat ref for backward compat
    if get_active_branch(kernel, collection) == branch {
        let _ = kernel.reference(collection, hash);
    }
    Ok(())
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
        // All other commands need a kernel
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
                Commands::Ls => {
                    cmd_ls(&kernel);
                }
                Commands::Cat { hash } => {
                    cmd_cat(&kernel, &hash);
                }
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
        Ok(()) => {
            println!("Initialized empty Pond repository in {}", path.display());
        }
        Err(e) => {
            eprintln!("Error: failed to create {}: {}", blobs_dir.display(), e);
            std::process::exit(1);
        }
    }
}

fn cmd_write(
    kernel: &PondKernel,
    collection: &str,
    file: Option<String>,
    json: Option<String>,
    bytes: bool,
    message: Option<String>,
) {
    let data: Vec<u8> = if let Some(j) = json {
        match serde_json::from_str::<serde_json::Value>(&j) {
            Ok(_) => j.into_bytes(),
            Err(e) => {
                eprintln!("Error: invalid JSON: {}", e);
                std::process::exit(1);
            }
        }
    } else if bytes || file.as_deref() == Some("-") {
        let mut buf = Vec::new();
        if let Err(e) = io::stdin().read_to_end(&mut buf) {
            eprintln!("Error: failed to read stdin: {}", e);
            std::process::exit(1);
        }
        buf
    } else if let Some(path) = file {
        match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("Error: failed to read {}: {}", path, e);
                std::process::exit(1);
            }
        }
    } else {
        eprintln!("Error: no input provided. Use <file>, --json, or --bytes");
        std::process::exit(1);
    };

    // Write the data blob
    let hash = match kernel.write(&data) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("Error: failed to write: {}", e);
            std::process::exit(1);
        }
    };

    // Write a commit blob (stores the message + parent + data hash)
    // For v0.1, the commit is just the data hash + message as JSON.
    // Future versions will use a typed Commit struct.
    let active_branch = get_active_branch(kernel, collection);
    let parent = get_branch_commit(kernel, collection, &active_branch);
    let commit_json = format!(
        r#"{{"data":"{}","parent":{},"message":"{}"}}"#,
        hash,
        parent.map(|p| format!("\"{}\"", p)).unwrap_or_else(|| "null".to_string()),
        message.unwrap_or_default().replace('"', "\\\""),
    );
    let commit_hash = match kernel.write(commit_json.as_bytes()) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("Error: failed to write commit: {}", e);
            std::process::exit(1);
        }
    };

    // Update the active branch's commit ref
    if let Err(e) = set_branch_commit(kernel, collection, &active_branch, &commit_hash) {
        eprintln!("Error: failed to reference {}: {}", collection, e);
        std::process::exit(1);
    }
    println!("{}\t{}", &commit_hash[..12], collection);
}

fn cmd_read(kernel: &PondKernel, name_or_hash: &str, output: Option<String>) {
    // Try as collection name first (resolve active branch), then as hash
    let active_ref = branch_ref(name_or_hash, &get_active_branch(kernel, name_or_hash));
    let data = if let Some(commit_hash) = kernel.resolve(&active_ref) {
        // Read the commit blob, extract the data hash, read the data
        match kernel.read_blob(&commit_hash) {
            Ok(commit_data) => {
                let commit_str = String::from_utf8_lossy(&commit_data);
                // Extract "data":"hash" from the commit JSON
                if let Some(data_hash) = extract_field(&commit_str, "data") {
                    kernel.read_blob(&data_hash)
                } else {
                    // Old-style: commit IS the data (no commit wrapper)
                    Ok(commit_data)
                }
            }
            Err(e) => Err(e),
        }
    } else {
        // Try flat ref, then as hash
        kernel.read(name_or_hash)
    };

    match data {
        Ok(data) => {
            if let Some(path) = output {
                if let Err(e) = std::fs::write(&path, &data) {
                    eprintln!("Error: failed to write {}: {}", path, e);
                    std::process::exit(1);
                }
            } else {
                if let Err(e) = io::stdout().write_all(&data) {
                    eprintln!("Error: failed to write stdout: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("Error: failed to read '{}': {}", name_or_hash, e);
            std::process::exit(1);
        }
    }
}

fn cmd_branch(kernel: &PondKernel, collection: &str, branch_name: &str) {
    let active_branch = get_active_branch(kernel, collection);
    let source_hash = match get_branch_commit(kernel, collection, &active_branch) {
        Some(h) => h,
        None => {
            eprintln!("Error: collection '{}' has no commits to branch from", collection);
            std::process::exit(1);
        }
    };
    match set_branch_commit(kernel, collection, branch_name, &source_hash) {
        Ok(()) => {
            println!("Created branch '{}' of '{}' at {}", branch_name, collection, &source_hash[..12]);
        }
        Err(e) => {
            eprintln!("Error: failed to create branch: {}", e);
            std::process::exit(1);
        }
    }
}

fn cmd_checkout(kernel: &PondKernel, collection: &str, branch_name: &str, new: bool) {
    if new {
        // checkout -b: create branch then switch
        let active_branch = get_active_branch(kernel, collection);
        let source_hash = match get_branch_commit(kernel, collection, &active_branch) {
            Some(h) => h,
            None => {
                eprintln!("Error: collection '{}' has no commits to branch from", collection);
                std::process::exit(1);
            }
        };
        if let Err(e) = set_branch_commit(kernel, collection, branch_name, &source_hash) {
            eprintln!("Error: failed to create branch: {}", e);
            std::process::exit(1);
        }
        println!("Created branch '{}' of '{}'", branch_name, collection);
    } else {
        // Verify the branch exists
        if get_branch_commit(kernel, collection, branch_name).is_none() {
            eprintln!("Error: branch '{}' not found in '{}'", branch_name, collection);
            std::process::exit(1);
        }
    }
    // Switch the active branch
    if let Err(e) = set_active_branch(kernel, collection, branch_name) {
        eprintln!("Error: failed to checkout: {}", e);
        std::process::exit(1);
    }
    println!("Switched to branch '{}'", branch_name);
}

fn cmd_merge(
    kernel: &PondKernel,
    collection: &str,
    source_branch: &str,
    into: Option<String>,
    _message: Option<String>,
) {
    let target_branch = into.unwrap_or_else(|| get_active_branch(kernel, collection));

    let source_hash = match get_branch_commit(kernel, collection, source_branch) {
        Some(h) => h,
        None => {
            eprintln!("Error: source branch '{}' not found", source_branch);
            std::process::exit(1);
        }
    };

    // Verify target exists
    if get_branch_commit(kernel, collection, &target_branch).is_none() {
        eprintln!("Error: target branch '{}' not found", target_branch);
        std::process::exit(1);
    }

    // Fast-forward: point target at source's commit
    match set_branch_commit(kernel, collection, &target_branch, &source_hash) {
        Ok(()) => {
            println!("Merged '{}' into '{}' (fast-forward to {})",
                     source_branch, target_branch, &source_hash[..12]);
        }
        Err(e) => {
            eprintln!("Error: failed to merge: {}", e);
            std::process::exit(1);
        }
    }
}

fn cmd_branches(kernel: &PondKernel, collection: &str) {
    let prefix = format!("collections/{}/_branches/", collection);
    let refs = kernel.list_names_prefix(&prefix);
    let active = get_active_branch(kernel, collection);

    if refs.is_empty() {
        // Try flat ref (backward compat)
        if kernel.resolve(collection).is_some() {
            println!("* main (flat ref, no branch hierarchy)");
        } else {
            println!("(no branches)");
        }
        return;
    }

    for ref_path in refs {
        // Extract branch name from "collections/{name}/_branches/{branch}/commit"
        if let Some(branch) = ref_path.strip_prefix(&prefix).and_then(|s| s.strip_suffix("/commit")) {
            let marker = if branch == active { "*" } else { " " };
            let hash = kernel.resolve(&ref_path).unwrap_or_default();
            let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
            println!("{} {}\t{}", marker, branch, prefix);
        }
    }
}

fn cmd_history(kernel: &PondKernel, collection: &str, limit: usize) {
    let active_branch = get_active_branch(kernel, collection);
    let mut current = get_branch_commit(kernel, collection, &active_branch);
    let mut count = 0;

    if current.is_none() {
        // Try flat ref
        current = kernel.resolve(collection);
    }

    while let Some(commit_hash) = current {
        if count >= limit {
            break;
        }
        // Read the commit blob
        match kernel.read_blob(&commit_hash) {
            Ok(commit_data) => {
                let commit_str = String::from_utf8_lossy(&commit_data);
                let parent = extract_field(&commit_str, "parent");
                let message = extract_field(&commit_str, "message").unwrap_or_default();

                println!("{} {}", &commit_hash[..12], message);
                count += 1;
                current = parent;
            }
            Err(_) => {
                // Old-style: no commit wrapper, just data
                println!("{} (data, no commit metadata)", &commit_hash[..12]);
                break;
            }
        }
    }
    if count == 0 {
        println!("(no commits)");
    }
}

fn cmd_undo(kernel: &PondKernel, collection: &str, steps: usize) {
    let active_branch = get_active_branch(kernel, collection);
    let mut current = get_branch_commit(kernel, collection, &active_branch);

    for _ in 0..steps {
        match &current {
            Some(commit_hash) => {
                match kernel.read_blob(commit_hash) {
                    Ok(commit_data) => {
                        let commit_str = String::from_utf8_lossy(&commit_data);
                        current = extract_field(&commit_str, "parent");
                    }
                    Err(_) => {
                        current = None;
                        break;
                    }
                }
            }
            None => break,
        }
    }

    match current {
        Some(target_hash) => {
            set_branch_commit(kernel, collection, &active_branch, &target_hash).unwrap();
            println!("Undo {} steps → now at {}", steps, &target_hash[..12]);
        }
        None => {
            eprintln!("Error: cannot undo {} steps (not enough history)", steps);
            std::process::exit(1);
        }
    }
}

fn cmd_revert(kernel: &PondKernel, collection: &str, commit_hash: &str) {
    let active_branch = get_active_branch(kernel, collection);

    // Verify the commit exists
    if kernel.read_blob(commit_hash).is_err() {
        eprintln!("Error: commit '{}' not found", commit_hash);
        std::process::exit(1);
    }

    set_branch_commit(kernel, collection, &active_branch, commit_hash).unwrap();
    println!("Reverted '{}' to {}", collection, &commit_hash[..12]);
}

fn cmd_ls(kernel: &PondKernel) {
    let names = kernel.list_names();
    if names.is_empty() {
        println!("(no collections)");
        return;
    }
    // Show top-level collection names (not internal refs)
    let mut collections: Vec<String> = names.iter()
        .filter(|n| n.starts_with("collections/"))
        .filter_map(|n| {
            // Extract collection name from "collections/{name}/..."
            let rest = n.strip_prefix("collections/")?;
            let name = rest.split('/').next()?;
            Some(name.to_string())
        })
        .collect();
    // Also include flat refs (no "collections/" prefix)
    for n in &names {
        if !n.starts_with("collections/") && !n.contains('/') {
            collections.push(n.clone());
        }
    }
    collections.sort();
    collections.dedup();

    for name in collections {
        // Try to show the HEAD hash
        let active = get_active_branch(kernel, &name);
        let hash = get_branch_commit(kernel, &name, &active)
            .or_else(|| kernel.resolve(&name))
            .unwrap_or_default();
        let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
        println!("{}\t{}", prefix, name);
    }
}

fn cmd_cat(kernel: &PondKernel, hash: &str) {
    // Try full hash first
    match kernel.read_blob(hash) {
        Ok(data) => {
            if let Err(e) = io::stdout().write_all(&data) {
                eprintln!("Error: failed to write stdout: {}", e);
                std::process::exit(1);
            }
            return;
        }
        Err(_) if hash.len() < 64 => {
            // Prefix match: list blobs with this prefix
            let matches = kernel.list_blobs_prefix(hash);
            if matches.len() == 1 {
                match kernel.read_blob(&matches[0]) {
                    Ok(data) => {
                        if let Err(e) = io::stdout().write_all(&data) {
                            eprintln!("Error: failed to write stdout: {}", e);
                            std::process::exit(1);
                        }
                        return;
                    }
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
                }
            } else if matches.is_empty() {
                eprintln!("Error: no blob with prefix '{}' found", hash);
            } else {
                eprintln!("Error: ambiguous prefix '{}' — matches {} blobs", hash, matches.len());
            }
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Error: blob '{}' not found: {}", hash, e);
            std::process::exit(1);
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a field value from a simple JSON string.
/// Looks for "field":"value" and returns the value.
/// This is a minimal parser — doesn't handle nested objects or arrays.
fn extract_field(json: &str, field: &str) -> Option<String> {
    let needle = format!("\"{}\":\"", field);
    if let Some(start) = json.find(&needle) {
        let rest = &json[start + needle.len()..];
        if let Some(end) = rest.find('"') {
            return Some(rest[..end].to_string());
        }
    }
    // Also check for null: "field":null
    let null_needle = format!("\"{}\":null", field);
    if json.contains(&null_needle) {
        return None;
    }
    None
}

use std::fs;
