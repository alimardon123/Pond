// Pond CLI — the `pond` command
//
// Uses pond_storage (the Rust UnifiedStorage port) for all storage operations.
// This ensures the CLI uses the SAME code path as the Python UnifiedStorage —
// no duplicate logic, no drift.
//
// Design principles:
//   - DuckDB philosophy: one binary, no server, embedded
//   - Git-style auto-discovery: `pond init` creates a `.pond/` marker;
//     subsequent commands find it by walking up from CWD
//   - Universal storage: accepts any data format (JSON, CSV, raw bytes)
//   - Simple: delegates to pond_storage for all logic
//   - Beautiful: CLI is a thin UI layer over the storage library
//
// STORAGE DISCOVERY (in priority order):
//   1. --root <url>           (explicit override)
//   2. POND_ROOT env var      (explicit override)
//   3. .pond/config file      (auto-discovery — walks up from CWD)
//   4. . (current directory)  (fallback)
//
// The .pond/ marker directory contains a `config` file:
//   - Lives at the storage root (local path or S3 prefix)
//   - For local FS: location is implicit (.pond/'s parent directory)
//   - For S3: .pond/config is a key in the S3 prefix
//   - Config holds Pond-level settings (NOT storage URL — that's passed via --root)

use clap::{Parser, Subcommand};
use pond_kernel::PondKernel;
use pond_storage::{UnifiedStorage, branch, commit, shard, write, read, transaction};
use std::io::{self, Read as IoRead, Write as IoWrite};

#[derive(Parser)]
#[command(name = "pond")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Content-addressed storage with branching and time-travel")]
struct Cli {
    /// Storage root URL. Overrides .pond/ auto-discovery.
    /// Can be a local path or an S3 URL:
    ///   /var/lib/pond                                   (local filesystem)
    ///   s3://bucket/prefix?region=us-east-1&endpoint=... (S3-compatible)
    ///
    /// If not provided, the CLI auto-discovers a .pond/ marker by walking
    /// up from the current directory (like git finds .git/).
    #[arg(long, env = "POND_ROOT", global = true)]
    root: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new Pond repository.
    /// Creates a .pond/ marker directory with a config file.
    /// For local FS: `pond init` or `pond init /path`
    /// For S3: `pond init "s3://bucket/prefix?region=..."`
    Init {
        /// Path (local FS) or S3 URL. Defaults to current directory.
        #[arg(default_value = ".")]
        location: String,
    },
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

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init { location } => {
            cmd_init(&location, cli.root.as_deref());
        }
        Commands::Version => {
            println!("pond {}", env!("CARGO_PKG_VERSION"));
        }
        cmd => {
            // Resolve the storage location using the discovery chain.
            let storage_url = resolve_storage_url(cli.root.as_deref());
            let storage = match open_storage(&storage_url) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error: failed to open storage: {}", e);
                    eprintln!("Hint: run 'pond init' first, or use --root / POND_ROOT");
                    std::process::exit(1);
                }
            };
            // CLI-specific: load persisted active branches from kernel refs.
            // The Python UnifiedStorage keeps these in-memory (long-running process).
            // The CLI is a new process each invocation, so we persist via kernel refs
            // at _active_branch/{collection} → blob containing branch name.
            load_persisted_active_branches(&storage);
            match cmd {
                Commands::Write { collection, file, json, bytes, message } => {
                    cmd_write(&storage, &collection, file, json, bytes, message);
                }
                Commands::Read { name_or_hash, output } => {
                    cmd_read(&storage, &name_or_hash, output);
                }
                Commands::Branch { collection, branch_name } => {
                    cmd_branch(&storage, &collection, &branch_name);
                }
                Commands::Checkout { collection, branch_name, new } => {
                    cmd_checkout(&storage, &collection, &branch_name, new);
                }
                Commands::Merge { collection, source_branch, into, message } => {
                    cmd_merge(&storage, &collection, &source_branch, into, message);
                }
                Commands::Branches { collection } => {
                    cmd_branches(&storage, &collection);
                }
                Commands::History { collection, limit } => {
                    cmd_history(&storage, &collection, limit);
                }
                Commands::Undo { collection, steps } => {
                    cmd_undo(&storage, &collection, steps);
                }
                Commands::Revert { collection, commit_hash } => {
                    cmd_revert(&storage, &collection, &commit_hash);
                }
                Commands::Ls => { cmd_ls(&storage); }
                Commands::Cat { hash } => { cmd_cat(&storage, &hash); }
                _ => unreachable!(),
            }
        }
    }
}

/// Load persisted active branches from kernel refs.
/// The CLI persists active branch at _active_branch/{collection} → blob containing branch name.
/// This is a CLI-specific persistence layer (the Python UnifiedStorage keeps it in-memory).
fn load_persisted_active_branches(storage: &UnifiedStorage) {
    let kernel = storage.kernel();
    // Find all _active_branch/ refs
    let names = kernel.list_names_prefix("_active_branch/");
    for name in names {
        if let Some(hash) = kernel.resolve(&name) {
            if let Ok(data) = kernel.read_blob(&hash) {
                let branch = String::from_utf8_lossy(&data).to_string();
                if let Some(collection) = name.strip_prefix("_active_branch/") {
                    storage.set_active_branch(collection, &branch);
                }
            }
        }
    }
}

/// Persist the active branch for a collection to a kernel ref.
fn persist_active_branch(storage: &UnifiedStorage, collection: &str, branch: &str) {
    let kernel = storage.kernel();
    let ref_name = format!("_active_branch/{}", collection);
    if let Ok(h) = kernel.write(branch.as_bytes()) {
        let _ = kernel.reference(&ref_name, &h);
    }
}

// ---------------------------------------------------------------------------
// Storage discovery (git-style .pond/ marker)
// ---------------------------------------------------------------------------

/// Resolve the storage URL using the discovery chain:
///   1. --root <url>           (explicit override — required for S3)
///   2. POND_ROOT env var      (explicit override — required for S3)
///   3. .pond/ marker in CWD   (auto-discovery — local FS only, walks up from CWD)
///   4. . (current directory)  (fallback)
///
/// Note: S3 storage requires --root or POND_ROOT (no auto-discovery for remote).
/// This is intentional — you can't "walk up" to find an S3 URL.
fn resolve_storage_url(explicit_root: Option<&str>) -> String {
    // 1. Explicit --root or POND_ROOT env var
    if let Some(root) = explicit_root {
        return root.to_string();
    }

    // 3. Auto-discover .pond/ by walking up from CWD (local FS only)
    if let Some(pond_dir) = find_pond_marker() {
        let repo_root = pond_dir.parent().unwrap_or(std::path::Path::new("."));
        // For local FS, the .pond/ marker's parent IS the storage root.
        // (No need to read .pond/config — location is implicit.)
        return repo_root.to_string_lossy().to_string();
    }

    // 4. Fallback: current directory
    ".".to_string()
}

/// Walk up from CWD looking for a `.pond/` directory (like git finds `.git/`).
fn find_pond_marker() -> Option<std::path::PathBuf> {
    let cwd = std::env::current_dir().ok()?;
    let mut current: &std::path::Path = &cwd;
    loop {
        let pond_dir = current.join(".pond");
        if pond_dir.is_dir() {
            return Some(pond_dir);
        }
        match current.parent() {
            Some(parent) => current = parent,
            None => return None,
        }
    }
}

/// Get a human-readable description of the storage connection for display.
fn describe_storage(url: &str) -> String {
    if url.starts_with("s3://") {
        // Parse S3 URL for a friendly description
        if let Some(bucket_end) = url[5..].find('/') {
            let bucket = &url[5..5 + bucket_end];
            let prefix = &url[6 + bucket_end..];
            // Check if it's R2, MinIO, etc. from the endpoint param
            let endpoint = url.split("endpoint=").nth(1).unwrap_or("");
            let provider = if endpoint.contains("r2.cloudflarestorage.com") {
                "Cloudflare R2"
            } else if endpoint.contains("localhost") || endpoint.contains("127.0.0.1") {
                "MinIO/Local"
            } else if endpoint.is_empty() {
                "AWS S3"
            } else {
                "S3-compatible"
            };
            format!("{} ({}: s3://{}/{})", provider, bucket, bucket, prefix)
        } else {
            format!("S3: {}", url)
        }
    } else {
        let path = std::path::Path::new(url);
        let abs = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir().unwrap_or_default().join(path)
        };
        format!("local FS: {}", abs.display())
    }
}

// ---------------------------------------------------------------------------
// Command implementations — thin wrappers over pond_storage
// ---------------------------------------------------------------------------

/// Open a storage backend from a root URL/path.
///
/// Auto-detects:
///   - `s3://bucket/prefix?...` → S3-compatible storage
///   - `/path/to/dir` or `.` → local filesystem
fn open_storage(root: &str) -> Result<UnifiedStorage, Box<dyn std::error::Error>> {
    if root.starts_with("s3://") {
        #[cfg(feature = "s3")]
        {
            let store = pond_s3::S3ObjectStore::from_url(root)?;
            let kernel = PondKernel::new_with_store(Box::new(store));
            Ok(UnifiedStorage::new(kernel))
        }
        #[cfg(not(feature = "s3"))]
        {
            Err(format!(
                "S3 support not compiled in (built with --no-default-features). \
                 Rebuild with `cargo build` (default features include s3)."
            ).into())
        }
    } else if let Some(path) = root.strip_prefix("file://") {
        UnifiedStorage::new_local(path).map_err(|e| e.into())
    } else {
        UnifiedStorage::new_local(root).map_err(|e| e.into())
    }
}

/// Initialize or connect to a Pond repository.
///
/// Behavior:
///   - `pond init` (no path) → create `.pond/` in CWD (local FS)
///   - `pond init /path` → create `.pond/` at /path (local FS); just connect if exists
///   - `pond init "s3://..."` → create `.pond/config` IN S3; just connect if exists
///
/// The `.pond/config` ALWAYS lives at the storage root (not in CWD for remote).
/// For local FS: `.pond/` is a directory in the storage path.
/// For S3: `.pond/config` is a key in the S3 prefix.
///
/// If `--root` is provided, it overrides the location argument.
fn cmd_init(location: &str, explicit_root: Option<&str>) {
    let location = explicit_root.unwrap_or(location);

    if location.starts_with("s3://") {
        cmd_init_s3(location);
    } else {
        cmd_init_local(location);
    }
}

/// Initialize/connect to a local FS storage.
/// `.pond/` lives in the storage path (not CWD).
fn cmd_init_local(location: &str) {
    let path_stripped = location.strip_prefix("file://").unwrap_or(location);
    let base_path = std::path::Path::new(path_stripped);
    let pond_dir = base_path.join(".pond");
    let blobs_dir = base_path.join("blobs");

    let already_initialized = pond_dir.is_dir();

    if !already_initialized {
        // Create blobs/ directory
        if let Err(e) = std::fs::create_dir_all(&blobs_dir) {
            eprintln!("Error: failed to create blobs directory: {}", e);
            std::process::exit(1);
        }
        // Create .pond/ marker with Pond-level config
        if let Err(e) = std::fs::create_dir_all(&pond_dir) {
            eprintln!("Error: failed to create .pond/ marker: {}", e);
            std::process::exit(1);
        }
        // Config holds Pond-level settings (NOT storage= — location is implicit)
        let config = "# Pond repository configuration\n\
            # Storage location: this directory (local filesystem)\n";
        if let Err(e) = std::fs::write(pond_dir.join("config"), config) {
            eprintln!("Error: failed to write .pond/config: {}", e);
            std::process::exit(1);
        }
    }

    // Display connection info
    let abs_path = if base_path.is_absolute() {
        base_path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(base_path)
    };
    println!("Connected to: local FS: {}", abs_path.display());
    if already_initialized {
        println!("(already initialized)");
    } else {
        println!("Initialized empty Pond repository.");
    }
    println!("\nNow you can run:");
    println!("  pond write users --json '[{{\"id\":1}}]' -m \"first commit\"");
}

/// Initialize/connect to S3-compatible storage.
/// `.pond/config` lives IN S3 (not in local CWD).
fn cmd_init_s3(url: &str) {
    #[cfg(feature = "s3")]
    {
        match pond_s3::S3ObjectStore::from_url(url) {
            Ok(store) => {
                use pond_kernel::ObjectStore;
                // Check if .pond/config already exists in S3
                let existing_config = store.get_path(".pond/config");
                let already_initialized = existing_config.is_some();

                // Write .pond/config to S3 (create or update)
                let config = format!(
                    "# Pond repository configuration\n\
                     # Storage: S3-compatible ({})\n",
                    url
                );
                // Store the config as a blob, then reference it at .pond/config
                if let Err(e) = store.put_path(".pond/config", &pond_kernel::hash_bytes(config.as_bytes())) {
                    eprintln!("Error: failed to write .pond/config to S3: {}", e);
                    std::process::exit(1);
                }
                // Also need to write the actual config content as a blob
                let config_hash = pond_kernel::hash_bytes(config.as_bytes());
                let _ = store.put_blob(config.as_bytes());
                let _ = store.put_path(".pond/config", &config_hash);

                // Verify connectivity
                match store.list_paths("") {
                    Ok(_) => {
                        // Display connection info
                        println!("Connected to: {}", describe_storage(url));
                        if already_initialized {
                            println!("(already initialized — .pond/config exists in S3)");
                        } else {
                            println!("Created .pond/config in S3.");
                        }
                        println!("\nTo use this storage in future commands:");
                        println!("  pond --root \"{}\" write users --json '[{{\"id\":1}}]' -m \"first\"", url);
                        println!("\nOr set POND_ROOT:");
                        println!("  export POND_ROOT=\"{}\"", url);
                    }
                    Err(e) => {
                        eprintln!("Error: cannot access S3 storage: {}", e);
                        eprintln!("Hint: check your credentials and endpoint URL.");
                        std::process::exit(1);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: invalid S3 URL: {}", e);
                std::process::exit(1);
            }
        }
    }
    #[cfg(not(feature = "s3"))]
    {
        eprintln!("Error: S3 support not compiled in.");
        eprintln!("Hint: rebuild with `cargo build` (default features include s3).");
        std::process::exit(1);
    }
}

fn cmd_write(storage: &UnifiedStorage, collection: &str, file: Option<String>,
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

    let active = storage.get_active_branch(collection);
    match write::write(storage.kernel(), collection, &active, &data,
                       &message.unwrap_or_default()) {
        Ok(hash) => println!("{}\t{}", &hash[..12], collection),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_read(storage: &UnifiedStorage, name_or_hash: &str, output: Option<String>) {
    let kernel = storage.kernel();
    // Try as collection name first (active branch), then as hash
    let active = storage.get_active_branch(name_or_hash);
    let data = match read::read(kernel, name_or_hash, &active) {
        Ok(data) => data,
        Err(_) => {
            // Try as hash or flat ref
            match kernel.read(name_or_hash) {
                Ok(data) => data,
                Err(e) => {
                    eprintln!("Error: '{}': {}", name_or_hash, e);
                    std::process::exit(1);
                }
            }
        }
    };

    if let Some(path) = output {
        std::fs::write(&path, &data).unwrap_or_else(|e| {
            eprintln!("Error: {}", e); std::process::exit(1);
        });
    } else {
        io::stdout().write_all(&data).unwrap_or_else(|e| {
            eprintln!("Error: {}", e); std::process::exit(1);
        });
    }
}

fn cmd_branch(storage: &UnifiedStorage, collection: &str, branch_name: &str) {
    let active = storage.get_active_branch(collection);
    match branch::branch(storage.kernel(), collection, branch_name, &active) {
        Ok(hash) => println!("Created branch '{}' at {}", branch_name, &hash[..12]),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_checkout(storage: &UnifiedStorage, collection: &str, branch_name: &str, new: bool) {
    if new {
        let active = storage.get_active_branch(collection);
        match branch::checkout_new(storage.kernel(), collection, branch_name, &active) {
            Ok(_) => {}
            Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
        }
    } else {
        match branch::checkout(storage.kernel(), collection, branch_name) {
            Ok(_) => {}
            Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
        }
    }
    storage.set_active_branch(collection, branch_name);
    persist_active_branch(storage, collection, branch_name);
    println!("Switched to branch '{}'", branch_name);
}

fn cmd_merge(storage: &UnifiedStorage, collection: &str, source_branch: &str,
             into: Option<String>, message: Option<String>) {
    let target = into.unwrap_or_else(|| storage.get_active_branch(collection));
    match branch::merge(storage.kernel(), collection, source_branch, &target,
                        &message.unwrap_or_default()) {
        Ok(hash) => println!("Merge commit {} ('{}' → '{}')", &hash[..12], source_branch, target),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_branches(storage: &UnifiedStorage, collection: &str) {
    let kernel = storage.kernel();
    let branches = branch::list_branches(kernel, collection);
    let active = storage.get_active_branch(collection);
    if branches.is_empty() {
        if kernel.resolve(collection).is_some() {
            println!("* main");
        } else {
            println!("(no branches)");
        }
        return;
    }
    for b in branches {
        let marker = if b == active { "*" } else { " " };
        let hash = kernel.resolve(&pond_storage::branch_ref(collection, &b))
            .unwrap_or_default();
        let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
        println!("{} {}\t{}", marker, b, prefix);
    }
}

fn cmd_history(storage: &UnifiedStorage, collection: &str, limit: usize) {
    let kernel = storage.kernel();
    let active = storage.get_active_branch(collection);
    let head = kernel.resolve(&pond_storage::branch_ref(collection, &active))
        .or_else(|| kernel.resolve(collection));

    match head {
        Some(h) => {
            let hist = commit::history(kernel, &h, limit);
            if hist.is_empty() {
                println!("(no commits)");
            } else {
                for (hash, commit) in hist {
                    let merge_marker = if commit.is_merge() { " (merge)" } else { "" };
                    println!("{}\t{}{}", &hash[..12], commit.message, merge_marker);
                }
            }
        }
        None => println!("(no commits)"),
    }
}

fn cmd_undo(storage: &UnifiedStorage, collection: &str, steps: usize) {
    let active = storage.get_active_branch(collection);
    match branch::undo(storage.kernel(), collection, &active, steps) {
        Ok(hash) => println!("Undo {} → now at {}", steps, &hash[..12]),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_revert(storage: &UnifiedStorage, collection: &str, commit_hash: &str) {
    let active = storage.get_active_branch(collection);
    match branch::revert(storage.kernel(), collection, &active, commit_hash) {
        Ok(()) => println!("Reverted to {}", &commit_hash[..12]),
        Err(e) => { eprintln!("Error: {}", e); std::process::exit(1); }
    }
}

fn cmd_ls(storage: &UnifiedStorage) {
    let kernel = storage.kernel();
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
        let active = storage.get_active_branch(&name);
        let hash = kernel.resolve(&pond_storage::branch_ref(&name, &active))
            .or_else(|| kernel.resolve(&name))
            .unwrap_or_default();
        let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
        println!("{}\t{}", prefix, name);
    }
}

fn cmd_cat(storage: &UnifiedStorage, hash: &str) {
    let kernel = storage.kernel();
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

use std::fs;
