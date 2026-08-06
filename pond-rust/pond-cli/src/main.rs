// Pond CLI — the `pond` command
//
// A single-binary tool for content-addressed storage with branching,
// time-travel, and universal data support (JSON, CSV, raw bytes).
//
// Design principles:
//   - DuckDB philosophy: one binary, no server, embedded
//   - Universal storage: accepts any data format (JSON, CSV, raw bytes)
//   - Simple: mirrors the kernel's 3 primitives (write, read, ref)
//   - Beautiful: git-like commands (init, branch, merge, history)
//
// Usage:
//   pond init [path]                    Initialize a .pond/ directory
//   pond write <collection> <file>      Write a file's contents as a blob
//   pond write <collection> --json '{}' Write JSON data
//   pond write <collection> --bytes -   Write raw bytes from stdin
//   pond read <collection>              Read a collection's data
//   pond read <collection> -o <file>    Write output to a file
//   pond branch <collection> <name>     Create a branch
//   pond merge <collection> <name>      Merge a branch (currently: point branch at HEAD)
//   pond history <collection>           Show commit log (currently: show HEAD chain)
//   pond ls                             List all collections
//   pond cat <hash>                     Read a blob by hash
//   pond gc                             (TODO) Garbage collect dead blobs
//   pond version                        Print version

use clap::{Parser, Subcommand};
use pond_kernel::PondKernel;
use std::io::{self, Read, Write};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "pond")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Content-addressed storage with branching and time-travel")]
struct Cli {
    /// The pond root directory (defaults to current directory).
    /// The kernel looks for .pond/ inside this directory.
    #[arg(long, env = "POND_ROOT", global = true)]
    root: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new .pond/ directory
    Init {
        /// Path to initialize (defaults to current directory)
        #[arg(default_value = ".")]
        path: PathBuf,
    },

    /// Write data to a collection
    Write {
        /// Collection name (e.g. "users", "events", "dev/config")
        collection: String,

        /// Read data from a file (use "-" for stdin)
        #[arg(group = "input")]
        file: Option<String>,

        /// Provide JSON data directly on the command line
        #[arg(long, group = "input")]
        json: Option<String>,

        /// Read raw bytes from stdin (alternative to <file> "-")
        #[arg(long, group = "input")]
        bytes: bool,

        /// Commit message (stored in the ref metadata)
        #[arg(short, long)]
        message: Option<String>,
    },

    /// Read a collection's data
    Read {
        /// Collection name or hash
        name_or_hash: String,

        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
    },

    /// Create a branch of a collection
    Branch {
        /// Source collection name
        collection: String,

        /// Branch name
        branch_name: String,
    },

    /// Merge a branch back into its parent (currently: point parent at branch HEAD)
    Merge {
        /// Collection name
        collection: String,

        /// Branch name to merge
        branch_name: String,
    },

    /// Show the history of a collection
    History {
        /// Collection name
        collection: String,

        /// Maximum number of commits to show
        #[arg(short, long, default_value = "20")]
        limit: usize,
    },

    /// List all collections
    Ls,

    /// Read a blob by its hash
    Cat {
        /// The blob hash (64 hex chars)
        hash: String,
    },

    /// Print version info
    Version,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => {
            // If --root is set, init relative to it. Otherwise use the
            // <path> argument (defaults to ".").
            let base = cli.root.unwrap_or(path);
            cmd_init(&base);
        }
        Commands::Write { collection, file, json, bytes, message: _ } => {
            let root = cli.root.unwrap_or_else(|| PathBuf::from("."));
            cmd_write(&root, &collection, file, json, bytes);
        }
        Commands::Read { name_or_hash, output } => {
            let root = cli.root.unwrap_or_else(|| PathBuf::from("."));
            cmd_read(&root, &name_or_hash, output);
        }
        Commands::Branch { collection, branch_name } => {
            let root = cli.root.unwrap_or_else(|| PathBuf::from("."));
            cmd_branch(&root, &collection, &branch_name);
        }
        Commands::Merge { collection, branch_name } => {
            let root = cli.root.unwrap_or_else(|| PathBuf::from("."));
            cmd_merge(&root, &collection, &branch_name);
        }
        Commands::History { collection, limit } => {
            let root = cli.root.unwrap_or_else(|| PathBuf::from("."));
            cmd_history(&root, &collection, limit);
        }
        Commands::Ls => {
            let root = cli.root.unwrap_or_else(|| PathBuf::from("."));
            cmd_ls(&root);
        }
        Commands::Cat { hash } => {
            let root = cli.root.unwrap_or_else(|| PathBuf::from("."));
            cmd_cat(&root, &hash);
        }
        Commands::Version => {
            println!("pond {}", env!("CARGO_PKG_VERSION"));
        }
    }
}

// ---------------------------------------------------------------------------
// Command implementations
// ---------------------------------------------------------------------------

fn cmd_init(path: &PathBuf) {
    let pond_dir = path.join(".pond");
    let objects_dir = pond_dir.join("objects");
    match std::fs::create_dir_all(&objects_dir) {
        Ok(()) => {
            println!("Initialized empty Pond repository in {}", pond_dir.display());
        }
        Err(e) => {
            eprintln!("Error: failed to create {}: {}", pond_dir.display(), e);
            std::process::exit(1);
        }
    }
}

fn cmd_write(
    root: &PathBuf,
    collection: &str,
    file: Option<String>,
    json: Option<String>,
    bytes: bool,
) {
    let kernel = match PondKernel::new(root) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error: failed to open kernel at {}: {}", root.display(), e);
            eprintln!("Hint: run 'pond init' first");
            std::process::exit(1);
        }
    };

    let data: Vec<u8> = if let Some(j) = json {
        // Validate JSON
        match serde_json::from_str::<serde_json::Value>(&j) {
            Ok(_) => j.into_bytes(),
            Err(e) => {
                eprintln!("Error: invalid JSON: {}", e);
                std::process::exit(1);
            }
        }
    } else if bytes || file.as_deref() == Some("-") {
        // Read from stdin
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

    match kernel.write(&data) {
        Ok(hash) => {
            // Reference the hash under the collection's main branch ref.
            // This uses the branch ref convention: collections/{name}/_branches/main/commit
            // so that branch/merge/history commands work consistently.
            let main_ref = format!("collections/{}/_branches/main/commit", collection);
            if let Err(e) = kernel.reference(&main_ref, &hash) {
                eprintln!("Error: failed to reference {}: {}", collection, e);
                std::process::exit(1);
            }
            // Also set the flat ref for backward compatibility / simple lookups
            let _ = kernel.reference(collection, &hash);
            println!("{}\t{}", &hash[..12], collection);
        }
        Err(e) => {
            eprintln!("Error: failed to write: {}", e);
            std::process::exit(1);
        }
    }
}

fn cmd_read(root: &PathBuf, name_or_hash: &str, output: Option<String>) {
    let kernel = match PondKernel::new(root) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error: failed to open kernel: {}", e);
            std::process::exit(1);
        }
    };

    // Try the branch ref convention first (collections/{name}/_branches/main/commit),
    // then fall back to the flat ref, then try as a hash.
    let main_ref = format!("collections/{}/_branches/main/commit", name_or_hash);
    let data = if let Some(h) = kernel.resolve(&main_ref) {
        kernel.read_blob(&h)
    } else {
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

fn cmd_branch(root: &PathBuf, collection: &str, branch_name: &str) {
    let kernel = match PondKernel::new(root) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error: failed to open kernel: {}", e);
            std::process::exit(1);
        }
    };

    // Resolve the source collection to its current hash
    let source_ref = format!("collections/{}/_branches/main/commit", collection);
    let source_hash = match kernel.resolve(&source_ref) {
        Some(h) => h,
        None => {
            // Try the flat ref (older convention)
            match kernel.resolve(collection) {
                Some(h) => h,
                None => {
                    eprintln!("Error: collection '{}' not found", collection);
                    std::process::exit(1);
                }
            }
        }
    };

    // Create the branch ref pointing at the same hash
    let branch_ref = format!("collections/{}/_branches/{}/commit", collection, branch_name);
    match kernel.reference(&branch_ref, &source_hash) {
        Ok(()) => {
            println!("Created branch '{}' of '{}' at {}", branch_name, collection, &source_hash[..12]);
        }
        Err(e) => {
            eprintln!("Error: failed to create branch: {}", e);
            std::process::exit(1);
        }
    }
}

fn cmd_merge(root: &PathBuf, collection: &str, branch_name: &str) {
    let kernel = match PondKernel::new(root) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error: failed to open kernel: {}", e);
            std::process::exit(1);
        }
    };

    // Resolve the branch's current hash
    let branch_ref = format!("collections/{}/_branches/{}/commit", collection, branch_name);
    let branch_hash = match kernel.resolve(&branch_ref) {
        Some(h) => h,
        None => {
            eprintln!("Error: branch '{}' not found in '{}'", branch_name, collection);
            std::process::exit(1);
        }
    };

    // Point main at the branch's hash (simple merge: fast-forward)
    let main_ref = format!("collections/{}/_branches/main/commit", collection);
    match kernel.reference(&main_ref, &branch_hash) {
        Ok(()) => {
            println!("Merged '{}' into '{}' (fast-forward to {})",
                     branch_name, collection, &branch_hash[..12]);
        }
        Err(e) => {
            eprintln!("Error: failed to merge: {}", e);
            std::process::exit(1);
        }
    }
}

fn cmd_history(root: &PathBuf, collection: &str, limit: usize) {
    let kernel = match PondKernel::new(root) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error: failed to open kernel: {}", e);
            std::process::exit(1);
        }
    };

    // Currently the kernel only stores the current HEAD (no commit chain yet).
    // The commit chain is a Lens-level pattern (UnifiedStorage manages it).
    // For v0.1, we show the current HEAD and a note that history is
    // lens-level (future CLI versions will walk the commit chain).
    let main_ref = format!("collections/{}/_branches/main/commit", collection);
    match kernel.resolve(&main_ref) {
        Some(h) => {
            println!("{} (HEAD)", &h[..12]);
            println!();
            println!("Note: full commit history is managed at the lens level");
            println!("      (UnifiedStorage commit chain). The kernel only stores");
            println!("      the current HEAD. Future CLI versions will walk the");
            println!("      commit chain when the Rust UnifiedStorage is implemented.");
            let _ = limit; // accepted for API compat; not yet used
        }
        None => {
            // Try flat ref
            match kernel.resolve(collection) {
                Some(h) => {
                    println!("{} (HEAD, flat ref)", &h[..12]);
                }
                None => {
                    eprintln!("Error: collection '{}' not found", collection);
                    std::process::exit(1);
                }
            }
        }
    }
}

fn cmd_ls(root: &PathBuf) {
    let kernel = match PondKernel::new(root) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error: failed to open kernel: {}", e);
            std::process::exit(1);
        }
    };

    let names = kernel.list_names();
    if names.is_empty() {
        println!("(no collections)");
        return;
    }
    for name in names {
        // Show the hash prefix alongside the name
        let hash = kernel.resolve(&name).unwrap_or_default();
        let prefix = if hash.len() >= 12 { &hash[..12] } else { &hash };
        println!("{}\t{}", prefix, name);
    }
}

fn cmd_cat(root: &PathBuf, hash: &str) {
    let kernel = match PondKernel::new(root) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("Error: failed to open kernel: {}", e);
            std::process::exit(1);
        }
    };

    // Try the full hash first
    match kernel.read_blob(hash) {
        Ok(data) => {
            if let Err(e) = io::stdout().write_all(&data) {
                eprintln!("Error: failed to write stdout: {}", e);
                std::process::exit(1);
            }
            return;
        }
        Err(_) if hash.len() < 64 => {
            // Prefix match: scan the shard directory for a matching blob
            let shard_dir = root.join(".pond").join("objects").join(&hash[..2]);
            match std::fs::read_dir(&shard_dir) {
                Ok(entries) => {
                    let mut matches: Vec<String> = Vec::new();
                    for entry in entries.flatten() {
                        let fname = entry.file_name();
                        let fname_str = fname.to_string_lossy();
                        // filenames are "{hash}.bin"
                        if let Some(blob_hash) = fname_str.strip_suffix(".bin") {
                            if blob_hash.starts_with(hash) {
                                matches.push(blob_hash.to_string());
                            }
                        }
                    }
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
                                eprintln!("Error: failed to read blob: {}", e);
                                std::process::exit(1);
                            }
                        }
                    } else if matches.is_empty() {
                        eprintln!("Error: no blob with prefix '{}' found", hash);
                    } else {
                        eprintln!("Error: ambiguous prefix '{}' — matches {} blobs", hash, matches.len());
                        for m in matches {
                            eprintln!("  {}", m);
                        }
                    }
                    std::process::exit(1);
                }
                Err(_) => {
                    eprintln!("Error: blob '{}' not found", hash);
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("Error: blob '{}' not found: {}", hash, e);
            std::process::exit(1);
        }
    }
}
