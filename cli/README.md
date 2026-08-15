# cli/ — The `pond` Command

The `pond` CLI binary. This is an APPLICATION, not a core library. It
depends on `core/kernel`, `core/storage`, and optionally `core/s3`.

## Design Principles

- **DuckDB philosophy:** one binary, no server, embedded
- **Git-style auto-discovery:** `pond init` creates a `.pond/` marker;
  subsequent commands find it by walking up from CWD (like `git` finds `.git/`)
- **Universal storage:** accepts any data format (JSON, CSV, raw bytes)
- **Simple:** delegates to `pond_storage` for all logic
- **Beautiful:** CLI is a thin UI layer over the storage library

## Build

```bash
# Build with S3 support (default)
cargo build -p pond_cli

# Build local-only (no S3 dependency)
cargo build -p pond_cli --no-default-features

# Release build (optimized)
cargo build --release -p pond_cli
```

The binary is at `target/debug/pond` or `target/release/pond`.

## Usage

### Local Filesystem (git-style)

```bash
# Initialize a Pond repository in the current directory
cd /var/lib/pond
pond init

# Now you can run commands WITHOUT --root — auto-discovery finds .pond/
pond write users --json '[{"id":1,"name":"alice"}]' -m "first"
pond read users
pond branch users dev
pond checkout -b users dev
pond merge users dev -m "merge dev"
pond history users
pond ls

# Auto-discovery works from subdirectories too (like git)
cd /var/lib/pond/subdir/nested
pond read users    # still finds /var/lib/pond/.pond/
```

### S3-Compatible Storage

```bash
# Initialize with an S3 URL — saves the URL to .pond/config
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
pond init "s3://my-bucket/prod?region=us-east-1"

# Now you can run commands WITHOUT --root — auto-discovery reads .pond/config
pond write users --json '[{"id":1}]' -m "first"
pond read users
pond ls

# Cloudflare R2
pond init "s3://bucket/prefix?region=auto&endpoint=https://<account>.r2.cloudflarestorage.com"

# MinIO
pond init "s3://bucket/prefix?region=us-east-1&endpoint=http://localhost:9000"
```

### Explicit Override (for scripts/CI)

```bash
# --root always overrides auto-discovery
pond --root /var/lib/pond read users
pond --root "s3://bucket/prefix?region=us-east-1" write users --json '[...]' -m "first"

# Or use POND_ROOT env var
export POND_ROOT="s3://bucket/prefix?region=us-east-1"
pond read users
```

## Storage Discovery (Priority Order)

The CLI resolves the storage location using this chain:

1. **`--root <url>`** — explicit override (highest priority)
2. **`POND_ROOT` env var** — explicit override
3. **`.pond/config` file** — auto-discovery (walks up from CWD)
4. **`.` (current directory)** — fallback (lowest priority)

The `.pond/` marker directory contains a `config` file:
- For local FS: `storage=local`
- For S3: `storage=s3://bucket/prefix?region=...&endpoint=...`

## Commands

| Command | Purpose |
|---|---|
| `init [location]` | Initialize a new Pond repository (creates `.pond/` marker) |
| `write <collection> [--json\|--file\|--bytes] -m <msg>` | Write data to a collection |
| `read <collection\|hash> [-o <file>]` | Read a collection or blob by hash |
| `branch <collection> <name>` | Create a new branch |
| `checkout <collection> <name> [-b]` | Switch branches (`-b` creates if missing) |
| `merge <collection> <source> [-i <target>] -m <msg>` | Merge source branch into target (default: active) |
| `branches <collection>` | List branches |
| `history <collection> [-l <limit>]` | Show commit history |
| `undo <collection> [steps]` | Undo last N commits |
| `revert <collection> <commit_hash>` | Revert to a specific commit |
| `ls` | List collections |
| `cat <hash>` | Print a blob by hash |
| `sql "<query>"` | Execute a SQL statement (one-shot) |
| `shell [--exec "<sql>"]` | Start the interactive REPL |
| `version` | Print version |

## Shell / REPL Mode

`pond shell` starts an interactive read-eval-print loop for exploring data
and running SQL without the per-invocation startup cost.

```bash
$ pond shell
Pond REPL v0.3.0
Type \h for help, \q to quit.
pond> SELECT * FROM users WHERE age > 30;
{
  "columns": ["id", "name", "age"],
  "rows": [ ... ]
}
pond> \l                          # list collections
pond> \d users                    # describe schema
pond> \b users                    # show branches
pond> \history                    # show last 100 commands
pond> \! ls -la                   # run a shell command
pond> \q                          # quit (or Ctrl+D / `exit`)
```

### `--exec` (startup SQL)

`--exec` runs a SQL statement on startup, then enters the REPL:

```bash
pond shell --exec "SELECT * FROM users"
```

When stdin is closed (EOF), the REPL exits after running the `--exec` query —
useful for piping:

```bash
pond shell --exec "SELECT * FROM users" < /dev/null
```

### Meta-commands

| Command | Action |
|---|---|
| `\l`, `\list` | List collections |
| `\d <name>`, `\describe <name>` | Show collection schema (columns, types, row groups) |
| `\b <name>` | Show branches for a collection |
| `\history` | Show command history (last 100 entries, in-memory only) |
| `\! <cmd>` | Execute a shell command (via `sh -c`) |
| `\h`, `\help`, `\?` | Show help |
| `\q`, `\quit`, `exit`, `quit` | Exit the REPL |

### Multi-line SQL

SQL statements accumulate across lines until a line ending with `;` is seen.
The prompt changes to `  ... ` for continuation lines:

```
pond> SELECT *
  ... FROM users
  ... WHERE age > 30;
{ "columns": [...], "rows": [...] }
pond>
```

Meta-commands (starting with `\`) execute immediately — no `;` needed. They do
not interfere with any in-progress SQL buffer.

### Ctrl+C / Ctrl+D

- **Ctrl+D** (EOF) exits the REPL cleanly.
- **Ctrl+C** terminates the process via the default SIGINT disposition
  (exit code 130). No external signal-handling crates are pulled in — the
  CLI stays dependency-light.

### History

Command history is kept in-memory only (a `Vec<String>` capped at 100
entries). It is not persisted to disk. Use `\history` to view it.

## S3 Support

S3 support is a cargo feature (`default = ["s3"]`). When enabled, the CLI
depends on `pond_s3` and accepts `s3://` URLs for `--root` / `POND_ROOT` /
`pond init`.

S3 credentials are read from the environment:
- `AWS_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY`)
- `AWS_SECRET_ACCESS_KEY` (or `AWS_SECRET_KEY`)
- `AWS_SESSION_TOKEN` (optional, for STS temporary credentials)

See [`../.env.example`](../.env.example) for the full configuration format.

## Tests

39 integration tests in `tests/cli_integration.rs`:
- init, write/read JSON, write from file, write from stdin
- dedup, ls, branch+merge, cat by prefix
- version, persistence across invocations
- **auto-discovery from subdirectory** (git-style)
- **auto-discovery creates .pond/ marker**
- write-rows / read-rows round-trips (WHERE filter, column projection)
- SQL: SELECT *, SELECT with WHERE + LIMIT
- **shell/REPL**: `--exec` SQL + exit, meta-commands (`\l`, `\d`, `\b`, `\h`,
  `\history`, `\!`, `\q`), multi-line SQL accumulation, history cap at 100,
  SQL errors don't crash the REPL, EOF exit, shell escape

```bash
cargo test -p pond_cli
```

## Binary Size

The release binary is < 10MB (DuckDB philosophy — small, self-contained).
