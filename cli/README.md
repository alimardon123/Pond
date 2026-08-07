# cli/ — The `pond` Command

The `pond` CLI binary. This is an APPLICATION, not a core library. It
depends on `core/kernel`, `core/storage`, and optionally `core/s3`.

## Design Principles

- **DuckDB philosophy:** one binary, no server, embedded
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

```bash
# Local filesystem
pond init /var/lib/pond
pond --root /var/lib/pond write users --json '[{"id":1,"name":"alice"}]' -m "first"
pond --root /var/lib/pond read users
pond --root /var/lib/pond branch users dev
pond --root /var/lib/pond checkout users dev
pond --root /var/lib/pond merge users dev -m "merge dev"
pond --root /var/lib/pond history users
pond --root /var/lib/pond branches users
pond --root /var/lib/pond ls
pond --root /var/lib/pond cat <hash>
pond --root /var/lib/pond undo users 1
pond --root /var/lib/pond revert users <commit_hash>

# S3-compatible storage (AWS S3, R2, MinIO, etc.)
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
pond --root "s3://bucket/prefix?region=us-east-1" init
pond --root "s3://bucket/prefix?region=us-east-1" write users --json '[...]' -m "first"

# Cloudflare R2
pond --root "s3://bucket/prefix?region=auto&endpoint=https://<account>.r2.cloudflarestorage.com" init

# MinIO
pond --root "s3://bucket/prefix?region=us-east-1&endpoint=http://localhost:9000" init

# Or use POND_ROOT env var instead of --root
export POND_ROOT="s3://bucket/prefix?region=us-east-1"
pond init
pond write users --json '[...]' -m "first"
```

## Commands

| Command | Purpose |
|---|---|
| `init [path]` | Initialize a new Pond repository (local FS) or verify S3 connectivity |
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
| `version` | Print version |

## S3 Support

S3 support is a cargo feature (`default = ["s3"]`). When enabled, the CLI
depends on `pond_s3` and accepts `s3://` URLs for `--root` / `POND_ROOT`.

S3 credentials are read from the environment:
- `AWS_ACCESS_KEY_ID` (or `AWS_ACCESS_KEY`)
- `AWS_SECRET_ACCESS_KEY` (or `AWS_SECRET_KEY`)
- `AWS_SESSION_TOKEN` (optional, for STS temporary credentials)

See [`../.env.example`](../.env.example) for the full configuration format.

## Tests

15 integration tests in `tests/cli_integration.rs`:
- init, write/read JSON, write from file, write from stdin
- dedup, ls, branch+merge, cat by prefix
- version, persistence across invocations

```bash
cargo test -p pond_cli
```

## Binary Size

The release binary is < 10MB (DuckDB philosophy — small, self-contained).
