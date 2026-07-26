# services/schema/

The **Schema Registry** — versioned schemas on the Names substrate.

## What it is

A thin layer over the kernel's Names substrate that implements the
Schema Evolution Algebra (§18 of `POND_FORMAL_ALGEBRAS.md`).

Per SE7: *"Schema Registry is a Naming convention. It uses the
existing Names substrate (Refs with prefix `__schema/`). No new
substrate, no new axiom."*

The Registry provides **storage and lookup**. The Lens provides the
**decoder and compatibility policy**. The Registry is a library, not
a kernel extension — the kernel still doesn't know what a schema is.

## Capabilities

- `register_schema(name, version, schema)` → `schema_hash`
- `get_schema(name, version)` → `schema`
- `latest_version(name)` / `list_versions(name)`
- `resolve_decoder(name)` → `(version, schema, decoder)`
- Migration: `v_old → v_new` via decode + re-encode

## Compatibility contracts (SE1–SE4)

- **SE1** Backward — new code reads old data (new fields have defaults)
- **SE2** Forward — old code reads new data (unknown fields skipped)
- **SE3** Writer schema is recorded in key prefix or blob header
- **SE4** Compatibility is the Lens's responsibility (kernel doesn't enforce)

Schema evolution is Parquet-native: missing columns become NULL.

## Files

| File | Purpose |
|---|---|
| `schema_registry.py` | `SchemaRegistry` — versioned schemas, compatibility checks, migration |
| `__init__.py` | Package exports |

## Storage model

Schemas are JSON-serializable dicts stored as kernel blobs.

- `__schema/{name}/v{version}` → `schema_hash`
- `__schema/{name}/latest` → `schema_hash` of latest version

## Architecture

Depends only on `pond-core` (per `REPO_ORGANIZATION.md` §7). Sits
between kernel and lenses — lenses query the registry to pick a
decoder; the kernel never inspects blob contents (Law 3 / Law 6).

## Dependencies

- `pond-core/` (kernel)
- Python stdlib only
