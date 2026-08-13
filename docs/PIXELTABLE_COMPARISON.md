# Pixeltable vs Pond — Research & Comparison

> **Date:** 2026-08-13
> **Purpose:** Research Pixeltable (pixeltable.com, github.com/pixeltable/pixeltable)
> and identify features Pond should consider adopting.
> **Sources:** pixeltable.com blog (4-layer storage architecture), docs.pixeltable.com
> (Why Pixeltable, Version Control & Lineage, Changelog), github.com/pixeltable/pixeltable
> README, Backblaze engineering blog. Cross-referenced against Pond's own
> `README.md`, `docs/PROJECT_OVERVIEW.md`, `docs/HONEST_COMPETITOR_COMPARISON.md`.

---

## TL;DR

Pixeltable and Pond solve **different problems** with **overlapping primitives**:

| Dimension | Pixeltable | Pond |
|---|---|---|
| **Primary goal** | Declarative data infrastructure for **multimodal AI apps** | Unified content-addressed storage kernel for **all workloads** |
| **Scope** | Application-layer platform (store + orchestrate + index + serve + version) | Storage kernel + lenses (storage, versioning, concurrency) |
| **Language** | Python-only | Rust core + Python/Go/C ABI bindings |
| **Metadata engine** | Embedded **PostgreSQL** (ACID, SQL) | JSON commit blobs + CollectionManifest (no SQL on metadata) |
| **Media handling** | First-class `pxt.Image/Video/Audio/Document` column types; files stored by reference, never as BLOBs | Raw bytes via `write()`/`read()`; no typed media columns |
| **Concurrency** | Single-writer (Postgres ACID); **no CRDT** | **CRDT G-Set shards, no CAS, multi-writer** ✅ |
| **Branching/merge** | **No Git-style branch/merge** — only linear versioning + named snapshots + revert | **Full branch/checkout/merge** ✅ |
| **Versioning** | Automatic, every-operation, time-travel via `table:N`, lineage DAG | Manifest-based branch/merge/history/revert; no computed-column lineage |
| **Derived data** | **Computed columns** (declarative, incremental, cached, lineage-tracked) — Pixeltable's killer feature | Lenses (workload interpretations), no equivalent "auto-recompute on insert" |
| **Query API** | Fluent SQL-like builder: `.where().order_by().select().limit().collect()` | `read_rows(predicates=...)` + full `.sql()` strings |
| **AI integration** | 30+ providers (OpenAI/Anthropic/Gemini/HF) callable from computed columns | None (storage-only) |
| **Serving** | `pxt serve` generates REST endpoints from schema/TOML | None |
| **UI** | `pxt dashboard` (browse tables, preview media, lineage graph) | None |

**Bottom line:** Pixeltable is a **shipped, AI-focused application platform** built on top
of embedded Postgres. Pond is a **lower-level, more general storage kernel** with two
genuine architectural advantages Pixeltable lacks: **CRDT multi-writer concurrency** and
**true Git-style branch/merge**. Pond should *not* try to clone Pixeltable's Postgres-backed
application layer, but it **should** borrow several ideas — most importantly **computed
columns with lineage**, **typed media references**, **incremental indexes that stay in
sync**, and a **fluent query builder** — implemented natively on Pond's content-addressed
substrate rather than via an embedded SQL DB.

---

## 1. Pixeltable Core Architecture & Data Storage

### 1.1 The Four-Layer Storage Architecture
*(Source: pixeltable.com/blog/understanding-pixeltable-storage-architecture, Sep 2025)*

Pixeltable deliberately **separates metadata from media** across four complementary layers:

| Layer | Stores | Persistence |
|---|---|---|
| **1. Embedded Postgres** | Schema, version history, structured data (strings/numbers/JSON), media file **paths & URLs** | Persistent local FS |
| **2. Media Store** | AI-generated / transformed media (DALL-E images, resized videos, UDF outputs) | Persistent local FS **or cloud** (S3/GCS/Azure) |
| **3. File Cache** | Downloaded remote-media files (LRU eviction, configurable size) | Local FS only (cache) |
| **4. Temp Store** | Ephemeral query-time media (frame extraction in SELECT, non-persisted transforms) | Local FS only (auto-cleaned) |

**Key design principles:**
- **Media files are NEVER stored in Postgres** — only references (paths/URLs). This avoids BLOB performance issues. Inserting a local file stores ~50 bytes (the path string); the file itself is never moved or copied.
- **Lazy media loading**: queries over metadata run entirely in Postgres (milliseconds); media bytes are only fetched when actually accessed for processing.
- **Smart caching**: remote URLs are downloaded once to the File Cache (LRU), validated, and reused across many operations.
- **ACID for metadata**: Postgres gives transactional consistency, advanced query optimization, and indexing for the metadata layer — without paying BLOB costs.
- **Hybrid storage backends**: different columns can use different Media Store backends (local + cloud in the same table).

### 1.2 What this means for Pond
Pond's current model stores **raw bytes by collection name** (`write('images/logo.png', png_bytes)`). It has no separate metadata index that can be queried without loading the blob, and no smart caching of remote references. Pixeltable's separation of "queryable metadata" from "media blobs" — with lazy loading and an LRU file cache — is directly applicable. (See §7 recommendations.)

---

## 2. Key Features — Especially Media (Video / Images / Audio / Documents)

### 2.1 Native multimodal column types
```python
t = pxt.create_table('media', {
    'img':      pxt.Image,
    'video':    pxt.Video,
    'audio':    pxt.Audio,
    'document': pxt.Document,
    'metadata': pxt.Json,
})
```
A single table mixes **structured** (`String`, `Int`, `Float`, `Json`, `Timestamp`) and
**unstructured** (`Image`, `Video`, `Audio`, `Document`) columns. The media columns store
**file paths or URLs**; the actual bytes live externally (local FS, S3, GCS, Azure, R2, B2, Tigris).

### 2.2 Iterators — exploding media into rows (the "view" primitive)
```python
from pixeltable.functions.video import frame_iterator
from pixeltable.functions.document import document_splitter

# Video → 1 row per frame at 0.5 fps
frames = pxt.create_view('frames', videos,
    iterator=frame_iterator(video=videos.video, fps=0.5))

# Document → overlapping chunks for RAG
chunks = pxt.create_view('chunks', docs,
    iterator=document_splitter(document=docs.doc,
        separators='sentence,token_limit', overlap=50, limit=500))
```
`create_view(iterator=...)` is the canonical way to turn one media row into many derived
rows (video→frames, audio→segments, doc→chunks, JSON list→rows). Custom iterators via
`@pxt.iterator`.

### 2.3 Media-processing built-ins
Built-in functions on media columns: `video.get_duration()`, `video.extract_frame(ts)`,
`image.thumbnail((320,320))`, etc. — all callable inside computed columns or SELECT queries.

---

## 3. Structured + Unstructured in the Same Table

Pixeltable's answer is **typed columns in one relational table**. Because the table is
backed by Postgres for metadata and an external media store for bytes, you get:
- **One query** can filter on structured metadata *and* trigger media processing:
  ```python
  large_videos = (videos
      .where(pxt.functions.video.get_duration(videos.video) > 600)
      .where(videos.metadata['category'] == 'training')
      .select(videos.title, videos.metadata,
              duration=pxt.functions.video.get_duration(videos.video))
      .collect())   # runs in Postgres; no media bytes touched
  ```
- The same table can later drive a vision call that *does* load the media:
  ```python
  frames.add_computed_column(analysis=openai.chat_completions(
      messages=[{'role':'user','content':[
          {'type':'text','text':'Describe this frame'},
          {'type':'image_url','image_url':frames.frame}]}],
      model='gpt-4o-mini')))
  ```

The unification is at the **schema/query layer**, not the storage layer: Postgres holds
the path; the media store/cache/temp store hold the bytes on demand.

---

## 4. Query API

Pixeltable exposes a **fluent, SQL-like builder** (not raw SQL strings, though SQL-like
syntax is the model):

```python
# Filter + order + project + limit, then materialize
results = (t.where(t.score > 0.8)
            .order_by(t.timestamp)
            .select(t.image, score=t.score)
            .limit(10)
            .collect())

# Semantic search: similarity + metadata filter in ONE query
t.add_embedding_index('img', embedding=clip.using(model_id='openai/clip-vit-base-patch32'))
sim = t.img.similarity(string='cat playing with yarn')
results = (t.where(t.category == 'pets')        # metadata filter
            .order_by(sim, asc=False)            # vector ranking
            .select(t.img, t.category, score=sim)
            .limit(10).collect())

# Test-before-commit workflow: sample, apply UDF ephemerally, then commit
t.sample(5).select(t.text, summary=summarize(t.text)).collect()  # nothing stored
t.add_computed_column(summary=summarize(t.text))                  # now persisted
```

Characteristics:
- **Chainable, Python-native** (no string parsing); column references are first-class objects (`t.score > 0.8`).
- **Lazy until `.collect()`** — builder constructs a plan, executed on collect.
- **Vector + metadata in one query** — no separate vector-DB-then-filter pipeline.
- **`pxt.query` decorator** turns a query into a reusable, named function (also exposable as an HTTP route).

Pond's current equivalents: `read_rows(predicates=[('id','>',1)])` (predicate list) and
`s.sql("SELECT ...")` (full SQL string). Neither is as ergonomic for interactive/exploratory
multimodal work.

---

## 5. Versioning, Branching, CRDT

### 5.1 Automatic versioning + time travel
*(Source: docs.pixeltable.com/platform/version-control)*

- **Every mutating operation creates a new version** — `insert()`, `update()`, `delete()`,
  `add_column()`, `add_computed_column()`, `drop_column()`, `rename_column()`. No config
  required; always on.
- **Time travel**: `pxt.get_table('demo/products:1')` returns a read-only handle to
  version 1. Compare with current via `products.collect()`.
- **`history()`**: DataFrame of all versions (timestamp, change type, row counts, schema diffs).
- **`revert()`**: undo the latest version (permanent; cannot revert past version 0 or a
  snapshot reference).
- **`get_versions()`**: programmatic list of version metadata.

### 5.2 Named snapshots
```python
baseline = pxt.create_snapshot('demo/products_baseline', products)
# source table mutates; snapshot stays frozen
products.insert([...]); products.count()   # 3
baseline.count()                            # 2 (frozen)
```
Snapshots are **named, persistent, independent point-in-time copies** — they survive even
if the source table is deleted.

### 5.3 Data lineage (computed-column DAG)
Pixeltable tracks the **complete lineage** of derived data:
- **Schema lineage**: every computed column records its expression + dependencies
  (`discounted_with_tax → discounted → price`).
- **View lineage**: views track source tables (`expensive_products → products`).
- **UDF versions**: the expression DAG, UDF versions, and model versions are all stored,
  enabling reproducibility (you can reconstruct exactly what produced a given row).

### 5.4 What Pixeltable does NOT have
Cross-checked against the official changelog (54K chars, through v0.7.0, Aug 2026) and the
version-control comparison table:

| Feature | Pixeltable |
|---|---|
| **Git-style branch / merge** | ❌ Not present. The version-control comparison table leaves branching "N/A" for Pixeltable. (A third-party Backblaze blog loosely calls snapshots "branching," but the official docs describe only linear versioning + snapshots + revert. There is no `merge()` of divergent histories.) |
| **CRDT / concurrent multi-writer** | ❌ Not present. Single-writer model backed by Postgres ACID transactions. No conflict-free replicated data types. |
| **Storage independence (no embedded DB)** | ❌ Depends on embedded Postgres for metadata. |
| **Content-addressed blob dedup** | ❌ Media files are stored by path, not content hash; no automatic dedup of identical media. |

> **This is the single most important finding for Pond.** Pixeltable is a strong
> *application platform* but a weaker *storage substrate*: no CRDT, no branch/merge,
> no content-addressing, no storage-backend independence from an embedded SQL DB.
> These are exactly Pond's documented architectural strengths (see Pond's
> `HONEST_COMPETITOR_COMPARISON.md` §5, §"Where Pond DOES win"). **Pond should not
> abandon them to imitate Pixeltable.**

---

## 6. Side-by-Side: Pixeltable vs Pond

| Capability | Pixeltable | Pond | Edge |
|---|---|---|---|
| Typed media columns (Image/Video/Audio/Doc) | ✅ native | ❌ raw bytes only | **Pixeltable** |
| Media stored by reference, never BLOB | ✅ explicit | ⚠️ implicit (bytes = blob) | **Pixeltable** |
| Lazy media loading + LRU file cache | ✅ File Cache layer | ❌ loads whole blob | **Pixeltable** |
| Structured + unstructured in one queryable table | ✅ | ⚠️ separate `write_rows` vs `write` | **Pixeltable** |
| Computed columns (declarative, incremental, cached) | ✅ killer feature | ❌ no equivalent | **Pixeltable** |
| Lineage DAG (expression + UDF + model versions) | ✅ automatic | ❌ version history only | **Pixeltable** |
| Iterators (video→frames, doc→chunks) | ✅ `create_view(iterator=)` | ❌ | **Pixeltable** |
| Incremental embedding indexes that auto-sync | ✅ `add_embedding_index` | ❌ IVF doesn't reduce I/O (per Pond's own honest doc) | **Pixeltable** |
| Fluent SQL-like query builder | ✅ `.where().select().collect()` | ⚠️ predicate lists + raw SQL strings | **Pixeltable** |
| 30+ AI provider integrations | ✅ | ❌ storage-only | **Pixeltable** |
| HTTP serving from schema | ✅ `pxt serve` | ❌ | **Pixeltable** |
| Dashboard UI (browse/preview/lineage) | ✅ `pxt dashboard` | ❌ | **Pixeltable** |
| Per-row error tracking on computed cols | ✅ `pxt errors` | ❌ | **Pixeltable** |
| **CRDT multi-writer (no CAS)** | ❌ | ✅ G-Set shards | **Pond** |
| **Git-style branch / checkout / merge** | ❌ | ✅ | **Pond** |
| **Storage-backend independence (no embedded DB)** | ❌ (needs Postgres) | ✅ local/S3/R2/MinIO/GCS | **Pond** |
| **Content-addressed blob dedup** | ❌ | ✅ | **Pond** |
| **Multi-language bindings** | ❌ Python-only | ✅ Python/Go/C ABI/Rust CLI | **Pond** |
| **Workload-agnostic lens architecture** | ⚠️ AI/ML-focused | ✅ KV/vector/streaming/lakehouse/OLTP | **Pond** |
| Embedded Postgres SQL on metadata | ✅ | ❌ JSON manifests only | **Pixeltable** (for metadata queries) |

---

## 7. What Pond Can Learn from Pixeltable (Prioritized Recommendations)

Ranked by impact-to-effort, and explicitly noting where Pond should **diverge** from
Pixeltable's implementation (because Pond's substrate is content-addressed, not Postgres-backed).

### Tier 1 — High impact, fits Pond's architecture

**1. Computed columns with lineage tracking (Pond's biggest gap).**
Pixeltable's `add_computed_column(expr)` runs incrementally on new rows, caches results,
retries failures, and records the expression DAG + UDF version. This is the feature that
makes Pixeltable "declarative." Pond should add a **derived-column / materialized-view lens**
where:
- A column is defined as `f(other_columns)` (Python UDF or SQL expression).
- On `write_rows`/`upsert_shard`, the kernel marks dependent rows "stale" and recomputes
  lazily or eagerly (LAZY/EAGER/MANUAL — Pond already has these index modes; reuse them).
- The commit blob stores the **expression DAG + UDF hash** so any row's provenance is
  reconstructable (this is "lineage").
- *Implement on Pond's substrate, NOT via Postgres* — store the DAG in the manifest, mark
  staleness via row-level version vectors (which Pond already has via `_version` HLC).

**2. Typed media references + lazy loading.**
Add a **media-reference column type** (or a small `MediaLens`) that stores:
- A resolver (local path / `s3://` URL / content-hash) — Pond already content-addresses, so
  the reference can be a **hash** (better than Pixeltable's path, because identical media
  dedup automatically).
- MIME/type tag (image/video/audio/document) + lightweight metadata (duration, dimensions).
- **Lazy materialization**: `read_rows` returns the reference + metadata without fetching
  bytes; a separate `materialize(ref)` / `.bytes` accessor pulls the blob (with an LRU cache
  like Pixeltable's File Cache). This directly addresses Pond's "loads whole blob" weakness.

**3. Iterators as a view primitive.**
Port Pixeltable's `create_view(iterator=...)` pattern as a Pond lens/extension:
`video→frames`, `document→chunks`, `audio→segments`, `json-list→rows`. This is the bridge
between "store a media blob" and "query/train on its parts." It composes naturally with
computed columns (#1): chunk a doc, then embed each chunk, all declaratively.

**4. Incremental indexes that stay in sync (fix Pond's IVF).**
Pixeltable's `add_embedding_index` auto-maintains as data changes. Pond's honest doc admits
IVF "reads ALL vectors" and doesn't reduce I/O. The Pixeltable model to copy:
- Index is defined once on a column; new/changed rows trigger incremental index updates
  (Pond already has ProllyTree commit-diff for O(changed) refresh — use it for vectors too).
- Store per-cluster blob references so search fetches only `n_probe` clusters (the fix
  Pond's own code comment describes but hasn't implemented).

### Tier 2 — Medium impact, ergonomic wins

**5. Fluent query builder.**
Add a chainable builder alongside `read_rows`/`.sql()`:
```python
s.query('users').where("age >= 18").where("city = 'NYC'").select('name','age').limit(10).collect()
```
Keeps `.sql()` for power users; the builder is for interactive/exploratory work where
Pixeltable shines. Pure SDK sugar — no kernel change.

**6. Named snapshots (distinct from branches).**
Pond conflates "snapshot" and "branch" (a branch is mutable). Pixeltable's
`create_snapshot()` is **frozen and independent** — it survives source deletion. Pond
should add a `snapshot()` op that creates an immutable, named ref to a manifest (cheap:
just a ref, no copy, thanks to content-addressing). This is subtly different from
`branch()` (mutable) and useful for "freeze training set before a model run."

**7. Per-row error tracking.**
Pixeltable's `pxt errors my_table` returns rows where a computed column failed, with a
queryable `errormsg` per cell. As Pond adds computed columns (#1), it should persist
per-row error state (a tombstone-like marker) so failed transformations are visible and
re-runnable, not silently dropped.

**8. Test-before-commit workflow.**
`sample(N).select(..., computed=udf(...)).collect()` evaluates a transformation ephemerally
before promoting it to a stored computed column. Cheap to add on top of #1 and #5 — and it
matches Pond's "beautiful API" design goal.

### Tier 3 — Lower priority / diverge intentionally

**9. AI provider integrations & serving (`pxt serve`, 30+ providers).**
Useful for adoption but **not core to Pond's storage mission**. If pursued, implement as a
**separate application layer / extension** (Pond already has an `extensions/` dir and a
`semantic/` layer concept), keeping the kernel pure. Do not embed AI logic into the storage
kernel the way computed columns blur that line in Pixeltable.

**10. Embedded Postgres for metadata queries.**
Pixeltable gets fast SQL-on-metadata "for free" from Postgres. Pond deliberately avoids an
embedded DB (no SQLite in production path; "no CAS anywhere"). **Do not adopt Postgres** —
it would violate Pond's storage-independence principle. Instead, invest in the
LakehouseLens native Arrow path (Pond's own Tier-1 fix) so metadata predicates push down
into PND2 scans. The lesson from Pixeltable is "metadata queries must be fast without
loading media" — Pond can achieve that via pruning + projection, not via Postgres.

**11. Dashboard UI.**
Nice-to-have for demos. Low priority; can be a thin web view over the existing CLI/lens APIs.

### What Pond should NOT copy from Pixeltable
- **Do not** adopt an embedded SQL DB for metadata (violates storage-independence).
- **Do not** drop CRDT for single-writer ACID (Pond's multi-writer is a real differentiator).
- **Do not** replace branch/merge with linear versioning + snapshots (Pond's merge is stronger).
- **Do not** store media by path instead of hash (Pond's content-addressing gives free dedup;
  Pixeltable cannot dedup identical media files).

---

## 8. Where Pond is Already Ahead (defend these)

Per Pond's own `HONEST_COMPETITOR_COMPARISON.md`, these are genuinely novel vs Pixeltable
*and* vs the broader market:

1. **CRDT multi-writer on object storage** — Pixeltable has no answer; Postgres is single-writer.
2. **Git-style branch/checkout/merge** — Pixeltable has only linear versioning + snapshots; no merge of divergent histories.
3. **Storage-backend independence (no embedded DB)** — Pixeltable is permanently coupled to Postgres.
4. **Content-addressed blob dedup** — Pixeltable stores media by path; identical files are duplicated.
5. **Multi-language bindings (Rust core, Go, C ABI, Python)** — Pixeltable is Python-only.
6. **Workload-agnostic lens architecture** — Pixeltable is AI/ML-specific; Pond's lenses span KV/vector/streaming/lakehouse/OLTP.

These should be **emphasized in positioning**, not traded away for Pixeltable-style features.

---

## 9. Concrete Next Actions

| # | Action | Tier | Owner area | Effort |
|---|---|---|---|---|
| 1 | Design a **derived-column lens** (expression DAG + UDF hash in manifest, stale-row marking via `_version` HLC, LAZY/EAGER/MANUAL recompute) | Tier 1 | `lenses/` + `core/storage` | L |
| 2 | Add a **media-reference column type** (hash + MIME + lightweight metadata, lazy `materialize()`, LRU blob cache) | Tier 1 | `core/codec` + new `lenses/media/` | M |
| 3 | Implement **iterators** (`frame_iterator`, `document_splitter`, `list_iterator`) as a view lens composing with #1 | Tier 1 | `lenses/` | M |
| 4 | Fix **IVF I/O** (per-cluster blob refs) + make indexes auto-sync via ProllyTree commit-diff | Tier 1 | `lenses/vector/` | M |
| 5 | Add a **fluent query builder** SDK layer over `read_rows`/`.sql()` | Tier 2 | `bindings/python/sdk` | S |
| 6 | Add a **`snapshot()` op** (immutable named ref, distinct from mutable `branch()`) | Tier 2 | `core/storage` | S |
| 7 | Persist **per-row error state** for computed-column failures | Tier 2 | `lenses/` | S |
| 8 | Add **`sample().select(computed=…)` ephemeral eval** before promoting to a stored column | Tier 2 | SDK | S |
| 9 | (Optional) **AI-integration extension** + `pond serve` as a separate app layer, NOT in kernel | Tier 3 | `extensions/` + new `services/` | L |

---

## 10. Sources

- **Pixeltable storage architecture (4 layers):** https://pixeltable.com/blog/understanding-pixeltable-storage-architecture (Sep 29, 2025)
- **Why Pixeltable? (core primitives, computed columns, views, indexes):** https://docs.pixeltable.com/overview/pixeltable
- **Version Control and Lineage (versioning, snapshots, time travel, lineage DAG, comparison table):** https://docs.pixeltable.com/platform/version-control
- **Changelog (verified no branch/CRDT features through v0.7.0, Aug 2026):** https://docs.pixeltable.com/changelog/changelog
- **GitHub README (core capabilities, deployment patterns, CLI serving):** https://github.com/pixeltable/pixeltable
- **Backblaze engineering blog (third-party "branching/snapshots" claim):** https://www.backblaze.com/blog/a-developers-guide-to-migrating-multimodal-ai-training-data-and-putting-it-to-work-with-pixeltable (Jan 21, 2026)
- **Pond internal:** `README.md`, `docs/PROJECT_OVERVIEW.md`, `docs/HONEST_COMPETITOR_COMPARISON.md`
