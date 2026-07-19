# RFC-0004: View Composition and Interoperability

## Status

Draft — formalizing the biggest remaining conceptual gap.

## Abstract

The kernel has 3 primitives. Views are defined as 5-tuples (RFC-0001).
But how do Views COMPOSE? Can a StreamingView feed a SQLView? Can a
SQLView share data with a VectorView? This RFC formalizes View
composition and interoperability.

---

## 1. View Interoperability (cross-View data sharing)

### Principle: content-addressing enables zero-copy sharing

Any blob written by any View is readable by any other View via its hash.
The hash is global. No View "owns" data — data is owned by the kernel's
object store, and Views are interpretations.

### Three levels of interoperability

**Level 1: Hash-level sharing (already proven)**
- View A writes a blob → gets hash H
- View B reads blob at hash H → gets the same bytes
- No format compatibility needed — bytes are bytes
- Tested: SQL → Streaming → SQL, zero copies

**Level 2: Format-level sharing**
- View A writes data in format F (e.g., JSON, Arrow, Parquet)
- View B can read format F → interprets the data
- Requires both Views to agree on format F
- Example: SQLView and StreamingView both use JSON → can read each other's records

**Level 3: Schema-level sharing**
- View A writes data with schema S (e.g., {"id": INT, "name": TEXT})
- View B understands schema S → can query by field name
- Requires both Views to agree on schema S
- Example: SQLView and a future FeatureStoreView both understand {"id", "name", "embedding"}

### What the kernel guarantees (Level 1)
Content-addressing guarantees Level 1 unconditionally. Any blob, any View, zero copies.

### What Views must agree on (Levels 2 and 3)
Views must agree on format and schema. The kernel doesn't enforce this.
Views that want interoperability use shared conventions (View Author's Guide C1-C7).

---

## 2. View Composition

### Definition

View composition is the ability to use one View's output as another View's input.

```
View A produces data → View B consumes data → View C indexes data
```

### Pattern 1: Pipeline composition (producer → consumer)

```python
# SQLView produces rows
sql.insert("events", {"id": 1, "type": "click", "ts": "2024-01-01"})
sql.commit()

# StreamingView reads SQL's state and produces to a topic
for key, hash in sql.base.read_all().items():
    if key.startswith("events/"):
        row = json.loads(kernel.read_blob(hash))
        stream.produce(row["type"], json.dumps(row).encode())
stream.flush()

# VectorView reads the stream and indexes embeddings
for record in stream.consume():
    row = json.loads(record["value"])
    vector.insert(row["embedding"])
    vector.commit()
```

This is already possible. No kernel changes needed. Views read each
other's state via `base.read_all()` or `base.lookup(key)`.

### Pattern 2: Shared substrate (same data, different Views)

```python
# Write data once
shared_hash = kernel.write(data_bytes)

# SQLView references it
sql.base.stage("table/row1", shared_hash)
sql.commit()

# VectorView references the SAME hash
vector.base.stage("embeddings/1", shared_hash)
vector.commit()

# Both Views see the same bytes. Zero copies.
```

This is the "one copy, many interpretations" pattern. Already proven
in cross_view_proof.py.

### Pattern 3: Derived Views (materialized views)

A derived View is a View whose state is computed from another View's state.

```python
# SQLView has a "users" table
# A DerivedView computes "user_count_by_region" from users

class UserCountByRegionView:
    def __init__(self, kernel, sql_view):
        self.kernel = kernel
        self.sql = sql_view
        self.base = ProllyViewBase(kernel, "user_count_by_region")

    def refresh(self):
        """Recompute from SQL's current state."""
        users = self.sql.select_all("users")
        counts = {}
        for u in users:
            region = u.get("region", "unknown")
            counts[region] = counts.get(region, 0) + 1

        for region, count in counts.items():
            h = self.kernel.write(json.dumps({"region": region, "count": count}).encode())
            self.base.stage(f"counts/{region}", h)
        self.base.commit("refresh from users")
```

Derived Views are just Views that read from other Views. No kernel
support needed. The "refresh" operation is View-level (triggered by
the application, not the kernel).

### Pattern 4: View chaining (output → input → output)

```
Streaming → SQL → Vector → Search
```

Each stage reads the previous stage's output and writes its own.
All stages share the same kernel. All data is content-addressed.
No copies between stages.

```python
# Stage 1: Streaming ingests events
stream.produce("event", event_data)
stream.flush()

# Stage 2: SQL reads stream and inserts as rows
for record in stream.consume():
    sql.insert("events", json.loads(record["value"]))
sql.commit()

# Stage 3: Vector reads SQL and builds embeddings
for row in sql.select_all("events"):
    embedding = model.encode(row["text"])
    vector.insert(embedding)
    vector.commit()

# Stage 4: Search reads vector index
results = vector.search(query_embedding, k=5)
```

This is the LTAP pattern (streaming → lakehouse → analytics → serving)
implemented on ONE substrate with ONE copy of data.

---

## 3. Formal composition laws

### Composition Law 1: Hash transparency
If View A writes blob H, and View B reads blob H, View B gets exactly
the same bytes View A wrote. No transformation, no loss, no format
conversion. (Kernel Law 1: immutability + Law 2: addressability)

### Composition Law 2: Zero-copy sharing
Two Views can reference the same hash H. The blob is stored once.
Both Views read the same bytes. No duplication. (Kernel Law 1: dedup)

### Composition Law 3: Format independence
The kernel does not enforce format compatibility between Views.
View A may use JSON; View B may use Arrow. They can share data at
the hash level (Level 1) but not at the format level (Level 2)
unless they agree on a format.

### Composition Law 4: Schema independence
The kernel does not enforce schema compatibility between Views.
Views that want schema-level sharing (Level 3) must agree on a
schema convention. The kernel stores bytes; schemas are View-level.

### Composition Law 5: Derived View consistency
A derived View's state is a function of its source View's state.
If the source View hasn't changed, the derived View's state is valid.
If the source View changes, the derived View may be stale until
refreshed. The kernel does NOT track derivation dependencies —
Views manage their own refresh logic.

---

## 4. What the kernel does NOT provide for composition

1. **Automatic derivation tracking.** The kernel doesn't know that
   View B depends on View A. If View A changes, the kernel doesn't
   notify View B. Views implement their own refresh triggers.

2. **Cross-View transactions.** Updating View A and View B atomically
   is not guaranteed. The kernel's Reference is single-name; multi-name
   atomic updates require external coordination.

3. **Format conversion.** If View A writes JSON and View B needs
   Arrow, the kernel doesn't convert. Views must either agree on
   format or implement conversion at the View level.

4. **Schema enforcement.** The kernel doesn't validate that View B's
   data matches View A's schema. Views must validate their own inputs.

---

## 5. Implications for the View ecosystem

### View composition is free (no kernel cost)
Any View can read any other View's data. No kernel changes, no
infrastructure, no coordination. Just `kernel.read_blob(hash)`.

### View composition is uncoordinated (no consistency guarantees)
Views that derive from other Views may be stale. The kernel doesn't
track dependencies. Applications must trigger refreshes.

### View composition is format-dependent (at Level 2+)
Views that want to read each other's data meaningfully must agree on
format. The kernel guarantees byte-level sharing; Views guarantee
format-level compatibility.

### The "LTAP on one substrate" vision is achievable
Streaming → SQL → Vector → Search, all on one kernel, one copy of
data. This is the original vision: one substrate, many workloads,
zero duplication. The composition patterns above show how.
