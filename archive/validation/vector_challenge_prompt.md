You are a systems engineer who has never seen the Pond project before.
You are given the SDK specification and asked to implement a Vector
Database View on top of it.

You have NO access to any existing Pond code except the SDK interface
described below. Implement using ONLY the specification.

After implementing, honestly report:
1. Was the SDK sufficient? Could you implement without asking questions?
2. Where was it ambiguous?
3. What did you have to invent?
4. What was impossible or required guessing?
5. Rate the developer experience (1-10) and explain.

--- THE SDK SPECIFICATION (everything you receive) ---

Pond is a content-addressed immutable object runtime. The SDK provides
a `View` base class and an `IndexedView` class with automatic indexing.

## Layer 0: Kernel (frozen, 3 primitives)

```python
kernel.write(data: bytes) -> str          # returns 64-char hex hash (SHA-256)
kernel.read(hash_or_name: str) -> bytes   # read by hash or name
kernel.reference(name: str, hash: str)    # set mutable name -> hash
kernel.resolve(name: str) -> str | None   # resolve name to hash
kernel.list_names() -> list[str]          # list all names
```

Laws:
- Objects are immutable (once written, never change)
- Objects are content-addressed (hash = SHA-256 of bytes)
- Names are mutable (the only mutation)
- Same bytes always produce the same hash (dedup for free)

## Layer 1: View base class

```python
class View:
    def __init__(self, kernel, name: str)
    
    # Write path
    def put(self, key: str, data: Any) -> str      # encode + write + stage
    def put_raw(self, key: str, blob_hash: str)     # stage pre-existing blob
    def delete(self, key: str)                       # stage deletion
    def commit(self, message: str = "") -> str       # commit staged changes
    
    # Read path
    def get(self, key: str) -> Any | None            # lookup + decode
    def get_all(self) -> dict[str, Any]              # read all entries
    def keys(self) -> list[str]                      # list keys
    def exists(self, key: str) -> bool               # check existence
    def count(self) -> int                           # count entries
    
    # Version control
    def branch(self, name: str) -> str               # create branch (O(1))
    def checkout(self, name: str) -> None            # switch to branch
    def merge(self, name: str) -> str                # merge branch
    def undo(self, steps: int = 1) -> str           # undo N commits
    def history(self, limit: int = 20) -> list[dict] # commit history
    def diff(self, a: str, b: str) -> dict           # diff two commits
    
    # Indexing (Layer 2)
    def create_index(self, name: str, extractor: Callable) -> str
    def drop_index(self, name: str) -> bool
    def refresh_index(self, name: str, extractor: Callable) -> str
    def list_indexes(self) -> list[str]
    def lookup_by_index(self, name: str, key: str) -> Any | None
    
    # Serialization (override in subclass)
    def encode(self, data: Any) -> bytes             # default: JSON
    def decode(self, data: bytes) -> Any              # default: JSON
```

## Layer 2: IndexedView (auto-indexing)

```python
class IndexedView(View):
    def register_index(self, name: str, extractor: Callable,
                       mode: str = "lazy",        # "lazy" | "eager"
                       staleness_budget: int = 5)  # commits before rebuild
    def unregister_index(self, name: str)
    def find_by(self, index_name: str, index_key: str) -> Any | None
    def find_all_by(self, index_name: str, index_key: str) -> list[Any]
    def refresh_all_indexes(self) -> None
    def get_index_staleness(self, index_name: str) -> int
```

Index modes:
- "lazy": O(1) writes, index rebuilt on read when stale (default)
- "eager": index rebuilt on every commit (always fresh reads)

Index updates are incremental: only changed entries are merged (not full rebuild).
All index operations are METADATA ONLY — data blobs are never modified.

## Cross-View access

```python
class CrossView:
    @staticmethod
    def read_from(view: View, key: str) -> Any | None
    @staticmethod
    def read_all_from(view: View) -> dict[str, Any]
    @staticmethod
    def write_to(view: View, key: str, data: Any) -> str
    @staticmethod
    def share_blob(from_view: View, from_key: str, to_view: View, to_key: str) -> bool
    @staticmethod
    def pipe(from_view: View, to_view: View, transformer: Callable | None = None) -> int
```

--- END OF SPECIFICATION ---

YOUR TASK:

Implement a Vector Database View. It must support:
1. insert(id, vector: list[float], metadata: dict) — insert a vector
2. search(query: list[float], k: int = 5) — find k nearest neighbors (L2 distance)
3. get(id) — retrieve a vector by ID
4. delete(id) — delete a vector
5. list_vectors() — list all vectors
6. count() — count vectors

Additional requirements:
- Use IndexedView for auto-indexing (index by ID for O(log N) lookups)
- Vectors should be stored efficiently (not JSON — use struct.pack for floats)
- Search can be linear scan (no HNSW needed for this exercise)
- Support branching (create a branch, insert vectors, merge back)
- Support version history (see when vectors were added)

Use Python. Assume the SDK classes (View, IndexedView, CrossView) are
available as imports:
```python
from pond_minimal import PondMinimal
from auto_index import IndexedView
from view_sdk import CrossView
```

Write the implementation to /home/z/my-project/pond_repo/pond-vector/vector_view.py
Write the honest report to /home/z/my-project/pond_repo/validation/vector_report.md

Your final message should be the complete report.
