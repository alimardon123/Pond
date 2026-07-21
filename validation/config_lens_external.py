"""
ConfigLens — an independent implementation of a Pond Lens for
application configuration management.

Built from RFC-0013 (The Lens Interpretation Contract) and
RFC-0012 (The Lens Architecture) ONLY. The author had never seen
the Pond project before.

This file contains three things, layered exactly as the contract
describes:

  1. ContextResolver     (~30 LOC) — code-level codec registry.
                           Section 8 of the contract.
  2. ContextLens         (~25 LOC) — the Lens override that delegates
                           encode/decode to the resolver by key prefix.
                           Sections 3.2, 3.3, 5, 6, 8 of the contract.
  3. ConfigLens          — the actual configuration-management Lens.
                           Registers the `config/` prefix with a JSON
                           codec and adds domain methods.

Contract compliance checklist (RFC-0013 §11):
  [x] Uses key prefix convention (`config/`)
  [x] Registers its codec with the Resolver
  [x] Does NOT write envelope/header bytes into blobs (pure JSON)
  [x] Does NOT write manifest or enable_view metadata
  [x] Can read blobs written by other Lenses (via the Resolver)
  [x] Provides get_raw(key) for raw-byte fallback
  [x] Supports branching, checkout, history (inherited from base Lens)
  [x] Passes the falsification-style scenario at the bottom of this file
"""

from __future__ import annotations

import os
import sys
import json
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional

# --- Wire up imports -------------------------------------------------------
# We import the Lens base class from the SDK. Per the task, we looked at
# lens_sdk.py ONLY for the import path and constructor signature
# (Lens = View; View.__init__(self, kernel, name)). Everything else here
# is built from the contract.
_HERE = os.path.dirname(os.path.abspath(__file__))
_POND_SDK = os.path.normpath(os.path.join(_HERE, "..", "pond-sdk"))
sys.path.insert(0, _POND_SDK)

from lens_sdk import Lens            # noqa: E402  (base Lens = View)
from pond_minimal import PondMinimal  # noqa: E402  (the kernel)


# ===========================================================================
# 1. ContextResolver  (RFC-0013 §8)
#
#    A CODE-level (not DATA-level) codec registry. Maps key prefix -> codec.
#    ~30 LOC. Lives in the application, not in the kernel.
# ===========================================================================

class ContextResolver:
    """Maps key prefixes to (encode, decode) codecs.

    The Resolver is the ONLY place that knows about codecs. The kernel
    never sees a codec_id, an envelope, or a type tag. Decode falls back
    to raw bytes if no codec matches or if decode raises.
    """

    def __init__(self) -> None:
        self._codecs: dict[str, tuple[Callable[[Any], bytes], Callable[[bytes], Any]]] = {}

    def register(self, prefix: str, encode: Callable[[Any], bytes],
                 decode: Callable[[bytes], Any]) -> None:
        self._codecs[prefix] = (encode, decode)

    def _codec_for(self, key: str):
        # Longest-prefix match wins, so "config/" beats a hypothetical "c".
        best = None
        for prefix, codec in self._codecs.items():
            if key.startswith(prefix) and (best is None or len(prefix) > len(best[0])):
                best = (prefix, codec)
        return best[1] if best else None

    def encode_for_key(self, key: str, data: Any) -> bytes:
        codec = self._codec_for(key)
        if codec is None:
            raise ValueError(f"No codec registered for key prefix: {key!r}")
        return codec[0](data)

    def decode_for_key(self, key: str, raw: bytes) -> Any:
        codec = self._codec_for(key)
        if codec is None:
            return raw                      # fallback: raw bytes
        try:
            return codec[1](raw)
        except Exception:
            return raw                      # fallback: raw bytes on decode failure


# ===========================================================================
# 2. ContextLens  (RFC-0013 §3.2, §5, §6, §8)
#
#    A Lens that delegates encode/decode to a ContextResolver based on
#    the key prefix. ~25 LOC override. Everything else (commit, branch,
#    history, indexes) is inherited from the base Lens unchanged.
# ===========================================================================

class ContextLens(Lens):
    """A Lens whose codec is chosen by key prefix via a ContextResolver.

    Inheriting from `Lens` gives us, for free, everything the contract
    says a Lens must support: put/get/get_raw/keys/count/commit, and
    branch/checkout/merge/history (the shared commit DAG). We only
    override put / get / get_all so the codec is selected by key prefix
    instead of by class.
    """

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: ContextResolver, prefix: str) -> None:
        super().__init__(kernel, name)
        self.resolver = resolver
        self.prefix = prefix

    # --- Write path: encode by key prefix ---------------------------------
    def put(self, key: str, data: Any) -> str:
        raw = self.resolver.encode_for_key(key, data)
        blob_hash = self.kernel.write(raw)          # pure payload, no envelope
        self.base.stage(key, blob_hash)
        return blob_hash

    # --- Read path: decode by key prefix, with raw-byte fallback ----------
    def get(self, key: str) -> Optional[Any]:
        h = self.base.lookup(key)
        if h is None:
            return None
        raw = self.kernel.read_blob(h)
        return self.resolver.decode_for_key(key, raw)

    def get_all(self) -> dict[str, Any]:
        state = self.base.read_all()
        out: dict[str, Any] = {}
        for k, h in state.items():
            if k.startswith("_"):
                continue
            out[k] = self.resolver.decode_for_key(k, self.kernel.read_blob(h))
        return out


# ===========================================================================
# 3. ConfigLens  — the actual configuration-management Lens.
#
#    Each config entry is a JSON object with:
#       key            : the bare config name (e.g. "db_host")
#       value          : the config value (any JSON-serialisable value)
#       environment    : "dev" | "staging" | "prod"
#       service        : the owning service name
#       last_updated   : ISO-8601 UTC timestamp
#
#    The kernel key is `config/<key>`. The blob is PURE JSON — no
#    envelope, no header, no codec_id (per contract §4.1, §4.3).
# ===========================================================================

class ConfigLens(ContextLens):
    PREFIX = "config/"

    def __init__(self, kernel: PondMinimal, name: str,
                 resolver: ContextResolver) -> None:
        # Register the config/ JSON codec with the resolver BEFORE chaining
        # up, so the lens is immediately usable. Per contract §3.1, the
        # prefix is the Lens author's convention; the Resolver uses it to
        # pick the codec.
        resolver.register(
            self.PREFIX,
            encode=lambda d: json.dumps(d, sort_keys=True).encode("utf-8"),
            decode=lambda b: json.loads(b.decode("utf-8")),
        )
        super().__init__(kernel, name, resolver, self.PREFIX)

    # --- Domain API --------------------------------------------------------
    def _full(self, bare_key: str) -> str:
        return bare_key if bare_key.startswith(self.PREFIX) else self.PREFIX + bare_key

    def put_config(self, bare_key: str, value: Any,
                   environment: str, service: str) -> str:
        """Store (or overwrite) a config entry. Returns the full kernel key."""
        entry = {
            "key": bare_key,
            "value": value,
            "environment": environment,
            "service": service,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        full_key = self._full(bare_key)
        self.put(full_key, entry)
        return full_key

    def get_config(self, bare_key: str) -> Optional[dict]:
        return self.get(self._full(bare_key))

    def get_raw(self, bare_key: str) -> Optional[bytes]:
        """Raw-byte fallback (contract §3.3). Bypasses the resolver."""
        return super().get_raw(self._full(bare_key))

    def list_configs(self) -> list[str]:
        return [k for k in self.keys() if k.startswith(self.PREFIX)]

    # --- Filtering (task requirements 7 & 8) ------------------------------
    def get_by_environment(self, environment: str) -> list[dict]:
        return [e for e in self.get_all().values()
                if isinstance(e, dict) and e.get("environment") == environment]

    def get_by_service(self, service: str) -> list[dict]:
        return [e for e in self.get_all().values()
                if isinstance(e, dict) and e.get("service") == service]


# ===========================================================================
# 4. Verification / demo  — exercises every requirement of the task.
#    Run:  python config_lens_external.py
# ===========================================================================

def _section(title: str) -> None:
    print(f"\n--- {title} ---")


def main() -> int:
    import tempfile, shutil
    tmp = tempfile.mkdtemp(prefix="configlens_")
    try:
        kernel = PondMinimal(tmp)
        resolver = ContextResolver()
        # One shared name => one shared byte graph / commit DAG.
        # Multiple Lenses with the same name share history (contract §3.4).
        WORKSPACE = "app_config"
        config = ConfigLens(kernel, WORKSPACE, resolver)

        failures: list[str] = []

        # ------------------------------------------------------------------
        # Req 1 & 2 & 3: store JSON entries under config/ prefix with the
        # codec registered in the resolver.
        # ------------------------------------------------------------------
        _section("Req 1-3: store JSON config entries under config/ prefix")
        config.put_config("db_host", "db.prod.example.com", "prod", "payments")
        config.put_config("db_host", "db.dev.example.com",  "dev",  "payments")
        config.put_config("feature_flag_x", True,  "staging", "checkout")
        config.put_config("max_connections", 100,   "prod",    "payments")
        config.put_config("log_level", "debug",     "dev",     "search")
        config.put_config("log_level", "info",      "prod",    "search")
        config.commit("initial config seed")

        print(f"keys        : {sorted(config.list_configs())}")
        print(f"count       : {config.count()}")
        print(f"db_host(dev): {config.get_config('db_host') if config.get_by_environment('dev') else None}")

        # Verify the blob is PURE JSON (no envelope / no header).
        raw_db_host = config.get_raw("db_host")
        assert raw_db_host is not None
        decoded_direct = json.loads(raw_db_host.decode("utf-8"))
        assert set(decoded_direct.keys()) == {"key", "value", "environment",
                                              "service", "last_updated"}, \
               "config entry must have exactly the 5 required fields"
        assert raw_db_host.lstrip().startswith(b"{"), \
               "blob must be pure JSON (no envelope/header) — contract §4.1"
        print(f"raw blob    : {raw_db_host!r}")
        print("PASS: entries are JSON with the 5 required fields, no envelope")

        # ------------------------------------------------------------------
        # Req 4: cross-Lens reading. A *different* Lens (different prefix)
        # using the SAME resolver + SAME name must read config/ entries.
        # ------------------------------------------------------------------
        _section("Req 4: cross-Lens reading")
        # A "deploy" lens — different prefix, same workspace, same resolver.
        deploy_lens = ContextLens(kernel, WORKSPACE, resolver, "deploy/")
        cross_read = deploy_lens.get("config/db_host")  # bare cross-prefix read
        # There are two db_host entries? No — keys are unique; the last
        # put_config("db_host", "dev") was staged but only one commit
        # happened. Actually both db_host puts used the SAME key
        # "config/db_host", so the second overwrote the first in the
        # working set before commit. So get returns the dev one.
        print(f"deploy_lens.get('config/db_host') = {cross_read}")
        assert cross_read is not None, "cross-lens read must find the blob"
        assert cross_read["key"] == "db_host"
        assert cross_read["environment"] == "dev"
        print("PASS: another Lens read a config/ blob via the shared resolver")

        # ------------------------------------------------------------------
        # Req 5: branching. Create a branch, mutate on it, verify isolation.
        # ------------------------------------------------------------------
        _section("Req 5: branching")
        # The contract says branching is inherited from the shared commit
        # DAG, but does not specify the default branch name. We therefore
        # establish an explicit named branch ("main") right after the
        # initial commit, so we have a stable ref to return to.
        config.branch("main")
        branches_before = set(config.list_branches())
        print(f"branches after creating 'main': {sorted(branches_before)}")

        main_count_before = config.count()
        config.branch("config-experiment")
        config.checkout("config-experiment")
        config.put_config("experimental_flag", "on", "staging", "checkout")
        config.commit("add experimental flag on branch")
        branch_count = config.count()
        print(f"branch 'config-experiment' count = {branch_count}")
        assert branch_count == main_count_before + 1, \
               "branch should have one more entry than main"

        config.checkout("main")
        main_count_after = config.count()
        print(f"main count (after branch work) = {main_count_after}")
        assert main_count_after == main_count_before, \
               "main must be unaffected by branch commits (contract §3.4, Law 6)"
        assert config.get_config("experimental_flag") is None, \
               "experimental_flag must NOT be visible on main"
        print("PASS: branch isolated from main; shared DAG works")

        # Branch is visible to the OTHER lens too (shared history).
        branches_seen_by_deploy = deploy_lens.list_branches()
        print(f"branches visible to deploy_lens: {sorted(branches_seen_by_deploy)}")
        assert "config-experiment" in branches_seen_by_deploy, \
               "branch must be visible to all Lenses sharing the name"
        print("PASS: branch visible to the other Lens (shared commit DAG)")

        # ------------------------------------------------------------------
        # Req 6: get_raw — transform-later capability.
        # ------------------------------------------------------------------
        _section("Req 6: get_raw (transform-later)")
        raw = config.get_raw("log_level")
        assert raw is not None
        # The caller can parse, transform, re-encode however it likes.
        as_text = raw.decode("utf-8")
        re_parsed = json.loads(as_text)
        print(f"raw bytes   : {raw!r}")
        print(f"re-parsed   : {re_parsed}")
        assert re_parsed["service"] == "search"
        print("PASS: get_raw returns pure payload bytes for transform-later")

        # ------------------------------------------------------------------
        # Req 7: environment filtering.
        # ------------------------------------------------------------------
        _section("Req 7: environment filtering")
        prod_configs = config.get_by_environment("prod")
        dev_configs = config.get_by_environment("dev")
        staging_configs = config.get_by_environment("staging")
        print(f"prod    : {len(prod_configs)} entries")
        print(f"dev     : {len(dev_configs)} entries")
        print(f"staging : {len(staging_configs)} entries")
        assert all(e["environment"] == "prod" for e in prod_configs)
        assert all(e["environment"] == "dev" for e in dev_configs)
        assert all(e["environment"] == "staging" for e in staging_configs)
        # On the original branch the 4 committed entries are:
        #   db_host(dev/payments), feature_flag_x(staging/checkout),
        #   max_connections(prod/payments), log_level(prod/search)
        # (the second put to a duplicate key wins in the staging buffer.)
        assert len(prod_configs) == 2, f"expected 2 prod configs, got {len(prod_configs)}"
        assert len(dev_configs) == 1, f"expected 1 dev config, got {len(dev_configs)}"
        assert len(staging_configs) == 1, f"expected 1 staging config, got {len(staging_configs)}"
        print("PASS: environment filter returns exactly the matching entries")

        # ------------------------------------------------------------------
        # Req 8: service filtering.
        # ------------------------------------------------------------------
        _section("Req 8: service filtering")
        payments_configs = config.get_by_service("payments")
        search_configs = config.get_by_service("search")
        checkout_configs = config.get_by_service("checkout")
        print(f"payments : {len(payments_configs)} entries")
        print(f"search   : {len(search_configs)} entries")
        print(f"checkout : {len(checkout_configs)} entries")
        assert all(e["service"] == "payments" for e in payments_configs)
        assert all(e["service"] == "search" for e in search_configs)
        assert all(e["service"] == "checkout" for e in checkout_configs)
        # payments: db_host(dev), max_connections(prod)  => 2
        # search:   log_level(prod)                       => 1
        # checkout: feature_flag_x(staging)               => 1
        assert len(payments_configs) == 2, f"expected 2 payments configs, got {len(payments_configs)}"
        assert len(search_configs) == 1, f"expected 1 search config, got {len(search_configs)}"
        assert len(checkout_configs) == 1, f"expected 1 checkout config, got {len(checkout_configs)}"
        print("PASS: service filter returns exactly the matching entries")

        # ------------------------------------------------------------------
        # Contract §5 fallback: unknown prefix => raw bytes.
        # ------------------------------------------------------------------
        _section("Contract §5: fallback decoding for unknown prefix")
        # Write a blob under an unregistered prefix using the kernel directly,
        # then read it through a lens. The resolver must return raw bytes.
        unknown_payload = b"binary-opaque-data"
        h = kernel.write(unknown_payload)
        # Manually stage it under a prefix no codec covers.
        config.base.stage("weird/blob", h)
        config.commit("add an unknown-prefix blob")
        fallback = config.get("weird/blob")
        print(f"fallback read of 'weird/blob' = {fallback!r}")
        assert fallback == unknown_payload, \
               "unknown-prefix read must fall back to raw bytes (contract §5)"
        print("PASS: unknown prefix falls back to raw bytes")

        # ------------------------------------------------------------------
        # Kernel purity check: the kernel never saw a codec_id, envelope,
        # or manifest. The only thing it stored was pure JSON bytes.
        # ------------------------------------------------------------------
        _section("Kernel purity (contract §4, §9)")
        for k in config.list_configs():
            raw = config.get_raw(k)
            assert raw is not None
            assert raw.lstrip().startswith(b"{"), \
                   f"blob for {k} is not pure JSON: {raw[:20]!r}"
        stats = kernel.storage_stats()
        print(f"kernel stats: writes={stats['writes']}, "
              f"reads={stats['reads']}, blobs={stats['blob_count']}")
        print("PASS: every config blob is pure JSON; kernel never saw metadata")

        # ------------------------------------------------------------------
        _section("ALL CHECKS PASSED")
        print(f"ConfigLens implementation is contract-compliant.")
        return 0

    except AssertionError as e:
        print(f"\nFAIL: {e}")
        return 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
