#!/usr/bin/env python3
"""
Pond Feature Store — CLI application.

A production-quality Feature Store serving online and offline features
from the same immutable substrate. Built on the full Pond SDK.

Usage:
  python3 cli.py init                              # initialize feature store
  python3 cli.py define <name> <type> <source>     # define a feature
  python3 cli.py ingest <source_view> <feature> <entity_field> <value_field>  # ingest from source
  python3 cli.py online <feature> <entity_id>      # online serving (point lookup)
  python3 cli.py vector <entity_id> <features...>  # feature vector
  python3 cli.py offline <feature>                # offline serving (batch scan)
  python3 cli.py point-in-time <feature> <ts>     # point-in-time lookup
  python3 cli.py lineage                          # show feature lineage
  python3 cli.py freshness                        # show feature freshness
  python3 cli.py list                             # list all features
  python3 cli.py history                          # show commit history
  python3 cli.py branch <name>                    # create branch
  python3 cli.py checkout <name>                  # switch to branch
  python3 cli.py semantic <semantic_view>         # register with semantic model
"""

import sys
import os
import json
import time

# Add all package paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
for pkg in ["pond-core", "pond-sdk", "pond-feature-store", "pond-semantic"]:
    sys.path.insert(0, os.path.join(REPO_ROOT, pkg))
sys.path.insert(0, SCRIPT_DIR)  # for feature_store.py

from kernel import PondMinimal
from feature_store import FeatureStore
from keyvalue_lens import View, SemanticLens


def get_store():
    store_dir = os.environ.get("POND_FS_DIR", ".pond_feature_store")
    kernel = PondMinimal(store_dir)
    return kernel, FeatureStore(kernel, "feature_store")


def cmd_init(args):
    kernel, fs = get_store()
    # Initialize with empty commit
    fs.put("_init", {"created_at": time.time()})
    fs.commit("initialize feature store")
    print(f"Feature store initialized at {os.path.abspath(os.environ.get('POND_FS_DIR', '.pond_feature_store'))}")
    kernel.close()


def cmd_define(args):
    if len(args) < 3:
        print("Usage: define <name> <type> <source> [description]")
        sys.exit(1)
    name, ftype, source = args[0], args[1], args[2]
    desc = args[3] if len(args) > 3 else ""
    kernel, fs = get_store()
    fs.define_feature(name, ftype, source, description=desc)
    fs.commit(f"define feature '{name}'")
    print(f"Defined feature: {name} (type={ftype}, source={source})")
    kernel.close()


def cmd_ingest(args):
    if len(args) < 4:
        print("Usage: ingest <source_view_name> <feature> <entity_field> <value_field>")
        sys.exit(1)
    source_name, feature, entity_field, value_field = args[0], args[1], args[2], args[3]
    kernel, fs = get_store()
    # Create a source Lens and read from it
    source = View(kernel, source_name)
    count = fs.ingest_from_view(source, feature, entity_field, value_field)
    fs.commit(f"ingest {count} values for '{feature}' from '{source_name}'")
    print(f"Ingested {count} values for feature '{feature}' from '{source_name}'")
    kernel.close()


def cmd_ingest_csv(args):
    """Ingest from CSV: feature,entity_id,value,timestamp"""
    if len(args) < 1:
        print("Usage: ingest-csv <feature> < csv_file")
        sys.exit(1)
    feature = args[0]
    kernel, fs = get_store()
    count = 0
    for line in sys.stdin:
        parts = line.strip().split(",")
        if len(parts) < 3:
            continue
        entity_id, value = parts[0], parts[1]
        ts = float(parts[2]) if len(parts) > 2 else time.time()
        try:
            value = float(value) if "." in value else int(value)
        except ValueError:
            pass
        fs.write_feature_value(feature, entity_id, value, ts)
        count += 1
    fs.commit(f"ingest {count} CSV values for '{feature}'")
    print(f"Ingested {count} values for '{feature}'")
    kernel.close()


def cmd_online(args):
    if len(args) < 2:
        print("Usage: online <feature> <entity_id>")
        sys.exit(1)
    feature, entity_id = args[0], args[1]
    kernel, fs = get_store()
    value = fs.get_feature_value(feature, entity_id)
    if value is None:
        print(f"  No value for feature '{feature}', entity '{entity_id}'")
    else:
        print(f"  {feature}[{entity_id}] = {value}")
    kernel.close()


def cmd_vector(args):
    if len(args) < 2:
        print("Usage: vector <entity_id> <feature1> [feature2] ...")
        sys.exit(1)
    entity_id = args[0]
    features = args[1:]
    kernel, fs = get_store()
    vec = fs.get_feature_vector(entity_id, features)
    print(f"  Feature vector for {entity_id}:")
    for name, val in vec.items():
        print(f"    {name}: {val}")
    kernel.close()


def cmd_offline(args):
    if len(args) < 1:
        print("Usage: offline <feature>")
        sys.exit(1)
    feature = args[0]
    kernel, fs = get_store()
    values = fs.get_all_values(feature)
    print(f"  Feature '{feature}': {len(values)} values")
    for v in values:
        print(f"    {v['entity_id']}: {v['value']} (ts={v['timestamp']})")
    kernel.close()


def cmd_point_in_time(args):
    if len(args) < 2:
        print("Usage: point-in-time <feature> <timestamp>")
        sys.exit(1)
    feature, ts_str = args[0], args[1]
    try:
        ts = float(ts_str)
    except ValueError:
        print(f"Invalid timestamp: {ts_str}")
        sys.exit(1)
    kernel, fs = get_store()
    values = fs.get_feature_values_at_time(feature, ts)
    print(f"  Feature '{feature}' at ts={ts}: {len(values)} values")
    for v in values:
        print(f"    {v['entity_id']}: {v['value']} (ts={v['timestamp']})")
    kernel.close()


def cmd_lineage(args):
    kernel, fs = get_store()
    for feat in fs.list_features():
        lineage = fs.get_lineage(feat)
        if lineage:
            print(f"  {lineage['feature']} ← {lineage['source']} ({lineage['values_count']} values)")
    kernel.close()


def cmd_freshness(args):
    kernel, fs = get_store()
    for feat in fs.list_features():
        freshness = fs.get_freshness(feat)
        if freshness is not None:
            print(f"  {feat}: {freshness:.0f}s ago")
        else:
            print(f"  {feat}: no data")
    kernel.close()


def cmd_list(args):
    kernel, fs = get_store()
    features = fs.list_features()
    if not features:
        print("  (no features defined)")
    for name in features:
        feat = fs.get_feature_definition(name)
        print(f"  {name:<20} type={feat['type']:<8} source={feat['source']}")
    kernel.close()


def cmd_history(args):
    kernel, fs = get_store()
    for h in fs.history():
        print(f"  {h['commit']}  {h['type']}  {h['message']}")
    kernel.close()


def cmd_branch(args):
    if not args:
        print("Usage: branch <name>")
        sys.exit(1)
    kernel, fs = get_store()
    fs.branch(args[0])
    print(f"Created branch '{args[0]}'")
    kernel.close()


def cmd_checkout(args):
    if not args:
        print("Usage: checkout <name>")
        sys.exit(1)
    kernel, fs = get_store()
    fs.checkout(args[0])
    print(f"Switched to branch '{args[0]}'")
    kernel.close()


def cmd_semantic(args):
    if not args:
        print("Usage: semantic <semantic_view_name>")
        sys.exit(1)
    kernel, fs = get_store()
    semantic = SemanticLens(kernel, args[0])
    fs.register_with_semantic_view(semantic)
    semantic.commit("register features")
    print(f"Registered {len(fs.list_features())} features with semantic view '{args[0]}'")
    kernel.close()


def cmd_put(args):
    """Manually write a feature value: put <feature> <entity_id> <value> [timestamp]"""
    if len(args) < 3:
        print("Usage: put <feature> <entity_id> <value> [timestamp]")
        sys.exit(1)
    feature, entity_id, value = args[0], args[1], args[2]
    ts = float(args[3]) if len(args) > 3 else time.time()
    try:
        value = float(value) if "." in value else int(value)
    except ValueError:
        pass
    kernel, fs = get_store()
    fs.write_feature_value(feature, entity_id, value, ts)
    fs.commit(f"write {feature}[{entity_id}]={value}")
    print(f"Wrote {feature}[{entity_id}] = {value} (ts={ts})")
    kernel.close()


COMMANDS = {
    "init": cmd_init,
    "define": cmd_define,
    "ingest": cmd_ingest,
    "ingest-csv": cmd_ingest_csv,
    "put": cmd_put,
    "online": cmd_online,
    "vector": cmd_vector,
    "offline": cmd_offline,
    "point-in-time": cmd_point_in_time,
    "lineage": cmd_lineage,
    "freshness": cmd_freshness,
    "list": cmd_list,
    "history": cmd_history,
    "branch": cmd_branch,
    "checkout": cmd_checkout,
    "semantic": cmd_semantic,
}


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in COMMANDS:
        print(__doc__)
        sys.exit(1)
    COMMANDS[sys.argv[1]](sys.argv[2:])


if __name__ == "__main__":
    main()
