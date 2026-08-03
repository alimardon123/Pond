"""Pond keyvalue lens — re-exports from lenses/keyvalue/."""
import os, sys
_repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
_path = os.path.join(_repo, "lenses", "keyvalue")
if _path not in sys.path:
    sys.path.insert(0, _path)
# Also need pond-core and pond-sdk on path
for _p in ["pond-core", "pond-sdk", "pond-sdk/extensions/physical_structures"]:
    _full = os.path.join(_repo, _p)
    if _full not in sys.path:
        sys.path.insert(0, _full)
