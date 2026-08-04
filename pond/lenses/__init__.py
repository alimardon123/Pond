"""Pond Lenses — workload-specific APIs (lakehouse, keyvalue, vector, streaming, oltp)."""

import os, sys
_repo = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for _lens in ["keyvalue", "lakehouse", "vector", "streaming", "oltp"]:
    _path = os.path.join(_repo, "lenses", _lens)
    if _path not in sys.path:
        sys.path.insert(0, _path)
