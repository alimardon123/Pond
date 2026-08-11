"""
Semantic extensions — legacy adapter interface.

DEPRECATED: This module contains the legacy SemanticLens / SemanticModelAdapter
API. New code should use the SemanticLayer API exposed via the PyO3 `pond`
module:

    import pond
    s = pond.Storage(...)
    m = s.layer('sales', adapters=['ossie'])  # SemanticLayer handle
    m.add_datasets(['orders', 'users'])
    m.add_metrics({'revenue': 'SUM(orders.amount)'})
    m.add_adapter('cube')         # multi-adapter support
    m.remove_adapter('cube')
    m.info()
    m.export('ossie')

Why "layer" (not "model"): the word "model" collides with ML models, which
Pond may host in the future. "Semantic Layer" is the industry-standard term
(dbt Semantic Layer, Cube Semantic Layer, Looker LookML).

This legacy module is kept for backward compat. It implements an older
single-adapter API where SemanticLens wraps a KeyValueLens and uses the
SemanticModelAdapter trait. The new SemanticLayer (in pond-rust) supports
multiple adapters per layer, independent add/remove of adapters, batch
operations, auto-exposure, and reflection config.

Available adapters (legacy):
  - ossie: Apache Ossie open semantic interchange spec

Future adapters:
  - cube: Cube.js semantic model
  - dbt: dbt metrics
  - custom: implement SemanticModelAdapter
"""

from extensions.semantic.base import SemanticModelAdapter
from extensions.semantic.ossie import SemanticLens, OssieAdapter

__all__ = ["SemanticModelAdapter", "SemanticLens", "OssieAdapter"]
