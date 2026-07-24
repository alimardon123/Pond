"""
Semantic model extensions — pluggable adapters for different semantic standards.

Each adapter implements SemanticModelAdapter (from base.py) and translates
between Pond's internal metric/dimension/relationship storage and an
external semantic model standard.

Available adapters:
  - ossie: Apache Ossie open semantic interchange spec

Future adapters:
  - cube: Cube.js semantic model
  - dbt: dbt metrics
  - custom: implement SemanticModelAdapter

Usage:
    from extensions.semantic.ossie import SemanticLens, OssieAdapter

    # Default (Ossie)
    semantic = SemanticLens(kernel, "semantic")

    # Custom adapter
    from extensions.semantic.base import SemanticModelAdapter
    class MyAdapter(SemanticModelAdapter): ...
    semantic = SemanticLens(kernel, "semantic", adapter=MyAdapter())
"""

from extensions.semantic.base import SemanticModelAdapter
from extensions.semantic.ossie import SemanticLens, OssieAdapter

__all__ = ["SemanticModelAdapter", "SemanticLens", "OssieAdapter"]
