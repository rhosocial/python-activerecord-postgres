# src/rhosocial/activerecord/backend/impl/postgres/expression_registry.py
"""PostgreSQL's expression table: core's kinds merged with PostgreSQL's own.

``POSTGRES_EXPRESSIONS`` is the mapping this backend's expression package
declares. The published table is the merge of core's mapping and that one --
never a sibling backend's, and no dialect takes part.

Both mappings are data (``kind -> fully qualified class name``); resolution
imports a class only when an artifact names its kind.
"""

from rhosocial.activerecord.backend.expression_registry import (
    CORE_EXPRESSIONS,
    ExpressionRegistry,
    collect_expressions,
    merge_expressions,
)

#: The kinds PostgreSQL's own expression package declares.
POSTGRES_EXPRESSIONS = collect_expressions(
    "rhosocial.activerecord.backend.impl.postgres.expression"
)

#: PostgreSQL's table: core's kinds plus PostgreSQL's own, frozen.
POSTGRES_EXPRESSION_REGISTRY = ExpressionRegistry(
    merge_expressions(CORE_EXPRESSIONS, POSTGRES_EXPRESSIONS)
).freeze()

__all__ = ["POSTGRES_EXPRESSIONS", "POSTGRES_EXPRESSION_REGISTRY"]
