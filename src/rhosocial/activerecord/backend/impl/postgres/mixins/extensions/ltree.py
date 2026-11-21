# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/ltree.py
"""ltree capability probes and index renderer.

Validation lives here, not in the factory: the node only collects arguments.
"""

from typing import Tuple

from rhosocial.activerecord.backend.expression.objects import Index, Table

from ...expression.ddl import (
    PostgresCreateIndexExpression,
    PostgresIndexType,
    PostgresLtreeIndexExpression,
)


class PostgresLtreeMixin:
    """ltree label tree implementation."""

    def supports_ltree_type(self) -> bool:
        return self.check_extension_feature("ltree", "type")

    def supports_ltree_operators(self) -> bool:
        return self.check_extension_feature("ltree", "operators")

    def supports_ltree_index(self) -> bool:
        return self.check_extension_feature("ltree", "index")

    def format_postgres_ltree_index_expression(
        self, expr: PostgresLtreeIndexExpression
    ) -> Tuple[str, tuple]:
        method = PostgresIndexType.normalize(expr.index_type or "gist").value
        if method not in ("gist", "btree"):
            raise ValueError(
                f"ltree index_type must be 'gist' or 'btree', got {expr.index_type!r}"
            )
        return PostgresCreateIndexExpression(
            self,
            index=Index(self, expr.name, schema_name=expr.schema),
            table=Table(self, expr.table, schema_name=expr.schema),
            columns=expr.columns,
            index_type=method,
        ).to_sql()
