# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pg_trgm.py
"""pg_trgm capability probes and index renderer.

The node carries raw arguments; the formatter decides which access methods and
operator classes are legal.
"""

from typing import Tuple

from rhosocial.activerecord.backend.expression.objects import Index, Table

from ...expression.ddl import (
    PostgresCreateIndexExpression,
    PostgresIndexType,
    PostgresTrgmIndexExpression,
)


class PostgresPgTrgmMixin:
    """pg_trgm capability implementation."""

    def supports_pg_trgm_similarity(self) -> bool:
        return self.check_extension_feature("pg_trgm", "similarity")

    def supports_pg_trgm_index(self) -> bool:
        return self.check_extension_feature("pg_trgm", "index")

    def format_postgres_trgm_index_expression(
        self, expr: PostgresTrgmIndexExpression
    ) -> Tuple[str, tuple]:
        method = PostgresIndexType.normalize(expr.index_type or "gin").value
        if method not in ("gin", "gist"):
            raise ValueError(
                f"pg_trgm index_type must be 'gin' or 'gist', got {expr.index_type!r}"
            )
        opclass = "gin_trgm_ops" if method == "gin" else "gist_trgm_ops"
        return PostgresCreateIndexExpression(
            self,
            index=Index(self, expr.name, schema_name=expr.schema),
            table=Table(self, expr.table, schema_name=expr.schema),
            columns=expr.columns,
            index_type=method,
            opclasses={column: opclass for column in expr.columns},
        ).to_sql()
