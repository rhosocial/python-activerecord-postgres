# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/citext.py
"""citext capability probe and column renderer."""

from typing import Tuple

from rhosocial.activerecord.backend.expression import ColumnDefinition

from ...expression.ddl import PostgresCitextColumnExpression


class PostgresCitextMixin:
    """citext case-insensitive text functionality implementation."""

    def supports_citext_type(self) -> bool:
        return self.check_extension_feature("citext", "type")

    def format_postgres_citext_column_expression(
        self, expr: PostgresCitextColumnExpression
    ) -> Tuple[str, tuple]:
        if expr.length is not None:
            raise ValueError("CITEXT has no length modifier in PostgreSQL")
        return ColumnDefinition(
            self, expr.name, self.parse_type("CITEXT")
        ).to_sql()
