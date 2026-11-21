# src/rhosocial/activerecord/backend/impl/postgres/expression/tablefunc.py
"""tablefunc query nodes with typed output definitions."""

from typing import Optional

from rhosocial.activerecord.backend.expression.bases import BaseExpression


class PostgresCrosstabWithDefinitionStatement(BaseExpression):
    def __init__(
        self,
        dialect,
        source_sql: str,
        output_columns: str,
        categories_sql: Optional[str] = None,
    ):
        super().__init__(dialect)
        self.source_sql = source_sql
        self.output_columns = output_columns
        self.categories_sql = categories_sql

    @property
    def format_method(self) -> str:
        return "format_postgres_crosstab_with_definition_statement"


class PostgresConnectbyFullStatement(BaseExpression):
    def __init__(
        self,
        dialect,
        table_name: str,
        key_column: str,
        parent_column: str,
        start_value: str,
        max_depth: int,
        branch_delim: str = "~",
    ):
        super().__init__(dialect)
        self.table_name = table_name
        self.key_column = key_column
        self.parent_column = parent_column
        self.start_value = start_value
        self.max_depth = max_depth
        self.branch_delim = branch_delim

    @property
    def format_method(self) -> str:
        return "format_postgres_connectby_full_statement"


__all__ = [
    "PostgresCrosstabWithDefinitionStatement",
    "PostgresConnectbyFullStatement",
]
