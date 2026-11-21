# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/extension_index.py
"""Index nodes whose access methods come from extensions.

They carry raw arguments only: legal access methods and operator classes are
the formatter's decision, not the factory's.
"""

from typing import Any, List, Optional

from rhosocial.activerecord.backend.expression.bases import BaseExpression


class PostgresLtreeIndexExpression(BaseExpression):
    def __init__(self, dialect, name, table, columns, index_type=None, schema=None):
        super().__init__(dialect)
        self.name = name
        self.table = table
        self.columns = list(columns)
        self.index_type = index_type
        self.schema = schema

    @property
    def format_method(self) -> str:
        return "format_postgres_ltree_index_expression"


class PostgresTrgmIndexExpression(BaseExpression):
    def __init__(self, dialect, name, table, columns, index_type=None, schema=None):
        super().__init__(dialect)
        self.name = name
        self.table = table
        self.columns = list(columns)
        self.index_type = index_type
        self.schema = schema

    @property
    def format_method(self) -> str:
        return "format_postgres_trgm_index_expression"


class PostgresSpatialIndexExpression(BaseExpression):
    def __init__(self, dialect, name, table, columns, index_type=None, schema=None):
        super().__init__(dialect)
        self.name = name
        self.table = table
        self.columns = list(columns)
        self.index_type = index_type
        self.schema = schema

    @property
    def format_method(self) -> str:
        return "format_postgres_spatial_index_expression"


class PostgresCitextColumnExpression(BaseExpression):
    def __init__(self, dialect, name, length=None):
        super().__init__(dialect)
        self.name = name
        self.length = length

    @property
    def format_method(self) -> str:
        return "format_postgres_citext_column_expression"


class PostgresDropTypeNameExpression(BaseExpression):
    """DROP TYPE addressed by name; both schema spellings stay raw until rendering."""

    def __init__(
        self, dialect, name, schema=None, schema_name=None,
        if_exists=False, cascade=False, restrict=False,
    ):
        super().__init__(dialect)
        self.name = name
        self.schema = schema
        self.schema_name = schema_name
        self.if_exists = if_exists
        self.cascade = cascade
        self.restrict = restrict

    @property
    def format_method(self) -> str:
        return "format_postgres_drop_type_name_expression"


__all__ = [
    "PostgresDropTypeNameExpression",
    "PostgresLtreeIndexExpression",
    "PostgresTrgmIndexExpression",
    "PostgresSpatialIndexExpression",
    "PostgresCitextColumnExpression",
]
