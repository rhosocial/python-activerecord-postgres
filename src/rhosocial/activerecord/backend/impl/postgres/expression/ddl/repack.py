# src/rhosocial/activerecord/backend/impl/postgres/expression/ddl/repack.py
"""Typed PostgreSQL 19 REPACK expression."""

from typing import List, Optional, Sequence, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresRepackExpression"]


class PostgresRepackExpression(BaseExpression):
    """A native PostgreSQL 19 ``REPACK`` statement expression."""

    def __init__(
        self,
        dialect: "SQLDialectBase",
        table_name: Optional[str] = None,
        *,
        schema: Optional[str] = None,
        columns: Optional[Sequence[str]] = None,
        using_index: Optional[Union[bool, str]] = None,
        all_using_index: bool = False,
        verbose: bool = False,
        analyze: bool = False,
        concurrently: bool = False,
    ):
        super().__init__(dialect)
        if isinstance(columns, (str, bytes)) or (columns is not None and not isinstance(columns, Sequence)):
            raise TypeError("columns must be a sequence of identifiers")
        normalized_columns = None if columns is None else list(columns)
        if normalized_columns is not None and any(not isinstance(column, str) for column in normalized_columns):
            raise TypeError("columns must contain only string identifiers")
        if not isinstance(all_using_index, bool):
            raise TypeError("all_using_index must be a bool")
        self.table_name = table_name
        self.schema = schema
        self.columns: Optional[List[str]] = normalized_columns
        self.using_index = using_index
        self.all_using_index = all_using_index
        self.verbose = verbose
        self.analyze = analyze
        self.concurrently = concurrently

    @property
    def format_method(self) -> str:
        return "format_repack_statement"
