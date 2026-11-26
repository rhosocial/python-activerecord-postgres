# src/rhosocial/activerecord/backend/impl/postgres/expression/load_library.py
"""Typed ``LOAD '<library>'`` statement expression.

A library name is validated as an identifier at construction.  Rendering
happens only in the dialect formatter.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import BaseExpression

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


class PostgresLoadLibraryExpression(BaseExpression):
    """Load a PostgreSQL extension library into the current session."""

    def __init__(self, dialect: "SQLDialectBase", library: str):
        super().__init__(dialect)
        if not isinstance(library, str) or not library.isidentifier():
            raise ValueError(f"Invalid library name: {library!r}")
        self.library = library

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_pg_load_library"


__all__ = ["PostgresLoadLibraryExpression"]
