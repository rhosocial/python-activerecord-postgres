# src/rhosocial/activerecord/backend/impl/postgres/expression/statements/status.py
"""PostgreSQL SHOW statement.

The catalog-introspection query nodes now live in ``..introspection.status``.

The status introspector used to hold the catalog SQL itself -- f-strings
assembled at the call site, or plain string literals inline in the method. A
SQL string is a formatting artefact, so it belongs to the dialect formatter,
not to the layer that executes the query (expression-dialect §16.2 of the
org's conformance guide): the node collects the parameters, the formatter
writes the text, and the introspector only constructs and executes.

Each node therefore carries only the parameters its query needs. Where the old
text bound nothing, the node binds nothing; where it carried literal
comparison values inline, the node takes them as bound values instead -- a
`WHERE name IN (...)` list is data, and a data position is a bind position.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases.bases import BaseExpression

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


__all__ = ["PostgresShowStatement"]


class PostgresShowStatement(BaseExpression):
    """``SHOW`` a PostgreSQL run-time parameter.

    The parameter is a configuration name, not free text: an identifier-shaped
    string, validated at construction so a name carrying quotes or spaces fails
    at the caller instead of reaching the server as part of a statement.
    """

    def __init__(self, dialect: "SQLDialectBase", parameter: str):
        super().__init__(dialect)
        if not parameter or not parameter.replace("_", "").isalnum():
            raise ValueError(
                f"PostgresShowStatement takes a configuration name, got {parameter!r}"
            )
        self.parameter = parameter

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this expression."""
        return "format_postgres_show_statement"
