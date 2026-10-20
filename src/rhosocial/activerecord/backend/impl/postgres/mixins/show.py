# src/rhosocial/activerecord/backend/impl/postgres/mixins/show.py
"""PostgreSQL's ``SHOW`` statement.

``SHOW`` is not a query: it names one run-time parameter and answers with its
current value. That is why it lives apart from the catalog introspection
formatters, which all build a SELECT over ``pg_catalog`` -- the same split the
MySQL and MariaDB backends make between ``show/dialect.py`` and their
introspection mixins.

The parameter *name* is not data: PostgreSQL parses it as a configuration name,
and ``SHOW`` accepts no bind parameter at all. :class:`PostgresShowStatement`
therefore validates the name at construction (an identifier-shaped string), and
this formatter is the only place in the backend that writes the keyword.
"""

from typing import Tuple, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
    from ..expression.status import PostgresShowStatement


class PostgresShowMixin:
    """Renders ``SHOW <parameter>``."""

    def format_postgres_show_statement(
        self, expr: "PostgresShowStatement"
    ) -> Tuple[str, tuple]:
        """Render ``SHOW`` for a run-time parameter.

        The node validated the name, so what reaches the statement is a
        configuration identifier and nothing else.

        Args:
            expr: The SHOW node, carrying the parameter name.

        Returns:
            Tuple of (SQL string, empty parameters tuple). ``SHOW`` takes no
            bind parameter, so the tuple is always empty.
        """
        return f"SHOW {expr.parameter}", ()


__all__ = ["PostgresShowMixin"]
