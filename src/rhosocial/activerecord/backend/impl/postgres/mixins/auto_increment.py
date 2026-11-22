# src/rhosocial/activerecord/backend/impl/postgres/mixins/auto_increment.py
"""PostgreSQL auto-increment marker support implementation.

``AUTO_INCREMENT`` is MySQL / MariaDB spelling. PostgreSQL has no such column
marker, so this backend answers the mechanism probe ``False`` and the core
formatter refuses the clause instead of rendering a token the server rejects.

The probe is declared explicitly, the way ``supports_sequence_order`` is:
a ``False`` answer is a decision about the backend's grammar, and it should be
readable here rather than inferred from an inherited default.
"""

from rhosocial.activerecord.backend.dialect.mixins import AutoIncrementColumnMixin


class PostgresAutoIncrementColumnMixin(AutoIncrementColumnMixin):
    """PostgreSQL auto-increment marker override implementation."""

    def supports_auto_increment_column(self) -> bool:
        """Whether a bare ``AUTO_INCREMENT`` column marker can be used.

        Always ``False``: ``AUTO_INCREMENT`` is MySQL / MariaDB syntax and is
        absent from the PostgreSQL grammar. The server-generated-value
        mechanism PostgreSQL does have is the ``SERIAL`` type family; the
        capability-level answer lives on the dialect
        (``PostgresDialect.supports_database_assigned_integer_column``), which
        is what a declaration asks for.
        """
        return False
