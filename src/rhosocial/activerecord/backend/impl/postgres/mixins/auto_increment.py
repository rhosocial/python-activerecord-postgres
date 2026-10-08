# src/rhosocial/activerecord/backend/impl/postgres/mixins/auto_increment.py
"""PostgreSQL auto-increment marker support implementation.

``AUTO_INCREMENT`` is MySQL / MariaDB spelling. PostgreSQL has no such column
marker, so this backend answers the mechanism probe ``False`` and the core
formatter refuses the clause instead of rendering a token the server rejects.

The probe is declared explicitly, the way ``supports_sequence_order`` is:
a ``False`` answer is a decision about the backend's grammar, and it should be
readable here rather than inferred from an inherited default.
"""

from rhosocial.activerecord.backend.dialect.mixins import AutoIncrementMixin


class PostgresAutoIncrementMixin(AutoIncrementMixin):
    """PostgreSQL auto-increment marker override implementation."""

    def supports_auto_increment_column(self) -> bool:
        """Whether a bare ``AUTO_INCREMENT`` column marker can be used.

        Always ``False``: ``AUTO_INCREMENT`` is MySQL / MariaDB syntax and is
        absent from the PostgreSQL grammar. The server-generated-value
        mechanisms PostgreSQL does have are the SQL-standard identity clause
        (:class:`~...mixins.identity_column.PostgresIdentityColumnMixin`) and
        the ``SERIAL`` type family.
        """
        return False
