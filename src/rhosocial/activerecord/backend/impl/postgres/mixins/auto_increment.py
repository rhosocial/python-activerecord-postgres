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
        mechanisms PostgreSQL does have are the ``SERIAL`` type family
        (:meth:`supports_database_assigned_integer_column` answers yes through
        it) and the SQL-standard identity clause
        (:class:`~...mixins.identity_column.PostgresIdentityColumnMixin`).
        """
        return False

    def supports_database_assigned_integer_column(self) -> bool:
        """Whether an integer column's value comes from the database.

        ``True``: SERIAL is a pseudo-type that carries the value, and its
        observable behaviour is the one a declaration asks about -- omit the
        value on INSERT and the database supplies it, and the row can return
        it. The spelling is not ``AUTO_INCREMENT`` (that probe stays ``False``),
        which is why the capability and the mechanism are separate questions.

        Not guaranteed: the MySQL counter behaviour. Inserting an explicit
        value larger than the sequence's current one raises MySQL's counter to
        ``max + 1``; a SERIAL column leaves its sequence untouched, so a later
        generated value may collide. Any test mixing explicit keys with
        generated ones must be scoped to the mechanism that behaves that way.
        """
        return True
