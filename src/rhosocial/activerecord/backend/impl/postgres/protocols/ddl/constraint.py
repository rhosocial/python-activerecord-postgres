# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/constraint.py
"""PostgreSQL-proprietary constraint capability protocol definitions.

This module contains the PostgresConstraintSupport protocol which defines
the interface for PostgreSQL's proprietary constraint features that are
not part of the SQL standard.

SQL standard constraint capabilities are defined in the core
ConstraintSupport protocol (rhosocial.activerecord.backend.dialect.protocols).

PostgreSQL-proprietary features:
- NOT VALID (add constraint without validating existing data)
- EXCLUDE constraints
"""

from typing import Protocol, runtime_checkable, Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.protocols import ConstraintSupport

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.statements import AddTableConstraint


@runtime_checkable
class PostgresConstraintSupport(ConstraintSupport, Protocol):
    """PostgreSQL-proprietary constraint capabilities protocol.

    These features are PostgreSQL-specific and not part of the SQL standard.
    SQL standard constraint features (PRIMARY KEY, UNIQUE, CHECK, FK, etc.)
    are defined in the core ConstraintSupport protocol.

    Feature Source: Native support (no extension required)

    Official Documentation:
    - NOT VALID: https://www.postgresql.org/docs/current/sql-altertable.html
    - EXCLUDE: https://www.postgresql.org/docs/current/ddl-constraints.html#DDL-CONSTRAINTS-EXCLUSION
    """

    def supports_constraint_novalidate(self) -> bool:
        """Whether NOT VALID constraint option is supported.

        PostgreSQL-proprietary feature. Allows adding a constraint
        without validating existing data, then validating later
        with VALIDATE CONSTRAINT.

        Native feature, all PostgreSQL versions.
        """
        ...

    def supports_exclude_constraint(self) -> bool:
        """Whether EXCLUDE constraints are supported.

        PostgreSQL-proprietary feature. Exclusion constraints ensure
        that no two rows overlap on specified columns using specified
        operators (e.g., && for range overlap).

        Native feature, all PostgreSQL versions.
        Requires appropriate operator class (e.g., btree_gist for gist).
        """
        ...

    def supports_drop_constraint_if_exists(self) -> bool:
        """Whether ``DROP CONSTRAINT IF EXISTS`` is supported.

        Vendor extension (not in ISO/IEC 9075-2 §11.10). PostgreSQL has
        supported it since 9.6.
        """
        ...

    def format_add_table_constraint_action(
        self, action: "AddTableConstraint"
    ) -> Tuple[str, tuple]:
        """Format ALTER TABLE ADD CONSTRAINT with PostgreSQL extensions.

        Adds EXCLUDE constraint support and the PostgreSQL-proprietary
        ``NOT VALID`` suffix to the standard formatting.

        Args:
            action: AddTableConstraint action to format.

        Returns:
            Tuple of (SQL string, parameters tuple)
        """
        ...
