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

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresConstraintSupport(Protocol):
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
