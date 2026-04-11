# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/constraint.py
"""PostgreSQL-specific constraint capability defaults.

Provides default implementations for PostgreSQL-proprietary constraint
features that are not part of the SQL standard.

SQL standard constraint capability defaults are defined in the core
ConstraintMixin (rhosocial.activerecord.backend.dialect.mixins).

Note: The format_add_table_constraint_action method is defined directly
in PostgresDialect rather than in this mixin, because Python's C3 MRO
places SQLDialectBase before mixins in the inheritance chain, which
prevents mixin methods from overriding base class methods of the same name.
This mixin provides capability detection defaults only.
"""


class PostgresConstraintMixin:
    """PostgreSQL-proprietary constraint capability defaults.

    These features are PostgreSQL-specific and not part of the SQL standard.
    Both NOT VALID and EXCLUDE constraints are supported by all PostgreSQL
    versions, so they default to True.

    SQL standard constraint capability defaults are in ConstraintMixin.
    """

    def supports_constraint_novalidate(self) -> bool:
        """NOT VALID constraint option (PostgreSQL-proprietary).

        Allows adding a constraint without validating existing data.
        Supported by all PostgreSQL versions.
        """
        return True

    def supports_exclude_constraint(self) -> bool:
        """EXCLUDE constraints (PostgreSQL-proprietary).

        Exclusion constraints prevent overlapping rows on specified
        columns using specified operators.
        Supported by all PostgreSQL versions.
        """
        return True
