# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/constraint.py
"""PostgreSQL-specific constraint DDL support.

Provides PostgreSQL-specific constraint features:
- NOT VALID (add constraint without validating existing data)
- EXCLUDE constraints

Note: The format_add_table_constraint_action method is defined directly
in PostgresDialect rather than in this mixin, because Python's C3 MRO
places SQLDialectBase before mixins in the inheritance chain, which
prevents mixin methods from overriding base class methods of the same name.
This mixin is retained for organizational consistency within the ddl package.
"""


class PostgresConstraintMixin:
    """PostgreSQL-specific constraint formatting placeholder.

    The actual constraint formatting logic resides in PostgresDialect
    for MRO compatibility. See PostgresDialect.format_add_table_constraint_action
    and PostgresDialect._format_exclude_constraint.
    """
    pass
