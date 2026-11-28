# src/rhosocial/activerecord/backend/impl/postgres/expression/statements/ddl/__init__.py
"""
PostgreSQL DDL expressions.

Directory structure:
- vacuum.py     - VACUUM/ANALYZE expressions
- partition.py  - Partition DDL expressions
- index.py      - Index DDL expressions
- index_definition.py - PostgreSQL index definition / CREATE INDEX expressions
- statistics.py - Statistics DDL expressions
- comment.py   - COMMENT expressions
- mv.py         - Materialized view expressions
- type.py       - Enum/Range type expressions
- extension.py  - Extension DDL expressions
- policy.py     - Row-Level Security POLICY expressions (CREATE/ALTER/DROP)
- rls_config.py - Row-Level Security table configuration (ENABLE/DISABLE/FORCE)
- table_settings.py - ALTER TABLE SET LOGGED/UNLOGGED/ACCESS METHOD
- cluster.py    - CLUSTER expressions
- domain.py     - CREATE/ALTER/DROP DOMAIN expressions
- collation.py  - CREATE/DROP COLLATION object expressions
- foreign_table.py - CREATE/DROP FOREIGN TABLE expressions
- routine.py    - CREATE/DROP FUNCTION / AGGREGATE expressions
- publication.py - CREATE/DROP PUBLICATION / SUBSCRIPTION expressions

PostgreSQL index expressions
============================
The generic ``CreateIndexStatement`` / ``DropIndexStatement`` (from
``rhosocial.activerecord.backend.expression.statements.ddl_index``) carry all
common parameters.

PostgreSQL-only features are supplied through typed fields, not a
``dialect_options`` bag:

* ``PostgresCreateIndexExpression`` adds ``opclasses``,
  ``nulls_not_distinct`` and ``with_options`` for ``CREATE INDEX``.
* ``PostgresIndexDefinition`` adds the same options to an inline table index
  definition.
* ``PostgresDropIndexExpression`` marks PostgreSQL ownership of a
  ``DROP INDEX`` (the generic expression already carries ``concurrent``).

The typed fields are consumed by ``PostgresIndexMixin`` (see ``index.py``).

Example::

    from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
    from rhosocial.activerecord.backend.impl.postgres.expression.statements.ddl import (
        PostgresCreateIndexExpression,
    )

    d = PostgresDialect((15, 0, 0))

    # NULLS NOT DISTINCT via a typed field
    expr = PostgresCreateIndexExpression(
        d, "idx_uniq_abc", "t", ["a", "b"],
        unique=True,
        nulls_not_distinct=True,
    )
    sql, _ = expr.to_sql()   # → CREATE UNIQUE INDEX … NULLS NOT DISTINCT
"""

import importlib

_MODULES = (
    'alter_column', 'cluster', 'collation', 'column', 'comment', 'constraint', 'domain', 'exclude_constraint',
    'extension', 'extension_index', 'foreign_table', 'index', 'index_definition', 'index_type', 'multirange', 'mv',
    'partition', 'pg_partman', 'policy', 'procedure', 'publication', 'repack', 'rls_config', 'routine',
    'statistics', 'table_options', 'table_settings', 'type', 'vacuum', 'vector',
)


def __getattr__(name):
    """Resolve a public name from this namespace lazily (no import cycles)."""
    for module_name in _MODULES:
        module = importlib.import_module(f'.{module_name}', __name__)
        namespace = vars(module)
        if name in namespace:
            return namespace[name]
        nested = namespace.get('__getattr__')
        if nested is not None:
            try:
                return nested(name)
            except AttributeError:
                continue
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
