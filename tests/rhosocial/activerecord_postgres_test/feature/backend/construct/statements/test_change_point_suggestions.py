# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/statements/test_change_point_suggestions.py
"""Focused contract tests for PostgreSQL's four migration change-point answers.

No server is required. Each answer is exercised on a real PostgresDialect with a
frozen point built the way core's change planner builds it, and the returned
operations are real expression nodes that render through the dialect's own
formatters. Exact SQL strings pin the PostgreSQL spelling of every supported
chain.
"""
import dataclasses

import pytest
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import (
    DropTableStatement,
    IndexDefinition,
    PartitionClause,
)
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AddTableConstraintAction,
    AlterTableStatement,
    DropIndexAction,
    DropTableConstraintAction,
    AddIndexAction,
)
from rhosocial.activerecord.backend.expression.statements.ddl_index import (
    CreateIndexStatement,
    DropIndexStatement,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    TableConstraint,
    TableUniqueConstraint,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.suggestion import (
    ColumnReorderSuggestion,
    PartitionSchemeChangeSuggestion,
    TableConstraintChangeSuggestion,
    TableIndexChangeSuggestion,
)
from rhosocial.activerecord.backend.suggestion.protocols import (
    ColumnReorderSuggestionSupport,
    PartitionSchemeChangeSuggestionSupport,
    TableConstraintChangeSuggestionSupport,
    TableIndexChangeSuggestionSupport,
)
from rhosocial.activerecord.migration.schema_change import _freeze

CASES = [
    (ColumnReorderSuggestionSupport, ColumnReorderSuggestion, "suggest_column_reorder"),
    (
        TableConstraintChangeSuggestionSupport,
        TableConstraintChangeSuggestion,
        "suggest_table_constraint_change",
    ),
    (
        TableIndexChangeSuggestionSupport,
        TableIndexChangeSuggestion,
        "suggest_table_index_change",
    ),
    (
        PartitionSchemeChangeSuggestionSupport,
        PartitionSchemeChangeSuggestion,
        "suggest_partition_scheme_change",
    ),
]


def _dialect(version=(14, 0, 0)):
    return PostgresDialect(version=version)


def _constraint(dialect, name="uq_created", column="created_at"):
    return TableConstraint(
        dialect, branch=TableUniqueConstraint(dialect, [column]), name=name
    )


def _index(dialect, name="idx_created", column="created_at", **kwargs):
    return IndexDefinition(dialect, name=name, columns=[column], **kwargs)


def _partition(dialect, word="HASH", column="id"):
    return PartitionClause(dialect, word, [Column(dialect, column)])


def _sql(operations):
    return [operation.to_sql()[0] for operation in operations]


# --------------------------------------------------------------- registration


def test_dialect_satisfies_all_four_change_point_protocols():
    dialect = _dialect()
    for protocol, value_type, method in CASES:
        assert isinstance(dialect, protocol), protocol.__name__
        answer = getattr(dialect, method)(value_type(table_name="events"))
        assert isinstance(answer, value_type), protocol.__name__
        assert answer.label == value_type().label
        assert dataclasses.is_dataclass(answer)


# --------------------------------------------------------------- column reorder


def test_column_reorder_is_explicitly_unsupported_with_documented_reason():
    dialect = _dialect()
    point = ColumnReorderSuggestion(
        table_name="events",
        old_order=("id", "created_at"),
        new_order=("created_at", "id"),
        reordered_columns=("id", "created_at"),
    )
    answer = dialect.suggest_column_reorder(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"
    doc = dialect.suggest_column_reorder.__doc__
    assert "RENAME COLUMN" in doc and "SET DATA TYPE" in doc and "rebuild" in doc


# --------------------------------------------------------------- constraints


def test_constraint_drop_chain_renders_alter_table_drop_constraint_if_exists():
    dialect = _dialect()
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(_freeze(_constraint(dialect)),),
        new_constraints=(),
    )
    operations = dialect.suggest_table_constraint_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    statement = operations[0]
    assert isinstance(statement, AlterTableStatement)
    assert len(statement.actions) == 1
    assert isinstance(statement.actions[0], DropTableConstraintAction)
    assert _sql(operations) == ['ALTER TABLE "events" DROP CONSTRAINT IF EXISTS "uq_created"']


def test_constraint_add_reuses_live_table_constraint_node():
    dialect = _dialect()
    live = _constraint(dialect)
    point = TableConstraintChangeSuggestion(
        table_name="events", old_constraints=(), new_constraints=(live,)
    )
    operations = dialect.suggest_table_constraint_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    assert isinstance(operations[0].actions[0], AddTableConstraintAction)
    assert operations[0].actions[0].constraint is live
    assert _sql(operations) == ['ALTER TABLE "events" ADD CONSTRAINT "uq_created" UNIQUE ("created_at")']


def test_constraint_add_from_frozen_projection_is_false():
    """A frozen projection cannot distinguish PRIMARY KEY from UNIQUE."""
    dialect = _dialect()
    point = TableConstraintChangeSuggestion(
        table_name="events", old_constraints=(), new_constraints=(_freeze(_constraint(dialect)),)
    )
    answer = dialect.suggest_table_constraint_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"


def test_constraint_change_with_no_difference_is_absent():
    dialect = _dialect()
    frozen = _freeze(_constraint(dialect))
    point = TableConstraintChangeSuggestion(
        table_name="events", old_constraints=(frozen,), new_constraints=(frozen,)
    )
    assert dialect.suggest_table_constraint_change(point).operations is None


def test_constraint_change_without_a_name_is_false():
    dialect = _dialect()
    unnamed = _constraint(dialect)
    unnamed.name = None
    point = TableConstraintChangeSuggestion(
        table_name="events", old_constraints=(_freeze(unnamed),), new_constraints=()
    )
    assert dialect.suggest_table_constraint_change(point).operations is False


# --------------------------------------------------------------------- indexes


def test_index_drop_chain_renders_standalone_drop_index():
    dialect = _dialect()
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(_freeze(_index(dialect)),), new_indexes=()
    )
    operations = dialect.suggest_table_index_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    assert isinstance(operations[0], DropIndexStatement)
    assert _sql(operations) == ['DROP INDEX "idx_created"']


def test_index_create_chain_renders_standalone_create_index():
    dialect = _dialect()
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(), new_indexes=(_freeze(_index(dialect)),)
    )
    operations = dialect.suggest_table_index_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    assert isinstance(operations[0], CreateIndexStatement)
    assert isinstance(operations[0].table, Table) and operations[0].table.name == "events"
    assert _sql(operations) == ['CREATE INDEX "idx_created" ON "events" ("created_at")']


def test_index_create_carries_declared_options_in_dialect_format():
    dialect = _dialect(version=(15, 0, 0))
    index = _index(
        dialect,
        name="idx_uq",
        column="a",
        unique=True,
        if_not_exists=True,
        type="BTREE",
        include_columns=["b"],
        tablespace="fast",
    )
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(), new_indexes=(_freeze(index),)
    )
    operations = dialect.suggest_table_index_change(point).operations
    assert _sql(operations) == [
        'CREATE UNIQUE INDEX IF NOT EXISTS "idx_uq" ON "events" USING BTREE ("a") '
        'INCLUDE ("b") TABLESPACE "fast"'
    ]


def test_index_alter_table_actions_are_never_used():
    dialect = _dialect()
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(), new_indexes=(_freeze(_index(dialect)),)
    )
    operations = dialect.suggest_table_index_change(point).operations
    assert all(not isinstance(op, (AlterTableStatement, AddIndexAction, DropIndexAction))
               for op in operations)


def test_index_change_with_no_difference_is_absent():
    dialect = _dialect()
    frozen = _freeze(_index(dialect))
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(frozen,), new_indexes=(frozen,)
    )
    assert dialect.suggest_table_index_change(point).operations is None


def test_index_change_with_frozen_expression_column_or_predicate_is_false():
    dialect = _dialect()
    expression_index = IndexDefinition(dialect, "idx_expr", [Column(dialect, "a")])
    partial_index = IndexDefinition(
        dialect,
        "idx_partial",
        ["a"],
        partial_condition=Column(dialect, "a") != Column(dialect, "b"),
    )
    for index in (expression_index, partial_index):
        point = TableIndexChangeSuggestion(
            table_name="events", old_indexes=(), new_indexes=(_freeze(index),)
        )
        assert dialect.suggest_table_index_change(point).operations is False


def test_index_change_reuses_live_expression_nodes():
    dialect = _dialect()
    live = IndexDefinition(dialect, "idx_expr", [Column(dialect, "a")])
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(), new_indexes=(live,)
    )
    operations = dialect.suggest_table_index_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    assert operations[0].columns[0] is live.columns[0]
    assert "idx_expr" in _sql(operations)[0]


def test_drop_index_concurrently_is_gated_by_version():
    index = _index(_dialect(), concurrent=True)
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(_freeze(index),), new_indexes=()
    )
    assert _dialect((17, 0, 0)).suggest_table_index_change(point).operations is False
    operations = _dialect((18, 0, 0)).suggest_table_index_change(point).operations
    assert _sql(operations) == ['DROP INDEX CONCURRENTLY "idx_created"']


@pytest.mark.parametrize("version", [None, (13, 0, 0), (14, 0, 0), (17, 0, 0), (18, 0, 0)])
def test_create_index_concurrently_version_gate(version):
    index = _index(_dialect(), concurrent=True)
    point = TableIndexChangeSuggestion(
        table_name="events", old_indexes=(), new_indexes=(_freeze(index),)
    )
    dialect = _dialect(version) if version else PostgresDialect()
    operations = dialect.suggest_table_index_change(point).operations
    if version and version >= (11, 0, 0):
        assert _sql(operations) == [
            'CREATE INDEX CONCURRENTLY "idx_created" ON "events" ("created_at")'
        ]
    else:
        assert operations is False


# ----------------------------------------------------------- partition scheme


def test_scheme_adoption_reuses_build_clause():
    dialect = _dialect()
    point = PartitionSchemeChangeSuggestion(
        table_name="events", new_partition=_freeze(_partition(dialect))
    )
    operations = dialect.suggest_partition_scheme_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    assert isinstance(operations[0], PartitionClause)
    assert _sql(operations)[0].strip() == 'PARTITION BY HASH ("id")'
    assert all(isinstance(op, BaseExpression) for op in operations)


def test_scheme_removal_reuses_table_drop_statement():
    dialect = _dialect()
    point = PartitionSchemeChangeSuggestion(
        table_name="events", old_partition=_freeze(_partition(dialect))
    )
    operations = dialect.suggest_partition_scheme_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    assert isinstance(operations[0], DropTableStatement)
    assert _sql(operations) == ['DROP TABLE IF EXISTS "events"']


def test_scheme_switch_is_false_because_postgres_cannot_alter_partitioning():
    dialect = _dialect()
    point = PartitionSchemeChangeSuggestion(
        table_name="events",
        old_partition=_freeze(_partition(dialect, "HASH", "id")),
        new_partition=_freeze(_partition(dialect, "RANGE", "created_at")),
    )
    answer = dialect.suggest_partition_scheme_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"


def test_scheme_with_no_change_is_absent():
    dialect = _dialect()
    frozen = _freeze(_partition(dialect))
    point = PartitionSchemeChangeSuggestion(
        table_name="events", old_partition=frozen, new_partition=frozen
    )
    assert dialect.suggest_partition_scheme_change(point).operations is None


def test_scheme_with_unsupported_method_or_no_keys_is_false():
    dialect = _dialect()
    key = _partition(dialect, "KEY", "id")
    # A clause cannot be constructed without keys, but a point may still carry
    # a projection that lost them; the answer must decline rather than guess.
    no_keys = ("params", (("keys", ()), ("method", "HASH")))
    for clause in (key, no_keys):
        point = PartitionSchemeChangeSuggestion(
            table_name="events", new_partition=_freeze(clause)
        )
        assert dialect.suggest_partition_scheme_change(point).operations is False


@pytest.mark.parametrize("version", [None, (14, 0, 0), (17, 0, 0), (18, 0, 0)])
def test_community_postgres_has_no_merge_or_split_partition(version):
    """Pin the corrected fact at every version, including the unset version."""
    dialect = _dialect(version) if version else PostgresDialect()
    plan = object()
    strategy = _partition(dialect)
    assert dialect.suggest_partition_merge(strategy, plan).operations is False
    assert dialect.suggest_partition_split(strategy, plan).operations is False
    point = PartitionSchemeChangeSuggestion(
        table_name="events",
        old_partition=_freeze(_partition(dialect, "HASH", "id")),
        new_partition=_freeze(_partition(dialect, "RANGE", "id")),
    )
    assert dialect.suggest_partition_scheme_change(point).operations is False
