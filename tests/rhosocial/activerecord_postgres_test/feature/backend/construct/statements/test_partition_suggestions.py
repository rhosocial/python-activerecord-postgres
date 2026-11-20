# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/statements/test_partition_suggestions.py
"""Focused contract tests for PostgreSQL's eight partition suggestion mixins.

No server is required. Each method is exercised on a real PostgresDialect with a
concrete PostgresPartitionPlan / PartitionSpec, and the returned operations are
real expression nodes that render through the dialect's own formatters.
"""

from datetime import date

from rhosocial.activerecord.backend.dialect.protocols import (
    PartitionAddSuggestionSupport,
    PartitionBuildSuggestionSupport,
    PartitionDropSuggestionSupport,
    PartitionMergeSuggestionSupport,
    PartitionRetentionSuggestionSupport,
    PartitionSplitSuggestionSupport,
    PartitionTableAlterSuggestionSupport,
    PartitionTableDropSuggestionSupport,
)
from rhosocial.activerecord.backend.expression import (
    PartitionAddSuggestion,
    PartitionBuildSuggestion,
    PartitionDropSuggestion,
    PartitionMergeSuggestion,
    PartitionRetentionSuggestion,
    PartitionSplitSuggestion,
    PartitionTableAlterSuggestion,
    PartitionTableDropSuggestion,
)
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.statements import (
    DropTableStatement,
    PartitionClause,
    PartitionStrategy,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresCreatePartitionStatement,
    PostgresDetachPartitionStatement,
)
from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch,
    HashPolicy,
    ListPolicy,
    PostgresPartitionPlan,
    RangePolicy,
)

CASES = [
    ("build", PartitionBuildSuggestionSupport, PartitionBuildSuggestion),
    ("table_drop", PartitionTableDropSuggestionSupport, PartitionTableDropSuggestion),
    ("table_alter", PartitionTableAlterSuggestionSupport, PartitionTableAlterSuggestion),
    ("add", PartitionAddSuggestionSupport, PartitionAddSuggestion),
    ("drop", PartitionDropSuggestionSupport, PartitionDropSuggestion),
    ("split", PartitionSplitSuggestionSupport, PartitionSplitSuggestion),
    ("merge", PartitionMergeSuggestionSupport, PartitionMergeSuggestion),
    ("retention", PartitionRetentionSuggestionSupport, PartitionRetentionSuggestion),
]


def _dialect():
    return PostgresDialect(version=(14, 0, 0))


def _range_plan():
    return PostgresPartitionPlan(
        parent="orders",
        method="RANGE",
        columns=("created_at",),
        schema="public",
        policy=RangePolicy(bounds=((date(2024, 1, 1), date(2024, 2, 1)),)),
    )


def test_dialect_satisfies_all_eight_protocols_and_returns_right_dataclass():
    dialect = _dialect()
    plan = _range_plan()
    for name, protocol, dataclass in CASES:
        assert isinstance(dialect, protocol), name
        suggestion = getattr(dialect, "suggest_partition_" + name)(PartitionStrategy.RANGE, plan)
        assert isinstance(suggestion, dataclass), name


def test_build_renders_partition_by_clause_for_supported_method():
    dialect = _dialect()
    suggestion = dialect.suggest_partition_build(PartitionStrategy.RANGE, _range_plan())
    assert isinstance(suggestion.operations, tuple) and len(suggestion.operations) == 1
    clause = suggestion.operations[0]
    assert isinstance(clause, PartitionClause)
    assert isinstance(clause.keys[0], Column)
    sql, _ = clause.to_sql()
    assert "PARTITION BY RANGE" in sql and "created_at" in sql


def test_build_refuses_non_partition_strategy():
    assert _dialect().suggest_partition_build("KEY", _range_plan()).operations is False


def test_table_drop_renders_drop_table():
    suggestion = _dialect().suggest_partition_table_drop(PartitionStrategy.RANGE, _range_plan())
    assert isinstance(suggestion.operations, tuple) and len(suggestion.operations) == 1
    statement = suggestion.operations[0]
    assert isinstance(statement, DropTableStatement)
    assert isinstance(statement.table, Table) and statement.table.name == "orders"
    sql, _ = statement.to_sql()
    assert "DROP TABLE" in sql and "orders" in sql


def test_no_mechanism_points_are_false():
    dialect = _dialect()
    plan = _range_plan()
    assert dialect.suggest_partition_table_alter(PartitionStrategy.RANGE, plan).operations is False
    assert dialect.suggest_partition_split(PartitionStrategy.RANGE, plan).operations is False
    assert dialect.suggest_partition_merge(PartitionStrategy.RANGE, plan).operations is False


def test_add_renders_create_table_partition_of():
    dialect = _dialect()
    suggestion = dialect.suggest_partition_add(PartitionStrategy.RANGE, _range_plan())
    assert isinstance(suggestion.operations, tuple) and len(suggestion.operations) == 1
    statement = suggestion.operations[0]
    assert isinstance(statement, PostgresCreatePartitionStatement)
    assert statement.partition_type == "RANGE"
    assert "from" in statement.partition_values and "to" in statement.partition_values
    sql, _ = statement.to_sql()
    assert "PARTITION OF" in sql and "FOR VALUES FROM" in sql


def test_add_hash_and_list_plans_render_for_values():
    dialect = _dialect()
    hash_plan = PostgresPartitionPlan(
        parent="events",
        method="HASH",
        columns="tenant",
        policy=HashPolicy(modulus=4, remainders=(0,)),
    )
    statement = dialect.suggest_partition_add(PartitionStrategy.HASH, hash_plan).operations[0]
    assert isinstance(statement, PostgresCreatePartitionStatement)
    assert "MODULUS 4, REMAINDER 0" in statement.to_sql()[0]

    list_plan = PostgresPartitionPlan(
        parent="tickets",
        method="LIST",
        columns="status",
        policy=ListPolicy(groups=(("open",),)),
    )
    statement = dialect.suggest_partition_add(PartitionStrategy.LIST, list_plan).operations[0]
    assert isinstance(statement, PostgresCreatePartitionStatement)
    assert "FOR VALUES IN" in statement.to_sql()[0]


def test_drop_leaf_is_detach_then_drop_chain():
    dialect = _dialect()
    suggestion = dialect.suggest_partition_drop(PartitionStrategy.RANGE, _range_plan())
    assert isinstance(suggestion.operations, tuple) and len(suggestion.operations) == 2
    detach, drop = suggestion.operations
    assert isinstance(detach, PostgresDetachPartitionStatement)
    assert detach.concurrently is True
    assert isinstance(drop, DropTableStatement)
    assert "DETACH PARTITION" in detach.to_sql()[0]
    assert "CONCURRENTLY" in detach.to_sql()[0]
    assert "DROP TABLE" in drop.to_sql()[0]


def test_drop_subtree_is_cascading_drop():
    plan = PostgresPartitionPlan(
        parent="events",
        method="HASH",
        columns="tenant",
        modulus=2,
        branches=(
            HashBranch(0, child=RangePolicy(bounds=((date(2024, 1, 1), date(2024, 2, 1)),))),
        ),
    )
    suggestion = _dialect().suggest_partition_drop(PartitionStrategy.HASH, plan)
    assert isinstance(suggestion.operations, tuple) and len(suggestion.operations) == 1
    statement = suggestion.operations[0]
    assert isinstance(statement, DropTableStatement) and statement.cascade is True
    assert isinstance(statement.table, Table)


def test_retention_needs_dated_range_plan():
    dialect = _dialect()
    suggestion = dialect.suggest_partition_retention(PartitionStrategy.RANGE, _range_plan())
    assert isinstance(suggestion.operations, tuple) and len(suggestion.operations) == 2
    assert isinstance(suggestion.operations[0], PostgresDetachPartitionStatement)
    assert isinstance(suggestion.operations[1], DropTableStatement)

    list_plan = PostgresPartitionPlan(
        parent="tickets",
        method="LIST",
        columns="status",
        policy=ListPolicy(groups=(("open",),)),
    )
    assert (
        dialect.suggest_partition_retention(PartitionStrategy.LIST, list_plan).operations is False
    )


def test_all_returned_operations_are_real_expressions():
    plan = _range_plan()
    dialect = _dialect()
    for name, _protocol, _dataclass in CASES:
        operations = getattr(dialect, "suggest_partition_" + name)(
            PartitionStrategy.RANGE, plan
        ).operations
        if isinstance(operations, tuple):
            assert operations
            assert all(isinstance(op, BaseExpression) for op in operations), name
            for op in operations:
                op.to_sql()
