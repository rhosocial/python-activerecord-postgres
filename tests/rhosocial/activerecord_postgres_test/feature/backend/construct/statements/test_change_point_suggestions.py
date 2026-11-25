# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/statements/test_change_point_suggestions.py
"""Focused contract tests for PostgreSQL's four migration change-point answers.

No server is required. Each answer is exercised on a real PostgresDialect with a
frozen point built the way core's change planner builds it, and the returned
operations are real expression nodes that render through the dialect's own
formatters. Exact SQL strings pin the PostgreSQL spelling of every supported
chain.
"""
import dataclasses
import re

import pytest
from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.expression.objects import Table
from rhosocial.activerecord.backend.expression.predicates import ComparisonPredicate
from rhosocial.activerecord.backend.expression.statements import (
    ColumnDefinition,
    ConstraintValidation,
    DropTableStatement,
    IndexDefinition,
    PartitionClause,
)
from rhosocial.activerecord.backend.expression.types import DataType
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
    TableCheckConstraint,
    TableConstraint,
    TableConstraintBranch,
    TableExcludeConstraint,
    TableForeignKeyConstraint,
    TablePrimaryKeyConstraint,
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


def test_column_reorder_stays_false_with_full_column_declarations():
    """Full typed definitions do not invent a column-position clause."""
    dialect = _dialect()
    old = ColumnDefinition(dialect, "id", DataType.as_type(dialect, "INTEGER"))
    new = ColumnDefinition(dialect, "created_at", DataType.as_type(dialect, "TEXT"))
    point = ColumnReorderSuggestion(
        table_name="events",
        old_order=("id",),
        new_order=("created_at",),
        reordered_columns=("id",),
        old_fields={"id": old},
        new_fields={"created_at": new},
    )
    answer = dialect.suggest_column_reorder(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"


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


# ---- constraints recovered from the enriched live declarations


@pytest.mark.parametrize(
    ("make_branch", "expected"),
    [
        (
            lambda d: TablePrimaryKeyConstraint(d, ["id"]),
            'ALTER TABLE "events" ADD CONSTRAINT "ckind" PRIMARY KEY ("id")',
        ),
        (
            lambda d: TableUniqueConstraint(d, ["email"]),
            'ALTER TABLE "events" ADD CONSTRAINT "ckind" UNIQUE ("email")',
        ),
        (
            lambda d: TableCheckConstraint(
                d, ComparisonPredicate(d, ">", Column(d, "age"), Column(d, "limit"))
            ),
            'ALTER TABLE "events" ADD CONSTRAINT "ckind" CHECK ("age" > "limit")',
        ),
        (
            lambda d: TableForeignKeyConstraint(
                d, ["user_id"], (Table(d, "users"), ["id"])
            ),
            'ALTER TABLE "events" ADD CONSTRAINT "ckind" '
            'FOREIGN KEY ("user_id") REFERENCES "users"("id")',
        ),
        (
            lambda d: TableExcludeConstraint(d, ["room"], operator="&&", index_method="gist"),
            'ALTER TABLE "events" ADD CONSTRAINT "ckind" '
            'EXCLUDE USING gist ("room" WITH &&)',
        ),
    ],
)
def test_constraint_add_from_enriched_declaration_renders_spellable_kind(
    make_branch, expected
):
    """The live branch says which kind the frozen projection could not."""
    dialect = _dialect()
    live = TableConstraint(dialect, branch=make_branch(dialect), name="ckind")
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(),
        new_constraints=(_freeze(live),),
        new_constraint_declarations=(live,),
    )
    operations = dialect.suggest_table_constraint_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    statement = operations[0]
    assert isinstance(statement, AlterTableStatement)
    assert len(statement.actions) == 1
    action = statement.actions[0]
    assert isinstance(action, AddTableConstraintAction)
    assert action.constraint is live
    assert _sql(operations) == [expected]


def test_constraint_add_from_frozen_projection_without_declaration_stays_false():
    """A frozen branch cannot distinguish PRIMARY KEY from UNIQUE."""
    dialect = _dialect()
    live = TableConstraint(
        dialect, branch=TablePrimaryKeyConstraint(dialect, ["id"]), name="pk_events"
    )
    point = TableConstraintChangeSuggestion(
        table_name="events", old_constraints=(), new_constraints=(_freeze(live),)
    )
    answer = dialect.suggest_table_constraint_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"


def test_constraint_add_with_unspellable_branch_from_declaration_is_false():
    dialect = _dialect()

    class _UnspellableBranch(TableConstraintBranch):
        @property
        def format_method(self):
            return "format_no_such_constraint"

    live = TableConstraint(dialect, branch=_UnspellableBranch(dialect), name="weird")
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(),
        new_constraints=(_freeze(live),),
        new_constraint_declarations=(live,),
    )
    answer = dialect.suggest_table_constraint_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"


def test_constraint_add_with_not_valid_trait_gate():
    dialect = _dialect()
    predicate = ComparisonPredicate(dialect, "=", Column(dialect, "a"), Column(dialect, "b"))
    check = TableConstraint(
        dialect,
        branch=TableCheckConstraint(dialect, predicate),
        name="ck",
        validation=ConstraintValidation.NOVALIDATE,
    )
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(),
        new_constraints=(_freeze(check),),
        new_constraint_declarations=(check,),
    )
    assert _sql(dialect.suggest_table_constraint_change(point).operations) == [
        'ALTER TABLE "events" ADD CONSTRAINT "ck" CHECK ("a" = "b") NOT VALID'
    ]
    # NOT VALID on a kind PostgreSQL refuses it for is not spelled by guessing.
    primary = TableConstraint(
        dialect,
        branch=TablePrimaryKeyConstraint(dialect, ["id"]),
        name="pk",
        validation=ConstraintValidation.NOVALIDATE,
    )
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(),
        new_constraints=(_freeze(primary),),
        new_constraint_declarations=(primary,),
    )
    assert dialect.suggest_table_constraint_change(point).operations is False


def test_constraint_modification_drops_then_adds_one_action_per_statement():
    dialect = _dialect()
    old = TableConstraint(dialect, branch=TableUniqueConstraint(dialect, ["name"]), name="uq")
    new = TableConstraint(dialect, branch=TableUniqueConstraint(dialect, ["email"]), name="uq")
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(_freeze(old),),
        new_constraints=(_freeze(new),),
        old_constraint_declarations=(old,),
        new_constraint_declarations=(new,),
    )
    operations = dialect.suggest_table_constraint_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 2
    assert [len(statement.actions) for statement in operations] == [1, 1]
    assert isinstance(operations[0].actions[0], DropTableConstraintAction)
    assert operations[1].actions[0].constraint is new
    assert _sql(operations) == [
        'ALTER TABLE "events" DROP CONSTRAINT IF EXISTS "uq"',
        'ALTER TABLE "events" ADD CONSTRAINT "uq" UNIQUE ("email")',
    ]


def test_constraint_change_with_no_difference_and_declarations_is_absent():
    dialect = _dialect()
    live = _constraint(dialect)
    frozen = _freeze(live)
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(frozen,),
        new_constraints=(frozen,),
        old_constraint_declarations=(live,),
        new_constraint_declarations=(live,),
    )
    answer = dialect.suggest_table_constraint_change(point)
    assert answer.operations is None
    assert answer.state == "absent"


def test_constraint_drop_from_enriched_declaration_stays_safe():
    dialect = _dialect()
    live = _constraint(dialect)
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(_freeze(live),),
        new_constraints=(),
        old_constraint_declarations=(live,),
    )
    operations = dialect.suggest_table_constraint_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    assert isinstance(operations[0], AlterTableStatement)
    assert isinstance(operations[0].actions[0], DropTableConstraintAction)
    assert _sql(operations) == ['ALTER TABLE "events" DROP CONSTRAINT IF EXISTS "uq_created"']
    assert all("DROP TABLE" not in sql.upper() for sql in _sql(operations))


def test_constraint_change_without_a_name_stays_false_with_declarations():
    dialect = _dialect()
    unnamed = _constraint(dialect)
    unnamed.name = None
    point = TableConstraintChangeSuggestion(
        table_name="events",
        old_constraints=(_freeze(unnamed),),
        new_constraints=(),
        old_constraint_declarations=(unnamed,),
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


# ---- indexes recovered from the enriched live declarations


def test_index_create_from_enriched_expression_and_predicate_reuses_live_nodes():
    dialect = _dialect()
    predicate = ComparisonPredicate(dialect, "=", Column(dialect, "a"), Column(dialect, "b"))
    live = IndexDefinition(
        dialect, "idx_expr_partial", [Column(dialect, "a"), Column(dialect, "b")],
        partial_condition=predicate,
    )
    frozen = _freeze(live)
    # The old answer, with only the frozen projection, cannot rebuild either node.
    assert dialect.suggest_table_index_change(
        TableIndexChangeSuggestion(table_name="events", new_indexes=(frozen,))
    ).operations is False
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(),
        new_indexes=(frozen,),
        new_index_declarations=(live,),
    )
    operations = dialect.suggest_table_index_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    statement = operations[0]
    assert isinstance(statement, CreateIndexStatement)
    assert statement.columns[0] is live.columns[0]
    assert statement.where is live.partial_condition
    assert _sql(operations) == [
        'CREATE INDEX "idx_expr_partial" ON "events" ("a", "b") WHERE "a" = "b"'
    ]


def test_index_create_from_enriched_declaration_renders_rich_options():
    dialect = _dialect((15, 0, 0))
    live = IndexDefinition(
        dialect,
        "idx_rich",
        ["a"],
        unique=True,
        if_not_exists=True,
        type="BTREE",
        include_columns=["b"],
        tablespace="fast",
        partial_condition=ComparisonPredicate(
            dialect, "=", Column(dialect, "a"), Column(dialect, "b")
        ),
    )
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(),
        new_indexes=(_freeze(live),),
        new_index_declarations=(live,),
    )
    operations = dialect.suggest_table_index_change(point).operations
    assert isinstance(operations, tuple) and len(operations) == 1
    assert all(not isinstance(op, (AlterTableStatement, AddIndexAction, DropIndexAction))
               for op in operations)
    assert _sql(operations) == [
        'CREATE UNIQUE INDEX IF NOT EXISTS "idx_rich" ON "events" '
        'USING BTREE ("a") INCLUDE ("b") WHERE "a" = "b" TABLESPACE "fast"'
    ]


def test_index_create_concurrently_from_enriched_declaration_is_version_gated():
    live = IndexDefinition(
        _dialect(), "idx_conc", ["a"], concurrent=True, if_not_exists=True, type="BTREE"
    )
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(),
        new_indexes=(_freeze(live),),
        new_index_declarations=(live,),
    )
    assert _dialect((10, 0, 0)).suggest_table_index_change(point).operations is False
    operations = _dialect((11, 0, 0)).suggest_table_index_change(point).operations
    assert _sql(operations) == [
        'CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_conc" '
        'ON "events" USING BTREE ("a")'
    ]


def test_drop_index_concurrently_from_enriched_declaration_is_version_gated():
    live = IndexDefinition(_dialect(), "idx_drop", ["a"], if_exists=True, concurrent=True)
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(_freeze(live),),
        new_indexes=(),
        old_index_declarations=(live,),
    )
    assert _dialect((17, 0, 0)).suggest_table_index_change(point).operations is False
    operations = _dialect((18, 0, 0)).suggest_table_index_change(point).operations
    assert _sql(operations) == ['DROP INDEX CONCURRENTLY IF EXISTS "idx_drop"']


@pytest.mark.parametrize(
    ("version", "option"),
    [
        ((9, 4, 0), {"if_not_exists": True}),
        ((10, 0, 0), {"include_columns": ["b"]}),
        ((10, 0, 0), {"concurrent": True}),
    ],
)
def test_index_unsupported_options_from_declaration_stay_false(version, option):
    dialect = _dialect(version)
    live = IndexDefinition(dialect, "idx_opt", ["a"], **option)
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(),
        new_indexes=(_freeze(live),),
        new_index_declarations=(live,),
    )
    answer = dialect.suggest_table_index_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"


def test_index_change_with_no_difference_and_declarations_is_absent():
    dialect = _dialect()
    live = _index(dialect)
    frozen = _freeze(live)
    point = TableIndexChangeSuggestion(
        table_name="events",
        old_indexes=(frozen,),
        new_indexes=(frozen,),
        old_index_declarations=(live,),
        new_index_declarations=(live,),
    )
    answer = dialect.suggest_table_index_change(point)
    assert answer.operations is None
    assert answer.state == "absent"


# ----------------------------------------------------------- partition scheme


def test_scheme_adoption_is_explicitly_unsupported_not_a_clause_fragment():
    dialect = _dialect()
    point = PartitionSchemeChangeSuggestion(
        table_name="events", new_partition=_freeze(_partition(dialect))
    )
    answer = dialect.suggest_partition_scheme_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"
    doc = dialect.suggest_partition_scheme_change.__doc__
    assert "CREATE TABLE" in doc and "standalone" in doc


def test_scheme_removal_is_explicitly_unsupported_not_a_drop_table():
    dialect = _dialect()
    point = PartitionSchemeChangeSuggestion(
        table_name="events", old_partition=_freeze(_partition(dialect))
    )
    answer = dialect.suggest_partition_scheme_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"
    doc = dialect.suggest_partition_scheme_change.__doc__
    for word in ("data-preserving rebuild", "CREATE TABLE", "copy", "swap", "authorized"):
        assert word in doc


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


def test_scheme_stays_false_with_live_partition_declarations():
    """A live PARTITION BY clause is still a CREATE TABLE fragment, not a chain."""
    dialect = _dialect()
    old = _partition(dialect, "HASH", "id")
    new = _partition(dialect, "RANGE", "created_at")
    point = PartitionSchemeChangeSuggestion(
        table_name="events",
        old_partition=_freeze(old),
        new_partition=_freeze(new),
        old_partition_declaration=old,
        new_partition_declaration=new,
    )
    answer = dialect.suggest_partition_scheme_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"


@pytest.mark.parametrize("which", ["adopt", "abandon"])
def test_scheme_adopt_and_abandon_stay_false_with_live_declarations(which):
    dialect = _dialect()
    live = _partition(dialect)
    point = PartitionSchemeChangeSuggestion(
        table_name="events",
        new_partition=_freeze(live) if which == "adopt" else None,
        old_partition=None if which == "adopt" else _freeze(live),
        new_partition_declaration=live if which == "adopt" else None,
        old_partition_declaration=None if which == "adopt" else live,
    )
    answer = dialect.suggest_partition_scheme_change(point)
    assert answer.operations is False
    assert answer.state == "unaddressable"


@pytest.mark.parametrize("version", [None, (14, 0, 0), (17, 0, 0), (18, 0, 0)])
def test_merge_and_split_always_false_even_with_enriched_scheme(version):
    dialect = _dialect(version) if version else PostgresDialect()
    strategy = _partition(dialect)
    assert dialect.suggest_partition_merge(strategy, object()).operations is False
    assert dialect.suggest_partition_split(strategy, object()).operations is False
    point = PartitionSchemeChangeSuggestion(
        table_name="events",
        old_partition=_freeze(strategy),
        new_partition=_freeze(_partition(dialect, "RANGE", "id")),
        old_partition_declaration=strategy,
        new_partition_declaration=_partition(dialect, "RANGE", "id"),
    )
    assert dialect.suggest_partition_scheme_change(point).operations is False


# ------------------------------------------------------------ safety invariant

# resolve_suggestions appends every returned expression verbatim to
# plan.statements, which MigrationContext.apply renders and executes. A drop of
# a data-bearing object (or a truncate/delete) can never be a chain member.
# Dropping an index/constraint/trigger is recoverable and stays allowed.
_BENIGN_DROP_CLASSES = {
    "DropTableConstraintAction",
    "DropIndexAction",
    "DropIndexStatement",
    "DropFulltextIndexStatement",
    "PostgresDropIndexExpression",
    "DropTriggerStatement",
    "DropDomainDefaultAction",
    "DropDomainNotNullAction",
    "DropDomainCheckAction",
    "PostgresDropDomainCheckAction",
    "PostgresDropPolicyStatement",
    "PostgresDropTypeAttributeAction",
}
_ROW_DESTROYING_CLASSES = {"TruncateStatement", "DeleteStatement"}
_DESTRUCTIVE_SQL = re.compile(
    r"\b(?:DROP\s+(?:TABLE|SCHEMA|DATABASE|DOMAIN|FUNCTION|SEQUENCE|TYPE|"
    r"MATERIALIZED\s+VIEW|VIEW|COLUMN|FOREIGN\s+TABLE)|TRUNCATE|DELETE\s+FROM)\b",
    re.IGNORECASE,
)


def _is_destructive_class(class_name):
    if class_name in _ROW_DESTROYING_CLASSES:
        return True
    return (
        class_name.startswith(("Drop", "PostgresDrop"))
        and class_name not in _BENIGN_DROP_CLASSES
    )


def _change_point_cases(dialect):
    """Representative points for all four consumed change-point protocols."""
    frozen_partition = _freeze(_partition(dialect))
    frozen_constraint = _freeze(_constraint(dialect))
    frozen_index = _freeze(_index(dialect))
    return [
        (
            "suggest_column_reorder",
            ColumnReorderSuggestion(
                table_name="events",
                old_order=("id",),
                new_order=("name",),
                reordered_columns=("id",),
            ),
        ),
        (
            "suggest_table_constraint_change",
            TableConstraintChangeSuggestion(
                table_name="events", old_constraints=(frozen_constraint,), new_constraints=()
            ),
        ),
        (
            "suggest_table_constraint_change",
            TableConstraintChangeSuggestion(
                table_name="events", old_constraints=(), new_constraints=(_constraint(dialect),)
            ),
        ),
        (
            "suggest_table_constraint_change",  # enriched add
            TableConstraintChangeSuggestion(
                table_name="events",
                old_constraints=(),
                new_constraints=(_freeze(_constraint(dialect)),),
                new_constraint_declarations=(_constraint(dialect),),
            ),
        ),
        (
            "suggest_table_index_change",
            TableIndexChangeSuggestion(
                table_name="events", old_indexes=(frozen_index,), new_indexes=()
            ),
        ),
        (
            "suggest_table_index_change",
            TableIndexChangeSuggestion(
                table_name="events", old_indexes=(), new_indexes=(frozen_index,)
            ),
        ),
        (
            "suggest_table_index_change",  # enriched expression/predicate create
            TableIndexChangeSuggestion(
                table_name="events",
                old_indexes=(),
                new_indexes=(
                    _freeze(
                        IndexDefinition(
                            dialect,
                            "idx_rich",
                            [Column(dialect, "a")],
                            partial_condition=ComparisonPredicate(
                                dialect, "=", Column(dialect, "a"), Column(dialect, "b")
                            ),
                        )
                    ),
                ),
                new_index_declarations=(
                    IndexDefinition(
                        dialect,
                        "idx_rich",
                        [Column(dialect, "a")],
                        partial_condition=ComparisonPredicate(
                            dialect, "=", Column(dialect, "a"), Column(dialect, "b")
                        ),
                    ),
                ),
            ),
        ),
        (
            "suggest_partition_scheme_change",  # adopt
            PartitionSchemeChangeSuggestion(
                table_name="events", new_partition=frozen_partition
            ),
        ),
        (
            "suggest_partition_scheme_change",  # abandon
            PartitionSchemeChangeSuggestion(
                table_name="events", old_partition=frozen_partition
            ),
        ),
        (
            "suggest_partition_scheme_change",  # switch
            PartitionSchemeChangeSuggestion(
                table_name="events",
                old_partition=frozen_partition,
                new_partition=_freeze(_partition(dialect, "RANGE", "created_at")),
            ),
        ),
        (
            "suggest_partition_scheme_change",  # enriched switch stays False
            PartitionSchemeChangeSuggestion(
                table_name="events",
                old_partition=frozen_partition,
                new_partition=_freeze(_partition(dialect, "RANGE", "created_at")),
                old_partition_declaration=_partition(dialect),
                new_partition_declaration=_partition(dialect, "RANGE", "created_at"),
            ),
        ),
    ]


def _chain_nodes(expression):
    """A statement plus any composite actions it carries."""
    yield expression
    actions = getattr(expression, "actions", ())
    if isinstance(actions, (list, tuple)):
        for action in actions:
            yield from _chain_nodes(action)


@pytest.mark.parametrize("version", [None, (14, 0, 0), (18, 0, 0)])
def test_no_change_suggestion_chain_contains_destructive_ddl(version):
    """No returned chain may carry a destructive DDL statement."""
    dialect = _dialect(version) if version else PostgresDialect()
    for method_name, point in _change_point_cases(dialect):
        operations = getattr(dialect, method_name)(point).operations
        if not isinstance(operations, (list, tuple)):
            continue
        for operation in operations:
            for node in _chain_nodes(operation):
                class_name = type(node).__name__
                rendered = node.to_sql()[0] if hasattr(node, "to_sql") else ""
                where = f"{method_name}: {class_name} -> {rendered}"
                assert not isinstance(node, DropTableStatement), where
                assert not _is_destructive_class(class_name), where
                assert not _DESTRUCTIVE_SQL.search(rendered), where
