# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/ddl/test_create_table_expression_diff.py
"""PostgreSQL coverage for expression-level CREATE TABLE diffing.

The generic diff pipeline (``CreateTableExpressionDiffMixin`` on
``SQLDialectBase``, dataclasses in
``rhosocial.activerecord.backend.expression.statements.ddl_diff``) is covered
backend-agnostically in the core repository. This module pins the
PostgreSQL-specific hook configuration:

- ``_supports_alter_column_properties()`` → True: SET/DROP DEFAULT and
  SET/DROP NOT NULL stay on the AlterColumn path and render PostgreSQL
  syntax (``ALTER TABLE ... ALTER COLUMN ...``).
- ``_supports_alter_column_type()`` → False: PostgreSQL can change types
  in place, but the core has no corresponding action class in v1, so type
  changes route to a rebuild plan.
- ``_supports_alter_table_index_actions()`` → False: PostgreSQL has no
  ``ALTER TABLE ADD/DROP INDEX`` (indexes use independent CREATE/DROP
  INDEX statements), so index changes route to a rebuild plan.

All cases are expression-level only — no database connection is used.
"""

import pytest

from rhosocial.activerecord.backend.dialect.protocols import CreateTableExpressionDiffSupport
from rhosocial.activerecord.backend.expression import DiffPlan, RebuildPlan
from rhosocial.activerecord.backend.expression.statements.ddl_alter import (
    AddColumn,
    AlterColumn,
    ColumnAlterOperation,
    RenameTable,
)
from rhosocial.activerecord.backend.expression.statements.ddl_table import (
    ColumnConstraint,
    ColumnConstraintType,
    ColumnDefinition,
    CreateTableExpression,
    IndexDefinition,
    TableConstraint,
    TableConstraintType,
)
from rhosocial.activerecord.backend.expression.types import (
    IntegerType,
    TextType,
    VarCharType,
)
from rhosocial.activerecord.backend.impl.dummy.dialect import DummyDialect
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


def _col(name, dtype, *constraints):
    return ColumnDefinition(name=name, data_type=dtype, constraints=list(constraints))


def _pk():
    return ColumnConstraint(constraint_type=ColumnConstraintType.PRIMARY_KEY)


def _not_null():
    return ColumnConstraint(constraint_type=ColumnConstraintType.NOT_NULL)


def _expr(dialect, columns, indexes=None, constraints=None, **kwargs):
    return CreateTableExpression(
        dialect=dialect,
        table=kwargs.pop("table", "items"),
        columns=columns,
        indexes=indexes,
        table_constraints=constraints,
        **kwargs,
    )


@pytest.fixture
def dialect():
    return PostgresDialect()


# ---------------------------------------------------------------------------
# Protocol conformance / capability hooks
# ---------------------------------------------------------------------------

class TestProtocolConformance:

    def test_postgres_dialect_satisfies_protocol(self, dialect):
        assert isinstance(dialect, CreateTableExpressionDiffSupport)

    def test_property_changes_stay_in_place(self, dialect):
        """SET/DROP DEFAULT, SET/DROP NOT NULL are all legal on PostgreSQL."""
        assert dialect._supports_alter_column_properties() is True

    def test_type_changes_rebuild(self, dialect):
        # PostgreSQL supports ALTER COLUMN TYPE, but the core has no
        # type-change action class in v1 — see hook docstring.
        assert dialect._supports_alter_column_type() is False

    def test_index_actions_rebuild(self, dialect):
        # PostgreSQL has no ALTER TABLE ADD/DROP INDEX; the renderers raise
        # UnsupportedFeatureError, so the diff must not emit them.
        assert dialect._supports_alter_table_index_actions() is False

    def test_add_index_renderer_rejects(self, dialect):
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression.statements.ddl_alter import AddIndex
        action = AddIndex(dialect, index=IndexDefinition(name="idx_x", columns=["id"]))
        with pytest.raises(UnsupportedFeatureError):
            dialect.format_add_index_action(action)

    def test_drop_index_renderer_rejects(self, dialect):
        from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
        from rhosocial.activerecord.backend.expression.statements.ddl_alter import DropIndex
        action = DropIndex(dialect, index="idx_x")
        with pytest.raises(UnsupportedFeatureError):
            dialect.format_drop_index_action(action)


# ---------------------------------------------------------------------------
# No change
# ---------------------------------------------------------------------------

class TestNoChange:

    def test_identical_definitions_empty_plan(self, dialect):
        cols = [_col("id", IntegerType(), _pk()), _col("name", TextType())]
        plan = _expr(dialect, cols).diff(_expr(dialect, cols))
        assert isinstance(plan, DiffPlan)
        assert not plan.has_changes
        assert plan.alters == []
        assert plan.rebuild is None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class TestValidation:

    def test_cross_dialect_raises(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(DummyDialect(), [_col("id", IntegerType(), _pk())])
        with pytest.raises(ValueError, match="different dialects"):
            old.diff(new)

    def test_cross_table_raises(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())], table="other")
        with pytest.raises(ValueError, match="different tables"):
            old.diff(new)


# ---------------------------------------------------------------------------
# Column add / drop
# ---------------------------------------------------------------------------

class TestColumnChanges:

    def test_added_column_yields_add_action(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("bio", TextType())])
        plan = old.diff(new)
        assert plan.rebuild is None and plan.has_changes
        (alter,) = plan.alters
        (action,) = alter.actions
        assert isinstance(action, AddColumn)
        assert action.column.name == "bio"
        sql, params = alter.to_sql()
        assert sql == 'ALTER TABLE "items"  ADD COLUMN "bio" TEXT'
        assert params == ()

    def test_removed_column_yields_drop_action(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("bio", TextType())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())])
        plan = old.diff(new)
        (alter,) = plan.alters
        assert type(alter.actions[0]).__name__ == "DropColumn"
        sql, _ = alter.to_sql()
        assert 'DROP COLUMN "bio"' in sql


# ---------------------------------------------------------------------------
# Column property changes → AlterColumn (PostgreSQL syntax)
# ---------------------------------------------------------------------------

class TestColumnPropertyChanges:

    def _plan_for(self, dialect, old_col, new_col):
        return _expr(dialect, [old_col]).diff(_expr(dialect, [new_col]))

    def test_set_default(self, dialect):
        plan = self._plan_for(
            dialect,
            _col("status", TextType()),
            _col("status", TextType(),
                 ColumnConstraint(constraint_type=ColumnConstraintType.DEFAULT, default_value="ok")),
        )
        (action,) = plan.alters[0].actions
        assert isinstance(action, AlterColumn)
        assert action.operation == ColumnAlterOperation.SET_DEFAULT
        assert action.new_value == "ok"
        sql, params = plan.alters[0].to_sql()
        assert sql == 'ALTER TABLE "items"  ALTER COLUMN "status" SET DEFAULT %s'
        assert params == ("ok",)

    def test_drop_default(self, dialect):
        plan = self._plan_for(
            dialect,
            _col("status", TextType(),
                 ColumnConstraint(constraint_type=ColumnConstraintType.DEFAULT, default_value="ok")),
            _col("status", TextType()),
        )
        (action,) = plan.alters[0].actions
        assert action.operation == ColumnAlterOperation.DROP_DEFAULT
        sql, params = plan.alters[0].to_sql()
        assert sql == 'ALTER TABLE "items"  ALTER COLUMN "status" DROP DEFAULT'
        assert params == ()

    def test_set_not_null(self, dialect):
        plan = self._plan_for(
            dialect,
            _col("name", TextType()),
            _col("name", TextType(), _not_null()),
        )
        (action,) = plan.alters[0].actions
        assert action.operation == ColumnAlterOperation.SET_NOT_NULL
        sql, params = plan.alters[0].to_sql()
        assert sql == 'ALTER TABLE "items"  ALTER COLUMN "name" SET NOT NULL'
        assert params == ()

    def test_drop_not_null(self, dialect):
        plan = self._plan_for(
            dialect,
            _col("name", TextType(), _not_null()),
            _col("name", TextType()),
        )
        (action,) = plan.alters[0].actions
        assert action.operation == ColumnAlterOperation.DROP_NOT_NULL
        sql, params = plan.alters[0].to_sql()
        assert sql == 'ALTER TABLE "items"  ALTER COLUMN "name" DROP NOT NULL'
        assert params == ()


# ---------------------------------------------------------------------------
# Type changes → RebuildPlan
# ---------------------------------------------------------------------------

class TestTypeChangeRebuild:

    def test_type_change_yields_rebuild_plan(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", IntegerType())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", TextType())])
        plan = old.diff(new)
        assert plan.alters == []
        rp = plan.rebuild
        assert isinstance(rp, RebuildPlan)
        assert "type change" in rp.reason

    def test_rebuild_plan_shape(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", IntegerType())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", TextType())])
        rp = old.diff(new).rebuild
        assert rp.create.table_name == "items__rebuild__"
        assert rp.drop_old.table.name == "items"
        assert rp.temp_table_name == "items__rebuild__"
        rename_action = rp.rename.actions[0]
        assert isinstance(rename_action, RenameTable)
        assert rename_action.new_name == "items"
        assert rp.copy_columns == ["id", "code"]
        stmts = rp.ordered_statements()
        assert stmts[0] is rp.create and stmts[1] is rp.drop_old and stmts[2] is rp.rename

    def test_rebuild_plan_renders_sql(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", IntegerType())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", TextType())])
        rp = old.diff(new).rebuild
        create_sql, _ = rp.create.to_sql()
        drop_sql, _ = rp.drop_old.to_sql()
        rename_sql, _ = rp.rename.to_sql()
        assert create_sql == 'CREATE TABLE "items__rebuild__" ("id" INTEGER PRIMARY KEY, "code" TEXT)'
        assert drop_sql == 'DROP TABLE "items"'
        assert rename_sql == 'ALTER TABLE "items__rebuild__"  RENAME TO "items"'

    def test_length_change_rebuilds(self, dialect):
        old = _expr(dialect, [_col("name", VarCharType(length=50))])
        new = _expr(dialect, [_col("name", VarCharType(length=100))])
        plan = old.diff(new)
        assert plan.alters == []
        assert plan.rebuild is not None


# ---------------------------------------------------------------------------
# Index changes → RebuildPlan (no ALTER TABLE ADD/DROP INDEX on PostgreSQL)
# ---------------------------------------------------------------------------

class TestIndexChanges:

    def test_added_index(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())],
                    indexes=[IndexDefinition(name="idx_id", columns=["id"])])
        plan = old.diff(new)
        assert plan.alters == []
        rp = plan.rebuild
        assert rp is not None
        assert "index change" in rp.reason
        # The recreated table carries the new index set.
        assert {i.name for i in rp.create.indexes} == {"idx_id"}

    def test_removed_index(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk())],
                    indexes=[IndexDefinition(name="idx_id", columns=["id"])])
        new = _expr(dialect, [_col("id", IntegerType(), _pk())])
        plan = old.diff(new)
        assert plan.alters == []
        assert plan.rebuild is not None
        assert plan.rebuild.create.indexes == []

    def test_redefined_index_rebuilds(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", IntegerType())],
                    indexes=[IndexDefinition(name="idx_code", columns=["code"])])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("code", IntegerType())],
                    indexes=[IndexDefinition(name="idx_code", columns=["code"], unique=True)])
        plan = old.diff(new)
        assert plan.alters == []
        assert plan.rebuild is not None


# ---------------------------------------------------------------------------
# Table constraints
# ---------------------------------------------------------------------------

class TestTableConstraintChanges:

    def test_pk_change_rebuilds(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType()), _col("code", TextType())],
                    constraints=[TableConstraint(
                        constraint_type=TableConstraintType.PRIMARY_KEY, columns=["id"])])
        new = _expr(dialect, [_col("id", IntegerType()), _col("code", TextType())],
                    constraints=[TableConstraint(
                        constraint_type=TableConstraintType.PRIMARY_KEY, columns=["code"])])
        plan = old.diff(new)
        assert plan.rebuild is not None
        assert "primary key" in plan.rebuild.reason

    def test_named_unique_constraint_add(self, dialect):
        old = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("email", TextType())])
        new = _expr(dialect, [_col("id", IntegerType(), _pk()), _col("email", TextType())],
                    constraints=[TableConstraint(
                        constraint_type=TableConstraintType.UNIQUE,
                        name="uq_email", columns=["email"])])
        plan = old.diff(new)
        (alter,) = plan.alters
        assert len(alter.actions) == 1
        assert type(alter.actions[0]).__name__ == "AddTableConstraint"
        sql, params = alter.to_sql()
        assert 'ADD CONSTRAINT "uq_email" UNIQUE ("email")' in sql
        assert params == ()
