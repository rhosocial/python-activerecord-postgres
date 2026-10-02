# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_expression_fields_match_formatters.py
"""A formatter may not read a field its statement does not carry.

A statement formatter that reads ``expr.schema_name`` needs the expression to
have that attribute. When the formatter was changed to qualify names and the
expression was not given the field, the result is not wrong SQL -- it is an
``AttributeError`` on a statement that can never be built, which is how
SQLServerColumnstoreIndexExpression reached CI.

These were source scans rather than runtime tests. A scan does not work here:
whether the field exists depends on inheritance reaching core, which lives in
another repository, and on **core_kwargs forwarding. Reading the source of this
repository can see neither, so a scan reported defects that were not there --
two were chased down and both were false alarms -- while a field genuinely
removed still passed. Building the statement answers the question the defect
actually asks: does this statement build, and does the schema reach the SQL?
"""
import importlib
import inspect

import pytest

#: Statement fields a formatter may read that some expression classes carry
#: under a different name. Reading these by their own name is the defect.
#: TruncateExpression and the PostgreSQL vacuum/statistics expressions name the
#: field `schema`; the DDL statements name it `schema_name`.
KNOWN_ALIASES = {
    "schema": {"TruncateExpression"},
}


class TestQualifiedStatementsRender:
    """A statement whose formatter qualifies names must build with a schema.

    Checked by building each statement and rendering it, not by scanning
    source. Each case names the statement and how to build it, so adding
    coverage for a newly qualified object type is one entry rather than a new
    mechanism.
    """

    @pytest.fixture
    def dialect(self):
        from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

        return PostgresDialect(version=(16, 0, 0))

    def test_create_index(self, dialect):
        """Both the index and the table it sits on are qualified."""
        from rhosocial.activerecord.backend.expression import CreateIndexExpression

        expr = CreateIndexExpression(
            dialect, index_name="idx_orders_id", table_name="orders", columns=["id"]
        )
        assert expr.to_sql()[0] == (
            'CREATE INDEX "idx_orders_id" ON "orders" ("id")'
        ), expr.to_sql()[0]
        qualified = CreateIndexExpression(
            dialect,
            index_name="idx_orders_id",
            table_name="orders",
            columns=["id"],
            schema_name="app",
        )
        assert qualified.to_sql()[0] == (
            'CREATE INDEX "app"."idx_orders_id" ON "app"."orders" ("id")'
        ), qualified.to_sql()[0]

    def test_drop_index(self, dialect):
        from rhosocial.activerecord.backend.expression import DropIndexExpression

        expr = DropIndexExpression(
            dialect, index_name="idx_orders_id", table_name="orders"
        )
        assert expr.to_sql()[0] == 'DROP INDEX "idx_orders_id"', expr.to_sql()[0]
        qualified = DropIndexExpression(
            dialect,
            index_name="idx_orders_id",
            table_name="orders",
            schema_name="app",
        )
        assert qualified.to_sql()[0] == (
            'DROP INDEX "app"."idx_orders_id"'
        ), qualified.to_sql()[0]

    def test_create_trigger(self, dialect):
        """The trigger, its table and the function it calls are all qualified."""
        from rhosocial.activerecord.backend.expression import CreateTriggerExpression
        from rhosocial.activerecord.backend.expression.statements.ddl_trigger import (
            TriggerEvent,
            TriggerLevel,
            TriggerTiming,
        )

        def build(schema_name=None):
            return CreateTriggerExpression(
                dialect,
                trigger_name="trg_audit",
                table_name="orders",
                timing=TriggerTiming.BEFORE,
                events=[TriggerEvent.INSERT],
                function_name="audit_fn",
                level=TriggerLevel.ROW,
                schema_name=schema_name,
            )

        expr = build()
        assert expr.to_sql()[0] == (
            'CREATE TRIGGER "trg_audit" BEFORE INSERT ON "orders" FOR EACH ROW '
            'EXECUTE FUNCTION "audit_fn"()'
        ), expr.to_sql()[0]
        qualified = build(schema_name="app").to_sql()[0]
        assert qualified == (
            'CREATE TRIGGER "app"."trg_audit" BEFORE INSERT ON "app"."orders" '
            'FOR EACH ROW EXECUTE FUNCTION "app"."audit_fn"()'
        ), qualified

    def test_create_view(self, dialect):
        from rhosocial.activerecord.backend.expression import (
            Column,
            CreateViewExpression,
            QueryExpression,
        )

        query = QueryExpression(dialect, [Column(dialect, "id")], from_="orders")
        expr = CreateViewExpression(dialect, view_name="v_orders", query=query)
        assert expr.to_sql()[0] == (
            'CREATE VIEW "v_orders" AS SELECT "id" FROM "orders"'
        ), expr.to_sql()[0]
        qualified = CreateViewExpression(
            dialect, view_name="v_orders", query=query, schema_name="app"
        )
        assert qualified.to_sql()[0] == (
            'CREATE VIEW "app"."v_orders" AS SELECT "id" FROM "orders"'
        ), qualified.to_sql()[0]

    def test_drop_type_inherits_the_field_from_core(self, dialect):
        """The case a source scan got wrong in both directions.

        DropTypeExpression lives in core and assigns schema_name there. A scan
        of this repository sees the formatter reading the field and no
        assignment at all, so it either misses a field that is there or reports
        one that is not, depending on how it resolves the base. Building it
        settles the question.
        """
        from rhosocial.activerecord.backend.expression import DropTypeExpression

        expr = DropTypeExpression(dialect, type_name="my_enum")
        assert expr.to_sql()[0] == 'DROP TYPE "my_enum"', expr.to_sql()[0]
        qualified = DropTypeExpression(
            dialect, type_name="my_enum", schema_name="app"
        )
        assert qualified.to_sql()[0] == 'DROP TYPE "app"."my_enum"', qualified.to_sql()[0]


class TestExpressionSignatures:
    """The expressions this backend's formatters qualify must take the field."""

    @pytest.mark.parametrize(
        "import_path,class_name",
        [
            (
                "rhosocial.activerecord.backend.expression.statements.ddl_index",
                "CreateIndexExpression",
            ),
            (
                "rhosocial.activerecord.backend.expression.statements.ddl_index",
                "DropIndexExpression",
            ),
            (
                "rhosocial.activerecord.backend.expression.statements.ddl_trigger",
                "CreateTriggerExpression",
            ),
            (
                "rhosocial.activerecord.backend.expression.statements.ddl_view",
                "CreateViewExpression",
            ),
            (
                "rhosocial.activerecord.backend.expression.statements.ddl_type",
                "DropTypeExpression",
            ),
        ],
    )
    def test_qualified_expression_accepts_schema_name(self, import_path, class_name):
        module = importlib.import_module(import_path)
        cls = getattr(module, class_name)
        params = inspect.signature(cls.__init__).parameters
        assert "schema_name" in params, (
            f"{class_name} is qualified by its formatter, so it needs the "
            f"field; got {list(params)}"
        )
        assert params["schema_name"].default is None, (
            f"{class_name} must default schema_name to None -- None is what "
            f"means unqualified"
        )
