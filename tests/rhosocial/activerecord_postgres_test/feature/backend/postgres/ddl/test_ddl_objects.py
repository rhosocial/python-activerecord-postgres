# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/ddl/test_ddl_objects.py
"""Tests for PostgreSQL DDL expression classes.

This module tests the expression-based format methods for DDL operations,
including materialized view refresh, comment, and partition expressions.
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.dialect.exceptions import (
    UnsupportedFeatureError,
)
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    PostgresVacuumExpression,  # noqa: F401
    PostgresAnalyzeExpression,  # noqa: F401
    AlterDomainActionType,
    PostgresCreateDomainExpression,
    PostgresAlterDomainExpression,
    PostgresDropDomainExpression,
    PostgresCreateCollationExpression,
    PostgresDropCollationExpression,
    PostgresCreateForeignTableExpression,
    PostgresDropForeignTableExpression,
    PostgresCreateFunctionExpression,
    PostgresDropFunctionExpression,
    PostgresCreateAggregateExpression,
    PostgresDropAggregateExpression,
    PostgresCreatePublicationExpression,
    PostgresDropPublicationExpression,
    PostgresCreateSubscriptionExpression,
    PostgresDropSubscriptionExpression,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.dml.extended_statistics import (
    PostgresExtendedStatisticsMixin,  # noqa: F401
)

class TestPostgresDomainExpression:
    """CREATE / ALTER / DROP DOMAIN."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateDomainExpression(
            dialect, "posint", "NUMERIC",
            default="0", constraints=["CHECK (VALUE > 0)"],
        ).to_sql()
        assert sql == "CREATE DOMAIN \"posint\" AS NUMERIC DEFAULT 0 CHECK (VALUE > 0)"

    def test_alter_set_default(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.SET_DEFAULT, new_value="0"
        ).to_sql()
        assert sql == 'ALTER DOMAIN "posint" SET DEFAULT 0'

    def test_alter_rename(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.RENAME_TO, new_name="posint2"
        ).to_sql()
        assert sql == 'ALTER DOMAIN "posint" RENAME TO "posint2"'

    def test_drop(self, dialect):
        sql, _ = PostgresDropDomainExpression(
            dialect, "posint", if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP DOMAIN IF EXISTS "posint" CASCADE'

    def test_create_with_schema_and_collation(self, dialect):
        sql, _ = PostgresCreateDomainExpression(
            dialect, "posint", "NUMERIC",
            schema="app", collation="C",
        ).to_sql()
        assert sql == 'CREATE DOMAIN "app"."posint" AS NUMERIC COLLATE C'

    def test_create_without_default(self, dialect):
        sql, _ = PostgresCreateDomainExpression(
            dialect, "posint", "NUMERIC", constraints=["CHECK (VALUE > 0)"]
        ).to_sql()
        assert sql == 'CREATE DOMAIN "posint" AS NUMERIC CHECK (VALUE > 0)'

    def test_alter_drop_default(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.DROP_DEFAULT
        ).to_sql()
        assert sql == 'ALTER DOMAIN "posint" DROP DEFAULT'

    def test_alter_set_default_with_schema(self, dialect):
        sql, _ = PostgresAlterDomainExpression(
            dialect, "posint", AlterDomainActionType.SET_DEFAULT,
            schema="app", new_value="1",
        ).to_sql()
        assert sql == 'ALTER DOMAIN "app"."posint" SET DEFAULT 1'

    def test_alter_unsupported_action_raises(self, dialect):
        expr = PostgresAlterDomainExpression(dialect, "posint", "RANDOM")
        with pytest.raises(ValueError, match="Unsupported ALTER DOMAIN"):
            expr.to_sql()

    def test_drop_restrict(self, dialect):
        sql, _ = PostgresDropDomainExpression(
            dialect, "posint", schema="app", restrict=True
        ).to_sql()
        assert sql == 'DROP DOMAIN "app"."posint" RESTRICT'

    def test_drop_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropDomainExpression(
            dialect, "posint", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateDomainExpression(d, "posint", "INTEGER").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresAlterDomainExpression(
                d, "posint", AlterDomainActionType.SET_DEFAULT, new_value="0"
            ).to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropDomainExpression(d, "posint").to_sql()


class TestPostgresCollationDDLExpression:
    """CREATE / DROP COLLATION DDL."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateCollationExpression(
            dialect, "enloc", locale="en_US.UTF-8", provider="libc"
        ).to_sql()
        assert sql == 'CREATE COLLATION "enloc" (LOCALE = en_US.UTF-8, PROVIDER = libc)'

    def test_create_if_not_exists(self, dialect):
        sql, _ = PostgresCreateCollationExpression(
            dialect, "myloc", if_not_exists=True, lc_collate="en_US",
        ).to_sql()
        assert sql == 'CREATE COLLATION IF NOT EXISTS "myloc" (LC_COLLATE = en_US)'

    def test_drop(self, dialect):
        sql, _ = PostgresDropCollationExpression(
            dialect, "enloc", if_exists=True
        ).to_sql()
        assert sql == 'DROP COLLATION IF EXISTS "enloc"'

    def test_create_with_schema_and_all_params(self, dialect):
        sql, _ = PostgresCreateCollationExpression(
            dialect, "custom", schema="app", if_not_exists=True,
            locale="en_US.UTF-8", lc_collate="en_US", lc_ctype="en_US",
            provider="icu", version="153.14",
        ).to_sql()
        assert sql == (
            'CREATE COLLATION IF NOT EXISTS "app"."custom" '
            '(LOCALE = en_US.UTF-8, LC_COLLATE = en_US, LC_CTYPE = en_US, '
            'PROVIDER = icu, VERSION = 153.14)'
        )

    def test_create_without_params(self, dialect):
        sql, _ = PostgresCreateCollationExpression(dialect, "bare").to_sql()
        assert sql == 'CREATE COLLATION "bare"'

    def test_drop_restrict_with_schema(self, dialect):
        sql, _ = PostgresDropCollationExpression(
            dialect, "enloc", schema="app", restrict=True
        ).to_sql()
        assert sql == 'DROP COLLATION "app"."enloc" RESTRICT'

    def test_drop_cascade(self, dialect):
        sql, _ = PostgresDropCollationExpression(
            dialect, "enloc", cascade=True
        ).to_sql()
        assert sql == 'DROP COLLATION "enloc" CASCADE'

    def test_drop_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropCollationExpression(
            dialect, "enloc", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateCollationExpression(d, "c").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropCollationExpression(d, "c").to_sql()


class TestPostgresForeignTableDDLExpression:
    """CREATE / DROP FOREIGN TABLE."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create(self, dialect):
        sql, _ = PostgresCreateForeignTableExpression(
            dialect, "ft", "srv", columns=["a integer", "b text"],
            options=["host 'h'"],
        ).to_sql()
        assert sql == (
            'CREATE FOREIGN TABLE "ft" (a integer, b text) '
            'SERVER "srv" OPTIONS (host \'h\')'
        )

    def test_drop(self, dialect):
        sql, _ = PostgresDropForeignTableExpression(
            dialect, "ft", if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP FOREIGN TABLE IF EXISTS "ft" CASCADE'

    def test_create_if_not_exists_schema(self, dialect):
        sql, _ = PostgresCreateForeignTableExpression(
            dialect, "ft", "srv", schema="app", if_not_exists=True,
            columns=["a integer"],
        ).to_sql()
        assert sql == (
            'CREATE FOREIGN TABLE IF NOT EXISTS "app"."ft" (a integer) '
            'SERVER "srv"'
        )

    def test_create_without_columns_or_options(self, dialect):
        sql, _ = PostgresCreateForeignTableExpression(
            dialect, "bar_ft", "fdw_bar"
        ).to_sql()
        assert sql == 'CREATE FOREIGN TABLE "bar_ft" SERVER "fdw_bar"'

    def test_drop_restrict_with_schema(self, dialect):
        sql, _ = PostgresDropForeignTableExpression(
            dialect, "ft", schema="app", restrict=True
        ).to_sql()
        assert sql == 'DROP FOREIGN TABLE "app"."ft" RESTRICT'

    def test_drop_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropForeignTableExpression(
            dialect, "ft", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateForeignTableExpression(d, "ft", "srv").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropForeignTableExpression(d, "ft").to_sql()


class TestPostgresRoutineDDLExpression:
    """CREATE / DROP FUNCTION and AGGREGATE."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_function(self, dialect):
        sql, _ = PostgresCreateFunctionExpression(
            dialect, "add", "integer", "int 1;",
            args=["a integer"], strict=True, security="DEFINER",
        ).to_sql()
        assert sql == (
            'CREATE FUNCTION "add"(a integer) RETURNS integer '
            'STRICT SECURITY DEFINER LANGUAGE plpgsql AS $$ int 1; $$'
        )

    def test_drop_function(self, dialect):
        sql, _ = PostgresDropFunctionExpression(
            dialect, "add", args=["integer"], cascade=True
        ).to_sql()
        assert sql == 'DROP FUNCTION "add" (integer) CASCADE'

    def test_create_aggregate(self, dialect):
        sql, _ = PostgresCreateAggregateExpression(
            dialect, "mysum", "sum", "integer", initcond="0"
        ).to_sql()
        assert sql == 'CREATE AGGREGATE "mysum" (SFUNC=sum, STYPE=integer, INITCOND=0)'

    def test_drop_aggregate(self, dialect):
        sql, _ = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", if_exists=True
        ).to_sql()
        assert sql == 'DROP AGGREGATE IF EXISTS "mysum" (integer)'

    def test_create_function_full_options(self, dialect):
        sql, _ = PostgresCreateFunctionExpression(
            dialect, "add", "integer", "int 1;", schema="app",
            args=["a integer", "b integer"], or_replace=True,
            security="INVOKER", cost=10.0, rows=50,
        ).to_sql()
        assert 'CREATE OR REPLACE FUNCTION "app"."add"' in sql
        assert "(a integer, b integer)" in sql
        assert "SECURITY INVOKER" in sql
        assert "COST 10.0" in sql
        assert "ROWS 50" in sql

    def test_create_function_no_args_no_strict(self, dialect):
        sql, _ = PostgresCreateFunctionExpression(
            dialect, "now", "timestamptz", "SELECT now();", language="sql"
        ).to_sql()
        assert sql == (
            'CREATE FUNCTION "now"() RETURNS timestamptz LANGUAGE sql '
            'AS $$ SELECT now(); $$'
        )

    def test_create_function_invalid_security(self, dialect):
        expr = PostgresCreateFunctionExpression(
            dialect, "f", "integer", "int 1;", security="OWNER"
        )
        with pytest.raises(ValueError, match="DEFINER"):
            expr.to_sql()

    def test_drop_function_restrict(self, dialect):
        sql, _ = PostgresDropFunctionExpression(
            dialect, "add", schema="app", if_exists=True, restrict=True
        ).to_sql()
        assert sql == 'DROP FUNCTION IF EXISTS "app"."add" RESTRICT'

    def test_drop_function_without_args(self, dialect):
        sql, _ = PostgresDropFunctionExpression(
            dialect, "add", schema="app"
        ).to_sql()
        assert sql == 'DROP FUNCTION "app"."add"'

    def test_drop_function_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropFunctionExpression(
            dialect, "add", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_create_aggregate_full(self, dialect):
        sql, _ = PostgresCreateAggregateExpression(
            dialect, "mysum", "sum", "integer", schema="app",
            finalfunc="mysum_final",
        ).to_sql()
        assert sql == (
            'CREATE AGGREGATE "app"."mysum" '
            '(SFUNC=sum, STYPE=integer, FINALFUNC=mysum_final)'
        )

    def test_drop_aggregate_cascade(self, dialect):
        sql, _ = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", schema="app", cascade=True
        ).to_sql()
        assert sql == 'DROP AGGREGATE "app"."mysum" (integer) CASCADE'

    def test_drop_aggregate_restrict(self, dialect):
        sql, _ = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", if_exists=True, restrict=True
        ).to_sql()
        assert sql == 'DROP AGGREGATE IF EXISTS "mysum" (integer) RESTRICT'

    def test_drop_aggregate_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropAggregateExpression(
            dialect, "mysum", "integer", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_routine_version_gate_96(self):
        d = PostgresDialect(version=(9, 5, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateFunctionExpression(
                d, "f", "integer", "int 1;"
            ).to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropFunctionExpression(d, "f").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateAggregateExpression(d, "s", "sum", "integer").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropAggregateExpression(d, "s", "integer").to_sql()


class TestPostgresPublicationExpression:
    """CREATE / DROP PUBLICATION and SUBSCRIPTION."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect(version=(14, 0, 0))

    def test_create_publication_tables(self, dialect):
        sql, _ = PostgresCreatePublicationExpression(
            dialect, "pub1", tables=["orders", "users"]
        ).to_sql()
        assert sql == 'CREATE PUBLICATION "pub1" FOR TABLE "orders", "users"'

    def test_create_publication_all_tables(self, dialect):
        sql, _ = PostgresCreatePublicationExpression(
            dialect, "pub_all", all_tables=True,
            options=["publish='insert'"],
        ).to_sql()
        assert sql == (
            'CREATE PUBLICATION "pub_all" FOR ALL TABLES WITH (publish=\'insert\')'
        )

    def test_drop_publication(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", if_exists=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION IF EXISTS "pub1"'

    def test_create_subscription(self, dialect):
        sql, _ = PostgresCreateSubscriptionExpression(
            dialect, "sub1", "host=db port=5432", ["pub1"]
        ).to_sql()
        assert sql == (
            'CREATE SUBSCRIPTION "sub1" CONNECTION \'host=db port=5432\' '
            'PUBLICATION "pub1"'
        )

    def test_drop_subscription(self, dialect):
        sql, _ = PostgresDropSubscriptionExpression(dialect, "sub1", cascade=True).to_sql()
        assert sql == 'DROP SUBSCRIPTION "sub1" CASCADE'

    def test_create_publication_tables_options(self, dialect):
        sql, _ = PostgresCreatePublicationExpression(
            dialect, "pub1", tables=["orders"],
            options=["publish='insert'"],
        ).to_sql()
        assert sql == (
            'CREATE PUBLICATION "pub1" FOR TABLE "orders" '
            "WITH (publish='insert')"
        )

    def test_create_publication_tables_and_all_raises(self, dialect):
        expr = PostgresCreatePublicationExpression(
            dialect, "pub1", tables=["orders"], all_tables=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_create_publication_neither_raises(self, dialect):
        expr = PostgresCreatePublicationExpression(dialect, "pub1")
        with pytest.raises(ValueError, match="requires either"):
            expr.to_sql()

    def test_drop_publication_cascade(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", cascade=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION "pub1" CASCADE'

    def test_drop_publication_restrict(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", restrict=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION "pub1" RESTRICT'

    def test_drop_publication_cascade_and_restrict_raises(self, dialect):
        expr = PostgresDropPublicationExpression(
            dialect, "pub1", cascade=True, restrict=True
        )
        with pytest.raises(ValueError, match="mutually exclusive"):
            expr.to_sql()

    def test_drop_publication_if_exists(self, dialect):
        sql, _ = PostgresDropPublicationExpression(
            dialect, "pub1", if_exists=True
        ).to_sql()
        assert sql == 'DROP PUBLICATION IF EXISTS "pub1"'

    def test_create_subscription_options(self, dialect):
        sql, _ = PostgresCreateSubscriptionExpression(
            dialect, "sub2", "host=db port=5432", ["pub1", "pub2"],
            options=["copy_data = false"],
        ).to_sql()
        assert sql == (
            'CREATE SUBSCRIPTION "sub2" CONNECTION \'host=db port=5432\' '
            'PUBLICATION "pub1", "pub2" WITH (copy_data = false)'
        )

    def test_drop_subscription_if_exists(self, dialect):
        sql, _ = PostgresDropSubscriptionExpression(
            dialect, "sub1", if_exists=True
        ).to_sql()
        assert sql == 'DROP SUBSCRIPTION IF EXISTS "sub1"'

    def test_pubsub_version_gate_10(self):
        d = PostgresDialect(version=(9, 6, 0))
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreatePublicationExpression(
                d, "pub1", tables=["orders"]
            ).to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropPublicationExpression(d, "pub1").to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateSubscriptionExpression(d, "s", "conn", ["p"]).to_sql()
        with pytest.raises(UnsupportedFeatureError):
            PostgresDropSubscriptionExpression(d, "s").to_sql()

