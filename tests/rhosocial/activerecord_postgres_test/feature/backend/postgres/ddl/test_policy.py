# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/ddl/test_policy.py
"""Unit tests for PostgreSQL Row-Level Security POLICY DDL.

Tests cover:
- ``PostgresPolicyMixin`` capability switches across PG versions
- ``format_create_policy_statement`` for all FOR commands, AS clause
  variants, role lists (identifiers + reserved role keywords), USING /
  WITH CHECK expression parameter passing, command/expression
  incompatibilities, and version gates.
- ``format_alter_policy_statement`` for the RENAME form and the REPLACE
  form (independent clauses, mutual-exclusivity, REPLACE-no-clause error).
- ``format_drop_policy_statement`` for ``IF EXISTS`` / ``CASCADE`` /
  ``RESTRICT``, their mutual exclusion, and version gates.

These tests are pure SQL-string / params assertions — no active DB.
"""
import pytest

from rhosocial.activerecord.backend.expression import Column, Literal
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.postgres import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.policy import (
    AlterPolicyMode,
    PostgresAlterPolicyExpression,
    PostgresCreatePolicyExpression,
    PostgresDropPolicyExpression,
    PolicyCommand,
    PolicyType,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.ddl.policy import (
    PostgresPolicyMixin,
)
from rhosocial.activerecord.backend.impl.postgres.protocols import (
    PostgresPolicySupport,
)


# --------------------------------------------------------------------------- #
# Capability switches
# --------------------------------------------------------------------------- #
class TestPolicyFeatureDetection:
    """Test capability switches across PG versions."""

    @pytest.mark.parametrize("ver,expected", [
        ((9, 4, 0), False),
        ((9, 5, 0), True),
        ((10, 0, 0), True),
        ((14, 0, 0), True),
    ])
    def test_supports_create_policy(self, ver, expected):
        assert PostgresDialect(ver).supports_create_policy() is expected

    @pytest.mark.parametrize("ver,expected", [
        ((9, 5, 0), False),
        ((9, 6, 0), False),
        ((10, 0, 0), True),
        ((14, 0, 0), True),
    ])
    def test_supports_policy_restrictive(self, ver, expected):
        assert PostgresDialect(ver).supports_policy_restrictive() is expected

    @pytest.mark.parametrize("ver,expected", [
        ((9, 5, 0), False),
        ((10, 0, 0), True),
    ])
    def test_supports_explicit_permissive_keyword(self, ver, expected):
        assert PostgresDialect(ver).supports_explicit_permissive_keyword() is expected

    @pytest.mark.parametrize("ver,expected", [
        ((9, 4, 0), False),
        ((9, 5, 0), True),
        ((14, 0, 0), True),
    ])
    def test_supports_drop_policy_if_exists(self, ver, expected):
        assert PostgresDialect(ver).supports_drop_policy_if_exists() is expected

    @pytest.mark.parametrize("ver,expected", [
        ((9, 4, 0), False),
        ((9, 5, 0), True),
    ])
    def test_supports_alter_policy_rename(self, ver, expected):
        assert PostgresDialect(ver).supports_alter_policy_rename() is expected

    @pytest.mark.parametrize("ver,expected", [
        ((9, 4, 0), False),
        ((9, 5, 0), True),
    ])
    def test_supports_alter_policy_replace(self, ver, expected):
        assert PostgresDialect(ver).supports_alter_policy_replace() is expected

    @pytest.mark.parametrize("ver,expected", [
        ((9, 5, 0), False),
        ((14, 0, 0), False),
        ((15, 0, 0), True),
        ((18, 0, 0), True),
    ])
    def test_supports_current_role_keyword(self, ver, expected):
        assert PostgresDialect(ver).supports_current_role_keyword() is expected


# --------------------------------------------------------------------------- #
# Protocol conformance
# --------------------------------------------------------------------------- #
class TestPolicyProtocolConformance:
    """PostgresDialect must satisfy PostgresPolicySupport Protocol."""

    @pytest.mark.parametrize("ver", [(9, 5, 0), (10, 0, 0), (14, 0, 0), (18, 0, 0)])
    def test_isinstance_protocol(self, ver):
        assert isinstance(PostgresDialect(ver), PostgresPolicySupport)

    def test_protocol_runtime_checkable(self):
        # Protocol must be runtime_checkable (decorated)
        assert hasattr(PostgresPolicySupport, "_is_protocol")
        # PyPy / CPython alternative: explicit isinstance check
        assert isinstance(PostgresDialect((14, 0, 0)), PostgresPolicySupport)


# --------------------------------------------------------------------------- #
# CREATE POLICY
# --------------------------------------------------------------------------- #
class TestFormatCreatePolicyStatement:
    """Test CREATE POLICY statement formatting."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect((14, 0, 0))

    def test_basic_create(self, dialect):
        """Bare statement — only name + table required."""
        expr = PostgresCreatePolicyExpression(
            dialect, name="p1", table="orders"
        )
        sql, params = expr.to_sql()
        assert sql == 'CREATE POLICY "p1" ON "orders"'
        assert params == ()

    def test_create_with_schema(self, dialect):
        """Schema-qualified table reference."""
        expr = PostgresCreatePolicyExpression(
            dialect, name="p1", table="orders", schema="analytics"
        )
        sql, _ = expr.to_sql()
        assert sql == 'CREATE POLICY "p1" ON "analytics"."orders"'

    def test_create_permissive_implicit_omitted_on_95(self):
        """AS PERMISSIVE keyword must be silently omitted on PG 9.5."""
        d = PostgresDialect((9, 5, 0))
        expr = PostgresCreatePolicyExpression(
            d, name="p1", table="t1", policy_type=PolicyType.PERMISSIVE
        )
        sql, _ = expr.to_sql()
        assert "AS" not in sql
        assert sql == 'CREATE POLICY "p1" ON "t1"'

    def test_create_permissive_keyword_emitted_on_10(self):
        """AS PERMISSIVE keyword emitted on PG 10+."""
        d = PostgresDialect((10, 0, 0))
        expr = PostgresCreatePolicyExpression(
            d, name="p1", table="t1", policy_type=PolicyType.PERMISSIVE
        )
        sql, _ = expr.to_sql()
        assert "AS PERMISSIVE" in sql

    def test_create_restrictive_requires_10(self):
        """RESTRICTIVE rejects on PG 9.5."""
        d = PostgresDialect((9, 5, 0))
        expr = PostgresCreatePolicyExpression(
            d, name="p1", table="t1", policy_type=PolicyType.RESTRICTIVE
        )
        with pytest.raises(UnsupportedFeatureError) as ei:
            expr.to_sql()
        assert "RESTRICTIVE" in str(ei.value)
        assert "10" in str(ei.value)

    def test_create_restrictive_ok_10(self, dialect):
        """RESTRICTIVE allowed on PG 10+."""
        expr = PostgresCreatePolicyExpression(
            dialect, name="p1", table="t1", policy_type=PolicyType.RESTRICTIVE
        )
        sql, _ = expr.to_sql()
        assert "AS RESTRICTIVE" in sql

    @pytest.mark.parametrize("cmd,expected_kw", [
        (PolicyCommand.ALL, "FOR ALL"),
        (PolicyCommand.SELECT, "FOR SELECT"),
        (PolicyCommand.INSERT, "FOR INSERT"),
        (PolicyCommand.UPDATE, "FOR UPDATE"),
        (PolicyCommand.DELETE, "FOR DELETE"),
    ])
    def test_create_all_commands(self, dialect, cmd, expected_kw):
        expr = PostgresCreatePolicyExpression(
            dialect, name="p1", table="t1", command=cmd
        )
        sql, _ = expr.to_sql()
        assert expected_kw in sql

    def test_create_roles_identifiers_quoted(self, dialect):
        """Plain role names are identifiers — wrapped by format_identifier."""
        expr = PostgresCreatePolicyExpression(
            dialect, name="p1", table="t1", roles=["analyst", "reporter"]
        )
        sql, _ = expr.to_sql()
        assert 'TO "analyst", "reporter"' in sql

    def test_create_roles_reserved_keywords_verbatim(self, dialect):
        """PUBLIC / CURRENT_USER / SESSION_USER emitted verbatim (no quoting)."""
        expr = PostgresCreatePolicyExpression(
            dialect,
            name="p1", table="t1",
            roles=["PUBLIC", "CURRENT_USER", "SESSION_USER"],
        )
        sql, _ = expr.to_sql()
        # Reserved role keywords must NOT be double-quoted
        assert "TO PUBLIC, CURRENT_USER, SESSION_USER" in sql
        assert '"PUBLIC"' not in sql
        assert '"CURRENT_USER"' not in sql

    def test_create_roles_current_role_requires_15(self):
        """CURRENT_ROLE keyword requires PG 15; rejected on 14."""
        d = PostgresDialect((14, 0, 0))
        expr = PostgresCreatePolicyExpression(
            d, name="p1", table="t1", roles=["CURRENT_ROLE"]
        )
        with pytest.raises(UnsupportedFeatureError) as ei:
            expr.to_sql()
        assert "CURRENT_ROLE" in str(ei.value)
        assert "15" in str(ei.value)

    def test_create_using_with_param_passing(self, dialect):
        """USING expression parameters are passed through to_sql()."""
        expr = PostgresCreatePolicyExpression(
            dialect,
            name="p1", table="t1",
            command=PolicyCommand.SELECT,
            using=Column(dialect, "user_id") == Literal(dialect, 1),
        )
        sql, params = expr.to_sql()
        assert "USING (" in sql
        assert "%s" in sql
        assert params == (1,)

    def test_create_with_check(self, dialect):
        """WITH CHECK expression for UPDATE."""
        expr = PostgresCreatePolicyExpression(
            dialect,
            name="p1", table="t1",
            command=PolicyCommand.UPDATE,
            with_check=Column(dialect, "status") == Literal(dialect, "active"),
        )
        sql, params = expr.to_sql()
        assert "WITH CHECK (" in sql
        assert params == ("active",)

    def test_create_insert_with_using_rejected(self, dialect):
        """INSERT command + USING is invalid (INSERT has no USING semantics)."""
        expr = PostgresCreatePolicyExpression(
            dialect,
            name="p1", table="t1",
            command=PolicyCommand.INSERT,
            using=Column(dialect, "user_id") == Literal(dialect, 1),
        )
        with pytest.raises(ValueError, match="INSERT.*USING"):
            expr.to_sql()

    def test_create_select_with_check_rejected(self, dialect):
        """SELECT command + WITH CHECK is invalid (SELECT has no new rows)."""
        expr = PostgresCreatePolicyExpression(
            dialect,
            name="p1", table="t1",
            command=PolicyCommand.SELECT,
            with_check=Column(dialect, "user_id") == Literal(dialect, 1),
        )
        with pytest.raises(ValueError, match="SELECT.*WITH CHECK"):
            expr.to_sql()

    def test_create_delete_with_check_rejected(self, dialect):
        """DELETE command + WITH CHECK is invalid (DELETE has no new rows)."""
        expr = PostgresCreatePolicyExpression(
            dialect,
            name="p1", table="t1",
            command=PolicyCommand.DELETE,
            with_check=Column(dialect, "user_id") == Literal(dialect, 1),
        )
        with pytest.raises(ValueError, match="DELETE.*WITH CHECK"):
            expr.to_sql()

    def test_create_full_combination(self, dialect):
        """Full statement with all clauses."""
        expr = PostgresCreatePolicyExpression(
            dialect,
            name="user_select_own",
            table="orders",
            schema="public",
            policy_type=PolicyType.RESTRICTIVE,
            command=PolicyCommand.UPDATE,
            roles=["app_user", "PUBLIC"],
            using=Column(dialect, "owner") == Literal(dialect, "alice"),
            with_check=Column(dialect, "owner").is_not_null(),
        )
        sql, params = expr.to_sql()
        # All clauses present in canonical order
        assert sql.startswith('CREATE POLICY "user_select_own"')
        assert 'ON "public"."orders"' in sql
        assert "AS RESTRICTIVE" in sql
        assert "FOR UPDATE" in sql
        assert 'TO "app_user", PUBLIC' in sql
        assert "USING (" in sql
        assert "WITH CHECK (" in sql
        # USING uses the literal "alice"; WITH CHECK IS NOT NULL yields no params
        assert params == ("alice",)

    def test_create_rejected_on_94(self):
        """Whole CREATE POLICY statement rejected before PG 9.5."""
        d = PostgresDialect((9, 4, 0))
        expr = PostgresCreatePolicyExpression(d, name="p1", table="t1")
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()


# --------------------------------------------------------------------------- #
# ALTER POLICY
# --------------------------------------------------------------------------- #
class TestFormatAlterPolicyStatement:
    """Test ALTER POLICY statement formatting."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect((14, 0, 0))

    def test_alter_rename(self, dialect):
        """Form 1: RENAME TO."""
        expr = PostgresAlterPolicyExpression(
            dialect, name="p1", table="t1", new_name="p1_v2"
        )
        assert expr.mode is AlterPolicyMode.RENAME
        sql, params = expr.to_sql()
        assert sql == 'ALTER POLICY "p1" ON "t1" RENAME TO "p1_v2"'
        assert params == ()

    def test_alter_rename_with_schema(self, dialect):
        expr = PostgresAlterPolicyExpression(
            dialect, name="p1", table="t1",
            schema="my_schema", new_name="p1_v2",
        )
        sql, _ = expr.to_sql()
        assert 'ON "my_schema"."t1" RENAME TO "p1_v2"' in sql

    def test_alter_rename_rejects_replace_clauses(self, dialect):
        """new_name is mutually exclusive with roles/using/with_check."""
        expr = PostgresAlterPolicyExpression(
            dialect, name="p1", table="t1", new_name="p1_v2",
            roles=["app_user"],
        )
        with pytest.raises(ValueError, match="RENAME.*mutually exclusive"):
            expr.to_sql()

    def test_alter_replace_roles_only(self, dialect):
        """Form 2: only TO clause replaced."""
        expr = PostgresAlterPolicyExpression(
            dialect, name="p1", table="t1", roles=["app_user", "PUBLIC"]
        )
        assert expr.mode is AlterPolicyMode.REPLACE
        sql, _ = expr.to_sql()
        assert sql == 'ALTER POLICY "p1" ON "t1" TO "app_user", PUBLIC'

    def test_alter_replace_using_only(self, dialect):
        """Form 2: only USING clause replaced."""
        expr = PostgresAlterPolicyExpression(
            dialect, name="p1", table="t1",
            using=Column(dialect, "user_id") == Literal(dialect, 42),
        )
        sql, params = expr.to_sql()
        assert "USING (" in sql
        assert params == (42,)

    def test_alter_replace_with_check_only(self, dialect):
        """Form 2: only WITH CHECK clause replaced."""
        expr = PostgresAlterPolicyExpression(
            dialect, name="p1", table="t1",
            with_check=Column(dialect, "status") == Literal(dialect, "active"),
        )
        sql, params = expr.to_sql()
        assert "WITH CHECK (" in sql
        assert params == ("active",)

    def test_alter_replace_full(self, dialect):
        """Form 2: all three clauses (TO/USING/WITH CHECK) replaced."""
        expr = PostgresAlterPolicyExpression(
            dialect, name="p1", table="t1",
            roles=["app_user"],
            using=Column(dialect, "a") == Literal(dialect, 1),
            with_check=Column(dialect, "b") == Literal(dialect, "x"),
        )
        sql, params = expr.to_sql()
        assert 'TO "app_user"' in sql
        assert "USING (" in sql
        assert "WITH CHECK (" in sql
        assert params == (1, "x")

    def test_alter_replace_no_clause_rejected(self, dialect):
        """Form 2 requires at least one of TO/USING/WITH CHECK."""
        expr = PostgresAlterPolicyExpression(dialect, name="p1", table="t1")
        assert expr.mode is AlterPolicyMode.REPLACE
        with pytest.raises(ValueError, match="requires at least one"):
            expr.to_sql()

    def test_alter_rejected_on_94(self):
        """ALTER POLICY totally rejected before PG 9.5."""
        d = PostgresDialect((9, 4, 0))
        expr = PostgresAlterPolicyExpression(
            d, name="p1", table="t1", new_name="p2"
        )
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()


# --------------------------------------------------------------------------- #
# DROP POLICY
# --------------------------------------------------------------------------- #
class TestFormatDropPolicyStatement:
    """Test DROP POLICY statement formatting."""

    @pytest.fixture
    def dialect(self):
        return PostgresDialect((14, 0, 0))

    def test_drop_basic(self, dialect):
        expr = PostgresDropPolicyExpression(
            dialect, name="p1", table="t1"
        )
        sql, params = expr.to_sql()
        assert sql == 'DROP POLICY "p1" ON "t1"'
        assert params == ()

    def test_drop_with_schema(self, dialect):
        expr = PostgresDropPolicyExpression(
            dialect, name="p1", table="t1", schema="rhosocial"
        )
        sql, _ = expr.to_sql()
        assert sql == 'DROP POLICY "p1" ON "rhosocial"."t1"'

    def test_drop_if_exists(self, dialect):
        expr = PostgresDropPolicyExpression(
            dialect, name="p1", table="t1", if_exists=True
        )
        sql, _ = expr.to_sql()
        assert "DROP POLICY IF EXISTS" in sql

    def test_drop_cascade(self, dialect):
        expr = PostgresDropPolicyExpression(
            dialect, name="p1", table="t1", cascade=True
        )
        sql, _ = expr.to_sql()
        assert sql.endswith("CASCADE")

    def test_drop_restrict(self, dialect):
        expr = PostgresDropPolicyExpression(
            dialect, name="p1", table="t1", restrict=True
        )
        sql, _ = expr.to_sql()
        assert sql.endswith("RESTRICT")

    def test_drop_cascade_and_restrict_mutually_exclusive(self, dialect):
        expr = PostgresDropPolicyExpression(
            dialect, name="p1", table="t1",
            cascade=True, restrict=True,
        )
        with pytest.raises(ValueError, match="CASC.*RESTRICT"):
            expr.to_sql()

    def test_drop_rejected_on_94(self):
        d = PostgresDialect((9, 4, 0))
        expr = PostgresDropPolicyExpression(d, name="p1", table="t1")
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()


# --------------------------------------------------------------------------- #
# Mixin import sanity (defensive)
# --------------------------------------------------------------------------- #
class TestPolicyMixinImport:
    """Mixin class exists and is re-exported through the expected package path."""

    def test_mixin_importable(self):
        from rhosocial.activerecord.backend.impl.postgres.mixins import (
            PostgresPolicyMixin as M1,
        )
        from rhosocial.activerecord.backend.impl.postgres.mixins.ddl import (
            PostgresPolicyMixin as M2,
        )
        assert M1 is M2 is PostgresPolicyMixin
