# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_pgroonga_functions.py
"""SQL construction tests for the PGroonga factories and index DDL."""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.pgroonga import (
    pgroonga_command,
    pgroonga_equal_query,
    pgroonga_highlight_html,
    pgroonga_match,
    pgroonga_match_in,
    pgroonga_match_positions_character,
    pgroonga_prefix,
    pgroonga_query,
    pgroonga_query_escape,
    pgroonga_query_extract_keywords,
    pgroonga_query_in,
    pgroonga_regexp,
    pgroonga_score,
    pgroonga_snippet_html,
)
from rhosocial.activerecord.backend.impl.postgres.protocols.base import PostgresExtensionInfo


@pytest.fixture
def dialect():
    return PostgresDialect()


class TestPgRoongaOperators:
    """The v2 operators emit bound parameters and column references."""

    def test_match(self, dialect):
        sql, params = pgroonga_match(dialect, "content", "groonga").to_sql()
        assert sql == '"content" &@ %s'
        assert params == ("groonga",)

    def test_query(self, dialect):
        sql, params = pgroonga_query(dialect, "content", "groonga OR postgres").to_sql()
        assert sql == '"content" &@~ %s'
        assert params == ("groonga OR postgres",)

    def test_match_in_and_query_in(self, dialect):
        assert pgroonga_match_in(dialect, "content", ["a", "b"]).to_sql() == (
            '"content" &@| %s',
            (["a", "b"],),
        )
        assert pgroonga_query_in(dialect, "content", ["a", "b"]).to_sql() == (
            '"content" &@~| %s',
            (["a", "b"],),
        )

    def test_prefix_regexp_and_equal_query(self, dialect):
        assert pgroonga_prefix(dialect, "title", "pre").to_sql()[0] == '"title" &^ %s'
        assert pgroonga_regexp(dialect, "title", "^a").to_sql()[0] == '"title" &~ %s'
        assert pgroonga_equal_query(dialect, "title", "exact").to_sql()[0] == '"title" &=~ %s'

    def test_legacy_spelling_is_not_used(self, dialect):
        """v1 operators (``@@``, ``%%``) must not be emitted."""
        sql, _ = pgroonga_match(dialect, "content", "x").to_sql()
        assert "@@" not in sql and "%%" not in sql


class TestPgRoongaFunctions:
    """Function factories produce expression nodes with bound arguments."""

    def test_score_uses_system_columns(self, dialect):
        sql, params = pgroonga_score(dialect).to_sql()
        assert sql == "PGROONGA_SCORE(tableoid, ctid)"
        assert params == ()

    def test_snippet_and_highlight(self, dialect):
        snippet = pgroonga_snippet_html(dialect, "content", ["groonga"]).to_sql()
        assert snippet[0] == 'PGROONGA_SNIPPET_HTML("content", %s)'
        assert snippet[1] == (["groonga"],)
        highlight = pgroonga_highlight_html(dialect, "content", ["groonga"]).to_sql()
        assert highlight[0].startswith("PGROONGA_HIGHLIGHT_HTML(")

    def test_positions(self, dialect):
        assert pgroonga_match_positions_character(dialect, "content", ["中文"]).to_sql()[0].startswith(
            "PGROONGA_MATCH_POSITIONS_CHARACTER("
        )

    def test_escape_extract_and_command(self, dialect):
        assert pgroonga_query_escape(dialect, "a OR b").to_sql() == (
            "PGROONGA_QUERY_ESCAPE(%s)",
            ("a OR b",),
        )
        assert pgroonga_query_extract_keywords(dialect, "a OR b").to_sql()[0].startswith(
            "PGROONGA_QUERY_EXTRACT_KEYWORDS("
        )
        assert pgroonga_command(dialect, "status").to_sql() == ("PGROONGA_COMMAND(%s)", ("status",))


class TestPgRoongaIndexExpression:
    """Index DDL goes through the typed core expression, not a scalar format_*."""

    def test_default_text_index(self, dialect):
        expr = dialect.build_pgroonga_index_expression("idx_pgrn", "docs", ["content"])
        sql, params = expr.to_sql()
        assert sql == (
            'CREATE INDEX "idx_pgrn" ON "docs" USING pgroonga '
            '("content" pgroonga_text_full_text_search_ops_v2)'
        )
        assert params == ()

    def test_tokenizer_and_plugins(self, dialect):
        expr = dialect.build_pgroonga_index_expression(
            "idx_pgrn", "docs", ["content"], plugins="token_filters/stem", token_filters="TokenFilterStem"
        )
        sql, _ = expr.to_sql()
        assert "USING pgroonga" in sql
        assert "plugins = 'token_filters/stem'" in sql
        assert "token_filters = 'TokenFilterStem'" in sql
        # Groonga processes WITH options in order; plugins must come first.
        assert sql.index("plugins") < sql.index("token_filters")

    def test_jsonb_opclass_override(self, dialect):
        expr = dialect.build_pgroonga_index_expression(
            "idx_pgrn_json", "docs", ["payload"], opclasses={"payload": "pgroonga_jsonb_ops_v2"}
        )
        assert '"payload" pgroonga_jsonb_ops_v2' in expr.to_sql()[0]

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"tokenizer": "BadTokenizer"},
            {"tokenizer": "TokenMecab'; DROP TABLE docs; --"},
            {"token_filters": "TokenFilterBogus"},
            {"plugins": "evil/plugin"},
        ],
    )
    def test_untrusted_options_are_rejected(self, dialect, kwargs):
        with pytest.raises(ValueError):
            dialect.build_pgroonga_index_expression("idx", "docs", ["content"], **kwargs)

    def test_columns_are_required(self, dialect):
        with pytest.raises(ValueError):
            dialect.build_pgroonga_index_expression("idx", "docs", [])


class TestPgRoongaCapabilityGates:
    """Feature probes use the shared version-aware extension map."""

    @pytest.mark.parametrize(
        ("version", "feature", "expected"),
        [
            ("4.1.0", "match", True),
            ("3.2.4", "equal_query", True),
            ("2.0.0", "equal_query", False),
            ("3.2.4", "semantic_search", False),
            ("4.1.0", "semantic_search", True),
        ],
    )
    def test_version_gates(self, version, feature, expected):
        dialect = PostgresDialect()
        dialect._extensions = {
            "pgroonga": PostgresExtensionInfo(
                name="pgroonga", installed=True, available=True, version=version
            )
        }
        assert dialect.check_extension_feature("pgroonga", feature) is expected

    def test_not_installed_means_no_support(self):
        dialect = PostgresDialect()
        assert dialect.supports_pgroonga_match() is False
        assert dialect.supports_pgroonga_score() is False
