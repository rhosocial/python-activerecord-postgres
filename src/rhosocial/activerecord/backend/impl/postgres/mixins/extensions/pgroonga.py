# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/pgroonga.py
"""PostgreSQL PGroonga full text search mixin.

Capability probes are version-aware via the shared extension feature map.
Index DDL is expressed with the existing typed ``PostgresCreateIndexExpression``
(``index_type="pgroonga"`` plus per-column opclasses and ``WITH`` options) --
this module builds that expression instead of adding a scalar ``format_*``
helper, so the dialect keeps its one-expression-one-formatter contract.

For SQL expression generation use the factories in ``functions/pgroonga.py``.
"""

from typing import Any, Dict, List, Optional

from rhosocial.activerecord.backend.expression.objects import Index, Table

from ...expression.statements.ddl.index_definition import PostgresCreateIndexExpression

# Groonga option values are SQL string literals built here. Validation is a
# character-level scan against allow-lists -- the backend's src tree contains
# no regular expressions.

_TOKENIZERS = frozenset(
    {
        "TokenBigram",
        "TokenBigramSplitSymbol",
        "TokenBigramSplitSymbolAlpha",
        "TokenBigramSplitSymbolAlphaDigit",
        "TokenBigramIgnoreBlank",
        "TokenBigramIgnoreBlankSplitSymbol",
        "TokenBigramIgnoreBlankSplitSymbolAlpha",
        "TokenBigramIgnoreBlankSplitSymbolAlphaDigit",
        "TokenDelimit",
        "TokenMecab",
        "TokenMecabWithReading",
        "TokenNgram",
        "TokenRegexp",
        "TokenSymbol",
        "TokenUnigram",
    }
)
_TOKEN_FILTERS = frozenset(
    {"TokenFilterStem", "TokenFilterStopWord", "TokenFilterNFKC100"}
)
_PLUGINS = frozenset(
    {"token_filters/stem", "token_filters/stop_word", "language_model/knn"}
)


_NORMALIZERS = frozenset(
    {"NormalizerAuto", "NormalizerNFKC100", "NormalizerNFKC130", "NormalizerNFKC150"}
)


def _scan_named_list(value: str, allowed: frozenset) -> None:
    """Validate a comma-separated list of names without a regular expression."""
    parts = value.split(",")
    if not parts:
        raise ValueError(f"Empty PGroonga option list: {value!r}")
    for part in parts:
        name = part.strip()
        if name not in allowed:
            raise ValueError(f"Unknown PGroonga option value: {name!r}")


def _scan_tokenizer(value: str, allowed: frozenset) -> None:
    """Validate ``Name`` or ``Name(arguments)`` with a character-level scan."""
    open_at = value.find("(")
    head = value if open_at < 0 else value[:open_at]
    if not head.isidentifier() or head not in allowed:
        raise ValueError(f"Unknown PGroonga option: {value!r}")
    if open_at < 0:
        return
    if not value.endswith(")"):
        raise ValueError(f"Unbalanced PGroonga option arguments: {value!r}")
    arguments = value[open_at + 1 : -1]
    for char in arguments:
        if not (char.isalnum() or char in ' ",._+-'):
            raise ValueError(f"Unsafe PGroonga option arguments: {value!r}")


def _quote_option(name: str, value: str) -> str:
    """Validate a Groonga option value and return an SQL string literal."""
    if "'" in value or "\\" in value or "\n" in value or "\r" in value:
        raise ValueError(f"Invalid PGroonga option value for {name}: {value!r}")
    if name == "tokenizer":
        _scan_tokenizer(value, _TOKENIZERS)
    elif name == "token_filters":
        _scan_named_list(value, _TOKEN_FILTERS)
    elif name == "plugins":
        _scan_named_list(value, _PLUGINS)
    elif name == "normalizers":
        _scan_tokenizer(value, _NORMALIZERS)
    return f"'{value}'"


class PostgresPgroongaMixin:
    """PGroonga full text search capability detection and index DDL."""

    def supports_pgroonga_match(self) -> bool:
        """Whether the ``&@`` keyword match operator is available."""
        return self.check_extension_feature("pgroonga", "match")

    def supports_pgroonga_query(self) -> bool:
        """Whether the ``&@~`` query-language operator is available."""
        return self.check_extension_feature("pgroonga", "query")

    def supports_pgroonga_match_in(self) -> bool:
        """Whether the ``&@|`` keyword-array operator is available."""
        return self.check_extension_feature("pgroonga", "match_in")

    def supports_pgroonga_prefix(self) -> bool:
        """Whether prefix search (``&^`` / ``&^|``) is available."""
        return self.check_extension_feature("pgroonga", "prefix")

    def supports_pgroonga_regexp(self) -> bool:
        """Whether regular expression search (``&~`` / ``&~|``) is available."""
        return self.check_extension_feature("pgroonga", "regexp")

    def supports_pgroonga_score(self) -> bool:
        """Whether ``pgroonga_score`` ranking is available."""
        return self.check_extension_feature("pgroonga", "score")

    def supports_pgroonga_jsonb(self) -> bool:
        """Whether the jsonb operator class is available."""
        return self.check_extension_feature("pgroonga", "jsonb")

    def supports_pgroonga_rls(self) -> bool:
        """Whether row level security support is available (2.3.3+)."""
        return self.check_extension_feature("pgroonga", "rls")

    def supports_pgroonga_equal_query(self) -> bool:
        """Whether the ``&=~`` equal-query operator is available (3.0.0+)."""
        return self.check_extension_feature("pgroonga", "equal_query")

    def supports_pgroonga_semantic_search(self) -> bool:
        """Whether semantic search is available (4.0.5+ and plugin)."""
        return self.check_extension_feature("pgroonga", "semantic_search")

    def build_pgroonga_index_expression(
        self,
        index_name: str,
        table_name: str,
        columns: List[Any],
        *,
        opclasses: Optional[Dict[str, str]] = None,
        tokenizer: Optional[str] = None,
        token_filters: Optional[str] = None,
        plugins: Optional[str] = None,
        normalizers: Optional[str] = None,
        schema: Optional[str] = None,
        unique: bool = False,
        if_not_exists: bool = False,
        concurrent: bool = False,
        include: Optional[List[str]] = None,
        tablespace: Optional[str] = None,
    ) -> PostgresCreateIndexExpression:
        """Build ``CREATE INDEX ... USING pgroonga`` as an expression node.

        Args:
            index_name: Index name (rendered as an identifier).
            table_name: Target table name (rendered as an identifier).
            columns: Column names or expression nodes.  A multi-column search
                index can be expressed with ``RawSQL(dialect, "(ARRAY[a, b])")``.
            opclasses: Per-column operator class overrides.  Defaults to the
                v2 text full text search operator class for string columns.
            tokenizer: Groonga tokenizer name, e.g. ``TokenMecab`` or
                ``TokenNgram("n", 3)``.  Defaults to Groonga's TokenBigram.
            token_filters: Comma-separated Groonga token filter names.
            plugins: Comma-separated Groonga plugin names (must come first in
                the emitted WITH clause; Groonga processes options in order).
            normalizers: Groonga normalizer name(s).
            schema: Optional schema for both the index and the table.
            unique, if_not_exists, concurrent, include, tablespace: Passed to
                the typed index expression as usual.

        Returns:
            A :class:`PostgresCreateIndexExpression` ready for ``to_sql()``.
        """
        if not columns:
            raise ValueError("A PGroonga index needs at least one column")

        resolved: Dict[str, str] = dict(opclasses or {})
        for column in columns:
            if isinstance(column, str) and column not in resolved:
                resolved[column] = "pgroonga_text_full_text_search_ops_v2"

        options: Dict[str, str] = {}
        for name, value in (
            ("plugins", plugins),
            ("tokenizer", tokenizer),
            ("normalizers", normalizers),
            ("token_filters", token_filters),
        ):
            if value is not None:
                options[name] = _quote_option(name, value)

        return PostgresCreateIndexExpression(
            self,
            index=Index(self, index_name, schema_name=schema),
            table=Table(self, table_name, schema_name=schema),
            columns=columns,
            unique=unique,
            if_not_exists=if_not_exists,
            index_type="pgroonga",
            include=include,
            tablespace=tablespace,
            concurrent=concurrent,
            opclasses=resolved,
            with_options=options,
        )
