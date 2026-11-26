# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pgroonga.py
"""PGroonga extension protocol definition.

PGroonga is a full text search extension backed by Groonga.  Unlike PostgreSQL's
built-in tsvector search it handles CJK text with the default TokenBigram
tokenizer, and exposes its own v2 operators (``&@``, ``&@~``, ...).

For SQL expression generation use the factories in ``functions/pgroonga.py``.
"""

from typing import Any, Dict, List, Optional, Protocol, runtime_checkable


@runtime_checkable
class PostgresPgRoongaSupport(Protocol):
    """PGroonga functionality protocol.

    Feature Source: Extension support (requires the ``pgroonga`` extension)

    Extension Information:
    - Extension name: pgroonga
    - Install command: CREATE EXTENSION pgroonga;
    - Minimum version: 2.0 (v2 operators)
    - Documentation: https://pgroonga.github.io/reference/
    """

    def supports_pgroonga_match(self) -> bool:
        """Whether the ``&@`` keyword match operator is available."""
        ...

    def supports_pgroonga_query(self) -> bool:
        """Whether the ``&@~`` query-language operator is available."""
        ...

    def supports_pgroonga_match_in(self) -> bool:
        """Whether the ``&@|`` keyword-array operator is available."""
        ...

    def supports_pgroonga_prefix(self) -> bool:
        """Whether prefix search (``&^`` / ``&^|``) is available."""
        ...

    def supports_pgroonga_regexp(self) -> bool:
        """Whether regular expression search (``&~`` / ``&~|``) is available."""
        ...

    def supports_pgroonga_score(self) -> bool:
        """Whether ``pgroonga_score`` ranking is available."""
        ...

    def supports_pgroonga_jsonb(self) -> bool:
        """Whether the jsonb operator class is available."""
        ...

    def supports_pgroonga_rls(self) -> bool:
        """Whether row level security support is available (2.3.3+)."""
        ...

    def supports_pgroonga_equal_query(self) -> bool:
        """Whether the ``&=~`` equal-query operator is available (3.0.0+)."""
        ...

    def supports_pgroonga_semantic_search(self) -> bool:
        """Whether semantic search is available (4.0.5+ and plugin)."""
        ...

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
    ) -> Any:
        """Build a ``CREATE INDEX ... USING pgroonga`` expression."""
        ...
