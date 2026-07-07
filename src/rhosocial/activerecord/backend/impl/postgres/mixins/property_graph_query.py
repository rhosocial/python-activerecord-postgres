# src/rhosocial/activerecord/backend/impl/postgres/mixins/property_graph_query.py
"""PostgreSQL Property Graph Query (SQL/PGQ) support mixin.

PostgreSQL 19+ natively supports SQL/PGQ:
- GRAPH_TABLE (MATCH ... COLUMNS ...)
- CREATE / DROP / ALTER PROPERTY GRAPH
- Vertex/edge pattern matching with WHERE

Known PG 19 PGQ limitations (not supported):
- Variable-length (quantified) path patterns (``+``, ``*``, ``{n,m}``)
- Comma-separated multiple patterns in a single MATCH clause
- Heterogeneous graphs require explicit edge label specification
"""


class PostgresPropertyGraphQueryMixin:
    """PostgreSQL Property Graph Query support implementation.

    SQL/PGQ is a native feature since PostgreSQL 19.
    """

    def supports_graph_match(self) -> bool:
        """MATCH clause is supported since PostgreSQL 19."""
        return self.version >= (19, 0, 0)

    def supports_quantified_path(self) -> bool:
        """Quantified (variable-length) path patterns are **not** supported
        in PostgreSQL 19."""
        return False

    def supports_comma_separated_patterns(self) -> bool:
        """Comma-separated multi-pattern MATCH is **not** supported
        in PostgreSQL 19."""
        return False

    def supports_graph_table(self) -> bool:
        """GRAPH_TABLE expression is supported since PostgreSQL 19."""
        return self.version >= (19, 0, 0)
