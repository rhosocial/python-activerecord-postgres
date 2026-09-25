# src/rhosocial/activerecord/backend/impl/postgres/mixins/property_graph_query.py
"""PostgreSQL property graph feature gates and formatter safeguards.

PostgreSQL 19 Beta 4 withdrew SQL/PGQ. The formatter remains available only
through explicit feature overrides so a future implementation can opt in
without treating a server version as proof of support.
"""

from typing import Callable, Mapping, Tuple, TYPE_CHECKING, cast

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.expression.graph import (
        AlterPropertyGraphExpression,
        ColumnsClause,
        GraphEdge,
        MatchClause,
    )


class PostgresPropertyGraphQueryMixin:
    """Fail-closed PostgreSQL property graph capabilities and validation."""

    GRAPH_FEATURE_NAMES = frozenset(("graph_match", "graph_table"))
    ALTER_ACTIONS = {
        "add": "ADD",
        "drop": "DROP",
    }
    ALTER_TARGETS = {
        "edge tables": "EDGE TABLES",
        "tables": "TABLES",
        "vertex tables": "VERTEX TABLES",
    }

    def supports_graph_match(self) -> bool:
        """Return the explicit graph MATCH capability override."""
        overrides = getattr(self, "_graph_feature_overrides", {}) or {}
        return overrides.get("graph_match") is True

    def supports_quantified_path(self) -> bool:
        """Return whether quantified graph paths are explicitly enabled."""
        return False

    def supports_comma_separated_patterns(self) -> bool:
        """Return whether comma-separated graph patterns are explicitly enabled."""
        return False

    def supports_graph_table(self) -> bool:
        """Return the explicit GRAPH_TABLE capability override."""
        overrides = getattr(self, "_graph_feature_overrides", {}) or {}
        return (
            overrides.get("graph_match") is True
            and overrides.get("graph_table") is True
        )

    def _runtime_attribute(self, name: str) -> object:
        return getattr(self, name)

    def _require_graph_match(self) -> None:
        if not self.supports_graph_match():
            dialect_name = cast(str, self._runtime_attribute("name"))
            raise UnsupportedFeatureError(dialect_name, "graph MATCH clause")

    def _require_graph_table(self, feature_name: str) -> None:
        if not self.supports_graph_table():
            dialect_name = cast(str, self._runtime_attribute("name"))
            raise UnsupportedFeatureError(dialect_name, feature_name)

    def _delegate_graph_formatter(
        self, method_name: str, expression: object
    ) -> Tuple[str, tuple]:
        formatter = cast(
            Callable[[object], Tuple[str, tuple]],
            getattr(super(), method_name),
        )
        return formatter(expression)

    def format_graph_edge(self, edge: "GraphEdge") -> Tuple[str, tuple]:
        """Validate and format a graph edge."""
        self._require_graph_match()

        from rhosocial.activerecord.backend.expression.graph import GraphEdge, GraphEdgeDirection

        if not isinstance(edge, GraphEdge):
            raise TypeError("GraphEdge must be a GraphEdge expression")
        if edge.table is not None and edge.variable is None:
            raise ValueError("GraphEdge table requires a variable")
        if not isinstance(edge.direction, GraphEdgeDirection):
            raise ValueError(f"Invalid graph edge direction: {edge.direction!r}")

        return self._delegate_graph_formatter("format_graph_edge", edge)

    def format_match_clause(self, clause: "MatchClause") -> Tuple[str, tuple]:
        """Validate and format a MATCH clause."""
        self._require_graph_match()

        return self._delegate_graph_formatter("format_match_clause", clause)

    def format_graph_columns_clause(self, columns: "ColumnsClause") -> Tuple[str, tuple]:
        """Format a GRAPH_TABLE COLUMNS clause after checking capability."""
        self._require_graph_table("GRAPH_TABLE COLUMNS clause")
        return self._delegate_graph_formatter("format_graph_columns_clause", columns)

    @staticmethod
    def _normalize_alter_keyword(value: object, allowed: Mapping[str, str], label: str) -> str:
        if isinstance(value, str):
            normalized = allowed.get(value.casefold())
            if normalized is not None:
                return normalized
        raise ValueError(f"Invalid ALTER PROPERTY GRAPH {label}: {value!r}")

    def format_alter_property_graph_statement(
        self, expr: "AlterPropertyGraphExpression"
    ) -> Tuple[str, tuple]:
        """Validate and format an ALTER PROPERTY GRAPH statement."""
        self._require_graph_table("ALTER PROPERTY GRAPH")

        from rhosocial.activerecord.backend.expression.graph import AlterPropertyGraphExpression

        if not isinstance(expr, AlterPropertyGraphExpression):
            raise TypeError(
                "ALTER PROPERTY GRAPH expression must be an "
                "AlterPropertyGraphExpression"
            )
        self._normalize_alter_keyword(expr.action, self.ALTER_ACTIONS, "action")
        self._normalize_alter_keyword(expr.target, self.ALTER_TARGETS, "target")
        return self._delegate_graph_formatter(
            "format_alter_property_graph_statement", expr
        )
