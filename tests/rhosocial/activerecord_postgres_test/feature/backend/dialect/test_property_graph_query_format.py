# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_pgq_format.py
"""Tests for fail-closed PostgreSQL graph capability and formatter safeguards."""

import pytest
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.protocols import GraphSupport, GraphTableSupport
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import (
    AlterPropertyGraphExpression,
    ColumnsClause,
    CreatePropertyGraphExpression,
    DropPropertyGraphExpression,
    EdgeTable,
    GraphColumn,
    GraphEdge,
    GraphEdgeDirection,
    GraphTableExpression,
    GraphVertex,
    MatchClause,
    TablePropertiesClause,
    VertexTable,
)
from rhosocial.activerecord.backend.expression.core import Column, Literal
from rhosocial.activerecord.backend.expression.query_parts import WhereClause


class TestPGQProtocolVersionGating:
    """Capability and protocol tests for PostgreSQL graph features."""

    @pytest.mark.parametrize("version", [(15, 0, 0), (19, 0, 4), (20, 0, 0)])
    def test_graph_capabilities_fail_closed(self, version):
        dialect = PostgresDialect(version)

        assert dialect.supports_graph_match() is False
        assert dialect.supports_graph_table() is False

    @pytest.mark.requires_protocol((GraphSupport, "supports_graph_match"))
    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_graph_support_protocols_are_implemented(self):
        dialect = PostgresDialect((19, 0, 4))

        assert isinstance(dialect, GraphSupport)
        assert isinstance(dialect, GraphTableSupport)

    def test_explicit_graph_feature_override(self):
        dialect = PostgresDialect(
            (19, 0, 4),
            graph_feature_overrides={
                "graph_match": True,
                "graph_table": True,
            },
        )

        assert dialect.supports_graph_match() is True
        assert dialect.supports_graph_table() is True

    def test_match_only_override_does_not_enable_graph_table(self):
        dialect = PostgresDialect(
            (19, 0, 4),
            graph_feature_overrides={"graph_match": True},
        )

        assert dialect.supports_graph_match() is True
        assert dialect.supports_graph_table() is False

    def test_graph_table_override_without_match_fails_closed(self):
        dialect = PostgresDialect(
            (19, 0, 4),
            graph_feature_overrides={"graph_table": True},
        )
        vertex = GraphVertex(dialect, "p", "person")
        match = MatchClause(dialect, vertex)
        columns = ColumnsClause(dialect, GraphColumn("p", "name"))

        assert dialect.supports_graph_match() is False
        assert dialect.supports_graph_table() is False
        with pytest.raises(UnsupportedFeatureError, match="GRAPH_TABLE"):
            GraphTableExpression(dialect, "g", match, columns).to_sql()

    def test_unknown_graph_feature_override_is_rejected(self):
        with pytest.raises(ValueError, match="Unknown graph feature override"):
            PostgresDialect((19, 0, 4), graph_feature_overrides={"graph_sql": True})

    def test_non_boolean_graph_feature_override_is_rejected(self):
        with pytest.raises(TypeError, match="override values must be booleans"):
            PostgresDialect((19, 0, 4), graph_feature_overrides={"graph_match": 1})


class TestPGQLimitationGating:
    """Tests that withdrawn and unimplemented graph features stay disabled."""

    def test_quantified_path_unsupported_by_default(self):
        dialect = PostgresDialect((19, 0, 4))
        assert dialect.supports_quantified_path() is False

    def test_comma_separated_patterns_unsupported_by_default(self):
        dialect = PostgresDialect((19, 0, 4))
        assert dialect.supports_comma_separated_patterns() is False

    def test_quantified_path_raises_on_format(self, pg19_dialect: PostgresDialect):
        from rhosocial.activerecord.backend.expression import QuantifiedPath

        edge = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        quantified_path = QuantifiedPath(pg19_dialect, edge)
        with pytest.raises(UnsupportedFeatureError):
            quantified_path.to_sql()


@pytest.fixture
def pg19_dialect():
    return PostgresDialect(
        (19, 0, 4),
        graph_feature_overrides={
            "graph_match": True,
            "graph_table": True,
        },
    )


class TestPGQGraphVertexFormat:
    """SQL formatting tests for GraphVertex with PG19 dialect."""

    def test_basic(self, pg19_dialect: PostgresDialect):
        v = GraphVertex(pg19_dialect, "p", "person")
        sql, params = v.to_sql()
        assert sql == '(p IS "person")'
        assert params == ()

    def test_anonymous(self, pg19_dialect: PostgresDialect):
        v = GraphVertex(pg19_dialect, variable=None, table="person")
        sql, params = v.to_sql()
        assert sql == '("person")'
        assert params == ()

    def test_with_where(self, pg19_dialect: PostgresDialect):
        where = WhereClause(
            pg19_dialect,
            condition=Column(pg19_dialect, "age") > Literal(pg19_dialect, 18),
        )
        vertex = GraphVertex(pg19_dialect, "p", "person", where=where)
        sql, params = vertex.to_sql()
        assert "(p IS" in sql
        assert "WHERE" in sql
        assert params == (18,)

    def test_invalid_variable_name(self, pg19_dialect: PostgresDialect):
        vertex = GraphVertex(pg19_dialect, "bad var", "person")
        with pytest.raises(ValueError, match="Invalid variable name"):
            vertex.to_sql()


class TestPGQGraphEdgeFormat:
    """SQL formatting tests for GraphEdge with explicit capability overrides."""

    def test_right(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        sql, params = edge.to_sql()
        assert sql == '-[e IS "knows"]->'

    def test_left(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.LEFT)
        assert edge.to_sql()[0] == '<-[e IS "knows"]-'

    def test_any(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.ANY)
        assert edge.to_sql()[0] == '<-[e IS "knows"]->'

    def test_anonymous(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(pg19_dialect, direction=GraphEdgeDirection.RIGHT)
        assert edge.to_sql()[0] == '-[]->'

    def test_none_direction(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.NONE)
        assert edge.to_sql()[0] == '-[e IS "knows"]-'

    def test_table_without_variable_is_rejected(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(pg19_dialect, table="knows", direction=GraphEdgeDirection.RIGHT)
        with pytest.raises(ValueError, match="table requires a variable"):
            edge.to_sql()

    def test_invalid_direction_is_rejected(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(pg19_dialect, "e", "knows", direction="right")
        with pytest.raises(ValueError, match="Invalid graph edge direction"):
            edge.to_sql()

    def test_variable_only(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(pg19_dialect, variable="e", direction=GraphEdgeDirection.RIGHT)
        assert edge.to_sql()[0] == '-[e]->'

    def test_invalid_variable_name(self, pg19_dialect: PostgresDialect):
        edge = GraphEdge(
            pg19_dialect,
            variable="bad var",
            table="knows",
            direction=GraphEdgeDirection.RIGHT,
        )
        with pytest.raises(ValueError, match="Invalid variable name"):
            edge.to_sql()


class TestPGQMatchClauseFormat:
    """SQL formatting tests for MatchClause with explicit capability overrides."""

    def test_single_vertex(self, pg19_dialect: PostgresDialect):
        vertex = GraphVertex(pg19_dialect, "p", "person")
        match = MatchClause(pg19_dialect, vertex)
        sql, params = match.to_sql()
        assert "MATCH" in sql
        assert "(p IS" in sql

    def test_path_pattern(self, pg19_dialect: PostgresDialect):
        first = GraphVertex(pg19_dialect, "a", "person")
        edge = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        second = GraphVertex(pg19_dialect, "b", "person")
        match = MatchClause(pg19_dialect, first, edge, second)
        sql, params = match.to_sql()
        assert "MATCH" in sql
        assert "(a IS" in sql
        assert "[e IS" in sql
        assert "(b IS" in sql

    def test_empty_path_is_rejected(self, pg19_dialect: PostgresDialect):
        match = MatchClause(pg19_dialect)
        with pytest.raises(ValueError, match="at least one path element"):
            match.to_sql()

    def test_anonymous_vertex(self, pg19_dialect: PostgresDialect):
        vertex = GraphVertex(pg19_dialect, variable=None, table="person")
        match = MatchClause(pg19_dialect, vertex)
        sql, params = match.to_sql()
        assert sql == 'MATCH ("person")'
        assert params == ()


class TestPGQGraphTableFormat:
    """SQL formatting tests for GraphTableExpression with PG19 dialect."""

    def test_basic(self, pg19_dialect: PostgresDialect):
        v = GraphVertex(pg19_dialect, "p", "person")
        cols = ColumnsClause(pg19_dialect, GraphColumn("p", "name"))
        m = MatchClause(pg19_dialect, v)
        gt = GraphTableExpression(pg19_dialect, "g", m, cols)
        sql, params = gt.to_sql()
        assert 'GRAPH_TABLE ("g" MATCH' in sql
        assert "COLUMNS" in sql

    def test_with_where(self, pg19_dialect: PostgresDialect):
        where = WhereClause(pg19_dialect,
                            condition=Column(pg19_dialect, "age") > Literal(pg19_dialect, 18))
        v = GraphVertex(pg19_dialect, "p", "person", where=where)
        e = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(pg19_dialect, "b", "person")
        cols = ColumnsClause(pg19_dialect, GraphColumn("b", "name"))
        m = MatchClause(pg19_dialect, v, e, b)
        gt = GraphTableExpression(pg19_dialect, "g", m, cols)
        sql, params = gt.to_sql()
        assert "WHERE" in sql
        assert params == (18,)

    def test_with_alias(self, pg19_dialect: PostgresDialect):
        v = GraphVertex(pg19_dialect, "p", "person")
        cols = ColumnsClause(pg19_dialect, GraphColumn("p", "name"))
        m = MatchClause(pg19_dialect, v)
        gt = GraphTableExpression(pg19_dialect, "g", m, cols, alias="t")
        sql, params = gt.to_sql()
        assert 'GRAPH_TABLE ("g" MATCH' in sql
        assert 'AS "t"' in sql

    def test_graph_column_without_alias(self, pg19_dialect: PostgresDialect):
        cols = ColumnsClause(pg19_dialect, GraphColumn("p", "name"))
        sql, params = cols.to_sql()
        assert sql == 'COLUMNS ("p"."name")'
        assert params == ()


class TestPGQDDLFormat:
    """SQL formatting tests for PGQ DDL expressions with PG19 dialect."""

    def test_create_property_graph(self, pg19_dialect: PostgresDialect):
        vt = VertexTable(pg19_dialect, "people",
                         labels=["person"],
                         key_columns=["id"],
                         properties=TablePropertiesClause(pg19_dialect, columns=["id", "name"]))
        et = EdgeTable(pg19_dialect, "knows", ["person_a"], ["person_b"],
                       references_source=("people", ["id"]),
                       references_destination=("people", ["id"]),
                       labels=["knows"],
                       properties=TablePropertiesClause(pg19_dialect, columns=["since"]))
        expr = CreatePropertyGraphExpression(pg19_dialect, "test_graph", [vt], [et])
        sql, params = expr.to_sql()
        assert "CREATE PROPERTY GRAPH" in sql
        assert '"people"' in sql
        assert '"knows"' in sql
        assert "SOURCE KEY" in sql
        assert "DESTINATION KEY" in sql

    def test_create_property_graph_if_not_exists(self, pg19_dialect: PostgresDialect):
        vt = VertexTable(pg19_dialect, "person")
        expr = CreatePropertyGraphExpression(pg19_dialect, "my_graph", [vt],
                                              if_not_exists=True)
        sql, params = expr.to_sql()
        assert "CREATE PROPERTY GRAPH IF NOT EXISTS" in sql

    def test_create_property_graph_no_edges(self, pg19_dialect: PostgresDialect):
        vt = VertexTable(pg19_dialect, "person")
        expr = CreatePropertyGraphExpression(pg19_dialect, "my_graph", [vt])
        sql, params = expr.to_sql()
        assert "EDGE TABLES" not in sql
        assert 'VERTEX TABLES ("person"' in sql

    def test_vertex_table_with_alias(self, pg19_dialect: PostgresDialect):
        vt = VertexTable(pg19_dialect, "person", alias="p",
                         labels=["Person"],
                         key_columns=["id"])
        sql, params = vt.to_sql()
        assert '"person" AS "p"' in sql or '"person" AS p' in sql
        assert "LABEL" in sql
        assert "KEY" in sql

    def test_edge_table_with_alias(self, pg19_dialect: PostgresDialect):
        et = EdgeTable(pg19_dialect, "knows", ["pid"], ["fid"],
                       alias="k",
                       labels=["Knows"])
        sql, params = et.to_sql()
        assert '"knows" AS "k"' in sql or '"knows" AS k' in sql
        assert "SOURCE KEY" in sql
        assert "DESTINATION KEY" in sql
        assert "LABEL" in sql

    def test_properties_all_columns(self, pg19_dialect: PostgresDialect):
        p = TablePropertiesClause(pg19_dialect, columns=None)
        sql, params = p.to_sql()
        assert sql == "PROPERTIES ALL COLUMNS"
        assert params == ()

    def test_properties_none(self, pg19_dialect: PostgresDialect):
        p = TablePropertiesClause(pg19_dialect, columns=[])
        sql, params = p.to_sql()
        assert sql == "PROPERTIES NONE"
        assert params == ()

    def test_drop_property_graph(self, pg19_dialect: PostgresDialect):
        expr = DropPropertyGraphExpression(pg19_dialect, "test_graph", if_exists=True)
        sql, params = expr.to_sql()
        assert 'DROP PROPERTY GRAPH IF EXISTS "test_graph"' in sql

    def test_drop_cascade(self, pg19_dialect: PostgresDialect):
        expr = DropPropertyGraphExpression(pg19_dialect, "test_graph", cascade=True)
        sql, params = expr.to_sql()
        assert "CASCADE" in sql

    def test_edge_table_no_references(self, pg19_dialect: PostgresDialect):
        et = EdgeTable(pg19_dialect, "knows", ["pid"], ["fid"])
        sql, params = et.to_sql()
        assert 'SOURCE KEY ("pid")' in sql
        assert 'DESTINATION KEY ("fid")' in sql
        assert "REFERENCES" not in sql

    def test_edge_table_with_key_columns(self, pg19_dialect: PostgresDialect):
        et = EdgeTable(pg19_dialect, "knows", ["pid"], ["fid"],
                       key_columns=["id"],
                       references_source=("people", ["id"]),
                       references_destination=("people", ["id"]))
        sql, params = et.to_sql()
        assert 'KEY ("id")' in sql

    def test_alter_add_vertex(self, pg19_dialect: PostgresDialect):
        vt = VertexTable(pg19_dialect, "new_table", labels=["NewLabel"])
        expr = AlterPropertyGraphExpression(pg19_dialect, "g", "ADD", "VERTEX TABLES",
                                            vertex_tables=[vt])
        sql, params = expr.to_sql()
        assert "ALTER PROPERTY GRAPH" in sql
        assert "ADD" in sql

    def test_alter_drop_vertex_tables(self, pg19_dialect: PostgresDialect):
        vt = VertexTable(pg19_dialect, "old_table")
        expr = AlterPropertyGraphExpression(pg19_dialect, "g", "DROP", "VERTEX TABLES",
                                            vertex_tables=[vt])
        sql, params = expr.to_sql()
        assert "DROP" in sql
        assert "VERTEX TABLES" in sql
        assert '"old_table"' in sql

    def test_alter_with_edge_tables(self, pg19_dialect: PostgresDialect):
        et = EdgeTable(pg19_dialect, "knows", ["pid"], ["fid"])
        expr = AlterPropertyGraphExpression(pg19_dialect, "g", "DROP", "EDGE TABLES",
                                            edge_tables=[et])
        sql, params = expr.to_sql()
        assert "DROP" in sql
        assert '"knows"' in sql

    def test_alter_with_both_tables(self, pg19_dialect: PostgresDialect):
        vt = VertexTable(pg19_dialect, "person")
        et = EdgeTable(pg19_dialect, "knows", ["pid"], ["fid"])
        expr = AlterPropertyGraphExpression(pg19_dialect, "g", "ADD", "TABLES",
                                            vertex_tables=[vt], edge_tables=[et])
        sql, params = expr.to_sql()
        assert '"person"' in sql
        assert '"knows"' in sql

    @pytest.mark.parametrize(
        ("action", "target", "message"),
        [
            ("ADD; DROP TABLE people", "VERTEX TABLES", "action"),
            ("ADD", "VERTEX TABLES; DROP TABLE people", "target"),
            ("ALTER", "TABLES", "action"),
            ("ADD", "ALL TABLES", "target"),
        ],
    )
    def test_alter_rejects_non_whitelisted_keywords(
        self, pg19_dialect, action, target, message
    ):
        expr = AlterPropertyGraphExpression(pg19_dialect, "g", action, target)

        with pytest.raises(ValueError, match=f"Invalid ALTER PROPERTY GRAPH {message}"):
            expr.to_sql()


class TestPGQUnsupportedFormat:
    """Tests that graph formatters fail closed without explicit opt-in."""

    @pytest.fixture
    def withdrawn_dialect(self):
        return PostgresDialect((19, 0, 4))

    def test_graph_vertex_unsupported(self, withdrawn_dialect: PostgresDialect):
        vertex = GraphVertex(withdrawn_dialect, "p", "person")
        with pytest.raises(UnsupportedFeatureError):
            vertex.to_sql()

    def test_graph_edge_unsupported(self, withdrawn_dialect: PostgresDialect):
        edge = GraphEdge(withdrawn_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        with pytest.raises(UnsupportedFeatureError):
            edge.to_sql()

    def test_match_clause_unsupported(self, withdrawn_dialect: PostgresDialect):
        vertex = GraphVertex(withdrawn_dialect, "p", "person")
        match = MatchClause(withdrawn_dialect, vertex)
        with pytest.raises(UnsupportedFeatureError):
            match.to_sql()

    def test_graph_columns_clause_unsupported(self, withdrawn_dialect: PostgresDialect):
        columns = ColumnsClause(withdrawn_dialect, GraphColumn("p", "name"))
        with pytest.raises(UnsupportedFeatureError):
            columns.to_sql()

    def test_graph_table_unsupported(self, withdrawn_dialect: PostgresDialect):
        vertex = GraphVertex(withdrawn_dialect, "p", "person")
        columns = ColumnsClause(withdrawn_dialect, GraphColumn("p", "name"))
        match = MatchClause(withdrawn_dialect, vertex)
        graph_table = GraphTableExpression(withdrawn_dialect, "g", match, columns)
        with pytest.raises(UnsupportedFeatureError):
            graph_table.to_sql()

    def test_vertex_table_unsupported(self, withdrawn_dialect: PostgresDialect):
        vertex_table = VertexTable(withdrawn_dialect, "person")
        with pytest.raises(UnsupportedFeatureError):
            vertex_table.to_sql()

    def test_edge_table_unsupported(self, withdrawn_dialect: PostgresDialect):
        edge_table = EdgeTable(withdrawn_dialect, "knows", ["pid"], ["fid"])
        with pytest.raises(UnsupportedFeatureError):
            edge_table.to_sql()

    def test_create_property_graph_unsupported(self, withdrawn_dialect: PostgresDialect):
        vertex_table = VertexTable(withdrawn_dialect, "person")
        expression = CreatePropertyGraphExpression(withdrawn_dialect, "g", [vertex_table])
        with pytest.raises(UnsupportedFeatureError):
            expression.to_sql()

    def test_drop_property_graph_unsupported(self, withdrawn_dialect: PostgresDialect):
        expression = DropPropertyGraphExpression(withdrawn_dialect, "g")
        with pytest.raises(UnsupportedFeatureError):
            expression.to_sql()

    def test_alter_property_graph_unsupported(self, withdrawn_dialect: PostgresDialect):
        expression = AlterPropertyGraphExpression(
            withdrawn_dialect,
            "g",
            "ADD",
            "VERTEX TABLES",
        )
        with pytest.raises(UnsupportedFeatureError):
            expression.to_sql()

    def test_table_properties_clause_unsupported(self, withdrawn_dialect: PostgresDialect):
        properties = TablePropertiesClause(withdrawn_dialect, columns=["id"])
        with pytest.raises(UnsupportedFeatureError):
            properties.to_sql()

    def test_table_properties_all_columns_unsupported(self, withdrawn_dialect: PostgresDialect):
        properties = TablePropertiesClause(withdrawn_dialect, columns=None)
        with pytest.raises(UnsupportedFeatureError):
            properties.to_sql()

    def test_table_properties_none_unsupported(self, withdrawn_dialect: PostgresDialect):
        properties = TablePropertiesClause(withdrawn_dialect, columns=[])
        with pytest.raises(UnsupportedFeatureError):
            properties.to_sql()
