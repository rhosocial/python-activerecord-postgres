# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_pgq_format.py
"""
Tests for PostgreSQL PGQ dialect version gating and SQL formatting.

Uses requires_protocol markers for protocol-level capability documentation.
"""
import pytest
from rhosocial.activerecord.backend.dialect.protocols import GraphSupport, GraphTableSupport
from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.expression import (
    GraphVertex, GraphEdge, GraphEdgeDirection, MatchClause,
    GraphColumn, ColumnsClause, GraphTableExpression,
    TablePropertiesClause, VertexTable, EdgeTable,
    CreatePropertyGraphExpression, DropPropertyGraphExpression,
    AlterPropertyGraphExpression,
)
from rhosocial.activerecord.backend.expression.query_parts import WhereClause
from rhosocial.activerecord.backend.expression.core import Column, Literal


class TestPGQProtocolVersionGating:
    """Version gating tests for PGQ support in PostgreSQL."""

    def test_supports_graph_match_pg19_true(self):
        d = PostgresDialect((19, 0, 0))
        assert d.supports_graph_match() is True

    def test_supports_graph_match_pg15_false(self):
        d = PostgresDialect((15, 0, 0))
        assert d.supports_graph_match() is False

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_supports_graph_table_pg19_true(self):
        d = PostgresDialect((19, 0, 0))
        assert d.supports_graph_table() is True

    @pytest.mark.requires_protocol((GraphTableSupport, "supports_graph_table"))
    def test_supports_graph_table_pg15_false(self):
        d = PostgresDialect((15, 0, 0))
        assert d.supports_graph_table() is False

    @pytest.mark.requires_protocol((GraphSupport, "supports_graph_match"))
    def test_graph_support_protocol_implemented_pg19(self):
        d = PostgresDialect((19, 0, 0))
        assert isinstance(d, GraphSupport)
        assert isinstance(d, GraphTableSupport)

    def test_graph_support_protocol_implemented_pg15(self):
        d = PostgresDialect((15, 0, 0))
        assert isinstance(d, GraphSupport)
        assert isinstance(d, GraphTableSupport)


@pytest.fixture
def pg19_dialect():
    return PostgresDialect((19, 0, 0))


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
        where = WhereClause(pg19_dialect,
                            condition=Column(pg19_dialect, "age") > Literal(pg19_dialect, 18))
        v = GraphVertex(pg19_dialect, "p", "person", where=where)
        sql, params = v.to_sql()
        assert "(p IS" in sql
        assert "WHERE" in sql
        assert params == (18,)

    def test_invalid_variable_name(self, pg19_dialect: PostgresDialect):
        v = GraphVertex(pg19_dialect, "bad var", "person")
        with pytest.raises(ValueError, match="Invalid variable name"):
            v.to_sql()




class TestPGQGraphEdgeFormat:
    """SQL formatting tests for GraphEdge with PG19 dialect."""

    def test_right(self, pg19_dialect: PostgresDialect):
        e = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        sql, params = e.to_sql()
        assert sql == '-[e IS "knows"]->'

    def test_left(self, pg19_dialect: PostgresDialect):
        e = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.LEFT)
        assert e.to_sql()[0] == '<-[e IS "knows"]-'

    def test_any(self, pg19_dialect: PostgresDialect):
        e = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.ANY)
        assert e.to_sql()[0] == '<-[e IS "knows"]->'

    def test_anonymous(self, pg19_dialect: PostgresDialect):
        e = GraphEdge(pg19_dialect, direction=GraphEdgeDirection.RIGHT)
        assert e.to_sql()[0] == '-[]->'

    def test_none_direction(self, pg19_dialect: PostgresDialect):
        e = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.NONE)
        assert e.to_sql()[0] == '-[e IS "knows"]-'

    def test_table_without_variable(self, pg19_dialect: PostgresDialect):
        e = GraphEdge(pg19_dialect, table="knows", direction=GraphEdgeDirection.RIGHT)
        sql = e.to_sql()[0]
        assert sql == '-[]->'

    def test_variable_only(self, pg19_dialect: PostgresDialect):
        e = GraphEdge(pg19_dialect, variable="e", direction=GraphEdgeDirection.RIGHT)
        assert e.to_sql()[0] == '-[e]->'

    def test_invalid_variable_name(self, pg19_dialect: PostgresDialect):
        e = GraphEdge(pg19_dialect, variable="bad var", table="knows", direction=GraphEdgeDirection.RIGHT)
        with pytest.raises(ValueError, match="Invalid variable name"):
            e.to_sql()


class TestPGQMatchClauseFormat:
    """SQL formatting tests for MatchClause with PG19 dialect."""

    def test_single_vertex(self, pg19_dialect: PostgresDialect):
        v = GraphVertex(pg19_dialect, "p", "person")
        m = MatchClause(pg19_dialect, v)
        sql, params = m.to_sql()
        assert "MATCH" in sql
        assert "(p IS" in sql

    def test_path_pattern(self, pg19_dialect: PostgresDialect):
        a = GraphVertex(pg19_dialect, "a", "person")
        e = GraphEdge(pg19_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        b = GraphVertex(pg19_dialect, "b", "person")
        m = MatchClause(pg19_dialect, a, e, b)
        sql, params = m.to_sql()
        assert "MATCH" in sql
        assert "(a IS" in sql
        assert "[e IS" in sql
        assert "(b IS" in sql

    def test_empty_path(self, pg19_dialect: PostgresDialect):
        m = MatchClause(pg19_dialect)
        sql, params = m.to_sql()
        assert sql == "MATCH "
        assert params == ()

    def test_anonymous_vertex(self, pg19_dialect: PostgresDialect):
        v = GraphVertex(pg19_dialect, variable=None, table="person")
        m = MatchClause(pg19_dialect, v)
        sql, params = m.to_sql()
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


class TestPGQUnsupportedFormat:
    """Tests that PG15 raises errors for PGQ formatting."""

    @pytest.fixture
    def pg15_dialect(self):
        return PostgresDialect((15, 0, 0))

    def test_graph_vertex_unsupported(self, pg15_dialect: PostgresDialect):
        v = GraphVertex(pg15_dialect, "p", "person")
        with pytest.raises(Exception):
            v.to_sql()

    def test_graph_edge_unsupported(self, pg15_dialect: PostgresDialect):
        e = GraphEdge(pg15_dialect, "e", "knows", GraphEdgeDirection.RIGHT)
        with pytest.raises(Exception):
            e.to_sql()

    def test_graph_table_unsupported(self, pg15_dialect: PostgresDialect):
        v = GraphVertex(pg15_dialect, "p", "person")
        cols = ColumnsClause(pg15_dialect, GraphColumn("p", "name"))
        m = MatchClause(pg15_dialect, v)
        gt = GraphTableExpression(pg15_dialect, "g", m, cols)
        with pytest.raises(Exception):
            gt.to_sql()

    def test_vertex_table_unsupported(self, pg15_dialect: PostgresDialect):
        vt = VertexTable(pg15_dialect, "person")
        with pytest.raises(UnsupportedFeatureError):
            vt.to_sql()

    def test_edge_table_unsupported(self, pg15_dialect: PostgresDialect):
        et = EdgeTable(pg15_dialect, "knows", ["pid"], ["fid"])
        with pytest.raises(UnsupportedFeatureError):
            et.to_sql()

    def test_create_property_graph_unsupported(self, pg15_dialect: PostgresDialect):
        vt = VertexTable(pg15_dialect, "person")
        expr = CreatePropertyGraphExpression(pg15_dialect, "g", [vt])
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()

    def test_drop_property_graph_unsupported(self, pg15_dialect: PostgresDialect):
        expr = DropPropertyGraphExpression(pg15_dialect, "g")
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()

    def test_alter_property_graph_unsupported(self, pg15_dialect: PostgresDialect):
        vt = VertexTable(pg15_dialect, "person")
        expr = AlterPropertyGraphExpression(pg15_dialect, "g", "ADD", "VERTEX TABLES",
                                            vertex_tables=[vt])
        with pytest.raises(UnsupportedFeatureError):
            expr.to_sql()

    def test_table_properties_clause_unsupported(self, pg15_dialect: PostgresDialect):
        p = TablePropertiesClause(pg15_dialect, columns=["id"])
        with pytest.raises(UnsupportedFeatureError):
            p.to_sql()

    def test_table_properties_all_columns_unsupported(self, pg15_dialect: PostgresDialect):
        p = TablePropertiesClause(pg15_dialect, columns=None)
        with pytest.raises(UnsupportedFeatureError):
            p.to_sql()

    def test_table_properties_none_unsupported(self, pg15_dialect: PostgresDialect):
        p = TablePropertiesClause(pg15_dialect, columns=[])
        with pytest.raises(UnsupportedFeatureError):
            p.to_sql()
