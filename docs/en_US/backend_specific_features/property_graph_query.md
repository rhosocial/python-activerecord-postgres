# Property Graph Query

PostgreSQL 19+ supports the SQL/PGQ (Property Graph Query) standard for property graph queries.

## MATCH Clause

```python
from rhosocial.activerecord.backend.expression.graph import (
    GraphVertex, GraphEdge, GraphEdgeDirection, MatchClause,
    GraphTableExpression, GraphColumn, ColumnsClause,
)

person = GraphVertex(dialect, variable="p", table="persons")
product = GraphVertex(dialect, variable="pr", table="products")
purchased = GraphEdge(dialect, variable="pu", table="purchases",
                      direction=GraphEdgeDirection.RIGHT)

match = MatchClause(dialect, person, purchased, product)
# sql: 'MATCH (p) - [pu] -> (pr)'

graph_table = GraphTableExpression(
    dialect,
    match_clause=match,
    columns=ColumnsClause(dialect, columns=[
        GraphColumn(dialect, name="person_name", type="VARCHAR"),
        GraphColumn(dialect, name="product_name", type="VARCHAR"),
    ]),
)
```

## Property Graph DDL

```python
from rhosocial.activerecord.backend.expression.graph import (
    CreatePropertyGraphExpression, DropPropertyGraphExpression,
    VertexTable, EdgeTable,
)

create_graph = CreatePropertyGraphExpression(
    dialect,
    graph_name="social_graph",
    vertices=[VertexTable(dialect, table_name="persons", graph_label="Person")],
    edges=[EdgeTable(dialect, table_name="knows",
                     source_vertex="Person", dest_vertex="Person")],
)
```

## Dialect Feature Detection

```python
if dialect.supports_graph_match():
    # PG 19+: MATCH clause
if dialect.supports_graph_table():
    # PG 19+: GRAPH_TABLE expression
```

> **Note**: SQL/PGQ support requires PostgreSQL 19+.
