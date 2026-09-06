# 属性图查询 (Property Graph Query)

PostgreSQL 19+ 支持 SQL/PGQ（Property Graph Query）属性图查询标准。

## MATCH 子句

```python
from rhosocial.activerecord.backend.expression.graph import (
    GraphVertex, GraphEdge, GraphEdgeDirection, MatchClause, GraphTableExpression,
    GraphColumn, ColumnsClause,
)

# 定义顶点
person = GraphVertex(dialect, variable="p", table="persons")
product = GraphVertex(dialect, variable="pr", table="products")

# 定义边
purchased = GraphEdge(dialect, variable="pu", table="purchases",
                      direction=GraphEdgeDirection.RIGHT)

# MATCH 子句
match = MatchClause(dialect, person, purchased, product)
# sql: 'MATCH (p) - [pu] -> (pr)'

# GRAPH_TABLE 表达式
graph_table = GraphTableExpression(
    dialect,
    match_clause=match,
    columns=ColumnsClause(dialect, columns=[
        GraphColumn(dialect, name="person_name", type="VARCHAR"),
        GraphColumn(dialect, name="product_name", type="VARCHAR"),
    ]),
)
```

## 属性图 DDL

```python
from rhosocial.activerecord.backend.expression.graph import (
    CreatePropertyGraphExpression, DropPropertyGraphExpression,
    VertexTable, EdgeTable, TablePropertiesClause,
)

# 创建属性图
create_graph = CreatePropertyGraphExpression(
    dialect,
    graph_name="social_graph",
    vertices=[
        VertexTable(dialect, table_name="persons", graph_label="Person"),
    ],
    edges=[
        EdgeTable(dialect, table_name="knows",
                  source_vertex="Person", dest_vertex="Person"),
    ],
)
# sql: 'CREATE PROPERTY GRAPH "social_graph" ...'

# 删除属性图
drop_graph = DropPropertyGraphExpression(
    dialect, graph_name="social_graph", if_exists=True,
)
```

## 方言检查

```python
if dialect.supports_graph_match():
    # PG 19+: 支持 MATCH 子句

if dialect.supports_graph_table():
    # PG 19+: 支持 GRAPH_TABLE 表达式
```

> **注意**：SQL/PGQ 支持需要 PostgreSQL 19+。
