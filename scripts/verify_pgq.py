#!/usr/bin/env python3
"""
PostgreSQL 19 SQL/PGQ 功能验证脚本。

验证 Postgres 19 的 Property Graph Query 能力，
包括 DDL、GRAPH_TABLE 查询、模式匹配语法等。

用法:
    python verify_pgq.py [host] [port] [user] [password] [dbname]
"""

import sys
import psycopg


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 16690
    user = sys.argv[3] if len(sys.argv) > 3 else "root"
    password = sys.argv[4] if len(sys.argv) > 4 else "password"
    dbname = sys.argv[5] if len(sys.argv) > 5 else "test_db"

    conn = psycopg.connect(host=host, port=port, user=user,
                           password=password, dbname=dbname)
    cur = conn.cursor()
    passed = 0
    failed = 0

    def test(name, fn):
        nonlocal passed, failed
        try:
            fn(cur)
            print(f"  [PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {name}: {e}")
            conn.rollback()
            failed += 1

    # ========== 0. Setup ==========
    cur.execute("DROP TABLE IF EXISTS knows, people CASCADE")
    conn.commit()
    cur.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT)")
    cur.execute("""CREATE TABLE knows (
        id INTEGER PRIMARY KEY,
        person_a INTEGER REFERENCES people(id),
        person_b INTEGER REFERENCES people(id),
        since TEXT
    )""")
    conn.commit()

    # ========== 1. Version ==========
    def test_version(cur):
        cur.execute("SELECT version()")
        ver = cur.fetchone()[0]
        cur.execute("SHOW server_version_num")
        ver_num = cur.fetchone()[0]
        assert int(ver_num) >= 190000, f"Need PG 19+, got {ver_num}"
        assert "19" in ver, f"Unexpected version: {ver}"

    # ========== 2. CREATE PROPERTY GRAPH ==========
    def test_create_pg(cur):
        cur.execute("""
            CREATE PROPERTY GRAPH test_graph
                VERTEX TABLES (people LABEL person PROPERTIES (id, name))
                EDGE TABLES (
                    knows SOURCE KEY (person_a) REFERENCES people (id)
                          DESTINATION KEY (person_b) REFERENCES people (id)
                          LABEL knows
                )
        """)
        conn.commit()

    # ========== 3. Insert test data ==========
    def test_insert(cur):
        cur.execute("""
            INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')
            ON CONFLICT DO NOTHING
        """)
        cur.execute("""
            INSERT INTO knows VALUES (1, 1, 2, '2024-01-01'), (2, 2, 3, '2024-02-01')
            ON CONFLICT DO NOTHING
        """)
        conn.commit()

    # ========== 4. Single-hop GRAPH_TABLE ==========
    def test_single_hop(cur):
        cur.execute("""
            SELECT b_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person WHERE a.name = 'Alice')
                      -[k IS knows]->(b IS person)
                COLUMNS (b.name AS b_name)
            )
        """)
        rows = cur.fetchall()
        assert len(rows) == 1 and rows[0][0] == "Bob", f"Got {rows}"

    # ========== 5. Two-hop ==========
    def test_two_hop(cur):
        cur.execute("""
            SELECT b_name, c_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person WHERE a.name = 'Alice')
                      -[k1 IS knows]->(b IS person)
                      -[k2 IS knows]->(c IS person)
                COLUMNS (b.name AS b_name, c.name AS c_name)
            )
        """)
        rows = cur.fetchall()
        assert len(rows) == 1 and rows[0] == ("Bob", "Charlie"), f"Got {rows}"

    # ========== 6. Abbreviated edge ==========
    def test_abbrev_edge(cur):
        cur.execute("""
            SELECT b_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person WHERE a.name = 'Alice')->(b IS person)
                COLUMNS (b.name AS b_name)
            )
        """)
        rows = cur.fetchall()
        assert len(rows) == 1 and rows[0][0] == "Bob", f"Got {rows}"

    # ========== 7. Anonymous edge ==========
    def test_anon_edge(cur):
        cur.execute("""
            SELECT b_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person WHERE a.name = 'Alice')-[]->(b IS person)
                COLUMNS (b.name AS b_name)
            )
        """)
        rows = cur.fetchall()
        assert len(rows) == 1 and rows[0][0] == "Bob", f"Got {rows}"

    # ========== 8. Multi-label OR ==========
    def test_multi_label(cur):
        cur.execute("""
            SELECT b_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person|person WHERE a.name = 'Alice')
                      -[IS knows]->(b IS person)
                COLUMNS (b.name AS b_name)
            )
        """)
        rows = cur.fetchall()
        assert len(rows) == 1 and rows[0][0] == "Bob", f"Got {rows}"

    # ========== 9. Composite with ORDER BY ==========
    def test_composite(cur):
        cur.execute("""
            SELECT g.b_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person WHERE a.name = 'Alice')
                      -[k IS knows]->(b IS person)
                COLUMNS (b.name AS b_name)
            ) AS g ORDER BY g.b_name
        """)
        rows = cur.fetchall()
        assert len(rows) == 1 and rows[0][0] == "Bob", f"Got {rows}"

    # ========== 10. WHERE on graph result ==========
    def test_where_result(cur):
        cur.execute("""
            SELECT b_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person WHERE a.name = 'Alice')
                      -[k IS knows]->(b IS person)
                COLUMNS (b.name AS b_name)
            ) AS g WHERE g.b_name = 'Bob'
        """)
        rows = cur.fetchall()
        assert len(rows) == 1 and rows[0][0] == "Bob", f"Got {rows}"

    # ========== 11. Parameter binding ==========
    def test_params(cur):
        cur.execute("""
            SELECT b_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person WHERE a.name = %s)
                      -[k IS knows]->(b IS person)
                COLUMNS (b.name AS b_name)
            )
        """, ("Alice",))
        rows = cur.fetchall()
        assert len(rows) == 1 and rows[0][0] == "Bob", f"Got {rows}"

    # ========== 12. Catalog check ==========
    def test_catalog(cur):
        cur.execute("SELECT oid, relname FROM pg_class WHERE relkind = 'g'")
        rows = cur.fetchall()
        names = [r[1] for r in rows]
        assert "test_graph" in names, f"Property graph not in pg_class: {names}"

    # ========== 13. EXPLAIN shows joins ==========
    def test_explain(cur):
        cur.execute("""
            EXPLAIN (VERBOSE) SELECT b_name FROM GRAPH_TABLE (test_graph
                MATCH (a IS person WHERE a.name = %s)
                      -[k IS knows]->(b IS person)
                COLUMNS (b.name AS b_name)
            )
        """, ("Alice",))
        plan = cur.fetchall()
        plan_text = "\n".join(r[0] for r in plan)
        # Should contain relational join operations
        assert any(kw in plan_text for kw in ["Join", "Nested Loop", "Hash", "Merge"]), \
            f"EXPLAIN shows no joins:\n{plan_text}"

    # ========== 14. Variable-length path ==========
    def test_var_len_rejected(cur):
        try:
            cur.execute("""
                SELECT b_name FROM GRAPH_TABLE (test_graph
                    MATCH (a IS person WHERE a.name = 'Alice')
                          -[IS knows]->{1,2} (b IS person)
                    COLUMNS (b.name AS b_name)
                )
            """)
            assert False, "Variable-length path should fail"
        except Exception as e:
            err = str(e)
            assert "quantifier" in err or "not supported" in err, \
                f"Unexpected error for variable-length path: {err}"
            conn.rollback()

    # ========== 15. DROP ==========
    def test_drop(cur):
        cur.execute("DROP PROPERTY GRAPH test_graph")
        conn.commit()

    # ========== Run ==========
    print(f"\nPostgreSQL 19 PGQ Verification ({host}:{port}/{dbname})")
    print("=" * 50)
    for name, fn in [
        ("1.  Version check (PG 19+)", test_version),
        ("2.  CREATE PROPERTY GRAPH", test_create_pg),
        ("3.  Insert test data", test_insert),
        ("4.  Single-hop GRAPH_TABLE", test_single_hop),
        ("5.  Two-hop traversal", test_two_hop),
        ("6.  Abbreviated edge ->", test_abbrev_edge),
        ("7.  Anonymous edge []", test_anon_edge),
        ("8.  Multi-label OR |", test_multi_label),
        ("9.  Composite with ORDER BY", test_composite),
        ("10. WHERE on graph result", test_where_result),
        ("11. Parameter binding (%s)", test_params),
        ("12. pg_class relkind='g'", test_catalog),
        ("13. EXPLAIN shows joins", test_explain),
        ("14. Variable-length rejected", test_var_len_rejected),
        ("15. DROP PROPERTY GRAPH", test_drop),
    ]:
        test(name, fn)

    print("=" * 50)
    print(f"Result: {passed} passed, {failed} failed out of {passed + failed}")
    cur.close()
    conn.close()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
