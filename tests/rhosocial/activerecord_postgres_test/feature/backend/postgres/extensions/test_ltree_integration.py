# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_ltree_integration.py
"""
Integration tests for PostgreSQL ltree extension with real database.

These tests require a live PostgreSQL connection with ltree extension installed
and test:
- ltree path operations (@>, <@, ~)
- Ancestor/descendant queries
- nlevel and subpath functions
- GiST index on ltree columns
"""
import pytest


class TestLtreeIntegration:
    """Integration tests for ltree extension."""

    def test_ltree_path_operations(self, postgres_backend_single):
        """Test ltree path operators @>, <@, ~ with table data."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("ltree"):
            pytest.skip("Extension 'ltree' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_ltree")
            backend.execute("""
                CREATE TABLE test_ltree (
                    id SERIAL PRIMARY KEY,
                    path LTREE
                )
            """)
            backend.execute("""
                INSERT INTO test_ltree (path) VALUES
                    ('Top'),
                    ('Top.Science'),
                    ('Top.Science.Astronomy'),
                    ('Top.Science.Physics'),
                    ('Top.History')
            """)

            # Test @> (ancestor) operator: does 'Top.Science' contain 'Top.Science.Astronomy'?
            result = backend.fetch_one("""
                SELECT 'Top.Science'::ltree @> path AS is_ancestor
                FROM test_ltree WHERE path = 'Top.Science.Astronomy'
            """)
            assert result['is_ancestor'] is True

            # 'Top.History' does not contain 'Top.Science.Astronomy'
            result = backend.fetch_one("""
                SELECT 'Top.History'::ltree @> path AS is_ancestor
                FROM test_ltree WHERE path = 'Top.Science.Astronomy'
            """)
            assert result['is_ancestor'] is False

            # Test <@ (descendant) operator
            result = backend.fetch_one("""
                SELECT path <@ 'Top.Science'::ltree AS is_descendant
                FROM test_ltree WHERE path = 'Top.Science.Astronomy'
            """)
            assert result['is_descendant'] is True

            # Test ~ (lquery match) operator: find paths matching pattern
            result = backend.fetch_all("""
                SELECT path FROM test_ltree
                WHERE path ~ 'Top.Science.*'::lquery
                ORDER BY path
            """)
            assert len(result) >= 2
            paths = [r['path'] for r in result]
            assert 'Top.Science' in paths
            assert 'Top.Science.Astronomy' in paths
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_ltree")
            except Exception:
                pass

    def test_ltree_ancestor_descendant(self, postgres_backend_single):
        """Test finding all ancestors and descendants of a path."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("ltree"):
            pytest.skip("Extension 'ltree' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_ltree_tree")
            backend.execute("""
                CREATE TABLE test_ltree_tree (
                    id SERIAL PRIMARY KEY,
                    path LTREE,
                    label TEXT
                )
            """)
            backend.execute("""
                INSERT INTO test_ltree_tree (path, label) VALUES
                    ('root', 'Root'),
                    ('root.level1', 'Level 1'),
                    ('root.level1.level2a', 'Level 2a'),
                    ('root.level1.level2b', 'Level 2b'),
                    ('root.other', 'Other branch')
            """)

            # Find all ancestors of 'root.level1.level2a'
            result = backend.fetch_all("""
                SELECT path, label FROM test_ltree_tree
                WHERE path @> 'root.level1.level2a'::ltree
                ORDER BY nlevel(path)
            """)
            assert len(result) == 3  # root, root.level1, root.level1.level2a
            labels = [r['label'] for r in result]
            assert 'Root' in labels
            assert 'Level 1' in labels
            assert 'Level 2a' in labels

            # Find all descendants of 'root.level1'
            result = backend.fetch_all("""
                SELECT path, label FROM test_ltree_tree
                WHERE path <@ 'root.level1'::ltree
                ORDER BY path
            """)
            assert len(result) == 3  # root.level1, root.level1.level2a, root.level1.level2b
            labels = [r['label'] for r in result]
            assert 'Level 1' in labels
            assert 'Level 2a' in labels
            assert 'Level 2b' in labels
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_ltree_tree")
            except Exception:
                pass

    def test_ltree_nlevel_subpath(self, postgres_backend_single):
        """Test nlevel and subpath functions."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("ltree"):
            pytest.skip("Extension 'ltree' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_ltree_func")
            backend.execute("""
                CREATE TABLE test_ltree_func (
                    id SERIAL PRIMARY KEY,
                    path LTREE
                )
            """)
            backend.execute("""
                INSERT INTO test_ltree_func (path) VALUES ('A.B.C.D')
            """)

            # Test nlevel: count number of labels
            result = backend.fetch_one("""
                SELECT nlevel(path) AS depth FROM test_ltree_func WHERE id = 1
            """)
            assert result['depth'] == 4

            # Test subpath: get first 2 levels
            result = backend.fetch_one("""
                SELECT subpath(path, 0, 2) AS sub FROM test_ltree_func WHERE id = 1
            """)
            assert result['sub'] is not None
            assert 'A.B' in str(result['sub'])

            # Test subpath: get remaining from offset 2
            result = backend.fetch_one("""
                SELECT subpath(path, 2) AS sub FROM test_ltree_func WHERE id = 1
            """)
            assert result['sub'] is not None
            assert 'C.D' in str(result['sub'])
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_ltree_func")
            except Exception:
                pass

    def test_ltree_gist_index(self, postgres_backend_single):
        """Test creating GiST index on ltree column."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("ltree"):
            pytest.skip("Extension 'ltree' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_ltree_idx")
            backend.execute("DROP INDEX IF EXISTS idx_ltree_path")
            backend.execute("""
                CREATE TABLE test_ltree_idx (
                    id SERIAL PRIMARY KEY,
                    path LTREE
                )
            """)

            # Create GiST index for ltree
            backend.execute("""
                CREATE INDEX idx_ltree_path ON test_ltree_idx USING gist (path)
            """)

            # Verify index exists
            result = backend.fetch_one("""
                SELECT COUNT(*) AS cnt
                FROM pg_indexes
                WHERE tablename = 'test_ltree_idx'
                  AND indexname = 'idx_ltree_path'
            """)
            assert result['cnt'] >= 1

            # Insert data and query using index
            backend.execute("""
                INSERT INTO test_ltree_idx (path) VALUES
                    ('a.b.c'), ('a.b.d'), ('x.y.z')
            """)

            result = backend.fetch_all("""
                SELECT path FROM test_ltree_idx
                WHERE path <@ 'a.b'::ltree
                ORDER BY path
            """)
            assert len(result) >= 2
        finally:
            try:
                backend.execute("DROP INDEX IF EXISTS idx_ltree_path")
                backend.execute("DROP TABLE IF EXISTS test_ltree_idx")
            except Exception:
                pass

    def test_ltree_ltxtquery_search(self, postgres_backend_single):
        """Test ltxtquery for full-text-like search on ltree paths."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("ltree"):
            pytest.skip("Extension 'ltree' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_ltree_txtq")
            backend.execute("""
                CREATE TABLE test_ltree_txtq (
                    id SERIAL PRIMARY KEY,
                    path LTREE
                )
            """)
            backend.execute("""
                INSERT INTO test_ltree_txtq (path) VALUES
                    ('Top.Science.Astronomy'),
                    ('Top.Science.Physics'),
                    ('Top.History')
            """)

            # @ operator with ltxtquery: paths matching 'Science & Astronomy'
            result = backend.fetch_all("""
                SELECT path FROM test_ltree_txtq
                WHERE path @ 'Science & Astronomy'::ltxtquery
            """)
            assert len(result) >= 1
            paths = [r['path'] for r in result]
            assert 'Top.Science.Astronomy' in paths
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_ltree_txtq")
            except Exception:
                pass
