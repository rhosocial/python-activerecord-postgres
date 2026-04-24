# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/extensions/test_fuzzystrmatch_integration.py
"""
Integration tests for PostgreSQL fuzzystrmatch extension with real database.

These tests require a live PostgreSQL connection with fuzzystrmatch extension installed
and test:
- levenshtein() distance function
- soundex() encoding
- difference() function
- metaphone() function
- dmetaphone() function
"""
import pytest


class TestFuzzystrmatchIntegration:
    """Integration tests for fuzzystrmatch extension."""

    def test_levenshtein(self, postgres_backend_single):
        """Test levenshtein() distance function."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("fuzzystrmatch"):
            pytest.skip("Extension 'fuzzystrmatch' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_fuzzy_lev")
            backend.execute("""
                CREATE TABLE test_fuzzy_lev (
                    id SERIAL PRIMARY KEY,
                    word TEXT
                )
            """)
            backend.execute("""
                INSERT INTO test_fuzzy_lev (word) VALUES
                    ('kitten'),
                    ('sitting'),
                    ('sunday'),
                    ('saturday')
            """)

            # Levenshtein distance between 'kitten' and 'sitting' is 3
            result = backend.fetch_one("""
                SELECT levenshtein('kitten', 'sitting') AS dist
            """)
            assert result['dist'] == 3

            # Levenshtein distance between 'sunday' and 'saturday' is 3
            result = backend.fetch_one("""
                SELECT levenshtein('sunday', 'saturday') AS dist
            """)
            assert result['dist'] == 3

            # Same word should have distance 0
            result = backend.fetch_one("""
                SELECT levenshtein('hello', 'hello') AS dist
            """)
            assert result['dist'] == 0

            # Find words within Levenshtein distance of 3 from 'kitten'
            result = backend.fetch_all("""
                SELECT word FROM test_fuzzy_lev
                WHERE levenshtein('kitten', word) <= 3
            """)
            words = [r['word'] for r in result]
            assert 'kitten' in words
            assert 'sitting' in words
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_fuzzy_lev")
            except Exception:
                pass

    def test_soundex(self, postgres_backend_single):
        """Test soundex() encoding function."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("fuzzystrmatch"):
            pytest.skip("Extension 'fuzzystrmatch' not installed")

        # Homophones should have the same soundex code
        result1 = backend.fetch_one("SELECT soundex('Robert') AS code")
        result2 = backend.fetch_one("SELECT soundex('Rupert') AS code")
        assert result1['code'] == result2['code']

        # Different-sounding words should have different codes
        result3 = backend.fetch_one("SELECT soundex('Ashcraft') AS code")
        # Note: Ashcraft and Ashcroft have the same soundex
        result4 = backend.fetch_one("SELECT soundex('Ashcroft') AS code")
        assert result3['code'] == result4['code']

        # soundex code is 4 characters: letter + 3 digits
        result = backend.fetch_one("SELECT soundex('Hello') AS code")
        assert len(result['code']) == 4
        assert result['code'][0].isalpha()
        assert result['code'][1:].isdigit()

    def test_difference(self, postgres_backend_single):
        """Test difference() function (0-4 similarity based on soundex)."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("fuzzystrmatch"):
            pytest.skip("Extension 'fuzzystrmatch' not installed")

        try:
            backend.execute("DROP TABLE IF EXISTS test_fuzzy_diff")
            backend.execute("""
                CREATE TABLE test_fuzzy_diff (
                    id SERIAL PRIMARY KEY,
                    name TEXT
                )
            """)
            backend.execute("""
                INSERT INTO test_fuzzy_diff (name) VALUES
                    ('Smith'),
                    ('Smythe'),
                    ('Johnson')
            """)

            # Smith and Smythe should have high similarity
            result = backend.fetch_one("""
                SELECT difference('Smith', 'Smythe') AS diff
            """)
            assert result['diff'] >= 3  # High similarity

            # Smith and Johnson should have low similarity
            result = backend.fetch_one("""
                SELECT difference('Smith', 'Johnson') AS diff
            """)
            assert result['diff'] <= 2  # Low similarity

            # Find names that sound similar to 'Smith'
            result = backend.fetch_all("""
                SELECT name FROM test_fuzzy_diff
                WHERE difference(name, 'Smith') >= 3
            """)
            names = [r['name'] for r in result]
            assert 'Smith' in names
            assert 'Smythe' in names
        finally:
            try:
                backend.execute("DROP TABLE IF EXISTS test_fuzzy_diff")
            except Exception:
                pass

    def test_metaphone(self, postgres_backend_single):
        """Test metaphone() function."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("fuzzystrmatch"):
            pytest.skip("Extension 'fuzzystrmatch' not installed")

        result = backend.fetch_one("SELECT metaphone('hello', 6) AS meta")
        assert result['meta'] is not None
        assert len(result['meta']) <= 6

        # Words with similar pronunciation should have similar metaphone codes
        result1 = backend.fetch_one("SELECT metaphone('phone', 6) AS meta")
        result2 = backend.fetch_one("SELECT metaphone('fone', 6) AS meta")
        assert result1['meta'] == result2['meta']

    def test_dmetaphone(self, postgres_backend_single):
        """Test dmetaphone() function (double metaphone)."""
        backend = postgres_backend_single
        dialect = backend.dialect

        if not dialect.is_extension_installed("fuzzystrmatch"):
            pytest.skip("Extension 'fuzzystrmatch' not installed")

        result = backend.fetch_one("SELECT dmetaphone('hello') AS dmeta")
        assert result['dmeta'] is not None

        # dmetaphone provides better encoding for various languages
        result1 = backend.fetch_one("SELECT dmetaphone('Phillip') AS dmeta")
        result2 = backend.fetch_one("SELECT dmetaphone('Filip') AS dmeta")
        # Both should produce similar (or same) dmetaphone codes
        assert result1['dmeta'] is not None
        assert result2['dmeta'] is not None
