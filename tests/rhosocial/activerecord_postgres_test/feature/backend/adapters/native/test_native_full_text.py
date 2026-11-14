# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_full_text.py
"""Native tests for PostgreSQL ``tsvector`` and ``tsquery``.

Both types are asserted through ``col::text``, which is PostgreSQL's own
lexeme serialisation, and then decoded with the backend wrappers.  A second
write must reproduce the same text (S2), and native literals written by SQL
must read back (S3).
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.adapters.text_search import (
    PostgresTsQueryAdapter,
    PostgresTsVectorAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.type_values import (
    PostgresTsQuery,
    PostgresTsVector,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    update_cell,
)

TABLE = "native_full_text"

TSVECTOR = PostgresTsVectorAdapter()
TSQUERY = PostgresTsQueryAdapter()


class TestFullText:
    TABLES = (TABLE,)

    def test_tsvector_keeps_positions_and_weights(self, native_backend):
        """S1/S2: sorted lexemes with positions/weights survive a rewrite."""
        backend = native_backend
        value = PostgresTsVector.from_string("'hello':1A 'world':2B")
        bound = TSVECTOR.to_database(value, str)
        assert bound == "'hello':1A 'world':2B"

        row_id = insert_row(
            backend, TABLE, {"vector_value": bound}, {"vector_value": "tsvector"}
        )
        raw = read_raw(backend, TABLE, "vector_value", row_id)
        assert raw == "'hello':1A 'world':2B"

        decoded = TSVECTOR.from_database(raw, PostgresTsVector)
        assert decoded == value
        assert decoded.get_positions("hello") == [(1, "A")]

        update_cell(
            backend, TABLE, "vector_value", row_id,
            TSVECTOR.to_database(decoded, str), "tsvector",
        )
        assert read_raw(backend, TABLE, "vector_value", row_id) == raw

    def test_tsquery_keeps_boolean_structure(self, native_backend):
        """S1/S2: the AND structure and its text stay stable."""
        backend = native_backend
        value = PostgresTsQuery.parse("hello & world")
        bound = TSQUERY.to_database(value, str)
        assert bound == "'hello' & 'world'"

        row_id = insert_row(
            backend, TABLE, {"query_value": bound}, {"query_value": "tsquery"}
        )
        raw = read_raw(backend, TABLE, "query_value", row_id)
        assert raw == "'hello' & 'world'"

        decoded = TSQUERY.from_database(raw, PostgresTsQuery)
        assert decoded == value

        update_cell(
            backend, TABLE, "query_value", row_id,
            TSQUERY.to_database(decoded, str), "tsquery",
        )
        assert read_raw(backend, TABLE, "query_value", row_id) == raw

    def test_external_full_text_literals_are_parsed(self, native_backend):
        """S3: native text forms produced by SQL read back as wrappers."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (vector_value, query_value) VALUES "
            "(to_tsvector('simple', 'cat dog'), "
            "to_tsquery('simple', 'cat & dog')) RETURNING id".format(TABLE),
        )

        raw_vector = read_raw(backend, TABLE, "vector_value", row_id)
        raw_query = read_raw(backend, TABLE, "query_value", row_id)
        assert raw_vector == "'cat':1 'dog':2"
        assert raw_query == "'cat' & 'dog'"

        vector = TSVECTOR.from_database(raw_vector, PostgresTsVector)
        query = TSQUERY.from_database(raw_query, PostgresTsQuery)
        assert vector.get_lexemes() == ["cat", "dog"]
        assert vector.get_positions("dog") == [(2, None)]
        assert query == PostgresTsQuery.parse("cat & dog")

    def test_invalid_tsquery_text_raises_value_error(self):
        """The legacy wrapper adapter raises ValueError, not TypeConversionError.

        The family-based adapters wrap failures, but ``PostgresTsQueryAdapter``
        predates that base class and surfaces the parser's ValueError directly.
        Recording it here keeps the contract explicit instead of guessing.
        """
        with pytest.raises(ValueError):
            TSQUERY.from_database("hello &", PostgresTsQuery)
