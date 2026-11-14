# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_arrays.py
"""Native tests for PostgreSQL ``T[]`` arrays.

The registered suggestion for ``list`` must stay the native
``PostgresListAdapter`` (list -> list), never the JSON text adapter.  Arrays
are asserted three ways: the server's ``{...}`` text, the driver's decoded
list, and a native ``{1,2}`` literal written without the adapter.
"""

import pytest

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.impl.postgres.adapters.base import (
    PostgresListAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.json import (
    PostgresJSONBAdapter,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    read_value,
    update_cell,
)

TABLE = "native_arrays"

ARRAY = PostgresListAdapter()


class TestArrays:
    TABLES = (TABLE,)

    def test_native_int_and_text_arrays_round_trip(self, native_backend):
        """S1/S2: bound lists stay lists and their server text is ``{...}``."""
        backend = native_backend
        ints = [1, 2, 3]
        texts = ["a", "b"]
        row_id = insert_row(
            backend,
            TABLE,
            {
                "ints": ARRAY.to_database(ints, list),
                "texts": ARRAY.to_database(texts, list),
            },
            {"ints": "int[]", "texts": "text[]"},
        )

        assert read_raw(backend, TABLE, "ints", row_id) == "{1,2,3}"
        assert read_raw(backend, TABLE, "texts", row_id) == "{a,b}"
        assert read_value(backend, TABLE, "ints", row_id) == ints
        assert read_value(backend, TABLE, "texts", row_id) == texts

        assert ARRAY.from_database(read_value(backend, TABLE, "ints", row_id), list) == ints
        assert ARRAY.from_database(read_value(backend, TABLE, "texts", row_id), list) == texts

        update_cell(
            backend, TABLE, "ints", row_id,
            ARRAY.to_database(ARRAY.from_database(ints, list), list), "int[]",
        )
        assert read_raw(backend, TABLE, "ints", row_id) == "{1,2,3}"

    def test_empty_list_is_distinct_from_null(self, native_backend):
        """An empty list stores ``{}``, while NULL remains NULL."""
        backend = native_backend
        empty_row = insert_row(
            backend, TABLE, {"ints": ARRAY.to_database([], list)}, {"ints": "int[]"}
        )
        null_row = insert_row(backend, TABLE, {"ints": None}, {"ints": "int[]"})

        assert read_raw(backend, TABLE, "ints", empty_row) == "{}"
        assert read_value(backend, TABLE, "ints", empty_row) == []
        assert read_value(backend, TABLE, "ints", null_row) is None

    def test_native_array_literal_reads_back_as_a_list(self, native_backend):
        """S3: postgres-written ``{4,5}`` and ``{x,y}`` are plain lists."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (ints, texts) VALUES "
            "('{{4,5}}'::int[], '{{x,y}}'::text[]) RETURNING id".format(TABLE),
        )
        assert read_raw(backend, TABLE, "ints", row_id) == "{4,5}"
        assert read_raw(backend, TABLE, "texts", row_id) == "{x,y}"
        assert read_value(backend, TABLE, "ints", row_id) == [4, 5]
        assert read_value(backend, TABLE, "texts", row_id) == ["x", "y"]
        assert ARRAY.from_database([4, 5], list) == [4, 5]

    def test_array_suggestion_is_native_and_not_json(self, native_backend):
        """J9 guard: ``list`` resolves to the array adapter, not JSON text."""
        suggestions = native_backend.get_default_adapter_suggestions()
        adapter, target = suggestions[list]
        assert target is list
        assert isinstance(adapter, PostgresListAdapter)
        assert not isinstance(adapter, PostgresJSONBAdapter)
        # The write-side value is handed to psycopg unchanged.
        assert adapter.to_database([1, 2], list) == [1, 2]

    def test_wrong_element_type_is_rejected_by_the_server(self, native_backend):
        """The array's element type is enforced by PostgreSQL."""
        with pytest.raises(DatabaseError):
            native_backend.execute(
                "SELECT %s::int[]", (ARRAY.to_database(["not-an-int"], list),)
            )
