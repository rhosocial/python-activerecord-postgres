# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_remaining.py
"""Native tests for ``bytea`` and ``interval``: the two pending gaps.

Neither type has an adapter registered by this backend: ``bytes`` and
``timedelta`` are absent from ``get_default_adapter_suggestions()`` and from
the adapter registry.  psycopg still round-trips both natively, so the tests
pin that pass-through and record the missing adapter as a Phase 3b gap
instead of inventing an adapter here.
"""

from datetime import timedelta

from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    read_value,
    update_cell,
)

TABLE = "native_remaining"


def _adapter_keys(backend, py_type):
    return [
        key
        for key in backend.adapter_registry.get_all_adapters()
        if key[0] is py_type
    ]


class TestRemainingTypes:
    TABLES = (TABLE,)

    def test_bytea_has_no_adapter_and_passes_through(self, native_backend):
        """Gap: bytes has no adapter; psycopg binds and returns it unchanged."""
        backend = native_backend
        suggestions = backend.get_default_adapter_suggestions()
        assert suggestions.get(bytes) is None
        assert _adapter_keys(backend, bytes) == []

        value = b"\x00\x01\xff"
        row_id = insert_row(backend, TABLE, {"payload": value}, {"payload": "bytea"})
        assert read_raw(backend, TABLE, "payload", row_id) == "\\x0001ff"
        assert read_value(backend, TABLE, "payload", row_id) == value

        update_cell(backend, TABLE, "payload", row_id, value, "bytea")
        assert read_value(backend, TABLE, "payload", row_id) == value

    def test_bytea_external_hex_literal_reads_back(self, native_backend):
        """S3: the server's own hex form decodes to the same bytes."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (payload) VALUES ('\\x0001ff'::bytea) RETURNING id".format(
                TABLE
            ),
        )
        assert read_raw(backend, TABLE, "payload", row_id) == "\\x0001ff"
        assert read_value(backend, TABLE, "payload", row_id) == b"\x00\x01\xff"

    def test_interval_has_no_adapter_and_passes_through(self, native_backend):
        """Gap: timedelta has no adapter; the driver handles interval natively."""
        backend = native_backend
        suggestions = backend.get_default_adapter_suggestions()
        assert suggestions.get(timedelta) is None
        assert _adapter_keys(backend, timedelta) == []

        value = timedelta(days=1, seconds=7384)
        row_id = insert_row(backend, TABLE, {"span": value}, {"span": "interval"})
        assert read_raw(backend, TABLE, "span", row_id) == "1 day 02:03:04"
        assert read_value(backend, TABLE, "span", row_id) == value

        update_cell(backend, TABLE, "span", row_id, read_value(backend, TABLE, "span", row_id), "interval")
        assert read_raw(backend, TABLE, "span", row_id) == "1 day 02:03:04"

    def test_interval_external_literal_reads_back(self, native_backend):
        """S3: a server-written interval text decodes to a timedelta."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (span) VALUES ('1 day 02:03:04'::interval) RETURNING id".format(
                TABLE
            ),
        )
        assert read_raw(backend, TABLE, "span", row_id) == "1 day 02:03:04"
        assert read_value(backend, TABLE, "span", row_id) == timedelta(
            days=1, seconds=7384
        )
