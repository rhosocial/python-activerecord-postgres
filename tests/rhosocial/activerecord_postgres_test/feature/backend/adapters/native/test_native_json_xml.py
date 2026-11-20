# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_json_xml.py
"""Native tests for PostgreSQL ``json``/``jsonb``/``jsonpath`` and ``xml``.

psycopg decodes ``json`` and ``jsonb`` to ``dict``/``list`` before the
adapter sees them, so ``from_database`` must pass the decoded document
through instead of calling ``json.loads`` again.  ``jsonpath`` and ``xml``
have text wire forms and are asserted through ``col::text``.
"""

import json

import pytest

from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.impl.postgres.adapters.base import (
    PostgresJSONBAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.json import (
    PostgresJsonPathAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.xml import (
    PostgresXMLAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.type_values import (
    PostgresJsonPath,
    PostgresXML,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    read_value,
    update_cell,
)

TABLE = "native_json_xml"

JSON_ADAPTER = PostgresJSONBAdapter()
JSONPATH = PostgresJsonPathAdapter()
XML = PostgresXMLAdapter()


class TestJsonXml:
    TABLES = (TABLE,)

    def test_json_and_jsonb_pass_decoded_documents_through(self, native_backend):
        """S1: psycopg returns dicts, and the adapter does not re-decode them."""
        backend = native_backend
        value = {"a": 1, "b": [1, 2]}
        bound = JSON_ADAPTER.to_database(value, dict)
        row_id = insert_row(
            backend,
            TABLE,
            {"payload_json": bound, "payload_jsonb": bound},
            {"payload_json": "json", "payload_jsonb": "jsonb"},
        )

        # Both forms are serialised by the same dumper; jsonb canonicalises
        # the whitespace, json keeps the dumped compact shape.
        assert read_raw(backend, TABLE, "payload_json", row_id) == '{"a": 1, "b": [1, 2]}'
        assert read_raw(backend, TABLE, "payload_jsonb", row_id) == '{"a": 1, "b": [1, 2]}'

        for column in ("payload_json", "payload_jsonb"):
            driver_value = read_value(backend, TABLE, column, row_id)
            assert isinstance(driver_value, dict)
            assert driver_value == value
            assert JSON_ADAPTER.from_database(driver_value, dict) == value

        update_cell(backend, TABLE, "payload_jsonb", row_id, bound, "jsonb")
        assert read_raw(backend, TABLE, "payload_jsonb", row_id) == (
            '{"a": 1, "b": [1, 2]}'
        )

    def test_jsonpath_canonicalises_the_path_text(self, native_backend):
        """S1/S2: ``$.a`` is stored as ``$."a"`` and remains stable."""
        backend = native_backend
        value = PostgresJsonPath("$.a")
        bound = JSONPATH.to_database(value, str)
        assert bound == "$.a"

        row_id = insert_row(
            backend, TABLE, {"path_value": bound}, {"path_value": "jsonpath"}
        )
        raw = read_raw(backend, TABLE, "path_value", row_id)
        assert raw == '$."a"'
        decoded = JSONPATH.from_database(raw, PostgresJsonPath)
        assert decoded == PostgresJsonPath('$."a"')
        assert JSONPATH.to_database(decoded, str) == raw

        update_cell(backend, TABLE, "path_value", row_id, raw, "jsonpath")
        assert read_raw(backend, TABLE, "path_value", row_id) == raw

    def test_xml_text_round_trips(self, native_backend):
        """S1/S2: XML content is preserved verbatim and remains stable."""
        backend = native_backend
        value = PostgresXML("<r><a>1</a></r>")
        bound = XML.to_database(value, str)
        row_id = insert_row(backend, TABLE, {"doc": bound}, {"doc": "xml"})
        raw = read_raw(backend, TABLE, "doc", row_id)
        assert raw == "<r><a>1</a></r>"
        assert XML.from_database(raw, PostgresXML) == value

        update_cell(backend, TABLE, "doc", row_id, raw, "xml")
        assert read_raw(backend, TABLE, "doc", row_id) == raw

    def test_external_json_jsonpath_and_xml_literals(self, native_backend):
        """S3: native SQL text is decoded into the same Python shapes."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (payload_json, payload_jsonb, path_value, doc) VALUES "
            "('{{\"k\": \"v\"}}'::json, '{{\"k\":\"v\"}}'::jsonb, "
            "'$.a'::jsonpath, '<r><a>1</a></r>'::xml) RETURNING id".format(TABLE),
        )

        assert read_raw(backend, TABLE, "payload_json", row_id) == '{"k": "v"}'
        assert read_raw(backend, TABLE, "payload_jsonb", row_id) == '{"k": "v"}'
        assert read_raw(backend, TABLE, "path_value", row_id) == '$."a"'
        assert read_raw(backend, TABLE, "doc", row_id) == "<r><a>1</a></r>"

        for column in ("payload_json", "payload_jsonb"):
            driver_value = read_value(backend, TABLE, column, row_id)
            assert driver_value == {"k": "v"}
            assert JSON_ADAPTER.from_database(driver_value, dict) == {"k": "v"}

        assert JSONPATH.from_database('$."a"', PostgresJsonPath) == PostgresJsonPath(
            '$."a"'
        )
        assert XML.from_database("<r><a>1</a></r>", PostgresXML) == PostgresXML(
            "<r><a>1</a></r>"
        )

    def test_malformed_xml_is_rejected_by_the_server(self, native_backend):
        """PostgreSQL validates XML on input."""
        with pytest.raises(DatabaseError):
            native_backend.execute("SELECT %s::xml", ("<r><a></r>",))

    def test_jsonpath_requires_a_root_marker(self):
        """The adapter rejects a path that is not rooted at ``$``."""
        with pytest.raises(ValueError):
            JSONPATH.to_database("a", str)
        with pytest.raises(ValueError):
            JSONPATH.from_database("a", PostgresJsonPath)

    def test_json_document_is_never_double_encoded(self, native_backend):
        """S2 guard: a dict read back stays a dict, not a JSON string."""
        backend = native_backend
        value = {"nested": {"n": 1}}
        bound = JSON_ADAPTER.to_database(value, dict)
        row_id = insert_row(
            backend, TABLE, {"payload_jsonb": bound}, {"payload_jsonb": "jsonb"}
        )
        driver_value = read_value(backend, TABLE, "payload_jsonb", row_id)
        decoded = JSON_ADAPTER.from_database(driver_value, dict)
        assert decoded == value
        assert not isinstance(decoded, str)
        assert json.dumps(decoded, sort_keys=True) == json.dumps(value, sort_keys=True)
