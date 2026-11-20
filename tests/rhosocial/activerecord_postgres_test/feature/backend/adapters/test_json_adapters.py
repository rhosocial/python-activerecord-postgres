# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/test_json_adapters.py
"""Database-free coverage for the JSON/JSONB adapter hooks.

Both definitions of ``PostgresJSONBAdapter`` are exercised: the one exported
by ``adapters`` (and registered on the backend) and the copy in
``adapters.json`` that offline tests import.
"""

import json
from decimal import Decimal

import pytest
from psycopg.types.json import Jsonb, JsonbDumper

from rhosocial.activerecord.backend.impl.postgres.adapters.base import (
    PostgresJSONBAdapter as BaseJSONBAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.json import (
    PostgresJSONBAdapter as JsonModuleJSONBAdapter,
)

ADAPTER_CLASSES = [
    pytest.param(BaseJSONBAdapter, id="adapters.base"),
    pytest.param(JsonModuleJSONBAdapter, id="adapters.json"),
]


class _Tagged:
    """Marker returned by the sample object hook."""

    def __init__(self, type_name, payload):
        self.type_name = type_name
        self.payload = payload


def _object_hook(mapping):
    if "__type__" in mapping:
        return _Tagged(mapping["__type__"], mapping.get("payload"))
    return mapping


def _dumped_json(wrapper):
    """Run the value through psycopg's own JSONB dumper, without a database."""
    return json.loads(JsonbDumper(Jsonb).dump(wrapper))


@pytest.mark.parametrize("adapter_cls", ADAPTER_CLASSES)
class TestJSONBNoHooks:
    def test_constructor_defaults(self, adapter_cls):
        adapter = adapter_cls()
        assert adapter.default is None
        assert adapter.object_hook is None

    def test_supported_types_unchanged(self, adapter_cls):
        adapter = adapter_cls()
        assert adapter.supported_types == {dict: {Jsonb}}

    def test_to_database_returns_jsonb(self, adapter_cls):
        adapter = adapter_cls()
        result = adapter.to_database({"k": 1, "v": [2, 3]}, dict)
        assert isinstance(result, Jsonb)
        assert result.obj == {"k": 1, "v": [2, 3]}
        assert _dumped_json(result) == {"k": 1, "v": [2, 3]}

    def test_to_database_none(self, adapter_cls):
        assert adapter_cls().to_database(None, dict) is None

    def test_from_database_dict_and_list_passthrough(self, adapter_cls):
        adapter = adapter_cls()
        payload = {"a": 1, "nested": {"b": 2}}
        decoded_list = [1, "two", {"three": 3}]
        assert adapter.from_database(payload, dict) == payload
        assert adapter.from_database(decoded_list, list) == decoded_list

    def test_from_database_string_is_parsed(self, adapter_cls):
        adapter = adapter_cls()
        assert adapter.from_database('{"a": 1}', dict) == {"a": 1}
        assert adapter.from_database("[1, 2, 3]", list) == [1, 2, 3]

    def test_from_database_str_target_keeps_json_text(self, adapter_cls):
        adapter = adapter_cls()
        payload = {"title": "\u4e2d\u6587"}
        assert adapter.from_database(payload, str) == json.dumps(payload, ensure_ascii=False)
        assert adapter.from_database("raw text", str) == "raw text"

    def test_unserialisable_value_raises_without_default(self, adapter_cls):
        adapter = adapter_cls()
        wrapper = adapter.to_database({"amount": Decimal("1.5")}, dict)
        with pytest.raises(TypeError):
            JsonbDumper(Jsonb).dump(wrapper)


@pytest.mark.parametrize("adapter_cls", ADAPTER_CLASSES)
class TestJSONBDefaultHook:
    def test_default_is_applied_by_the_dumper(self, adapter_cls):
        adapter = adapter_cls(default=str)
        wrapper = adapter.to_database({"amount": Decimal("1.5")}, dict)
        payload = _dumped_json(wrapper)
        assert payload == {"amount": "1.5"}

    def test_default_is_wired_into_the_jsonb_wrapper(self, adapter_cls):
        adapter = adapter_cls(default=lambda value: "<unserialisable>")
        wrapper = adapter.to_database({"when": object()}, dict)
        assert wrapper.dumps is not None
        assert _dumped_json(wrapper) == {"when": "<unserialisable>"}

    def test_default_none_uses_psycopg_global_dumps(self, adapter_cls):
        adapter = adapter_cls()
        wrapper = adapter.to_database({"a": 1}, dict)
        assert wrapper.dumps is None

    def test_default_applies_to_str_target_serialisation(self, adapter_cls):
        adapter = adapter_cls(default=str)
        # psycopg already decoded the value; serialising back to str must
        # route through the same ``default`` instead of raising.
        assert adapter.from_database({"amount": Decimal("1.5")}, str) == '{"amount": "1.5"}'

    def test_default_applies_to_batch(self, adapter_cls):
        adapter = adapter_cls(default=str)
        wrappers = adapter.to_database_batch([{"amount": Decimal("2.5")}, None], dict)
        assert wrappers[1] is None
        assert _dumped_json(wrappers[0]) == {"amount": "2.5"}


@pytest.mark.parametrize("adapter_cls", ADAPTER_CLASSES)
class TestJSONBObjectHook:
    def test_hook_runs_over_already_decoded_dict(self, adapter_cls):
        """psycopg decodes JSONB before the adapter; the hook still applies."""
        adapter = adapter_cls(object_hook=_object_hook)
        decoded = adapter.from_database({"__type__": "user", "payload": {"name": "ann"}}, dict)
        assert isinstance(decoded, _Tagged)
        assert decoded.type_name == "user"
        assert decoded.payload == {"name": "ann"}

    def test_hook_runs_over_nested_objects(self, adapter_cls):
        adapter = adapter_cls(object_hook=_object_hook)
        decoded = adapter.from_database(
            {"outer": {"__type__": "inner", "payload": 1}, "plain": 2}, dict
        )
        assert decoded["plain"] == 2
        assert isinstance(decoded["outer"], _Tagged)
        assert decoded["outer"].payload == 1

    def test_hook_runs_over_decoded_list(self, adapter_cls):
        adapter = adapter_cls(object_hook=_object_hook)
        decoded = adapter.from_database([{"__type__": "a", "payload": 1}, {"b": 2}], list)
        assert isinstance(decoded[0], _Tagged)
        assert decoded[1] == {"b": 2}

    def test_hook_runs_over_json_string(self, adapter_cls):
        adapter = adapter_cls(object_hook=_object_hook)
        decoded = adapter.from_database('{"__type__": "user", "payload": {"name": "ann"}}', dict)
        assert isinstance(decoded, _Tagged)
        assert decoded.payload == {"name": "ann"}

    def test_hook_is_not_applied_for_str_target(self, adapter_cls):
        adapter = adapter_cls(object_hook=_object_hook)
        assert adapter.from_database({"__type__": "user"}, str) == '{"__type__": "user"}'

    def test_plain_decode_without_hook_matches_dict(self, adapter_cls):
        adapter = adapter_cls()
        assert adapter.from_database({"__type__": "user"}, dict) == {"__type__": "user"}

    def test_default_and_object_hook_together(self, adapter_cls):
        adapter = adapter_cls(default=str, object_hook=_object_hook)
        wrapper = adapter.to_database({"__type__": "user", "amount": Decimal("1.5")}, dict)
        assert _dumped_json(wrapper) == {"__type__": "user", "amount": "1.5"}
        decoded = adapter.from_database({"__type__": "user", "payload": 1}, dict)
        assert isinstance(decoded, _Tagged)

    def test_batch_from_database_applies_hook(self, adapter_cls):
        adapter = adapter_cls(object_hook=_object_hook)
        decoded = adapter.from_database_batch([{"__type__": "user", "payload": 1}, None], dict)
        assert isinstance(decoded[0], _Tagged)
        assert decoded[1] is None
