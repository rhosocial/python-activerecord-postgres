# tests/rhosocial/activerecord_postgres_test/feature/backend/core/test_get_type_adapters.py
"""The backend's adapter inventory is a catalogue, not a recommendation.

PostgreSQL-specific adapters are registered during construction and never need
a server, so the whole file runs without a database. What is pinned here is the
shape of ``get_type_adapters()``: it implements the core protocol, it lists
classes once each, and it answers in the same order every time.
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.backend import (
    AsyncPostgresBackend,
    PostgresBackend,
)
from rhosocial.activerecord.backend.type_adapter import TypeAdapterCatalogProtocol


def _backend_class_key(adapter):
    adapter_class = type(adapter)
    return (adapter_class.__module__, adapter_class.__qualname__)


@pytest.fixture(params=[PostgresBackend, AsyncPostgresBackend])
def backend(request):
    """A constructed backend: neither branch opens a connection."""
    return request.param(database="adapter_inventory_unit")


class TestGetTypeAdapters:
    def test_implements_the_catalogue_protocol(self, backend):
        assert isinstance(backend, TypeAdapterCatalogProtocol)

    def test_inventory_is_not_empty(self, backend):
        assert backend.get_type_adapters()

    def test_each_adapter_class_appears_once(self, backend):
        keys = [_backend_class_key(adapter) for adapter in backend.get_type_adapters()]
        assert len(keys) == len(set(keys))

    def test_order_is_deterministic_and_sorted_by_class(self, backend):
        first = [_backend_class_key(adapter) for adapter in backend.get_type_adapters()]
        second = [_backend_class_key(adapter) for adapter in backend.get_type_adapters()]
        assert first == second
        assert first == sorted(first)

    def test_a_new_list_is_returned_on_each_call(self, backend):
        first = backend.get_type_adapters()
        second = backend.get_type_adapters()
        assert first is not second
        assert first == second

    def test_default_adapter_suggestions_are_still_provided(self, backend):
        suggestions = backend.suggest_data_adapters().table
        assert isinstance(suggestions, dict)
        assert suggestions

def test_every_extra_value_class_has_an_adapter(backend):
    """An extra suggestion is only usable if the value can also be adapted.

    The suggestion table says what column a bare field is declared as; the
    adapter registry says whether that field can be written and read back. An
    entry without an adapter would build a column nothing can carry a value
    into, which is worse than refusing the type outright.
    """
    extras = backend.dialect.suggest_extra_data_types().table
    registered = {py for (py, _db) in backend.adapter_registry.get_all_adapters()}

    missing = [entry.__name__ for entry in extras if entry not in registered]
    assert missing == [], (
        f"{type(backend).__name__} suggests a declaration for {missing} but has no "
        f"adapter registered for those value classes"
    )

