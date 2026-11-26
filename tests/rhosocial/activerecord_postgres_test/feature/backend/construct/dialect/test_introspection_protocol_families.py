# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/dialect/test_introspection_protocol_families.py
"""Guard this backend's join with core's four-family introspection split.

Core split the single ``IntrospectionSupport`` protocol into four families,
each with an async twin, and deleted ``supports_partition_info``. These checks
pin the consequences this backend depends on:

* neither backend-local partition spelling survives;
* the CLI's ``partitions`` family resolves to the core partition probe, which
  is PG 10+ and the same predicate as ``supports_table_partitioning``;
* the four family protocols import from their new core modules;
* the old single protocol names are not exported anywhere.
"""

import importlib

import pytest

from rhosocial.activerecord.backend import dialect as core_dialect
from rhosocial.activerecord.backend.dialect import protocols as core_protocols
from rhosocial.activerecord.backend.impl.postgres import mixins as postgres_mixins
from rhosocial.activerecord.backend.impl.postgres.cli import introspect as introspect_mod
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect

pytestmark = [pytest.mark.feature]

#: family protocol -> the core module it is declared in.
FAMILY_MODULES = [
    (
        "StructureIntrospectionSupport",
        "rhosocial.activerecord.backend.dialect.protocols.introspect.structure",
    ),
    (
        "OperationalStatsIntrospectionSupport",
        "rhosocial.activerecord.backend.dialect.protocols.introspect.operational_stats",
    ),
    (
        "DataProfileIntrospectionSupport",
        "rhosocial.activerecord.backend.dialect.protocols.introspect.data_profile",
    ),
    (
        "DdlExtractionIntrospectionSupport",
        "rhosocial.activerecord.backend.dialect.protocols.introspect.ddl_extraction",
    ),
]

ASYNC_TWINS = {
    "StructureIntrospectionSupport": "AsyncStructureIntrospectionSupport",
    "OperationalStatsIntrospectionSupport": "AsyncOperationalStatsIntrospectionSupport",
    "DataProfileIntrospectionSupport": "AsyncDataProfileIntrospectionSupport",
    "DdlExtractionIntrospectionSupport": "AsyncDdlExtractionIntrospectionSupport",
}


class TestPartitionProbeResolution:
    """One partition question, answered by the core partition protocol."""

    def test_the_backend_local_partition_spellings_are_gone(self):
        dialect = PostgresDialect(version=(16, 0))
        assert not hasattr(dialect, "supports_partition_info")
        assert not hasattr(dialect, "supports_partition_introspection")
        assert not hasattr(postgres_mixins.PostgresIntrospectionCapabilityMixin, "supports_partition_info")
        assert not hasattr(
            postgres_mixins.PostgresIntrospectionCapabilityMixin,
            "supports_partition_introspection",
        )

    def test_the_cli_partitions_family_uses_the_metadata_probe(self):
        assert (
            introspect_mod._TYPE_CAPABILITIES["partitions"]
            == "supports_partition_metadata_introspection"
        )

    @pytest.mark.parametrize(
        "version,expected",
        [
            ((16, 0, 0), True),
            ((10, 0, 0), True),
            ((9, 6, 0), False),
        ],
    )
    def test_the_metadata_probe_is_the_pg10_predicate(self, version, expected):
        dialect = PostgresDialect(version=version)
        assert dialect.supports_partition_metadata_introspection() is expected
        # Identical predicate: declarative partitioning is when the family
        # starts existing, so the two answers cannot drift apart.
        assert dialect.supports_table_partitioning() is dialect.supports_partition_metadata_introspection()


class TestFamilyProtocolImports:
    """The four families live in their own core modules, not one old name."""

    @pytest.mark.parametrize("name,module_name", FAMILY_MODULES)
    def test_sync_family_imports_from_its_own_module(self, name, module_name):
        module = importlib.import_module(module_name)
        protocol = getattr(module, name)
        assert protocol.__module__ == module_name
        assert getattr(core_protocols, name) is protocol

    @pytest.mark.parametrize("name,module_name", FAMILY_MODULES)
    def test_async_twin_imports_from_the_same_module(self, name, module_name):
        module = importlib.import_module(module_name)
        twin = getattr(module, ASYNC_TWINS[name])
        assert twin.__module__ == module_name

    def test_the_old_single_protocol_names_are_not_exported(self):
        for name in ("IntrospectionSupport", "AsyncIntrospectionSupport"):
            assert not hasattr(core_protocols, name)
            assert not hasattr(core_dialect, name)
