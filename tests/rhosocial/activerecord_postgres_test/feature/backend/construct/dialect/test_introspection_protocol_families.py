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
from pathlib import Path

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


def _repository_root() -> Path:
    """Walk up to the root that owns both ``docs/`` and ``pyproject.toml``."""
    for parent in Path(__file__).resolve().parents:
        if (parent / "pyproject.toml").is_file() and (parent / "docs").is_dir():
            return parent
    raise AssertionError("repository root not found from the test directory")


class TestDocsDoNotTeachRemovedNames:
    """The docs must not present a deleted API as current usage.

    A hit is allowed only on a line that says the name was removed — English
    "removed"/"no longer", or Chinese "已移除"/"不再". A bare mention reads as
    a live call and sends callers to an attribute that does not exist.
    """

    REMOVED_NAMES = ("list_partitions", "supports_partition_info")
    REMOVAL_WORDS = ("removed", "no longer", "已移除", "不再")

    def test_docs_mention_removed_names_only_when_recording_the_removal(self):
        docs = _repository_root() / "docs"
        offenders = []
        for path in sorted(docs.rglob("*.md")):
            for lineno, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1
            ):
                lowered = line.lower()
                for name in self.REMOVED_NAMES:
                    if name not in line:
                        continue
                    if any(word in lowered for word in self.REMOVAL_WORDS):
                        continue
                    offenders.append(
                        f"{path.relative_to(docs.parent)}:{lineno}: {line.strip()}"
                    )
        assert not offenders, "docs present removed names as live API:\n" + "\n".join(
            offenders
        )
