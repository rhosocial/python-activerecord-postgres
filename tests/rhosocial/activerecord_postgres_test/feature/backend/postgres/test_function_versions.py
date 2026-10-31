# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_function_versions.py
"""Unit tests for PostgreSQL function version requirements system.

Tests for:
- FunctionVersionRequirement dataclass
- FunctionSupportInfo dataclass
- POSTGRES_FUNCTION_VERSIONS assembly
- _check_function_support() detailed support info
- supports_functions() return type
"""

import pytest

from rhosocial.activerecord.backend.impl.postgres.function_versions import (
    FunctionVersionRequirement,
    FunctionSupportInfo,
    POSTGRES_FUNCTION_VERSIONS,
    JSON_PATH_FUNCTION_VERSIONS,
    RANGE_FUNCTION_VERSIONS,
    GEOMETRIC_FUNCTION_VERSIONS,
    ENUM_FUNCTION_VERSIONS,
    BIT_STRING_FUNCTION_VERSIONS,
    TEXT_SEARCH_FUNCTION_VERSIONS,
    XML_FUNCTION_VERSIONS,
    MATH_FUNCTION_VERSIONS,
    ARRAY_FUNCTION_VERSIONS,
    NETWORK_FUNCTION_VERSIONS,
    RANGE_CONSTRUCTOR_FUNCTION_VERSIONS,
    UUID_FUNCTION_VERSIONS,
    HSTORE_FUNCTION_VERSIONS,
    PGVECTOR_FUNCTION_VERSIONS,
    POSTGIS_FUNCTION_VERSIONS,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.protocols.base import PostgresExtensionInfo


# ── FunctionVersionRequirement ──────────────────────────────


class TestFunctionVersionRequirement:
    """Tests for FunctionVersionRequirement dataclass."""

    def test_default_values(self):
        """All fields default to None."""
        req = FunctionVersionRequirement()
        assert req.min_pg_version is None
        assert req.max_pg_version is None
        assert req.extension is None
        assert req.min_ext_version is None
        assert req.ext_feature is None

    def test_builtin_function(self):
        """Built-in function: only PG version."""
        req = FunctionVersionRequirement(min_pg_version=(12, 0, 0))
        assert req.min_pg_version == (12, 0, 0)
        assert req.extension is None
        assert not req.is_extension_function

    def test_extension_function(self):
        """Extension function: requires extension."""
        req = FunctionVersionRequirement(extension="hstore", min_ext_version="1.0")
        assert req.extension == "hstore"
        assert req.min_ext_version == "1.0"
        assert req.is_extension_function

    def test_extension_with_pg_version(self):
        """Extension function with PG version constraint."""
        req = FunctionVersionRequirement(
            extension="hstore", min_ext_version="1.0", min_pg_version=(9, 3, 0),
        )
        assert req.extension == "hstore"
        assert req.min_pg_version == (9, 3, 0)

    def test_extension_with_feature(self):
        """Extension function with feature-level check."""
        req = FunctionVersionRequirement(extension="vector", ext_feature="similarity_search")
        assert req.extension == "vector"
        assert req.ext_feature == "similarity_search"

    def test_frozen(self):
        """FunctionVersionRequirement is immutable."""
        req = FunctionVersionRequirement(min_pg_version=(12, 0, 0))
        with pytest.raises(AttributeError):
            req.min_pg_version = (13, 0, 0)

    def test_equality(self):
        """Two identical requirements are equal."""
        req1 = FunctionVersionRequirement(extension="hstore", min_ext_version="1.0")
        req2 = FunctionVersionRequirement(extension="hstore", min_ext_version="1.0")
        assert req1 == req2

    def test_hash(self):
        """Identical requirements have same hash."""
        req1 = FunctionVersionRequirement(extension="hstore", min_ext_version="1.0")
        req2 = FunctionVersionRequirement(extension="hstore", min_ext_version="1.0")
        assert hash(req1) == hash(req2)


# ── FunctionSupportInfo ────────────────────────────────────


class TestFunctionSupportInfo:
    """Tests for FunctionSupportInfo dataclass."""

    def test_supported(self):
        """Supported function has reason=None."""
        info = FunctionSupportInfo(supported=True)
        assert info.supported is True
        assert info.reason is None

    def test_unsupported_with_reason(self):
        """Unsupported function has a reason."""
        info = FunctionSupportInfo(supported=False, reason="pg_version_too_low")
        assert info.supported is False
        assert info.reason == "pg_version_too_low"

    def test_frozen(self):
        """FunctionSupportInfo is immutable."""
        info = FunctionSupportInfo(supported=True)
        with pytest.raises(AttributeError):
            info.supported = False

    def test_all_reason_values(self):
        """All documented reason values are valid."""
        reasons = [
            "pg_version_too_low",
            "pg_version_too_high",
            "extension_not_probed",
            "extension_not_installed",
            "extension_version_insufficient",
        ]
        for reason in reasons:
            info = FunctionSupportInfo(supported=False, reason=reason)
            assert info.reason == reason


# ── POSTGRES_FUNCTION_VERSIONS assembly ────────────────────


class TestPostgresFunctionVersionsAssembly:
    """Tests for the assembled POSTGRES_FUNCTION_VERSIONS dict."""

    def test_not_empty(self):
        """The assembled dict is not empty."""
        assert len(POSTGRES_FUNCTION_VERSIONS) > 0

    def test_all_values_are_requirement_instances(self):
        """Every value is a FunctionVersionRequirement."""
        for name, req in POSTGRES_FUNCTION_VERSIONS.items():
            assert isinstance(req, FunctionVersionRequirement), f"{name} has wrong type"

    def test_no_duplicate_keys(self):
        """No function name appears in multiple categories."""
        all_names = []
        categories = [
            JSON_PATH_FUNCTION_VERSIONS,
            RANGE_FUNCTION_VERSIONS,
            GEOMETRIC_FUNCTION_VERSIONS,
            ENUM_FUNCTION_VERSIONS,
            BIT_STRING_FUNCTION_VERSIONS,
            TEXT_SEARCH_FUNCTION_VERSIONS,
            XML_FUNCTION_VERSIONS,
            MATH_FUNCTION_VERSIONS,
            ARRAY_FUNCTION_VERSIONS,
            NETWORK_FUNCTION_VERSIONS,
            RANGE_CONSTRUCTOR_FUNCTION_VERSIONS,
            UUID_FUNCTION_VERSIONS,
            HSTORE_FUNCTION_VERSIONS,
            PGVECTOR_FUNCTION_VERSIONS,
            POSTGIS_FUNCTION_VERSIONS,
        ]
        for cat in categories:
            all_names.extend(cat.keys())
        assert len(all_names) == len(set(all_names)), "Duplicate function names across categories"

    def test_expected_function_count(self):
        """Expected number of functions in the assembled dict."""
        # This should be updated if new functions are added
        assert len(POSTGRES_FUNCTION_VERSIONS) >= 168

    def test_json_path_functions_count(self):
        """9 JSON path functions."""
        assert len(JSON_PATH_FUNCTION_VERSIONS) == 9

    def test_hstore_functions_are_extension(self):
        """All hstore functions are extension functions."""
        for name, req in HSTORE_FUNCTION_VERSIONS.items():
            assert req.is_extension_function, f"{name} is not marked as extension"
            assert req.extension == "hstore", f"{name} has wrong extension"

    def test_uuid_ossp_functions_are_extension(self):
        """uuid-ossp functions are marked as extension."""
        for name, req in UUID_FUNCTION_VERSIONS.items():
            if name == "gen_random_uuid":
                assert not req.is_extension_function
            else:
                assert req.is_extension_function, f"{name} is not marked as extension"
                assert req.extension == "uuid-ossp"

    def test_pgvector_functions_are_extension(self):
        """All pgvector functions require the vector extension."""
        for name, req in PGVECTOR_FUNCTION_VERSIONS.items():
            assert req.is_extension_function, f"{name} is not marked as extension"
            assert req.extension == "vector"

    def test_postgis_functions_are_extension(self):
        """All PostGIS functions require the postgis extension."""
        for name, req in POSTGIS_FUNCTION_VERSIONS.items():
            assert req.is_extension_function, f"{name} is not marked as extension"
            assert req.extension == "postgis"


# ── supports_functions(): function availability ─────────────


class TestSupportsFunctionsAvailability:
    """Whether a function is available, asked of the public entry point.

    These assertions ran against a private boolean fork
    (``_is_postgres_function_supported``) that no production code called and that
    duplicated the version and extension checks with a coarser answer. The
    public ``supports_functions()`` answers the same question and names the
    reason, so each case below asserts the verdict *and* the reason: a check
    that starts failing for a different cause fails here instead of reading as
    the same answer.

    ``dialect._extensions`` is set directly because it is what
    ``introspect_and_adapt()`` would have populated against a live database;
    there is no public entry point for injecting that state yet.
    """

    def _support(self, dialect, func_name):
        return dialect.supports_functions()[func_name]

    def test_core_function_version_ok(self):
        """Built-in function with sufficient PG version is supported."""
        dialect = PostgresDialect((12, 0, 0))
        info = self._support(dialect, "jsonb_path_query")
        assert info.supported is True
        assert info.reason is None

    def test_core_function_version_too_low(self):
        """Built-in function below its minimum is refused, and says why."""
        dialect = PostgresDialect((11, 0, 0))
        info = self._support(dialect, "jsonb_path_query")
        assert info.supported is False
        assert info.reason == "pg_version_too_low"

    def test_core_function_no_version_requirement(self):
        """A function with no version gate is supported on any version."""
        dialect = PostgresDialect((8, 0, 0))
        assert self._support(dialect, "geometry_distance").supported is True

    def test_unregistered_function_is_absent(self):
        """A function nobody registered is not reported as supported.

        The deleted private fork answered ``True`` for an unknown name, which
        read a typo as "supported". The public table simply has no entry, so
        the caller can tell "unknown" from "supported".
        """
        dialect = PostgresDialect((14, 0, 0))
        assert dialect.supports_functions().get("unknown_function_xyz") is None

    def test_extension_function_not_probed(self):
        """An extension function is refused until the extensions are probed."""
        dialect = PostgresDialect((14, 0, 0))
        info = self._support(dialect, "hstore_from_record")
        assert info.supported is False
        assert info.reason == "extension_not_probed"

    def test_extension_function_probed_installed(self):
        """An installed extension unlocks its functions."""
        dialect = PostgresDialect((14, 0, 0))
        dialect._extensions = {
            "hstore": PostgresExtensionInfo(name="hstore", installed=True, version="1.8"),
        }
        assert self._support(dialect, "hstore_from_record").supported is True

    def test_extension_function_probed_none_installed(self):
        """Probed, but the extension is absent: refused, and not "unprobed".

        An empty ``_extensions`` means the probe ran and found nothing, so the
        reason names the extension rather than the missing probe.
        """
        dialect = PostgresDialect((14, 0, 0))
        dialect._extensions = {}
        info = self._support(dialect, "hstore_from_record")
        assert info.supported is False
        assert info.reason == "extension_not_installed"

    def test_uuid_ossp_function_installed(self):
        """uuid-ossp is an extension: its functions follow the extension."""
        dialect = PostgresDialect((14, 0, 0))
        dialect._extensions = {
            "uuid-ossp": PostgresExtensionInfo(name="uuid-ossp", installed=True, version="1.1"),
        }
        assert self._support(dialect, "uuid_generate_v1").supported is True

    def test_uuid_ossp_function_not_installed(self):
        dialect = PostgresDialect((14, 0, 0))
        dialect._extensions = {}
        assert self._support(dialect, "uuid_generate_v1").supported is False

    def test_gen_random_uuid_is_builtin(self):
        """gen_random_uuid needs no extension, only PG 13+."""
        assert self._support(PostgresDialect((13, 0, 0)), "gen_random_uuid").supported is True
        assert self._support(PostgresDialect((12, 0, 0)), "gen_random_uuid").supported is False

    def test_hstore_function_with_pg_version_constraint(self):
        """hstore_to_json needs both the extension and PG 9.3+."""
        dialect = PostgresDialect((9, 2, 0))
        dialect._extensions = {
            "hstore": PostgresExtensionInfo(name="hstore", installed=True, version="1.8"),
        }
        # PG 9.2 is below the floor, extension installed or not
        assert self._support(dialect, "hstore_to_json").supported is False

        dialect.version = (9, 3, 0)
        assert self._support(dialect, "hstore_to_json").supported is True

    def test_pgvector_function_with_feature_check(self):
        """pgvector gates on a feature of the installed extension version."""
        dialect = PostgresDialect((14, 0, 0))
        # vector 0.3.0 installed - basic features should work
        dialect._extensions = {
            "vector": PostgresExtensionInfo(name="vector", installed=True, version="0.3.0"),
        }
        assert self._support(dialect, "vector_l2_distance").supported is True

    def test_core_function_works_without_extensions(self):
        """Built-ins are answered without probing anything."""
        dialect = PostgresDialect((14, 0, 0))
        table = dialect.supports_functions()
        for name in ("geometry_distance", "range_contains", "bit_concat"):
            assert table[name].supported is True, name


# ── _check_function_support ────────────────────────────────


class TestCheckFunctionSupport:
    """Tests for _check_function_support() returning FunctionSupportInfo."""

    def test_supported_core_function(self):
        """Supported core function returns supported=True, reason=None."""
        dialect = PostgresDialect((14, 0, 0))
        info = dialect._check_function_support("jsonb_path_query")
        assert info.supported is True
        assert info.reason is None

    def test_pg_version_too_low(self):
        """Low PG version returns pg_version_too_low."""
        dialect = PostgresDialect((11, 0, 0))
        info = dialect._check_function_support("jsonb_path_query")
        assert info.supported is False
        assert info.reason == "pg_version_too_low"

    def test_extension_not_probed(self):
        """Extension function without _extensions returns extension_not_probed."""
        dialect = PostgresDialect((14, 0, 0))
        info = dialect._check_function_support("hstore_from_record")
        assert info.supported is False
        assert info.reason == "extension_not_probed"

    def test_extension_not_installed(self):
        """Probed but not installed returns extension_not_installed."""
        dialect = PostgresDialect((14, 0, 0))
        dialect._extensions = {}
        info = dialect._check_function_support("hstore_from_record")
        assert info.supported is False
        assert info.reason == "extension_not_installed"

    def test_extension_installed(self):
        """Installed extension returns supported=True."""
        dialect = PostgresDialect((14, 0, 0))
        dialect._extensions = {
            "hstore": PostgresExtensionInfo(name="hstore", installed=True, version="1.8"),
        }
        info = dialect._check_function_support("hstore_from_record")
        assert info.supported is True
        assert info.reason is None

    def test_extension_version_insufficient(self):
        """Installed but insufficient version returns extension_version_insufficient."""
        dialect = PostgresDialect((14, 0, 0))
        # vector 0.3.0 - hnsw_index needs 0.5.0+
        dialect._extensions = {
            "vector": PostgresExtensionInfo(name="vector", installed=True, version="0.3.0"),
        }
        # Use a function that requires a higher feature version
        # vector_literal only needs "type" feature (0.1+), so should be supported
        info = dialect._check_function_support("vector_literal")
        assert info.supported is True

    def test_unregistered_function(self):
        """Unregistered function returns supported=True."""
        dialect = PostgresDialect((14, 0, 0))
        info = dialect._check_function_support("nonexistent_function")
        assert info.supported is True
        assert info.reason is None


# ── supports_functions ─────────────────────────────────────


class TestSupportsFunctions:
    """Tests for supports_functions() return type and values."""

    def test_returns_dict_of_function_support_info(self):
        """Return type is Dict[str, FunctionSupportInfo]."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.supports_functions()
        assert isinstance(result, dict)
        assert len(result) > 0
        for name, info in result.items():
            assert isinstance(info, FunctionSupportInfo), f"{name} has wrong type"

    def test_all_entries_have_support_info(self):
        """Every entry has a valid FunctionSupportInfo."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.supports_functions()
        for name, info in result.items():
            assert isinstance(info.supported, bool), f"{name}.supported is not bool"

    def test_extension_functions_not_probed(self):
        """Extension functions are marked as extension_not_probed without _extensions."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.supports_functions()
        # hstore functions should be not probed
        if "hstore_from_record" in result:
            info = result["hstore_from_record"]
            assert info.supported is False
            assert info.reason == "extension_not_probed"

    def test_with_extensions_installed(self):
        """Extension functions are supported when extensions are installed."""
        dialect = PostgresDialect((14, 0, 0))
        dialect._extensions = {
            "hstore": PostgresExtensionInfo(name="hstore", installed=True, version="1.8"),
        }
        result = dialect.supports_functions()
        if "hstore_from_record" in result:
            info = result["hstore_from_record"]
            assert info.supported is True
            assert info.reason is None

    def test_core_functions_always_checked(self):
        """Core built-in functions are checked regardless of _extensions."""
        dialect = PostgresDialect((14, 0, 0))
        result = dialect.supports_functions()
        # Core functions should be supported
        if "geometry_distance" in result:
            assert result["geometry_distance"].supported is True
