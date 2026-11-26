# tests/rhosocial/activerecord_postgres_test/feature/backend/construct/functions/test_diagnostics_extensions.py
"""SQL construction tests for the diagnostics extension factories."""

import pytest

from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.functions.amcheck import (
    SkipMode,
    bt_index_check,
    bt_index_parent_check,
    verify_heapam,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pageinspect import (
    bt_page_stats,
    get_raw_page,
    heap_page_items,
    page_header,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pg_logicalinspect import (
    pg_get_logical_snapshot_info,
    pg_get_logical_snapshot_meta,
)
from rhosocial.activerecord.backend.impl.postgres.functions.pgstattuple import (
    pg_relpages,
    pgstatindex,
    pgstattuple,
)
from rhosocial.activerecord.backend.impl.postgres.protocols.base import PostgresExtensionInfo


@pytest.fixture
def dialect():
    return PostgresDialect()


class TestDiagnosticsFactories:
    @pytest.mark.parametrize(
        ("factory", "expected_sql", "expected_params"),
        [
            (lambda d: bt_index_check(d, "idx"), "BT_INDEX_CHECK(%s)", ("idx",)),
            (
                lambda d: bt_index_parent_check(d, "idx", heapallindexed=True),
                "BT_INDEX_PARENT_CHECK(%s, %s)",
                ("idx", True),
            ),
            (
                lambda d: get_raw_page(d, "t", "main", 0),
                "GET_RAW_PAGE(%s, %s, %s)",
                ("t", "main", 0),
            ),
            (lambda d: page_header(d, "page"), "PAGE_HEADER(%s)", ("page",)),
            (lambda d: bt_page_stats(d, "t", 1), "BT_PAGE_STATS(%s, %s)", ("t", 1)),
            (lambda d: heap_page_items(d, "page"), "HEAP_PAGE_ITEMS(%s)", ("page",)),
            (lambda d: pgstattuple(d, "t"), "PGSTATTUPLE(%s)", ("t",)),
            (lambda d: pgstatindex(d, "idx"), "PGSTATINDEX(%s)", ("idx",)),
            (lambda d: pg_relpages(d, "t"), "PG_RELPAGES(%s)", ("t",)),
            (
                lambda d: pg_get_logical_snapshot_meta(d, "x.snap"),
                "PG_GET_LOGICAL_SNAPSHOT_META(%s)",
                ("x.snap",),
            ),
            (
                lambda d: pg_get_logical_snapshot_info(d, "x.snap"),
                "PG_GET_LOGICAL_SNAPSHOT_INFO(%s)",
                ("x.snap",),
            ),
        ],
    )
    def test_sql(self, dialect, factory, expected_sql, expected_params):
        assert factory(dialect).to_sql() == (expected_sql, expected_params)

    def test_verify_heapam_defaults(self, dialect):
        sql, params = verify_heapam(dialect, "t").to_sql()
        assert sql == (
            "VERIFY_HEAPAM(%s::REGCLASS, %s, %s, %s, %s::BIGINT, %s::BIGINT)"
        )
        assert params == ("t", False, False, "none", None, None)

    def test_verify_heapam_options(self, dialect):
        _, params = verify_heapam(
            dialect, "t", on_error_stop=True, check_toast=True,
            skip=SkipMode.ALL_VISIBLE, start_block=3, end_block=4,
        ).to_sql()
        assert params == ("t", True, True, "all-visible", 3, 4)

    @pytest.mark.parametrize("value", [1, "true"])
    def test_heapallindexed_must_be_bool(self, dialect, value):
        with pytest.raises(TypeError):
            bt_index_check(dialect, "idx", heapallindexed=value)

    @pytest.mark.parametrize("value", [-1, True, "1"])
    def test_block_number_validation(self, dialect, value):
        with pytest.raises((TypeError, ValueError)):
            get_raw_page(dialect, "t", "main", value)


class TestCapabilityGates:
    @pytest.mark.parametrize(
        ("name", "feature", "version", "expected"),
        [
            ("amcheck", "integrity", "1.5", True),
            ("amcheck", "integrity", "0.9", False),
            ("pageinspect", "page", "1.13", True),
            ("pgstattuple", "tuple_stats", "1.5", True),
            ("pg_logicalinspect", "snapshot", "1.0", True),
        ],
    )
    def test_version_gates(self, name, feature, version, expected):
        dialect = PostgresDialect()
        dialect._extensions = {
            name: PostgresExtensionInfo(name=name, installed=True, available=True, version=version)
        }
        assert dialect.check_extension_feature(name, feature) is expected

    def test_not_installed(self):
        dialect = PostgresDialect()
        assert dialect.supports_amcheck() is False
        assert dialect.supports_pageinspect() is False
        assert dialect.supports_pgstattuple() is False
        assert dialect.supports_pg_logicalinspect() is False
