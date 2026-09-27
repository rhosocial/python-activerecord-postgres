# tests/rhosocial/activerecord_postgres_test/feature/backend/dialect/test_materialized_view.py
"""PostgreSQL materialized view DDL tests (SQL generation only, no database).

Covers the regression matrix documented in
``.claude/plan/2026-09-26/materialized-view-coverage.md``:

- G1: ``PostgresMaterializedViewMixin`` must precede the core ``ViewMixin`` in
  the MRO, otherwise the PostgreSQL CREATE formatter is dead code and
  ``storage_options`` is silently dropped.
- G2/G3: version gates and exception contract for ``REFRESH ... CONCURRENTLY``.
- G4: schema qualification and identifier quoting for all MV statements.
- G5: ``CREATE MATERIALIZED VIEW IF NOT EXISTS`` (PG 9.4+).
- G6: ``ALTER MATERIALIZED VIEW`` (RENAME / SET SCHEMA / SET() / RESET() / OWNER).
- G8: protocol and package surface.
"""

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins import ViewMixin
from rhosocial.activerecord.backend.expression import (
    Column,
    CreateMaterializedViewExpression,
    DropMaterializedViewExpression,
    QueryExpression,
    RefreshMaterializedViewExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl import (
    MaterializedViewAlterAction,
    PostgresAlterMaterializedViewExpression,
    PostgresChangeMaterializedViewOwnerAction,
    PostgresCreateMaterializedViewExpression,
    PostgresDropMaterializedViewExpression,
    PostgresRefreshMaterializedViewExpression,
    PostgresRenameMaterializedViewAction,
    PostgresResetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewPropertiesAction,
    PostgresSetMaterializedViewSchemaAction,
)
from rhosocial.activerecord.backend.impl.postgres.mixins.materialized_view import (
    PostgresMaterializedViewMixin,
)
from rhosocial.activerecord.backend.impl.postgres.protocols.materialized_view import (
    PostgresMaterializedViewSupport,
)
from rhosocial.activerecord.backend.impl.postgres.storage_parameters import (
    PostgresStorageParameter,
    PostgresStorageParameterValueType,
    resolve_storage_parameter,
    validate_storage_parameters,
)

PG_93 = (9, 3, 0)
PG_94 = (9, 4, 0)
PG_15 = (15, 0, 0)


def _dialect(version=PG_15):
    return PostgresDialect(version=version)


def _source_query(dialect):
    return QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "product_id")],
        from_=TableExpression(dialect, "sales"),
    )


def _create(dialect, **kwargs):
    kwargs.setdefault("view_name", "sales_summary")
    kwargs.setdefault("query", _source_query(dialect))
    return CreateMaterializedViewExpression(dialect=dialect, **kwargs)


class TestMaterializedViewMixinResolution:
    """Lock down mixin resolution so PG-specific MV formatting cannot go dead again."""

    def test_pg_mv_mixin_precedes_core_view_mixin(self):
        """T-01: PG MV mixin must come before the core ViewMixin."""
        mro = PostgresDialect.__mro__
        assert mro.index(PostgresMaterializedViewMixin) < mro.index(ViewMixin)

    def test_create_mv_formatter_owned_by_pg_mixin(self):
        """T-02: CREATE MATERIALIZED VIEW must be formatted by the PG mixin."""
        owner = _dialect().format_create_materialized_view_statement.__qualname__
        assert owner.startswith("PostgresMaterializedViewMixin.")

    def test_drop_mv_formatter_owned_by_pg_mixin(self):
        """T-03: the DROP path is PG-owned (schema aware) and keeps the core SQL shape."""
        dialect = _dialect()
        owner = dialect.format_drop_materialized_view_statement.__qualname__
        assert owner.startswith("PostgresMaterializedViewMixin.")
        sql, _ = DropMaterializedViewExpression(
            dialect=dialect, view_name="sales_summary", if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP MATERIALIZED VIEW IF EXISTS "sales_summary" CASCADE'

    @pytest.mark.parametrize(
        "capability,formatter",
        [
            ("supports_materialized_view_tablespace", "format_create_materialized_view_statement"),
            ("supports_materialized_view_storage_options", "format_create_materialized_view_statement"),
        ],
    )
    def test_pg_only_capability_has_pg_owned_formatter(self, capability, formatter):
        """T-04: a declared PG-only capability must have a PG-owned formatter behind it.

        This is the generic guard against "capability probe says True but the
        formatter never runs" (the exact shape of the G1 defect).
        """
        dialect = _dialect()
        assert getattr(dialect, capability)() is True
        owner = getattr(dialect, formatter).__qualname__
        assert owner.startswith("PostgresMaterializedViewMixin.")


class TestCreateMaterializedViewFormatting:
    """G1: PostgreSQL CREATE MATERIALIZED VIEW specifics."""

    def test_storage_options_rendered(self):
        """T-05: storage_options must appear in the generated SQL."""
        dialect = _dialect()
        sql, _ = _create(dialect, storage_options={"fillfactor": 70}).to_sql()
        assert sql == (
            'CREATE MATERIALIZED VIEW "sales_summary" WITH (fillfactor = 70) '
            'AS SELECT "product_id" FROM "sales" WITH DATA'
        )

    def test_multiple_storage_options_rendered(self):
        """T-06: every storage option is rendered, keys upper-cased."""
        dialect = _dialect()
        sql, _ = _create(
            dialect,
            storage_options={"fillfactor": 70, "autovacuum_enabled": "true"},
        ).to_sql()
        assert sql == (
            'CREATE MATERIALIZED VIEW "sales_summary" '
            'WITH (fillfactor = 70, autovacuum_enabled = true) '
            'AS SELECT "product_id" FROM "sales" WITH DATA'
        )

    def test_tablespace_column_aliases_with_data(self):
        """T-07: PG grammar order is column aliases -> WITH (...) -> TABLESPACE -> AS."""
        dialect = _dialect()
        sql, _ = _create(
            dialect,
            column_aliases=["product_id", "total_sales"],
            storage_options={"fillfactor": 90},
            tablespace="fast_ssd",
        ).to_sql()
        assert sql == (
            'CREATE MATERIALIZED VIEW "sales_summary" ("product_id", "total_sales") '
            'WITH (fillfactor = 90) TABLESPACE "fast_ssd" '
            'AS SELECT "product_id" FROM "sales" WITH DATA'
        )

    def test_with_no_data(self):
        """T-08: WITH NO DATA tail."""
        dialect = _dialect()
        sql, _ = _create(dialect, with_data=False).to_sql()
        assert sql.endswith("WITH NO DATA")

    def test_empty_storage_options_omitted(self):
        """T-09: no empty ``WITH ()`` is emitted when storage_options is empty."""
        dialect = _dialect()
        sql, _ = _create(dialect).to_sql()
        assert "WITH (" not in sql
        assert sql == (
            'CREATE MATERIALIZED VIEW "sales_summary" '
            'AS SELECT "product_id" FROM "sales" WITH DATA'
        )


class TestDropMaterializedViewFormatting:
    """G1: DROP MATERIALIZED VIEW keeps IF EXISTS / CASCADE."""

    def test_drop_plain(self):
        dialect = _dialect()
        sql, params = DropMaterializedViewExpression(
            dialect=dialect, view_name="sales_summary"
        ).to_sql()
        assert sql == 'DROP MATERIALIZED VIEW "sales_summary"'
        assert params == ()

    def test_drop_if_exists_cascade(self):
        dialect = _dialect()
        sql, _ = DropMaterializedViewExpression(
            dialect=dialect, view_name="sales_summary", if_exists=True, cascade=True
        ).to_sql()
        assert sql == 'DROP MATERIALIZED VIEW IF EXISTS "sales_summary" CASCADE'


class TestRefreshMaterializedViewFormatting:
    """G2: CONCURRENTLY must be gated on PostgreSQL 9.4+ on every code path."""

    def test_refresh_plain(self):
        dialect = _dialect()
        sql, params = RefreshMaterializedViewExpression(
            dialect=dialect, view_name="sales_summary"
        ).to_sql()
        assert sql == 'REFRESH MATERIALIZED VIEW "sales_summary"'
        assert params == ()

    @pytest.mark.parametrize("with_data,expected", [(True, " WITH DATA"), (False, " WITH NO DATA")])
    def test_refresh_with_data_variants(self, with_data, expected):
        dialect = _dialect()
        sql, _ = RefreshMaterializedViewExpression(
            dialect=dialect, view_name="sales_summary", with_data=with_data
        ).to_sql()
        assert sql == f'REFRESH MATERIALIZED VIEW "sales_summary"{expected}'

    def test_refresh_without_with_data_omits_clause(self):
        dialect = _dialect()
        sql, _ = RefreshMaterializedViewExpression(
            dialect=dialect, view_name="sales_summary"
        ).to_sql()
        assert "WITH DATA" not in sql

    @pytest.mark.parametrize("version", [PG_94, (10, 0, 0), PG_15, (17, 0, 0)])
    def test_refresh_concurrently_allowed_since_94(self, version):
        """T-12: CONCURRENTLY is emitted from PG 9.4 onwards."""
        dialect = _dialect(version)
        sql, _ = RefreshMaterializedViewExpression(
            dialect=dialect, view_name="sales_summary", concurrent=True
        ).to_sql()
        assert sql == 'REFRESH MATERIALIZED VIEW CONCURRENTLY "sales_summary"'

    def test_refresh_concurrently_rejected_before_94(self):
        """T-13: the generic expression must refuse CONCURRENTLY before PG 9.4."""
        dialect = _dialect(PG_93)
        with pytest.raises(UnsupportedFeatureError) as exc:
            RefreshMaterializedViewExpression(
                dialect=dialect, view_name="sales_summary", concurrent=True
            ).to_sql()
        assert "CONCURRENTLY" in str(exc.value)

    def test_pg_expression_shares_same_gate(self):
        """T-15: the PostgreSQL expression is gated identically."""
        dialect = _dialect(PG_93)
        with pytest.raises(UnsupportedFeatureError):
            PostgresRefreshMaterializedViewExpression(
                dialect=dialect, name="sales_summary", concurrently=True
            ).to_sql()

    def test_pg_expression_renders_concurrently_since_94(self):
        dialect = _dialect()
        sql, _ = PostgresRefreshMaterializedViewExpression(
            dialect=dialect, name="sales_summary", concurrently=True, with_data=True
        ).to_sql()
        assert sql == 'REFRESH MATERIALIZED VIEW CONCURRENTLY "sales_summary" WITH DATA'

    @pytest.mark.parametrize(
        "version,feature,should_raise",
        [
            (PG_93, "concurrent", True),
            (PG_93, "if_not_exists", True),
            (PG_93, "tablespace", False),
            (PG_93, "storage_options", False),
            (PG_94, "concurrent", False),
            (PG_94, "if_not_exists", False),
            ((9, 2, 0), "concurrent", True),
            ((9, 2, 0), "if_not_exists", True),
            ((9, 2, 0), "tablespace", True),
            ((9, 2, 0), "storage_options", True),
        ],
    )
    def test_version_gate_matrix(self, version, feature, should_raise):
        """T-16: version gate matrix across MV features."""
        dialect = _dialect(version)
        build = {
            "concurrent": lambda: RefreshMaterializedViewExpression(
                dialect=dialect, view_name="mv", concurrent=True
            ),
            "if_not_exists": lambda: PostgresCreateMaterializedViewExpression(
                dialect=dialect,
                view_name="mv",
                query=_source_query(dialect),
                if_not_exists=True,
            ),
            "tablespace": lambda: PostgresCreateMaterializedViewExpression(
                dialect=dialect,
                view_name="mv",
                query=_source_query(dialect),
                tablespace="fast_ssd",
            ),
            "storage_options": lambda: PostgresCreateMaterializedViewExpression(
                dialect=dialect,
                view_name="mv",
                query=_source_query(dialect),
                storage_options={"fillfactor": 70},
            ),
        }[feature]
        if should_raise:
            with pytest.raises(UnsupportedFeatureError):
                build().to_sql()
        else:
            sql, _ = build().to_sql()
            assert sql.startswith("CREATE MATERIALIZED VIEW") or sql.startswith(
                "REFRESH MATERIALIZED VIEW"
            )


class TestMaterializedViewSchemaQualification:
    """G4: MV names must be schema-qualified and quoted."""

    def test_refresh_schema_qualified_and_quoted(self):
        """T-17."""
        dialect = _dialect()
        sql, _ = PostgresRefreshMaterializedViewExpression(
            dialect=dialect, name="mv_sales", schema="reporting"
        ).to_sql()
        assert sql == 'REFRESH MATERIALIZED VIEW "reporting"."mv_sales"'

    def test_mixed_case_and_reserved_word_quoted(self):
        """T-18: the previous bare f-string concatenation broke here."""
        dialect = _dialect()
        sql, _ = PostgresRefreshMaterializedViewExpression(
            dialect=dialect, name="Order Summary", schema="Reporting"
        ).to_sql()
        assert sql == 'REFRESH MATERIALIZED VIEW "Reporting"."Order Summary"'

    def test_create_mv_schema_qualified(self):
        """T-19."""
        dialect = _dialect()
        sql, _ = PostgresCreateMaterializedViewExpression(
            dialect=dialect,
            view_name="mv_sales",
            query=_source_query(dialect),
            schema="reporting",
        ).to_sql()
        assert sql.startswith('CREATE MATERIALIZED VIEW "reporting"."mv_sales" ')

    def test_drop_mv_schema_qualified(self):
        """T-20."""
        dialect = _dialect()
        sql, _ = PostgresDropMaterializedViewExpression(
            dialect=dialect, view_name="mv_sales", schema="reporting", if_exists=True
        ).to_sql()
        assert sql == 'DROP MATERIALIZED VIEW IF EXISTS "reporting"."mv_sales"'

    def test_no_schema_emits_bare_identifier(self):
        """T-21: no stray separator when schema is absent."""
        dialect = _dialect()
        sql, _ = PostgresDropMaterializedViewExpression(
            dialect=dialect, view_name="mv_sales"
        ).to_sql()
        assert sql == 'DROP MATERIALIZED VIEW "mv_sales"'
        assert "." not in sql

    def test_rejects_empty_names(self):
        dialect = _dialect()
        with pytest.raises(ValueError):
            PostgresCreateMaterializedViewExpression(
                dialect=dialect, view_name="  ", query=_source_query(dialect)
            )
        with pytest.raises(ValueError):
            PostgresCreateMaterializedViewExpression(
                dialect=dialect, view_name="mv", query=_source_query(dialect), schema=""
            )


class TestCreateMaterializedViewIfNotExists:
    """G5: CREATE MATERIALIZED VIEW IF NOT EXISTS (PG 9.4+)."""

    def test_if_not_exists_rendered(self):
        """T-22."""
        dialect = _dialect()
        sql, _ = PostgresCreateMaterializedViewExpression(
            dialect=dialect,
            view_name="sales_summary",
            query=_source_query(dialect),
            if_not_exists=True,
        ).to_sql()
        assert sql == (
            'CREATE MATERIALIZED VIEW IF NOT EXISTS "sales_summary" '
            'AS SELECT "product_id" FROM "sales" WITH DATA'
        )

    def test_if_not_exists_false_omitted(self):
        """T-23."""
        dialect = _dialect()
        sql, _ = PostgresCreateMaterializedViewExpression(
            dialect=dialect, view_name="sales_summary", query=_source_query(dialect)
        ).to_sql()
        assert "IF NOT EXISTS" not in sql

    def test_if_not_exists_rejected_before_94(self):
        """T-24."""
        dialect = _dialect(PG_93)
        with pytest.raises(UnsupportedFeatureError):
            PostgresCreateMaterializedViewExpression(
                dialect=dialect,
                view_name="sales_summary",
                query=_source_query(dialect),
                if_not_exists=True,
            ).to_sql()


class TestMaterializedViewExceptionContract:
    """G3: MV feature gates raise UnsupportedFeatureError, never bare ValueError."""

    @pytest.mark.parametrize(
        "build",
        [
            lambda dialect: RefreshMaterializedViewExpression(
                dialect=dialect, view_name="mv", concurrent=True
            ),
            lambda dialect: PostgresRefreshMaterializedViewExpression(
                dialect=dialect, name="mv", concurrently=True
            ),
            lambda dialect: PostgresCreateMaterializedViewExpression(
                dialect=dialect, view_name="mv", query=_source_query(dialect), if_not_exists=True
            ),
        ],
    )
    def test_all_mv_gates_raise_unsupported_feature_error(self, build):
        """T-25."""
        dialect = _dialect(PG_93)
        with pytest.raises(UnsupportedFeatureError) as exc:
            build(dialect).to_sql()
        assert exc.value.dialect_name == dialect.name
        assert exc.value.feature_name

    def test_error_message_mentions_dialect_and_feature(self):
        """T-26."""
        dialect = _dialect(PG_93)
        with pytest.raises(UnsupportedFeatureError) as exc:
            RefreshMaterializedViewExpression(
                dialect=dialect, view_name="mv", concurrent=True
            ).to_sql()
        message = str(exc.value)
        assert dialect.name in message
        assert "CONCURRENTLY" in message

    def test_unsupported_feature_error_is_not_value_error(self):
        """T-27: guards the core invariant so MV gates cannot regress to ValueError."""
        assert not issubclass(UnsupportedFeatureError, ValueError)


class TestAlterMaterializedView:
    """G6: ALTER MATERIALIZED VIEW actions."""

    def _alter(self, dialect, *actions, **kwargs):
        return PostgresAlterMaterializedViewExpression(
            dialect=dialect, view_name=kwargs.pop("view_name", "sales_summary"),
            actions=list(actions), **kwargs
        )

    def test_rename_to(self):
        """T-28."""
        dialect = _dialect()
        sql, params = self._alter(
            dialect, PostgresRenameMaterializedViewAction(dialect, "sales_summary_v2")
        ).to_sql()
        assert sql == 'ALTER MATERIALIZED VIEW "sales_summary" RENAME TO "sales_summary_v2"'
        assert params == ()

    def test_set_schema(self):
        """T-29."""
        dialect = _dialect()
        sql, _ = self._alter(
            dialect, PostgresSetMaterializedViewSchemaAction(dialect, "archive")
        ).to_sql()
        assert sql == 'ALTER MATERIALIZED VIEW "sales_summary" SET SCHEMA "archive"'

    def test_set_properties(self):
        """T-30."""
        dialect = _dialect()
        sql, _ = self._alter(
            dialect,
            PostgresSetMaterializedViewPropertiesAction(
                dialect, {"fillfactor": 90, "autovacuum_enabled": "true"}
            ),
        ).to_sql()
        assert sql == (
            'ALTER MATERIALIZED VIEW "sales_summary" '
            "SET (fillfactor = 90, autovacuum_enabled = true)"
        )

    def test_reset_parameters(self):
        """T-31."""
        dialect = _dialect()
        sql, _ = self._alter(
            dialect,
            PostgresResetMaterializedViewPropertiesAction(
                dialect, ["fillfactor", "autovacuum_enabled"]
            ),
        ).to_sql()
        assert sql == (
            'ALTER MATERIALIZED VIEW "sales_summary" RESET (fillfactor, autovacuum_enabled)'
        )

    def test_owner_to(self):
        """T-32: role names are quoted, CURRENT_* keywords are not."""
        dialect = _dialect()
        sql, _ = self._alter(
            dialect, PostgresChangeMaterializedViewOwnerAction(dialect, "app_owner")
        ).to_sql()
        assert sql == 'ALTER MATERIALIZED VIEW "sales_summary" OWNER TO "app_owner"'

        for keyword in ("CURRENT_USER", "CURRENT_ROLE", "SESSION_USER", "current_user"):
            sql, _ = self._alter(
                dialect, PostgresChangeMaterializedViewOwnerAction(dialect, keyword)
            ).to_sql()
            assert sql.endswith(f"OWNER TO {keyword.upper()}")

    def test_multiple_actions(self):
        """T-33."""
        dialect = _dialect()
        sql, _ = self._alter(
            dialect,
            PostgresRenameMaterializedViewAction(dialect, "sales_summary_v2"),
            PostgresSetMaterializedViewPropertiesAction(dialect, {"fillfactor": 90}),
        ).to_sql()
        assert sql == (
            'ALTER MATERIALIZED VIEW "sales_summary" RENAME TO "sales_summary_v2";\n'
            'ALTER MATERIALIZED VIEW "sales_summary" SET (fillfactor = 90)'
        )

    def test_alter_target_schema_qualified(self):
        """T-34."""
        dialect = _dialect()
        sql, _ = self._alter(
            dialect,
            PostgresSetMaterializedViewSchemaAction(dialect, "archive"),
            view_name="mv_sales",
            schema="reporting",
        ).to_sql()
        assert sql == (
            'ALTER MATERIALIZED VIEW "reporting"."mv_sales" SET SCHEMA "archive"'
        )

    def test_set_tablespace_not_offered(self):
        """T-35: PostgreSQL cannot relocate a MV to another tablespace."""
        import rhosocial.activerecord.backend.impl.postgres.expression.ddl.mv as mv_module

        assert not hasattr(mv_module, "PostgresSetMaterializedViewTablespaceAction")
        assert not any(
            "TABLESPACE" in (action.__doc__ or "")
            for action in (
                PostgresRenameMaterializedViewAction,
                PostgresSetMaterializedViewSchemaAction,
                PostgresSetMaterializedViewPropertiesAction,
                PostgresResetMaterializedViewPropertiesAction,
                PostgresChangeMaterializedViewOwnerAction,
            )
        )

    def test_requires_at_least_one_action(self):
        dialect = _dialect()
        with pytest.raises(ValueError):
            PostgresAlterMaterializedViewExpression(
                dialect=dialect, view_name="mv", actions=[]
            )

    def test_rejects_non_action_objects(self):
        dialect = _dialect()
        with pytest.raises(TypeError):
            PostgresAlterMaterializedViewExpression(
                dialect=dialect, view_name="mv", actions=[object()]
            )

    def test_rejects_unlisted_storage_parameters(self):
        """Names must exist in PostgresStorageParameter; no pattern matching."""
        dialect = _dialect()
        for bogus in ("nonexistent_opt", "fillfactor = 70) --", "toast.autovacuum_enabled"):
            with pytest.raises(ValueError) as exc:
                PostgresSetMaterializedViewPropertiesAction(dialect, {bogus: 1})
            assert "unknown PostgreSQL storage parameter" in str(exc.value)
            with pytest.raises(ValueError):
                PostgresResetMaterializedViewPropertiesAction(dialect, [bogus])
            with pytest.raises(ValueError):
                PostgresCreateMaterializedViewExpression(
                    dialect=dialect,
                    view_name="mv",
                    query=_source_query(dialect),
                    storage_options={bogus: 1},
                )

    def test_allows_unlisted_storage_parameters_explicitly(self):
        """Escape hatch for table access method options and toast.* names."""
        dialect = _dialect()
        expression = PostgresCreateMaterializedViewExpression(
            dialect=dialect,
            view_name="mv",
            query=_source_query(dialect),
            storage_options={"compresslevel": 4},
            allow_unlisted_storage_parameters=True,
        )
        sql, _ = expression.to_sql()
        assert "WITH (compresslevel = 4)" in sql

    def test_rejects_unknown_action(self):
        dialect = _dialect()

        class RogueAction(MaterializedViewAlterAction):
            action_kind = "rogue"

        with pytest.raises(UnsupportedFeatureError):
            dialect.format_materialized_view_alter_action(RogueAction(dialect))


class TestPostgresStorageParameterEnum:
    """Storage parameter names are enumerated, never pattern matched."""

    def test_enum_covers_heap_relation_options(self):
        """The enum is the authoritative allow-list for MV reloptions."""
        values = {member.value for member in PostgresStorageParameter}
        assert len(values) == len(list(PostgresStorageParameter))
        for expected in (
            "fillfactor",
            "toast_tuple_target",
            "parallel_workers",
            "user_catalog_table",
            "vacuum_index_cleanup",
            "vacuum_truncate",
            "autovacuum_enabled",
            "autovacuum_vacuum_threshold",
            "autovacuum_analyze_threshold",
            "autovacuum_freeze_min_age",
            "autovacuum_multixact_freeze_table_age",
            "log_autovacuum_min_duration",
        ):
            assert expected in values

    def test_lookup_by_name_and_member(self):
        assert resolve_storage_parameter("fillfactor") is PostgresStorageParameter.FILLFACTOR
        assert (
            resolve_storage_parameter(PostgresStorageParameter.FILLFACTOR)
            is PostgresStorageParameter.FILLFACTOR
        )
        assert resolve_storage_parameter("toast_tuple_target") is (
            PostgresStorageParameter.TOAST_TUPLE_TARGET
        )

    def test_lookup_rejects_unknown(self):
        assert resolve_storage_parameter("fillfactor ") is None
        assert resolve_storage_parameter(None) is None
        assert resolve_storage_parameter(70) is None

    def test_validate_accepts_mixed_forms(self):
        resolved = validate_storage_parameters(
            ["fillfactor", PostgresStorageParameter.AUTOVACUUM_ENABLED],
            "properties",
        )
        assert resolved == (
            PostgresStorageParameter.FILLFACTOR,
            PostgresStorageParameter.AUTOVACUUM_ENABLED,
        )

    def test_validate_rejects_unknown_by_default(self):
        with pytest.raises(ValueError):
            validate_storage_parameters(["fillfactor", "compresslevel"], "properties")

    def test_validate_can_allow_unlisted(self):
        assert validate_storage_parameters(
            ["compresslevel"], "properties", allow_unlisted=True
        ) == ()

    def test_validate_rejects_non_iterable(self):
        with pytest.raises(TypeError):
            validate_storage_parameters("fillfactor", "properties")

    def test_value_types_are_declared(self):
        assert (
            PostgresStorageParameter.FILLFACTOR.value_type
            is PostgresStorageParameterValueType.INT
        )
        assert (
            PostgresStorageParameter.AUTOVACUUM_VACUUM_SCALE_FACTOR.value_type
            is PostgresStorageParameterValueType.REAL
        )
        assert (
            PostgresStorageParameter.AUTOVACUUM_ENABLED.value_type
            is PostgresStorageParameterValueType.TERNARY
        )
        assert (
            PostgresStorageParameter.VACUUM_INDEX_CLEANUP.enum_values
            == ("auto", "enabled", "disabled")
        )
        assert PostgresStorageParameter.FILLFACTOR.enum_values is None

    def test_min_version_metadata(self):
        assert PostgresStorageParameter.PARALLEL_WORKERS.min_version == (11, 0, 0)
        assert PostgresStorageParameter.AUTOVACUUM_VACUUM_INSERT_THRESHOLD.min_version == (13, 0, 0)
        assert PostgresStorageParameter.LOG_AUTOANALYZE_MIN_DURATION.min_version == (15, 0, 0)
        assert PostgresStorageParameter.AUTOVACUUM_PARALLEL_WORKERS.min_version == (16, 0, 0)
        assert PostgresStorageParameter.FILLFACTOR.min_version is None

    def test_enum_exported_from_package_root(self):
        from rhosocial.activerecord.backend.impl import postgres as pg

        assert pg.PostgresStorageParameter is PostgresStorageParameter
        assert "PostgresStorageParameter" in pg.__all__

    def test_expressions_render_enum_members(self):
        """Enum members and their string values render identically."""
        dialect = _dialect()
        by_member = PostgresSetMaterializedViewPropertiesAction(
            dialect, {PostgresStorageParameter.FILLFACTOR: 70}
        )
        by_name = PostgresSetMaterializedViewPropertiesAction(dialect, {"fillfactor": 70})
        assert by_member.to_sql() == by_name.to_sql()
        assert "fillfactor = 70" in by_member.to_sql()


class TestMaterializedViewProtocolAndSurface:
    """G8: protocol and package surface stay in sync."""

    def test_protocol_isinstance(self):
        """T-36."""
        assert isinstance(_dialect(), PostgresMaterializedViewSupport)

    def test_protocol_declares_all_formatters(self):
        """T-37: the protocol must cover every MV formatter the mixin exposes."""
        for name in (
            "supports_materialized_view",
            "supports_materialized_view_concurrent_refresh",
            "supports_materialized_view_if_not_exists",
            "supports_materialized_view_tablespace",
            "supports_materialized_view_storage_options",
            "supports_alter_materialized_view",
            "format_create_materialized_view_statement",
            "format_drop_materialized_view_statement",
            "format_refresh_materialized_view_statement",
            "format_refresh_materialized_view_pg_statement",
            "format_alter_materialized_view_statement",
            "format_materialized_view_alter_action",
        ):
            assert hasattr(_dialect(), name), name
            assert hasattr(PostgresMaterializedViewSupport, name), name

    def test_new_expressions_exported(self):
        """T-38."""
        from rhosocial.activerecord.backend.impl.postgres import expression as pg_expression

        for name in (
            "PostgresCreateMaterializedViewExpression",
            "PostgresDropMaterializedViewExpression",
            "PostgresRefreshMaterializedViewExpression",
            "PostgresAlterMaterializedViewExpression",
            "PostgresRenameMaterializedViewAction",
            "PostgresSetMaterializedViewSchemaAction",
            "PostgresSetMaterializedViewPropertiesAction",
            "PostgresResetMaterializedViewPropertiesAction",
            "PostgresChangeMaterializedViewOwnerAction",
            "MaterializedViewAlterAction",
        ):
            assert name in pg_expression.__all__, name
            assert hasattr(pg_expression, name), name

    def test_format_method_targets_exist(self):
        """T-39: every MV expression routes to a formatter the dialect implements."""
        dialect = _dialect()
        query = _source_query(dialect)
        expressions = [
            _create(dialect),
            PostgresCreateMaterializedViewExpression(
                dialect=dialect, view_name="mv", query=query
            ),
            DropMaterializedViewExpression(dialect=dialect, view_name="mv"),
            PostgresDropMaterializedViewExpression(dialect=dialect, view_name="mv"),
            RefreshMaterializedViewExpression(dialect=dialect, view_name="mv"),
            PostgresRefreshMaterializedViewExpression(dialect=dialect, name="mv"),
            PostgresAlterMaterializedViewExpression(
                dialect=dialect,
                view_name="mv",
                actions=[PostgresRenameMaterializedViewAction(dialect, "mv2")],
            ),
        ]
        for expression in expressions:
            formatter = getattr(dialect, expression.format_method, None)
            assert callable(formatter), f"{type(expression).__name__} -> {expression.format_method}"

    def test_legacy_aliases_preserved(self):
        """T-40: examples rely on the name=/concurrently= aliases."""
        dialect = _dialect()
        expression = PostgresRefreshMaterializedViewExpression(
            dialect=dialect, name="sales_summary", concurrently=True
        )
        assert expression.name == expression.view_name == "sales_summary"
        assert expression.concurrently is True
        assert expression.concurrent is True

    @pytest.mark.parametrize(
        "version,expected",
        [
            ((9, 2, 0), False),
            (PG_93, True),
            (PG_15, True),
        ],
    )
    def test_base_capability_probe_respects_version(self, version, expected):
        """T-41a: materialized views themselves require PostgreSQL 9.3+."""
        assert _dialect(version).supports_materialized_view() is expected

    @pytest.mark.parametrize(
        "version,expected", [(PG_93, False), (PG_94, True), (PG_15, True)]
    )
    def test_concurrent_refresh_probe_matches_version(self, version, expected):
        """T-41b."""
        assert _dialect(version).supports_materialized_view_concurrent_refresh() is expected

    def test_pg_only_capability_has_pg_owned_formatter_for_refresh(self):
        """T-04 (refresh half): CONCURRENTLY gating lives in the PG mixin."""
        dialect = _dialect()
        assert dialect.supports_materialized_view_concurrent_refresh() is True
        owner = dialect.format_refresh_materialized_view_statement.__qualname__
        assert owner.startswith("PostgresMaterializedViewMixin.")
