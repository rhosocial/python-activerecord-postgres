# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/introspection/test_introspection_coverage_families.py  # noqa: E501
"""Offline tests for the catalogue families added for introspection coverage.

The statements are rendered trees and the parsers are pure Python, so
everything here runs without a PostgreSQL server. What is pinned is the two
halves that used to be hand-written SQL: the expression each family renders,
and the record each row becomes.
"""

from types import SimpleNamespace
from typing import List

import pytest

from rhosocial.activerecord.backend.introspection.types import (
    ConstraintState,
    DerivedObjectState,
    IndexState,
    ItemKind,
    PartitionScheme,
    SequenceState,
    TableState,
)
from rhosocial.activerecord.backend.impl.postgres.cli.introspect import (
    INTROSPECT_TYPES,
    _NAME_RULE,
    _TYPE_CAPABILITIES,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.introspection import (
    PostgresConstraintStateQueryStatement,
    PostgresIndexStateQueryStatement,
    PostgresOperatorClassListQueryStatement,
    PostgresOperatorFamilyListQueryStatement,
    PostgresOperatorListQueryStatement,
    PostgresPartitionListQueryStatement,
    PostgresRoutineListQueryStatement,
    PostgresSchemaListQueryStatement,
    PostgresSequenceInfoQueryStatement,
    PostgresSequenceListQueryStatement,
    PostgresSequenceStateQueryStatement,
    PostgresTableStateQueryStatement,
    PostgresTypeListQueryStatement,
)
from rhosocial.activerecord.backend.impl.postgres.introspection.introspector import (
    PostgreSQLIntrospector,
    PostgreSQLIntrospectorMixin,
)
from rhosocial.activerecord.backend.impl.postgres.introspection.types import (
    PostgresOperatorClassInfo,
    PostgresOperatorFamilyInfo,
    PostgresOperatorInfo,
    PostgresPartitionBound,
    PostgresPartitionStrategy,
    PostgresRoutineInfo,
    PostgresRoutineKind,
    PostgresTypeInfo,
    PostgresTypeKind,
    SequenceInfo,
)


def _dialect(version=(17, 0, 0)):
    return PostgresDialect(version=version)


class _FakeBackend:
    def __init__(self, dialect, database="app"):
        self.dialect = dialect
        self._cached_version = None
        self.config = SimpleNamespace(
            database=database,
            default_schema="public",
            search_path=None,
        )

    def get_cached_server_version(self):
        return self._cached_version


class _ParsingIntrospector(PostgreSQLIntrospectorMixin):
    """An introspector with no executor: parsing never asks for one."""

    def __init__(self, backend):
        self._backend = backend


def _parser(version=(17, 0, 0)):
    return _ParsingIntrospector(_FakeBackend(_dialect(version)))


class _ScriptedExecutor:
    """An executor that answers whichever statement's marker matches."""

    def __init__(self, script):
        self.script = script
        self.seen: List[str] = []

    def execute(self, sql, params):
        self.seen.append(sql)
        matches = [
            (marker, rows) for marker, rows in self.script if marker in sql
        ]
        if matches:
            # Several catalog queries share a relation name; the longest
            # marker is the one that names the family, not a join it happens
            # to contain.
            return max(matches, key=lambda match: len(match[0]))[1]
        return []


def _scripted(script, version=(17, 0, 0)):
    backend = _FakeBackend(_dialect(version))
    return PostgreSQLIntrospector(backend, _ScriptedExecutor(script))


def _table_row(name, table_type):
    return {
        "table_name": name,
        "table_type": table_type,
        "comment": None,
        "row_count": 0,
        "live_row_count": 0,
        "size_bytes": 0,
        "data_size_bytes": 0,
        "index_size_bytes": 0,
    }


class TestSequenceSource:
    """I-01: the object list is pg_sequences, not only the standard view."""

    def test_list_reads_pg_sequences_both_as_list_and_state(self):
        sql, params = PostgresSequenceListQueryStatement(
            _dialect(), schema="public", include_pg_sequences=True
        ).to_sql()
        assert "FROM pg_sequences AS p" in sql
        assert "information_schema.sequences" not in sql
        assert "p.schemaname AS \"schema_name\"" in sql
        assert "p.sequencename AS \"sequence_name\"" in sql
        assert "p.cycle AS \"cycle\"" in sql
        assert params == ("public",)

    def test_info_narrows_to_one_sequence(self):
        sql, params = PostgresSequenceInfoQueryStatement(
            _dialect(), "order_id_seq", schema="public", include_pg_sequences=True
        ).to_sql()
        assert "p.sequencename = %s" in sql
        assert params == ("public", "order_id_seq")

    def test_the_narrow_shape_is_the_standard_view(self):
        sql, params = PostgresSequenceListQueryStatement(
            _dialect((9, 6, 0)), schema="public", include_pg_sequences=False
        ).to_sql()
        assert "FROM information_schema.sequences AS s" in sql
        assert "s.cycle_option AS \"cycle\"" in sql
        assert "s.start_value::BIGINT" in sql
        assert params == ("public",)

    @pytest.mark.parametrize(
        "cycle,expected",
        [(True, True), (False, False), ("YES", True), ("NO", False), (None, False)],
    )
    def test_cycle_accepts_both_catalog_spellings(self, cycle, expected):
        parser = _parser()
        info = parser.parse_sequence(
            {
                "sequence_name": "seq_demo",
                "schema_name": "public",
                "cycle": cycle,
            },
            "public",
        )
        assert isinstance(info, SequenceInfo)
        assert info.cycle is expected


class TestColumnIdentity:
    """I-02: identity and SERIAL both answer is_auto_increment."""

    @staticmethod
    def _row(**overrides):
        row = {
            "column_name": "id",
            "ordinal_position": 1,
            "data_type": "integer",
            "is_nullable": False,
            "default_value": None,
            "is_primary_key": True,
            "comment": None,
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
            "datetime_precision": None,
            "identity_kind": "",
            "generated_kind": "",
        }
        row.update(overrides)
        return row

    def _parse(self, rows):
        return _parser().parse_columns(rows, "orders", "public")[0]

    def test_the_statement_selects_identity_and_generated_flags(self):
        from rhosocial.activerecord.backend.expression.introspection import (
            ColumnInfoQueryStatement,
        )

        sql, _ = ColumnInfoQueryStatement(
            _dialect(), table_name="orders", schema="public"
        ).to_sql()
        assert 'a.attidentity AS "identity_kind"' in sql
        assert 'a.attgenerated AS "generated_kind"' in sql

    def test_pre_10_statement_omits_the_identity_column(self):
        from rhosocial.activerecord.backend.expression.introspection import (
            ColumnInfoQueryStatement,
        )

        sql, _ = ColumnInfoQueryStatement(
            _dialect((9, 6, 0)), table_name="orders", schema="public"
        ).to_sql()
        assert "attidentity" not in sql
        assert "attgenerated" not in sql

    def test_pre_12_statement_keeps_identity_but_omits_generated(self):
        from rhosocial.activerecord.backend.expression.introspection import (
            ColumnInfoQueryStatement,
        )

        sql, _ = ColumnInfoQueryStatement(
            _dialect((11, 0, 0)), table_name="orders", schema="public"
        ).to_sql()
        assert 'a.attidentity AS "identity_kind"' in sql
        assert "attgenerated" not in sql

    @pytest.mark.parametrize("identity", ["a", "d"])
    def test_identity_columns_are_auto_increment(self, identity):
        column = self._parse([self._row(identity_kind=identity)])
        assert column.is_auto_increment is True

    def test_serial_nextval_default_is_auto_increment(self):
        column = self._parse(
            [self._row(default_value="nextval('orders_id_seq'::regclass)")]
        )
        assert column.is_auto_increment is True

    def test_plain_default_is_not_auto_increment(self):
        column = self._parse([self._row(default_value="42")])
        assert column.is_auto_increment is False

    def test_generated_column_reports_its_expression(self):
        column = self._parse(
            [self._row(generated_kind="s", default_value="(amount * 2)")]
        )
        assert column.is_generated is True
        assert column.generated_expression == "(amount * 2)"


class TestPartitions:
    """I-04/I-10: the child partitions are a family of their own."""

    def test_statement_reads_the_key_and_the_children(self):
        sql, params = PostgresPartitionListQueryStatement(
            _dialect(), "orders", schema="public"
        ).to_sql()
        assert "PG_GET_PARTKEYDEF(pc.oid) AS \"partition_key\"" in sql
        assert "pg_inherits AS i" in sql
        assert 'ch.relname AS "name"' in sql
        assert 'PG_GET_EXPR(ch.relpartbound, ch.oid) AS "bound"' in sql
        assert "PG_TOTAL_RELATION_SIZE(ch.oid) AS \"size_bytes\"" in sql
        assert "pc.relkind = 'p'" in sql
        assert params == ("orders", "public")

    def test_statistics_can_be_left_out(self):
        sql, _ = PostgresPartitionListQueryStatement(
            _dialect(), "orders", schema="public", include_statistics=False
        ).to_sql()
        assert "pg_total_relation_size" not in sql.lower()
        assert 'ch.reltuples' not in sql

    @pytest.mark.parametrize(
        "key,strategy,columns",
        [
            ("RANGE (created_at)", PostgresPartitionStrategy.RANGE, ["created_at"]),
            (
                "RANGE (tenant_id, created_at)",
                PostgresPartitionStrategy.RANGE,
                ["tenant_id", "created_at"],
            ),
            ("LIST (status)", PostgresPartitionStrategy.LIST, ["status"]),
            ("HASH (user_id)", PostgresPartitionStrategy.HASH, ["user_id"]),
        ],
    )
    def test_key_text_splits_into_strategy_and_columns(self, key, strategy, columns):
        parsed = _parser()._parse_partition_key(key)
        assert parsed[0] is strategy
        assert parsed[1] == columns
        assert parsed[2] is None

    def test_key_expression_stays_an_expression(self):
        parsed = _parser()._parse_partition_key("RANGE (date_trunc('month', created_at))")
        assert parsed[0] is PostgresPartitionStrategy.RANGE
        assert parsed[1] == []
        assert "date_trunc" in parsed[2]

    @pytest.mark.parametrize(
        "text,bound",
        [
            (
                "FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')",
                PostgresPartitionBound(lower="2026-11-01", upper="2026-12-01"),
            ),
            (
                "FOR VALUES IN ('active', 'pending')",
                PostgresPartitionBound(values=["active", "pending"]),
            ),
            (
                "FOR VALUES WITH (MODULUS 4, REMAINDER 1)",
                PostgresPartitionBound(modulus=4, remainder=1),
            ),
            ("DEFAULT", PostgresPartitionBound()),
        ],
    )
    def test_bounds_decode_to_their_shape(self, text, bound):
        parsed = _parser()._parse_partition_bound(text)
        assert parsed is not None
        assert parsed.lower == bound.lower
        assert parsed.upper == bound.upper
        assert parsed.values == bound.values
        assert parsed.modulus == bound.modulus
        assert parsed.remainder == bound.remainder

    def test_rows_become_one_neutral_state(self):
        rows = [
            {
                "parent_table": "orders",
                "partition_key": "RANGE (created_at)",
                "name": "orders_p2026_11",
                "bound": "FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')",
                "comment": None,
                "row_count": 10,
                "size_bytes": 8192,
            },
            {
                "parent_table": "orders",
                "partition_key": "RANGE (created_at)",
                "name": "orders_p2026_12",
                "bound": "FOR VALUES FROM ('2026-12-01') TO ('2027-01-01')",
                "comment": None,
                "row_count": -1,
                "size_bytes": 4096,
            },
        ]
        state = _parser().parse_partition_state(rows, "orders", "public")
        assert state is not None
        assert state.scheme is PartitionScheme.RANGE
        assert state.key_columns == ("created_at",)
        assert [bound.name for bound in state.bounds] == [
            "orders_p2026_11",
            "orders_p2026_12",
        ]
        assert state.bounds[0].upper == "2026-12-01"
        assert state.bounds[0].row_count == 10
        assert state.bounds[1].row_count is None
        # Lower and hash facts have no slot on the neutral bound; they stay
        # structured beside the engine's own text.
        assert state.raw["partitions"][0]["lower"] == "2026-11-01"

    def test_a_default_partition_is_named_as_such(self):
        rows = [
            {
                "parent_table": "orders",
                "partition_key": "LIST (status)",
                "name": "orders_default",
                "bound": "DEFAULT",
            }
        ]
        state = _parser().parse_partition_state(rows, "orders", "public")
        assert state is not None
        assert state.has_default is True
        assert state.bounds[0].is_default is True

    def test_a_parent_with_no_child_still_answers_its_scheme(self):
        rows = [
            {
                "parent_table": "orders",
                "partition_key": "RANGE (created_at)",
                "name": None,
            }
        ]
        state = _parser().parse_partition_state(rows, "orders", "public")
        assert state is not None
        assert state.scheme is PartitionScheme.RANGE
        assert state.bounds == ()


class TestNeutralObjectState:
    """The neutral object-state families, rendered and parsed."""

    def test_table_state_statement_reads_kind_and_storage(self):
        sql, params = PostgresTableStateQueryStatement(
            _dialect(), "orders", schema="public"
        ).to_sql()
        assert '"relpersistence"' in sql
        assert '"reloptions"' in sql
        assert "pg_tablespace AS ts" in sql
        assert "pg_am AS am" in sql
        assert params == ("public", "orders")

    def test_table_state_is_regular_unlogged_or_partitioned(self):
        parser = _parser()
        regular = parser.parse_table_state(
            [{"table_name": "orders", "relkind": "r", "relpersistence": "p"}],
            "orders",
            "public",
        )
        assert isinstance(regular, TableState)
        assert regular.kind == "regular"
        assert regular.traits == ()

        unlogged = parser.parse_table_state(
            [
                {
                    "table_name": "cache",
                    "relkind": "r",
                    "relpersistence": "u",
                    "reloptions": ["fillfactor=70"],
                    "tablespace": "fast",
                    "access_method": "heap",
                    "comment": "scratch",
                }
            ],
            "cache",
            "public",
        )
        assert unlogged is not None
        assert unlogged.kind == "unlogged"
        assert unlogged.storage.tablespace == "fast"
        assert unlogged.storage.engine == "heap"
        assert unlogged.storage.raw["reloptions"] == {"fillfactor": "70"}
        assert unlogged.comment == "scratch"

        partitioned = parser.parse_table_state(
            [{"table_name": "orders", "relkind": "p", "relpersistence": "p"}],
            "orders",
            "public",
        )
        assert partitioned is not None
        assert partitioned.traits == ("partitioned",)

    def test_table_state_missing_table_is_none_at_the_parser(self):
        assert _parser().parse_table_state([], "orders", "public") is None

    def test_index_state_statement_reads_validity_and_backing_constraint(self):
        sql, params = PostgresIndexStateQueryStatement(
            _dialect(), "orders", schema="public"
        ).to_sql()
        assert "ix.indisvalid" in sql
        assert '"backs_constraint"' in sql
        assert "pg_constraint AS con" in sql
        assert params == ("public", "orders")

    def test_index_state_maps_indisvalid_to_usable(self):
        rows = [
            {
                "index_name": "orders_pkey",
                "table_name": "orders",
                "is_unique": True,
                "is_valid": False,
                "method": "btree",
                "backs_constraint": "orders_pkey",
            }
        ]
        states = _parser().parse_index_states(rows, "orders", "public")
        assert len(states) == 1
        assert isinstance(states[0], IndexState)
        assert states[0].usable is False
        assert states[0].backs_constraint == "orders_pkey"

    def test_constraint_state_statement_reads_validation(self):
        sql, params = PostgresConstraintStateQueryStatement(
            _dialect(), "orders", schema="public"
        ).to_sql()
        assert "con.convalidated" in sql
        assert '"backing_index"' in sql
        assert params == ("public", "orders")

    def test_constraint_state_maps_convalidated_to_validated(self):
        rows = [
            {
                "name": "orders_total_check",
                "table_name": "orders",
                "constraint_type": "c",
                "is_validated": False,
                "definition": "CHECK (total >= 0) NOT VALID",
            }
        ]
        states = _parser().parse_constraint_states(rows, "orders", "public")
        assert len(states) == 1
        assert isinstance(states[0], ConstraintState)
        assert states[0].kind == "check"
        assert states[0].validated is False
        assert states[0].raw["definition"] == "CHECK (total >= 0) NOT VALID"

    def test_primary_key_backing_index_is_a_derived_object(self):
        states = [
            ConstraintState(
                name="orders_pkey",
                table="orders",
                kind="primary_key",
                backing_index="orders_pkey_idx",
            ),
            ConstraintState(name="orders_total_check", table="orders", kind="check"),
        ]
        derived = _parser().derived_backing_indexes(states)
        assert derived == [
            DerivedObjectState(
                kind="backing_index",
                name="orders_pkey_idx",
                owned_by="orders_pkey",
                raw={"constraint_kind": "primary_key"},
            )
        ]

    def test_sequence_state_statement_reads_ownership(self):
        sql, params = PostgresSequenceStateQueryStatement(
            _dialect(), schema="public"
        ).to_sql()
        assert "pg_depend AS d" in sql
        assert '"owned_by"' in sql
        assert "pg_sequence AS s" in sql
        assert params == ("public",)

    def test_sequence_state_statement_resolves_the_dependency_class_oid(self):
        """``pg_depend``'s ``oid`` columns are compared to a resolved name.

        ``classid`` and ``refclassid`` are ``oid``-typed, so comparing one to
        the bare text ``'pg_class'`` asks the server to read text as an
        ``oid``. A live PostgreSQL answers ``invalid input syntax for type
        oid`` for that, while a scripted executor hands back whatever rows the
        fixture wrote -- which is why the spelling is pinned here, where no
        server is involved. ``regclass`` is the type whose input function
        turns a name into the relation's oid, so the comparison has to go
        through it.

        The name is schema-qualified because ``regclass`` resolves names
        through ``search_path``: a bare ``pg_class`` would answer a caller's
        same-named relation if one shadowed the catalog.
        """
        sql, _ = PostgresSequenceStateQueryStatement(
            _dialect(), schema="public"
        ).to_sql()

        resolved = "'pg_catalog.pg_class'::REGCLASS"
        assert f"d.classid = {resolved}" in sql
        assert f"d.refclassid = {resolved}" in sql
        # The spellings a live server rejects: a bare name compared directly,
        # and the same name against the unqualified catalog relation.
        assert "d.classid = 'pg_class'" not in sql
        assert "d.refclassid = 'pg_class'" not in sql
        assert "::REGCLASS" in sql

    def test_sequence_state_on_nine_drops_the_pg_sequence_join(self):
        sql, _ = PostgresSequenceStateQueryStatement(
            _dialect((9, 6, 0)), schema="public"
        ).to_sql()
        assert "pg_sequence" not in sql

    def test_sequence_state_carries_the_owning_column(self):
        states = _parser().parse_sequence_states(
            [
                {
                    "name": "orders_id_seq",
                    "schema_name": "public",
                    "owned_by": "public.orders.id",
                    "start": 1,
                    "increment": 1,
                }
            ],
            "public",
        )
        assert len(states) == 1
        assert isinstance(states[0], SequenceState)
        assert states[0].owned_by == "public.orders.id"
        assert states[0].start == 1

    def test_sequence_state_without_an_owner_reports_absence(self):
        """``concat_ws`` yields ``''`` when every part is NULL, not NULL.

        The projection builds ``schema.table.column`` from three columns that
        are all NULL for a sequence nobody owns, and ``concat_ws`` skips NULL
        arguments -- three skipped arguments leave the empty string. The
        neutral field is optional, so the row has to read as "not owned"
        rather than as a column named by nothing.
        """
        states = _parser().parse_sequence_states(
            [
                {
                    "name": "loose_seq",
                    "schema_name": "public",
                    "owned_by": "",
                    "start": 1,
                    "increment": 1,
                }
            ],
            "public",
        )
        assert len(states) == 1
        assert states[0].owned_by is None

    def test_schema_list_statement_skips_the_system_namespaces(self):
        sql, params = PostgresSchemaListQueryStatement(_dialect()).to_sql()
        assert "pg_namespace AS n" in sql
        assert "pg_catalog" in sql and "information_schema" in sql and "pg_toast" in sql
        assert params == ()

    def test_schema_rows_reduce_to_names(self):
        assert _parser().parse_schemas(
            [{"name": "public"}, {"name": "app"}]
        ) == ["public", "app"]

    def test_partition_method_answers_for_a_parent_with_no_children(self):
        introspector = _scripted(
            [
                ("pg_stat_user_tables", [_table_row("orders", "PARTITIONED TABLE")]),
                (
                    "pg_inherits",
                    [
                        {
                            "parent_table": "orders",
                            "partition_key": "RANGE (created_at)",
                            "name": None,
                        }
                    ],
                ),
            ]
        )
        state = introspector.get_partition_state("orders")
        assert state is not None
        assert state.scheme is PartitionScheme.RANGE
        assert state.bounds == ()

    def test_partition_method_answers_none_for_a_plain_table(self):
        introspector = _scripted(
            [("pg_stat_user_tables", [_table_row("users", "BASE TABLE")])]
        )
        assert introspector.get_partition_state("users") is None
        assert all("pg_inherits" not in sql for sql in introspector._executor.seen)

    def test_table_state_fills_partitioning_from_the_kind(self):
        introspector = _scripted(
            [
                (
                    "relpersistence",
                    [
                        {
                            "table_name": "orders",
                            "relkind": "p",
                            "relpersistence": "p",
                        }
                    ],
                ),
                (
                    "pg_inherits",
                    [
                        {
                            "parent_table": "orders",
                            "partition_key": "LIST (status)",
                            "name": "orders_active",
                            "bound": "FOR VALUES IN ('active')",
                        }
                    ],
                ),
            ]
        )
        state = introspector.get_table_state("orders")
        assert state.traits == ("partitioned",)
        assert state.partitioning is not None
        assert state.partitioning.scheme is PartitionScheme.LIST
        assert state.partitioning.bounds[0].values == ("active",)

    def test_table_state_reads_unlogged(self):
        introspector = _scripted(
            [
                (
                    "relpersistence",
                    [
                        {
                            "table_name": "cache",
                            "relkind": "r",
                            "relpersistence": "u",
                            "reloptions": ["fillfactor=70"],
                        }
                    ],
                )
            ]
        )
        state = introspector.get_table_state("cache")
        assert state.kind == "unlogged"
        assert state.partitioning is None

    def test_derived_objects_are_the_constraint_backing_indexes(self):
        introspector = _scripted(
            [
                (
                    "convalidated",
                    [
                        {
                            "name": "orders_pkey",
                            "table_name": "orders",
                            "constraint_type": "p",
                            "is_validated": True,
                            "backing_index": "orders_pkey",
                        },
                        {
                            "name": "orders_total_check",
                            "table_name": "orders",
                            "constraint_type": "c",
                            "is_validated": True,
                            "backing_index": None,
                        },
                    ],
                )
            ]
        )
        derived = introspector.list_derived_objects("orders")
        assert [item.name for item in derived] == ["orders_pkey"]
        assert derived[0].owned_by == "orders_pkey"

    def test_item_exists_asks_the_listing_for_each_kind(self):
        introspector = _scripted(
            [
                ("pg_namespace AS n", [{"name": "public"}, {"name": "app"}]),
                (
                    "pg_type AS t JOIN pg_namespace AS n",
                    [
                        {
                            "name": "order_status",
                            "schema_name": "public",
                            "type_code": "e",
                        },
                        {
                            "name": "positive_total",
                            "schema_name": "public",
                            "type_code": "d",
                        },
                    ],
                ),
                (
                    "pg_sequences AS p",
                    [
                        {
                            "schema_name": "public",
                            "sequence_name": "orders_id_seq",
                            "data_type": "bigint",
                        }
                    ],
                ),
                ("pg_extension AS e LEFT JOIN pg_namespace", [{"name": "pgcrypto"}]),
            ]
        )
        assert introspector.item_exists(ItemKind.SCHEMA, "app") is True
        assert introspector.item_exists(ItemKind.SCHEMA, "missing") is False
        assert introspector.item_exists(ItemKind.TYPE, "order_status") is True
        assert introspector.item_exists(ItemKind.DOMAIN, "order_status") is False
        assert introspector.item_exists(ItemKind.DOMAIN, "positive_total") is True
        assert introspector.item_exists(ItemKind.SEQUENCE, "orders_id_seq") is True
        assert introspector.item_exists(ItemKind.EXTENSION, "pgcrypto") is True


class TestRoutines:
    """I-03: functions, procedures, aggregates and windows."""

    def test_statement_reads_pg_proc_and_pg_language(self):
        sql, params = PostgresRoutineListQueryStatement(
            _dialect(), schema="public"
        ).to_sql()
        assert "FROM (pg_proc AS p JOIN pg_namespace AS n" in sql
        assert "pg_language AS l" in sql
        assert "PG_GET_FUNCTION_IDENTITY_ARGUMENTS(p.oid) AS \"identity_arguments\"" in sql
        assert "PG_GET_FUNCTION_RESULT(p.oid) AS \"result_type\"" in sql
        assert params == ("public",)

    def test_a_name_narrows_the_listing(self):
        sql, params = PostgresRoutineListQueryStatement(
            _dialect(), schema="public", routine_name="app_total"
        ).to_sql()
        assert "p.proname = %s" in sql
        assert params == ("public", "app_total")

    def test_pre_11_uses_the_two_legacy_kind_flags(self):
        sql, _ = PostgresRoutineListQueryStatement(
            _dialect((10, 0, 0)), schema="public"
        ).to_sql()
        assert "p.prokind" not in sql
        assert "p.proiswindow" in sql and "p.proisagg" in sql

    @pytest.mark.parametrize(
        "code,kind",
        [
            ("f", PostgresRoutineKind.FUNCTION),
            ("p", PostgresRoutineKind.PROCEDURE),
            ("a", PostgresRoutineKind.AGGREGATE),
            ("w", PostgresRoutineKind.WINDOW),
        ],
    )
    def test_prokind_maps_to_the_record_kind(self, code, kind):
        routine = _parser().parse_routine(
            {
                "name": "app_total",
                "schema_name": "public",
                "kind": code,
                "identity_arguments": "integer",
                "result_type": "integer",
                "language": "sql",
                "volatility": "i",
                "security_definer": False,
                "comment": None,
            },
            "public",
        )
        assert isinstance(routine, PostgresRoutineInfo)
        assert routine.kind is kind
        assert routine.volatility == "immutable"


class TestTypes:
    """I-03: enum/domain/composite/range/multirange, one family."""

    def test_statement_reads_type_enum_and_attribute_catalogs(self):
        sql, params = PostgresTypeListQueryStatement(
            _dialect(), schema="public"
        ).to_sql()
        assert "pg_type AS t" in sql
        assert "pg_enum AS e" in sql
        assert "pg_attribute AS a" in sql
        assert "pg_range AS r" in sql
        assert "r.rngmultitypid = t.oid" in sql
        assert "t.typtype IN ('e', 'd', 'c', 'r', 'm')" in sql
        assert params == ("public",)

    def test_pre_14_omits_the_multirange_edge(self):
        sql, _ = PostgresTypeListQueryStatement(
            _dialect((13, 0, 0)), schema="public"
        ).to_sql()
        assert "rngmultitypid" not in sql
        assert "t.typtype IN ('e', 'd', 'c', 'r')" in sql

    def test_rows_group_into_one_record_per_type(self):
        rows = [
            {
                "name": "order_status",
                "schema_name": "public",
                "type_code": "e",
                "label": "pending",
                "label_order": 1,
                "field_name": None,
                "field_order": None,
                "base_type": None,
                "domain_not_null": None,
                "domain_default": None,
                "comment": None,
                "relation_kind": None,
                "relation_name": None,
            },
            {
                "name": "order_status",
                "schema_name": "public",
                "type_code": "e",
                "label": "active",
                "label_order": 2,
                "field_name": None,
                "field_order": None,
                "base_type": None,
                "domain_not_null": None,
                "domain_default": None,
                "comment": None,
                "relation_kind": None,
                "relation_name": None,
            },
            {
                "name": "order_total",
                "schema_name": "public",
                "type_code": "d",
                "label": None,
                "label_order": None,
                "field_name": None,
                "field_order": None,
                "base_type": "numeric(10,2)",
                "domain_not_null": True,
                "domain_default": "0",
                "comment": None,
                "relation_kind": None,
                "relation_name": None,
            },
        ]
        types = _parser().parse_types(rows, "public")
        status = next(t for t in types if t.name == "order_status")
        total = next(t for t in types if t.name == "order_total")
        assert status.kind is PostgresTypeKind.ENUM
        assert status.labels == ["pending", "active"]
        assert total.kind is PostgresTypeKind.DOMAIN
        assert total.base_type == "numeric(10,2)"
        assert total.domain_not_null is True
        assert total.domain_default == "0"

    def test_composite_fields_are_ordered_and_keep_relation_kind(self):
        rows = [
            {
                "name": "address",
                "schema_name": "public",
                "type_code": "c",
                "label": None,
                "label_order": None,
                "field_name": "street",
                "field_order": 1,
                "base_type": None,
                "domain_not_null": None,
                "domain_default": None,
                "comment": None,
                "relation_kind": "c",
                "relation_name": "address",
            },
            {
                "name": "address",
                "schema_name": "public",
                "type_code": "c",
                "label": None,
                "label_order": None,
                "field_name": "zip",
                "field_order": 2,
                "base_type": None,
                "domain_not_null": None,
                "domain_default": None,
                "comment": None,
                "relation_kind": "c",
                "relation_name": "address",
            },
        ]
        type_info = _parser().parse_types(rows, "public")[0]
        assert isinstance(type_info, PostgresTypeInfo)
        assert type_info.kind is PostgresTypeKind.COMPOSITE
        assert type_info.fields == ["street", "zip"]
        assert type_info.extra["relation_kind"] == "c"


class TestOperators:
    """I-03: operators, operator classes and operator families."""

    def test_operator_statement_reads_pg_operator(self):
        sql, params = PostgresOperatorListQueryStatement(
            _dialect(), schema="public"
        ).to_sql()
        assert "pg_operator AS o" in sql
        assert "PG_CATALOG.FORMAT_TYPE(o.oprleft, NULL)" in sql
        assert "CASE WHEN o.oprleft = 0 THEN NULL" in sql
        assert "p.proname AS \"function_name\"" in sql
        assert params == ("public",)

    def test_operator_kind_and_absent_operands(self):
        operator = _parser().parse_operator(
            {
                "name": "!",
                "schema_name": "public",
                "operator_kind": "l",
                "left_type": None,
                "right_type": "integer",
                "result_type": "integer",
                "function_name": "int4not",
                "owner": "postgres",
                "comment": None,
            },
            "public",
        )
        assert isinstance(operator, PostgresOperatorInfo)
        assert operator.operator_kind == "prefix"
        assert operator.left_type is None
        assert operator.right_type == "integer"

    def test_operator_class_statement_reads_pg_opclass(self):
        sql, params = PostgresOperatorClassListQueryStatement(
            _dialect(), schema="public"
        ).to_sql()
        assert "pg_opclass AS oc" in sql
        assert "pg_am AS am" in sql
        assert "pg_opfamily AS f" in sql
        assert params == ("public",)

    def test_operator_class_default_and_key_type(self):
        operator_class = _parser().parse_operator_class(
            {
                "name": "gist_trgm_ops",
                "schema_name": "public",
                "method": "gist",
                "input_type": "text",
                "key_type": None,
                "is_default": False,
                "family": "gist_trgm_ops",
                "owner": "postgres",
                "comment": None,
            },
            "public",
        )
        assert isinstance(operator_class, PostgresOperatorClassInfo)
        assert operator_class.method == "gist"
        assert operator_class.input_type == "text"
        assert operator_class.key_type is None
        assert operator_class.is_default is False

    def test_operator_family_statement_reads_pg_opfamily(self):
        sql, params = PostgresOperatorFamilyListQueryStatement(
            _dialect(), schema="public"
        ).to_sql()
        assert "pg_opfamily AS f" in sql
        assert params == ("public",)

    def test_operator_family_record(self):
        family = _parser().parse_operator_family(
            {
                "name": "gist_trgm_ops",
                "schema_name": "public",
                "method": "gist",
                "owner": "postgres",
                "comment": None,
            },
            "public",
        )
        assert isinstance(family, PostgresOperatorFamilyInfo)
        assert family.method == "gist"


class TestCliVocabulary:
    """The CLI offers every new family and knows what its NAME means."""

    @pytest.mark.parametrize(
        "type_name,switch",
        [
            ("partitions", "supports_partition_introspection"),
            ("routines", "supports_routine_introspection"),
            ("types", "supports_type_introspection"),
            ("operators", "supports_operator_introspection"),
            ("operator-classes", "supports_operator_class_introspection"),
            ("operator-families", "supports_operator_family_introspection"),
        ],
    )
    def test_the_family_is_advertised_and_gated(self, type_name, switch):
        assert type_name in INTROSPECT_TYPES
        assert _TYPE_CAPABILITIES[type_name] == switch
        assert type_name in _NAME_RULE

    def test_partitions_requires_a_parent_name(self):
        assert _NAME_RULE["partitions"] == "required"

    @pytest.mark.parametrize(
        "type_name",
        [
            "routines",
            "types",
            "operators",
            "operator-classes",
            "operator-families",
        ],
    )
    def test_the_catalog_families_narrow_by_name(self, type_name):
        assert _NAME_RULE[type_name] == "optional"
