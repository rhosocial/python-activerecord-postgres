# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_partition_syntax_matrix.py
"""Offline matrix for PostgreSQL partition syntax families.

Each test consumes ``partition_syntax_case`` from
``partition_syntax_fixtures.py``. The matrix proves plan generation, spec
shape, DDL rendering, retention selection and protocol conformance without a
PostgreSQL server: a dialect plus a scripted catalog is enough.

The only statement-only row is ``attach_detach``; every other row is
plan-driven and is checked against its declared expected specs rather than
against names written inside the test.
"""

from datetime import datetime

import pytest

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.ddl.partition import (
    PostgresAttachPartitionStatement,
)
from rhosocial.activerecord.backend.impl.postgres.partition import (
    PostgresPartitionMaintenance,
)
from rhosocial.activerecord.backend.impl.postgres.protocols.ddl.partition import (
    PostgresPartitionSupport,
)
from rhosocial.activerecord.backend.partition import (
    PartitionEnsurer,
    PartitionInventory,
    PartitionRetention,
    PartitionValidator,
)
from partition_syntax_fixtures import (  # noqa: F401  (fixture re-export)
    PartitionSyntaxCase,
    partition_syntax_case as _matrix_case,
)


@pytest.fixture(name="partition_syntax_case")
def _partition_syntax_case_fixture(_matrix_case):  # noqa: F811
    """Expose the parameterized matrix fixture under its public name."""
    return _matrix_case


_METHOD_FRAGMENTS = {
    "RANGE": "FOR VALUES FROM",
    "LIST": "FOR VALUES IN",
    "HASH": "FOR VALUES WITH",
}


class _FakeTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class _FakeBackend:
    """Scripted backend: a fixed catalog plus DDL capture."""

    def __init__(self, rows):
        self.dialect = PostgresDialect(version=(17, 0, 0))
        self.rows = list(rows)
        self.executed = []

    def fetch_all(self, sql, params=None):
        return list(self.rows)

    def execute(self, sql, params=None):
        self.executed.append((sql, tuple(params or ())))

    def transaction(self):
        return _FakeTransaction()


def _format_bound_value(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


def _bound_text(bounds):
    if "modulus" in bounds:
        return "FOR VALUES WITH (MODULUS %d, REMAINDER %d)" % (
            bounds["modulus"], bounds["remainder"],
        )
    if "values" in bounds:
        values = ", ".join("'%s'" % value for value in bounds["values"])
        return "FOR VALUES IN (%s)" % values
    return "FOR VALUES FROM ('%s') TO ('%s')" % (
        _format_bound_value(bounds["from"]),
        _format_bound_value(bounds["to"]),
    )


def _catalog_rows(case: PartitionSyntaxCase):
    """Build the catalog a live server would return for the fixture facts."""
    plan = case.plan
    if plan is None:
        return []
    root_key = "%s (%s)" % (plan.method, ", ".join(plan.columns))
    rows = [{
        "name": plan.parent,
        "parent": None,
        "level": 0,
        "schema_name": "public",
        "partition_key": root_key,
        "bound": None,
        "row_count": 0,
        "total_bytes": 0,
        "is_partitioned": True,
    }]
    for fact in case.expected_specs:
        partition_key = None
        if fact.child_method is not None:
            partition_key = "%s (%s)" % (
                fact.child_method, ", ".join(fact.child_columns),
            )
        rows.append({
            "name": fact.name,
            "parent": fact.parent,
            "level": fact.level,
            "schema_name": "public",
            "partition_key": partition_key,
            "bound": _bound_text(fact.bounds),
            "row_count": 0,
            "total_bytes": 0,
            "is_partitioned": fact.child_method is not None,
        })
    return rows


def _plan_ddl(case: PartitionSyntaxCase, if_not_exists=True):
    """Render the plan's actual specs, keyed by partition name."""
    maintenance = PostgresPartitionMaintenance(_FakeBackend([]))
    return {
        spec.name: maintenance._create_statement(case.plan, spec, if_not_exists)[0]
        for spec in case.plan.iter_specs()
    }


def _clause_sql(case: PartitionSyntaxCase):
    dialect = PostgresDialect(version=(17, 0, 0))
    return [
        statement.build(dialect).to_sql()
        for statement in case.clause.statements
    ]


def _rendered_sql(case: PartitionSyntaxCase):
    if case.plan is not None:
        return tuple(_plan_ddl(case).values())
    return tuple(sql for sql, _ in _clause_sql(case))


def test_plan_specs_match_fixture_facts(partition_syntax_case):
    case = partition_syntax_case
    if case.plan is None:
        pytest.skip("statement-only case carries no plan specs")

    actual = tuple(case.plan.iter_specs())
    assert len(actual) == len(case.expected_specs)
    assert [spec.name for spec in actual] == [
        fact.name for fact in case.expected_specs
    ]

    for spec, fact in zip(actual, case.expected_specs):
        assert spec.name == fact.name
        assert spec.parent == fact.parent
        assert spec.level == fact.level
        assert spec.is_leaf is fact.is_leaf
        assert spec.method == fact.method
        assert spec.bounds == fact.bounds
        assert spec.child_method == fact.child_method
        assert spec.child_columns == fact.child_columns


def test_branch_leaf_counts_and_shape(partition_syntax_case):
    case = partition_syntax_case
    if not case.branch_leaf_counts:
        pytest.skip("flat or statement-only case has no branch leaf counts")

    actual = tuple(case.plan.iter_specs())
    for branch_name, expected_count in case.branch_leaf_counts:
        branch = next(spec for spec in actual if spec.name == branch_name)
        leaves = [
            spec for spec in actual
            if spec.parent == branch_name and spec.level == 2
        ]
        assert branch.level == 1
        assert len(leaves) == expected_count
        if expected_count:
            assert branch.is_leaf is False
            assert branch.child_method is not None
        else:
            assert branch.is_leaf is True
            assert branch.child_method is None


def test_leaf_and_subpartitioned_branch_ddl(partition_syntax_case):
    case = partition_syntax_case
    if case.plan is None:
        pytest.skip("statement-only case has no plan DDL")

    ddl = _plan_ddl(case)
    for spec in case.plan.iter_specs():
        sql = ddl[spec.name]
        assert _METHOD_FRAGMENTS[spec.method] in sql
        if spec.child_method is None:
            assert "PARTITION BY" not in sql
        else:
            assert "PARTITION BY %s" % spec.child_method in sql
            for column in spec.child_columns:
                assert column in sql


def test_ddl_required_and_forbidden_fragments(partition_syntax_case):
    case = partition_syntax_case
    rendered = "\n".join(_rendered_sql(case))
    for fragment in case.required_fragments:
        assert fragment in rendered
    for fragment in case.forbidden_fragments:
        assert fragment not in rendered


def test_drop_before_candidate_selection(partition_syntax_case):
    case = partition_syntax_case
    if case.drop_before is None:
        pytest.skip("case has no range leaves or no retention expectation")

    backend = _FakeBackend(_catalog_rows(case))
    maintenance = PostgresPartitionMaintenance(backend)
    report = maintenance.drop_before(
        case.plan, case.drop_before.cutoff, dry_run=True,
    )

    assert report.dry_run is True
    assert [op.name for op in report.operations] == list(case.drop_before.candidates)
    assert backend.executed == []

    facts = {fact.name: fact for fact in case.expected_specs}
    for operation in report.operations:
        assert facts[operation.name].is_leaf is True


def test_clause_statements_render(partition_syntax_case):
    case = partition_syntax_case
    if case.clause is None:
        pytest.skip("plan-driven case has no standalone clause statements")

    for statement, (sql, params) in zip(
        case.clause.statements, _clause_sql(case),
    ):
        assert sql == statement.expected_sql
        assert params == ()

    if case.clause.concurrent_attach_rejected:
        dialect = PostgresDialect(version=(17, 0, 0))
        statement = PostgresAttachPartitionStatement(
            dialect=dialect,
            partition_name="orders_2026_q1",
            parent_table="orders",
            partition_type="RANGE",
            partition_values={"from": "2026-01-01", "to": "2026-04-01"},
            concurrently=True,
        )
        with pytest.raises(UnsupportedFeatureError):
            statement.to_sql()
        assert dialect.supports_concurrent_attach() is False


def test_protocol_conformance(partition_syntax_case):
    case = partition_syntax_case
    dialect = PostgresDialect(version=(17, 0, 0))
    assert isinstance(dialect, PostgresPartitionSupport)
    assert dialect.supports_table_partitioning()
    assert dialect.supports_range_table_partitioning()
    assert dialect.supports_list_table_partitioning()
    assert dialect.supports_hash_table_partitioning()
    assert dialect.supports_default_partition()
    assert dialect.supports_attach_partition()
    assert dialect.supports_detach_partition()

    expected_detach = dialect.supports_concurrent_detach()
    assert expected_detach is True
    assert dialect.supports_concurrent_attach() is False
    assert dialect.supports_reorganize_partition() is False

    executor = PostgresPartitionMaintenance(_FakeBackend(_catalog_rows(case)))
    assert isinstance(executor, PartitionInventory)
    assert isinstance(executor, PartitionEnsurer)
    assert isinstance(executor, PartitionRetention)
    assert isinstance(executor, PartitionValidator)
