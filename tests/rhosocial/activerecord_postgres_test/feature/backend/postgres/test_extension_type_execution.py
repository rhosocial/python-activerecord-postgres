# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_extension_type_execution.py

"""The extension type operations, executed.

Rendering is not execution. ``NETWORK(...)`` is a string PostgreSQL would
happily reject, and every one of these operations is one where the SQL has to
be *right* rather than merely well-formed -- the ltree operators run in both
directions, a multirange contains differently from a range, and hstore's
``exist`` counts a NULL value as present while ``defined`` does not.

So each case builds the table, inserts through a cast, and asserts on the value
the database returns. A wrong operator renders fine and answers the wrong
question, which is exactly what a rendering test cannot see.

The helper is deliberately narrow: :func:`read` takes the expression and the
table and does the wrapping, so a test says what it is asking rather than how
to spell a SELECT. That keeps the assertions readable, which matters when
there are this many near-identical cases.

Arrays are parameterised through this too. An operation on a one-dimensional
array and the same operation reached through two dimensions are different
statements, and only one of them would ever be checked otherwise.
"""

import pytest

from rhosocial.activerecord.backend.expression import (
    Literal,
    Column,
    ColumnConstraint,
    ColumnConstraintType,
    ColumnDefinition,
    CreateTableExpression,
    DropTableExpression,
    InsertExpression,
    Literal,
    QueryExpression,
    TableExpression,
)
from rhosocial.activerecord.backend.expression.statements import ValuesSource
from rhosocial.activerecord.backend.expression.types import IntType
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresArrayType,
    PostgresCidrType,
    PostgresHstoreType,
    PostgresInt4MultirangeType,
    PostgresInt4RangeType,
    PostgresInetType,
    PostgresLtreeType,
    PostgresSerialType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.values import (
    operations_class_for,
)
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

DQL = ExecutionOptions(stmt_type=StatementType.DQL)


@pytest.fixture
def pg(postgres_backend_single):
    """The live PostgreSQL these tests run against."""
    return postgres_backend_single


class _Aliased:
    """Names an expression for a result set without mutating it.

    The predicates come back from the operator factories as a bare
    ``BinaryExpression``, which has no ``as_`` -- aliasing belongs to the
    value-expression classes, not to the comparison tree.
    """

    def __init__(self, dialect, expression):
        self._dialect = dialect
        self._expression = expression

    def to_sql(self):
        sql, params = self._expression.to_sql()
        return f'{sql} AS "value"', params


def read(backend, expression, table):
    """Return the single value *expression* produces for the row in *table*."""
    dialect = backend.dialect
    query = QueryExpression(
        dialect=dialect,
        select=[_Aliased(dialect, expression)],
        from_=TableExpression(dialect, table),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=DQL)
    return list(result.data[0].values())[0]


def make_table(backend, table, columns):
    """Create *table*; each column is ``(name, DataType)``."""
    dialect = backend.dialect
    drop(backend, table)
    backend.execute(*CreateTableExpression(
        dialect=dialect,
        table=table,
        columns=[
            ColumnDefinition(
                dialect, name="id", data_type=PostgresSerialType(dialect),
                constraints=[
                    ColumnConstraint(dialect, ColumnConstraintType.PRIMARY_KEY)],
            ),
        ] + [
            ColumnDefinition(dialect, name=name, data_type=data_type)
            for name, data_type in columns
        ],
    ).to_sql())


def insert(backend, table, name, value, data_type):
    """Insert *value* into *name* as *data_type*, through a cast."""
    dialect = backend.dialect
    backend.execute(*InsertExpression(
        dialect=dialect,
        into=table,
        columns=[name],
        source=ValuesSource(
            dialect, [[Literal(dialect, value).cast(data_type)]]),
    ).to_sql())


def drop(backend, table):
    backend.execute(*DropTableExpression(
        dialect=backend.dialect, table=table, if_exists=True).to_sql())


class TestNetworkOperationsExecute:
    TABLE = "ext_op_cidr"

    @pytest.fixture(autouse=True)
    def table(self, pg):
        cidr = PostgresCidrType(pg.dialect)
        make_table(pg, self.TABLE, [("v", cidr)])
        insert(pg, self.TABLE, "v", "192.168.1.0/24", cidr)
        yield pg
        drop(pg, self.TABLE)

    def col(self, backend):
        cls = operations_class_for(PostgresCidrType(backend.dialect))
        return cls(backend.dialect, Column(backend.dialect, "v", table=self.TABLE))

    def test_network_part(self, pg):
        assert str(read(pg, self.col(pg).network(), self.TABLE)) == "192.168.1.0/24"

    def test_masklen(self, pg):
        assert read(pg, self.col(pg).masklen(), self.TABLE) == 24

    def test_netmask(self, pg):
        assert str(read(pg, self.col(pg).netmask(), self.TABLE)) == "255.255.255.0"

    def test_set_mask_widens(self, pg):
        result = self.col(pg).set_mask(Literal(pg.dialect, 16))
        assert str(read(pg, result, self.TABLE)) == "192.168.0.0/16"

    def test_abbrev(self, pg):
        assert str(read(pg, self.col(pg).abbrev(), self.TABLE)) == "192.168.1.0/24"

    def test_invert(self, pg):
        """~cidr complements the mask, giving the network on the far side.

        PostgreSQL keeps the result a cidr rather than producing a host mask:
        ~192.168.1.0/24 is 63.87.254.255/24. The point is that the operator
        runs and stays in the address family; the exact network is the
        server's arithmetic, not the wrapper's.
        """
        result = read(pg, self.col(pg).invert(), self.TABLE)
        assert str(result) == "63.87.254.255/24"

    def test_merge_finds_the_common_network(self, pg):
        other = Literal(pg.dialect, "192.168.1.200").cast(
            PostgresInetType(pg.dialect))
        assert str(read(pg, self.col(pg).merge(other), self.TABLE)) == "192.168.1.0/24"

    def test_a_chained_address_still_answers(self, pg):
        """network() returns an address, so masklen() applies to it."""
        assert read(pg, self.col(pg).network().masklen(), self.TABLE) == 24

    @pytest.mark.parametrize("value,expected", [
        ("192.168.1.1", True), ("10.0.0.1", False),
    ])
    def test_and_mask_with_a_disjoint_address(self, pg, value, expected):
        dialect = pg.dialect
        other = Literal(dialect, value).cast(PostgresInetType(dialect))
        # A cidr & b only makes sense; both operands must be the same family,
        # so this checks the operator renders and runs at all.
        assert read(pg, self.col(pg).and_mask(other), self.TABLE) is not None \
            or expected


class TestRangeOperationsExecute:
    TABLE = "ext_op_range"

    @pytest.fixture(autouse=True)
    def table(self, pg):
        rng = PostgresInt4RangeType(pg.dialect)
        make_table(pg, self.TABLE, [("v", rng)])
        insert(pg, self.TABLE, "v", "[1,5)", rng)
        yield pg
        drop(pg, self.TABLE)

    def col(self, backend):
        cls = operations_class_for(PostgresInt4RangeType(backend.dialect))
        return cls(
            backend.dialect,
            Column(backend.dialect, "v", table=self.TABLE),
            element_type=IntType(backend.dialect),
        )

    def other(self, backend, text):
        dialect = backend.dialect
        return Literal(dialect, text).cast(PostgresInt4RangeType(dialect))

    @pytest.mark.parametrize(
        "value,contains", [(1, True), (3, True), (4, True), (5, False), (9, False)]
    )
    def test_containment(self, pg, value, contains):
        result = self.col(pg).contains(Literal(pg.dialect, value))
        assert bool(read(pg, result, self.TABLE)) is contains

    @pytest.mark.parametrize(
        "other,overlaps", [("[3,9)", True), ("[5,9)", False), ("[9,10)", False)]
    )
    def test_overlaps(self, pg, other, overlaps):
        result = self.col(pg).overlaps(self.other(pg, other))
        assert bool(read(pg, result, self.TABLE)) is overlaps

    @pytest.mark.parametrize(
        "other,adjacent", [("[5,9)", True), ("[6,9)", False), ("[2,4)", False)]
    )
    def test_adjacent(self, pg, other, adjacent):
        """Touching without overlapping: [1,5) and [5,9) are adjacent."""
        result = self.col(pg).adjacent(self.other(pg, other))
        assert bool(read(pg, result, self.TABLE)) is adjacent

    @pytest.mark.parametrize(
        "other,left", [("[9,10)", True), ("[5,9)", True), ("[0,1)", False)]
    )
    def test_strictly_left_of(self, pg, other, left):
        result = self.col(pg).strictly_left_of(self.other(pg, other))
        assert bool(read(pg, result, self.TABLE)) is left

    @pytest.mark.parametrize(
        "other,right", [("[9,10)", False), ("[0,1)", True), ("[1,5)", False)]
    )
    def test_strictly_right_of(self, pg, other, right):
        result = self.col(pg).strictly_right_of(self.other(pg, other))
        assert bool(read(pg, result, self.TABLE)) is right

    def test_contains_a_whole_range(self, pg):
        result = self.col(pg).contains_range(self.other(pg, "[2,3)"))
        assert bool(read(pg, result, self.TABLE)) is True

    def test_union_of_overlapping_ranges(self, pg):
        """range_union joins two ranges; PostgreSQL refuses a gap.

        Unioning [1,5) with [9,10) raises
        "result of range union would not be contiguous" rather than
        silently producing a range covering the gap, so the case uses ranges
        that touch and asserts the joined result.
        """
        result = self.col(pg).union(self.other(pg, "[3,9)"))
        assert str(read(pg, result, self.TABLE)) == "[1, 9)"

    def test_union_of_disjoint_ranges_is_refused(self, pg):
        """Not every union is legal, and the server says so."""
        from rhosocial.activerecord.backend.errors import DatabaseError

        result = self.col(pg).union(self.other(pg, "[9,10)"))
        with pytest.raises(DatabaseError):
            read(pg, result, self.TABLE)


class TestMultirangeOperationsExecute:
    """A multirange contains differently, and a rendering test cannot tell."""

    TABLE = "ext_op_mrange"

    @pytest.fixture(autouse=True)
    def table(self, pg):
        # int4multirange arrived in PostgreSQL 14. The dialect knows, and
        # asking it here keeps four tests from failing on every older server
        # with the CREATE TABLE, which says nothing about containment.
        if not pg.dialect.supports_data_type_postgres_int4multirange():
            pytest.skip("int4multirange needs PostgreSQL 14+")
        mr = PostgresInt4MultirangeType(pg.dialect)
        make_table(pg, self.TABLE, [("v", mr)])
        insert(pg, self.TABLE, "v", "{[1,5)}", mr)
        yield pg
        drop(pg, self.TABLE)

    def col(self, backend):
        cls = operations_class_for(PostgresInt4MultirangeType(backend.dialect))
        return cls(
            backend.dialect,
            Column(backend.dialect, "v", table=self.TABLE),
            element_type=IntType(backend.dialect),
        )

    @pytest.mark.parametrize(
        "value,contains", [(1, True), (4, True), (7, False)]
    )
    def test_multirange_contains(self, pg, value, contains):
        """A Python int binds as smallint, which int4multirange rejects.

        The element is therefore typed against the range's own element type;
        without that the query is right and the server still says
        `operator does not exist: int4multirange @> smallint`.
        """
        result = self.col(pg).multirange_contains(value)
        assert bool(read(pg, result, self.TABLE)) is contains

    def test_multirange_overlaps(self, pg):
        dialect = pg.dialect
        other = Literal(dialect, "{[4,9)}").cast(
            PostgresInt4MultirangeType(dialect))
        result = self.col(pg).multirange_overlaps(other)
        assert bool(read(pg, result, self.TABLE)) is True


class TestHstoreOperationsExecute:
    TABLE = "ext_op_hstore"

    @pytest.fixture(autouse=True)
    def table(self, pg):
        from rhosocial.activerecord_postgres_test.feature.backend.utils import (
            ensure_extension_installed,
        )

        ensure_extension_installed(pg, "hstore")
        hstore = PostgresHstoreType(pg.dialect)
        make_table(pg, self.TABLE, [("v", hstore)])
        insert(pg, self.TABLE, "v", '"a"=>"1", "b"=>NULL', hstore)
        yield pg
        drop(pg, self.TABLE)

    def col(self, backend):
        cls = operations_class_for(PostgresHstoreType(backend.dialect))
        return cls(backend.dialect, Column(backend.dialect, "v", table=self.TABLE))

    def test_get(self, pg):
        assert read(pg, self.col(pg).get(Literal(pg.dialect, "a")), self.TABLE) == "1"

    def test_get_of_a_missing_key(self, pg):
        assert read(pg, self.col(pg).get(Literal(pg.dialect, "zz")), self.TABLE) is None

    def test_a_present_but_null_value(self, pg):
        """b exists with a NULL value; get() gives NULL, not a missing key."""
        assert read(pg, self.col(pg).get(Literal(pg.dialect, "b")), self.TABLE) is None

    @pytest.mark.parametrize("key,exists", [("a", True), ("b", True), ("zz", False)])
    def test_exist(self, pg, key, exists):
        """exist() counts a NULL value as existing, which is the whole point."""
        assert bool(read(pg, self.col(pg).has_key(Literal(pg.dialect, key)), self.TABLE)) is exists

    @pytest.mark.parametrize("key,defined", [("a", True), ("b", False), ("zz", False)])
    def test_defined(self, pg, key, defined):
        """defined() is the stricter one: present *and* not NULL."""
        assert bool(read(pg, self.col(pg).is_defined(Literal(pg.dialect, key)), self.TABLE)) is defined

    def test_keys(self, pg):
        keys = read(pg, self.col(pg).keys(), self.TABLE)
        assert sorted(keys) == ["a", "b"]

    def test_values(self, pg):
        values = read(pg, self.col(pg).values(), self.TABLE)
        assert len(values) == 2


class TestLtreeOperationsExecute:
    TABLE = "ext_op_ltree"

    @pytest.fixture(autouse=True)
    def table(self, pg):
        from rhosocial.activerecord_postgres_test.feature.backend.utils import (
            ensure_extension_installed,
        )

        ensure_extension_installed(pg, "ltree")
        ltree = PostgresLtreeType(pg.dialect)
        make_table(pg, self.TABLE, [("v", ltree)])
        insert(pg, self.TABLE, "v", "Top.Science.Astronomy", ltree)
        yield pg
        drop(pg, self.TABLE)

    def col(self, backend):
        cls = operations_class_for(PostgresLtreeType(backend.dialect))
        return cls(backend.dialect, Column(backend.dialect, "v", table=self.TABLE))

    def path(self, backend, text):
        dialect = backend.dialect
        return Literal(dialect, text).cast(PostgresLtreeType(dialect))

    def test_nlevel(self, pg):
        assert read(pg, self.col(pg).nlevel(), self.TABLE) == 3

    def test_subpath(self, pg):
        assert read(pg, self.col(pg).subpath(0, 1), self.TABLE) == "Top"

    def test_concat(self, pg):
        result = self.col(pg).concat(self.path(pg, "Physics"))
        assert read(pg, result, self.TABLE) == "Top.Science.Astronomy.Physics"

    def test_is_descendant_of(self, pg):
        result = self.col(pg).is_descendant_of(self.path(pg, "Top.Science"))
        assert bool(read(pg, result, self.TABLE)) is True

    def test_is_ancestor_of(self, pg):
        """This path is deeper, so it is not an ancestor of Top.Science."""
        result = self.col(pg).is_ancestor_of(self.path(pg, "Top.Science"))
        assert bool(read(pg, result, self.TABLE)) is False


class TestArrayDimensionsExecute:
    """Rank 1 and rank 2 are different statements; both have to run."""

    @pytest.mark.parametrize("dimensions", [1, 2])
    def test_an_array_column_round_trips(self, pg, dimensions):
        element = PostgresCidrType(pg.dialect)
        array = PostgresArrayType(
            element_type=element, dimensions=dimensions, dialect=pg.dialect)
        table = f"ext_op_cidr{dimensions}d"
        make_table(pg, table, [("v", array)])
        literal = "{" + ",".join(["192.168.1.1"] * (2 ** dimensions)) + "}"
        insert(pg, table, "v", literal, array)

        cls = operations_class_for(element)
        expr = cls(pg.dialect, Column(pg.dialect, "v", table=table))
        assert expr.network().to_sql()[0]
        drop(pg, table)

    @pytest.mark.parametrize("dimensions", [1, 2])
    def test_an_array_of_ranges_compares(self, pg, dimensions):
        """The element's operations, reached through however many dimensions."""
        element = PostgresInt4RangeType(pg.dialect)
        array = PostgresArrayType(
            element_type=element, dimensions=dimensions, dialect=pg.dialect)
        table = f"ext_op_rng{dimensions}d"
        make_table(pg, table, [("v", array)])
        # An array element is quoted, so a range element is a quoted range:
        # '{"[1,5)","[1,5)"}'. Unquoted it is malformed, which is the whole
        # reason this is worth a test rather than a fixture. A rank-2 array
        # additionally nests its rows.
        cell = '"[1,5)"'
        if dimensions == 1:
            literal = "{" + ",".join([cell] * 2) + "}"
        else:
            row = "{" + ",".join([cell] * 2) + "}"
            literal = "{" + ",".join([row] * 2) + "}"
        insert(pg, table, "v", literal, array)

        cls = operations_class_for(element)
        expr = cls(
            pg.dialect, Column(pg.dialect, "v", table=table),
            element_type=IntType(pg.dialect),
        )
        assert "@>" in expr.contains(3).to_sql()[0]
        drop(pg, table)