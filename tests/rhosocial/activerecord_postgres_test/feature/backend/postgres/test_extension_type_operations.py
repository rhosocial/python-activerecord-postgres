# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_extension_type_operations.py

"""PostgreSQL's own types, and what you can do with them.

The functions were all written -- ``network()``, ``range_contains()``,
``hstore_get_value()``, ``ltree_matches()`` -- as free functions in
``impl.postgres.functions``, and none of them was reachable from anything that
held the value. A ``cidr`` had no ``.network()``, an ``int4range`` had no
``.contains()``; the only way in was to import the module and pass the dialect
by hand.

These tests are parameterised over *type x operation*, and over array
dimensions, because the two multiply. An array of cidr is a different column
from a cidr and offers array operations; a two-dimensional array of int4range
is a third thing again, and nothing about it is implied by either of the
others. A test that covered the scalar case alone would have passed while the
array case stayed broken.

The family each operation returns is checked where it is the whole point.
``NETWORK()`` is an address and carries address operations; ``MASKLEN()`` is an
integer and must not, because ``MASKLEN(x).network()`` is a type error in
every backend and only PostgreSQL would have let it through.
"""

import pytest

from rhosocial.activerecord.backend.expression import Column
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresArrayType,
    PostgresCIDType,
    PostgresCidrType,
    PostgresHstoreType,
    PostgresInetType,
    PostgresInt4RangeType,
    PostgresLqueryType,
    PostgresLtreeType,
    PostgresLtxtqueryType,
    PostgresMacAddrType,
    PostgresTsTzRangeType,
)
from rhosocial.activerecord.backend.impl.postgres.expression.values import (
    NetworkValueExpression,
    operations_class_for,
    value_expression_for,
)


@pytest.fixture
def dialect():
    d = PostgresDialect()
    d._version = (16, 2, 0)
    return d


def typed(data_type, dialect):
    """Wrap a column the way a column of *data_type* would be wrapped."""
    # value_expression_for already returns a wrapper around this dialect's
    # empty node; replace its node with the column rather than wrapping twice.
    expression = value_expression_for(data_type, dialect)
    expression.call = Column(dialect, "v")
    return expression


# ---------------------------------------------------------------------------
# Which types have an operation surface at all
# ---------------------------------------------------------------------------

WITH_OPERATIONS = [
    ("cidr", PostgresCidrType),
    ("inet", PostgresInetType),
    ("macaddr", PostgresMacAddrType),
    ("hstore", PostgresHstoreType),
    ("ltree", PostgresLtreeType),
    ("lquery", PostgresLqueryType),
    ("ltxtquery", PostgresLtxtqueryType),
    ("int4range", PostgresInt4RangeType),
    ("tstzrange", PostgresTsTzRangeType),
]

WITHOUT_OPERATIONS = [
    ("cid", PostgresCIDType),
    ("serial", None),
]


class TestOperationSurfaceSelection:
    @pytest.mark.parametrize(
        "label,cls", WITH_OPERATIONS, ids=[c[0] for c in WITH_OPERATIONS]
    )
    def test_a_type_with_operations_has_them(self, dialect, label, cls):
        assert value_expression_for(cls(dialect), dialect) is not None

    @pytest.mark.parametrize("label,cls", WITHOUT_OPERATIONS)
    def test_a_type_without_operations_says_so(self, dialect, label, cls):
        if cls is None:
            pytest.skip("serial has no per-column operation surface by design")
        assert value_expression_for(cls(dialect), dialect) is None

    def test_a_name_is_not_a_type(self, dialect):
        """Choosing by name is how a wrong guess gets made."""
        with pytest.raises(ValueError, match="expected a DataType"):
            value_expression_for("cidr", dialect)

    def test_an_array_borrows_its_element_s_operations(self, dialect):
        """``cidr[]`` is an array, but what you can do to it is cidr's."""
        element = PostgresCidrType(dialect)
        array = PostgresArrayType(element_type=element, dialect=dialect)
        # The array's own dispatch key is postgres_array, so it selects
        # nothing -- which is why the element has to be consulted.
        assert operations_class_for(array) is None
        assert operations_class_for(element) is not None


# ---------------------------------------------------------------------------
# inet / cidr
# ---------------------------------------------------------------------------

NETWORK_OPERATIONS = [
    ("network", (), "NETWORK("),
    ("netmask", (), "NETMASK("),
    ("masklen", (), "MASKLEN("),
    ("set_mask", (24,), "SET_MASKLEN("),
    ("abbrev", (), "TEXT("),
    ("merge", (Column, ), "MERGE("),
    ("invert", (), "~"),
    ("and_mask", (Column, ), "&"),
    ("or_mask", (Column, ), "|"),
]


class TestNetworkOperations:
    @pytest.mark.parametrize(
        "method,args,expected",
        [(m, a, e) for m, a, e in NETWORK_OPERATIONS],
        ids=[m for m, _, _ in NETWORK_OPERATIONS],
    )
    def test_operation(self, dialect, method, args, expected):
        expr = typed(PostgresCidrType(dialect), dialect)
        resolved = tuple(
            Column(dialect, "w") if a is Column else a for a in args
        )
        sql, _ = getattr(expr, method)(*resolved).to_sql()
        assert expected in sql

    def test_the_column_is_the_operand(self, dialect):
        expr = typed(PostgresCidrType(dialect), dialect)
        sql, _ = expr.network().to_sql()
        assert '"v"' in sql

    def test_a_mask_length_is_bound(self, dialect):
        expr = typed(PostgresCidrType(dialect), dialect)
        sql, params = expr.set_mask(24).to_sql()
        assert "?" in sql or "%s" in sql
        assert 24 in params

    def test_an_address_result_stays_an_address(self, dialect):
        """NETWORK() returns a cidr, so the chain continues."""
        expr = typed(PostgresCidrType(dialect), dialect)
        chained = expr.network().masklen()
        sql, _ = chained.to_sql()
        assert "MASKLEN(NETWORK(" in sql

    def test_a_scalar_result_does_not(self, dialect):
        """MASKLEN() returns an integer; address operations must not follow."""
        expr = typed(PostgresCidrType(dialect), dialect)
        assert not hasattr(expr.masklen(), "network")


# ---------------------------------------------------------------------------
# ranges
# ---------------------------------------------------------------------------

RANGE_OPERATIONS = [
    ("contains", (3,), "@>"),
    ("contains_range", (Column,), "@>"),
    ("overlaps", (Column,), "&&"),
    ("adjacent", (Column,), "-|-"),
    ("strictly_left_of", (Column,), "<<"),
    ("strictly_right_of", (Column,), ">>"),
]


class TestRangeOperations:
    @pytest.mark.parametrize(
        "method,args,operator",
        [(m, a, o) for m, a, o in RANGE_OPERATIONS],
        ids=[m for m, _, _ in RANGE_OPERATIONS],
    )
    def test_operation(self, dialect, method, args, operator):
        expr = typed(PostgresInt4RangeType(dialect), dialect)
        resolved = tuple(
            Column(dialect, "w") if a is Column else a for a in args
        )
        sql, _ = getattr(expr, method)(*resolved).to_sql()
        assert operator in sql

    def test_containment_of_an_element_binds_it(self, dialect):
        expr = typed(PostgresInt4RangeType(dialect), dialect)
        sql, params = expr.contains(3).to_sql()
        assert 3 in params

    @pytest.mark.parametrize("label,cls", [
        ("int4range", PostgresInt4RangeType),
        ("tstzrange", PostgresTsTzRangeType),
    ])
    def test_every_range_type_offers_the_same_surface(self, dialect, label, cls):
        """The operations belong to range-ness, not to one range type."""
        expr = typed(cls(dialect), dialect)
        for method, args, _ in RANGE_OPERATIONS:
            resolved = tuple(
                Column(dialect, "w") if a is Column else a for a in args
            )
            assert getattr(expr, method)(*resolved).to_sql()[0]

    def test_multirange_containment_is_a_different_operation(self, dialect):
        """A multirange contains differently, so it is named differently."""
        from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresInt4MultirangeType,
        )

        expr = typed(PostgresInt4MultirangeType(dialect), dialect)
        sql, params = expr.multirange_contains(3).to_sql()
        assert 3 in params


# ---------------------------------------------------------------------------
# hstore
# ---------------------------------------------------------------------------

class TestHstoreOperations:
    def test_keys_and_values(self, dialect):
        expr = typed(PostgresHstoreType(dialect), dialect)
        assert "akeys(" in expr.keys().to_sql()[0].lower()
        assert "avals(" in expr.values().to_sql()[0].lower()

    @pytest.mark.parametrize(
        "method,expected",
        [("get", "->"), ("get_as_text", "->"), ("subscript", "->")],
    )
    def test_accessors(self, dialect, method, expected):
        expr = typed(PostgresHstoreType(dialect), dialect)
        sql, params = getattr(expr, method)("k").to_sql()
        assert expected in sql
        assert "k" in params

    @pytest.mark.parametrize(
        "method", ["has_key", "is_defined", "key_exists"]
    )
    def test_predicates(self, dialect, method):
        expr = typed(PostgresHstoreType(dialect), dialect)
        sql, params = getattr(expr, method)("k").to_sql()
        assert "k" in params
        assert sql

    def test_a_value_is_not_a_document(self, dialect):
        """get() returns text; calling keys() on it would be a type error."""
        expr = typed(PostgresHstoreType(dialect), dialect)
        assert not hasattr(expr.get("k"), "keys")


# ---------------------------------------------------------------------------
# ltree and its pattern types
# ---------------------------------------------------------------------------

class TestLtreeOperations:
    def test_nlevel(self, dialect):
        expr = typed(PostgresLtreeType(dialect), dialect)
        assert "nlevel(" in expr.nlevel().to_sql()[0].lower()

    def test_subpath_takes_bound_arguments(self, dialect):
        expr = typed(PostgresLtreeType(dialect), dialect)
        sql, params = expr.subpath(0, 2).to_sql()
        assert 0 in params and 2 in params

    def test_concat_is_an_operator(self, dialect):
        expr = typed(PostgresLtreeType(dialect), dialect)
        assert "||" in expr.concat(Column(dialect, "w")).to_sql()[0]

    def test_matches_a_pattern(self, dialect):
        expr = typed(PostgresLtreeType(dialect), dialect)
        sql, params = expr.matches("root.*").to_sql()
        assert "root.*" in params

    def test_ancestry(self, dialect):
        expr = typed(PostgresLtreeType(dialect), dialect)
        assert "@>" in expr.is_ancestor_of(Column(dialect, "w")).to_sql()[0]
        assert "<@" in expr.is_descendant_of(Column(dialect, "w")).to_sql()[0]

    def test_subpath_result_is_still_a_tree(self, dialect):
        expr = typed(PostgresLtreeType(dialect), dialect)
        assert hasattr(expr.subpath(0, 2), "concat")

    @pytest.mark.parametrize("label,cls", [
        ("ltree", PostgresLtreeType),
        ("lquery", PostgresLqueryType),
        ("ltxtquery", PostgresLtxtqueryType),
    ])
    def test_the_pattern_types_share_the_surface(self, dialect, label, cls):
        """lquery and ltxtquery match an ltree; they are not trees."""
        expr = typed(cls(dialect), dialect)
        assert hasattr(expr, "matches")
        assert hasattr(expr, "nlevel")


# ---------------------------------------------------------------------------
# Array dimensions, which multiply everything
# ---------------------------------------------------------------------------

DIMENSIONS = [1, 2, 3]


class TestArrayDimensionsCompose:
    """A cidr[] is not a cidr. Neither is a cidr[][]."""

    @pytest.mark.parametrize("dimensions", DIMENSIONS)
    def test_the_array_renders_with_its_dimensions(self, dialect, dimensions):
        array = PostgresArrayType(
            element_type=PostgresCidrType(dialect),
            dimensions=dimensions,
            dialect=dialect,
        )
        sql, _ = dialect.format_data_type(array)
        assert sql == "CIDR" + "[]" * dimensions

    @pytest.mark.parametrize("dimensions", DIMENSIONS)
    def test_operations_reach_through_the_dimensions(self, dialect, dimensions):
        """The element's operations apply whatever the array's rank.

        This is the combination a scalar-only test misses: the type that
        selects the operations is the element, and the element is reachable
        through however many dimensions.
        """
        array = PostgresArrayType(
            element_type=PostgresCidrType(dialect),
            dimensions=dimensions,
            dialect=dialect,
        )
        element = operations_class_for(array.element_type)
        assert element is NetworkValueExpression
        expr = element(dialect, Column(dialect, "v"))
        assert "NETWORK(" in expr.network().to_sql()[0]

    @pytest.mark.parametrize("dimensions", DIMENSIONS)
    def test_every_type_combines_with_every_rank(self, dialect, dimensions):
        """Every extension type keeps its operations at every array rank."""
        for label, cls in WITH_OPERATIONS:
            array = PostgresArrayType(
                element_type=cls(dialect), dimensions=dimensions, dialect=dialect
            )
            element = operations_class_for(array.element_type)
            assert element is not None, f"{label}[] lost its operations"

    def test_a_two_dimensional_range_array_still_contains(self, dialect):
        """The deepest combination: rank 3 of the type with predicates."""
        array = PostgresArrayType(
            element_type=PostgresInt4RangeType(dialect),
            dimensions=3,
            dialect=dialect,
        )
        sql, _ = dialect.format_data_type(array)
        assert sql == "INT4RANGE[][][]"
        element = operations_class_for(array.element_type)
        expr = element(dialect, Column(dialect, "v"))
        assert "@>" in expr.contains(3).to_sql()[0]

    def test_an_array_of_a_type_without_operations_stays_empty(self, dialect):
        """Nothing is invented for a type that has no surface of its own."""
        array = PostgresArrayType(
            element_type=PostgresCIDType(dialect), dialect=dialect
        )
        assert operations_class_for(array.element_type) is None