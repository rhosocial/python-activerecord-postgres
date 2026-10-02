# src/rhosocial/activerecord/backend/impl/postgres/expression/values.py
"""Value expressions for PostgreSQL's own types.

The functions these wrap already existed -- ``network()``, ``range_contains()``,
``hstore_get_value()``, ``ltree_matches()`` -- as free functions in
``impl.postgres.functions``. They were unreachable from a column: a ``cidr``
column had no ``.network()``, an ``int4range`` had no ``.contains()``, and the
only way in was to import the module and spell out the dialect.

So each type gets a value expression carrying its own operations. The functions
are not reimplemented -- what these classes add is what the *result* is, which a
bare FunctionCall cannot say. ``network()`` returns an address and so carries
address operations itself; ``masklen()`` returns an integer and deliberately
does not, because calling ``.network()`` on an integer is a type error in every
backend.

Argument order is taken from the functions rather than from what reads best,
which in two places means the opposite: ``range_contained_by(element, range)``
puts the range second, and ``ltree_descendant(ancestor, descendant)`` reads
backwards from the method name. Both are wrapped so the method reads naturally
and the call site still matches the function.
"""

from typing import Callable, TypeVar

from rhosocial.activerecord.backend.expression.core import SQLValueExpression
from rhosocial.activerecord.backend.expression.operators import BinaryExpression
from rhosocial.activerecord.backend.expression.mixins import (
    AliasableMixin,
    ComparisonMixin,
    StringPatternPredicateMixin,
    TypeCastingMixin,
)
from rhosocial.activerecord.backend.impl.postgres.functions import hstore as _hstore
from rhosocial.activerecord.backend.impl.postgres.functions import ltree as _ltree
from rhosocial.activerecord.backend.impl.postgres.functions import network as _network
from rhosocial.activerecord.backend.impl.postgres.functions import range as _range
from rhosocial.activerecord.backend.expression.core import Literal
from rhosocial.activerecord.backend.expression.types import BigIntType, IntType


def _typed_element(dialect, element_type, element):
    """Type *element* the way the multirange's element type expects it.

    The range operators are defined per element type, and a Python ``int``
    arrives as the narrowest integer PostgreSQL has -- ``smallint`` -- which
    ``int4multirange @> $1`` rejects with

        operator does not exist: int4multirange @> smallint

    The query is right and the parameter is an integer; only the width is
    wrong, and no amount of care at the call site would catch it because
    nothing about a Python int says which PostgreSQL type is wanted. The
    width is taken from the range's own element type, so an int8range gets
    bigint and an int4range gets integer.

    A ``Literal`` wrapping a number is unwrapped and re-typed the same way.
    The width is a property of the range, not of how the caller chose to spell
    the value, and testing the Python type instead meant that passing an
    explicit ``Literal(dialect, 5)`` -- the form everything else in this
    codebase now takes -- silently bound smallint again and took the operator
    down with it.
    """
    if isinstance(element, Literal):
        value = getattr(element, "value", None)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return element
        element = value
    if not isinstance(element, int) or isinstance(element, bool):
        return element
    name = getattr(element_type, "name", "")
    if name == "postgres_array":
        # An array compares elementwise, so its element type is the one that
        # decides the width.
        name = getattr(getattr(element_type, "element_type", None), "name", "")
    # Both spellings are accepted: the element type may be a core type named
    # "integer" or a PostgreSQL one named "postgres_int4", and both mean the
    # same width. Only the width matters here, not which module declared it.
    if name in ("postgres_int4", "integer", "int4", "int"):
        return Literal(dialect, element).cast(IntType(dialect))
    if name in ("postgres_int8", "bigint", "int8"):
        return Literal(dialect, element).cast(BigIntType(dialect))
    return element


_T = TypeVar("_T")
_F = TypeVar("_F", bound=Callable[..., _T])


class _ResultMixinBase:
    """Shared wrapping for the result-type mixins below.

    Only the Python class changes; the node, and therefore the SQL, is left as
    it was. The statement was already correct -- what was missing was the
    knowledge of what it yields.
    """

    def _retype(self, cls):
        return cls(self._dialect, self)


class NetworkResultMixin(_ResultMixinBase):
    """State that an expression yields a PostgreSQL address.

    For a column whose type is not among the ones this backend models -- a
    domain type of the user's, say -- this is how the address operations become
    reachable on it.
    """

    def as_inet(self) -> "NetworkValueExpression":
        """This yields an inet or cidr, whatever the data turns out to be."""
        return self._retype(NetworkValueExpression)

    def as_cidr(self) -> "NetworkValueExpression":
        """This yields a cidr specifically."""
        return self._retype(NetworkValueExpression)


class RangeResultMixin(_ResultMixinBase):
    """State that an expression yields a PostgreSQL range or multirange."""

    def as_range(self) -> "RangeValueExpression":
        """This yields a range, so containment and overlap apply."""
        return self._retype(RangeValueExpression)

    def as_multirange(self) -> "RangeValueExpression":
        """This yields a multirange, whose containment is a different operator."""
        return self._retype(RangeValueExpression)


class HstoreResultMixin(_ResultMixinBase):
    """State that an expression yields an hstore document.

    The accessors return values rather than documents, so they are typed as what
    they are: ``hstore_get_value(h, k)`` is text, and asking it for keys would
    be a type error.
    """

    def as_hstore(self) -> "HstoreValueExpression":
        """This yields an hstore, so the key and value accessors apply."""
        return self._retype(HstoreValueExpression)


class LtreeResultMixin(_ResultMixinBase):
    """State that an expression yields an ltree path or one of its patterns."""

    def as_ltree(self) -> "LtreeValueExpression":
        """This yields an ltree, so the path operations apply."""
        return self._retype(LtreeValueExpression)

    def as_lquery(self) -> "LtreeValueExpression":
        """This yields an lquery, the pattern an ltree is matched against."""
        return self._retype(LtreeValueExpression)

    def as_ltxtquery(self) -> "LtreeValueExpression":
        """This yields an ltxtquery, the label-matching pattern."""
        return self._retype(LtreeValueExpression)


class NetworkValueExpression(
    NetworkResultMixin,
    AliasableMixin,
    ComparisonMixin,
    StringPatternPredicateMixin,
    TypeCastingMixin,
    SQLValueExpression,
):
    """An ``inet`` or ``cidr`` address.

    Carries the address operations, so ``col.network()`` is still an address
    and can be masked or merged -- the chain does not stop at the first call.
    """

    VALUE_FAMILY = "network"

    @property
    def format_method(self) -> str:
        """Render through the wrapped call.

        The wrapper adds operations, not syntax: every one of these renders
        exactly the FunctionCall it holds, so it needs no dialect method of its
        own and cannot drift from the SQL the function module produces.
        """
        return "format_function_call"

    def __init__(self, dialect, call, family=None):
        super().__init__(dialect)
        self.call = call
        self._family = family

    @property
    def args(self):
        """The wrapped call's arguments, so a factory can take this node.

        The function factories build a FunctionCall from whatever they are
        given, and they accept a Column, a Literal or another expression. This
        wrapper is neither of those, so without this the second operation in a
        chain -- ``col.network().masklen()`` -- would have nothing to pass on.
        """
        return self.call.args

    def to_sql(self):
        """Render the wrapped call."""
        return self.call.to_sql()

    def _network_op(self, factory: _F, *args) -> _T:
        """Call one of ``functions.network`` and hand back what it returns.

        Args:
            factory: The function to call, imported by name. A string looked up
                with getattr would be invisible to a checker, and the result
                would arrive as Any -- every chained call unchecked.
            args: Positional arguments forwarded to the factory.

        Returns:
            The factory's result. Which of these is an address and which is a
            scalar is stated by the method that calls this -- by wrapping the
            address ones and declaring the return type -- so there is no list
            here to fall out of step with the methods above.
        """
        return factory(self._dialect, self, *args)

    # --- parts of an address ---

    def network(self) -> "NetworkValueExpression":
        """The network part. ``NETWORK(addr)``"""
        return NetworkValueExpression(
            self._dialect, self._network_op(_network.inet_network))

    def netmask(self) -> "NetworkValueExpression":
        """The netmask. ``NETMASK(addr)``"""
        return NetworkValueExpression(
            self._dialect, self._network_op(_network.inet_netmask))

    def masklen(self) -> "SQLValueExpression":
        """The netmask length in bits. ``MASKLEN(addr)`` -> integer"""
        return self._network_op(_network.inet_masklen)

    def set_mask(self, mask_len: int) -> "NetworkValueExpression":
        """Re-mask to *mask_len* bits. ``SET_MASKLEN(addr, n)``"""
        return NetworkValueExpression(
            self._dialect, self._network_op(_network.inet_set_mask, mask_len))

    # --- set operations ---

    def merge(self, other) -> "NetworkValueExpression":
        """The smallest network containing both. ``NETMERGE(a, b)``"""
        return NetworkValueExpression(
            self._dialect, self._network_op(_network.inet_merge, other))

    def and_mask(self, other) -> "NetworkValueExpression":
        """Bitwise AND of two addresses. ``a & b``"""
        return NetworkValueExpression(
            self._dialect, self._network_op(_network.inet_and, other))

    def or_mask(self, other) -> "NetworkValueExpression":
        """Bitwise OR of two addresses. ``a | b``"""
        return NetworkValueExpression(
            self._dialect, self._network_op(_network.inet_or, other))

    def invert(self) -> "NetworkValueExpression":
        """The complement. ``~addr``"""
        return NetworkValueExpression(
            self._dialect, self._network_op(_network.inetnot))

    def abbrev(self) -> "SQLValueExpression":
        """The abbreviated first-octet form. ``ABBREV(addr)``"""
        return self._network_op(_network.inet_show)


class RangeValueExpression(
    RangeResultMixin,
    AliasableMixin,
    ComparisonMixin,
    StringPatternPredicateMixin,
    TypeCastingMixin,
    SQLValueExpression,
):
    """A range or multirange value.

    Carries containment and overlap, which answer questions about the range
    rather than deriving one, so they return predicates. The operations that
    do derive something -- ``lower``, ``upper`` -- return scalars and are
    therefore plain expressions, not more ranges.

    Takes the range's ``element_type`` when it is known, because that is what
    an element comparison has to be typed against.
    """

    def __init__(self, dialect, call, family=None, element_type=None):
        super().__init__(dialect)
        self.call = call
        self._family = family
        #: The range's element type. Needed because a Python int binds as
        #: smallint, and ``int4range @> smallint`` does not exist.
        self.element_type = element_type

    @property
    def format_method(self) -> str:
        """Render through the wrapped call.

        The wrapper adds operations, not syntax: every one of these renders
        exactly the FunctionCall it holds, so it needs no dialect method of its
        own and cannot drift from the SQL the function module produces.
        """
        return "format_function_call"

    @property
    def args(self):
        """The wrapped call's arguments, so a factory can take this node.

        The function factories build a FunctionCall from whatever they are
        given, and they accept a Column, a Literal or another expression. This
        wrapper is neither of those, so without this the second operation in a
        chain -- ``col.network().masklen()`` -- would have nothing to pass on.
        """
        return self.call.args

    def to_sql(self):
        """Render the wrapped call."""
        return self.call.to_sql()

    def _range_op(self, factory: _F, *args) -> _T:
        """Call a range factory.

        Args:
            factory: The function to call, imported by name.
            args: Positional arguments forwarded to the factory.

        Returns:
            The factory's result.
        """
        return factory(self._dialect, self, *args)

    # --- containment ---

    def contains(self, other) -> "BinaryExpression":
        """The range holds *other*. ``range_contains(range, x)``"""
        return self._range_op(
            _range.range_contains,
            _typed_element(self._dialect, self.element_type, other))

    def contains_range(self, other) -> "BinaryExpression":
        """The range holds *other* whole. ``range_contains(range1, range2)``"""
        return self._range_op(_range.range_contains_range, other)

    def overlaps(self, other) -> "BinaryExpression":
        """The two share at least one point. ``range_overlaps(r1, r2)``"""
        return self._range_op(_range.range_overlaps, other)

    def adjacent(self, other) -> "BinaryExpression":
        """The two touch without overlapping. ``range_adjacent(r1, r2)``"""
        return self._range_op(_range.range_adjacent, other)

    def strictly_left_of(self, other) -> "BinaryExpression":
        """Entirely below *other*. ``range_strictly_left_of(r1, r2)``"""
        return self._range_op(_range.range_strictly_left_of, other)

    def strictly_right_of(self, other) -> "BinaryExpression":
        """Entirely above *other*. ``range_strictly_right_of(r1, r2)``"""
        return self._range_op(_range.range_strictly_right_of, other)

    def merge(self) -> "SQLValueExpression":
        """The smallest range covering every range in a multirange.

        ``range_merge(mr)`` collapses a multirange to one range; it is not a
        two-argument union. PostgreSQL has no operator for "join two ranges",
        because the answer depends on whether they overlap and what to do about
        a gap -- ``range_union`` and ``multirange_merge`` are the two that do
        exist, and both are named for what they take.
        """
        return self._range_op(_range.range_merge)

    def union(self, other) -> "SQLValueExpression":
        """The union of two ranges. ``range_union(r1, r2)``"""
        return self._range_op(_range.range_union, other)

    # --- multiranges, which contain differently ---

    def multirange_contains(self, element) -> "BinaryExpression":
        """The multirange holds *element*. ``multirange_contains(mr, x)``"""
        return self._range_op(
            _range.multirange_contains,
            _typed_element(self._dialect, self.element_type, element))

    def multirange_overlaps(self, other) -> "BinaryExpression":
        """The multirange shares a point with *other*."""
        return self._range_op(_range.multirange_overlaps, other)


class RangeBoundMixin:
    """The two operations that take a range's *element* as the subject.

    ``contained_by`` and ``strictly_right_of`` read backwards from the method
    name in the underlying functions: the function puts the range second, so a
    wrapper that put it first would not compile against it. They live apart
    from :class:`RangeValueExpression` because they belong on the value being
    compared, not on the range -- ``3 <@ r`` is a question about 3.
    """

    def contained_by(self, range_value):
        """This value lies inside *range_value*. ``range_contained_by(x, r)``"""
        return _range.range_contained_by(self._dialect, self, range_value)

    def element_strictly_right_of(self, range_value):
        """This value is above *range_value* entirely."""
        return _range.range_strictly_right_of(self._dialect, range_value, self)


class HstoreValueExpression(
    HstoreResultMixin,
    AliasableMixin,
    ComparisonMixin,
    StringPatternPredicateMixin,
    TypeCastingMixin,
    SQLValueExpression,
):
    """An ``hstore`` key/value document.

    The accessors return values rather than hstores, so they are typed as what
    they are: ``hstore_get_value(h, k)`` is text and calling an hstore
    operation on it would be a type error. Only the key and value lists come
    back as documents.
    """

    @property
    def format_method(self) -> str:
        """Render through the wrapped call.

        The wrapper adds operations, not syntax: every one of these renders
        exactly the FunctionCall it holds, so it needs no dialect method of its
        own and cannot drift from the SQL the function module produces.
        """
        return "format_function_call"

    def __init__(self, dialect, call, family=None):
        super().__init__(dialect)
        self.call = call
        self._family = family

    @property
    def args(self):
        """The wrapped call's arguments, so a factory can take this node.

        The function factories build a FunctionCall from whatever they are
        given, and they accept a Column, a Literal or another expression. This
        wrapper is neither of those, so without this the second operation in a
        chain -- ``col.network().masklen()`` -- would have nothing to pass on.
        """
        return self.call.args

    def to_sql(self):
        """Render the wrapped call."""
        return self.call.to_sql()

    def _hstore_op(self, factory: _F, *args) -> _T:
        """Call an hstore factory.

        Args:
            factory: The function to call, imported by name.
            args: Positional arguments forwarded to the factory.

        Returns:
            The factory's result.
        """
        return factory(self._dialect, self, *args)

    def keys(self) -> "SQLValueExpression":
        """The keys, as an array. ``akeys(h)``

        An array of text rather than a document: nothing can be looked up in it,
        and typing it as an hstore would let ``h.keys().get("k")`` check out.
        """
        return self._hstore_op(_hstore.hstore_akeys)

    def values(self) -> "SQLValueExpression":
        """The values, as an array. ``avals(h)``"""
        return self._hstore_op(_hstore.hstore_avals)

    def get(self, key: str) -> "SQLValueExpression":
        """The value at *key*, or NULL. ``hstore_get_value(h, k)``"""
        return self._hstore_op(_hstore.hstore_get_value, key)

    def get_as_text(self, key: str) -> "SQLValueExpression":
        """The value at *key*, as text. ``->(h, k)``"""
        return self._hstore_op(_hstore.hstore_get_value_as_text, key)

    def subscript(self, key: str) -> "SQLValueExpression":
        """The value at *key*. ``h -> k``"""
        return self._hstore_op(_hstore.hstore_subscript_get, key)

    def has_key(self, key: str):
        """Whether *key* is present. ``exist(h, k)``"""
        return self._hstore_op(_hstore.hstore_exist, key)

    def is_defined(self, key: str):
        """Whether *key* is present and not NULL. ``defined(h, k)``"""
        return self._hstore_op(_hstore.hstore_defined, key)

    def key_exists(self, key: str):
        """Whether *key* is present and non-NULL. ``hstore_key_exists(h, k)``"""
        return self._hstore_op(_hstore.hstore_key_exists, key)

    def to_json(self) -> "SQLValueExpression":
        """The document as json. ``hstore_to_json(h)``"""
        return self._hstore_op(_hstore.hstore_to_json)


class LtreeValueExpression(
    LtreeResultMixin,
    AliasableMixin,
    ComparisonMixin,
    StringPatternPredicateMixin,
    TypeCastingMixin,
    SQLValueExpression,
):
    """An ``ltree`` label path, or the pattern types that match one.

    One class for all three because none of them carries an operation the
    others lack a meaning for: the pattern functions take a tree and a pattern
    and answer a question, and the tree functions take a tree and answer with
    a tree.
    """

    @property
    def format_method(self) -> str:
        """Render through the wrapped call.

        The wrapper adds operations, not syntax: every one of these renders
        exactly the FunctionCall it holds, so it needs no dialect method of its
        own and cannot drift from the SQL the function module produces.
        """
        return "format_function_call"

    def __init__(self, dialect, call, family=None):
        super().__init__(dialect)
        self.call = call
        self._family = family

    @property
    def args(self):
        """The wrapped call's arguments, so a factory can take this node.

        The function factories build a FunctionCall from whatever they are
        given, and they accept a Column, a Literal or another expression. This
        wrapper is neither of those, so without this the second operation in a
        chain -- ``col.network().masklen()`` -- would have nothing to pass on.
        """
        return self.call.args

    def to_sql(self):
        """Render the wrapped call."""
        return self.call.to_sql()

    def _ltree_op(self, factory: _F, *args) -> _T:
        """Call an ltree factory.

        Args:
            factory: The function to call, imported by name.
            args: Positional arguments forwarded to the factory.

        Returns:
            The factory's result. ``subpath`` and ``concat`` are wrapped by the
            methods below, which declare that they are paths again; a string
            looked up here would make the whole chain Any to a checker.
        """
        return factory(self._dialect, self, *args)

    def is_ancestor_of(self, path) -> "BinaryExpression":
        """This path is an ancestor of *path*. ``ltree_ancestor(l, path)``

        Named for what it answers rather than for what it returns: the
        underlying ``ltree_ancestor`` puts the ancestor first, and a method
        called ``ancestors_of`` would read as returning a list of ancestors
        when it returns a predicate about one.
        """
        return self._ltree_op(_ltree.ltree_ancestor, path)

    def is_descendant_of(self, path) -> "BinaryExpression":
        """This path is below *path*. ``ltree_descendant(tree, path)``"""
        return self._ltree_op(_ltree.ltree_descendant, path)

    def matches(self, pattern) -> "BinaryExpression":
        """This path matches an ``lquery``. ``ltree_matches(l, q)``"""
        return self._ltree_op(_ltree.ltree_matches, pattern)

    def matches_text(self, query) -> "BinaryExpression":
        """This path matches an ``ltxtquery``. ``ltree_text_search(l, q)``"""
        return self._ltree_op(_ltree.ltree_text_search, query)

    def nlevel(self) -> "SQLValueExpression":
        """The number of labels. ``nlevel(l)`` -> integer"""
        return self._ltree_op(_ltree.ltree_nlevel)

    def subpath(self, start: int, length=None) -> "LtreeValueExpression":
        """A slice of the path. ``subpath(l, off[, len])``"""
        return LtreeValueExpression(
            self._dialect, self._ltree_op(_ltree.ltree_subpath, start, length))

    def concat(self, other) -> "LtreeValueExpression":
        """Append *other*. ``l || r``"""
        return LtreeValueExpression(
            self._dialect, self._ltree_op(_ltree.ltree_concat, other))

    def lca(self, paths) -> "SQLValueExpression":
        """The longest common ancestor of the given paths."""
        return self._ltree_op(_ltree.ltree_lca, paths)


__all__ = [
    "NetworkValueExpression",
    "RangeValueExpression",
    "RangeBoundMixin",
    "HstoreValueExpression",
    "LtreeValueExpression",
]

# ---------------------------------------------------------------------------
# Typed columns: the only way an extension type's operations are reachable
# ---------------------------------------------------------------------------

#: DataType name -> value expression carrying its operations. Keyed on the
#: dispatch key, which is what a DataType declares, so a type added here needs
#: no registration elsewhere.
_OPERATION_EXPRESSIONS = {
    "postgres_cidr": NetworkValueExpression,
    "postgres_inet": NetworkValueExpression,
    "postgres_macaddr": NetworkValueExpression,
    "postgres_macaddr8": NetworkValueExpression,
    "postgres_hstore": HstoreValueExpression,
    "postgres_ltree": LtreeValueExpression,
    "postgres_lquery": LtreeValueExpression,
    "postgres_ltxtquery": LtreeValueExpression,
}
for _range_name in (
    "postgres_int4range", "postgres_int8range", "postgres_numrange",
    "postgres_tsrange", "postgres_tstzrange", "postgres_daterange",
    "postgres_int4multirange", "postgres_int8multirange",
    "postgres_nummultirange", "postgres_tsmultirange",
    "postgres_tstzmultirange", "postgres_datemultirange",
):
    _OPERATION_EXPRESSIONS[_range_name] = RangeValueExpression


def value_expression_for(data_type, dialect):
    """Return the value expression whose operations *data_type* offers.

    Args:
        data_type: The :class:`DataType` the column holds.
        dialect: The dialect that will render the column.

    Returns:
        A value expression carrying the type's operations, or ``None`` when
        the type has no operation surface of its own -- a ``text`` column is
        served by the core's string operations and needs nothing here.

    Raises:
        ValueError: If *data_type* is not a DataType.
    """
    from rhosocial.activerecord.backend.expression.types._base import DataType

    if not isinstance(data_type, DataType):
        raise ValueError(
            f"expected a DataType, got {type(data_type).__name__}; the column's "
            f"operations are chosen by the type it stores, not by a name."
        )
    expression = _OPERATION_EXPRESSIONS.get(data_type.name)
    if expression is None:
        return None
    return expression(dialect, None)


def operations_class_for(data_type):
    """Return the value expression class whose operations *data_type* offers.

    The class rather than an instance, for a caller that wants to wrap a node
    it already has -- ``operations_class_for(dt)(dialect, some_call)``. Pass a
    type with no operation surface and this gives ``None``, the same answer
    :func:`value_expression_for` gives.

    Args:
        data_type: The :class:`DataType` to look up.

    Returns:
        A value expression class, or ``None``.

    Note:
        Callers that wrap a node should pass ``element_type`` through to the
        constructor; a range needs it to type an element comparison, since a
        Python int binds as smallint and ``int4range @> smallint`` is not an
        operator PostgreSQL has.
    """
    from rhosocial.activerecord.backend.expression.types._base import DataType

    if not isinstance(data_type, DataType):
        raise ValueError(
            f"expected a DataType, got {type(data_type).__name__}; the column's "
            f"operations are chosen by the type it stores, not by a name."
        )
    cls = _OPERATION_EXPRESSIONS.get(data_type.name)
    if cls is None:
        return None
    return cls
