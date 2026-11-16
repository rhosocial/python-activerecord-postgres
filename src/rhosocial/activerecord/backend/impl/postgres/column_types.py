# src/rhosocial/activerecord/backend/impl/postgres/column_types.py
"""PostgreSQL's own column classes.

A column class is an *operation surface*: it says what a value held in a column
can be asked to do. Core narrows by the Python type (``str`` -> StringColumn,
``dict`` -> JSONColumn), and those classes deliberately promise only what every
backend can execute. PostgreSQL carries value families the framework does not
have a core class for -- text search, ``jsonpath``, hstore, LSN, the object
identifiers -- and richer spellings for the ones it does (PostGIS, the range
operators, ``macaddr``).

This module is where those names live, so a model can choose one explicitly::

    Annotated[Point, UseColumnType(PostgresGeometryColumn())]

Each class inherits the core class that already carries the right operation
floor (``PostgresGeometryColumn(GeometryColumn)``), or ``ColumnBase`` when the
family has no core class at all (text search, hstore, LSN). The server's own
operations -- ``ST_Distance``, ``@>``, ``justify_hours`` -- are added here as
they are measured; until then the class states the same floor the core class
does, which is the honest minimum rather than a promise the server refuses.

``POSTGRES_EXTRA_COLUMN_TYPES`` in :mod:`.mixins.column_type` keys these by the
value class they answer for, so a field annotated with one of PostgreSQL's own
value types resolves without a declaration.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.expression.column_types import (
    BitStringColumn,
    ColumnBase,
    GeometryColumn,
    IntegerColumn,
    MacAddressColumn,
    MultirangeColumn,
    NumericColumn,
    RangeColumn,
    VectorColumn,
    XMLColumn,
)

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.operators import (
        BinaryExpression,
        UnaryExpression,
    )
    from rhosocial.activerecord.backend.expression.functions import FunctionCall


def _require(dialect, switch: str) -> None:
    """Fail loudly, and by name, when the server does not answer a capability.

    A column method must never render an operator the server has no switch
    for: that would produce SQL which fails on the first query, with the error
    pointing at the column instead of at the missing capability.
    """
    answer = getattr(dialect, switch, None)
    if answer is None or not answer():
        raise UnsupportedFeatureError(
            type(dialect).__name__,
            switch,
            f"this operation needs the {switch}() capability",
        )


class PostgresGeometryColumn(GeometryColumn):
    """A column holding a PostgreSQL geometric value (point, line, box, ...).

    Seven server geometric types and the PostGIS ``geometry`` wrapper share one
    operation surface here. ``ST_Distance`` and the overlap operators are the
    server's own spellings and are added to this class as they are measured.
    """

    def distance_to(self, other) -> "BinaryExpression":
        """``col <-> other``: the distance between two geometric values.

        ``<->`` is the one geometric operator every native geometric type
        answers (point, line, lseg, box, path, polygon, circle), so it is safe
        on this shared class.
        """
        from .functions.geometric import geometry_distance

        return geometry_distance(self._dialect, self, other)


class PostgresAreaGeometryColumn(PostgresGeometryColumn):
    """A geometry column whose value can contain and overlap another value.

    ``@>`` / ``<@`` / ``&&`` exist for boxes, polygons and circles, not for
    points, lines or segments. Keeping them on this narrower class means a
    ``Point`` field cannot offer an operator the server would refuse.
    """

    def contains(self, other) -> "BinaryExpression":
        """``col @> other``: whether this geometry contains the other."""
        from .functions.geometric import geometry_contains

        return geometry_contains(self._dialect, self, other)

    def contained_by(self, other) -> "BinaryExpression":
        """``col <@ other``: whether this geometry is contained by the other."""
        from .functions.geometric import geometry_contained_by

        return geometry_contained_by(self._dialect, self, other)

    def overlaps(self, other) -> "BinaryExpression":
        """``col && other``: whether the two geometries overlap."""
        from .functions.geometric import geometry_overlaps

        return geometry_overlaps(self._dialect, self, other)


class PostgresVectorColumn(VectorColumn):
    """A column holding a pgvector value.

    The ``<->`` / ``<=>`` / ``<#>`` distance operators belong here; the core
    class states only the shared column surface until they are measured.
    """

    def l2_distance(self, other) -> "BinaryExpression":
        """``col <-> other``: Euclidean distance."""
        _require(self._dialect, "supports_pgvector_similarity_search")
        from .functions.pgvector import vector_l2_distance

        return vector_l2_distance(self._dialect, self, other)

    def cosine_distance(self, other) -> "BinaryExpression":
        """``col <=> other``: cosine distance."""
        _require(self._dialect, "supports_pgvector_similarity_search")
        from .functions.pgvector import vector_cosine_distance

        return vector_cosine_distance(self._dialect, self, other)

    def inner_product(self, other) -> "BinaryExpression":
        """``col <#> other``: negative inner product."""
        _require(self._dialect, "supports_pgvector_similarity_search")
        from .functions.pgvector import vector_inner_product

        return vector_inner_product(self._dialect, self, other)


class PostgresRangeColumn(RangeColumn):
    """A column holding a native PostgreSQL range (``int4range``, ...).

    ``@>`` / ``<@`` / ``&&`` and ``lower()`` / ``upper()`` are PostgreSQL's own;
    they are added here as the range family is completed.
    """

    def contains(self, other) -> "BinaryExpression":
        """``col @> other``: does the range contain the element or range?"""
        _require(self._dialect, "supports_range_operators")
        from .functions.range import range_contains

        return range_contains(self._dialect, self, other)

    def contained_by(self, other) -> "BinaryExpression":
        """``col <@ other``."""
        _require(self._dialect, "supports_range_operators")
        from .functions.range import range_contained_by

        return range_contained_by(self._dialect, self, other)

    def overlaps(self, other) -> "BinaryExpression":
        """``col && other``."""
        _require(self._dialect, "supports_range_operators")
        from .functions.range import range_overlaps

        return range_overlaps(self._dialect, self, other)

    def is_empty(self) -> "FunctionCall":
        """``ISEMPTY(col)``."""
        _require(self._dialect, "supports_range_functions")
        from .functions.range import range_is_empty

        return range_is_empty(self._dialect, self)

    def lower(self) -> "FunctionCall":
        """``LOWER(col)``."""
        _require(self._dialect, "supports_range_functions")
        from .functions.range import range_lower

        return range_lower(self._dialect, self)

    def upper(self) -> "FunctionCall":
        """``UPPER(col)``."""
        _require(self._dialect, "supports_range_functions")
        from .functions.range import range_upper

        return range_upper(self._dialect, self)


class PostgresMultirangeColumn(MultirangeColumn):
    """A column holding a native PostgreSQL multirange."""

    def contains(self, other) -> "BinaryExpression":
        """``col @> other``."""
        _require(self._dialect, "supports_multirange")
        from .functions.range import multirange_contains

        return multirange_contains(self._dialect, self, other)

    def contained_by(self, other) -> "BinaryExpression":
        """``col <@ other``."""
        _require(self._dialect, "supports_multirange")
        from .functions.range import multirange_is_contained_by

        return multirange_is_contained_by(self._dialect, self, other)

    def overlaps(self, other) -> "BinaryExpression":
        """``col && other``."""
        _require(self._dialect, "supports_multirange")
        from .functions.range import multirange_overlaps

        return multirange_overlaps(self._dialect, self, other)


class PostgresBitStringColumn(BitStringColumn):
    """A column holding ``bit`` / ``varbit``.

    PostgreSQL spells exclusive-or ``#`` where MySQL spells it ``^``, so the
    bitwise family lives on this class rather than on the shared one.
    """

    def bit_and(self, other) -> "BinaryExpression":
        """``col & other``."""
        from .functions.bit_string import bit_and

        return bit_and(self._dialect, self, other)

    def bit_or(self, other) -> "BinaryExpression":
        """``col | other``."""
        from .functions.bit_string import bit_or

        return bit_or(self._dialect, self, other)

    def bit_xor(self, other) -> "BinaryExpression":
        """``col # other`` -- PostgreSQL's spelling of bitwise XOR."""
        from .functions.bit_string import bit_xor

        return bit_xor(self._dialect, self, other)

    def bit_not(self) -> "UnaryExpression":
        """``~ col``."""
        from .functions.bit_string import bit_not

        return bit_not(self._dialect, self)

    def bit_length(self) -> "FunctionCall":
        """``BIT_LENGTH(col)``, not the generic ``LENGTH``."""
        from .functions.bit_string import bit_length_func

        return bit_length_func(self._dialect, self)

    def bit_count(self) -> "FunctionCall":
        """``BIT_COUNT(col)``: the number of set bits."""
        from .functions.bit_string import bit_count

        return bit_count(self._dialect, self)

    def shift_left(self, n: int) -> "BinaryExpression":
        """``col << n``."""
        from .functions.bit_string import bit_shift_left

        return bit_shift_left(self._dialect, self, n)

    def shift_right(self, n: int) -> "BinaryExpression":
        """``col >> n``."""
        from .functions.bit_string import bit_shift_right

        return bit_shift_right(self._dialect, self, n)


class PostgresTextSearchColumn(ColumnBase):
    """A column holding ``tsvector`` or ``tsquery``.

    ``to_tsvector`` / ``to_tsquery`` / ``@@`` are PostgreSQL's; no core class
    models a text-search value, so this class starts from the column surface.
    """

    def matches(self, query) -> "BinaryExpression":
        """``col @@ query``: does the vector match the query?"""
        from .functions.text_search import ts_matches

        return ts_matches(self._dialect, self, query)

    def rank(self, query, weights=None) -> "FunctionCall":
        """``ts_rank(col, query[, weights])``: the relevance of the match."""
        from .functions.text_search import ts_rank

        return ts_rank(self._dialect, self, query, weights)

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this column."""
        return "format_column"


class PostgresJsonPathColumn(ColumnBase):
    """A column holding ``jsonpath``.

    The type has no core counterpart: it is a path expression, not a document,
    so it neither navigates like JSON nor compares like text.
    """

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this column."""
        return "format_column"


class PostgresHstoreColumn(ColumnBase):
    """A column holding ``hstore``: a key/value map.

    The ``->`` accessor and the ``@>`` containment operator belong here; the
    class starts from the column surface.
    """

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this column."""
        return "format_column"


class PostgresXmlColumn(XMLColumn):
    """A column holding PostgreSQL ``xml``.

    Inherits the core XML surface: casting and null tests, but no comparison --
    ``xml = xml`` is refused by this server too.
    """


class PostgresMacAddressColumn(MacAddressColumn):
    """A column holding ``macaddr`` or ``macaddr8``."""


class PostgresMoneyColumn(NumericColumn):
    """A column holding ``money``.

    It inherits the numeric surface, which is what the server gives it, and the
    currency/locale facts stay with the DDL layer.
    """


class PostgresIdentifierColumn(IntegerColumn):
    """A column holding an object identifier (``oid``, ``regclass``, ``xid``, ...).

    The registry names are OID aliases, so they carry the whole-number surface
    while the spelling of the declaration is the DDL layer's job.
    """


class PostgresLsnColumn(ColumnBase):
    """A column holding ``pg_lsn``: a log sequence number.

    The type compares and subtracts like a number, but it is a position in the
    write-ahead log rather than a value to compute with, so no core numeric
    surface is claimed here.
    """

    @property
    def format_method(self) -> str:
        """The dialect formatting method that renders this column."""
        return "format_column"


__all__ = [
    "PostgresGeometryColumn",
    "PostgresAreaGeometryColumn",
    "PostgresVectorColumn",
    "PostgresRangeColumn",
    "PostgresMultirangeColumn",
    "PostgresBitStringColumn",
    "PostgresTextSearchColumn",
    "PostgresJsonPathColumn",
    "PostgresHstoreColumn",
    "PostgresXmlColumn",
    "PostgresMacAddressColumn",
    "PostgresMoneyColumn",
    "PostgresIdentifierColumn",
    "PostgresLsnColumn",
]
