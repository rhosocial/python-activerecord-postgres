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


class PostgresGeometryColumn(GeometryColumn):
    """A column holding a PostgreSQL geometric value (point, line, box, ...).

    Seven server geometric types and the PostGIS ``geometry`` wrapper share one
    operation surface here. ``ST_Distance`` and the overlap operators are the
    server's own spellings and are added to this class as they are measured.
    """


class PostgresVectorColumn(VectorColumn):
    """A column holding a pgvector value.

    The ``<->`` / ``<=>`` / ``<#>`` distance operators belong here; the core
    class states only the shared column surface until they are measured.
    """


class PostgresRangeColumn(RangeColumn):
    """A column holding a native PostgreSQL range (``int4range``, ...).

    ``@>`` / ``<@`` / ``&&`` and ``lower()`` / ``upper()`` are PostgreSQL's own;
    they are added here as the range family is completed.
    """


class PostgresMultirangeColumn(MultirangeColumn):
    """A column holding a native PostgreSQL multirange."""


class PostgresBitStringColumn(BitStringColumn):
    """A column holding ``bit`` / ``varbit``.

    PostgreSQL spells exclusive-or ``#`` where MySQL spells it ``^``, so the
    bitwise family lives on this class rather than on the shared one.
    """


class PostgresTextSearchColumn(ColumnBase):
    """A column holding ``tsvector`` or ``tsquery``.

    ``to_tsvector`` / ``to_tsquery`` / ``@@`` are PostgreSQL's; no core class
    models a text-search value, so this class starts from the column surface.
    """

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
