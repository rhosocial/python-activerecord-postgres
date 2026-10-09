# src/rhosocial/activerecord/backend/impl/postgres/mixins/types/data_type_formatting.py
"""PostgreSQL DataType formatting and parsing mixin."""

from __future__ import annotations

import re
from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError
from rhosocial.activerecord.backend.dialect.mixins.ddl_type import DDLTypeMixin
from rhosocial.activerecord.backend.dialect.protocols import DDLTypeSupport
from rhosocial.activerecord.backend.expression.types import (
    ArrayType,
    BigIntType,
    BlobType,
    BooleanType,
    CharType,
    DataType,
    DateType,
    DateTimeType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    IntervalType,
    JsonBType,
    JsonType,
    RealType,
    SmallIntType,
    TextType,
    TimeType,
    TimeTzType,
    TimestampType,
    TimestampTzType,
    TinyIntType,
    VarCharType,
    XmlType,
)
from ...expression.types import PostgresArrayType

class PostgresTypeFormatSupportMixin(DDLTypeMixin, DDLTypeSupport):
    """PostgreSQL DataType formatting and parsing.

    Implements ``DDLTypeSupport`` so the dialect can render ``DataType``
    expressions to SQL strings and parse raw SQL type strings back into
    ``DataType`` instances.
    """

    if TYPE_CHECKING:
        name: str
        version: Tuple[int, int, int]

    # ------------------------------------------------------------------
    # DDLTypeSupport — formatting
    # ------------------------------------------------------------------

    # --- PostgreSQL-specific formatters ---

    def format_data_type_postgres_enum(self, data_type) -> Tuple[str, tuple]:
        """Render a PostgresEnumType reference for DDL column definitions.

        PostgreSQL ENUM types are referenced by name in column definitions;
        the CREATE TYPE ... AS ENUM is a separate DDL operation.
        """
        return data_type.name, ()

    def format_data_type_postgres_bytea(self, data_type) -> Tuple[str, tuple]:
        return "BYTEA", ()

    def format_data_type_postgres_smallserial(self, data_type) -> Tuple[str, tuple]:
        return "SMALLSERIAL", ()

    def format_data_type_postgres_serial(self, data_type) -> Tuple[str, tuple]:
        return "SERIAL", ()

    def format_data_type_postgres_bigserial(self, data_type) -> Tuple[str, tuple]:
        return "BIGSERIAL", ()

    def format_data_type_postgres_uuid(self, data_type) -> Tuple[str, tuple]:
        return "UUID", ()

    def format_data_type_postgres_xml(self, data_type) -> Tuple[str, tuple]:
        return "XML", ()

    def format_data_type_postgres_tsvector(self, data_type) -> Tuple[str, tuple]:
        return "TSVECTOR", ()

    def format_data_type_postgres_tsquery(self, data_type) -> Tuple[str, tuple]:
        return "TSQUERY", ()

    def format_data_type_postgres_jsonpath(self, data_type) -> Tuple[str, tuple]:
        return "JSONPATH", ()

    def format_data_type_postgres_bit(self, data_type) -> Tuple[str, tuple]:
        if data_type.n is not None:
            return f"BIT({data_type.n})", ()
        return "BIT", ()

    def format_data_type_postgres_varbit(self, data_type) -> Tuple[str, tuple]:
        if data_type.n is not None:
            return f"VARBIT({data_type.n})", ()
        return "VARBIT", ()

    def format_data_type_postgres_inet(self, data_type) -> Tuple[str, tuple]:
        return "INET", ()

    def format_data_type_postgres_cidr(self, data_type) -> Tuple[str, tuple]:
        return "CIDR", ()

    def format_data_type_postgres_macaddr(self, data_type) -> Tuple[str, tuple]:
        return "MACADDR", ()

    def format_data_type_postgres_macaddr8(self, data_type) -> Tuple[str, tuple]:
        return "MACADDR8", ()

    def format_data_type_postgres_point(self, data_type) -> Tuple[str, tuple]:
        return "POINT", ()

    def format_data_type_postgres_line(self, data_type) -> Tuple[str, tuple]:
        return "LINE", ()

    def format_data_type_postgres_line_segment(self, data_type) -> Tuple[str, tuple]:
        return "LSEG", ()

    def format_data_type_postgres_box(self, data_type) -> Tuple[str, tuple]:
        return "BOX", ()

    def format_data_type_postgres_path(self, data_type) -> Tuple[str, tuple]:
        return "PATH", ()

    def format_data_type_postgres_polygon(self, data_type) -> Tuple[str, tuple]:
        return "POLYGON", ()

    def format_data_type_postgres_circle(self, data_type) -> Tuple[str, tuple]:
        return "CIRCLE", ()

    def format_data_type_postgres_money(self, data_type) -> Tuple[str, tuple]:
        return "MONEY", ()

    def format_data_type_postgres_int4range(self, data_type) -> Tuple[str, tuple]:
        return "INT4RANGE", ()

    def format_data_type_postgres_int8range(self, data_type) -> Tuple[str, tuple]:
        return "INT8RANGE", ()

    def format_data_type_postgres_numrange(self, data_type) -> Tuple[str, tuple]:
        return "NUMRANGE", ()

    def format_data_type_postgres_tsrange(self, data_type) -> Tuple[str, tuple]:
        return "TSRANGE", ()

    def format_data_type_postgres_tstzrange(self, data_type) -> Tuple[str, tuple]:
        return "TSTZRANGE", ()

    def format_data_type_postgres_daterange(self, data_type) -> Tuple[str, tuple]:
        return "DATERANGE", ()

    def _format_postgres_multirange_data_type(self, sql: str) -> Tuple[str, tuple]:
        if self.version < (14, 0, 0):
            raise UnsupportedFeatureError(
                self.name,
                sql,
                suggestion="requires PostgreSQL 14+",
            )
        return sql, ()

    def format_data_type_postgres_int4multirange(self, data_type) -> Tuple[str, tuple]:
        return self._format_postgres_multirange_data_type("INT4MULTIRANGE")

    def format_data_type_postgres_int8multirange(self, data_type) -> Tuple[str, tuple]:
        return self._format_postgres_multirange_data_type("INT8MULTIRANGE")

    def format_data_type_postgres_nummultirange(self, data_type) -> Tuple[str, tuple]:
        return self._format_postgres_multirange_data_type("NUMMULTIRANGE")

    def format_data_type_postgres_tsmultirange(self, data_type) -> Tuple[str, tuple]:
        return self._format_postgres_multirange_data_type("TSMULTIRANGE")

    def format_data_type_postgres_tstzmultirange(self, data_type) -> Tuple[str, tuple]:
        return self._format_postgres_multirange_data_type("TSTZMULTIRANGE")

    def format_data_type_postgres_datemultirange(self, data_type) -> Tuple[str, tuple]:
        return self._format_postgres_multirange_data_type("DATEMULTIRANGE")

    def format_data_type_postgres_oid(self, data_type) -> Tuple[str, tuple]:
        return "OID", ()

    def format_data_type_postgres_regclass(self, data_type) -> Tuple[str, tuple]:
        return "REGCLASS", ()

    def format_data_type_postgres_regtype(self, data_type) -> Tuple[str, tuple]:
        return "REGTYPE", ()

    def format_data_type_postgres_xid(self, data_type) -> Tuple[str, tuple]:
        return "XID", ()

    def format_data_type_postgres_xid8(self, data_type) -> Tuple[str, tuple]:
        return "XID8", ()

    def format_data_type_postgres_cid(self, data_type) -> Tuple[str, tuple]:
        return "CID", ()

    def format_data_type_postgres_tid(self, data_type) -> Tuple[str, tuple]:
        return "TID", ()

    def format_data_type_postgres_pg_lsn(self, data_type) -> Tuple[str, tuple]:
        return "PG_LSN", ()

    def format_data_type_postgres_hstore(self, data_type) -> Tuple[str, tuple]:
        return "HSTORE", ()

    def format_data_type_postgres_geometry(self, data_type) -> Tuple[str, tuple]:
        return "GEOMETRY", ()

    def format_data_type_postgres_geography(self, data_type) -> Tuple[str, tuple]:
        return "GEOGRAPHY", ()

    def format_data_type_postgres_vector(self, data_type) -> Tuple[str, tuple]:
        return f"VECTOR({data_type.dim})", ()

    def format_data_type_postgres_halfvec(self, data_type) -> Tuple[str, tuple]:
        return f"HALFVEC({data_type.dim})", ()

    def format_data_type_postgres_sparsevec(self, data_type) -> Tuple[str, tuple]:
        return f"SPARSEVEC({data_type.dim})", ()

    def format_data_type_postgres_citext(self, data_type) -> Tuple[str, tuple]:
        return "CITEXT", ()

    def format_data_type_postgres_cube(self, data_type) -> Tuple[str, tuple]:
        return "CUBE", ()

    def format_data_type_postgres_ltree(self, data_type) -> Tuple[str, tuple]:
        return "LTREE", ()

    def format_data_type_postgres_lquery(self, data_type) -> Tuple[str, tuple]:
        return "LQUERY", ()

    def format_data_type_postgres_ltxtquery(self, data_type) -> Tuple[str, tuple]:
        return "LTXTQUERY", ()

    def format_data_type_postgres_raster(self, data_type) -> Tuple[str, tuple]:
        return "RASTER", ()

    def format_data_type_postgres_array(self, data_type: ArrayType) -> Tuple[str, tuple]:
        element_sql, _ = self.format_data_type(data_type.element_type)
        return element_sql + "[]" * data_type.dimensions, ()

    # --- Core formatters (PostgreSQL specialized) ---

    def _refuse_unsigned_integer(
        self,
        expr,
        word: str,
    ) -> None:
        """Refuse ``unsigned=True``, because PostgreSQL has no unsigned integer.

        The core integer concepts carry signedness as a field rather than as a
        class, so ``IntegerType(unsigned=True)`` is constructible and the flag
        reaches the formatter. PostgreSQL's honest answer is a refusal:

        * The numeric-types chapter enumerates the integer types as ``smallint``
          / ``int2`` (-32768 to 32767), ``integer`` / ``int4``
          (-2147483648 to 2147483647) and ``bigint`` / ``int8``, each with one
          symmetric range, and there is no unsigned entry anywhere in it.
          https://www.postgresql.org/docs/current/datatype-numeric.html
        * ``UNSIGNED`` is an attribute MySQL and MariaDB define. PostgreSQL's
          ``CREATE TABLE`` grammar has no such attribute to place after a type
          name, so there is not even a spelling that would parse.

        Writing a bare ``INTEGER`` for an unsigned request would create a column
        that accepts the negatives the caller declared it would not, and report
        success: the same silent data loss as accepting the flag and discarding
        it, which is what this replaces.

        The signed range does cover the unsigned range, so nothing is lost by
        dropping the *concept* here; what would be lost is the caller's
        statement that this column holds no negative value, and that belongs in
        a ``CHECK`` constraint.
        """
        if not expr.unsigned:
            return
        raise UnsupportedFeatureError(
            self.name,
            f"an unsigned {word} column "
            f"(PostgreSQL has no unsigned integer type; {word} has one "
            f"symmetric signed range and no unsigned variant)",
            suggestion=(
                "Declare the column signed and enforce the range with a CHECK "
                "constraint if negatives must be rejected."
            ),
        )

    def _refuse_unsigned_numeric(self, expr, word: str) -> None:
        """Refuse ``unsigned=True`` on ``DECIMAL``, ``FLOAT``, ``REAL`` or
        ``DOUBLE``.

        The same field reaches these four concepts that it reaches the four
        integer widths -- ``DecimalType``, ``FloatType``, ``RealType`` and
        ``DoubleType`` each carry ``unsigned`` in ``PARAMETERS``, so two
        declarations differing only in it are *different columns* as far as the
        schema differ is concerned -- and PostgreSQL's answer is the same refusal
        as :meth:`_refuse_unsigned_integer`, for its own reasons:

        * **The type list is closed and carries no signedness.**  PostgreSQL's
          Table 8.2 enumerates the numeric types as ``smallint``, ``integer``,
          ``bigint``, ``decimal``, ``numeric``, ``real``, ``double precision``
          and the three ``serial`` conveniences.  ``decimal``/``numeric`` are
          "variable, user-specified precision, exact, up to 131072 digits before
          the decimal point; up to 16383 digits after the decimal point";
          ``real`` is "4 bytes, variable-precision, inexact, 6 decimal digits
          precision"; ``double precision`` is "8 bytes ... 15 decimal digits
          precision".  There is no unsigned row for any of them, and a closed
          table is what makes the absence evidence rather than a gap.
          https://www.postgresql.org/docs/current/datatype-numeric.html
        * **The storages are signed by construction, not by declaration.**
          ``real`` and ``double precision`` are "implementations of IEEE
          Standard 754 for Binary Floating-Point Arithmetic (single and double
          precision, respectively)", and IEEE 754 binary formats carry a signed
          exponent in their field layout -- there is no unsigned binary format to
          select.  Their documented ranges are signed on both sides: ``real``
          "around 1E-37 to 1E+37", ``double precision`` "around 1E-307 to
          1E+308".  ``numeric`` is signed just as concretely: its documented
          special values include ``Infinity`` **and** ``-Infinity``, and a type
          that admits ``-Infinity`` is not a type with an unsigned form.
        * **There is not even a spelling that would parse.**  PostgreSQL accepts
          the standard's ``float`` / ``float(p)`` notations for an inexact type
          and its own ``numeric(precision, scale)``, but ``CREATE TABLE``'s
          grammar has no attribute slot after the type name in which
          ``UNSIGNED`` could go.  ``UNSIGNED`` is an attribute MySQL and MariaDB
          define; PostgreSQL does not, and documents no such attribute.

        Writing a bare ``DECIMAL`` or ``DOUBLE PRECISION`` for an unsigned
        request would create a column that accepts the negatives the caller
        declared it would not, and report success: the same silent loss as
        accepting the flag and discarding it, which is what this replaces.

        ``word`` is the word PostgreSQL writes, so the message can say both what
        the caller asked for and what it would otherwise have written.

        ``UnsupportedFeatureError``, not ``ValueError``: this is a declaration
        the grammar cannot express at all rather than a wrong value, and the two
        exceptions do not share a base class.
        """
        if not getattr(expr, "unsigned", False):
            return
        raise UnsupportedFeatureError(
            self.name,
            f"an unsigned {word} column "
            f"(PostgreSQL has no unsigned floating-point or fixed-point type: "
            f"Table 8.2 lists decimal/numeric, real and double precision with no "
            f"unsigned variant, real and double precision are IEEE 754 binary "
            f"formats whose documented ranges are signed on both sides, numeric's "
            f"own special values include -Infinity, and CREATE TABLE has no "
            f"attribute slot after the type name in which UNSIGNED could go)",
            suggestion=(
                "Declare the column signed and enforce the range with a CHECK "
                "constraint if negatives must be rejected."
            ),
        )

    def format_data_type_tinyint(self, data_type: TinyIntType) -> Tuple[str, tuple]:
        """PostgreSQL has neither ``TINYINT`` nor ``INT1`` — no 1-byte integer
        exists — so the concept is *widened* to the next size that does, and
        the rendered column is a ``SMALLINT``. Both spellings are accepted
        because the caller asked for one concept, and refusing would leave
        ``TinyIntType`` unusable on this backend without telling them anything
        a widened column does not already say.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_integer`, which is where the reason is written
        down."""
        self._check_spelling(data_type, TinyIntType)
        self._refuse_unsigned_integer(data_type, "SMALLINT")
        return "SMALLINT", ()

    def format_data_type_smallint(self, data_type: SmallIntType) -> Tuple[str, tuple]:
        """Both ``SMALLINT`` and ``INT2`` are PostgreSQL spellings of this
        concept, so both render as written.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_integer`."""
        self._check_spelling(data_type, SmallIntType)
        self._refuse_unsigned_integer(data_type, data_type.spelling.upper())
        return data_type.spelling.upper(), ()

    def format_data_type_integer(self, data_type: IntegerType) -> Tuple[str, tuple]:
        """PostgreSQL writes this concept both ways, ``INT`` and ``INTEGER``, and
        the two are distinct renderings of one 4-byte signed integer — so the
        spelling is honoured rather than normalised.

        ``INT4`` is refused, and now **explicitly**. It used to be refused by
        absence — it was simply not on the concept's list — but it has a
        documentation page on MySQL and on MariaDB, so it belongs to the
        union, and the union is what the class carries. The list is therefore
        every dialect's vocabulary; picking the subset this one *writes* is the
        dialect's job, and PostgreSQL's subset is ``("integer", "int")``:
        ``INT4`` is an internal catalog name, not a word its ``CREATE TABLE``
        grammar accepts. Without this the formatter would have started emitting
        ``INT4`` the moment the list grew.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_integer`."""
        self._check_spelling(data_type, ("integer", "int"))
        self._refuse_unsigned_integer(data_type, data_type.spelling.upper())
        return data_type.spelling.upper(), ()

    def format_data_type_bigint(self, data_type: BigIntType) -> Tuple[str, tuple]:
        """``BIGINT`` and ``INT8`` are both PostgreSQL spellings of this
        concept, so both render as written.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_integer`."""
        self._check_spelling(data_type, BigIntType)
        self._refuse_unsigned_integer(data_type, data_type.spelling.upper())
        return data_type.spelling.upper(), ()

    def format_data_type_float(self, data_type: FloatType) -> Tuple[str, tuple]:
        """``FLOAT(p)`` for a declared precision, ``REAL`` for none.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_numeric`, which is where the reason is written
        down.  The refusal comes first so it cannot be masked by the precision
        check below, and so an unsigned request is answered the same way whether
        or not it also named a precision.
        """
        self._refuse_unsigned_numeric(data_type, "FLOAT")
        if data_type.precision is not None:
            if not (1 <= data_type.precision <= 53):
                raise ValueError(
                    f"FLOAT precision must be between 1 and 53 "
                    f"(PostgreSQL limit), got {data_type.precision}"
                )
            return f"FLOAT({data_type.precision})", ()
        return "REAL", ()

    def format_data_type_real(self, data_type: RealType) -> Tuple[str, tuple]:
        """``REAL``, and ``unsigned`` refused rather than ignored; see
        :meth:`_refuse_unsigned_numeric`, which is where the reason is written
        down.

        ``REAL`` is refused for the same reason as ``DOUBLE PRECISION``, and by
        the same helper: PostgreSQL's numeric-types chapter gives ``real`` a
        single documented row -- "4 bytes, variable-precision, inexact, 6
        decimal digits precision", an IEEE 754 binary32 whose range is signed on
        both sides -- and a closed table with no unsigned row is what makes the
        absence evidence rather than a gap.
        https://www.postgresql.org/docs/current/datatype-numeric.html

        The refusal is checked before anything else could answer for it, and
        there is nothing else here to check: ``RealType`` carries ``unsigned``
        as its only parameter, and ``REAL`` is one of the two words PostgreSQL
        writes verbatim, so nothing about a *signed* declaration moves.
        """
        self._refuse_unsigned_numeric(data_type, "REAL")
        return "REAL", ()

    def format_data_type_double(self, data_type: DoubleType) -> Tuple[str, tuple]:
        """``double`` and ``double precision`` are both PostgreSQL spellings,
        and PostgreSQL writes the long one itself — so both render as
        ``DOUBLE PRECISION`` rather than the bare ``DOUBLE``, which is valid
        but not what the server reports back.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_numeric`."""
        self._check_spelling(data_type, DoubleType)
        self._refuse_unsigned_numeric(data_type, "DOUBLE PRECISION")
        return "DOUBLE PRECISION", ()

    def format_data_type_decimal(self, data_type: DecimalType) -> Tuple[str, tuple]:
        """``DECIMAL``, ``NUMERIC`` and ``DEC`` are interchangeable in
        PostgreSQL — the manual states they are synonyms, not variants — so all
        three are accepted and the declared one is rendered verbatim.

        ``unsigned`` is refused rather than ignored; see
        :meth:`_refuse_unsigned_numeric`, which is where the reason is written
        down.  It is checked **before** the precision and scale checks below, and
        that ordering is deliberate rather than incidental: the flag is part of
        this concept's identity, so an unsigned request is answered the same way
        whether or not it also named numbers this grammar could not have taken.
        The scale-without-precision refusal and the two ``ValueError`` range
        checks below are untouched and still fire for a signed declaration."""
        self._check_spelling(data_type, DecimalType)
        self._refuse_unsigned_numeric(data_type, "DECIMAL")
        if data_type.precision is not None:
            if not (1 <= data_type.precision <= 1000):
                raise ValueError(
                    f"DECIMAL precision must be between 1 and 1000 "
                    f"(PostgreSQL limit), got {data_type.precision}"
                )
        if data_type.scale is not None:
            if not (0 <= data_type.scale <= data_type.precision if data_type.precision else True):
                raise ValueError(
                    f"DECIMAL scale must be between 0 and precision "
                    f"({data_type.precision}), got {data_type.scale}"
                )
        if data_type.scale is not None and data_type.precision is None:
            # PostgreSQL's grammar is `numeric(precision, scale)`: the scale is
            # the second argument, so there is nowhere to put it without a
            # precision. `numeric(3)` means scale 0, so the alternative reading
            # -- "scale with the default precision" -- does not exist either.
            # Dropping the scale silently would hand back a column with unlimited
            # integer digits, so say what is missing instead.
            # https://www.postgresql.org/docs/current/datatype-numeric.html
            #
            # ``UnsupportedFeatureError`` rather than ``ValueError``, and the
            # distinction is not cosmetic -- the two do not share a base class,
            # so a caller catching one cannot catch the other. The rule across
            # the backends: a value that is *wrong* raises ``ValueError`` (the
            # range checks below), while a declaration this grammar cannot
            # express at all raises ``UnsupportedFeatureError``, which carries
            # the dialect name and a route forward. Every other backend answers
            # the identical "scale without precision" case the same way; this
            # used to be the one that did not.
            raise UnsupportedFeatureError(
                self.name,
                f"a DECIMAL with scale={data_type.scale} but no precision "
                f"(PostgreSQL's grammar is numeric(precision, scale) -- there is "
                f"nowhere to put a bare scale, and numeric(precision) alone means "
                f"scale 0, not the scale requested)",
                suggestion=(
                    "Declare the precision as well: numeric(precision, scale)."
                ),
            )
        if data_type.precision is not None and data_type.scale is not None:
            return f"DECIMAL({data_type.precision},{data_type.scale})", ()
        if data_type.precision is not None:
            return f"DECIMAL({data_type.precision})", ()
        return "DECIMAL", ()

    def format_data_type_char(self, data_type: CharType) -> Tuple[str, tuple]:
        """``CHARACTER`` is PostgreSQL's own long form and is rendered as
        written, since PostgreSQL emits it back from the catalog."""
        self._check_spelling(data_type, CharType)
        width = f"({data_type.length})" if data_type.length is not None else ""
        return f"{data_type.spelling.upper()}{width}", ()

    def format_data_type_varchar(self, data_type: VarCharType) -> Tuple[str, tuple]:
        """PostgreSQL writes this concept both ways — and emits
        ``character varying`` from the catalog, which is why the long form is a
        spelling of the core type rather than a class of its own."""
        self._check_spelling(data_type, VarCharType)
        width = f"({data_type.length})" if data_type.length is not None else ""
        if data_type.spelling == "character varying":
            return f"CHARACTER VARYING{width}", ()
        return f"VARCHAR{width}", ()

    def format_data_type_text(self, data_type: TextType) -> Tuple[str, tuple]:
        """``TEXT`` only.

        ``CLOB`` is in the concept's spelling list but is not PostgreSQL's word
        for unbounded text — PostgreSQL has no CLOB type at all — so asking for
        that spelling here is an error rather than a quiet rewrite into
        something else. The default spelling must always render, or the concept
        would be unusable on this backend.
        """
        self._check_spelling(data_type, ("text",))
        return "TEXT", ()

    def format_data_type_boolean(self, data_type: BooleanType) -> Tuple[str, tuple]:
        """``BOOL`` is a PostgreSQL alias, so it is accepted and rendered."""
        self._check_spelling(data_type, BooleanType)
        return data_type.spelling.upper(), ()

    def format_data_type_blob(self, data_type: BlobType) -> Tuple[str, tuple]:
        """Both spellings are accepted and both render ``BYTEA``.

        ``BYTEA`` is PostgreSQL's name for this concept and ``BLOB`` is not a
        PostgreSQL type — but refusing ``BLOB`` would refuse the concept's own
        default spelling, which is the spelling every plain ``BlobType(d)``
        carries. A backend that writes the concept under another name normalises
        the spelling; it does not refuse the request.
        """
        self._check_spelling(data_type, BlobType)
        return "BYTEA", ()

    def format_data_type_date(self, data_type: DateType) -> Tuple[str, tuple]:
        return "DATE", ()

    def format_data_type_time(self, data_type: TimeType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            if not (0 <= data_type.precision <= 6):
                raise ValueError(
                    f"TIME precision must be between 0 and 6 "
                    f"(PostgreSQL limit), got {data_type.precision}"
                )
            return f"TIME({data_type.precision})", ()
        return "TIME", ()

    def format_data_type_timetz(self, data_type: TimeTzType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            if not (0 <= data_type.precision <= 6):
                raise ValueError(
                    f"TIME precision must be between 0 and 6 "
                    f"(PostgreSQL limit), got {data_type.precision}"
                )
            return f"TIME({data_type.precision}) WITH TIME ZONE", ()
        return "TIME WITH TIME ZONE", ()

    def format_data_type_datetime(self, data_type: DateTimeType) -> Tuple[str, tuple]:
        """``TIMESTAMP`` without a fraction-seconds precision, which is what
        PostgreSQL reports for every such column -- so the bare form is what a
        default declaration renders.

        A declared precision is honoured rather than dropped: PostgreSQL's
        datetime chapter documents ``timestamp(p)`` with 0 to 6 fractional
        digits, and silently discarding the caller's ``precision=3`` would create
        a column that keeps more digits than they asked for while reporting
        success.
        https://www.postgresql.org/docs/current/datatype-datetime.html
        """
        if data_type.precision is not None:
            if not (0 <= data_type.precision <= 6):
                raise ValueError(
                    f"TIMESTAMP precision must be between 0 and 6 "
                    f"(PostgreSQL limit), got {data_type.precision}"
                )
            return f"TIMESTAMP({data_type.precision})", ()
        return "TIMESTAMP", ()

    def format_data_type_timestamp(self, data_type: TimestampType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            if not (0 <= data_type.precision <= 6):
                raise ValueError(
                    f"TIMESTAMP precision must be between 0 and 6 "
                    f"(PostgreSQL limit), got {data_type.precision}"
                )
            return f"TIMESTAMP({data_type.precision})", ()
        return "TIMESTAMP", ()

    def format_data_type_timestamptz(self, data_type: TimestampTzType) -> Tuple[str, tuple]:
        if data_type.precision is not None:
            if not (0 <= data_type.precision <= 6):
                raise ValueError(
                    f"TIMESTAMP precision must be between 0 and 6 "
                    f"(PostgreSQL limit), got {data_type.precision}"
                )
            return f"TIMESTAMP({data_type.precision}) WITH TIME ZONE", ()
        return "TIMESTAMP WITH TIME ZONE", ()

    def format_data_type_interval(self, data_type: IntervalType) -> Tuple[str, tuple]:
        if data_type.fields is not None:
            return f"INTERVAL {data_type.fields}", ()
        return "INTERVAL", ()

    def format_data_type_json(self, data_type: JsonType) -> Tuple[str, tuple]:
        return "JSON", ()

    def format_data_type_jsonb(self, data_type: JsonBType) -> Tuple[str, tuple]:
        return "JSONB", ()

    # ------------------------------------------------------------------
    # supports_data_type_<name> — per-type capability queries
    # ------------------------------------------------------------------

    def supports_data_type_postgres_enum(self) -> bool:
        return True

    def supports_data_type_postgres_bytea(self) -> bool:
        return True

    def supports_data_type_postgres_smallserial(self) -> bool:
        return True

    def supports_data_type_postgres_serial(self) -> bool:
        return True

    def supports_data_type_postgres_bigserial(self) -> bool:
        return True

    def supports_data_type_postgres_uuid(self) -> bool:
        return True

    def supports_data_type_postgres_xml(self) -> bool:
        return True

    def supports_data_type_postgres_tsvector(self) -> bool:
        return True

    def supports_data_type_postgres_tsquery(self) -> bool:
        return True

    def supports_data_type_postgres_jsonpath(self) -> bool:
        """jsonpath arrived in PostgreSQL 12.

        Measured against the scenario servers: 'CREATE TABLE t(p jsonpath)' is
        rejected on 11 with 'type "jsonpath" does not exist' and accepted from
        12 on. Unconditional True meant a 9, 10 or 11 server was told it could
        have the type, and found out by executing the DDL.
        """
        return self.version >= (12, 0, 0)

    def supports_data_type_postgres_bit(self) -> bool:
        return True

    def supports_data_type_postgres_varbit(self) -> bool:
        return True

    def supports_data_type_postgres_inet(self) -> bool:
        return True

    def supports_data_type_postgres_cidr(self) -> bool:
        return True

    def supports_data_type_postgres_macaddr(self) -> bool:
        return True

    def supports_data_type_postgres_macaddr8(self) -> bool:
        """macaddr8 arrived in PostgreSQL 10.

        The fourth empty gate in this file and the last one: creating
        (v MACADDR8) on 9.6 comes back 'type "macaddr8" does not exist'.
        jsonpath is 12, xid8 is 13, the multiranges are 14, this is 10.
        """
        return self.version >= (10, 0, 0)

    def supports_data_type_postgres_point(self) -> bool:
        return True

    def supports_data_type_postgres_line(self) -> bool:
        return True

    def supports_data_type_postgres_line_segment(self) -> bool:
        return True

    def supports_data_type_postgres_box(self) -> bool:
        return True

    def supports_data_type_postgres_path(self) -> bool:
        return True

    def supports_data_type_postgres_polygon(self) -> bool:
        return True

    def supports_data_type_postgres_circle(self) -> bool:
        return True

    def supports_data_type_postgres_money(self) -> bool:
        return True

    def supports_data_type_postgres_int4range(self) -> bool:
        return True

    def supports_data_type_postgres_int8range(self) -> bool:
        return True

    def supports_data_type_postgres_numrange(self) -> bool:
        return True

    def supports_data_type_postgres_tsrange(self) -> bool:
        return True

    def supports_data_type_postgres_tstzrange(self) -> bool:
        return True

    def supports_data_type_postgres_daterange(self) -> bool:
        return True

    def supports_data_type_postgres_int4multirange(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_data_type_postgres_int8multirange(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_data_type_postgres_nummultirange(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_data_type_postgres_tsmultirange(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_data_type_postgres_tstzmultirange(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_data_type_postgres_datemultirange(self) -> bool:
        return self.version >= (14, 0, 0)

    def supports_data_type_postgres_oid(self) -> bool:
        return True

    def supports_data_type_postgres_regclass(self) -> bool:
        return True

    def supports_data_type_postgres_regtype(self) -> bool:
        return True

    def supports_data_type_postgres_xid(self) -> bool:
        return True

    def supports_data_type_postgres_xid8(self) -> bool:
        """xid8 arrived in PostgreSQL 13.

        Measured: rejected on 11 and 12, accepted on 13 and 14. Unconditional
        True sent CREATE TABLE ... (v XID8) to a 9 or 10 server, where it came
        back as 'type "xid8" does not exist' rather than at the point of asking
        the dialect what it supports.
        """
        return self.version >= (13, 0, 0)

    def supports_data_type_postgres_cid(self) -> bool:
        return True

    def supports_data_type_postgres_tid(self) -> bool:
        return True

    def supports_data_type_postgres_pg_lsn(self) -> bool:
        return True

    def supports_data_type_postgres_hstore(self) -> bool:
        return True

    def supports_data_type_postgres_geometry(self) -> bool:
        return True

    def supports_data_type_postgres_geography(self) -> bool:
        return True

    def supports_data_type_postgres_vector(self) -> bool:
        return True

    def supports_data_type_postgres_halfvec(self) -> bool:
        return True

    def supports_data_type_postgres_sparsevec(self) -> bool:
        return True

    def supports_data_type_postgres_citext(self) -> bool:
        return True

    def supports_data_type_postgres_cube(self) -> bool:
        return True

    def supports_data_type_postgres_ltree(self) -> bool:
        return True

    def supports_data_type_postgres_lquery(self) -> bool:
        return True

    def supports_data_type_postgres_ltxtquery(self) -> bool:
        return True

    def supports_data_type_postgres_raster(self) -> bool:
        return True

    def supports_data_type_postgres_array(self) -> bool:
        return True

    # --- Core type support ---

    def supports_data_type_tinyint(self) -> bool:
        return True

    def supports_data_type_smallint(self) -> bool:
        return True

    def supports_data_type_integer(self) -> bool:
        return True

    def supports_data_type_bigint(self) -> bool:
        return True

    def supports_data_type_float(self) -> bool:
        return True

    def supports_data_type_real(self) -> bool:
        return True

    def supports_data_type_double(self) -> bool:
        return True

    def supports_data_type_decimal(self) -> bool:
        return True

    def supports_data_type_char(self) -> bool:
        return True

    def supports_data_type_varchar(self) -> bool:
        return True

    def supports_data_type_text(self) -> bool:
        return True

    def supports_data_type_boolean(self) -> bool:
        return True

    def supports_data_type_blob(self) -> bool:
        return True

    def supports_data_type_date(self) -> bool:
        return True

    def supports_data_type_time(self) -> bool:
        return True

    def supports_data_type_timetz(self) -> bool:
        return True

    def supports_data_type_datetime(self) -> bool:
        return True

    def supports_data_type_timestamp(self) -> bool:
        return True

    def supports_data_type_timestamptz(self) -> bool:
        return True

    def supports_data_type_interval(self) -> bool:
        return True

    def supports_data_type_json(self) -> bool:
        return True

    def supports_data_type_jsonb(self) -> bool:
        return True

    def supports_data_type_uuid(self) -> bool:
        return True

    def supports_data_type_custom(self) -> bool:
        return True

    def supports_data_type_array(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Supported types advertisement
    # ------------------------------------------------------------------

    def supports_data_types(self) -> dict:
        """Mapping ``{<name>: concrete type class}`` of every type this
        dialect renders.

        The formatters a dialect defines **are** the set of types it
        supports (naming-convention dispatch has no registry), so this
        derives the mapping from the ``format_data_type_<name>`` methods
        and checks ``supports_data_type_<name>()`` for each.
        """
        result = {}
        for member_name in dir(type(self)):
            match = re.match(r"^format_data_type_([a-z][a-z0-9_]*)$", member_name)
            if not match:
                continue
            name = match.group(1)
            supports = getattr(self, f"supports_data_type_{name}", None)
            if supports is not None and supports():
                cls = self._type_class_for(name)
                if cls is not None:
                    result[name] = cls
        return result

    def format_data_type_postgres_enum(self, data_type) -> Tuple[str, tuple]:
        """Render the reference to a created enum type.

        A column of an enum type states the type's name, not its labels — the
        labels belong to the CREATE TYPE that made it.
        """
        return self.format_enum_type_name(data_type.type_name, data_type.schema)

    def supports_data_type_postgres_enum(self) -> bool:
        return True

    def suggested_data_types(self) -> dict:
        """Cross-backend type-consistency suggestion map.

        PostgreSQL renders every core type it supports through its own
        ``format_data_type_<name>`` formatters — including ``blob`` (as
        ``BYTEA``) and ``xml`` (natively). What it cannot spell, it names here:

        ``binary`` / ``varbinary``
            PostgreSQL has **no** fixed-length or variable-length byte-string
            type. ``BYTEA`` is the only byte storage it has, and it is
            unbounded, so a ``BINARY(16)`` on this backend is a ``BYTEA`` with
            the length enforced by a CHECK constraint — a fact about the
            column, not a type. The substitute is therefore the byte-string
            concept this backend does have.

        ``enum``
            PostgreSQL does have enums, but a *named* type created once by
            ``CREATE TYPE ... AS ENUM`` and referred to by name, which is what
            lets two columns share one set of labels. The generic ``EnumType``
            carries labels and no name, so there is nothing to render. The
            suggestion is the DataType-shaped door into that machinery — it
            needs a ``type_name``, and constructing one without it is an error
            rather than a guess.
        """
        from ...expression.types import PostgresEnumColumnType

        return {
            "binary": BlobType,
            "varbinary": BlobType,
            "enum": PostgresEnumColumnType,
        }

    def format_data_type_uuid(self, data_type) -> Tuple[str, tuple]:
        return "UUID", ()

    def supports_data_type_xml(self) -> bool:
        """PostgreSQL has a native ``xml`` type, so it renders the XML concept
        rather than substituting text for it."""
        return True

    def format_data_type_xml(self, data_type: XmlType) -> Tuple[str, tuple]:
        """``xml`` in PostgreSQL.

        The type may be validated against an XML Schema registered for it, but
        the schema is a column/domain attribute rather than part of the type's
        identity, so it does not appear here. ``PostgresXMLType`` renders the
        same SQL under its own namespaced name — both are the XML concept, one
        reached through the core type and one through the backend's.
        """
        return "XML", ()

    def format_data_type_custom(self, data_type) -> Tuple[str, tuple]:
        return data_type.raw, ()

    def format_data_type_array(self, data_type: ArrayType) -> Tuple[str, tuple]:
        element_sql, _ = self.format_data_type(data_type.element_type)
        return element_sql + "[]" * data_type.dimensions, ()

    # ------------------------------------------------------------------
    # DDLTypeSupport — parsing
    # ------------------------------------------------------------------

    _PG_INTEGER_TYPES = re.compile(
        r"^(?:SMALLINT|INT2|INT|INTEGER|INT4|BIGINT|INT8)\b",
        re.IGNORECASE,
    )
    _PG_SERIAL_TYPES = re.compile(
        r"^(?:SMALLSERIAL|SERIAL2|SERIAL|SERIAL4|BIGSERIAL|SERIAL8)\b",
        re.IGNORECASE,
    )
    _PG_FLOAT_TYPES = re.compile(
        r"^(?:REAL|FLOAT4|FLOAT|DOUBLE)\b",
        re.IGNORECASE,
    )
    _PG_DECIMAL_TYPES = re.compile(
        r"^(?:DECIMAL|NUMERIC)\b",
        re.IGNORECASE,
    )
    _PG_STRING_TYPES = re.compile(
        r"^(?:CHARACTER|CHAR|VARCHAR|TEXT|CITEXT)\b",
        re.IGNORECASE,
    )
    _PG_BINARY_TYPES = re.compile(
        r"^(?:BYTEA|BLOB)\b",
        re.IGNORECASE,
    )
    _PG_DATE_TYPES = re.compile(
        r"^(?:DATETIME|DATE|TIMESTAMPTZ|TIMESTAMP|TIMETZ|TIME|INTERVAL)\b",
        re.IGNORECASE,
    )
    _PG_JSON_TYPES = re.compile(
        r"^(?:JSON|JSONB|JSONPATH)\b",
        re.IGNORECASE,
    )
    _PG_UUID_TYPES = re.compile(
        r"^(?:UUID)\b",
        re.IGNORECASE,
    )
    _PG_NET_TYPES = re.compile(
        r"^(?:INET|CIDR|MACADDR|MACADDR8)\b",
        re.IGNORECASE,
    )
    _PG_GEOM_TYPES = re.compile(
        r"^(?:POINT|LINE|LSEG|BOX|PATH|POLYGON|CIRCLE)\b",
        re.IGNORECASE,
    )
    _PG_BIT_TYPES = re.compile(
        r"^(?:BIT|VARBIT)\b",
        re.IGNORECASE,
    )
    _PG_RANGE_TYPES = re.compile(
        r"^(?:INT4RANGE|INT8RANGE|NUMRANGE|TSRANGE|TSTZRANGE|DATERANGE)\b",
        re.IGNORECASE,
    )
    _PG_MULTIRANGE_TYPES = re.compile(
        r"^(?:INT4MULTIRANGE|INT8MULTIRANGE|NUMMULTIRANGE|"
        r"TSMULTIRANGE|TSTZMULTIRANGE|DATEMULTIRANGE)\b",
        re.IGNORECASE,
    )
    _PG_OID_TYPES = re.compile(
        r"^(?:OID|REGCLASS|REGTYPE|XID|XID8|CID|TID)\b",
        re.IGNORECASE,
    )
    _PG_TS_TYPES = re.compile(
        r"^(?:TSVECTOR|TSQUERY)\b",
        re.IGNORECASE,
    )
    _PG_MISC_TYPES = re.compile(
        r"^(?:MONEY|XML|PG_LSN|HSTORE|GEOMETRY|GEOGRAPHY|CUBE|LTREE|LQUERY|"
        r"LTXTQUERY|RASTER)\b",
        re.IGNORECASE,
    )
    _PG_ARRAY_SUFFIX = re.compile(r"(\[(\d*)\])+\s*$")
    _PG_ARRAY_KEYWORD = re.compile(r"\s+ARRAY(?:\s*\[\s*(\d+)\s*\])?\s*$", re.IGNORECASE)

    def parse_type(self, raw: str) -> DataType:
        stripped = raw.strip()
        upper = stripped.upper()

        # ---- Array detection (must run before any type-specific match) ----

        # Check for ARRAY keyword: e.g. "INTEGER ARRAY" or "INTEGER ARRAY[3]"
        kw_match = self._PG_ARRAY_KEYWORD.search(upper)
        if kw_match:
            base_raw = stripped[:kw_match.start()]
            element_type = self.parse_type(base_raw)
            # PG normalises all array declarations to 1-D internally
            return PostgresArrayType(element_type=element_type, dimensions=1, dialect=self)

        # Check for array bracket suffix: e.g. "INTEGER[]", "INTEGER[][]"
        remaining = stripped
        while remaining.endswith("[]"):
            remaining = remaining[:-2]
        while remaining.endswith("]"):
            m = self._PG_ARRAY_SUFFIX.search(remaining)
            if m:
                remaining = remaining[:m.start()]
            else:
                break

        if remaining != stripped:
            element_type = self.parse_type(remaining)
            # PG normalises all array declarations to 1-D internally
            return PostgresArrayType(element_type=element_type, dimensions=1, dialect=self)

        # ---- Standard type dispatch ----

        # Serial family
        if self._PG_SERIAL_TYPES.match(upper):
            from ...expression.types import (
                PostgresBigSerialType,
                PostgresSerialType,
                PostgresSmallSerialType,
            )
            if upper.startswith("BIGSERIAL") or upper.startswith("SERIAL8"):
                return PostgresBigSerialType(self)
            if upper.startswith("SMALLSERIAL") or upper.startswith("SERIAL2"):
                return PostgresSmallSerialType(self)
            return PostgresSerialType(self)

        # Integer family
        if self._PG_INTEGER_TYPES.match(upper):
            if upper.startswith("SMALLINT") or upper.startswith("INT2"):
                return SmallIntType(self)
            if upper.startswith("BIGINT") or upper.startswith("INT8"):
                return BigIntType(self)
            return IntegerType(self)

        # Float family
        if self._PG_FLOAT_TYPES.match(upper):
            if upper.startswith("DOUBLE"):
                return DoubleType(dialect=self)
            if upper.startswith("REAL") or upper.startswith("FLOAT4"):
                return RealType(dialect=self)
            nums = re.findall(r"\d+", stripped)
            precision = int(nums[0]) if nums else None
            return FloatType(dialect=self, precision=precision)

        # Decimal family
        if self._PG_DECIMAL_TYPES.match(upper):
            nums = re.findall(r"\d+", stripped)
            if len(nums) >= 2:
                return DecimalType(dialect=self, precision=int(nums[0]), scale=int(nums[1]))
            if len(nums) == 1:
                return DecimalType(dialect=self, precision=int(nums[0]))
            return DecimalType(dialect=self)

        # String family
        if self._PG_STRING_TYPES.match(upper):
            length_match = re.search(r"\((\d+)\)", stripped)
            length = int(length_match.group(1)) if length_match else None
            if upper.startswith("CHARACTER VARYING"):
                return VarCharType(dialect=self, length=length,
                                   spelling="character varying")
            if upper.startswith("VARCHAR"):
                return VarCharType(dialect=self, length=length)
            if upper.startswith("CHARACTER"):
                return CharType(dialect=self, length=length,
                                spelling="character")
            if upper.startswith("CHAR"):
                return CharType(dialect=self, length=length)
            # citext: case-insensitive text, a contrib extension with its own
            # type. A TextType subclass rather than a spelling, because the
            # comparison semantics differ -- a citext column compares equal
            # case-insensitively, which text does not, so the differ must see
            # the difference.
            if upper.startswith("CITEXT"):
                from ...expression.types import PostgresCitextType
                return PostgresCitextType(self)
            return TextType(dialect=self)

        # Binary
        if self._PG_BINARY_TYPES.match(upper):
            from ...expression.types import PostgresByteaType
            return PostgresByteaType(self)

        # Date/time
        if self._PG_DATE_TYPES.match(upper):
            if upper.startswith("DATETIME"):
                return DateTimeType(dialect=self)
            if upper.startswith("DATE"):
                return DateType(dialect=self)
            if upper.startswith("TIMESTAMP"):
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMESTAMPTZ"):
                    return TimestampTzType(dialect=self, precision=precision)
                return TimestampType(dialect=self, precision=precision)
            if upper.startswith("TIME"):
                nums = re.findall(r"\d+", stripped)
                precision = int(nums[0]) if nums else None
                if "WITH TIME ZONE" in upper or upper.startswith("TIMETZ"):
                    return TimeTzType(dialect=self, precision=precision)
                return TimeType(dialect=self, precision=precision)
            if upper.startswith("INTERVAL"):
                fields_match = re.search(r"INTERVAL\s+(.*)", upper)
                fields = fields_match.group(1).strip() if fields_match else None
                return IntervalType(dialect=self, fields=fields)

        # JSON
        if self._PG_JSON_TYPES.match(upper):
            if upper.startswith("JSONB"):
                return JsonBType(dialect=self)
            if upper.startswith("JSONPATH"):
                from ...expression.types import PostgresJsonPathType
                return PostgresJsonPathType(dialect=self)
            return JsonType(dialect=self)

        # UUID
        if self._PG_UUID_TYPES.match(upper):
            from ...expression.types import PostgresUUIDType
            return PostgresUUIDType(dialect=self)

        # Boolean
        if upper.startswith("BOOLEAN") or upper.startswith("BOOL"):
            return BooleanType(dialect=self)

        # Bit string
        if self._PG_BIT_TYPES.match(upper):
            nums = re.findall(r"\d+", stripped)
            n = int(nums[0]) if nums else None
            # The variable-length family is tested first: live ``format_type``
            # emits ``bit varying(n)`` for a varbit column, and that string
            # starts with ``BIT`` exactly as the fixed-length word does.  A
            # leading ``startswith("BIT")`` test sent the catalog spelling to
            # ``PostgresBitType``; ``VARBIT`` and ``BIT VARYING`` are the two
            # words for the one storage and must answer the same class.
            if upper.startswith("VARBIT") or re.match(r"BIT\s+VARYING\b", upper):
                from ...expression.types import PostgresVarBitType
                return PostgresVarBitType(n=n, dialect=self)
            from ...expression.types import PostgresBitType
            return PostgresBitType(n=n, dialect=self)

        # Network address
        if self._PG_NET_TYPES.match(upper):
            from ...expression.types import (
                PostgresCidrType,
                PostgresInetType,
                PostgresMacAddr8Type,
                PostgresMacAddrType,
            )
            if upper.startswith("CIDR"):
                return PostgresCidrType(self)
            if upper.startswith("MACADDR8"):
                return PostgresMacAddr8Type(self)
            if upper.startswith("MACADDR"):
                return PostgresMacAddrType(self)
            return PostgresInetType(self)

        # Geometric
        if self._PG_GEOM_TYPES.match(upper):
            from ...expression.types import (
                PostgresBoxType,
                PostgresCircleType,
                PostgresLineSegmentType,
                PostgresLineType,
                PostgresPathType,
                PostgresPointType,
                PostgresPolygonType,
            )
            geom_map = {
                "POINT": PostgresPointType,
                "LINE": PostgresLineType,
                "LSEG": PostgresLineSegmentType,
                "BOX": PostgresBoxType,
                "PATH": PostgresPathType,
                "POLYGON": PostgresPolygonType,
                "CIRCLE": PostgresCircleType,
            }
            for name, cls in geom_map.items():
                if upper.startswith(name):
                    return cls()
            return PostgresPointType(self)

        # Range types
        if self._PG_RANGE_TYPES.match(upper):
            from ...expression.types import (
                PostgresDateRangeType,
                PostgresInt4RangeType,
                PostgresInt8RangeType,
                PostgresNumRangeType,
                PostgresTsRangeType,
                PostgresTsTzRangeType,
            )
            range_map = {
                "INT4RANGE": PostgresInt4RangeType,
                "INT8RANGE": PostgresInt8RangeType,
                "NUMRANGE": PostgresNumRangeType,
                "TSRANGE": PostgresTsRangeType,
                "TSTZRANGE": PostgresTsTzRangeType,
                "DATERANGE": PostgresDateRangeType,
            }
            for name, cls in range_map.items():
                if upper.startswith(name):
                    return cls()
            return PostgresInt4RangeType(self)

        # Multirange types
        if self._PG_MULTIRANGE_TYPES.match(upper):
            from ...expression.types import (
                PostgresDateMultirangeType,
                PostgresInt4MultirangeType,
                PostgresInt8MultirangeType,
                PostgresNumMultirangeType,
                PostgresTsMultirangeType,
                PostgresTsTzMultirangeType,
            )
            mr_map = {
                "INT4MULTIRANGE": PostgresInt4MultirangeType,
                "INT8MULTIRANGE": PostgresInt8MultirangeType,
                "NUMMULTIRANGE": PostgresNumMultirangeType,
                "TSMULTIRANGE": PostgresTsMultirangeType,
                "TSTZMULTIRANGE": PostgresTsTzMultirangeType,
                "DATEMULTIRANGE": PostgresDateMultirangeType,
            }
            for name, cls in mr_map.items():
                if upper.startswith(name):
                    return cls()
            return PostgresInt4MultirangeType(self)

        # OID types
        if self._PG_OID_TYPES.match(upper):
            from ...expression.types import (
                PostgresCIDType,
                PostgresOIDType,
                PostgresRegClassType,
                PostgresRegTypeType,
                PostgresTIDType,
                PostgresXID8Type,
                PostgresXIDType,
            )
            oid_map = {
                "OID": PostgresOIDType,
                "REGCLASS": PostgresRegClassType,
                "REGTYPE": PostgresRegTypeType,
                "XID8": PostgresXID8Type,
                "XID": PostgresXIDType,
                "CID": PostgresCIDType,
                "TID": PostgresTIDType,
            }
            for name, cls in oid_map.items():
                if upper.startswith(name):
                    return cls()
            return PostgresOIDType(self)

        # Text search
        if self._PG_TS_TYPES.match(upper):
            from ...expression.types import (
                PostgresTSQueryType,
                PostgresTSVectorType,
            )
            if upper.startswith("TSVECTOR"):
                return PostgresTSVectorType(self)
            return PostgresTSQueryType(self)

        # Miscellaneous
        if self._PG_MISC_TYPES.match(upper):
            from ...expression.types import (
                PostgresGeographyType,
                PostgresGeometryType,
                PostgresHstoreType,
                PostgresMoneyType,
                PostgresPgLSNType,
                PostgresXMLType,
            )
            if upper.startswith("MONEY"):
                return PostgresMoneyType(self)
            if upper.startswith("XML"):
                return PostgresXMLType(self)
            if upper.startswith("PG_LSN"):
                return PostgresPgLSNType(self)
            if upper.startswith("HSTORE"):
                return PostgresHstoreType(self)
            if upper.startswith("GEOGRAPHY"):
                return PostgresGeographyType(self)
            if upper.startswith("GEOMETRY"):
                return PostgresGeometryType(self)
            # Extension types with one word each: cube and the ltree family
            # are contrib extensions, RASTER is PostGIS -- the same home as
            # HSTORE and GEOMETRY already above them, and ungated for the
            # same reason they are: the word can only have come from a
            # server where the extension exists, because a column of the
            # type requires it.
            if upper.startswith("CUBE"):
                from ...expression.types import PostgresCubeType
                return PostgresCubeType(self)
            # LTXTQUERY is checked before LQUERY although the words cannot
            # collide (they differ at the second letter), because the pair
            # is where a prefix mistake would go if one were ever added.
            if upper.startswith("LTXTQUERY"):
                from ...expression.types import PostgresLtxtqueryType
                return PostgresLtxtqueryType(self)
            if upper.startswith("LQUERY"):
                from ...expression.types import PostgresLqueryType
                return PostgresLqueryType(self)
            if upper.startswith("LTREE"):
                from ...expression.types import PostgresLtreeType
                return PostgresLtreeType(self)
            if upper.startswith("RASTER"):
                from ...expression.types import PostgresRasterType
                return PostgresRasterType(self)

        # Vector types (pgvector)
        vector_match = re.match(r"^(VECTOR|HALFVEC|SPARSEVEC)\b", upper)
        if vector_match:
            if (
                hasattr(self, "_extensions")
                and "vector" in self._extensions
                and not self._extensions["vector"].installed
            ):
                raise RuntimeError(
                    "Cannot use pgvector types: the 'vector' (pgvector) extension "
                    "is not installed. Run: CREATE EXTENSION IF NOT EXISTS vector;"
                )
            nums = re.findall(r"\d+", stripped)
            dim = int(nums[0]) if nums else 0
            vector_kind = vector_match.group(1)
            if vector_kind == "HALFVEC":
                from ...expression.types import PostgresHalfvecType
                return PostgresHalfvecType(dim=dim, dialect=self)
            if vector_kind == "SPARSEVEC":
                from ...expression.types import PostgresSparsevecType
                return PostgresSparsevecType(dim=dim, dialect=self)
            from ...expression.types import PostgresVectorType
            return PostgresVectorType(dim=dim, dialect=self)

        # Fallback
        from rhosocial.activerecord.backend.expression.types import CustomType
        return CustomType(dialect=self, raw=stripped)
