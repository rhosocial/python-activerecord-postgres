# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_cast_integration.py

"""Type conversion against a live PostgreSQL, one case per type.

These execute. A cast renders into a grammar position that cannot take a bound
parameter, so nothing about it can be settled by looking at the SQL string --
``col::IntegerType()`` is obviously wrong to a reader, but the only way to know
that ``col::INTEGER`` is *accepted*, that ``'42'::int4range`` parses, or that a
chained ``::text::int`` round-trips is to run it and read the value back. So
each case builds a real table, inserts through a cast, and asserts on what came
back out.

Three layers, because they fail differently:

1. **Declare** -- every modelled type must be accepted by ``CREATE TABLE``.
   This is the layer that covers the types you cannot insert into (serial
   columns, and types whose value has no text form), and it is the one that
   catches a type whose rendered spelling the server does not recognise.
2. **Insert through a cast** -- for every type with a text form, the value goes
   in as ``Literal(text).cast(Type)``. This is the cast doing real work.
3. **Read back through a cast** -- the stored value is cast to text and
   compared, so the assertion is on the value PostgreSQL actually holds rather
   than on a driver-specific Python object.

Where a type genuinely has nothing to cast, the case declares the column
instead of inserting into it, and says so with ``declared_only``.
"""

import pytest

from rhosocial.activerecord.backend.expression import (
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
from rhosocial.activerecord.backend.impl.postgres.expression.types import (
    PostgresBitType,
    PostgresBoxType,
    PostgresByteaType,
    PostgresCIDType,
    PostgresCharacterVaryingType,
    PostgresCidrType,
    PostgresCircleType,
    PostgresCitextType,
    PostgresCubeType,
    PostgresDateMultirangeType,
    PostgresDateRangeType,
    PostgresGeographyType,
    PostgresGeometryType,
    PostgresHalfvecType,
    PostgresHstoreType,
    PostgresInetType,
    PostgresInt4MultirangeType,
    PostgresInt4RangeType,
    PostgresInt8MultirangeType,
    PostgresInt8RangeType,
    PostgresJsonPathType,
    PostgresLineSegmentType,
    PostgresLineType,
    PostgresLqueryType,
    PostgresLtreeType,
    PostgresLtxtqueryType,
    PostgresMacAddr8Type,
    PostgresMacAddrType,
    PostgresMoneyType,
    PostgresNumMultirangeType,
    PostgresNumRangeType,
    PostgresOIDType,
    PostgresPathType,
    PostgresPgLSNType,
    PostgresPointType,
    PostgresPolygonType,
    PostgresRegClassType,
    PostgresRegTypeType,
    PostgresSerialType,
    PostgresSmallSerialType,
    PostgresTIDType,
    PostgresTSQueryType,
    PostgresTSVectorType,
    PostgresTsMultirangeType,
    PostgresTsRangeType,
    PostgresTsTzMultirangeType,
    PostgresTsTzRangeType,
    PostgresVarBitType,
    PostgresVectorType,
    PostgresXID8Type,
    PostgresXIDType,
    PostgresXMLType,
)
from rhosocial.activerecord.backend.errors import DatabaseError
from rhosocial.activerecord.backend.options import ExecutionOptions
from rhosocial.activerecord.backend.schema import StatementType

DQL = ExecutionOptions(stmt_type=StatementType.DQL)


class Case:
    """One type, and what PostgreSQL does with it.

    Args:
        label: Used in the test id, so a failure names the type.
        factory: Builds the type for a dialect; the vector types need an
            argument the dialect cannot supply on their own.
        literal: The text form inserted and cast to the type.
        rendered: What ``value::text`` gives back, compared as a string.
        extension: An extension this type needs, if any.
        declared_only: Set when the column cannot be inserted into, so the
            case asserts the declaration is accepted and stops there.
    """

    def __init__(self, label, factory=None, literal=None, rendered=None,
                 extension=None, declared_only=False, tolerant=False,
                 make=None):
        self.label = label
        self.factory = factory or make
        self.literal = literal
        self.rendered = rendered
        self.extension = extension
        self.declared_only = declared_only
        #: Set when PostgreSQL's text rendering depends on server settings --
        #: the session timezone, the lc_monetary style -- so the text is not a
        #: fixed expectation. The value still has to survive the round trip.
        self.tolerant = tolerant


def _pg_types_module():
    import rhosocial.activerecord.backend.impl.postgres.expression.types as types

    return types


def _core(name):
    """Resolve a core type by class name, so the case table stays short."""
    import rhosocial.activerecord.backend.expression.types as core

    return getattr(core, name)


#: Every type worth a round trip. Ordered core-first, then PostgreSQL's own.
CASES = [
    # ---- core types PostgreSQL shares ----
    Case("smallint", lambda d: _core("SmallIntType")(d), "42", "42"),
    Case("integer", lambda d: _core("IntegerType")(d), "42", "42"),
    Case("int4", lambda d: _core("IntType")(d), "42", "42"),
    Case("bigint", lambda d: _core("BigIntType")(d), "42", "42"),
    Case("tinyint", lambda d: _core("TinyIntType")(d), "42", "42"),
    Case("real", lambda d: _core("RealType")(d), "1.5", "1.5"),
    Case("double", lambda d: _core("DoubleType")(d), "1.5", "1.5"),
    Case("float", lambda d: _core("FloatType")(d), "1.5", "1.5"),
    Case("numeric", lambda d: _core("DecimalType")(d), "10.25", "10.25"),
    Case("decimal", lambda d: _core("DecimalType")(d, precision=10, scale=2),
         "10.25", "10.25"),
    Case("boolean", lambda d: _core("BooleanType")(d), "true", "true"),
    Case("text", lambda d: _core("TextType")(d), "hello", "hello"),
    Case("varchar", lambda d: _core("VarCharType")(d, length=32), "hello", "hello"),
    Case("char", lambda d: _core("CharType")(d, length=8), "abc", "abc"),
    Case("date", lambda d: _core("DateType")(d), "2026-01-15", "2026-01-15"),
    Case("time", lambda d: _core("TimeType")(d), "13:45:30", "13:45:30"),
    Case("timestamp", lambda d: _core("TimestampType")(d), "2026-01-15 13:45:30",
         "2026-01-15 13:45:30"),
    # Rendered in the server's own timezone, so the exact text is not ours to
    # assert; ``tolerant`` checks the value survives the round trip instead.
    Case("timestamptz", lambda d: _core("TimestampTzType")(d),
         "2026-01-15 13:45:30+00", tolerant=True),
    Case("interval", lambda d: _core("IntervalType")(d), "1 day",
         "1 day"),
    Case("json", lambda d: _core("JsonType")(d), '{"a": 1}', '{"a": 1}'),
    Case("jsonb", lambda d: _core("JsonBType")(d), '{"a": 1}', '{"a": 1}'),
    Case("uuid", lambda d: _core("UUIDType")(d),
         "00000000-0000-0000-0000-000000000001",
         "00000000-0000-0000-0000-000000000001"),
    Case("blob", lambda d: _core("BlobType")(d), "abc", r"\x616263"),

    # ---- PostgreSQL built-ins ----
    Case("character_varying",
         lambda d: PostgresCharacterVaryingType(d, length=32), "hello", "hello"),
    Case("bytea", lambda d: PostgresByteaType(d), "abc", r"\x616263"),
    Case("bit", lambda d: PostgresBitType(n=3, dialect=d), "101", "101"),
    Case("varbit", lambda d: PostgresVarBitType(n=3, dialect=d), "101", "101"),
    Case("cidr", lambda d: PostgresCidrType(d), "192.168.0.0/24", "192.168.0.0/24"),
    Case("inet", lambda d: PostgresInetType(d), "192.168.1.1", "192.168.1.1/32"),
    Case("macaddr", lambda d: PostgresMacAddrType(d), "08:00:2b:01:02:03",
         "08:00:2b:01:02:03"),
    Case("macaddr8", lambda d: PostgresMacAddr8Type(d),
         "08:00:2b:01:02:03:04:05", "08:00:2b:01:02:03:04:05"),
    Case("money", lambda d: PostgresMoneyType(d), "12.34", "$12.34"),
    Case("oid", lambda d: PostgresOIDType(d), "1234", "1234"),
    Case("xid", lambda d: PostgresXIDType(d), "1", "1"),
    Case("xid8", lambda d: PostgresXID8Type(d), "1", "1"),
    Case("tid", lambda d: PostgresTIDType(d), "(0,1)", "(0,1)"),
    # CID is a network line number, not a dotted quad: it goes in as an
    # integer and casts back to the same integer.
    Case("cid", lambda d: PostgresCIDType(d), "16909060", "16909060"),
    # tolerant, because PostgreSQL 19 zero-pads the high half of a pg_lsn and
    # 16 through 18 do not: the same value reads back as '0/16B3748' on 16.15,
    # 17.11 and 18.6, and as '0/016B3748' on 19beta4. Both spellings are
    # accepted on input on every version, so the value round trips either way
    # and the text is the server's to choose.
    Case("pg_lsn", lambda d: PostgresPgLSNType(d), "0/16B3748",
         "0/16B3748", tolerant=True),
    Case("jsonpath", lambda d: PostgresJsonPathType(d), '"$.a"', '"$.a"'),
    Case("regtype", lambda d: PostgresRegTypeType(d), "integer", "integer"),
    Case("regclass", lambda d: PostgresRegClassType(d), "pg_class", "pg_class"),
    Case("xml", lambda d: PostgresXMLType(d), "<a/>", "<a/>"),
    Case("tsvector", lambda d: PostgresTSVectorType(d), "cat", "'cat'"),
    Case("tsquery", lambda d: PostgresTSQueryType(d), "cat", "'cat'"),

    # ---- geometry: built into the server, no extension ----
    Case("point", lambda d: PostgresPointType(d), "(1,2)", "(1,2)"),
    Case("line", lambda d: PostgresLineType(d), "{1,2,3}", "{1,2,3}"),
    Case("lseg", lambda d: PostgresLineSegmentType(d), "[(1,2),(3,4)]",
         "[(1,2),(3,4)]"),
    Case("box", lambda d: PostgresBoxType(d), "(1,2),(3,4)", "(3,4),(1,2)"),
    Case("path", lambda d: PostgresPathType(d), "((1,2),(3,4))", "((1,2),(3,4))"),
    Case("polygon", lambda d: PostgresPolygonType(d), "((1,2),(3,4),(5,6))",
         "((1,2),(3,4),(5,6))"),
    Case("circle", lambda d: PostgresCircleType(d), "<(1,2),3>", "<(1,2),3>"),

    # ---- ranges and multiranges ----
    Case("int4range", lambda d: PostgresInt4RangeType(d), "[1,5)", "[1,5)"),
    Case("int8range", lambda d: PostgresInt8RangeType(d), "[1,5)", "[1,5)"),
    Case("numrange", lambda d: PostgresNumRangeType(d), "[1.0,5.0)", "[1.0,5.0)"),
    Case("tsrange", lambda d: PostgresTsRangeType(d),
         '["2026-01-01 00:00:00","2026-02-01 00:00:00")',
         '["2026-01-01 00:00:00","2026-02-01 00:00:00")'),
    Case("tstzrange", lambda d: PostgresTsTzRangeType(d),
         '["2026-01-01 00:00:00+00","2026-02-01 00:00:00+00")',
         tolerant=True),
    Case("daterange", lambda d: PostgresDateRangeType(d),
         "[2026-01-01,2026-02-01)", "[2026-01-01,2026-02-01)"),
    Case("int4multirange", lambda d: PostgresInt4MultirangeType(d),
         "{[1,5)}", "{[1,5)}"),
    Case("int8multirange", lambda d: PostgresInt8MultirangeType(d),
         "{[1,5)}", "{[1,5)}"),
    Case("nummultirange", lambda d: PostgresNumMultirangeType(d),
         "{[1.0,5.0)}", "{[1.0,5.0)}"),
    Case("tsmultirange", lambda d: PostgresTsMultirangeType(d),
         '{["2026-01-01 00:00:00","2026-02-01 00:00:00")}',
         "{[\"2026-01-01 00:00:00\",\"2026-02-01 00:00:00\")}"),
    Case("tstzmultirange", lambda d: PostgresTsTzMultirangeType(d),
         '{["2026-01-01 00:00:00+00","2026-02-01 00:00:00+00")}',
         tolerant=True),
    Case("datemultirange", lambda d: PostgresDateMultirangeType(d),
         "{[2026-01-01,2026-02-01)}", "{[2026-01-01,2026-02-01)}"),

    # ---- extension types ----
    Case("citext", lambda d: PostgresCitextType(d), "Hello", "Hello",
         extension="citext"),
    Case("hstore", lambda d: PostgresHstoreType(d), '"a"=>"1"', '"a"=>"1"',
         extension="hstore"),
    Case("ltree", lambda d: PostgresLtreeType(d), "root.level1", "root.level1",
         extension="ltree"),
    Case("lquery", lambda d: PostgresLqueryType(d), "root.*", "root.*",
         extension="ltree"),
    Case("ltxtquery", lambda d: PostgresLtxtqueryType(d), "root", "root",
         extension="ltree"),
    Case("cube", lambda d: PostgresCubeType(d), "(1,2,3)", "(1, 2, 3)",
         extension="cube"),
    Case("vector", lambda d: PostgresVectorType(d, dim=3), "[1,2,3]", "[1,2,3]",
         extension="vector"),
    Case("halfvec", lambda d: PostgresHalfvecType(d, dim=3), "[1,2,3]",
         "[1,2,3]", extension="vector"),
    Case("geometry", lambda d: PostgresGeometryType(d), "POINT(1 2)",
         "0101000000000000000000F03F0000000000000040", extension="postgis"),
    Case("geography", lambda d: PostgresGeographyType(d), "POINT(1 2)",
         "0101000020E6100000000000000000F03F0000000000000040",
         extension="postgis"),

    # ---- declared only: the server fills these in ----
    Case("serial", lambda d: PostgresSerialType(d), declared_only=True),
    Case("smallserial", lambda d: PostgresSmallSerialType(d), declared_only=True),
    Case("bigserial", lambda d: _pg_types_module().PostgresBigSerialType(d),
         declared_only=True),
]

INSERTABLE = [c for c in CASES if not c.declared_only]
DECLARABLE = list(CASES)


def _ids(cases):
    return [c.label for c in cases]


@pytest.fixture
def pg(postgres_backend_single):
    """A live PostgreSQL."""
    backend = postgres_backend_single
    return backend, backend.dialect


def _need_extension(backend, case):
    """Install the extension *case* needs, skipping only that case if absent.

    Installing every extension up front would be simpler and wrong: the helper
    skips when an extension is unavailable, so one missing extension -- pgvector
    is not on every server -- would skip all 165 cases instead of the three
    that need it.
    """
    if not case.extension:
        return
    from rhosocial.activerecord_postgres_test.feature.backend.utils import (
        ensure_extension_installed,
    )

    ensure_extension_installed(backend, case.extension)


def _table(case):
    return f"cast_t_{case.label}"


def _drop(backend, dialect, table):
    sql, params = DropTableExpression(
        dialect=dialect, table=table, if_exists=True
    ).to_sql()
    backend.execute(sql, params)


def _select_as_text(backend, dialect, table):
    """Read the stored value as text, so the assertion is on PostgreSQL's value."""
    query = QueryExpression(
        dialect=dialect,
        select=[Column(dialect, "v").cast(_core("TextType")(dialect))],
        from_=TableExpression(dialect, table),
    )
    sql, params = query.to_sql()
    result = backend.execute(sql, params, options=DQL)
    return result


class TestEveryTypeCanBeDeclared:
    """CREATE TABLE has to accept what the type renders.

    This is the layer that covers the types nothing can be inserted into, and
    it is where a spelling the server does not recognise is caught.
    """

    @pytest.mark.parametrize("case", DECLARABLE, ids=_ids(DECLARABLE))
    def test_declared(self, pg, case):
        backend, dialect = pg
        _need_extension(backend, case)
        table = _table(case)
        _drop(backend, dialect, table)
        try:
            create = CreateTableExpression(
                dialect=dialect,
                table=table,
                columns=[
                    ColumnDefinition(
                        dialect, name="id",
                        data_type=PostgresSerialType(dialect),
                        constraints=[
                            ColumnConstraint(
                                dialect, ColumnConstraintType.PRIMARY_KEY),
                        ],
                    ),
                    ColumnDefinition(
                        dialect, name="v", data_type=case.factory(dialect)),
                ],
            )
            sql, params = create.to_sql()
            backend.execute(sql, params)
        finally:
            _drop(backend, dialect, table)


class TestInsertThroughACast:
    """The value goes in as text, cast to the type on the way."""

    @pytest.mark.parametrize("case", INSERTABLE, ids=_ids(INSERTABLE))
    def test_round_trip(self, pg, case):
        backend, dialect = pg
        _need_extension(backend, case)
        table = _table(case)
        _drop(backend, dialect, table)
        try:
            create = CreateTableExpression(
                dialect=dialect,
                table=table,
                columns=[
                    ColumnDefinition(
                        dialect, name="id",
                        data_type=PostgresSerialType(dialect),
                        constraints=[
                            ColumnConstraint(
                                dialect, ColumnConstraintType.PRIMARY_KEY),
                        ],
                    ),
                    ColumnDefinition(
                        dialect, name="v", data_type=case.factory(dialect)),
                ],
            )
            sql, params = create.to_sql()
            backend.execute(sql, params)

            insert = InsertExpression(
                dialect=dialect,
                into=table,
                columns=["v"],
                source=ValuesSource(
                    dialect,
                    [[Literal(dialect, case.literal).cast(
                        case.factory(dialect))]],
                ),
            )
            sql, params = insert.to_sql()
            backend.execute(sql, params)

            result = _select_as_text(backend, dialect, table)
            assert result.data is not None and len(result.data) == 1
            if case.tolerant:
                # The text form is the server's to choose, but the value still
                # has to parse back into the type it came from.
                assert result.data[0]["v"]
                check = QueryExpression(
                    dialect=dialect,
                    select=[Column(dialect, "v")],
                    from_=TableExpression(dialect, table),
                )
                sql, params = check.to_sql()
                assert backend.execute(sql, params, options=DQL).data is not None
            else:
                assert result.data[0]["v"] == case.rendered
        finally:
            _drop(backend, dialect, table)


class TestChainedCasts:
    """A chained cast has to nest the way the expressions do."""

    @pytest.mark.parametrize(
        "label,chain,expected",
        [
            ("text_to_int_to_text", ["text", "integer", "text"], "42"),
            ("text_to_numeric_to_int", ["text", "decimal", "integer"], "42"),
            ("text_to_int_to_bool", ["text", "integer", "boolean"], "true"),
            ("text_to_date_to_text", ["text", "date", "text"],
             "2026-01-15"),
            ("text_to_jsonb_to_text", ["text", "jsonb", "text"], '{"a": 1}'),
            ("text_to_uuid_to_text", ["text", "uuid", "text"],
             "00000000-0000-0000-0000-000000000001"),
            ("text_to_cidr_to_text", ["text", "cidr", "text"],
             "192.168.0.0/24"),
            ("text_to_int4range_to_text", ["text", "int4range", "text"],
             "[1,5)"),
        ],
    )
    def test_chain(self, pg, label, chain, expected):
        backend, dialect = pg
        table = "cast_t_chain"
        _drop(backend, dialect, table)
        try:
            create = CreateTableExpression(
                dialect=dialect,
                table=table,
                columns=[
                    ColumnDefinition(
                        dialect, name="id",
                        data_type=PostgresSerialType(dialect),
                        constraints=[
                            ColumnConstraint(
                                dialect, ColumnConstraintType.PRIMARY_KEY),
                        ],
                    ),
                    ColumnDefinition(
                        dialect, name="v",
                        data_type=_core("TextType")(dialect)),
                ],
            )
            sql, params = create.to_sql()
            backend.execute(sql, params)

            # The chain starts at text and ends back at text, so the input is
            # whatever the *intermediate* type parses -- chain[1], not
            # chain[0], which is text for every chain and would feed "42" to a
            # date or a uuid.
            expression = Literal(dialect, _CHAIN_INPUT[chain[1]])
            for name in chain:
                expression = expression.cast(_type_by_label(dialect, name))
            sql, params = InsertExpression(
                dialect=dialect, into=table, columns=["v"],
                source=ValuesSource(dialect, [[expression]]),
            ).to_sql()
            backend.execute(sql, params)

            result = _select_as_text(backend, dialect, table)
            assert result.data[0]["v"] == expected
        finally:
            _drop(backend, dialect, table)


_CHAIN_INPUT = {
    "text": "42",
    "integer": "42",
    "decimal": "42",
    "boolean": "42",
    "date": "2026-01-15",
    "jsonb": '{"a": 1}',
    "uuid": "00000000-0000-0000-0000-000000000001",
    "cidr": "192.168.0.0/24",
    "int4range": "[1,5)",
}

_BY_LABEL = {c.label: c.factory for c in CASES}


def _type_by_label(dialect, label):
    return _BY_LABEL[label](dialect)


class TestRejectedConversions:
    """A conversion the server refuses has to raise, not render."""

    @pytest.mark.parametrize(
        "label,literal",
        [
            ("integer", "not a number"),
            ("integer", ""),
            ("boolean", "maybe"),
            ("date", "2026-13-45"),
            ("uuid", "not-a-uuid"),
            ("cidr", "999.999.999.999/24"),
            ("int4range", "int4range('x','y')"),
            ("json", "{not json"),
        ],
    )
    def test_server_rejects_the_value(self, pg, label, literal):
        backend, dialect = pg
        case = next(c for c in CASES if c.label == label)
        _need_extension(backend, case)
        table = _table(case)
        _drop(backend, dialect, table)
        try:
            sql, params = CreateTableExpression(
                dialect=dialect, table=table,
                columns=[
                    ColumnDefinition(dialect, name="v",
                                     data_type=case.factory(dialect)),
                ],
            ).to_sql()
            backend.execute(sql, params)

            sql, params = InsertExpression(
                dialect=dialect, into=table, columns=["v"],
                source=ValuesSource(dialect, [[
                    Literal(dialect, literal).cast(case.factory(dialect))]]),
            ).to_sql()
            with pytest.raises(DatabaseError):
                backend.execute(sql, params)
        finally:
            _drop(backend, dialect, table)