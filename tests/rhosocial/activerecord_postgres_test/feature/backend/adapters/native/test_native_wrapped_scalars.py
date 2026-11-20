# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_wrapped_scalars.py
"""Native tests for PostgreSQL's wrapped scalar types.

``money``, ``oid``, the ``reg*`` family, ``xid``/``xid8``/``cid``, ``tid`` and
``pg_lsn`` all have a wrapper class and an adapter in this backend.  Reads
assert the server's own text (``col::text`` or ``col::numeric`` for money, so
the locale-formatted money output is never the truth), not the adapter's
re-serialisation.
"""

from decimal import Decimal

import pytest

from rhosocial.activerecord.backend.impl.postgres.adapters.monetary import (
    PostgresMoneyAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.object_identifier import (
    PostgresOidAdapter,
    PostgresTidAdapter,
    PostgresXidAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.pg_lsn import (
    PostgresLsnAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.type_values import (
    CID,
    OID,
    TID,
    XID,
    XID8,
    PostgresLsn,
    PostgresMoney,
    RegClass,
    RegConfig,
    RegNamespace,
    RegProc,
    RegType,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    update_cell,
)

TABLE = "native_wrapped_scalars"

MONEY = PostgresMoneyAdapter()
OID_ADAPTER = PostgresOidAdapter()
XID_ADAPTER = PostgresXidAdapter()
TID_ADAPTER = PostgresTidAdapter()
LSN_ADAPTER = PostgresLsnAdapter()


class TestWrappedScalars:
    TABLES = (TABLE,)

    # --- S1, without a server -------------------------------------------------

    def test_every_wrapper_round_trips_at_the_adapter_level(self):
        """S1: to_database then from_database returns the same semantic value."""
        cases = [
            (MONEY, PostgresMoney(Decimal("1234.56")), PostgresMoney),
            (OID_ADAPTER, OID(16384), OID),
            (OID_ADAPTER, RegClass("pg_class"), RegClass),
            (OID_ADAPTER, RegType("integer"), RegType),
            (OID_ADAPTER, RegProc("now"), RegProc),
            (OID_ADAPTER, RegConfig("english"), RegConfig),
            (OID_ADAPTER, RegNamespace("public"), RegNamespace),
            (XID_ADAPTER, XID(42), XID),
            (XID_ADAPTER, XID8(42), XID8),
            (XID_ADAPTER, CID(7), CID),
            (TID_ADAPTER, TID(3, 5), TID),
            (LSN_ADAPTER, PostgresLsn.from_string("16/B374D848"), PostgresLsn),
        ]
        for adapter, value, target in cases:
            database_value = adapter.to_database(value, str)
            decoded = adapter.from_database(database_value, target)
            assert decoded == value
            assert adapter.to_database(decoded, str) == database_value

    # --- money ----------------------------------------------------------------

    def test_money_records_numeric_truth_and_stays_stable(self, native_backend):
        """S2: the numeric column value survives a read/rewrite unchanged."""
        backend = native_backend
        value = PostgresMoney(Decimal("1234.56"))
        bound = MONEY.to_database(value, str)
        assert bound == "1234.56"

        row_id = insert_row(backend, TABLE, {"amount": bound}, {"amount": "money"})
        numeric = read_raw(backend, TABLE, "amount", row_id, "amount::numeric::text")
        assert numeric == "1234.56"

        # The raw money text is locale formatted; the adapter must parse it.
        raw_text = read_raw(backend, TABLE, "amount", row_id)
        assert raw_text.startswith("$")
        decoded = MONEY.from_database(raw_text, PostgresMoney)
        assert decoded.amount == Decimal("1234.56")

        # Writing the decoded value again must not change the stored amount.
        update_cell(backend, TABLE, "amount", row_id, MONEY.to_database(decoded, str), "money")
        assert read_raw(backend, TABLE, "amount", row_id, "amount::numeric::text") == numeric

    # --- oid and the reg* family ----------------------------------------------

    def test_oid_and_registry_names_read_back_as_wrappers(self, native_backend):
        """S1/S2: reg* columns resolve names and the adapters rebuild wrappers."""
        backend = native_backend
        values = {
            "oid_value": OID_ADAPTER.to_database(OID(16384), int),
            "regclass_value": OID_ADAPTER.to_database(RegClass("pg_class"), str),
            "regtype_value": OID_ADAPTER.to_database(RegType("integer"), str),
            "regproc_value": OID_ADAPTER.to_database(RegProc("now"), str),
            "regconfig_value": OID_ADAPTER.to_database(RegConfig("english"), str),
            "regnamespace_value": OID_ADAPTER.to_database(RegNamespace("public"), str),
        }
        casts = {
            "oid_value": "oid",
            "regclass_value": "regclass",
            "regtype_value": "regtype",
            "regproc_value": "regproc",
            "regconfig_value": "regconfig",
            "regnamespace_value": "regnamespace",
        }
        row_id = insert_row(backend, TABLE, values, casts)

        assert read_raw(backend, TABLE, "oid_value", row_id) == "16384"
        assert read_raw(backend, TABLE, "regclass_value", row_id) == "pg_class"
        assert read_raw(backend, TABLE, "regtype_value", row_id) == "integer"
        assert read_raw(backend, TABLE, "regproc_value", row_id) == "now"
        assert read_raw(backend, TABLE, "regconfig_value", row_id) == "english"
        assert read_raw(backend, TABLE, "regnamespace_value", row_id) == "public"

        assert OID_ADAPTER.from_database("16384", OID) == OID(16384)
        assert OID_ADAPTER.from_database("pg_class", RegClass) == RegClass("pg_class")
        assert OID_ADAPTER.from_database("integer", RegType) == RegType("integer")
        assert OID_ADAPTER.from_database("now", RegProc) == RegProc("now")
        assert OID_ADAPTER.from_database("english", RegConfig) == RegConfig("english")
        assert OID_ADAPTER.from_database("public", RegNamespace) == RegNamespace("public")

        # S2: rewrite the canonical text the adapter produces.
        update_cell(
            backend, TABLE, "regclass_value", row_id,
            OID_ADAPTER.to_database(RegClass("pg_class"), str), "regclass",
        )
        assert read_raw(backend, TABLE, "regclass_value", row_id) == "pg_class"

    # --- transaction and command identifiers ----------------------------------

    def test_xid_xid8_and_cid_keep_their_decimal_text(self, native_backend):
        """S1/S2: xid/xid8/cid bind as integer, but the server text is decimal."""
        backend = native_backend
        values = {
            "xid_value": XID_ADAPTER.to_database(XID(42), int),
            "xid8_value": XID_ADAPTER.to_database(XID8(42), int),
            "cid_value": XID_ADAPTER.to_database(CID(7), int),
        }
        casts = {
            "xid_value": "text::xid",
            "xid8_value": "text::xid8",
            "cid_value": "text::cid",
        }
        row_id = insert_row(backend, TABLE, values, casts)

        assert read_raw(backend, TABLE, "xid_value", row_id) == "42"
        assert read_raw(backend, TABLE, "xid8_value", row_id) == "42"
        assert read_raw(backend, TABLE, "cid_value", row_id) == "7"

        assert XID_ADAPTER.from_database("42", XID) == XID(42)
        assert XID_ADAPTER.from_database("42", XID8) == XID8(42)
        assert XID_ADAPTER.from_database("7", CID) == CID(7)

        update_cell(
            backend, TABLE, "xid_value", row_id,
            XID_ADAPTER.to_database(XID(42), int), "text::xid",
        )
        assert read_raw(backend, TABLE, "xid_value", row_id) == "42"

    # --- tid and pg_lsn -------------------------------------------------------

    def test_tid_and_lsn_use_their_native_text_forms(self, native_backend):
        """S1/S2: tid prints as ``(block,offset)`` and pg_lsn as ``HI/LO``."""
        backend = native_backend
        xid_bits = LSN_ADAPTER.to_database(
            PostgresLsn.from_string("16/b374d848"), str
        )
        assert xid_bits == "16/B374D848"  # the adapter canonicalises to upper case
        values = {
            "tid_value": TID_ADAPTER.to_database(TID(3, 5), str),
            "lsn_value": xid_bits,
        }
        casts = {"tid_value": "tid", "lsn_value": "pg_lsn"}
        row_id = insert_row(backend, TABLE, values, casts)

        assert read_raw(backend, TABLE, "tid_value", row_id) == "(3,5)"
        assert read_raw(backend, TABLE, "lsn_value", row_id) == "16/B374D848"

        assert TID_ADAPTER.from_database("(3,5)", TID) == TID(3, 5)
        assert LSN_ADAPTER.from_database("16/B374D848", PostgresLsn) == (
            PostgresLsn.from_string("16/B374D848")
        )

        update_cell(
            backend, TABLE, "lsn_value", row_id,
            LSN_ADAPTER.to_database(
                LSN_ADAPTER.from_database("16/B374D848", PostgresLsn), str
            ), "pg_lsn",
        )
        assert read_raw(backend, TABLE, "lsn_value", row_id) == "16/B374D848"

    # --- S3, external literals ------------------------------------------------

    def test_external_money_oid_and_regclass_literals_are_parsed(self, native_backend):
        """S3: values written by native SQL (no adapter) read back correctly."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (amount, oid_value, regclass_value) "
            "VALUES ('9876.54'::money, 20000::oid, 'pg_class'::regclass) "
            "RETURNING id".format(TABLE),
        )

        assert read_raw(backend, TABLE, "amount", row_id, "amount::numeric::text") == "9876.54"
        assert read_raw(backend, TABLE, "oid_value", row_id) == "20000"
        assert read_raw(backend, TABLE, "regclass_value", row_id) == "pg_class"

        money = MONEY.from_database(read_raw(backend, TABLE, "amount", row_id), PostgresMoney)
        assert money.amount == Decimal("9876.54")
        assert OID_ADAPTER.from_database("20000", OID) == OID(20000)

    # --- invalid input --------------------------------------------------------

    def test_out_of_range_and_malformed_wrappers_raise(self):
        """The adapter rejects values PostgreSQL could never store."""
        with pytest.raises(ValueError):
            OID_ADAPTER.to_database(4294967296, int)
        with pytest.raises(ValueError):
            TID_ADAPTER.to_database((0, 70000), str)
        with pytest.raises(ValueError):
            LSN_ADAPTER.to_database("not-an-lsn", str)
