# tests/rhosocial/activerecord_postgres_test/feature/backend/adapters/native/test_native_bit_strings.py
"""Native tests for PostgreSQL ``bit(n)`` and ``varbit``.

The neutral ``BitString`` adapter binds the bare binary digits; ``bit(n)``
right-pads them exactly as PostgreSQL does.  The wrapper
``PostgresBitStringAdapter`` emits the SQL literal ``B'1010'`` instead, which
is used inline for the wrapper path.  The bit length is asserted separately
from the bytes.
"""

import pytest

from rhosocial.activerecord.backend.adapters import BitString
from rhosocial.activerecord.backend.errors import TypeConversionError
from rhosocial.activerecord.backend.impl.postgres.adapters.bit_string import (
    PostgresBitStringAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.neutral import (
    PostgresNeutralBitStringAdapter,
)
from rhosocial.activerecord.backend.impl.postgres.type_values import (
    PostgresBitString,
)
from rhosocial.activerecord_postgres_test.feature.backend.adapters.native.helpers import (
    insert_row,
    insert_sql,
    read_raw,
    update_cell,
)

TABLE = "native_bits"

WRAPPER = PostgresBitStringAdapter()
NEUTRAL = PostgresNeutralBitStringAdapter()


class TestBitStrings:
    TABLES = (TABLE,)

    def test_bit8_pads_on_the_right_and_stays_stable(self, native_backend):
        """S2: a short input into bit(8) is padded, and re-writing is a no-op."""
        backend = native_backend
        value = BitString.from_bits("1010")
        bound = NEUTRAL.to_database(value, str)
        assert bound == "1010"

        row_id = insert_row(backend, TABLE, {"fixed_bits": bound}, {"fixed_bits": "bit(8)"})
        raw = read_raw(backend, TABLE, "fixed_bits", row_id)
        assert raw == "10100000"

        decoded = NEUTRAL.from_database(raw, BitString)
        assert decoded.to_bits() == "10100000"
        assert decoded.bit_length == 8

        update_cell(
            backend, TABLE, "fixed_bits", row_id,
            NEUTRAL.to_database(decoded, str), "bit(8)",
        )
        assert read_raw(backend, TABLE, "fixed_bits", row_id) == raw

    def test_varbit_preserves_non_byte_aligned_lengths(self, native_backend):
        """S1/S2: five bits stay five bits in both varbit and bit(5)."""
        backend = native_backend
        value = BitString.from_bits("10101")
        row_id = insert_row(
            backend,
            TABLE,
            {
                "var_bits": NEUTRAL.to_database(value, str),
                "five_bits": NEUTRAL.to_database(value, str),
            },
            {"var_bits": "varbit", "five_bits": "bit(5)"},
        )

        assert read_raw(backend, TABLE, "var_bits", row_id) == "10101"
        assert read_raw(backend, TABLE, "five_bits", row_id) == "10101"

        decoded = NEUTRAL.from_database("10101", BitString)
        assert decoded == value
        assert decoded.bit_length == 5

        update_cell(backend, TABLE, "var_bits", row_id, "10101", "varbit")
        assert read_raw(backend, TABLE, "var_bits", row_id) == "10101"

    def test_declared_length_option_pads_external_text(self):
        """S1: an explicit bit length restores the leading zeros in text."""
        padded = NEUTRAL.from_database("1010", BitString, {"bit_length": 8})
        assert padded.to_bits() == "10100000"
        assert padded.bit_length == 8

    def test_wrapper_adapter_emits_an_sql_literal(self, native_backend):
        """The wrapper adapter's wire form is ``B'...'``, used inline."""
        backend = native_backend
        value = PostgresBitString("1010")
        literal = WRAPPER.to_database(value, str)
        assert literal == "B'1010'"

        row_id = insert_sql(
            backend,
            "INSERT INTO {} (five_bits) VALUES ({}::bit(5)) RETURNING id".format(
                TABLE, literal
            ),
        )
        raw = read_raw(backend, TABLE, "five_bits", row_id)
        assert raw == "10100"
        decoded = WRAPPER.from_database(raw, PostgresBitString)
        assert decoded == PostgresBitString("10100")

    def test_external_bit_literal_is_accepted(self, native_backend):
        """S3: ``B'101'`` written by native SQL reads back as three bits."""
        backend = native_backend
        row_id = insert_sql(
            backend,
            "INSERT INTO {} (var_bits) VALUES (B'101'::varbit) RETURNING id".format(
                TABLE
            ),
        )
        raw = read_raw(backend, TABLE, "var_bits", row_id)
        assert raw == "101"
        decoded = NEUTRAL.from_database(raw, BitString)
        assert decoded == BitString.from_bits("101")
        assert decoded.bit_length == 3

    def test_invalid_bit_text_is_wrapped(self):
        """Non-binary text is rejected by the adapter, not passed to the driver."""
        for text in ("102", "", "B'101"):
            with pytest.raises(TypeConversionError):
                NEUTRAL.from_database(text, BitString)
