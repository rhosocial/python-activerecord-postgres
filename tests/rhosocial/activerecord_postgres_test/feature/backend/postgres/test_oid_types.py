# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_oid_types.py
"""Unit tests for PostgreSQL OID types.

Tests for:
- OID, RegClass, RegType and other registry type wrappers
- Transaction identifiers: XID, XID8, CID
- Tuple identifier: TID
- PostgresOidAdapter, PostgresXidAdapter, PostgresTidAdapter
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.object_identifier import (
    OID, RegClass, RegType, RegProc, RegProcedure,
    RegOper, RegOperator, RegConfig, RegDictionary,
    RegNamespace, RegRole, RegCollation,
    XID, XID8, CID, TID,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.object_identifier import (
    PostgresOidAdapter, PostgresXidAdapter, PostgresTidAdapter,
)


class TestOID:
    """Tests for OID wrapper class."""

    def test_create_valid_oid(self):
        """Test creating valid OID."""
        oid = OID(16384)
        assert oid.value == 16384

    def test_create_zero_oid(self):
        """Test creating OID with zero value."""
        oid = OID(0)
        assert oid.value == 0

    def test_create_max_oid(self):
        """Test creating OID with maximum value."""
        oid = OID(4294967295)
        assert oid.value == 4294967295

    def test_create_negative_oid_raises_error(self):
        """Test that negative OID raises ValueError."""
        with pytest.raises(ValueError, match="OID must be between 0 and 4294967295"):
            OID(-1)

    def test_create_overflow_oid_raises_error(self):
        """Test that overflow OID raises ValueError."""
        with pytest.raises(ValueError, match="OID must be between 0 and 4294967295"):
            OID(4294967296)

    def test_int_conversion(self):
        """Test integer conversion."""
        oid = OID(16384)
        assert int(oid) == 16384

    def test_string_representation(self):
        """Test string representation."""
        oid = OID(16384)
        assert str(oid) == "16384"

    def test_repr(self):
        """Test repr."""
        oid = OID(16384)
        assert repr(oid) == "OID(16384)"

    def test_equality_with_oid(self):
        """Test equality with another OID."""
        oid1 = OID(16384)
        oid2 = OID(16384)
        assert oid1 == oid2

    def test_equality_with_int(self):
        """Test equality with integer."""
        oid = OID(16384)
        assert oid == 16384

    def test_inequality(self):
        """Test inequality."""
        oid1 = OID(16384)
        oid2 = OID(16385)
        assert oid1 != oid2

    def test_hash(self):
        """Test hashability."""
        oid1 = OID(16384)
        oid2 = OID(16384)
        assert hash(oid1) == hash(oid2)
        assert len({oid1, oid2}) == 1


class TestRegClass:
    """Tests for RegClass wrapper class."""

    def test_create_with_name(self):
        """Test creating RegClass with name only."""
        reg = RegClass('users')
        assert reg.name == 'users'
        assert reg.oid is None

    def test_create_with_name_and_oid(self):
        """Test creating RegClass with name and OID."""
        reg = RegClass('users', 16384)
        assert reg.name == 'users'
        assert reg.oid == 16384

    def test_create_with_schema_qualified_name(self):
        """Test creating RegClass with schema-qualified name."""
        reg = RegClass('public.users')
        assert reg.name == 'public.users'

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegClass name cannot be empty"):
            RegClass('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegClass('users')
        assert str(reg) == 'users'

    def test_repr_without_oid(self):
        """Test repr without OID."""
        reg = RegClass('users')
        assert repr(reg) == "RegClass('users')"

    def test_repr_with_oid(self):
        """Test repr with OID."""
        reg = RegClass('users', 16384)
        assert repr(reg) == "RegClass('users', oid=16384)"


class TestRegType:
    """Tests for RegType wrapper class."""

    def test_create_with_name(self):
        """Test creating RegType with name only."""
        reg = RegType('integer')
        assert reg.name == 'integer'
        assert reg.oid is None

    def test_create_with_name_and_oid(self):
        """Test creating RegType with name and OID."""
        reg = RegType('integer', 23)
        assert reg.name == 'integer'
        assert reg.oid == 23

    def test_create_with_schema_qualified_name(self):
        """Test creating RegType with schema-qualified name."""
        reg = RegType('public.my_type')
        assert reg.name == 'public.my_type'

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegType name cannot be empty"):
            RegType('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegType('varchar')
        assert str(reg) == 'varchar'

    def test_repr_without_oid(self):
        """Test repr without OID."""
        reg = RegType('integer')
        assert repr(reg) == "RegType('integer')"

    def test_repr_with_oid(self):
        """Test repr with OID."""
        reg = RegType('integer', 23)
        assert repr(reg) == "RegType('integer', oid=23)"


class TestRegProc:
    """Tests for RegProc wrapper class."""

    def test_create_with_name(self):
        """Test creating RegProc with name only."""
        reg = RegProc('now')
        assert reg.name == 'now'
        assert reg.oid is None

    def test_create_with_schema_qualified_name(self):
        """Test creating RegProc with schema-qualified name."""
        reg = RegProc('pg_catalog.now')
        assert reg.name == 'pg_catalog.now'

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegProc name cannot be empty"):
            RegProc('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegProc('now')
        assert str(reg) == 'now'


class TestRegProcedure:
    """Tests for RegProcedure wrapper class."""

    def test_create_with_signature(self):
        """Test creating RegProcedure with function signature."""
        reg = RegProcedure('sum(integer)')
        assert reg.name == 'sum(integer)'
        assert reg.oid is None

    def test_create_with_schema_qualified_signature(self):
        """Test creating RegProcedure with schema-qualified signature."""
        reg = RegProcedure('pg_catalog.sum(bigint)')
        assert reg.name == 'pg_catalog.sum(bigint)'

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegProcedure name cannot be empty"):
            RegProcedure('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegProcedure('sum(integer)')
        assert str(reg) == 'sum(integer)'


class TestRegOper:
    """Tests for RegOper wrapper class."""

    def test_create_with_name(self):
        """Test creating RegOper with operator name."""
        reg = RegOper('+')
        assert reg.name == '+'
        assert reg.oid is None

    def test_create_with_schema_qualified_name(self):
        """Test creating RegOper with schema-qualified name."""
        reg = RegOper('pg_catalog.||')
        assert reg.name == 'pg_catalog.||'

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegOper name cannot be empty"):
            RegOper('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegOper('+')
        assert str(reg) == '+'


class TestRegOperator:
    """Tests for RegOperator wrapper class."""

    def test_create_with_signature(self):
        """Test creating RegOperator with operator signature."""
        reg = RegOperator('+(integer,integer)')
        assert reg.name == '+(integer,integer)'
        assert reg.oid is None

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegOperator name cannot be empty"):
            RegOperator('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegOperator('+(integer,integer)')
        assert str(reg) == '+(integer,integer)'


class TestRegConfig:
    """Tests for RegConfig wrapper class."""

    def test_create_with_name(self):
        """Test creating RegConfig with name."""
        reg = RegConfig('english')
        assert reg.name == 'english'
        assert reg.oid is None

    def test_create_with_schema_qualified_name(self):
        """Test creating RegConfig with schema-qualified name."""
        reg = RegConfig('pg_catalog.english')
        assert reg.name == 'pg_catalog.english'

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegConfig name cannot be empty"):
            RegConfig('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegConfig('english')
        assert str(reg) == 'english'


class TestRegDictionary:
    """Tests for RegDictionary wrapper class."""

    def test_create_with_name(self):
        """Test creating RegDictionary with name."""
        reg = RegDictionary('english_stem')
        assert reg.name == 'english_stem'
        assert reg.oid is None

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegDictionary name cannot be empty"):
            RegDictionary('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegDictionary('english_stem')
        assert str(reg) == 'english_stem'


class TestRegNamespace:
    """Tests for RegNamespace wrapper class."""

    def test_create_with_name(self):
        """Test creating RegNamespace with name."""
        reg = RegNamespace('public')
        assert reg.name == 'public'
        assert reg.oid is None

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegNamespace name cannot be empty"):
            RegNamespace('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegNamespace('public')
        assert str(reg) == 'public'


class TestRegRole:
    """Tests for RegRole wrapper class."""

    def test_create_with_name(self):
        """Test creating RegRole with name."""
        reg = RegRole('postgres')
        assert reg.name == 'postgres'
        assert reg.oid is None

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegRole name cannot be empty"):
            RegRole('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegRole('postgres')
        assert str(reg) == 'postgres'


class TestRegCollation:
    """Tests for RegCollation wrapper class."""

    def test_create_with_name(self):
        """Test creating RegCollation with name."""
        reg = RegCollation('en_US')
        assert reg.name == 'en_US'
        assert reg.oid is None

    def test_create_empty_name_raises_error(self):
        """Test that empty name raises ValueError."""
        with pytest.raises(ValueError, match="RegCollation name cannot be empty"):
            RegCollation('')

    def test_string_representation(self):
        """Test string representation."""
        reg = RegCollation('en_US')
        assert str(reg) == 'en_US'


class TestXID:
    """Tests for XID transaction identifier."""

    def test_create_valid_xid(self):
        """Test creating valid XID."""
        xid = XID(1000)
        assert xid.value == 1000

    def test_create_zero_xid(self):
        """Test creating XID with zero value."""
        xid = XID(0)
        assert xid.value == 0

    def test_create_max_xid(self):
        """Test creating XID with maximum value."""
        xid = XID(4294967295)
        assert xid.value == 4294967295

    def test_create_negative_xid_raises_error(self):
        """Test that negative XID raises ValueError."""
        with pytest.raises(ValueError, match="XID must be between 0 and 4294967295"):
            XID(-1)

    def test_create_overflow_xid_raises_error(self):
        """Test that overflow XID raises ValueError."""
        with pytest.raises(ValueError, match="XID must be between 0 and 4294967295"):
            XID(4294967296)

    def test_int_conversion(self):
        """Test integer conversion."""
        xid = XID(1000)
        assert int(xid) == 1000

    def test_string_representation(self):
        """Test string representation."""
        xid = XID(1000)
        assert str(xid) == "1000"

    def test_repr(self):
        """Test repr."""
        xid = XID(1000)
        assert repr(xid) == "XID(1000)"

    def test_equality_with_xid(self):
        """Test equality with another XID."""
        xid1 = XID(1000)
        xid2 = XID(1000)
        assert xid1 == xid2

    def test_equality_with_int(self):
        """Test equality with integer."""
        xid = XID(1000)
        assert xid == 1000

    def test_inequality(self):
        """Test inequality."""
        xid1 = XID(1000)
        xid2 = XID(2000)
        assert xid1 != xid2

    def test_hash(self):
        """Test hashability."""
        xid1 = XID(1000)
        xid2 = XID(1000)
        assert hash(xid1) == hash(xid2)
        assert len({xid1, xid2}) == 1


class TestXID8:
    """Tests for XID8 transaction identifier."""

    def test_create_valid_xid8(self):
        """Test creating valid XID8."""
        xid8 = XID8(1000000000)
        assert xid8.value == 1000000000

    def test_create_zero_xid8(self):
        """Test creating XID8 with zero value."""
        xid8 = XID8(0)
        assert xid8.value == 0

    def test_create_max_xid8(self):
        """Test creating XID8 with maximum value."""
        xid8 = XID8(18446744073709551615)
        assert xid8.value == 18446744073709551615

    def test_create_negative_xid8_raises_error(self):
        """Test that negative XID8 raises ValueError."""
        with pytest.raises(ValueError, match="XID8 must be between 0 and 18446744073709551615"):
            XID8(-1)

    def test_create_overflow_xid8_raises_error(self):
        """Test that overflow XID8 raises ValueError."""
        with pytest.raises(ValueError, match="XID8 must be between 0 and 18446744073709551615"):
            XID8(18446744073709551616)

    def test_int_conversion(self):
        """Test integer conversion."""
        xid8 = XID8(1000000000)
        assert int(xid8) == 1000000000

    def test_string_representation(self):
        """Test string representation."""
        xid8 = XID8(1000000000)
        assert str(xid8) == "1000000000"

    def test_repr(self):
        """Test repr."""
        xid8 = XID8(1000000000)
        assert repr(xid8) == "XID8(1000000000)"

    def test_equality_with_xid8(self):
        """Test equality with another XID8."""
        xid8_1 = XID8(1000000000)
        xid8_2 = XID8(1000000000)
        assert xid8_1 == xid8_2

    def test_equality_with_int(self):
        """Test equality with integer."""
        xid8 = XID8(1000000000)
        assert xid8 == 1000000000

    def test_hash(self):
        """Test hashability."""
        xid8_1 = XID8(1000000000)
        xid8_2 = XID8(1000000000)
        assert hash(xid8_1) == hash(xid8_2)


class TestCID:
    """Tests for CID command identifier."""

    def test_create_valid_cid(self):
        """Test creating valid CID."""
        cid = CID(5)
        assert cid.value == 5

    def test_create_zero_cid(self):
        """Test creating CID with zero value."""
        cid = CID(0)
        assert cid.value == 0

    def test_create_max_cid(self):
        """Test creating CID with maximum value."""
        cid = CID(4294967295)
        assert cid.value == 4294967295

    def test_create_negative_cid_raises_error(self):
        """Test that negative CID raises ValueError."""
        with pytest.raises(ValueError, match="CID must be between 0 and 4294967295"):
            CID(-1)

    def test_create_overflow_cid_raises_error(self):
        """Test that overflow CID raises ValueError."""
        with pytest.raises(ValueError, match="CID must be between 0 and 4294967295"):
            CID(4294967296)

    def test_int_conversion(self):
        """Test integer conversion."""
        cid = CID(5)
        assert int(cid) == 5

    def test_string_representation(self):
        """Test string representation."""
        cid = CID(5)
        assert str(cid) == "5"

    def test_repr(self):
        """Test repr."""
        cid = CID(5)
        assert repr(cid) == "CID(5)"

    def test_equality_with_cid(self):
        """Test equality with another CID."""
        cid1 = CID(5)
        cid2 = CID(5)
        assert cid1 == cid2

    def test_equality_with_int(self):
        """Test equality with integer."""
        cid = CID(5)
        assert cid == 5

    def test_hash(self):
        """Test hashability."""
        cid1 = CID(5)
        cid2 = CID(5)
        assert hash(cid1) == hash(cid2)


class TestTID:
    """Tests for TID tuple identifier."""

    def test_create_valid_tid(self):
        """Test creating valid TID."""
        tid = TID(0, 1)
        assert tid.block == 0
        assert tid.offset == 1

    def test_create_with_large_block(self):
        """Test creating TID with large block number."""
        tid = TID(4294967295, 1)
        assert tid.block == 4294967295

    def test_create_with_large_offset(self):
        """Test creating TID with large offset."""
        tid = TID(0, 65535)
        assert tid.offset == 65535

    def test_create_negative_block_raises_error(self):
        """Test that negative block raises ValueError."""
        with pytest.raises(ValueError, match="Block number must be between 0 and 4294967295"):
            TID(-1, 1)

    def test_create_overflow_block_raises_error(self):
        """Test that overflow block raises ValueError."""
        with pytest.raises(ValueError, match="Block number must be between 0 and 4294967295"):
            TID(4294967296, 1)

    def test_create_negative_offset_raises_error(self):
        """Test that negative offset raises ValueError."""
        with pytest.raises(ValueError, match="Offset must be between 0 and 65535"):
            TID(0, -1)

    def test_create_overflow_offset_raises_error(self):
        """Test that overflow offset raises ValueError."""
        with pytest.raises(ValueError, match="Offset must be between 0 and 65535"):
            TID(0, 65536)

    def test_string_representation(self):
        """Test string representation."""
        tid = TID(0, 1)
        assert str(tid) == "(0,1)"

    def test_repr(self):
        """Test repr."""
        tid = TID(100, 50)
        assert repr(tid) == "TID(block=100, offset=50)"

    def test_equality(self):
        """Test equality."""
        tid1 = TID(0, 1)
        tid2 = TID(0, 1)
        assert tid1 == tid2

    def test_inequality(self):
        """Test inequality."""
        tid1 = TID(0, 1)
        tid2 = TID(0, 2)
        assert tid1 != tid2

    def test_hash(self):
        """Test hashability."""
        tid1 = TID(0, 1)
        tid2 = TID(0, 1)
        assert hash(tid1) == hash(tid2)
        assert len({tid1, tid2}) == 1

    def test_to_postgres_string(self):
        """Test PostgreSQL string format."""
        tid = TID(100, 50)
        assert tid.to_postgres_string() == "(100,50)"

    def test_from_postgres_string(self):
        """Test parsing PostgreSQL tid string."""
        tid = TID.from_postgres_string("(100,50)")
        assert tid.block == 100
        assert tid.offset == 50

    def test_from_postgres_string_with_spaces(self):
        """Test parsing PostgreSQL tid string with spaces."""
        tid = TID.from_postgres_string("( 100 , 50 )")
        assert tid.block == 100
        assert tid.offset == 50

    def test_from_postgres_string_invalid_format(self):
        """Test parsing invalid tid string."""
        with pytest.raises(ValueError):
            TID.from_postgres_string("not_valid")


class TestPostgresOidAdapter:
    """Tests for PostgresOidAdapter."""

    def test_supported_types(self):
        """Test supported types property."""
        adapter = PostgresOidAdapter()
        supported = adapter.supported_types
        assert OID in supported
        assert RegClass in supported
        assert RegType in supported

    def test_to_database_oid(self):
        """Test converting OID to database."""
        adapter = PostgresOidAdapter()
        oid = OID(16384)
        result = adapter.to_database(oid, int)
        assert result == 16384

    def test_to_database_int(self):
        """Test converting int to database."""
        adapter = PostgresOidAdapter()
        result = adapter.to_database(16384, int)
        assert result == 16384

    def test_to_database_int_out_of_range(self):
        """Test converting out of range int raises error."""
        adapter = PostgresOidAdapter()
        with pytest.raises(ValueError, match="OID value out of range"):
            adapter.to_database(4294967296, int)

    def test_to_database_regclass(self):
        """Test converting RegClass to database."""
        adapter = PostgresOidAdapter()
        reg = RegClass('users')
        result = adapter.to_database(reg, str)
        assert result == 'users'

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresOidAdapter()
        result = adapter.to_database('users', str)
        assert result == 'users'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresOidAdapter()
        result = adapter.to_database(None, int)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises error."""
        adapter = PostgresOidAdapter()
        with pytest.raises(TypeError):
            adapter.to_database([], int)

    def test_from_database_int_to_oid(self):
        """Test converting int from database to OID."""
        adapter = PostgresOidAdapter()
        result = adapter.from_database(16384, OID)
        assert isinstance(result, OID)
        assert result.value == 16384

    def test_from_database_string_to_oid(self):
        """Test converting string from database to OID."""
        adapter = PostgresOidAdapter()
        result = adapter.from_database('16384', OID)
        assert isinstance(result, OID)
        assert result.value == 16384

    def test_from_database_string_to_regclass(self):
        """Test converting string from database to RegClass."""
        adapter = PostgresOidAdapter()
        result = adapter.from_database('users', RegClass)
        assert isinstance(result, RegClass)
        assert result.name == 'users'

    def test_from_database_int_to_regclass(self):
        """Test converting int from database to RegClass."""
        adapter = PostgresOidAdapter()
        result = adapter.from_database(16384, RegClass)
        assert isinstance(result, RegClass)
        assert result.name == '16384'
        assert result.oid == 16384

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresOidAdapter()
        result = adapter.from_database(None, OID)
        assert result is None

    def test_from_database_to_int(self):
        """Test converting to int."""
        adapter = PostgresOidAdapter()
        result = adapter.from_database(16384, int)
        assert result == 16384
        result = adapter.from_database('16384', int)
        assert result == 16384

    def test_from_database_to_str(self):
        """Test converting to str."""
        adapter = PostgresOidAdapter()
        result = adapter.from_database(16384, str)
        assert result == '16384'

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresOidAdapter()
        values = [OID(16384), 16385, 'users', None]
        results = adapter.to_database_batch(values, int)
        assert results[0] == 16384
        assert results[1] == 16385
        assert results[2] == 'users'
        assert results[3] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresOidAdapter()
        values = [16384, 16385, None]
        results = adapter.from_database_batch(values, OID)
        assert isinstance(results[0], OID)
        assert results[0].value == 16384
        assert isinstance(results[1], OID)
        assert results[1].value == 16385
        assert results[2] is None


class TestPostgresXidAdapter:
    """Tests for PostgresXidAdapter."""

    def test_supported_types(self):
        """Test supported types property."""
        adapter = PostgresXidAdapter()
        supported = adapter.supported_types
        assert XID in supported
        assert XID8 in supported
        assert CID in supported

    def test_to_database_xid(self):
        """Test converting XID to database."""
        adapter = PostgresXidAdapter()
        xid = XID(1000)
        result = adapter.to_database(xid, int)
        assert result == 1000

    def test_to_database_xid8(self):
        """Test converting XID8 to database."""
        adapter = PostgresXidAdapter()
        xid8 = XID8(1000000000)
        result = adapter.to_database(xid8, int)
        assert result == 1000000000

    def test_to_database_cid(self):
        """Test converting CID to database."""
        adapter = PostgresXidAdapter()
        cid = CID(5)
        result = adapter.to_database(cid, int)
        assert result == 5

    def test_to_database_int(self):
        """Test converting int to database."""
        adapter = PostgresXidAdapter()
        result = adapter.to_database(1000, int)
        assert result == 1000

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresXidAdapter()
        result = adapter.to_database('1000', int)
        assert result == 1000

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresXidAdapter()
        result = adapter.to_database(None, int)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises error."""
        adapter = PostgresXidAdapter()
        with pytest.raises(TypeError):
            adapter.to_database([], int)

    def test_from_database_int_to_xid(self):
        """Test converting int from database to XID."""
        adapter = PostgresXidAdapter()
        result = adapter.from_database(1000, XID)
        assert isinstance(result, XID)
        assert result.value == 1000

    def test_from_database_string_to_xid(self):
        """Test converting string from database to XID."""
        adapter = PostgresXidAdapter()
        result = adapter.from_database('1000', XID)
        assert isinstance(result, XID)
        assert result.value == 1000

    def test_from_database_int_to_xid8(self):
        """Test converting int from database to XID8."""
        adapter = PostgresXidAdapter()
        result = adapter.from_database(1000000000, XID8)
        assert isinstance(result, XID8)
        assert result.value == 1000000000

    def test_from_database_int_to_cid(self):
        """Test converting int from database to CID."""
        adapter = PostgresXidAdapter()
        result = adapter.from_database(5, CID)
        assert isinstance(result, CID)
        assert result.value == 5

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresXidAdapter()
        result = adapter.from_database(None, XID)
        assert result is None

    def test_from_database_to_int(self):
        """Test converting to int."""
        adapter = PostgresXidAdapter()
        result = adapter.from_database(1000, int)
        assert result == 1000
        result = adapter.from_database('1000', int)
        assert result == 1000

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresXidAdapter()
        values = [XID(1000), XID8(1000000000), CID(5), 2000, '3000', None]
        results = adapter.to_database_batch(values, int)
        assert results[0] == 1000
        assert results[1] == 1000000000
        assert results[2] == 5
        assert results[3] == 2000
        assert results[4] == 3000
        assert results[5] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresXidAdapter()
        values = [1000, '2000', None]
        results = adapter.from_database_batch(values, XID)
        assert isinstance(results[0], XID)
        assert results[0].value == 1000
        assert isinstance(results[1], XID)
        assert results[1].value == 2000
        assert results[2] is None


class TestPostgresTidAdapter:
    """Tests for PostgresTidAdapter."""

    def test_supported_types(self):
        """Test supported types property."""
        adapter = PostgresTidAdapter()
        supported = adapter.supported_types
        assert TID in supported

    def test_to_database_tid(self):
        """Test converting TID to database."""
        adapter = PostgresTidAdapter()
        tid = TID(100, 50)
        result = adapter.to_database(tid, str)
        assert result == '(100,50)'

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresTidAdapter()
        result = adapter.to_database('(100,50)', str)
        assert result == '(100,50)'

    def test_to_database_tuple(self):
        """Test converting tuple to database."""
        adapter = PostgresTidAdapter()
        result = adapter.to_database((100, 50), str)
        assert result == '(100,50)'

    def test_to_database_list(self):
        """Test converting list to database."""
        adapter = PostgresTidAdapter()
        result = adapter.to_database([100, 50], str)
        assert result == '(100,50)'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresTidAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises error."""
        adapter = PostgresTidAdapter()
        with pytest.raises(TypeError):
            adapter.to_database(123, str)

    def test_from_database_string_to_tid(self):
        """Test converting string from database to TID."""
        adapter = PostgresTidAdapter()
        result = adapter.from_database('(100,50)', TID)
        assert isinstance(result, TID)
        assert result.block == 100
        assert result.offset == 50

    def test_from_database_tid_passthrough(self):
        """Test that TID passes through."""
        adapter = PostgresTidAdapter()
        tid = TID(100, 50)
        result = adapter.from_database(tid, TID)
        assert result is tid

    def test_from_database_tuple_to_tid(self):
        """Test converting tuple from database to TID."""
        adapter = PostgresTidAdapter()
        result = adapter.from_database((100, 50), TID)
        assert isinstance(result, TID)
        assert result.block == 100
        assert result.offset == 50

    def test_from_database_list_to_tid(self):
        """Test converting list from database to TID."""
        adapter = PostgresTidAdapter()
        result = adapter.from_database([100, 50], TID)
        assert isinstance(result, TID)
        assert result.block == 100
        assert result.offset == 50

    def test_from_database_tid_to_tuple(self):
        """Test converting TID to tuple."""
        adapter = PostgresTidAdapter()
        tid = TID(100, 50)
        result = adapter.from_database(tid, tuple)
        assert result == (100, 50)

    def test_from_database_string_to_tuple(self):
        """Test converting string to tuple."""
        adapter = PostgresTidAdapter()
        result = adapter.from_database('(100,50)', tuple)
        assert result == (100, 50)

    def test_from_database_tid_to_str(self):
        """Test converting TID to str."""
        adapter = PostgresTidAdapter()
        tid = TID(100, 50)
        result = adapter.from_database(tid, str)
        assert result == '(100,50)'

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresTidAdapter()
        result = adapter.from_database(None, TID)
        assert result is None

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresTidAdapter()
        values = [TID(0, 1), '(100,50)', (200, 75), None]
        results = adapter.to_database_batch(values, str)
        assert results[0] == '(0,1)'
        assert results[1] == '(100,50)'
        assert results[2] == '(200,75)'
        assert results[3] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresTidAdapter()
        values = ['(0,1)', '(100,50)', None]
        results = adapter.from_database_batch(values, TID)
        assert isinstance(results[0], TID)
        assert results[0].block == 0
        assert results[0].offset == 1
        assert isinstance(results[1], TID)
        assert results[1].block == 100
        assert results[1].offset == 50
        assert results[2] is None
