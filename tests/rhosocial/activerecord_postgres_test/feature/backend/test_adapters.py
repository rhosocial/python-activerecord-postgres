# tests/rhosocial/activerecord_postgres_test/feature/backend/test_adapters.py
"""Offline adapter round-trip coverage for the PostgreSQL backend."""
import uuid
from decimal import Decimal
from typing import Any, Tuple

import pytest

from rhosocial.activerecord.backend.impl.postgres.adapters.bit_string import PostgresBitStringAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.hstore import PostgresHstoreAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.json import PostgresJSONBAdapter, PostgresJsonPathAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.pg_lsn import PostgresLsnAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.monetary import PostgresMoneyAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.network_address import PostgresMacaddrAdapter, PostgresNetworkAddressAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.object_identifier import PostgresOidAdapter, PostgresTidAdapter, PostgresXidAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.text_search import PostgresTsQueryAdapter, PostgresTsVectorAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.uuid import PostgresUUIDAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.xml import PostgresXMLAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.range import PostgresRangeAdapter, PostgresMultirangeAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.base import PostgresListAdapter, PostgresEnumAdapter
from rhosocial.activerecord.backend.impl.postgres.adapters.pgvector import PostgresVectorAdapter
from rhosocial.activerecord.backend.impl.postgres.types.range import (
    PostgresRange as PG_RANGE,
    PostgresMultirange as PG_MULTIRANGE,
)


@pytest.fixture
def uuid_a(): return PostgresUUIDAdapter()
@pytest.fixture
def jsonb(): return PostgresJSONBAdapter()
@pytest.fixture
def json_path(): return PostgresJsonPathAdapter()
@pytest.fixture
def range_a(): return PostgresRangeAdapter()
@pytest.fixture
def multi_range(): return PostgresMultirangeAdapter()
@pytest.fixture
def net_addr(): return PostgresNetworkAddressAdapter()
@pytest.fixture
def mac(): return PostgresMacaddrAdapter()
@pytest.fixture
def lsn(): return PostgresLsnAdapter()
@pytest.fixture
def money(): return PostgresMoneyAdapter()
@pytest.fixture
def hstore(): return PostgresHstoreAdapter()
@pytest.fixture
def tsvec(): return PostgresTsVectorAdapter()
@pytest.fixture
def tsquery(): return PostgresTsQueryAdapter()
@pytest.fixture
def xml_a(): return PostgresXMLAdapter()
@pytest.fixture
def bit(): return PostgresBitStringAdapter()
@pytest.fixture
def oid(): return PostgresOidAdapter()
@pytest.fixture
def tid(): return PostgresTidAdapter()
@pytest.fixture
def xid(): return PostgresXidAdapter()
@pytest.fixture
def list_a(): return PostgresListAdapter()
@pytest.fixture
def enum_a(): return PostgresEnumAdapter()
@pytest.fixture
def vec(): return PostgresVectorAdapter()


class TestUUID:
    def test_roundtrip(self, uuid_a):
        u = uuid.uuid4()
        assert uuid_a.to_database(u, str) == str(u)
        assert uuid_a.from_database(str(u), uuid.UUID) == u
    def test_none(self, uuid_a):
        assert uuid_a.to_database(None, str) is None

class TestJSONB:
    def test_to_database_returns_jsonb(self, jsonb):
        val = {"k": 1, "v": [2, 3]}
        result = jsonb.to_database(val, str)
        assert str(result)  # serializable to JSON string

class TestJsonPath:
    def test_roundtrip(self, json_path):
        p = '$.store.book[*].author'
        assert json_path.from_database(p, str) == p

class TestRange:
    def test_to_database(self, range_a):
        r = PG_RANGE(1, 10, lower_inc=True, upper_inc=False)
        s = r.to_postgres_string()
        assert isinstance(s, str)
        assert range_a.to_database(r, str) == s

class TestMultirange:
    def test_to_database(self, multi_range):
        mr = PG_MULTIRANGE([])
        assert multi_range.to_database(mr, str) is not None

class TestNetworkAddress:
    def test_inet_roundtrip(self, net_addr):
        assert net_addr.to_database("192.168.1.1", str) == "192.168.1.1"
        parsed = net_addr.from_database("192.168.1.1", str)
        assert str(parsed) == "192.168.1.1"  # IPv4Address/IPInterface repr
    def test_none(self, net_addr):
        assert net_addr.to_database(None, str) is None

class TestMacaddr:
    def test_roundtrip(self, mac):
        m = "08:00:2b:01:02:03"
        assert mac.to_database(m, str) == m
        assert mac.from_database(m, str) == m

class TestLsn:
    def test_roundtrip(self, lsn):
        s = "0/3000000"
        assert lsn.from_database(s, str) == s

class TestHstore:
    def test_roundtrip(self, hstore):
        d = {"key": "val", "k2": "v2"}
        s = hstore.to_database(d, str)
        assert isinstance(s, str)
        assert hstore.from_database(s, dict) == d

class TestTsVector:
    def test_from_database(self, tsvec):
        result = tsvec.from_database("hello world", str)
        assert result is not None
        assert hasattr(result, "lexemes")

class TestTsQuery:
    def test_from_database(self, tsquery):
        result = tsquery.from_database("hello & world", str)
        assert result is not None

class TestXML:
    def test_roundtrip(self, xml_a):
        s = "<root><a>1</a></root>"
        assert xml_a.from_database(s, str) == s

class TestBitString:
    def test_roundtrip(self, bit):
        s = "101010"
        assert bit.from_database(s, str) == s

class TestOid:
    def test_roundtrip(self, oid):
        assert oid.from_database(42, int) == 42

class TestXid:
    def test_roundtrip(self, xid):
        assert xid.from_database(123, int) == 123

class TestTid:
    def test_roundtrip(self, tid):
        s = "(0,1)"
        assert tid.from_database(s, str) == s

class TestMoney:
    def test_roundtrip(self, money):
        assert money.from_database(Decimal("100.50"), Decimal) == Decimal("100.50")
    def test_none(self, money):
        assert money.from_database(None, Decimal) is None

class TestEnum:
    def test_roundtrip(self, enum_a):
        s = "draft"
        assert enum_a.to_database(s, str) == s
        assert enum_a.from_database(s, str) == s

class TestList:
    def test_roundtrip(self, list_a):
        arr = [1, 2, 3]
        assert list_a.from_database(arr, list) == arr

class TestVector:
    def test_roundtrip(self, vec):
        v = [1.0, 2.5, 3.0]
        s = vec.to_database(v, str)
        assert isinstance(s, str)
        assert vec.from_database(s, list) == v