# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/types/test_xml_integration.py
"""
Integration tests for PostgreSQL XML type with real database.

These tests require a live PostgreSQL connection and test:
- xml storage and retrieval
- PostgresXMLAdapter output accepted by PostgreSQL
- XML function expressions executed by PostgreSQL
- Sync/async round-trip behavior
"""
import pytest
import pytest_asyncio

from rhosocial.activerecord.backend.expression import core
from rhosocial.activerecord.backend.expression.functions.xml import xmlparse
from rhosocial.activerecord.backend.impl.postgres.adapters.xml import PostgresXMLAdapter
from rhosocial.activerecord.backend.impl.postgres.functions.xml import (
    xml_is_well_formed,
    xpath_exists,
    xpath_query,
)
from rhosocial.activerecord.backend.impl.postgres.types.xml import PostgresXML


XML_TABLE = "test_xml_types"
ASYNC_XML_TABLE = "test_xml_types_async"


def _xml_literal(value):
    adapter = PostgresXMLAdapter()
    database_value = adapter.to_database(value, str)
    if database_value is None:
        return "NULL"
    escaped = database_value.replace("'", "''")
    return f"'{escaped}'::xml"


def _execute_expression(backend, expression, alias="result"):
    sql, params = expression.to_sql()
    return backend.fetch_one(f"SELECT {sql} AS {alias}", params)


def _execute_text_expression(backend, expression, alias="result"):
    sql, params = expression.to_sql()
    return backend.fetch_one(f"SELECT ({sql})::text AS {alias}", params)


class TestSyncXMLIntegration:
    """Synchronous integration tests for PostgreSQL XML type."""

    @pytest.fixture
    def xml_test_table(self, postgres_backend):
        """Create a table containing an xml column for sync tests."""
        postgres_backend.execute(f"DROP TABLE IF EXISTS {XML_TABLE}")
        postgres_backend.execute(f"""
            CREATE TABLE {XML_TABLE} (
                id SERIAL PRIMARY KEY,
                document xml
            )
        """)
        yield XML_TABLE
        postgres_backend.execute(f"DROP TABLE IF EXISTS {XML_TABLE}")

    def test_insert_and_select_xml_value(self, postgres_backend, xml_test_table):
        """Insert a PostgresXML document and verify PostgreSQL returns the XML text."""
        literal = _xml_literal(PostgresXML("<root><item>value</item></root>"))

        postgres_backend.execute(
            f"INSERT INTO {xml_test_table} (document) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT document::text AS document FROM {xml_test_table} WHERE id = 1"
        )

        assert result["document"] == "<root><item>value</item></root>"

    def test_insert_xml_from_plain_str(self, postgres_backend, xml_test_table):
        """Pass a plain XML string through the adapter and verify stored XML text."""
        literal = _xml_literal("<root><item id='1'>value</item></root>")

        postgres_backend.execute(
            f"INSERT INTO {xml_test_table} (document) VALUES ({literal})"
        )
        result = postgres_backend.fetch_one(
            f"SELECT document::text AS document FROM {xml_test_table}"
        )

        assert result["document"] == "<root><item id='1'>value</item></root>"

    def test_null_xml_round_trip(self, postgres_backend, xml_test_table):
        """Insert a NULL XML value and verify the fetched value is None."""
        postgres_backend.execute(
            f"INSERT INTO {xml_test_table} (document) VALUES (NULL)"
        )
        result = postgres_backend.fetch_one(
            f"SELECT document FROM {xml_test_table} WHERE id = 1"
        )

        assert result["document"] is None

    def test_xml_xpath_filter_matches_document(self, postgres_backend, xml_test_table):
        """Insert two XML documents and verify XPath filtering matches one row."""
        matching = _xml_literal("<root><item>target</item></root>")
        other = _xml_literal("<root><item>other</item></root>")

        postgres_backend.execute(
            f"INSERT INTO {xml_test_table} (document) VALUES ({matching}), ({other})"
        )
        result = postgres_backend.fetch_one(f"""
            SELECT COUNT(*) AS match_count
            FROM {xml_test_table}
            WHERE xpath_exists('/root/item[text()="target"]', document)
        """)

        assert result["match_count"] == 1

    def test_invalid_xml_rejected_by_postgres(self, postgres_backend, xml_test_table):
        """Insert malformed XML and verify PostgreSQL rejects it during execution."""
        with pytest.raises(Exception):  # noqa: B017
            postgres_backend.execute(
                f"INSERT INTO {xml_test_table} (document) VALUES ('<root>'::xml)"
            )


class TestSyncXMLFunctionsIntegration:
    """Synchronous integration tests for PostgreSQL XML functions."""

    def test_xmlparse_executes(self, postgres_backend):
        """Execute xmlparse and verify PostgreSQL returns the parsed XML text."""
        expr = xmlparse(
            postgres_backend.dialect,
            "<root><item>value</item></root>",
            document=True,
        )
        result = _execute_text_expression(postgres_backend, expr)

        assert result["result"] == "<root><item>value</item></root>"

    def test_xpath_query_executes(self, postgres_backend):
        """Execute xpath query and verify PostgreSQL returns the matching XML node."""
        xml_expr = core.Subquery(
            postgres_backend.dialect,
            "'<root><item>value</item></root>'::xml",
        )
        expr = xpath_query(postgres_backend.dialect, "/root/item/text()", xml_expr)
        result = _execute_expression(postgres_backend, expr)

        assert result["result"] == ["value"]

    def test_xpath_exists_executes(self, postgres_backend):
        """Execute xpath_exists and verify PostgreSQL reports a matching node."""
        xml_expr = core.Subquery(
            postgres_backend.dialect,
            "'<root><item>value</item></root>'::xml",
        )
        expr = xpath_exists(postgres_backend.dialect, "/root/item", xml_expr)
        result = _execute_expression(postgres_backend, expr)

        assert result["result"] is True

    def test_xml_is_well_formed_executes(self, postgres_backend):
        """Execute xml_is_well_formed and verify valid XML returns True."""
        expr = xml_is_well_formed(postgres_backend.dialect, "<root/>")
        result = _execute_expression(postgres_backend, expr)

        assert result["result"] is True


class TestAsyncXMLIntegration:
    """Asynchronous integration tests for PostgreSQL XML type."""

    @pytest_asyncio.fixture
    async def async_xml_test_table(self, async_postgres_backend):
        """Create a table containing an xml column for async tests."""
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_XML_TABLE}")
        await async_postgres_backend.execute(f"""
            CREATE TABLE {ASYNC_XML_TABLE} (
                id SERIAL PRIMARY KEY,
                document xml
            )
        """)
        yield ASYNC_XML_TABLE
        await async_postgres_backend.execute(f"DROP TABLE IF EXISTS {ASYNC_XML_TABLE}")

    @pytest.mark.asyncio
    async def test_async_xml_round_trip(
        self, async_postgres_backend, async_xml_test_table
    ):
        """Insert a PostgresXML document asynchronously and verify XML text."""
        literal = _xml_literal(PostgresXML("<root><item>async</item></root>"))

        await async_postgres_backend.execute(
            f"INSERT INTO {async_xml_test_table} (document) VALUES ({literal})"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT document::text AS document FROM {async_xml_test_table}"
        )

        assert result["document"] == "<root><item>async</item></root>"

    @pytest.mark.asyncio
    async def test_async_null_xml_round_trip(
        self, async_postgres_backend, async_xml_test_table
    ):
        """Insert a NULL XML value asynchronously and verify fetched value is None."""
        await async_postgres_backend.execute(
            f"INSERT INTO {async_xml_test_table} (document) VALUES (NULL)"
        )
        result = await async_postgres_backend.fetch_one(
            f"SELECT document FROM {async_xml_test_table}"
        )

        assert result["document"] is None

    @pytest.mark.asyncio
    async def test_async_xpath_exists_executes(self, async_postgres_backend):
        """Execute xpath_exists asynchronously and verify PostgreSQL returns True."""
        xml_expr = core.Subquery(
            async_postgres_backend.dialect,
            "'<root><item>async</item></root>'::xml",
        )
        expr = xpath_exists(async_postgres_backend.dialect, "/root/item", xml_expr)
        sql, params = expr.to_sql()
        result = await async_postgres_backend.fetch_one(f"SELECT {sql} AS result", params)

        assert result["result"] is True
