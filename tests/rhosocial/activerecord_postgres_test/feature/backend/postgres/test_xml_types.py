# tests/rhosocial/activerecord_postgres_test/feature/backend/postgres/test_xml_types.py
"""Unit tests for PostgreSQL XML type.

Tests for:
- PostgresXML data class
- PostgresXMLAdapter conversion
- XML utility functions
"""
import pytest

from rhosocial.activerecord.backend.impl.postgres.types.xml import (
    PostgresXML,
    xmlparse,
    xpath_query,
    xpath_exists,
    xml_is_well_formed,
)
from rhosocial.activerecord.backend.impl.postgres.adapters.xml import PostgresXMLAdapter


class TestPostgresXML:
    """Tests for PostgresXML data class."""

    def test_create_xml(self):
        """Test creating XML."""
        xml = PostgresXML('<root><item>value</item></root>')
        assert xml.content == '<root><item>value</item></root>'

    def test_empty_xml_raises_error(self):
        """Test that empty content raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            PostgresXML('')

    def test_string_representation(self):
        """Test string representation."""
        xml = PostgresXML('<root/>')
        assert str(xml) == '<root/>'

    def test_repr(self):
        """Test repr."""
        xml = PostgresXML('<root/>')
        assert '<root/>' in repr(xml)

    def test_equality_xml(self):
        """Test equality with PostgresXML."""
        xml1 = PostgresXML('<root/>')
        xml2 = PostgresXML('<root/>')
        xml3 = PostgresXML('<other/>')
        assert xml1 == xml2
        assert xml1 != xml3

    def test_equality_string(self):
        """Test equality with string."""
        xml = PostgresXML('<root/>')
        assert xml == '<root/>'
        assert xml != '<other/>'

    def test_hash(self):
        """Test hashability."""
        xml1 = PostgresXML('<root/>')
        xml2 = PostgresXML('<root/>')
        assert hash(xml1) == hash(xml2)
        assert len({xml1, xml2}) == 1

    def test_is_well_formed_valid(self):
        """Test is_well_formed with valid XML."""
        xml = PostgresXML('<root><item>value</item></root>')
        assert xml.is_well_formed() is True

    def test_is_well_formed_invalid(self):
        """Test is_well_formed with invalid content."""
        xml = PostgresXML('not xml')
        assert xml.is_well_formed() is False


class TestPostgresXMLAdapter:
    """Tests for PostgresXMLAdapter."""

    def test_adapter_supported_types(self):
        """Test supported types property."""
        adapter = PostgresXMLAdapter()
        supported = adapter.supported_types
        assert PostgresXML in supported

    def test_to_database_xml(self):
        """Test converting PostgresXML to database."""
        adapter = PostgresXMLAdapter()
        xml = PostgresXML('<root/>')
        result = adapter.to_database(xml, str)
        assert result == '<root/>'

    def test_to_database_string(self):
        """Test converting string to database."""
        adapter = PostgresXMLAdapter()
        result = adapter.to_database('<root/>', str)
        assert result == '<root/>'

    def test_to_database_none(self):
        """Test converting None to database."""
        adapter = PostgresXMLAdapter()
        result = adapter.to_database(None, str)
        assert result is None

    def test_to_database_invalid_type(self):
        """Test converting invalid type raises error."""
        adapter = PostgresXMLAdapter()
        with pytest.raises(TypeError):
            adapter.to_database(123, str)

    def test_from_database_string(self):
        """Test converting string from database."""
        adapter = PostgresXMLAdapter()
        result = adapter.from_database('<root/>', PostgresXML)
        assert isinstance(result, PostgresXML)
        assert result.content == '<root/>'

    def test_from_database_xml(self):
        """Test converting PostgresXML from database."""
        adapter = PostgresXMLAdapter()
        xml = PostgresXML('<root/>')
        result = adapter.from_database(xml, PostgresXML)
        assert result is xml

    def test_from_database_none(self):
        """Test converting None from database."""
        adapter = PostgresXMLAdapter()
        result = adapter.from_database(None, PostgresXML)
        assert result is None

    def test_from_database_invalid_type(self):
        """Test converting invalid type from database."""
        adapter = PostgresXMLAdapter()
        with pytest.raises(TypeError):
            adapter.from_database(123, PostgresXML)

    def test_batch_to_database(self):
        """Test batch conversion to database."""
        adapter = PostgresXMLAdapter()
        values = [
            PostgresXML('<root1/>'),
            '<root2/>',
            None
        ]
        results = adapter.to_database_batch(values, str)
        assert results[0] == '<root1/>'
        assert results[1] == '<root2/>'
        assert results[2] is None

    def test_batch_from_database(self):
        """Test batch conversion from database."""
        adapter = PostgresXMLAdapter()
        values = ['<root1/>', '<root2/>', None]
        results = adapter.from_database_batch(values, PostgresXML)
        assert results[0].content == '<root1/>'
        assert results[1].content == '<root2/>'
        assert results[2] is None


class TestXMLUtilityFunctions:
    """Tests for XML utility functions."""

    def test_xmlparse_document(self):
        """Test xmlparse with DOCUMENT."""
        result = xmlparse('<root/>', document=True)
        assert 'XMLPARSE' in result
        assert 'DOCUMENT' in result
        assert '<root/>' in result

    def test_xmlparse_content(self):
        """Test xmlparse with CONTENT."""
        result = xmlparse('<root/>', document=False)
        assert 'CONTENT' in result

    def test_xmlparse_preserve_whitespace(self):
        """Test xmlparse with preserve whitespace."""
        result = xmlparse('<root/>', preserve_whitespace=True)
        assert 'PRESERVE WHITESPACE' in result

    def test_xpath_query_basic(self):
        """Test xpath_query basic."""
        xml = PostgresXML('<root><item>value</item></root>')
        result = xpath_query(xml, '/root/item')
        assert "xpath('/root/item'" in result

    def test_xpath_query_with_column(self):
        """Test xpath_query with column reference."""
        result = xpath_query('xml_column', '/root/item')
        assert 'xml_column' in result

    def test_xpath_query_with_namespaces(self):
        """Test xpath_query with namespaces."""
        xml = PostgresXML('<ns:root xmlns:ns="http://example.com"/>')
        result = xpath_query(xml, '/ns:root', namespaces={'ns': 'http://example.com'})
        assert 'ARRAY' in result

    def test_xpath_exists_basic(self):
        """Test xpath_exists basic."""
        xml = PostgresXML('<root><item>value</item></root>')
        result = xpath_exists(xml, '/root/item')
        assert 'xpath_exists' in result
        assert '/root/item' in result

    def test_xpath_exists_with_namespaces(self):
        """Test xpath_exists with namespaces."""
        xml = PostgresXML('<ns:root xmlns:ns="http://example.com"/>')
        result = xpath_exists(xml, '/ns:root', namespaces={'ns': 'http://example.com'})
        assert 'xpath_exists' in result

    def test_xml_is_well_formed(self):
        """Test xml_is_well_formed function."""
        result = xml_is_well_formed('<root/>')
        assert "xml_is_well_formed('<root/>')" == result
