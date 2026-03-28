# src/rhosocial/activerecord/backend/impl/postgres/types/xml.py
"""
PostgreSQL XML type representation.

This module provides PostgresXML for representing PostgreSQL
XML values in Python.

The XML type stores well-formed XML documents.
PostgreSQL validates XML content on input and provides XML
functions and XPath support.

Available since early PostgreSQL versions.

For type adapter (conversion between Python and database),
see adapters.xml.PostgresXMLAdapter.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Union


@dataclass
class PostgresXML:
    """PostgreSQL XML type representation.

    The XML type stores well-formed XML documents.
    PostgreSQL validates XML content on input.

    Attributes:
        content: XML content as string

    Examples:
        PostgresXML('<root><item>value</item></root>')
    """

    content: str

    def __post_init__(self):
        """Validate that content is not empty."""
        if not self.content:
            raise ValueError("XML content cannot be empty")

    def __str__(self) -> str:
        """Return XML content."""
        return self.content

    def __repr__(self) -> str:
        return f"PostgresXML({self.content!r})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, PostgresXML):
            return self.content == other.content
        if isinstance(other, str):
            return self.content == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.content)

    def is_well_formed(self) -> bool:
        """Check if XML is well-formed (basic check).

        Note: This is a basic heuristic check. PostgreSQL
        does full validation on insert.

        Returns:
            True if appears to be well-formed
        """
        # Basic check for matching tags
        content = self.content.strip()
        if not content.startswith("<") or not content.endswith(">"):
            return False
        return True


# XML utility functions for use with PostgreSQL


def xmlparse(content: str, document: bool = True, preserve_whitespace: bool = False) -> str:
    """Generate XMLPARSE expression.

    Args:
        content: XML content string
        document: Parse as DOCUMENT (default) or CONTENT
        preserve_whitespace: Preserve whitespace

    Returns:
        SQL expression for XMLPARSE
    """
    doc_type = "DOCUMENT" if document else "CONTENT"
    whitespace = "PRESERVE WHITESPACE" if preserve_whitespace else ""
    return f"XMLPARSE({doc_type} '{content}' {whitespace})".strip()


def xpath_query(xml_value: Union[str, PostgresXML], xpath: str, namespaces: Optional[Dict[str, str]] = None) -> str:
    """Generate XPath query expression.

    Args:
        xml_value: XML value or column reference
        xpath: XPath expression
        namespaces: Optional namespace mappings

    Returns:
        SQL expression for xpath function
    """
    if isinstance(xml_value, PostgresXML):
        xml_value = f"'{xml_value.content}'"

    if namespaces:
        ns_array = ", ".join(f"ARRAY[{k!s}, {v!s}]" for k, v in namespaces.items())
        ns_param = f", ARRAY[{ns_array}]"
    else:
        ns_param = ""

    return f"xpath('{xpath}', {xml_value}{ns_param})"


def xpath_exists(xml_value: Union[str, PostgresXML], xpath: str, namespaces: Optional[Dict[str, str]] = None) -> str:
    """Generate xpath_exists expression.

    Args:
        xml_value: XML value or column reference
        xpath: XPath expression
        namespaces: Optional namespace mappings

    Returns:
        SQL expression for xpath_exists function
    """
    if isinstance(xml_value, PostgresXML):
        xml_value = f"'{xml_value.content}'"

    if namespaces:
        ns_array = ", ".join(f"ARRAY[{k!s}, {v!s}]" for k, v in namespaces.items())
        ns_param = f", ARRAY[{ns_array}]"
    else:
        ns_param = ""

    return f"xpath_exists('{xpath}', {xml_value}{ns_param})"


def xml_is_well_formed(content: str) -> str:
    """Generate xml_is_well_formed expression (PostgreSQL 9.1+).

    Args:
        content: XML content to check

    Returns:
        SQL expression for xml_is_well_formed function
    """
    return f"xml_is_well_formed('{content}')"


__all__ = ["PostgresXML", "xmlparse", "xpath_query", "xpath_exists", "xml_is_well_formed"]
