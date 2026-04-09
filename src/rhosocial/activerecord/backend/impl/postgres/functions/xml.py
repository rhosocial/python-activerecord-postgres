# src/rhosocial/activerecord/backend/impl/postgres/functions/xml.py
"""
PostgreSQL XML functions for SQL expression generation.

This module provides utility functions for generating PostgreSQL XML-related SQL expressions.

Supported functions:
- xmlparse() - Parse XML content
- xpath_query() - Execute XPath query on XML
- xpath_exists() - Test if XPath expression matches
- xml_is_well_formed() - Check if XML is well-formed

Examples:
    Parse XML document:
        >>> xmlparse('<root><item>value</item></root>')
        "XMLPARSE(DOCUMENT '<root><item>value</item></root>')"

    Query XML with XPath:
        >>> xpath_query("xml_column", "/root/item")
        "xpath('/root/item', xml_column)"

    Test if XPath exists:
        >>> xpath_exists("xml_column", "/root/item[@id='1']")
        "xpath_exists('/root/item[@id='1']', xml_column)"

    Check if well-formed:
        >>> xml_is_well_formed('<root></root>')
        "xml_is_well_formed('<root></root>')"
"""

from typing import Dict, Optional, Union

# Import type for annotations
from ..types.xml import PostgresXML


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


__all__ = [
    "xmlparse",
    "xpath_query",
    "xpath_exists",
    "xml_is_well_formed",
]
