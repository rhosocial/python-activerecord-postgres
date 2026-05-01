# src/rhosocial/activerecord/backend/impl/postgres/functions/xml.py
"""
PostgreSQL XML function factories.

This module provides SQL expression generators for PostgreSQL XML
functions. All functions return FunctionCall expression objects that
integrate with the expression-dialect architecture.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-xml.html

Supported functions:
- xmlparse() - Parse XML content
- xpath_query() - Execute XPath query on XML
- xpath_exists() - Test if XPath expression matches
- xml_is_well_formed() - Check if XML is well-formed

Examples:
    Parse XML document:
        >>> func = xmlparse(dialect, '<root><item>value</item></root>')
        >>> func.to_sql()
        ('XMLPARSE(DOCUMENT, %s, PRESERVE WHITESPACE)', ('<root><item>value</item></root>',))

    Query XML with XPath:
        >>> func = xpath_query(dialect, "/root/item", "xml_column")
        >>> func.to_sql()
        ('xpath(%s, xml_column)', ('/root/item',))

    Test if XPath exists:
        >>> func = xpath_exists(dialect, "/root/item", "xml_column")
        >>> func.to_sql()
        ('xpath_exists(%s, xml_column)', ('/root/item',))

    Check if well-formed:
        >>> func = xml_is_well_formed(dialect, '<root></root>')
        >>> func.to_sql()
        ('xml_is_well_formed(%s)', ('<root></root>',))
"""

from typing import Dict, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

# Import type for annotations
from ..types.xml import PostgresXML


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[PostgresXML, str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression."""
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, PostgresXML):
        return core.Literal(dialect, expr.content)
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


class _KeywordLiteral(bases.BaseExpression):
    """Represents a SQL keyword that should be rendered as-is (unquoted, unparameterized).

    Used for SQL keywords that appear inside function call arguments,
    such as DOCUMENT, CONTENT, PRESERVE WHITESPACE in XMLPARSE.
    """

    def __init__(self, dialect: "SQLDialectBase", keyword: str):
        super().__init__(dialect)
        self.keyword = keyword

    def to_sql(self) -> "bases.SQLQueryAndParams":
        return self.keyword, ()


def xmlparse(
    dialect: "SQLDialectBase",
    content: Union[PostgresXML, str, "bases.BaseExpression"],
    document: bool = True,
    preserve_whitespace: bool = False,
) -> core.FunctionCall:
    """Generate XMLPARSE expression.

    XMLPARSE parses XML content from a string. It produces either
    a DOCUMENT or CONTENT value, optionally preserving whitespace.

    Args:
        dialect: The SQL dialect instance
        content: XML content string, PostgresXML instance, or expression
        document: Parse as DOCUMENT (default) or CONTENT
        preserve_whitespace: Preserve whitespace (default False)

    Returns:
        FunctionCall: SQL expression for XMLPARSE

    Example:
        >>> func = xmlparse(dialect, '<root><item>value</item></root>')
        >>> func.to_sql()
        ('XMLPARSE(DOCUMENT, %s)', ('<root><item>value</item></root>',))

        >>> func = xmlparse(dialect, '<root/>', document=False, preserve_whitespace=True)
        >>> func.to_sql()
        ('XMLPARSE(CONTENT, %s, PRESERVE WHITESPACE)', ('<root/>',))
    """
    doc_type = "DOCUMENT" if document else "CONTENT"
    content_expr = _convert_to_expression(dialect, content)

    args = [_KeywordLiteral(dialect, doc_type), content_expr]

    if preserve_whitespace:
        args.append(_KeywordLiteral(dialect, "PRESERVE WHITESPACE"))

    return core.FunctionCall(dialect, "XMLPARSE", *args)


def xpath_query(
    dialect: "SQLDialectBase",
    xpath: Union[str, "bases.BaseExpression"],
    xml_value: Union[PostgresXML, str, "bases.BaseExpression"],
    namespaces: Optional[Dict[str, str]] = None,
) -> core.FunctionCall:
    """Generate XPath query expression.

    The xpath function evaluates the XPath expression against the XML value,
    optionally using namespace mappings.

    Args:
        dialect: The SQL dialect instance
        xpath: XPath expression string or expression
        xml_value: XML value, PostgresXML instance, or column expression
        namespaces: Optional namespace mappings (dict of prefix -> URI)

    Returns:
        FunctionCall: SQL expression for xpath function

    Example:
        >>> func = xpath_query(dialect, "/root/item", "xml_column")
        >>> func.to_sql()
        ('xpath(%s, xml_column)', ('/root/item',))
    """
    xpath_expr = _convert_to_expression(dialect, xpath)
    xml_expr = _convert_to_expression(dialect, xml_value)

    args = [xpath_expr, xml_expr]

    if namespaces:
        ns_parts = []
        for prefix, uri in namespaces.items():
            ns_parts.append(f"{prefix}={uri}")
        ns_literal = core.Literal(dialect, ",".join(ns_parts))
        args.append(ns_literal)

    return core.FunctionCall(dialect, "xpath", *args)


def xpath_exists(
    dialect: "SQLDialectBase",
    xpath: Union[str, "bases.BaseExpression"],
    xml_value: Union[PostgresXML, str, "bases.BaseExpression"],
    namespaces: Optional[Dict[str, str]] = None,
) -> core.FunctionCall:
    """Generate xpath_exists expression.

    The xpath_exists function checks whether the XPath expression
    matches any nodes in the XML value.

    Args:
        dialect: The SQL dialect instance
        xpath: XPath expression string or expression
        xml_value: XML value, PostgresXML instance, or column expression
        namespaces: Optional namespace mappings (dict of prefix -> URI)

    Returns:
        FunctionCall: SQL expression for xpath_exists function

    Example:
        >>> func = xpath_exists(dialect, "/root/item", "xml_column")
        >>> func.to_sql()
        ('xpath_exists(%s, xml_column)', ('/root/item',))
    """
    xpath_expr = _convert_to_expression(dialect, xpath)
    xml_expr = _convert_to_expression(dialect, xml_value)

    args = [xpath_expr, xml_expr]

    if namespaces:
        ns_parts = []
        for prefix, uri in namespaces.items():
            ns_parts.append(f"{prefix}={uri}")
        ns_literal = core.Literal(dialect, ",".join(ns_parts))
        args.append(ns_literal)

    return core.FunctionCall(dialect, "xpath_exists", *args)


def xml_is_well_formed(
    dialect: "SQLDialectBase",
    content: Union[PostgresXML, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate xml_is_well_formed expression (PostgreSQL 9.1+).

    Checks whether the input text is well-formed XML.

    Args:
        dialect: The SQL dialect instance
        content: XML content string, PostgresXML instance, or expression

    Returns:
        FunctionCall: SQL expression for xml_is_well_formed function

    Example:
        >>> func = xml_is_well_formed(dialect, '<root></root>')
        >>> func.to_sql()
        ('xml_is_well_formed(%s)', ('<root></root>',))
    """
    content_expr = _convert_to_expression(dialect, content)
    return core.FunctionCall(dialect, "xml_is_well_formed", content_expr)


__all__ = [
    "xmlparse",
    "xpath_query",
    "xpath_exists",
    "xml_is_well_formed",
]
