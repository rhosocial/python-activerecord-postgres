# src/rhosocial/activerecord/backend/impl/postgres/functions/xml.py
"""
PostgreSQL XML function factories.

This module provides SQL expression generators for PostgreSQL-specific XML
functions. SQL/XML expression constructors such as XMLPARSE live in
rhosocial.activerecord.backend.expression.functions.xml.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/functions-xml.html

Supported functions:
- xpath_query() - Execute XPath query on XML
- xpath_exists() - Test if XPath expression matches
- xml_is_well_formed() - Check if XML is well-formed
"""

from typing import Dict, Optional, Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase

from ..type_values.xml import PostgresXML


def _namespaces_to_expression(
    dialect: "SQLDialectBase",
    namespaces: Dict[str, str],
) -> bases.BaseExpression:
    namespace_pairs = [[prefix, uri] for prefix, uri in namespaces.items()]
    return core.Literal(dialect, namespace_pairs)


def xpath_query(
    dialect: "SQLDialectBase",
    xpath: str,
    xml_value: Union[PostgresXML, str, "bases.BaseExpression"],
    namespaces: Optional[Dict[str, str]] = None,
) -> core.FunctionCall:
    """Generate PostgreSQL xpath expression."""
    xpath_expr = (
        xpath if isinstance(xpath, bases.BaseExpression)
        else core.Literal(dialect, xpath.content) if isinstance(xpath, PostgresXML)
        else core.Literal(dialect, xpath)
    )
    xml_expr = (
        xml_value if isinstance(xml_value, bases.BaseExpression)
        else core.Literal(dialect, xml_value.content) if isinstance(xml_value, PostgresXML)
        else core.Literal(dialect, xml_value)
    )

    args = [xpath_expr, xml_expr]
    if namespaces:
        args.append(_namespaces_to_expression(dialect, namespaces))

    return core.FunctionCall(dialect, "xpath", *args)


def xpath_exists(
    dialect: "SQLDialectBase",
    xpath: str,
    xml_value: Union[PostgresXML, str, "bases.BaseExpression"],
    namespaces: Optional[Dict[str, str]] = None,
) -> core.FunctionCall:
    """Generate PostgreSQL xpath_exists expression."""
    xpath_expr = (
        xpath if isinstance(xpath, bases.BaseExpression)
        else core.Literal(dialect, xpath.content) if isinstance(xpath, PostgresXML)
        else core.Literal(dialect, xpath)
    )
    xml_expr = (
        xml_value if isinstance(xml_value, bases.BaseExpression)
        else core.Literal(dialect, xml_value.content) if isinstance(xml_value, PostgresXML)
        else core.Literal(dialect, xml_value)
    )

    args = [xpath_expr, xml_expr]
    if namespaces:
        args.append(_namespaces_to_expression(dialect, namespaces))

    return core.FunctionCall(dialect, "xpath_exists", *args)


def xml_is_well_formed(
    dialect: "SQLDialectBase",
    content: Union[PostgresXML, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate PostgreSQL xml_is_well_formed expression."""
    content_expr = (
        content if isinstance(content, bases.BaseExpression)
        else core.Literal(dialect, content.content) if isinstance(content, PostgresXML)
        else core.Literal(dialect, content)
    )
    return core.FunctionCall(dialect, "xml_is_well_formed", content_expr)


__all__ = [
    "xpath_query",
    "xpath_exists",
    "xml_is_well_formed",
]
