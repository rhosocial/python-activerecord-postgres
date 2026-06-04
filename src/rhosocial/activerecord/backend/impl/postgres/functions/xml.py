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

from ..types.xml import PostgresXML


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[PostgresXML, str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression."""
    if isinstance(expr, bases.BaseExpression):
        return expr
    if isinstance(expr, PostgresXML):
        return core.Literal(dialect, expr.content)
    return core.Literal(dialect, expr)


def _namespaces_to_expression(
    dialect: "SQLDialectBase",
    namespaces: Dict[str, str],
) -> "bases.BaseExpression":
    namespace_pairs = [[prefix, uri] for prefix, uri in namespaces.items()]
    return core.Literal(dialect, namespace_pairs)


def xpath_query(
    dialect: "SQLDialectBase",
    xpath: Union[str, "bases.BaseExpression"],
    xml_value: Union[PostgresXML, str, "bases.BaseExpression"],
    namespaces: Optional[Dict[str, str]] = None,
) -> core.FunctionCall:
    """Generate PostgreSQL xpath expression."""
    xpath_expr = _convert_to_expression(dialect, xpath)
    xml_expr = _convert_to_expression(dialect, xml_value)

    args = [xpath_expr, xml_expr]
    if namespaces:
        args.append(_namespaces_to_expression(dialect, namespaces))

    return core.FunctionCall(dialect, "xpath", *args)


def xpath_exists(
    dialect: "SQLDialectBase",
    xpath: Union[str, "bases.BaseExpression"],
    xml_value: Union[PostgresXML, str, "bases.BaseExpression"],
    namespaces: Optional[Dict[str, str]] = None,
) -> core.FunctionCall:
    """Generate PostgreSQL xpath_exists expression."""
    xpath_expr = _convert_to_expression(dialect, xpath)
    xml_expr = _convert_to_expression(dialect, xml_value)

    args = [xpath_expr, xml_expr]
    if namespaces:
        args.append(_namespaces_to_expression(dialect, namespaces))

    return core.FunctionCall(dialect, "xpath_exists", *args)


def xml_is_well_formed(
    dialect: "SQLDialectBase",
    content: Union[PostgresXML, str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Generate PostgreSQL xml_is_well_formed expression."""
    content_expr = _convert_to_expression(dialect, content)
    return core.FunctionCall(dialect, "xml_is_well_formed", content_expr)


__all__ = [
    "xpath_query",
    "xpath_exists",
    "xml_is_well_formed",
]
