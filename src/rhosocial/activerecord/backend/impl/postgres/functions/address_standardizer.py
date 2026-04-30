# src/rhosocial/activerecord/backend/impl/postgres/functions/address_standardizer.py
"""
PostgreSQL address_standardizer Extension Functions.

This module provides SQL expression generators for PostgreSQL address_standardizer
extension functions. All functions return Expression objects (FunctionCall)
that integrate with the expression-dialect architecture.

The address_standardizer extension (part of PostGIS) provides functions for
standardizing and parsing postal addresses into their component parts.

PostgreSQL Documentation: https://postgis.net/docs/Address_Standardizer.html

The address_standardizer extension must be installed:
    CREATE EXTENSION IF NOT EXISTS address_standardizer;

Supported functions:
- parse_address: Parse an address string into its component parts
- standardize_address: Standardize an address using lookup tables

The actual PostgreSQL function signatures are:
- parse_address(text) — returns a normalized_address record
- standardize_address(lextab text, gaztab text, rultab text, address text)
- standardize_address(lextab text, gaztab text, rultab text,
                       micro text, macro text)

Note: standardize_address requires lookup table names (lextab, gaztab, rultab)
which reference the address standardizer rule tables. The most common lookup
tables are 'pagc_lex' (lexer), 'pagc_gaz' (gazetteer), and 'pagc_rules' (rules)
which are installed with the extension.

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, etc.)
"""

from typing import Union, TYPE_CHECKING

from rhosocial.activerecord.backend.expression import bases, core

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _convert_to_expression(
    dialect: "SQLDialectBase",
    expr: Union[str, "bases.BaseExpression"],
) -> "bases.BaseExpression":
    """Convert an input value to an appropriate BaseExpression.

    Supports strings and existing BaseExpression objects.

    Args:
        dialect: The SQL dialect instance
        expr: Value to convert

    Returns:
        BaseExpression representing the value
    """
    if isinstance(expr, bases.BaseExpression):
        return expr
    elif isinstance(expr, str):
        return core.Literal(dialect, expr)
    else:
        return core.Literal(dialect, expr)


# ============== Address Functions ==============

def parse_address(
    dialect: "SQLDialectBase",
    address: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Parse an address string into its component parts.

    Takes a single-line address string and returns a normalized_address
    record containing the individual components of the address (e.g.,
    building number, street name, city, state, zip).

    The actual PostgreSQL function signature is:
        parse_address(text)

    Args:
        dialect: The SQL dialect instance
        address: The address string to parse

    Returns:
        FunctionCall for parse_address(address)

    Example:
        >>> parse_address(dialect, '123 Main St, Springfield, IL 62701')
        >>> parse_address(dialect, Literal(dialect, '456 Oak Avenue'))
    """
    return core.FunctionCall(
        dialect, "parse_address",
        _convert_to_expression(dialect, address),
    )


def standardize_address(
    dialect: "SQLDialectBase",
    lextab: Union[str, "bases.BaseExpression"],
    gaztab: Union[str, "bases.BaseExpression"],
    rultab: Union[str, "bases.BaseExpression"],
    address: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Standardize an address string using lookup tables.

    Takes a single-line address string and returns a standardized
    (normalized) version using the specified lookup tables for lexing,
    gazetteering, and rule application.

    The actual PostgreSQL function signature is:
        standardize_address(lextab text, gaztab text, rultab text, address text)

    Common lookup tables installed with the extension:
    - 'pagc_lex': Lexer table
    - 'pagc_gaz': Gazetteer table
    - 'pagc_rules': Rules table

    Args:
        dialect: The SQL dialect instance
        lextab: Name of the lexer lookup table (e.g., 'pagc_lex')
        gaztab: Name of the gazetteer lookup table (e.g., 'pagc_gaz')
        rultab: Name of the rules lookup table (e.g., 'pagc_rules')
        address: The address string to standardize

    Returns:
        FunctionCall for standardize_address(lextab, gaztab, rultab, address)

    Example:
        >>> standardize_address(dialect, 'pagc_lex', 'pagc_gaz', 'pagc_rules',
        ...                      '123 Main St, Springfield, IL 62701')
    """
    return core.FunctionCall(
        dialect, "standardize_address",
        _convert_to_expression(dialect, lextab),
        _convert_to_expression(dialect, gaztab),
        _convert_to_expression(dialect, rultab),
        _convert_to_expression(dialect, address),
    )


__all__ = [
    # Address functions
    "parse_address",
    "standardize_address",
]
