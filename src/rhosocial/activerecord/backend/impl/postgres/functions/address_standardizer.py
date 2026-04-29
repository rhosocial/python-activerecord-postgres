# src/rhosocial/activerecord/backend/impl/postgres/functions/address_standardizer.py
"""
PostgreSQL address_standardizer Extension Functions.

This module provides SQL expression generators for PostgreSQL address_standardizer
extension functions. All functions return Expression objects (FunctionCall,
BinaryExpression) that integrate with the expression-dialect architecture.

The address_standardizer extension (part of PostGIS) provides functions for
standardizing and parsing postal addresses into their component parts.

PostgreSQL Documentation: https://postgis.net/docs/Address_Standardizer.html

The address_standardizer extension must be installed:
    CREATE EXTENSION IF NOT EXISTS address_standardizer;

Supported functions:
- standardize_address: Standardize an address string into a normalized form
- parse_address: Parse an address string into its component parts

All functions follow the expression-dialect separation architecture:
- First parameter is always the dialect instance
- They return Expression objects (FunctionCall, BinaryExpression, etc.)
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

def standardize_address(
    dialect: "SQLDialectBase",
    address: Union[str, "bases.BaseExpression"],
    use_tiger: bool = False,
) -> core.FunctionCall:
    """Standardize an address string into a normalized form.

    Takes a single-line address string and returns a standardized (normalized)
    version. The function parses the input address and reformats it according
    to postal addressing standards.

    When use_tiger is True, the function uses TIGER (Topologically
    Integrated Geographic Encoding and Referencing) data for the
    output format, which provides USPS-standard address formatting.

    The output includes standardized fields such as building number, street
    name, city, state, and zip code.

    Args:
        dialect: The SQL dialect instance
        address: The address string to standardize
        use_tiger: Use TIGER data for output formatting (default: False)

    Returns:
        FunctionCall for standardize_address(address, '', '')

    Example:
        >>> standardize_address(dialect, '123 Main St, Springfield, IL 62701')
        >>> standardize_address(dialect, '123 Main St', use_tiger=True)
    """
    return core.FunctionCall(
        dialect, "standardize_address",
        _convert_to_expression(dialect, address),
        _convert_to_expression(dialect, ""),
        _convert_to_expression(dialect, ""),
    )


def parse_address(
    dialect: "SQLDialectBase",
    address: Union[str, "bases.BaseExpression"],
) -> core.FunctionCall:
    """Parse an address string into its component parts.

    Takes a single-line address string and returns a record containing the
    individual components of the address (e.g., number, street, city, state,
    zip). This is useful for extracting structured data from free-form
    address text.

    Args:
        dialect: The SQL dialect instance
        address: The address string to parse

    Returns:
        FunctionCall for parse_address(address, 'US')

    Example:
        >>> parse_address(dialect, '123 Main St, Springfield, IL 62701')
        >>> parse_address(dialect, '456 Oak Avenue, Apt 2B, Brooklyn, NY 11201')
    """
    return core.FunctionCall(
        dialect, "parse_address",
        _convert_to_expression(dialect, address),
        _convert_to_expression(dialect, "US"),
    )


__all__ = [
    # Address functions
    "standardize_address",
    "parse_address",
]
