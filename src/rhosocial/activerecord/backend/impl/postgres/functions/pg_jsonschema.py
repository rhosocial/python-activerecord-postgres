# src/rhosocial/activerecord/backend/impl/postgres/functions/pg_jsonschema.py
"""pg_jsonschema validation function factories.

Every factory takes the dialect first and returns an Expression node. Parameter
schemas and instances are cast to ``json`` / ``jsonb`` so PostgreSQL resolves
the extension's function signatures without ambiguity.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.functions import CastExpression
from rhosocial.activerecord.backend.expression.functions import FunctionCall
from rhosocial.activerecord.backend.expression.value_types import Parameter
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.types import JsonBType, JsonType

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.dialect import SQLDialectBase


def _typed(dialect: "SQLDialectBase", value, data_type):
    """Expressions pass through; other values become typed literals."""
    if isinstance(value, BaseExpression):
        return value
    return CastExpression(dialect, Parameter(dialect, value), data_type(dialect))


def json_matches_schema(dialect: "SQLDialectBase", schema, instance) -> FunctionCall:
    """``json_matches_schema(schema json, instance json)``."""
    return FunctionCall(
        dialect, "json_matches_schema",
        _typed(dialect, schema, JsonType), _typed(dialect, instance, JsonType),
    )


def jsonb_matches_schema(dialect: "SQLDialectBase", schema, instance) -> FunctionCall:
    """``jsonb_matches_schema(schema json, instance jsonb)``."""
    return FunctionCall(
        dialect, "jsonb_matches_schema",
        _typed(dialect, schema, JsonType), _typed(dialect, instance, JsonBType),
    )


def jsonschema_is_valid(dialect: "SQLDialectBase", schema) -> FunctionCall:
    """``jsonschema_is_valid(schema json)``."""
    return FunctionCall(dialect, "jsonschema_is_valid", _typed(dialect, schema, JsonType))


def jsonschema_validation_errors(dialect: "SQLDialectBase", schema, instance) -> FunctionCall:
    """``jsonschema_validation_errors(schema json, instance json)``."""
    return FunctionCall(
        dialect, "jsonschema_validation_errors",
        _typed(dialect, schema, JsonType), _typed(dialect, instance, JsonType),
    )


__all__ = [
    "json_matches_schema",
    "jsonb_matches_schema",
    "jsonschema_is_valid",
    "jsonschema_validation_errors",
]
