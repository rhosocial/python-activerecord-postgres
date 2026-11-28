# rhosocial/activerecord/backend/impl/postgres/expression/value_types/__init__.py
"""Expression namespace: value_types."""

import importlib

# Public name -> defining module. Private names here stay reachable
# through this package but are not part of __all__.
_EXPORTS = {
    'AliasableMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'ArrayType': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'BigIntType': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'BinaryExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'Callable': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'CastExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'ComparisonMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'DataType': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'HstoreResultMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'IntegerType': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'LtreeResultMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'NetworkResultMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'Optional': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.wildcard',
    'Parameter': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'PostgresHstoreValue': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'PostgresLtreeValue': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'PostgresNetworkValue': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'PostgresRangeValue': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'PostgresRowCountWildcard': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.wildcard',
    'RangeBoundMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'RangeResultMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'RowCountWildcard': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.wildcard',
    'SQLValue': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'StringPatternPredicateMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'TYPE_CHECKING': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.wildcard',
    'TypeCastingMixin': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'TypeVar': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_ELEMENT_WIDTH_BY_NAME': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_F': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_OPERATION_EXPRESSIONS': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_ResultMixinBase': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_T': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_hstore': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_ltree': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_network': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_range': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    '_typed_element': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'operations_class_for': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
    'value_expression_for': 'rhosocial.activerecord.backend.impl.postgres.expression.value_types.values',
}

__all__ = [
    'AliasableMixin',
    'ArrayType',
    'BigIntType',
    'BinaryExpression',
    'Callable',
    'CastExpression',
    'ComparisonMixin',
    'DataType',
    'HstoreResultMixin',
    'IntegerType',
    'LtreeResultMixin',
    'NetworkResultMixin',
    'Optional',
    'Parameter',
    'PostgresHstoreValue',
    'PostgresLtreeValue',
    'PostgresNetworkValue',
    'PostgresRangeValue',
    'PostgresRowCountWildcard',
    'RangeBoundMixin',
    'RangeResultMixin',
    'RowCountWildcard',
    'SQLValue',
    'StringPatternPredicateMixin',
    'TYPE_CHECKING',
    'TypeCastingMixin',
    'TypeVar',
    'operations_class_for',
    'value_expression_for',
]


def __getattr__(name):
    """Resolve a public name from this namespace lazily (no import cycles)."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = importlib.import_module(module_name)
    value = vars(module)[name]
    globals()[name] = value
    return value
