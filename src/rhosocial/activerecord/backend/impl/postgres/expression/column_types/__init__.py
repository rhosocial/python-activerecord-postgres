# rhosocial/activerecord/backend/impl/postgres/expression/column_types/__init__.py
"""Expression namespace: column_types."""

import importlib

# Public name -> defining module. Private names here stay reachable
# through this package but are not part of __all__.
_EXPORTS = {
    'BitStringColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'ColumnBase': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'GeometryColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'IntegerColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'MacAddressColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'MultirangeColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'NumericColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresAreaGeometryColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresBitStringColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresGeometryColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresHstoreColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresIdentifierColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresJsonPathColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresLsnColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresMacAddressColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresMoneyColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresMultirangeColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresPostgisGeometryColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresRangeColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresTextSearchColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresVectorColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'PostgresXmlColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'RangeColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'TYPE_CHECKING': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'UnsupportedFeatureError': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'VectorColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    'XMLColumn': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    '_CAPABILITY_SWITCHES': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
    '_require': 'rhosocial.activerecord.backend.impl.postgres.expression.column_types.column_types',
}

__all__ = [
    'BitStringColumn',
    'ColumnBase',
    'GeometryColumn',
    'IntegerColumn',
    'MacAddressColumn',
    'MultirangeColumn',
    'NumericColumn',
    'PostgresAreaGeometryColumn',
    'PostgresBitStringColumn',
    'PostgresGeometryColumn',
    'PostgresHstoreColumn',
    'PostgresIdentifierColumn',
    'PostgresJsonPathColumn',
    'PostgresLsnColumn',
    'PostgresMacAddressColumn',
    'PostgresMoneyColumn',
    'PostgresMultirangeColumn',
    'PostgresPostgisGeometryColumn',
    'PostgresRangeColumn',
    'PostgresTextSearchColumn',
    'PostgresVectorColumn',
    'PostgresXmlColumn',
    'RangeColumn',
    'TYPE_CHECKING',
    'UnsupportedFeatureError',
    'VectorColumn',
    'XMLColumn',
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
