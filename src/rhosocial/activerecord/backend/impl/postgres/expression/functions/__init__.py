# rhosocial/activerecord/backend/impl/postgres/expression/functions/__init__.py
"""Expression namespace: functions (advisory locks and standard-server functions)."""

import importlib

# Public name -> defining module. Private names here stay reachable
# through this package but are not part of __all__.
_EXPORTS = {
    'BaseExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.introspection',
    'PostgresAdvisoryLockExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.advisory.lock',
    'PostgresAdvisoryUnlockAllExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.advisory.lock',
    'PostgresAdvisoryUnlockExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.advisory.lock',
    'PostgresDatabaseSizeFunction': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.introspection',
    'PostgresIndexesSizeFunction': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.introspection',
    'PostgresRelationSizeFunction': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.introspection',
    'PostgresTryAdvisoryLockExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.advisory.lock',
    'TYPE_CHECKING': 'rhosocial.activerecord.backend.impl.postgres.expression.functions.introspection',
}

__all__ = [
    'BaseExpression',
    'PostgresDatabaseSizeFunction',
    'PostgresIndexesSizeFunction',
    'PostgresRelationSizeFunction',
    'TYPE_CHECKING',
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
