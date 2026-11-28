# src/rhosocial/activerecord/backend/impl/postgres/expression/functions/advisory/__init__.py
"""
Advisory lock expression classes.

This module provides expression classes for database advisory locks,
which are application-level locks managed by the database server.
"""

import importlib

_MODULES = ('lock',)


def __getattr__(name):
    """Resolve a public name from this namespace lazily (no import cycles)."""
    for module_name in _MODULES:
        module = importlib.import_module(f'.{module_name}', __name__)
        namespace = vars(module)
        if name in namespace:
            return namespace[name]
        nested = namespace.get('__getattr__')
        if nested is not None:
            try:
                return nested(name)
            except AttributeError:
                continue
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "PostgresAdvisoryLockExpression",
    "PostgresAdvisoryUnlockExpression",
    "PostgresAdvisoryUnlockAllExpression",
    "PostgresTryAdvisoryLockExpression",
    "AdvisoryLockType",
]
