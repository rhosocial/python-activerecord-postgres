# src/rhosocial/activerecord/backend/impl/postgres/expression/functions/__init__.py
"""Expression namespace: functions (advisory locks and standard-server functions)."""

import importlib

_MODULES = ('advisory', 'introspection')


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
