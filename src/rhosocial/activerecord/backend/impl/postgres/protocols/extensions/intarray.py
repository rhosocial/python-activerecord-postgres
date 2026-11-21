# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/intarray.py
"""intarray extension protocol — capability probes only.

Index DDL is an expression: ``functions.intarray.intarray_index`` returns a
``PostgresCreateIndexExpression``.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresIntarraySupport(Protocol):
    """intarray integer array protocol."""

    def supports_intarray_operators(self) -> bool: ...

    def supports_intarray_functions(self) -> bool: ...

    def supports_intarray_index(self) -> bool: ...
