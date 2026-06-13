# src/rhosocial/activerecord/backend/impl/postgres/protocols/cte.py
"""PostgreSQL cte feature support protocol."""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresCTESupport(Protocol):
    """PostgreSQL cte feature support protocol."""

    def supports_basic_cte(self)-> bool: ...

    def supports_recursive_cte(self)-> bool: ...

    def supports_materialized_cte(self)-> bool: ...
