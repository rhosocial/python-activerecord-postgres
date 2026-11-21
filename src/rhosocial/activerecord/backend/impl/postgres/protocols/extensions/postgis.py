# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/postgis.py
"""PostGIS extension protocol — capability probes only.

Index DDL is an expression: ``functions.postgis.spatial_index`` returns a
``PostgresCreateIndexExpression`` rendered by ``format_create_index_statement``.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPostGISSupport(Protocol):
    """PostGIS spatial functionality protocol."""

    def supports_postgis_geometry_type(self) -> bool: ...

    def supports_postgis_geography_type(self) -> bool: ...

    def supports_postgis_spatial_index(self) -> bool: ...

    def supports_postgis_spatial_functions(self) -> bool: ...

    def format_postgres_spatial_index_expression(self, expr) -> tuple: ...
