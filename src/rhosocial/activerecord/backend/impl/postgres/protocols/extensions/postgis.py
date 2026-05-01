# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/postgis.py
"""PostGIS extension protocol definition.

This module defines the protocol for PostGIS spatial database
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/postgis.py`` instead of the removed format_* methods.
For DDL index creation, use ``format_spatial_index_statement``.
"""

from typing import Optional, Protocol, Tuple, runtime_checkable


@runtime_checkable
class PostgresPostGISSupport(Protocol):
    """PostGIS spatial functionality protocol.

    Feature Source: Extension support (requires PostGIS extension)

    PostGIS provides complete spatial database functionality:
    - geometry and geography data types
    - Spatial indexes (GiST)
    - Spatial analysis functions
    - Coordinate system transformations

    Extension Information:
    - Extension name: postgis
    - Install command: CREATE EXTENSION postgis;
    - Minimum version: 2.0
    - Recommended version: 3.0+
    - Website: https://postgis.net/
    - Documentation: https://postgis.net/docs/
    """

    def supports_postgis_geometry_type(self) -> bool:
        """Whether PostGIS geometry type is supported."""
        ...

    def supports_postgis_geography_type(self) -> bool:
        """Whether PostGIS geography type is supported."""
        ...

    def supports_postgis_spatial_index(self) -> bool:
        """Whether PostGIS spatial indexing is supported."""
        ...

    def supports_postgis_spatial_functions(self) -> bool:
        """Whether PostGIS spatial analysis functions are supported."""
        ...

    def format_spatial_index_statement(
        self, index_name: str, table_name: str, column_name: str, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for spatial column."""
        ...
