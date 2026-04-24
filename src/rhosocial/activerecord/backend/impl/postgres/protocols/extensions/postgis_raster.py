# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/postgis_raster.py
"""PostGIS Raster extension protocol definition.

This module defines the protocol for PostGIS raster data type
functionality in PostgreSQL.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class PostgresPostgisRasterSupport(Protocol):
    """PostGIS raster functionality protocol.

    Feature Source: Extension support (requires postgis_raster extension)

    PostGIS Raster provides raster data type and processing functionality:
    - raster data type for storing and processing geospatial raster data
    - ST_Value for pixel value extraction
    - ST_Summary for raster metadata inspection
    - ST_RastFromHexWKB for raster literal construction

    Extension Information:
    - Extension name: postgis_raster
    - Install command: CREATE EXTENSION postgis_raster;
    - Minimum version: 3.0
    - Dependencies: postgis
    - Website: https://postgis.net/
    - Documentation: https://postgis.net/docs/RT_reference.html

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'postgis_raster';
    - Programmatic detection: dialect.is_extension_installed('postgis_raster')

    Notes:
    - Requires PostGIS extension as a dependency
    - Raster support was separated into its own extension in PostGIS 3.0
    - Installation requires superuser privileges
    """

    def supports_postgis_raster_type(self) -> bool:
        """Whether PostGIS raster type is supported.

        Requires postgis_raster extension.
        raster type is used for storing and processing geospatial raster data.
        """
        ...

    def format_raster_literal(self, raster_data: str, srid: int = 4326) -> str:
        """Format a raster literal from HexWKB data.

        Args:
            raster_data: HexWKB representation of raster data
            srid: Spatial reference system ID (default: 4326)

        Returns:
            SQL raster literal string
        """
        ...

    def format_st_value(self, raster_expr: str, x: int, y: int) -> str:
        """Format ST_Value function for pixel value extraction.

        Args:
            raster_expr: Raster expression
            x: Column number (1-based)
            y: Row number (1-based)

        Returns:
            SQL function call string
        """
        ...

    def format_st_summary(self, raster_expr: str) -> str:
        """Format ST_Summary function for raster metadata.

        Args:
            raster_expr: Raster expression

        Returns:
            SQL function call string
        """
        ...
