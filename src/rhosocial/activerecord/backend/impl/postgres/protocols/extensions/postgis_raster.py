# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/postgis_raster.py
"""PostGIS Raster extension protocol definition.

This module defines the protocol for PostGIS raster data type
functionality in PostgreSQL.

For SQL expression generation, use the function factories in
``functions/postgis_raster.py`` instead of the removed format_* methods.
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
