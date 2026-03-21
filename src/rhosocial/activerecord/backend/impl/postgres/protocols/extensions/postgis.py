# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/postgis.py
"""PostGIS extension protocol definition.

This module defines the protocol for PostGIS spatial database
functionality in PostgreSQL.
"""
from typing import Protocol, runtime_checkable


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

    Detection methods:
    - Automatic detection: introspect_and_adapt() queries pg_extension
    - Manual detection: SELECT * FROM pg_extension WHERE extname = 'postgis';
    - Programmatic detection: dialect.is_extension_installed('postgis')

    Notes:
    - PostGIS needs to be installed at database level
    - Installation requires superuser privileges
    - Features vary across versions
    """

    def supports_postgis_geometry_type(self) -> bool:
        """Whether PostGIS geometry type is supported.

        Requires PostGIS extension.
        geometry type is used for planar coordinate systems.
        """
        ...

    def supports_postgis_geography_type(self) -> bool:
        """Whether PostGIS geography type is supported.

        Requires PostGIS extension.
        geography type is used for spherical coordinate systems (lat/lon).
        """
        ...

    def supports_postgis_spatial_index(self) -> bool:
        """Whether PostGIS spatial indexing is supported.

        Requires PostGIS extension.
        PostgreSQL uses GiST index to support spatial queries.
        """
        ...

    def supports_postgis_spatial_functions(self) -> bool:
        """Whether PostGIS spatial analysis functions are supported.

        Requires PostGIS extension.
        Includes functions like: ST_Distance, ST_Within, ST_Contains, etc.
        """
        ...
