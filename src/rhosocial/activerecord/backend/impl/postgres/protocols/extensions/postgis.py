# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/postgis.py
"""PostGIS extension protocol definition.

This module defines the protocol for PostGIS spatial database
functionality in PostgreSQL.
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

    def format_geometry_literal(self, wkt: str, srid: Optional[int] = None, geometry_type: str = "geometry") -> str:
        """Format a geometry literal from WKT (Well-Known Text)."""
        ...

    def format_spatial_function(self, function_name: str, *args: str, **kwargs) -> str:
        """Format a spatial function call."""
        ...

    def format_spatial_index_statement(
        self, index_name: str, table_name: str, column_name: str, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for spatial column."""
        ...

    def format_st_distance(self, geom1: str, geom2: str, use_spheroid: bool = False) -> str:
        """Format ST_Distance function."""
        ...

    def format_st_dwithin(self, geom1: str, geom2: str, distance: float, use_spheroid: bool = False) -> str:
        """Format ST_DWithin function for proximity queries."""
        ...

    def format_st_contains(self, outer: str, inner: str) -> str:
        """Format ST_Contains function."""
        ...

    def format_st_intersects(self, geom1: str, geom2: str) -> str:
        """Format ST_Intersects function."""
        ...

    def format_st_make_point(self, x: float, y: float, z: Optional[float] = None, m: Optional[float] = None) -> str:
        """Format ST_MakePoint function."""
        ...

    def format_st_set_srid(self, geometry: str, srid: int) -> str:
        """Format ST_SetSRID function."""
        ...

    def format_geography_literal(self, lon: float, lat: float, srid: int = 4326) -> str:
        """Format a geography literal from longitude and latitude."""
        ...

    def format_st_area(self, geom_expr: str, use_spheroid: bool = False) -> str:
        """Format ST_Area function for area calculation."""
        ...

    def format_st_transform(self, geom_expr: str, target_srid: int) -> str:
        """Format ST_Transform function for coordinate system transformation."""
        ...

    def format_st_as_geojson(self, geom_expr: str) -> str:
        """Format ST_AsGeoJSON function for GeoJSON output."""
        ...

    def format_st_buffer(self, geom_expr: str, radius: float) -> str:
        """Format ST_Buffer function for buffer area calculation."""
        ...
