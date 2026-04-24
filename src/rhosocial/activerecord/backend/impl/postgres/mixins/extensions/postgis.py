# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/postgis.py
"""
PostGIS spatial functionality implementation.

This module provides the PostgresPostGISMixin class that adds support for
PostGIS extension features including geometry/geography types, spatial indexes,
and spatial functions.
"""

from typing import TYPE_CHECKING, Optional, Tuple

if TYPE_CHECKING:
    pass  # No type imports needed for this mixin


class PostgresPostGISMixin:
    """PostGIS spatial functionality implementation."""

    def supports_postgis_geometry_type(self) -> bool:
        """Check if PostGIS supports geometry type."""
        return self.check_extension_feature("postgis", "geometry_type")

    def supports_postgis_geography_type(self) -> bool:
        """Check if PostGIS supports geography type."""
        return self.check_extension_feature("postgis", "geography_type")

    def supports_postgis_spatial_index(self) -> bool:
        """Check if PostGIS supports spatial index."""
        return self.check_extension_feature("postgis", "spatial_index")

    def supports_postgis_spatial_functions(self) -> bool:
        """Check if PostGIS supports spatial functions."""
        return self.check_extension_feature("postgis", "spatial_functions")

    def format_geometry_literal(self, wkt: str, srid: Optional[int] = None, geometry_type: str = "geometry") -> str:
        """Format a geometry literal from WKT (Well-Known Text).

        Args:
            wkt: Well-Known Text representation of geometry
            srid: Optional spatial reference system ID
            geometry_type: 'geometry' or 'geography'

        Returns:
            SQL geometry literal string

        Example:
            >>> format_geometry_literal('POINT(0 0)', srid=4326)
            "ST_GeomFromText('POINT(0 0)', 4326)"
            >>> format_geometry_literal('POINT(0 0)', srid=4326, geometry_type='geography')
            "ST_GeogFromText('POINT(0 0)', 4326)"
        """
        if geometry_type.lower() == "geography":
            if srid is not None:
                return f"ST_GeogFromText('{wkt}', {srid})"
            return f"ST_GeogFromText('{wkt}')"
        else:
            if srid is not None:
                return f"ST_GeomFromText('{wkt}', {srid})"
            return f"ST_GeomFromText('{wkt}')"

    def format_spatial_function(self, function_name: str, *args: str, **kwargs) -> str:
        """Format a spatial function call.

        Args:
            function_name: Name of the spatial function (ST_*)
            *args: Positional arguments for the function
            **kwargs: Optional named parameters

        Returns:
            SQL function call string

        Example:
            >>> format_spatial_function('ST_Distance', 'geom1', 'geom2')
            "ST_Distance(geom1, geom2)"
            >>> format_spatial_function('ST_DWithin', 'geom1', 'geom2', 1000)
            "ST_DWithin(geom1, geom2, 1000)"
        """
        args_str = ", ".join(str(arg) for arg in args)
        return f"{function_name}({args_str})"

    def format_spatial_index_statement(
        self, index_name: str, table_name: str, column_name: str, schema: Optional[str] = None
    ) -> Tuple[str, tuple]:
        """Format CREATE INDEX statement for spatial column.

        Args:
            index_name: Name of the index
            table_name: Table name
            column_name: Geometry/Geography column name
            schema: Optional schema name

        Returns:
            Tuple of (SQL statement, parameters)

        Example:
            >>> format_spatial_index_statement('idx_geom', 'locations', 'geom')
            ("CREATE INDEX idx_geom ON locations USING gist (geom)", ())
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = f"CREATE INDEX {index_name} ON {full_table} USING gist ({column_name})"
        return (sql, ())

    def format_st_distance(self, geom1: str, geom2: str, use_spheroid: bool = False) -> str:
        """Format ST_Distance function.

        Args:
            geom1: First geometry
            geom2: Second geometry
            use_spheroid: Use spheroid for geography type

        Returns:
            SQL function call

        Example:
            >>> format_st_distance('geom1', 'geom2')
            "ST_Distance(geom1, geom2)"
        """
        if use_spheroid:
            return f"ST_Distance({geom1}, {geom2}, true)"
        return f"ST_Distance({geom1}, {geom2})"

    def format_st_dwithin(self, geom1: str, geom2: str, distance: float, use_spheroid: bool = False) -> str:
        """Format ST_DWithin function for proximity queries.

        Args:
            geom1: First geometry
            geom2: Second geometry
            distance: Distance threshold
            use_spheroid: Use spheroid for geography type

        Returns:
            SQL function call

        Example:
            >>> format_st_dwithin('geom', 'ST_MakePoint(0, 0)', 1000)
            "ST_DWithin(geom, ST_MakePoint(0, 0), 1000)"
        """
        if use_spheroid:
            return f"ST_DWithin({geom1}, {geom2}, {distance}, true)"
        return f"ST_DWithin({geom1}, {geom2}, {distance})"

    def format_st_contains(self, outer: str, inner: str) -> str:
        """Format ST_Contains function.

        Args:
            outer: The containing geometry
            inner: The contained geometry

        Returns:
            SQL function call

        Example:
            >>> format_st_contains('boundary', 'point')
            "ST_Contains(boundary, point)"
        """
        return f"ST_Contains({outer}, {inner})"

    def format_st_intersects(self, geom1: str, geom2: str) -> str:
        """Format ST_Intersects function.

        Args:
            geom1: First geometry
            geom2: Second geometry

        Returns:
            SQL function call

        Example:
            >>> format_st_intersects('geom1', 'geom2')
            "ST_Intersects(geom1, geom2)"
        """
        return f"ST_Intersects({geom1}, {geom2})"

    def format_st_make_point(self, x: float, y: float, z: Optional[float] = None, m: Optional[float] = None) -> str:
        """Format ST_MakePoint function.

        Args:
            x: X coordinate (longitude)
            y: Y coordinate (latitude)
            z: Optional Z coordinate
            m: Optional M coordinate

        Returns:
            SQL function call

        Example:
            >>> format_st_make_point(0, 0)
            "ST_MakePoint(0, 0)"
            >>> format_st_make_point(0, 0, 10)
            "ST_MakePoint(0, 0, 10)"
        """
        if z is not None and m is not None:
            return f"ST_MakePoint({x}, {y}, {z}, {m})"
        elif z is not None:
            return f"ST_MakePoint({x}, {y}, {z})"
        return f"ST_MakePoint({x}, {y})"

    def format_st_set_srid(self, geometry: str, srid: int) -> str:
        """Format ST_SetSRID function.

        Args:
            geometry: Geometry expression
            srid: Spatial reference system ID

        Returns:
            SQL function call

        Example:
            >>> format_st_set_srid("ST_MakePoint(0, 0)", 4326)
            "ST_SetSRID(ST_MakePoint(0, 0), 4326)"
        """
        return f"ST_SetSRID({geometry}, {srid})"

    def format_geography_literal(self, lon: float, lat: float, srid: int = 4326) -> str:
        """Format a geography literal from longitude and latitude.

        Args:
            lon: Longitude value
            lat: Latitude value
            srid: Spatial reference system ID (default: 4326)

        Returns:
            SQL geography literal string

        Example:
            >>> format_geography_literal(116.4, 39.9)
            "ST_GeogFromText('SRID=4326;POINT(116.4 39.9)')"
            >>> format_geography_literal(116.4, 39.9, srid=4326)
            "ST_GeogFromText('SRID=4326;POINT(116.4 39.9)')"
        """
        return f"ST_GeogFromText('SRID={srid};POINT({lon} {lat})')"

    def format_st_area(self, geom_expr: str, use_spheroid: bool = False) -> str:
        """Format ST_Area function for area calculation.

        Args:
            geom_expr: Geometry or geography expression
            use_spheroid: Use spheroid for geography type

        Returns:
            SQL function call

        Example:
            >>> format_st_area('geom')
            "ST_Area(geom)"
            >>> format_st_area('geog', use_spheroid=True)
            "ST_Area(geog, true)"
        """
        if use_spheroid:
            return f"ST_Area({geom_expr}, true)"
        return f"ST_Area({geom_expr})"

    def format_st_transform(self, geom_expr: str, target_srid: int) -> str:
        """Format ST_Transform function for coordinate system transformation.

        Args:
            geom_expr: Geometry expression
            target_srid: Target spatial reference system ID

        Returns:
            SQL function call

        Example:
            >>> format_st_transform('geom', 3857)
            "ST_Transform(geom, 3857)"
        """
        return f"ST_Transform({geom_expr}, {target_srid})"

    def format_st_as_geojson(self, geom_expr: str) -> str:
        """Format ST_AsGeoJSON function for GeoJSON output.

        Args:
            geom_expr: Geometry or geography expression

        Returns:
            SQL function call

        Example:
            >>> format_st_as_geojson('geom')
            "ST_AsGeoJSON(geom)"
        """
        return f"ST_AsGeoJSON({geom_expr})"

    def format_st_buffer(self, geom_expr: str, radius: float) -> str:
        """Format ST_Buffer function for buffer area calculation.

        Args:
            geom_expr: Geometry expression
            radius: Buffer radius

        Returns:
            SQL function call

        Example:
            >>> format_st_buffer('geom', 100)
            "ST_Buffer(geom, 100)"
        """
        return f"ST_Buffer({geom_expr}, {radius})"
