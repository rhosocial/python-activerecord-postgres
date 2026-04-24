# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/postgis_raster.py
"""
PostGIS Raster functionality implementation.

This module provides the PostgresPostgisRasterMixin class that adds support for
postgis_raster extension features including raster type, pixel value extraction,
and raster metadata inspection.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass  # No type imports needed for this mixin


class PostgresPostgisRasterMixin:
    """PostGIS Raster functionality implementation."""

    def supports_postgis_raster_type(self) -> bool:
        """Check if postgis_raster supports raster type."""
        return self.check_extension_feature("postgis_raster", "raster_type")

    def format_raster_literal(self, raster_data: str, srid: int = 4326) -> str:
        """Format a raster literal from HexWKB data.

        Args:
            raster_data: HexWKB representation of raster data
            srid: Spatial reference system ID (default: 4326)

        Returns:
            SQL raster literal string

        Example:
            >>> format_raster_literal('0100000...', srid=4326)
            "ST_SetSRID(ST_RastFromHexWKB('0100000...'), 4326)"
        """
        return f"ST_SetSRID(ST_RastFromHexWKB('{raster_data}'), {srid})"

    def format_st_value(self, raster_expr: str, x: int, y: int) -> str:
        """Format ST_Value function for pixel value extraction.

        Args:
            raster_expr: Raster expression
            x: Column number (1-based)
            y: Row number (1-based)

        Returns:
            SQL function call string

        Example:
            >>> format_st_value('rast', 1, 1)
            "ST_Value(rast, 1, 1)"
        """
        return f"ST_Value({raster_expr}, {x}, {y})"

    def format_st_summary(self, raster_expr: str) -> str:
        """Format ST_Summary function for raster metadata.

        Args:
            raster_expr: Raster expression

        Returns:
            SQL function call string

        Example:
            >>> format_st_summary('rast')
            "ST_Summary(rast)"
        """
        return f"ST_Summary({raster_expr})"
