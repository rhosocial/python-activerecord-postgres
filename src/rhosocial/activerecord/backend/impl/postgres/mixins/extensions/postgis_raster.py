# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/postgis_raster.py
"""
PostgreSQL PostGIS Raster functionality mixin.

This module provides functionality to check postgis_raster extension features.

For SQL expression generation (e.g. ST_Value, ST_Summary, raster literals),
use the function factories in ``functions/postgis_raster.py``
instead of the removed format_* methods.
"""


class PostgresPostgisRasterMixin:
    """PostGIS Raster functionality implementation."""

    def supports_postgis_raster_type(self) -> bool:
        """Check if postgis_raster supports raster type."""
        return self.check_extension_feature("postgis_raster", "raster_type")
