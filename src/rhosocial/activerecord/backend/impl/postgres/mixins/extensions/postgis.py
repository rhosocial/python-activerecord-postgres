# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/postgis.py
"""
PostgreSQL PostGIS spatial functionality mixin.

This module provides functionality to check PostGIS extension features,
including geometry/geography types and spatial indexes.

For SQL expression generation, use the function factories in
``functions/postgis.py`` instead of the removed format_* methods.
For DDL index creation, use ``format_spatial_index_statement``.
"""

from typing import Optional, Tuple


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
        """
        full_table = f"{schema}.{table_name}" if schema else table_name
        sql = f"CREATE INDEX {index_name} ON {full_table} USING gist ({column_name})"
        return (sql, ())
