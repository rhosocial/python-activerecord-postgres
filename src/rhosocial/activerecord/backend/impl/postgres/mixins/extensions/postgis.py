# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/postgis.py
"""PostGIS capability probes.

Index creation is an expression concern: use ``functions.postgis.spatial_index``,
which returns a ``PostgresCreateIndexExpression``. Dialect formatters only ever
take ``(self, expr)``.
"""


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
