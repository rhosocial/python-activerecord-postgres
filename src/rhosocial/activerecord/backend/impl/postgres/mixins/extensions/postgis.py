# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/postgis.py
"""PostGIS capability probes and spatial-index renderer.

Validation belongs to the formatter; the node is a plain argument bag.
"""

from typing import Tuple

from rhosocial.activerecord.backend.expression.objects import Index, Table

from ...expression.statements.ddl import (
    PostgresCreateIndexExpression,
    PostgresIndexType,
    PostgresSpatialIndexExpression,
)


class PostgresPostGISMixin:
    """PostGIS spatial functionality implementation."""

    def supports_postgis_geometry_type(self) -> bool:
        return self.check_extension_feature("postgis", "geometry_type")

    def supports_postgis_geography_type(self) -> bool:
        return self.check_extension_feature("postgis", "geography_type")

    def supports_postgis_spatial_index(self) -> bool:
        return self.check_extension_feature("postgis", "spatial_index")

    def supports_postgis_spatial_functions(self) -> bool:
        return self.check_extension_feature("postgis", "spatial_functions")

    def format_postgres_spatial_index_expression(
        self, expr: PostgresSpatialIndexExpression
    ) -> Tuple[str, tuple]:
        method = PostgresIndexType.normalize(expr.index_type or "gist").value
        if method not in ("gist", "spgist", "brin"):
            raise ValueError(
                f"PostGIS index_type must be 'gist', 'spgist' or 'brin', "
                f"got {expr.index_type!r}"
            )
        return PostgresCreateIndexExpression(
            self,
            index=Index(self, expr.name, schema_name=expr.schema),
            table=Table(self, expr.table, schema_name=expr.schema),
            columns=expr.columns,
            index_type=method,
        ).to_sql()
