# src/rhosocial/activerecord/backend/impl/postgres/types/postgis.py
"""
PostgreSQL PostGIS geometry/geography type.

This module provides the PostgresGeometry data class for representing
PostGIS geometry and geography values in Python, enabling type-safe
spatial operations.

PostGIS Documentation: https://postgis.net/docs/

The geometry/geography types require the PostGIS extension:
    CREATE EXTENSION IF NOT EXISTS postgis;
"""

import re
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class PostgresGeometry:
    """PostGIS geometry/geography type representation.

    Stores a geometry value as WKT (Well-Known Text) with optional
    SRID and geometry type annotation. This type maps to the PostgreSQL
    ``geometry`` or ``geography`` types provided by the PostGIS extension.

    Attributes:
        wkt: Well-Known Text representation (e.g., ``POINT(1 2)``)
        srid: Optional Spatial Reference System ID (e.g., 4326)
        geometry_type: Either ``geometry`` or ``geography``

    Examples:
        >>> g = PostgresGeometry.point(1.0, 2.0, srid=4326)
        >>> g.wkt
        'POINT(1.0 2.0)'
        >>> g.srid
        4326
        >>> g.to_postgres_string()
        "ST_GeomFromText('POINT(1.0 2.0)', 4326)"
    """

    wkt: str
    srid: Optional[int] = None
    geometry_type: str = "geometry"

    def __post_init__(self):
        if self.geometry_type not in ("geometry", "geography"):
            raise ValueError(
                f"geometry_type must be 'geometry' or 'geography', got {self.geometry_type!r}"
            )
        if not self.wkt.strip():
            raise ValueError("WKT must not be empty")

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL SQL function call string.

        Returns:
            SQL expression string like ``ST_GeomFromText('POINT(1 2)', 4326)``

        Examples:
            >>> PostgresGeometry.point(1.0, 2.0, srid=4326).to_postgres_string()
            "ST_GeomFromText('POINT(1.0 2.0)', 4326)"
            >>> PostgresGeometry.point(1.0, 2.0, srid=4326, geometry_type='geography').to_postgres_string()
            "ST_GeogFromText('SRID=4326;POINT(1.0 2.0)')"
        """
        if self.geometry_type == "geography":
            if self.srid is not None:
                return f"ST_GeogFromText('SRID={self.srid};{self.wkt}')"
            return f"ST_GeogFromText('{self.wkt}')"
        else:
            if self.srid is not None:
                return f"ST_GeomFromText('{self.wkt}', {self.srid})"
            return f"ST_GeomFromText('{self.wkt}')"

    @classmethod
    def from_postgres_string(cls, value: str) -> "PostgresGeometry":
        """Parse from PostgreSQL output (WKT or EWKT format).

        Handles common output formats from PostGIS:
        - EWKT: ``SRID=4326;POINT(1 2)``
        - WKT: ``POINT(1 2)``

        Args:
            value: WKT or EWKT string

        Returns:
            PostgresGeometry instance

        Raises:
            ValueError: If the string format is invalid
        """
        value = value.strip()
        srid = None

        # Handle EWKT format: SRID=4326;POINT(...)
        if value.upper().startswith("SRID="):
            parts = value.split(";", 1)
            if len(parts) != 2:
                raise ValueError(f"Invalid EWKT format: {value!r}")
            try:
                srid = int(parts[0][5:])
            except ValueError:
                raise ValueError(f"Invalid SRID in EWKT: {value!r}")
            wkt = parts[1].strip()
        else:
            wkt = value

        return cls(wkt=wkt, srid=srid)

    @classmethod
    def point(
        cls,
        x: float,
        y: float,
        srid: Optional[int] = None,
        geometry_type: str = "geometry",
    ) -> "PostgresGeometry":
        """Convenience constructor for POINT geometry.

        Args:
            x: X coordinate (longitude)
            y: Y coordinate (latitude)
            srid: Optional SRID
            geometry_type: ``geometry`` or ``geography``

        Returns:
            PostgresGeometry representing the point
        """
        return cls(wkt=f"POINT({x} {y})", srid=srid, geometry_type=geometry_type)

    @classmethod
    def linestring(
        cls,
        coordinates: List[Tuple[float, float]],
        srid: Optional[int] = None,
        geometry_type: str = "geometry",
    ) -> "PostgresGeometry":
        """Convenience constructor for LINESTRING geometry.

        Args:
            coordinates: List of (x, y) coordinate pairs
            srid: Optional SRID
            geometry_type: ``geometry`` or ``geography``

        Returns:
            PostgresGeometry representing the linestring
        """
        if len(coordinates) < 2:
            raise ValueError("LINESTRING requires at least 2 points")
        coords_str = ", ".join(f"{x} {y}" for x, y in coordinates)
        return cls(wkt=f"LINESTRING({coords_str})", srid=srid, geometry_type=geometry_type)

    @classmethod
    def polygon(
        cls,
        rings: List[List[Tuple[float, float]]],
        srid: Optional[int] = None,
        geometry_type: str = "geometry",
    ) -> "PostgresGeometry":
        """Convenience constructor for POLYGON geometry.

        Args:
            rings: List of rings, each ring is a list of (x, y) coordinate pairs.
                The first ring is the exterior ring, subsequent rings are holes.
            srid: Optional SRID
            geometry_type: ``geometry`` or ``geography``

        Returns:
            PostgresGeometry representing the polygon
        """
        if not rings:
            raise ValueError("POLYGON requires at least one ring")
        rings_str = ", ".join(
            f"({', '.join(f'{x} {y}' for x, y in ring)})"
            for ring in rings
        )
        return cls(wkt=f"POLYGON({rings_str})", srid=srid, geometry_type=geometry_type)

    def __str__(self) -> str:
        if self.srid is not None:
            return f"SRID={self.srid};{self.wkt}"
        return self.wkt

    def __eq__(self, other) -> bool:
        if isinstance(other, PostgresGeometry):
            return (self.wkt == other.wkt
                    and self.srid == other.srid
                    and self.geometry_type == other.geometry_type)
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.wkt, self.srid, self.geometry_type))


__all__ = [
    "PostgresGeometry",
]
