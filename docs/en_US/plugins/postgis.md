# PostGIS

PostGIS is a spatial database extension for PostgreSQL, providing complete GIS (Geographic Information System) functionality.

## Overview

PostGIS provides:
- **geometry** and **geography** data types
- Spatial index support (GiST)
- Spatial analysis functions
- Coordinate system transformations

💡 *AI Prompt:* "What is PostGIS and what problems can it solve?"

## Installation

```sql
CREATE EXTENSION postgis;
```

### Error When Not Installed

If PostGIS is not installed on the server, running `CREATE EXTENSION postgis` will result in:

```
ERROR: could not open extension control file 
"/usr/share/postgresql/{version}/extension/postgis.control": 
No such file or directory
```

This indicates PostGIS is not installed on the server. You need to install PostGIS at the OS level first:

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-{version}-postgis-3

# CentOS/RHEL
sudo yum install postgis33_{version}
```

### Permission Requirements

Installing extensions requires superuser privileges:
```sql
-- Grant user permission to install extensions
GRANT CREATE ON DATABASE mydb TO myuser;
```

## Version Support

| PostgreSQL Version | PostGIS Version | Status |
|-------------------|-----------------|--------|
| 17 | 3.4+ | ✅ Supported |
| 16 | 3.4+ | ✅ Supported |
| 15 | 3.3+ | ✅ Supported |
| 14 | 3.3+ | ✅ Plugin support guaranteed |
| 13 | 3.2+ | ✅ Supported |
| 12 | 3.0+ | ✅ Supported |
| 11 | 2.5+ | ⚠️ Limited support |
| 10 | 2.5+ | ⚠️ Limited support |
| 9.6 | 2.5+ | ⚠️ Limited support |

💡 *AI Prompt:* "What are the main differences between PostGIS 2.5 and PostGIS 3.4? What should I consider when upgrading?"

## Feature Detection

```python
# Check if PostGIS is installed
if backend.dialect.is_extension_installed('postgis'):
    print("PostGIS is installed")
    
# Get version
version = backend.dialect.get_extension_version('postgis')
print(f"PostGIS version: {version}")

# Check feature support
if backend.dialect.supports_geometry_type():
    # geometry type is supported
    pass

if backend.dialect.supports_spatial_index():
    # spatial index is supported
    pass
```

💡 *AI Prompt:* "How to check if PostGIS is installed in Python and get its version?"

## Data Types

### geometry

Geometry type for planar coordinate systems:

```sql
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    name TEXT,
    geom GEOMETRY(Point, 4326)
);
```

### geography

Geography type for spherical coordinate systems:

```sql
CREATE TABLE global_locations (
    id SERIAL PRIMARY KEY,
    name TEXT,
    geog GEOGRAPHY(Point, 4326)
);
```

## Spatial Index

```sql
-- Create GiST spatial index
CREATE INDEX idx_locations_geom ON locations USING GIST(geom);
```

## Common Functions

### Distance Calculation

```sql
-- Calculate distance between two points (meters)
SELECT ST_Distance(
    ST_MakePoint(116.4, 39.9)::geography,
    ST_MakePoint(121.5, 31.2)::geography
);
```

### Containment Query

```sql
-- Find points within a polygon
SELECT * FROM locations
WHERE ST_Contains(
    ST_MakePolygon(ST_MakePoint(0,0), ST_MakePoint(10,0), ST_MakePoint(10,10), ST_MakePoint(0,10)),
    geom
);
```

## Notes

1. PostGIS installation requires superuser privileges
2. Spatial indexes are crucial for query performance on large datasets
3. geography type is better suited for large-scale geographic calculations

💡 *AI Prompt:* "What is the difference between geometry and geography types in PostGIS? What are the use cases for each?"

💡 *AI Prompt:* "How to create efficient query indexes for PostGIS data?"

## Related Topics

- **[pgvector](./pgvector.md)**: Vector similarity search
- **[Plugin Support](./README.md)**: Plugin detection mechanism
