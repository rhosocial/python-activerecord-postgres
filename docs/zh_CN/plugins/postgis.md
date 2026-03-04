# PostGIS

PostGIS 是 PostgreSQL 的空间数据库扩展，提供完整的 GIS（地理信息系统）功能支持。

## 概述

PostGIS 提供以下功能：
- **geometry** 和 **geography** 数据类型
- 空间索引支持（GiST）
- 空间分析函数
- 坐标系统转换

💡 *AI 提示词：* "PostGIS 是什么？它能用来解决哪些问题？"

## 安装

```sql
CREATE EXTENSION postgis;
```

### 未安装时的错误

如果服务器未安装 PostGIS，执行 `CREATE EXTENSION postgis` 会报错：

```
ERROR: could not open extension control file 
"/usr/share/postgresql/{version}/extension/postgis.control": 
No such file or directory
```

这表示 PostGIS 未在服务器上安装。需要先在操作系统层面安装 PostGIS 包：

```bash
# Ubuntu/Debian
sudo apt-get install postgresql-{version}-postgis-3

# CentOS/RHEL
sudo yum install postgis33_{version}
```

### 权限要求

安装扩展需要超级用户权限：
```sql
-- 授权用户安装扩展的权限
GRANT CREATE ON DATABASE mydb TO myuser;
```

## 版本支持

| PostgreSQL 版本 | PostGIS 版本 | 状态 |
|----------------|-------------|------|
| 17 | 3.4+ | ✅ 支持 |
| 16 | 3.4+ | ✅ 支持 |
| 15 | 3.3+ | ✅ 支持 |
| 14 | 3.3+ | ✅ 插件支持保障 |
| 13 | 3.2+ | ✅ 支持 |
| 12 | 3.0+ | ✅ 支持 |
| 11 | 2.5+ | ⚠️ 有限支持 |
| 10 | 2.5+ | ⚠️ 有限支持 |
| 9.6 | 2.5+ | ⚠️ 有限支持 |

💡 *AI 提示词：* "PostGIS 2.5 与 PostGIS 3.4 之间有什么主要差距？升级需要注意什么？"

## 功能检测

```python
# 检查 PostGIS 是否已安装
if backend.dialect.is_extension_installed('postgis'):
    print("PostGIS 已安装")
    
# 获取版本
version = backend.dialect.get_extension_version('postgis')
print(f"PostGIS 版本: {version}")

# 检查功能支持
if backend.dialect.supports_geometry_type():
    # 支持 geometry 类型
    pass

if backend.dialect.supports_spatial_index():
    # 支持空间索引
    pass
```

💡 *AI 提示词：* "如何在 Python 中检测 PostGIS 是否已安装并获取其版本？"

## 数据类型

### geometry

平面坐标系统的几何类型：

```sql
CREATE TABLE locations (
    id SERIAL PRIMARY KEY,
    name TEXT,
    geom GEOMETRY(Point, 4326)
);
```

### geography

球面坐标系统的地理类型：

```sql
CREATE TABLE global_locations (
    id SERIAL PRIMARY KEY,
    name TEXT,
    geog GEOGRAPHY(Point, 4326)
);
```

## 空间索引

```sql
-- 创建 GiST 空间索引
CREATE INDEX idx_locations_geom ON locations USING GIST(geom);
```

## 常用函数

### 距离计算

```sql
-- 计算两点距离（米）
SELECT ST_Distance(
    ST_MakePoint(116.4, 39.9)::geography,
    ST_MakePoint(121.5, 31.2)::geography
);
```

### 包含查询

```sql
-- 查找多边形内的点
SELECT * FROM locations
WHERE ST_Contains(
    ST_MakePolygon(ST_MakePoint(0,0), ST_MakePoint(10,0), ST_MakePoint(10,10), ST_MakePoint(0,10)),
    geom
);
```

## 注意事项

1. PostGIS 安装需要超级用户权限
2. 空间索引对大数据集查询性能至关重要
3. geography 类型更适合大范围地理计算

💡 *AI 提示词：* "PostGIS 中的 geometry 和 geography 类型有什么区别？各自的适用场景是什么？"

💡 *AI 提示词：* "如何为 PostGIS 数据创建高效的查询索引？"

## 相关主题

- **[pgvector](./pgvector.md)**: 向量相似性搜索
- **[插件支持](./README.md)**: 插件检测机制
