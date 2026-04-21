# PostgreSQL with All Extensions

多架构 PostgreSQL Docker 镜像,包含所有常见扩展,用于测试 python-activerecord-postgres 项目。

## 支持的架构

- linux/amd64
- linux/arm64
- linux/ppc64le
- linux/s390x

## 支持的 PostgreSQL 版本

- PostgreSQL 14
- PostgreSQL 15
- PostgreSQL 16
- PostgreSQL 17
- PostgreSQL 18

## 预装的扩展

### 内置扩展 (contrib)

| 扩展 | 版本 | 说明 |
|-------|-----|------|
| uuid-ossp | 1.1 | UUID 生成函数 |
| pgcrypto | 1.3/1.4 | 加密函数 |
| ltree | 1.1~1.3 | 层级数据结构 |
| intarray | 1.2~1.5 | 整数数组操作符 |
| tablefunc | 1.0 | 表函数 (crosstab) |
| pg_trgm | 1.3~1.6 | 三元组相似度 |
| hstore | 1.4~1.8 | 键值存储 |
| cube | 1.2~1.5 | 多维立方体 |
| earthdistance | 1.1~1.3 | 地球距离 |
| pg_stat_statements | 1.4~1.12 | 查询统计 |

### 第三方扩展

| 扩展 | 版本 | 说明 |
|-------|-----|------|
| postgis | 3.4 | 空间数据库 |
| postgis_topology | 3.4 | PostGIS 拓扑 |
| postgis_tiger_geocoder | 3.4 | 地理编码 |
| vector | 0.8.0 | 向量相似度搜索 |

## 快速开始

### 使用预构建镜像

```bash
# 运行容器
docker run -d \
    --name postgres-all-ext \
    -e POSTGRES_PASSWORD=password \
    -e POSTGRES_DB=test_db \
    -p 5432:5432 \
    postgres:16

# 或者使用 docker-compose
docker compose up -d
```

### 构建本地镜像

```bash
# 进入目录
cd docker/postgres-all-ext

# 构建默认版本 (16)
./build.sh

# 构建所有版本
./build.sh all

# 构建特定版本
./build.sh 16
```

## 环境变量

| 变量 | 默认值 | 说明 |
|-----|-------|------|
| POSTGRES_USER | root | 数据库用户 |
| POSTGRES_PASSWORD | password | 数据库密码 |
| POSTGRES_DB | test_db | 数据库名称 |

## 验证扩展

```bash
# 连接数据库
docker exec -it postgres-all-ext psql -U root -d test_db

# 查看已安装的扩展
SELECT extname, extversion FROM pg_extension ORDER BY extname;

# 测试 postgis
SELECT postgis_version();

# 测试 pgvector
SELECT '[-1,0,1]'::vector;

# 测试 pg_trgm
SELECT similarity('hello', 'helo');
```

## 构建参数

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| PG_VERSION | 16 | PostgreSQL 主版本 |
| POSTGIS_VERSION | 3.4 | PostGIS 版本 |
| PGVECTOR_VERSION | 0.8.0 | pgvector 版本 |

## 目录结构

```
docker/postgres-all-ext/
├── Dockerfile                    # 多架构构建文件
├── docker-compose.yml           # Compose 配置
├── build.sh                # 构建脚本
├── README.md              # 本文件
└── docker-entrypoint-initdb.d/
    └── init-extensions.sh   # 扩展初始化脚本
```

## GitHub Actions

可以使用 GitHub Actions 构建多架构镜像:

```yaml
# .github/workflows/build.yml
name: Build PostgreSQL Images

on:
  push:
    branches:
      - main

jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        pg_version: [14, 15, 16, 17, 18]
    steps:
      - uses: actions/checkout@v4
      - uses: docker/build-push-action@v5
        with:
          context: docker/postgres-all-ext
          push: false
          tags: postgres-all-ext:${{ matrix.pg_version }}
          build-args: |
            PG_VERSION=${{ matrix.pg_version }}
```

## 注意事项

1. **架构支持**: Debian bookworm 原生支持 amd64, arm64; ppc64le 和 s390x 需要额外配置
2. **版本兼容性**: PostGIS 3.4 支持 PostgreSQL 13-16; PostgreSQL 17+ 需要 PostGIS 3.5+
3. **资源限制**: 建议至少 2GB 内存
4. **数据持久化**: 使用 docker volume 持久化数据

## License

继承 PostgreSQL 和各扩展的许可证

## 参考

- [PostgreSQL Docker](https://github.com/docker-library/postgres)
- [PostGIS Docker](https://github.com/postgis/docker-postgis)
- [pgvector](https://github.com/pgvector/pgvector)
- [PostgreSQL contrib](https://www.postgresql.org/docs/current/contrib.html)