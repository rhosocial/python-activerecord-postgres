# rhosocial-activerecord PostgreSQL 后端文档

> 🤖 **AI 学习助手**：本文档中关键概念旁标有 💡 AI 提示词标记。遇到不理解的概念时，可以直接向 AI 助手提问。

> **示例：** "PostgreSQL 后端如何处理 JSONB 数据类型？与普通 JSON 有什么优势？"

> 📖 **详细用法请参考**：[AI 辅助开发指南](introduction/ai_assistance.md)

## 目录 (Table of Contents)

1. **[简介 (Introduction)](introduction/README.md)**
    *   **[PostgreSQL 后端概述](introduction/README.md)**: 为什么选择 PostgreSQL 后端
    *   **[与核心库的关系](introduction/relationship.md)**: rhosocial-activerecord 与 PostgreSQL 后端的集成
    *   **[支持版本](introduction/supported_versions.md)**: PostgreSQL 8.0~17, Python 3.8+ 支持情况

2. **[安装与配置 (Installation & Configuration)](installation_and_configuration/README.md)**
    *   **[安装指南](installation_and_configuration/installation.md)**: pip 安装与环境要求
    *   **[连接配置](installation_and_configuration/configuration.md)**: host, port, database, username, password 等配置项
    *   **[SSL/TLS 配置](installation_and_configuration/ssl.md)**: 安全连接设置
    *   **[连接管理](installation_and_configuration/pool.md)**: 随用随连模式
    *   **[客户端编码](installation_and_configuration/encoding.md)**: 字符编码设置

3. **[PostgreSQL 特性 (PostgreSQL Specific Features)](backend_specific_features/README.md)**
    *   **[PostgreSQL 扩展](backend_specific_features/extensions.md)**: PostGIS、pgvector、pg_trgm 等 25+ 个扩展
    *   **[PostgreSQL 特定字段类型](backend_specific_features/field_types.md)**: ARRAY, JSONB, UUID, Range 类型
    *   **[PostgreSQL Dialect 表达式](backend_specific_features/dialect.md)**: PostgreSQL 特定的 SQL 方言
    *   **[高级索引](backend_specific_features/indexing.md)**: GIN, GiST, BRIN 索引类型
    *   **[RETURNING 子句](backend_specific_features/returning.md)**: INSERT/UPDATE/DELETE RETURNING 支持
    *   **[协议支持矩阵](backend_specific_features/protocol_support.md)**: 协议支持和版本兼容性
    *   **[数据库内省](backend_specific_features/introspection.md)**: 使用 pg_catalog 查询元数据
    *   **[EXPLAIN 支持](backend_specific_features/explain.md)**: 查询执行计划分析
    *   **[表分区](backend_specific_features/partition.md)**: RANGE / LIST / HASH 分区
    *   **[属性图查询](backend_specific_features/property_graph_query.md)**: SQL/PGQ 属性图查询

4. **[DDL 操作 (DDL Operations)](ddl/README.md)**
    *   **[PostgreSQL DDL 概览](ddl/README.md)**: CREATE TABLE, ALTER TABLE, DROP TABLE
    *   **[示例代码](../examples/chapter_04_ddl/ddl.py)**: DDL 示例

5. **[事务支持 (Transaction Support)](transaction_support/README.md)**
    *   **[事务隔离级别](transaction_support/isolation_level.md)**: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
    *   **[Savepoint 支持](transaction_support/savepoint.md)**: 嵌套事务
    *   **[DEFERRABLE 模式](transaction_support/deferrable.md)**: 延迟约束检查
    *   **[死锁处理](transaction_support/deadlock.md)**: 失败重试机制

6. **[类型适配器 (Type Adapters)](type_adapters/README.md)**
    *   **[PostgreSQL 到 Python 类型映射](type_adapters/mapping.md)**: 类型转换规则
    *   **[自定义类型适配器](type_adapters/custom.md)**: 扩展类型支持
    *   **[时区处理](type_adapters/timezone.md)**: TIMESTAMP WITH TIME ZONE
    *   **[数组类型处理](type_adapters/arrays.md)**: PostgreSQL 数组支持

7. **[测试 (Testing)](testing/README.md)**
    *   **[测试配置](testing/configuration.md)**: 测试环境设置
    *   **[本地 PostgreSQL 测试](testing/local.md)**: 本地数据库测试

8. **[故障排除 (Troubleshooting)](troubleshooting/README.md)**
    *   **[常见连接错误](troubleshooting/connection.md)**: 连接问题诊断
    *   **[性能问题](troubleshooting/performance.md)**: 性能瓶颈分析

9. **[场景实战 (Scenarios)](scenarios/README.md)**
    *   **[并行 Worker 处理](scenarios/parallel_workers.md)**: 多进程/异步并发场景的正确用法

10. **[自定义 (Customization)](customization/README.md)**
    *   **[自定义表达式](customization/custom_expressions.md)**: 为数据库特定 SQL 创建新的表达式类
    *   **[自定义数据类型](customization/custom_types.md)**: 为自定义列类型定义新的 DataType 子类
    *   **[自定义类型适配器](customization/custom_adapters.md)**: 注册 Python 与数据库值之间的自定义转换器

11. **[命令行界面 (CLI)](cli/README.md)**
    *   **[CLI 概述](cli/README.md)**: 查询、内省和管理 PostgreSQL 的命令

> 📖 **核心库文档**：要了解 ActiveRecord 框架的完整功能，请参考 [rhosocial-activerecord 文档](https://github.com/Rhosocial/python-activerecord/tree/main/docs/zh_CN)。
