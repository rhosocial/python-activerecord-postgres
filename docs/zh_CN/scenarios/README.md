# 场景实战

本章节提供 PostgreSQL 后端在真实业务场景中的完整应用示例，帮助开发者理解如何在实际项目中正确使用 rhosocial-activerecord。

## 场景列表

### [并行 Worker 处理](parallel_workers.md)

演示 PostgreSQL 后端在多进程/异步并发场景下的正确用法：

- **多进程正确用法**：`configure()` 必须在子进程内调用
- **PostgreSQL 异步优势**：psycopg 原生异步支持，真正的网络 I/O 并发
- **死锁处理**：PostgreSQL 自动检测死锁，生产环境推荐的重试机制
- **多线程陷阱**：psycopg 连接同样不支持多线程并发

> 📖 **配套代码**：完整可运行的实验代码位于 `docs/examples/chapter_08_scenarios/parallel_workers/` 目录。

## 与核心库场景的关系

本章节是 [核心库场景文档](https://github.com/Rhosocial/python-activerecord/tree/main/docs/zh_CN/scenarios) 的 PostgreSQL 特定补充，重点关注：

- PostgreSQL 特有的并发行为（MVCC、行级锁、死锁检测）
- psycopg 异步驱动的特点（单连接模型、真异步 I/O）
- RETURNING 子句的优势（INSERT/UPDATE/DELETE 返回数据）
- 与 MySQL 和 SQLite 的关键差异对比

核心库场景中介绍的通用 ActiveRecord 用法（如关联关系、查询构建）同样适用于 PostgreSQL 后端。
