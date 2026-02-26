# 高级索引

## B-Tree 索引（默认）

标准 B-Tree 索引适用于等值和范围查询：

```sql
CREATE INDEX idx_users_name ON users(name);
CREATE INDEX idx_users_email ON users(email);
```

## GIN 索引（通用倒排索引）

适用于数组和 JSONB：

```sql
-- 用于数组列
CREATE INDEX idx_articles_tags ON articles USING GIN(tags);

-- 用于 JSONB 列
CREATE INDEX idx_products_attributes ON products USING GIN(attributes);
```

## GiST 索引（通用搜索树）

适用于范围类型和全文搜索：

```sql
-- 用于范围类型
CREATE INDEX idx_bookings_date_range ON bookings USING GIST(date_range);

-- 用于全文搜索
CREATE INDEX idx_articles_content ON articles USING GIST(to_tsvector('english', content));
```

## BRIN 索引（块范围索引）

适用于有序数据的大型表：

```sql
-- 用于具有顺序数据的时间戳列
CREATE INDEX idx_logs_created_at ON logs USING BRIN(created_at);
```

## 部分索引

仅索引满足条件的行：

```sql
CREATE INDEX idx_active_users ON users(email) WHERE active = true;
```

💡 *AI 提示词：* "什么时候应该选择 GIN 而不是 GiST 用于 JSONB 索引？"
