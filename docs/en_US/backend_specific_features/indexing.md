# Advanced Indexing

## B-Tree Index (Default)

Standard B-Tree indexes work for equality and range queries:

```sql
CREATE INDEX idx_users_name ON users(name);
CREATE INDEX idx_users_email ON users(email);
```

## GIN Index (Generalized Inverted Index)

Ideal for arrays and JSONB:

```sql
-- For array columns
CREATE INDEX idx_articles_tags ON articles USING GIN(tags);

-- For JSONB columns
CREATE INDEX idx_products_attributes ON products USING GIN(attributes);
```

## GiST Index (Generalized Search Tree)

Useful for range types and full-text search:

```sql
-- For range types
CREATE INDEX idx_bookings_date_range ON bookings USING GIST(date_range);

-- For full-text search
CREATE INDEX idx_articles_content ON articles USING GIST(to_tsvector('english', content));
```

## BRIN Index (Block Range Index)

Efficient for large tables with sorted data:

```sql
-- For timestamp columns with sequential data
CREATE INDEX idx_logs_created_at ON logs USING BRIN(created_at);
```

## Partial Indexes

Index only rows meeting conditions:

```sql
CREATE INDEX idx_active_users ON users(email) WHERE active = true;
```

💡 *AI Prompt:* "When should I choose GIN over GiST for JSONB indexing?"
