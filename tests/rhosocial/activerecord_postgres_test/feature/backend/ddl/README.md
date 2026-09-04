# ddl tests

PostgreSQL DDL execution coverage: auto-increment / defaults DDL regressions, plus cross-backend DDL qualifiers (ALTER TABLE IF [NOT] EXISTS, DROP TABLE CASCADE/RESTRICT). Partition tests live in `postgres/partition/`.

## Key files

- `test_auto_increment_ddl.py` — auto-increment / boolean default / timestamp regressions
- `test_alter_table_if_exists.py` — IF [NOT] EXISTS qualifiers, USING, UNLOGGED
- `test_drop_table_cascade.py` — CASCADE / RESTRICT rendering
