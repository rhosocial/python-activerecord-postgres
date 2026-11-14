# Directory-bound database shards

Every directory under `execute/` that holds tests declares exactly one
database in its own `conftest.py`:

```python
from pathlib import Path

from providers.shards import register

register(Path(__file__).parent, "test_db_<n>")
```

The rules behind the map:

- **One leaf directory = one database.** A directory that only groups
  capabilities (e.g. `execute/postgres/extension/`, `execute/postgres/types/`)
  holds no test files and declares no shard; only its leaf subdirectories do.
- Shared, cross-backend features use the fixed numbers `test_db_1` …
  `test_db_23`; `test_db_24` is reserved for the backend namespace root
  (`execute/postgres/`), and backend-unique leaves start at `test_db_25` with
  no upper limit.
- A sharded directory's tests stay on one xdist worker
  (`xdist_group("db:<name>")`) and share that database: directories run
  serially, different directories run in parallel. Construct tests carry no
  group and no database.
- `resolve_database_name()` prefers the directory shard over worker-slot
  pooling, so serial runs use the directory's database too.
- Worker-slot pooling uses the `test_db_pool_<index>` base (see
  `tests/providers/pooling.py`), so pool slots never collide with
  `test_db_<n>` shards.
- All registered shards are created and cleared once per session by the
  autouse session fixture in `tests/conftest.py`; they are not dropped at the
  end of the session.
- When a leaf is added or moved, update this table and allocate the next free
  number.

## Shared leaves (fixed 1..23)

| Leaf directory | Database | Test modules |
|---|---|---|
| `execute/connection/` | `test_db_1` | 8 |
| `execute/crud/` | `test_db_2` | 4 |
| `execute/query/` | `test_db_3` | 5 |
| `execute/join/` | `test_db_4` | 1 |
| `execute/transaction/` | `test_db_5` | 8 |
| `execute/ddl_table/` | `test_db_6` | 1 |
| `execute/ddl_column/` | `test_db_7` | 0 (placeholder `.gitkeep`) |
| `execute/ddl_constraint/` | `test_db_8` | 0 (placeholder `.gitkeep`) |
| `execute/ddl_index/` | `test_db_9` | 0 (placeholder `.gitkeep`) |
| `execute/ddl_view/` | `test_db_10` | 2 |
| `execute/dml_insert/` | `test_db_11` | 2 |
| `execute/dml_update_delete/` | `test_db_12` | 1 |
| `execute/json/` | `test_db_13` | 1 |
| `execute/partition/` | `test_db_14` | 4 |
| `execute/introspection/` | `test_db_15` | 19 |
| `execute/errors/` | `test_db_16` | 3 |
| `execute/concurrency/` | `test_db_17` | 2 |
| `execute/cli/` | `test_db_18` | 4 |
| `execute/functions/` | `test_db_19` | 2 |
| `execute/sequence_identity/` | `test_db_20` | 1 |
| `execute/fulltext_spatial/` | `test_db_21` | 0 (placeholder `.gitkeep`; PostgreSQL full-text/spatial extensions live under `execute/postgres/extension/`) |
| `execute/show_admin/` | `test_db_22` | 0 (placeholder `.gitkeep`) |
| `execute/routine_domain/` | `test_db_23` | 0 (placeholder `.gitkeep`) |

## Backend namespace root

| Directory | Database | Test modules |
|---|---|---|
| `execute/postgres/` | `test_db_24` | 0 (namespace only; leaf subdirectories carry their own shards) |

## PostgreSQL-unique leaves (25+)

### `execute/postgres/extension/<extension>/`

| Leaf directory | Database | Test modules |
|---|---|---|
| `execute/postgres/extension/address_standardizer/` | `test_db_25` | 1 |
| `execute/postgres/extension/bloom/` | `test_db_26` | 1 |
| `execute/postgres/extension/btree_gin/` | `test_db_27` | 1 |
| `execute/postgres/extension/btree_gist/` | `test_db_28` | 1 |
| `execute/postgres/extension/citext/` | `test_db_29` | 1 |
| `execute/postgres/extension/cube/` | `test_db_30` | 1 |
| `execute/postgres/extension/fuzzystrmatch/` | `test_db_31` | 1 |
| `execute/postgres/extension/hstore/` | `test_db_32` | 1 |
| `execute/postgres/extension/hypopg/` | `test_db_33` | 1 |
| `execute/postgres/extension/intarray/` | `test_db_34` | 1 |
| `execute/postgres/extension/orafce/` | `test_db_35` | 1 |
| `execute/postgres/extension/pg_cron/` | `test_db_36` | 1 |
| `execute/postgres/extension/pg_partman/` | `test_db_37` | 1 |
| `execute/postgres/extension/pg_repack/` | `test_db_38` | 1 |
| `execute/postgres/extension/pg_stat_statements/` | `test_db_39` | 1 |
| `execute/postgres/extension/pg_surgery/` | `test_db_40` | 1 |
| `execute/postgres/extension/pg_walinspect/` | `test_db_41` | 1 |
| `execute/postgres/extension/pgaudit/` | `test_db_42` | 1 |
| `execute/postgres/extension/pgcrypto/` | `test_db_43` | 1 |
| `execute/postgres/extension/pglogical/` | `test_db_44` | 1 |
| `execute/postgres/extension/tablefunc/` | `test_db_45` | 1 |
| `execute/postgres/extension/uuid_ossp/` | `test_db_46` | 1 |
| `execute/postgres/extension/earthdistance/` | `test_db_47` | 1 |
| `execute/postgres/extension/ltree/` | `test_db_48` | 1 |
| `execute/postgres/extension/pg_trgm/` | `test_db_49` | 1 |
| `execute/postgres/extension/pgrouting/` | `test_db_50` | 1 |
| `execute/postgres/extension/pgvector/` | `test_db_51` | 1 |
| `execute/postgres/extension/postgis/` | `test_db_52` | 1 |
| `execute/postgres/extension/postgis_raster/` | `test_db_53` | 1 |
| `execute/postgres/extension/ddl/` | `test_db_54` | 1 (CREATE/DROP EXTENSION execution) |

### `execute/postgres/types/<group>/`

| Leaf directory | Database | Test modules |
|---|---|---|
| `execute/postgres/types/bitstring/` | `test_db_55` | 1 |
| `execute/postgres/types/cast/` | `test_db_56` | 1 |
| `execute/postgres/types/enum/` | `test_db_57` | 1 |
| `execute/postgres/types/extension_types/` | `test_db_58` | 1 |
| `execute/postgres/types/literal/` | `test_db_59` | 1 |
| `execute/postgres/types/lsn/` | `test_db_60` | 1 |
| `execute/postgres/types/money/` | `test_db_61` | 1 |
| `execute/postgres/types/network/` | `test_db_62` | 1 |
| `execute/postgres/types/oid/` | `test_db_63` | 1 |
| `execute/postgres/types/range/` | `test_db_64` | 1 |
| `execute/postgres/types/xml/` | `test_db_65` | 1 |

### Other PostgreSQL-unique leaves

| Leaf directory | Database | Test modules |
|---|---|---|
| `execute/postgres/dialect_security/` | `test_db_66` | 1 |
| `execute/postgres/capability_probe/` | `test_db_67` | 1 |
