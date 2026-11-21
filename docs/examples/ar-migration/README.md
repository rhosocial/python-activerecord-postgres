# PostgreSQL-specific AR migration examples

Offline (dry-run) rendering of the ActiveRecord-layer partition suggestions for the
PostgreSQL backend. This complements the portable examples in the core repo's
`docs/examples/ar-migration/core/` and shows how the **same migration intent**
produces PostgreSQL's own SQL spelling.

Everything here runs **without a database**: a connection-free `PostgresDialect`
is constructed directly, a suggestion is requested, and each returned expression
is printed via `expr.to_sql()`. No SQL text is hand-written and `ctx.apply()` is
never called (that would require a live backend connection).

## PostgreSQL differences worth noting

- **Plan is an object, not a mapping.** PostgreSQL accepts only a real
  `PostgresPartitionPlan` carrying policy objects from `partition/plan.py` —
  e.g. `RangePolicy(bounds=((date(...), date(...)),))`, `HashPolicy`,
  `ListPolicy` — plus a core `PartitionStrategy` constant. A plain `dict` is not
  the contract here (other backends do accept mappings).
- **Build = one `PARTITION BY` clause.** There are no inline named partitions;
  the parent's contribution at `CREATE TABLE` time is just
  `PARTITION BY <RANGE|LIST|HASH> (<keys>)`.
- **Add = `CREATE TABLE ... PARTITION OF ... FOR VALUES`.** RANGE renders
  `FROM (...) TO (...)`, HASH renders `WITH (MODULUS n, REMAINDER m)`, LIST
  renders `IN (...)`.
- **Drop a leaf = detach then drop.** On PostgreSQL 14+ this is
  `ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY` followed by `DROP TABLE`.
  Dropping a whole branch is a single cascading `DROP TABLE ... CASCADE`.
- **Retention is date-bound.** Only a RANGE partition with a concrete
  date/datetime bound has a mechanism (the same detach-then-drop chain); LIST
  and HASH retention report `operations is False`.
- **No in-place re-partitioning.** `table_alter`, `split` and `merge` all report
  `operations is False` — changing a partitioned table's scheme is a rebuild.
- **Version gate.** Use `PostgresDialect(version=(14, 0, 0))`: on older servers
  `DETACH ... CONCURRENTLY` is unavailable and the drop chain is just one
  `DROP TABLE`.

## Scripts

| Script | Scenarios shown |
|---|---|
| `01_partition_build.py` | Build a RANGE / HASH / LIST parent (`PARTITION BY`); unsupported strategy (KEY) → no mechanism; drop the whole parent (`DROP TABLE`) |
| `02_partition_lifecycle.py` | Add RANGE / HASH / LIST leaves (`CREATE TABLE ... PARTITION OF`); drop a leaf (detach → drop) and a whole subtree (`CASCADE`); dated RANGE retention and unsupported LIST retention; the no-mechanism trio (table_alter / split / merge) |

## Running

From this repository's root (worktree), with the worktree virtualenv:

```powershell
$env:PYTHONPATH = 'src'
.\.venv3.14\Scripts\python.exe docs\examples\ar-migration\01_partition_build.py
.\.venv3.14\Scripts\python.exe docs\examples\ar-migration\02_partition_lifecycle.py
```

Both scripts are pure offline rendering: no server is contacted and no files are
written. A successful run exits 0 and prints only SQL produced by `to_sql()`
(plus descriptive scenario labels).
