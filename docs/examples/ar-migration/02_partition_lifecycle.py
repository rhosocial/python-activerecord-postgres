# docs/examples/ar-migration/02_partition_lifecycle.py
"""Backend-specific AR migration example — PostgreSQL: partition lifecycle.

Runs fully OFFLINE: no server, no connection, no ``ctx.apply()``. As in the
build example, we use a connection-free :class:`PostgresDialect` and real
``PostgresPartitionPlan`` policy objects, then print the SQL each returned
expression renders via ``expr.to_sql()``. No SQL text is hand-written here.

PostgreSQL lifecycle answers:
  * add a leaf -> ``CREATE TABLE ... PARTITION OF ... FOR VALUES``;
  * drop a leaf -> ``DETACH PARTITION ... CONCURRENTLY`` then ``DROP TABLE``;
  * drop a whole branch -> a single cascading ``DROP TABLE``;
  * retention of a dated RANGE partition -> the same detach-then-drop chain;
  * split / merge / ALTER partitioning scheme have no mechanism — the honest
    answer is ``operations is False``.
"""

from datetime import date

from rhosocial.activerecord.backend.expression.column_types.column import Column
from rhosocial.activerecord.backend.expression.statements import (
    PartitionByHashClause,
    PartitionByListClause,
    PartitionByRangeClause,
)
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch,
    HashPolicy,
    ListPolicy,
    PostgresPartitionPlan,
    RangePolicy,
)

_CLAUSE_TYPES = {
    "RANGE": PartitionByRangeClause,
    "LIST": PartitionByListClause,
    "HASH": PartitionByHashClause,
}


def _strategy(dialect, word, *columns):
    """A strategy clause carrying ``word`` over the given key columns."""
    return _CLAUSE_TYPES[word](dialect, [Column(dialect, name) for name in columns])


def render(label, suggestion):
    """Print a suggestion's operations as SQL, or the honest 'no mechanism' answer."""
    print("== %s ==" % label)
    operations = suggestion.operations
    if operations is False:
        print("   <no PostgreSQL mechanism: operations is False>")
        return
    for operation in operations:
        print("   " + operation.to_sql()[0])


def main():
    dialect = PostgresDialect(version=(14, 0, 0))

    # A dated RANGE plan: the only plan shape retention has a mechanism for.
    orders = PostgresPartitionPlan(
        parent="orders",
        method="RANGE",
        columns=("created_at",),
        schema="public",
        policy=RangePolicy(bounds=((date(2024, 1, 1), date(2024, 2, 1)),)),
    )

    # 1) Add one partition: CREATE TABLE ... PARTITION OF ... FOR VALUES.
    render(
        "add RANGE leaf (orders_2024_01_01)",
        dialect.suggest_partition_add(_strategy(dialect, "RANGE", "created_at"), orders),
    )

    # HASH and LIST add use the same statement with different FOR VALUES forms.
    events = PostgresPartitionPlan(
        parent="events",
        method="HASH",
        columns=("tenant",),
        policy=HashPolicy(modulus=4, remainders=(0,)),
    )
    render(
        "add HASH leaf (MODULUS/REMAINDER)",
        dialect.suggest_partition_add(_strategy(dialect, "HASH", "tenant"), events),
    )

    tickets = PostgresPartitionPlan(
        parent="tickets",
        method="LIST",
        columns=("status",),
        policy=ListPolicy(groups=(("open",),)),
    )
    render(
        "add LIST leaf (FOR VALUES IN)",
        dialect.suggest_partition_add(_strategy(dialect, "LIST", "status"), tickets),
    )

    # 2) Drop a leaf: detach concurrently first, then drop the now-standalone table.
    render(
        "drop RANGE leaf (detach -> drop)",
        dialect.suggest_partition_drop(_strategy(dialect, "RANGE", "created_at"), orders),
    )

    # A branch carrying subpartitions goes through one cascading DROP instead.
    tree = PostgresPartitionPlan(
        parent="events",
        method="HASH",
        columns=("tenant",),
        modulus=2,
        branches=(
            HashBranch(0, child=RangePolicy(bounds=((date(2024, 1, 1), date(2024, 2, 1)),))),
        ),
    )
    render(
        "drop subtree (cascading DROP)",
        dialect.suggest_partition_drop(_strategy(dialect, "HASH", "tenant"), tree),
    )

    # 3) Retention: retire a dated RANGE partition by detaching then dropping it.
    render(
        "retire dated RANGE partition",
        dialect.suggest_partition_retention(_strategy(dialect, "RANGE", "created_at"), orders),
    )
    # Retention is a date-bound notion: LIST has no such mechanism.
    render(
        "retention on LIST (unsupported)",
        dialect.suggest_partition_retention(_strategy(dialect, "LIST", "status"), tickets),
    )

    # 4) Mechanisms PostgreSQL does not provide.
    render(
        "ALTER partitioning scheme (unsupported)",
        dialect.suggest_partition_table_alter(_strategy(dialect, "RANGE", "created_at"), orders),
    )
    render(
        "split partition (unsupported)",
        dialect.suggest_partition_split(_strategy(dialect, "RANGE", "created_at"), orders),
    )
    render(
        "merge partitions (unsupported)",
        dialect.suggest_partition_merge(_strategy(dialect, "RANGE", "created_at"), orders),
    )


if __name__ == "__main__":
    main()
