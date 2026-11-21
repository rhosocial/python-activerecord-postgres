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

from rhosocial.activerecord.backend.expression.statements import PartitionStrategy
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect
from rhosocial.activerecord.backend.impl.postgres.partition import (
    HashBranch,
    HashPolicy,
    ListPolicy,
    PostgresPartitionPlan,
    RangePolicy,
)


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
        dialect.suggest_partition_add(PartitionStrategy.RANGE, orders),
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
        dialect.suggest_partition_add(PartitionStrategy.HASH, events),
    )

    tickets = PostgresPartitionPlan(
        parent="tickets",
        method="LIST",
        columns=("status",),
        policy=ListPolicy(groups=(("open",),)),
    )
    render(
        "add LIST leaf (FOR VALUES IN)",
        dialect.suggest_partition_add(PartitionStrategy.LIST, tickets),
    )

    # 2) Drop a leaf: detach concurrently first, then drop the now-standalone table.
    render(
        "drop RANGE leaf (detach -> drop)",
        dialect.suggest_partition_drop(PartitionStrategy.RANGE, orders),
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
        dialect.suggest_partition_drop(PartitionStrategy.HASH, tree),
    )

    # 3) Retention: retire a dated RANGE partition by detaching then dropping it.
    render(
        "retire dated RANGE partition",
        dialect.suggest_partition_retention(PartitionStrategy.RANGE, orders),
    )
    # Retention is a date-bound notion: LIST has no such mechanism.
    render(
        "retention on LIST (unsupported)",
        dialect.suggest_partition_retention(PartitionStrategy.LIST, tickets),
    )

    # 4) Mechanisms PostgreSQL does not provide.
    render(
        "ALTER partitioning scheme (unsupported)",
        dialect.suggest_partition_table_alter(PartitionStrategy.RANGE, orders),
    )
    render(
        "split partition (unsupported)",
        dialect.suggest_partition_split(PartitionStrategy.RANGE, orders),
    )
    render(
        "merge partitions (unsupported)",
        dialect.suggest_partition_merge(PartitionStrategy.RANGE, orders),
    )


if __name__ == "__main__":
    main()
