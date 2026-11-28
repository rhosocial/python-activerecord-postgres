# docs/examples/ar-migration/01_partition_build.py
"""Backend-specific AR migration example — PostgreSQL: building a partition tree.

Runs fully OFFLINE: no server, no connection, no ``ctx.apply()``. We construct a
connection-free :class:`PostgresDialect` and a real :class:`PostgresPartitionPlan`
(policy objects from ``partition/plan.py``, *not* a mapping), ask the dialect for
its partition suggestions, and print the SQL each returned expression renders
via ``expr.to_sql()``. No SQL text is hand-written here.

PostgreSQL's contribution at CREATE TABLE time is a single ``PARTITION BY``
clause — there are no inline named partitions. The same intent rendered on this
backend therefore yields a clause naming the method and the key columns; the
whole parent is removed later with one ``DROP TABLE``.

Scenarios in this script (see README for the full map):
  * build a RANGE / HASH / LIST partitioned parent;
  * an unsupported strategy (KEY) has no PostgreSQL mechanism;
  * drop the whole partitioned parent with one statement.
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
    # Connection-free dialect. Version 14+ enables DETACH ... CONCURRENTLY,
    # which the lifecycle example relies on.
    dialect = PostgresDialect(version=(14, 0, 0))

    # PostgreSQL accepts ONLY a real PostgresPartitionPlan here, built from a
    # policy object (partition/plan.py); a plain dict is not the contract.
    orders = PostgresPartitionPlan(
        parent="orders",
        method="RANGE",
        columns=("created_at",),
        schema="public",
        policy=RangePolicy(bounds=((date(2024, 1, 1), date(2024, 2, 1)),)),
    )

    # 1) The parent's PARTITION BY clause — the entire "build" operation.
    render(
        "build RANGE parent (orders)",
        dialect.suggest_partition_build(_strategy(dialect, "RANGE", "created_at"), orders),
    )

    # Same intent, different method/key: HASH over a tenant column.
    events = PostgresPartitionPlan(
        parent="events",
        method="HASH",
        columns=("tenant",),
        policy=HashPolicy(modulus=4, remainders=(0,)),
    )
    render(
        "build HASH parent (events)",
        dialect.suggest_partition_build(_strategy(dialect, "HASH", "tenant"), events),
    )

    # LIST over a status column.
    tickets = PostgresPartitionPlan(
        parent="tickets",
        method="LIST",
        columns=("status",),
        policy=ListPolicy(groups=(("open",),)),
    )
    render(
        "build LIST parent (tickets)",
        dialect.suggest_partition_build(_strategy(dialect, "LIST", "status"), tickets),
    )

    # 2) A strategy PostgreSQL cannot spell (KEY) -> definite "no mechanism".
    render(
        "build with unsupported strategy KEY",
        dialect.suggest_partition_build("KEY", orders),
    )

    # 3) Removing the whole partitioned parent is one DROP TABLE.
    render(
        "drop whole parent table",
        dialect.suggest_partition_table_drop(_strategy(dialect, "RANGE", "created_at"), orders),
    )


if __name__ == "__main__":
    main()
