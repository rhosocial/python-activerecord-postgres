# src/rhosocial/activerecord/backend/impl/postgres/partition/__init__.py
"""PostgreSQL partition plan grammar.

Partition strategy lives in the backend, not on an ActiveRecord model: a
model only declares the primary-key shape a partitioned table needs. This
package describes PostgreSQL trees -- their parents, keys, policies and
generated branches -- as data. The DDL "how" is supplied by the backend's
suggestion mixin and by the DDL expression classes; live tree inspection is
supplied by the introspection layer.

The module is owner-agnostic: it never hard-codes an application's table names
or retention defaults.
"""

from .bounds import Interval, add_interval, default_label, iter_range_bounds, normalize_interval
from .plan import (
    HashBranch,
    HashPolicy,
    ListBranch,
    ListPolicy,
    PartitionPolicy,
    PartitionSpec,
    PostgresPartitionPlan,
    RangeBranch,
    RangePolicy,
)

__all__ = [
    "HashBranch",
    "HashPolicy",
    "Interval",
    "ListBranch",
    "ListPolicy",
    "PartitionPolicy",
    "PartitionSpec",
    "PostgresPartitionPlan",
    "RangeBranch",
    "RangePolicy",
    "add_interval",
    "default_label",
    "iter_range_bounds",
    "normalize_interval",
]
