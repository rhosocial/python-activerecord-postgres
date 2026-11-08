# src/rhosocial/activerecord/backend/impl/postgres/partition/__init__.py
"""PostgreSQL partition plans and maintenance.

Partition strategy lives in the backend, not on an ActiveRecord model: a
model only declares the primary-key shape a partitioned table needs. This
package describes PostgreSQL trees and maintains them:

* :class:`PostgresPartitionPlan` describes a parent, its key, and either one
  generated policy or explicit first-level branches with their own policies.
* :class:`PostgresPartitionMaintenance` and
  :class:`AsyncPostgresPartitionMaintenance` reconcile that description with
  the live catalog (``ensure`` / ``list`` / ``drop_before`` / ``drop_branch`` /
  ``validate``).

The module is owner-agnostic: it never hard-codes an application's table names
or retention defaults.
"""

from .bounds import Interval, add_interval, default_label, iter_range_bounds, normalize_interval
from .maintenance import (
    AsyncPostgresPartitionMaintenance,
    PostgresPartitionMaintenance,
)
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
from .report import (
    PairedDifference,
    PartitionBatchReport,
    PartitionInfo,
    PartitionOperation,
    PartitionReport,
)

__all__ = [
    "AsyncPostgresPartitionMaintenance",
    "HashBranch",
    "HashPolicy",
    "Interval",
    "ListBranch",
    "ListPolicy",
    "PairedDifference",
    "PartitionBatchReport",
    "PartitionInfo",
    "PartitionOperation",
    "PartitionPolicy",
    "PartitionReport",
    "PartitionSpec",
    "PostgresPartitionMaintenance",
    "PostgresPartitionPlan",
    "RangeBranch",
    "RangePolicy",
    "add_interval",
    "default_label",
    "iter_range_bounds",
    "normalize_interval",
]
