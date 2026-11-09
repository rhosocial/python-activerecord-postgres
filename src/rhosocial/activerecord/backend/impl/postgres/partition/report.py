# src/rhosocial/activerecord/backend/impl/postgres/partition/report.py
"""PostgreSQL's partition reports are the core contract's reports.

PostgreSQL is the backend whose report shape the contract was drawn from, so it
has no engine-specific row fields left: the tree row, the statement record, the
report envelope and the batch/paired view come from
:mod:`rhosocial.activerecord.backend.partition`. This module remains as the
import site the rest of this package already uses.
"""

from rhosocial.activerecord.backend.partition import (
    PairedDifference,
    PartitionBatchReport,
    PartitionInfo,
    PartitionOperation,
    PartitionReport,
)

__all__ = [
    "PairedDifference",
    "PartitionBatchReport",
    "PartitionInfo",
    "PartitionOperation",
    "PartitionReport",
]
