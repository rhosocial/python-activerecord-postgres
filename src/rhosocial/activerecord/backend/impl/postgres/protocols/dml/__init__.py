# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/__init__.py
"""DML-related PostgreSQL protocols."""

from .vacuum import PostgresVacuumSupport
from .copy import PostgresCopySupport
from .stored_procedure import PostgresStoredProcedureSupport
from .extended_statistics import PostgresExtendedStatisticsSupport
from .locking import PostgresLockingSupport

__all__ = [
    "PostgresVacuumSupport",
    "PostgresCopySupport",
    "PostgresStoredProcedureSupport",
    "PostgresExtendedStatisticsSupport",
    "PostgresLockingSupport",
]
