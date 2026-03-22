# src/rhosocial/activerecord/backend/impl/postgres/protocols/dml/__init__.py
"""DML-related PostgreSQL protocols."""

from .vacuum import PostgresVacuumSupport
from .stored_procedure import PostgresStoredProcedureSupport
from .extended_statistics import PostgresExtendedStatisticsSupport

__all__ = [
    'PostgresVacuumSupport',
    'PostgresStoredProcedureSupport',
    'PostgresExtendedStatisticsSupport',
]
