# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/__init__.py
"""Extension-related PostgreSQL protocols."""

from .pgvector import PostgresPgvectorSupport
from .postgis import PostgresPostGISSupport
from .hstore import PostgresHstoreSupport
from .ltree import PostgresLtreeSupport
from .intarray import PostgresIntarraySupport
from .pg_trgm import PostgresPgTrgmSupport
from .earthdistance import PostgresEarthdistanceSupport
from .tablefunc import PostgresTablefuncSupport
from .pg_stat_statements import PostgresPgStatStatementsSupport

__all__ = [
    'PostgresPgvectorSupport',
    'PostgresPostGISSupport',
    'PostgresHstoreSupport',
    'PostgresLtreeSupport',
    'PostgresIntarraySupport',
    'PostgresPgTrgmSupport',
    'PostgresEarthdistanceSupport',
    'PostgresTablefuncSupport',
    'PostgresPgStatStatementsSupport',
]
