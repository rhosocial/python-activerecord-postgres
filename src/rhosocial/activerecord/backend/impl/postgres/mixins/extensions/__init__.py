# src/rhosocial/activerecord/backend/impl/postgres/mixins/extensions/__init__.py
"""Extension-related PostgreSQL mixins."""

from .pgvector import PostgresPgvectorMixin
from .postgis import PostgresPostGISMixin
from .hstore import PostgresHstoreMixin
from .ltree import PostgresLtreeMixin
from .intarray import PostgresIntarrayMixin
from .pg_trgm import PostgresPgTrgmMixin
from .earthdistance import PostgresEarthdistanceMixin
from .tablefunc import PostgresTablefuncMixin
from .pg_stat_statements import PostgresPgStatStatementsMixin

__all__ = [
    "PostgresPgvectorMixin",
    "PostgresPostGISMixin",
    "PostgresHstoreMixin",
    "PostgresLtreeMixin",
    "PostgresIntarrayMixin",
    "PostgresPgTrgmMixin",
    "PostgresEarthdistanceMixin",
    "PostgresTablefuncMixin",
    "PostgresPgStatStatementsMixin",
]
