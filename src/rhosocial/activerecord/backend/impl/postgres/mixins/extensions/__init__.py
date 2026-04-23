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
from .pg_cron import PostgresPgCronMixin
from .citext import PostgresCitextMixin
from .bloom import PostgresBloomMixin
from .pg_partman import PostgresPgPartmanMixin
from .orafce import PostgresOrafceMixin
from .pglogical import PostgresPgLogicalMixin
from .pgaudit import PostgresPgauditMixin
from .pg_repack import PostgresPgRepackMixin
from .hypopg import PostgresHypoPgMixin
from .address_standardizer import PostgresAddressStandardizerMixin
from .pg_surgery import PostgresPgSurgeryMixin
from .pg_walinspect import PostgresPgWalinspectMixin
from .fuzzystrmatch import PostgresFuzzystrmatchMixin
from .cube import PostgresCubeMixin
from .pgcrypto import PostgresPgcryptoMixin
from .btree_gin import PostgresBtreeGinMixin
from .btree_gist import PostgresBtreeGistMixin
from .uuid_ossp import PostgresUuidOssMixin

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
    "PostgresPgCronMixin",
    "PostgresCitextMixin",
    "PostgresBloomMixin",
    "PostgresPgPartmanMixin",
    "PostgresOrafceMixin",
    "PostgresPgLogicalMixin",
    "PostgresPgauditMixin",
    "PostgresPgRepackMixin",
    "PostgresHypoPgMixin",
    "PostgresAddressStandardizerMixin",
    "PostgresPgSurgeryMixin",
    "PostgresPgWalinspectMixin",
    "PostgresFuzzystrmatchMixin",
    "PostgresCubeMixin",
    "PostgresPgcryptoMixin",
    "PostgresBtreeGinMixin",
    "PostgresBtreeGistMixin",
    "PostgresUuidOssMixin",
]
