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
from .pg_cron import PostgresPgCronSupport
from .citext import PostgresCitextSupport
from .bloom import PostgresBloomSupport
from .pg_partman import PostgresPgPartmanSupport
from .orafce import PostgresOrafceSupport
from .pglogical import PostgresPgLogicalSupport
from .pgaudit import PostgresPgauditSupport
from .pg_repack import PostgresPgRepackSupport
from .hypopg import PostgresHypoPgSupport
from .address_standardizer import PostgresAddressStandardizerSupport
from .pg_surgery import PostgresPgSurgerySupport
from .pg_walinspect import PostgresPgWalinspectSupport
from .fuzzystrmatch import PostgresFuzzystrmatchSupport
from .cube import PostgresCubeSupport
from .pgcrypto import PostgresPgcryptoSupport
from .btree_gin import PostgresBtreeGinSupport
from .btree_gist import PostgresBtreeGistSupport
from .uuid_ossp import PostgresUuidOssSupport
from .postgis_raster import PostgresPostgisRasterSupport
from .pgrouting import PostgresPgroutingSupport

__all__ = [
    "PostgresPgvectorSupport",
    "PostgresPostGISSupport",
    "PostgresHstoreSupport",
    "PostgresLtreeSupport",
    "PostgresIntarraySupport",
    "PostgresPgTrgmSupport",
    "PostgresEarthdistanceSupport",
    "PostgresTablefuncSupport",
    "PostgresPgStatStatementsSupport",
    "PostgresPgCronSupport",
    "PostgresCitextSupport",
    "PostgresBloomSupport",
    "PostgresPgPartmanSupport",
    "PostgresOrafceSupport",
    "PostgresPgLogicalSupport",
    "PostgresPgauditSupport",
    "PostgresPgRepackSupport",
    "PostgresHypoPgSupport",
    "PostgresAddressStandardizerSupport",
    "PostgresPgSurgerySupport",
    "PostgresPgWalinspectSupport",
    "PostgresFuzzystrmatchSupport",
    "PostgresCubeSupport",
    "PostgresPgcryptoSupport",
    "PostgresBtreeGinSupport",
    "PostgresBtreeGistSupport",
    "PostgresUuidOssSupport",
    "PostgresPostgisRasterSupport",
    "PostgresPgroutingSupport",
]
