# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/__init__.py
"""DDL-related PostgreSQL protocols."""

from .partition import PostgresPartitionSupport
from .index import PostgresIndexSupport
from .trigger import PostgresTriggerSupport
from .comment import PostgresCommentSupport
from .type import PostgresTypeSupport
from .constraint import PostgresConstraintSupport

__all__ = [
    "PostgresPartitionSupport",
    "PostgresIndexSupport",
    "PostgresTriggerSupport",
    "PostgresCommentSupport",
    "PostgresTypeSupport",
    "PostgresConstraintSupport",
]
