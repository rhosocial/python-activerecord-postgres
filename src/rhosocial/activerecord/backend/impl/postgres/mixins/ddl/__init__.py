# src/rhosocial/activerecord/backend/impl/postgres/mixins/ddl/__init__.py
"""DDL-related PostgreSQL mixins."""

from .partition import PostgresPartitionMixin
from .index import PostgresIndexMixin
from .trigger import PostgresTriggerMixin
from .comment import PostgresCommentMixin
from .type import PostgresTypeMixin
from .constraint import PostgresConstraintMixin

__all__ = [
    "PostgresPartitionMixin",
    "PostgresIndexMixin",
    "PostgresTriggerMixin",
    "PostgresCommentMixin",
    "PostgresTypeMixin",
    "PostgresConstraintMixin",
]
