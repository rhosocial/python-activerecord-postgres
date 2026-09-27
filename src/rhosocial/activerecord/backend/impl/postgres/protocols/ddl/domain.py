# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/domain.py
"""PostgreSQL DOMAIN DDL protocol."""

from typing import Protocol, Tuple, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import DomainSupport
from rhosocial.activerecord.backend.expression.statements.ddl_domain import AlterDomainExpression


__all__ = ["PostgresDomainSupport"]


@runtime_checkable
class PostgresDomainSupport(DomainSupport, Protocol):
    def format_postgres_alter_domain_statement(
        self,
        expr: AlterDomainExpression,
    ) -> Tuple[str, tuple]:
        ...
