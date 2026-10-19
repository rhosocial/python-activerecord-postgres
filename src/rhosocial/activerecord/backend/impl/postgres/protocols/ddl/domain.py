# src/rhosocial/activerecord/backend/impl/postgres/protocols/ddl/domain.py
"""PostgreSQL DOMAIN DDL protocol."""

from typing import Protocol, Tuple, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import (
    AlterDomainSupport,
    CreateDomainSupport,
    DropDomainSupport,
)
from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.statements.ddl_domain import AlterDomainStatement
from rhosocial.activerecord.backend.expression.statements.ddl_domain import DomainAlterAction


__all__ = ["PostgresDomainSupport"]


@runtime_checkable
class PostgresDomainSupport(
    CreateDomainSupport,
    AlterDomainSupport,
    DropDomainSupport,
    Protocol,
):
    def format_postgres_alter_domain_statement(
        self,
        expr: AlterDomainStatement,
    ) -> Tuple[str, tuple]:
        ...

    # The two formatters below serve this backend's own private nodes: the
    # legacy ``sql=`` argument still hands an expression that carries nothing
    # but raw SQL, and the ALTER DOMAIN action words PostgreSQL spells that no
    # generic ``DomainAlterAction`` kind carries. A node finds its dialect
    # member through ``format_method``, so a formatter the mixin implements has
    # to be a member the protocol declares (§7.1.1: a protocol is the
    # declaration of the surface its mixin serves).

    def format_postgres_legacy_sql_expression(
        self,
        expr: BaseExpression,
    ) -> Tuple[str, tuple]:
        ...

    def format_postgres_unsupported_domain_action(
        self,
        expr: DomainAlterAction,
    ) -> Tuple[str, tuple]:
        ...
