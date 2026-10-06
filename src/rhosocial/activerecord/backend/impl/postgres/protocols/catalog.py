# src/rhosocial/activerecord/backend/impl/postgres/protocols/catalog.py
"""PostgreSQL naming-side protocol: which namespace levels a name may carry.

Declared here, implemented by
:class:`~...mixins.namespace.PostgresNamespaceMixin` -- one mixin for the whole
naming side. It used to be split across two, with the schema *qualification*
switch filed under the schema DDL protocol; the two questions have different
owners and PostgreSQL happens to answer both the same way, which is the case
where conflating them hides the most.
"""

from typing import Protocol, runtime_checkable

from rhosocial.activerecord.backend.dialect.protocols import NamespaceSupport


@runtime_checkable
class PostgresNamespaceSupport(NamespaceSupport, Protocol):
    """PostgreSQL namespace-level support protocol.

    PostgreSQL's outer namespace is the *database*: ``db.schema.table`` is a
    legal three-part reference. The database is normally left implicit, but the
    namespace exists, so this backend declares it rather than reporting that
    PostgreSQL has no catalog at all. PostgreSQL is two-level with the database
    outside, which is why the mixin spells ``format_qualified_name`` itself
    rather than inheriting core's two-slot default.

    Derives from :class:`NamespaceSupport`, which is where the namespace
    questions live now that naming was split from rendering. Kept separate from
    :class:`~.schema.PostgresSchemaSupport` because the two are independent: an
    engine can have ``CREATE SCHEMA`` and still not qualify names with one.
    """

    def supports_catalog(self) -> bool: ...

    def supports_catalog_qualification(self) -> bool: ...

    def supports_schema_qualification(self) -> bool: ...

    def validate_catalog_name(self, expr) -> None: ...


#: Deprecated alias of :class:`PostgresNamespaceSupport`.
#:
#: One protocol under two names, not two protocols: a subclass here would declare
#: the same members twice and both spellings would then have to be kept in step.
#: ``supports_schema_qualification`` was added to this protocol because the
#: PostgreSQL mixin that implements it now answers that question too -- it used
#: to live on ``PostgresSchemaSupport``, which is a DDL question with a
#: different owner.
PostgresCatalogSupport = PostgresNamespaceSupport

__all__ = ["PostgresNamespaceSupport", "PostgresCatalogSupport"]