# src/rhosocial/activerecord/backend/impl/postgres/mixins/catalog.py
"""Deprecated import path for the PostgreSQL naming-side mixin.

The naming-side answers used to be split: ``PostgresCatalogMixin`` held the two
outer-namespace switches and ``PostgresSchemaMixin`` held ``supports_schema_
qualification`` next to its DDL siblings. That split is now
:class:`~...mixins.namespace.PostgresNamespaceMixin`, which holds all of them
plus PostgreSQL's own ``format_qualified_name``.

This module keeps the old name importable as an alias rather than deleting it, so
an out-of-tree dialect that inherited ``PostgresCatalogMixin`` keeps working.
"""

from .namespace import PostgresNamespaceMixin

#: Alias of :class:`~...mixins.namespace.PostgresNamespaceMixin`.
#:
#: One class under two names, not two classes: a subclass here would put a second
#: definition of the same switches into ``PostgresDialect``'s MRO and the loser
#: would be whichever one happened to be listed second -- which is the failure
#: mode that produced Snowflake's duplicate ``supports_schema``.
PostgresCatalogMixin = PostgresNamespaceMixin

__all__ = ["PostgresCatalogMixin"]