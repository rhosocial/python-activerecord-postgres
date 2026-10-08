# src/rhosocial/activerecord/backend/impl/postgres/mixins/namespace.py
"""PostgreSQL's answer to the naming-side questions, in one place.

The naming side asks two separate things about a name: which namespace levels
PostgreSQL has (:meth:`supports_catalog`, :meth:`supports_schema_qualification`)
and how those levels are spelled (:meth:`format_qualified_name`). They used to
live in two mixins -- ``PostgresCatalogMixin`` for the outer level and
``PostgresSchemaMixin`` for the inner one -- which meant a reader had to know
that the schema *qualification* switch was filed under the schema **DDL** mixin,
next to ``CREATE SCHEMA`` support. It is a different question with a different
owner: "can this engine have schemas" is DDL, "may a name be qualified with one"
is naming, and PostgreSQL answers both the same way for once.

Converging them here is also what makes the shape explicit. PostgreSQL is a
two-level engine -- a database outside, a schema inside -- so
:meth:`format_qualified_name` is spelled out rather than inherited: catalog
first, then schema, joined by :attr:`separator`. That is not a fact core can
assume. MySQL and MariaDB have a database and no inner schema, Oracle has no
database at all, Firebird has one unnamed namespace, and only four of the ten
engines have both levels. Hardcoding "two slots, catalog on the outside, joined
by a dot" in a shared mixin happened to fit those four and had to be worked
around for the rest.

MRO note: this subclasses core's ``NamespaceMixin``, and a subclass precedes its
base, so it has to appear *before* ``NamespaceMixin`` in ``PostgresDialect``'s
base list -- after the ``*NameMixin`` block, because those subclass
``NamespaceMixin`` too and each supplies one ``format_<kind>_object``.
"""

from typing import Tuple

from rhosocial.activerecord.backend.dialect.mixins import NamespaceMixin
from rhosocial.activerecord.backend.expression.objects import SchemaObject

__all__ = ["PostgresNamespaceMixin"]


class PostgresNamespaceMixin(NamespaceMixin):
    """Spells a PostgreSQL name, and says which levels PostgreSQL has."""

    #: PostgreSQL joins namespace levels with a dot: ``app.public.users``.
    separator: str = "."

    def supports_catalog(self) -> bool:
        """PostgreSQL sits on a database above the schema.

        The slot a PostgreSQL schema object carries as ``catalog_name`` is the
        database. It is rendered when supplied -- ``db.schema.table`` is legal
        PostgreSQL -- but callers normally leave it unset, which is why this is
        not a version switch: there is no version below which a database name
        stops being legal.
        """
        return True

    def supports_catalog_qualification(self) -> bool:
        """A database-qualified name renders when the caller supplies one."""
        return True

    def supports_schema_qualification(self) -> bool:
        """A name may be qualified with its schema.

        PostgreSQL qualifies, and ``"app"."users"`` is the ordinary way to write
        a table reference. Distinct from ``supports_schema``, which asks whether
        the engine can ``CREATE SCHEMA`` at all and lives on
        :class:`~...mixins.schema.PostgresSchemaMixin`.
        """
        return True

    def validate_catalog_name(self, expr: "SchemaObject") -> None:
        """Accept any non-empty database name.

        The base schema object has already rejected empty and non-string slots,
        and PostgreSQL imposes no further database-name restriction that a schema
        object cannot express, so there is nothing left to refuse here.
        """
        return None

    def format_qualified_name(self, expr: "SchemaObject") -> Tuple[str, tuple]:
        """Build ``[database.][schema.]name``, each level quoted separately.

        PostgreSQL is two-level with the database outside, so the spelling is
        written out rather than left to core's default. ``db`` is the database, so
        it is rendered only when the caller set ``catalog_name``; PostgreSQL
        resolves a name against the connection's database otherwise, and dropping
        the level silently would name a different object than the caller asked
        for -- which is why :meth:`validate_namespace` raises instead of
        discarding a level this dialect claims to support.

        Args:
            expr: The object being named.

        Returns:
            A ``(sql, params)`` tuple. ``params`` is empty because an identifier
            is never a bind parameter.

        Raises:
            UnsupportedFeatureError: ``validate_namespace`` refused a level this
                dialect declares it can express.
        """
        self.validate_namespace(expr)
        parts = []
        if expr.catalog_name:
            parts.append(
                self.format_identifier(expr.catalog_name, expr.catalog_need_quote)
            )
        if expr.schema_name:
            parts.append(
                self.format_identifier(expr.schema_name, expr.schema_need_quote)
            )
        parts.append(self.format_identifier(expr.name, expr.name_need_quote))
        return self.separator.join(parts), ()