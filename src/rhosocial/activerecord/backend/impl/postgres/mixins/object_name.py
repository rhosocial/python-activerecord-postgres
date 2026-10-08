# src/rhosocial/activerecord/backend/impl/postgres/mixins/object_name.py
"""Rendering the names of catalogue entries core has no object kind for.

A statement now holds the object it acts on, and the dialect renders it through
one ``format_<kind>_object`` method. Core supplies those methods for every kind
it models. Two PostgreSQL catalogue entries fall outside that set: a collation
(``CREATE COLLATION``, and the right-hand side of ``COLLATE``) and an extended
statistics object (``CREATE STATISTICS``). Neither has a class in the
schema-object tree, so neither has a core formatter.

Both are ordinary schema-resident names, so both are carried by the base
:class:`~...expression.objects.SchemaObject` -- kind, catalog, schema, name --
and rendered here from the two pieces core does provide:
:meth:`~...mixins.namespace.PostgresNamespaceMixin.validate_namespace` to accept
or refuse the levels the object carries, and
:meth:`~...mixins.namespace.PostgresNamespaceMixin.format_qualified_name` to spell
them. That is exactly what a core ``*NameMixin`` does; this is the same
composition for a kind core does not model, kept in one place so the two callers
cannot drift.
"""

from rhosocial.activerecord.backend.expression.objects import SchemaObject

__all__ = ["PostgresObjectNameMixin"]


class PostgresObjectNameMixin:
    """Names a schema-resident catalogue entry that has no dedicated object kind."""

    def _format_schema_object_name(self, obj: "SchemaObject") -> str:
        """Render *obj*'s qualified name.

        Args:
            obj: The :class:`SchemaObject` carrying the name and its namespace.

        Returns:
            The quoted, optionally qualified identifier.

        Raises:
            UnsupportedFeatureError: *obj* carries a namespace level PostgreSQL
                declares it cannot express.
        """
        self.validate_namespace(obj)
        qualified_sql, _ = self.format_qualified_name(obj)
        return qualified_sql
