# src/rhosocial/activerecord/backend/impl/postgres/mixins/schema.py
"""PostgreSQL schema feature support implementation.

Everything here is the DDL side: whether the engine has schemas, and what
``CREATE``/``DROP SCHEMA`` can carry. The naming side -- whether a *name* may be
qualified with a schema, and how that qualification is spelled -- belongs to
:class:`~...mixins.namespace.PostgresNamespaceMixin`. PostgreSQL answers both
with ``True``, which is exactly why they should not be one switch: an engine can
have ``CREATE SCHEMA`` and still refuse to qualify, and reading the wrong one
gives the wrong answer with no way to tell.
"""


class PostgresSchemaMixin:
    """PostgreSQL schema DDL override implementation.

    All features are native, using version number for detection.
    """

    def supports_schema(self) -> bool:
        """PostgreSQL models named schema namespaces natively.

        A DDL-side switch: it answers whether schemas exist in the engine, which is
        what decides whether ``CREATE SCHEMA`` can be offered at all. Whether a
        name may be *qualified* with a schema is
        :meth:`~...mixins.namespace.PostgresNamespaceMixin.supports_schema_qualification`,
        and it lives with the rest of the naming-side answers.
        """
        return True

    def supports_create_schema(self) -> bool:
        return True

    def supports_drop_schema(self) -> bool:
        return True

    def supports_schema_if_not_exists(self) -> bool:
        return True

    def supports_schema_if_exists(self) -> bool:
        return True

    def supports_schema_cascade(self) -> bool:
        return True

    def supports_schema_restrict(self) -> bool:
        """``DROP SCHEMA ... RESTRICT`` is in the synopsis.

        PostgreSQL accepts ``DROP SCHEMA name [ CASCADE | RESTRICT ]``
        (measured on PostgreSQL 16); RESTRICT is the default behavior but the
        token itself is legal.
        """
        return True

    def supports_schema_authorization(self) -> bool:
        return True
