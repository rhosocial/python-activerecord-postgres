# tests/rhosocial/activerecord_postgres_test/feature/query/cross_schema/test_qualifiers.py
"""PostgreSQL's rules for schema-qualified column references.

Verified against a live server:

==========================================  ==========================  ======
FROM range                                   Column reference            Result
==========================================  ==========================  ======
``"ar_crm"."orders"``   (no alias)          ``"ar_crm"."orders"."c"``  OK
``"ar_crm"."orders"``   (no alias)          ``"orders"."c"``            OK
``"ar_crm"."orders" AS "o"``                ``"ar_crm"."orders"."c"``  ERROR
``"ar_crm"."orders" AS "o"``                ``"o"."c"``                 OK
``"ar_crm"."orders" AS "o"``                ``"orders"."c"``            ERROR
==========================================  ==========================  ======

An unaliased range may be referenced either way; an aliased range must be
referenced by its alias alone. "Always two-part" is therefore not a
self-consistent rule -- it is wrong exactly when an alias is present.

The suppression for aliased ranges happens when the column expression is
*constructed*: ``FieldProxy`` sets ``schema_name`` to ``None`` as soon as a
table alias is in effect. ``PostgresColumnMixin.format_column`` is not what
enforces it -- that override inspects the *column* alias, which is unset on
this path -- so a hand-built ``Column`` that bypasses ``FieldProxy`` can emit
SQL the server rejects. The last test pins that boundary.
"""

import re


from rhosocial.activerecord.backend.expression.core import Column
from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


def _norm(sql: str) -> str:
    """Strip identifier quotes and fold case/space, as the testsuite helper does."""
    cleaned = sql.replace('"', "").replace("`", "").replace("[", "").replace("]", "")
    return re.sub(r"\s+", " ", cleaned).lower()


class TestUnaliasedRange:
    def test_range_is_namespace_qualified(self, pg_mixed_schema):
        _, _, MixedSchemaOrder = pg_mixed_schema
        sql, _ = MixedSchemaOrder.query().select(MixedSchemaOrder.c.order_number).to_sql()

        assert "from ar_crm.orders" in _norm(sql), (
            f"Expected a qualified range, got: {sql}"
        )

    def test_column_resolves_against_qualified_range(self, pg_mixed_schema):
        _, _, MixedSchemaOrder = pg_mixed_schema
        sql, _ = MixedSchemaOrder.query().select(MixedSchemaOrder.c.order_number).to_sql()

        assert "orders.order_number" in _norm(sql), (
            f"Expected a range-qualified column, got: {sql}"
        )


class TestAliasedRange:
    """The one case PostgreSQL rejects outright."""

    def test_aliased_schema_range_uses_alias_only(self, pg_mixed_schema):
        User, _, MixedSchemaOrder = pg_mixed_schema

        aliased = MixedSchemaOrder.c.with_table_alias("o")
        sql, _ = (
            User.query()
            .join(MixedSchemaOrder, on=aliased.user_id == User.c.id, alias="o")
            .select(aliased.order_number)
            .to_sql()
        )
        normed = _norm(sql)

        assert "join ar_crm.orders as o" in normed, (
            f"Expected an aliased qualified range, got: {sql}"
        )
        # The three-part form is exactly what PostgreSQL rejects here.
        assert "ar_crm.o." not in normed, (
            f"Aliased range must not produce schema-qualified columns: {sql}"
        )
        assert "o.order_number" in normed, (
            f"Expected the alias to qualify the column: {sql}"
        )


class TestCrossSchemaJoin:
    def test_join_binds_both_sides(self, pg_mixed_schema):
        User, _, MixedSchemaOrder = pg_mixed_schema

        sql, _ = (
            MixedSchemaOrder.query()
            .join(User, on=MixedSchemaOrder.c.user_id == User.c.id)
            .select(MixedSchemaOrder.c.order_number)
            .where(User.c.username == "nobody")
            .to_sql()
        )
        normed = _norm(sql)

        assert "from ar_crm.orders" in normed
        assert "orders.user_id = users.id" in normed, f"Got: {sql}"
        assert "users.username" in normed, f"Got: {sql}"


class TestHandBuiltColumnBoundary:
    """``FieldProxy`` is the guard; a hand-built ``Column`` bypasses it."""

    def test_hand_built_three_part_alias_is_what_the_server_rejects(self):
        d = PostgresDialect()
        # This is the shape PostgreSQL refuses for an aliased range. Nothing in
        # the dialect rewrites it, so the caller must not build it.
        assert Column(d, "id", table="o", schema_name="ar_crm").to_sql()[0] == (
            '"ar_crm"."o"."id"'
        )

    def test_field_proxy_form_is_two_part(self):
        """What ``with_table_alias`` actually emits for the same column."""
        d = PostgresDialect()
        assert Column(d, "id", table="o", schema_name=None).to_sql()[0] == '"o"."id"'
