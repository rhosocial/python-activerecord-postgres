# src/rhosocial/activerecord/backend/impl/postgres/mixins/uuid.py
"""PostgreSQL UUID value expressions.

PostgreSQL can generate a UUID two ways and which one is available depends on
the server version, not on the extension state: ``gen_random_uuid()`` is
built in from 13.0, while ``uuid_generate_v4()`` only exists once the
``uuid-ossp`` extension has been installed on the server. The version is the
honest gate, so this mixin overrides generation and leaves the rest to the
core ``UUIDMixin`` table.
"""

from typing import Tuple, TYPE_CHECKING

from rhosocial.activerecord.backend.dialect.exceptions import UnsupportedFeatureError

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.expression.uuid import UUIDGenerationExpression


class PostgresUUIDMixin:
    """UUID value support for PostgreSQL.

    Casting and the nil/max constants are version-independent, so they live in
    the inherited ``UUID_SQL`` table. Generation is overridden because its SQL
    depends on the server version.
    """

    #: ``gen_random_uuid()`` is absent from this table on purpose: it is not
    #: one fixed spelling across supported servers, so
    #: :meth:`format_uuid_generation` resolves it per version instead.
    UUID_SQL = {
        "constant": {
            "nil": "'00000000-0000-0000-0000-000000000000'::uuid",
            "max": "'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid",
        },
        "cast": "{inner}::uuid",
    }

    UUID_SUGGESTIONS = {
        "generation": (
            "Generate the value in Python instead — `field/uuid.py` "
            "(`UUIDMixin`) already does this with `uuid.uuid4()` and needs no "
            "database function."
        ),
        "constant": (
            "Compare against a literal instead — the nil UUID is "
            "'00000000-0000-0000-0000-000000000000' and the max UUID is "
            "'ffffffff-ffff-ffff-ffff-ffffffffffff'."
        ),
        "cast": (
            "Cast in Python instead — `uuid.UUID(value)` validates the text "
            "and raises ValueError on malformed input."
        ),
    }

    def supports_uuid_generation(self) -> bool:
        """Whether the server can generate a UUID.

        True when either route works: 13.0+, where ``gen_random_uuid()`` is a
        core function, or a server with the ``uuid-ossp`` extension installed.

        Both routes are folded into this one answer on purpose. The name
        ``supports_uuid_generation`` used to mean two different things — "can
        the database produce a UUID" here and "is uuid-ossp installed" in
        :class:`PostgresUuidOssMixin` — so the MRO decided which question got
        answered. There is one question, and this is its answer; the narrower
        "is the extension installed" question is
        ``supports_uuid_ossp_extension``.
        """
        if self.version >= (13, 0, 0):
            return True
        return self.supports_uuid_ossp_extension()

    def format_uuid_generation(
        self, expr: "UUIDGenerationExpression"
    ) -> Tuple[str, Tuple]:
        """Render the version-appropriate UUID generator.

        Args:
            expr: The UUIDGenerationExpression node.

        Returns:
            Tuple of (SQL string, parameters tuple).

        Raises:
            UnsupportedFeatureError: Before 13.0 without ``uuid-ossp``.
        """
        if self.version >= (13, 0, 0):
            sql = "gen_random_uuid()"
        elif self.supports_uuid_ossp_extension():
            sql = "uuid_generate_v4()"
        else:
            raise UnsupportedFeatureError(
                self.name,
                "UUID generation",
                "This PostgreSQL is older than 13.0, so gen_random_uuid() is "
                "not available. Install the uuid-ossp extension to use "
                "uuid_generate_v4(), or generate the value in Python with "
                "`field/uuid.py` (`UUIDMixin`), which needs no extension.",
            )

        if getattr(expr, "alias", None):
            sql = f"{sql} AS {self.format_identifier(expr.alias)}"
        return sql, ()


__all__ = ["PostgresUUIDMixin"]
