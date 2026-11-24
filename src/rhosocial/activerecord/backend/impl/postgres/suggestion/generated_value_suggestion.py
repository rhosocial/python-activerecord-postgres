# src/rhosocial/activerecord/backend/impl/postgres/suggestion/generated_value_suggestion.py
"""PostgreSQL's database-generated-value suggestion.

PostgreSQL has no column marker for an auto-generated key: the SERIAL pseudo-type
carries the sequence, so the storage type absorbs the semantics and there is no
clause to append. The width mapping mirrors the engine's three serial types.

This is a plain class, not a subclass of ``GeneratedValueSuggestionSupport``:
that protocol is runtime-checkable, and inheriting it ahead of the other dialect
bases would intercept ``__init__`` and leave ``strict_validation`` unset.
"""
from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import GeneratedValueSuggestion

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.types import DataType


class PostgresGeneratedValueSuggestionMixin:
    """Suggest PostgreSQL's SERIAL family for a generated integer key."""

    def suggest_generated_value(self, default_type: "DataType") -> GeneratedValueSuggestion:
        from ....expression.types import BigIntType, IntegerType, SmallIntType
        from ..expression.types import (
            PostgresBigSerialType,
            PostgresSerialType,
            PostgresSmallSerialType,
        )

        # The serial pseudo-type emits the sequence, so the framework must add
        # no clause. Precision travels in ``default_type``: pick the matching
        # serial width rather than defaulting to IDENTITY.
        for core_type, serial_type in (
            (SmallIntType, PostgresSmallSerialType),
            (IntegerType, PostgresSerialType),
            (BigIntType, PostgresBigSerialType),
        ):
            if isinstance(default_type, core_type):
                return GeneratedValueSuggestion(
                    state="embedded-in-type", storage_type=serial_type(self)
                )
        from ....dialect.exceptions import UnsupportedFeatureError

        raise UnsupportedFeatureError(
            self.name,
            "AUTO_INCREMENT column",
            "SERIAL covers smallint, integer and bigint; "
            f"{type(default_type).__name__} is none of them",
        )
