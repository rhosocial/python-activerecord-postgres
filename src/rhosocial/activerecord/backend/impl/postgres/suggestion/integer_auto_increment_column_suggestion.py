# src/rhosocial/activerecord/backend/impl/postgres/suggestion/integer_auto_increment_column_suggestion.py
"""PostgresIntegerAutoIncrementColumnSuggestionMixin -- postgres's integer auto-increment column suggestion.

The answer is the backend's own spelling, assembled from its capability
probes where it has one. It is placed first in the dialect bases so no core
mixin can shadow it, and it renders nothing itself -- the returned clause's
formatter owns the SQL spelling.
"""

from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.suggestion import IntegerAutoIncrementColumnSuggestion

if TYPE_CHECKING:  # pragma: no cover
    from ....expression.types._base import DataType


class PostgresIntegerAutoIncrementColumnSuggestionMixin:
    """Suggest postgres's declaration for a generated integer key."""

    def suggest_integer_auto_increment_column(
        self, default_type: "DataType"
    ) -> IntegerAutoIncrementColumnSuggestion:
        from ....expression.types import BigIntType, IntegerType, SmallIntType
        from ..expression.types import (
            PostgresBigSerialType,
            PostgresSerialType,
            PostgresSmallSerialType,
        )

        # PostgreSQL has no column marker: the SERIAL pseudo-type carries the
        # sequence, so auto_increment=False tells the framework to add no
        # clause. The width mapping mirrors the engine's three serial types.
        for core_type, serial_type in (
            (SmallIntType, PostgresSmallSerialType),
            (IntegerType, PostgresSerialType),
            (BigIntType, PostgresBigSerialType),
        ):
            if isinstance(default_type, core_type):
                return IntegerAutoIncrementColumnSuggestion(serial_type(self), False)
        from ....dialect.exceptions import UnsupportedFeatureError

        raise UnsupportedFeatureError(
            self.name,
            "AUTO_INCREMENT column",
            "SERIAL covers smallint, integer and bigint; "
            f"{type(default_type).__name__} is none of them",
        )
