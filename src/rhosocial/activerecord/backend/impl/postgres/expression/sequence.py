# src/rhosocial/activerecord/backend/impl/postgres/expression/sequence.py
"""PostgreSQL sequence value expressions.

``PostgresSequenceValueExpression`` renders ``nextval('seq')`` /
``currval('seq')`` as an inline SQL literal expression. Sequence names in
DDL contexts (e.g. ``DEFAULT nextval('seq')``) cannot be bind parameters,
so the dialect formatter inlines the safely-quoted identifier — the same
convention the Oracle backend applies to ``seq.NEXTVAL``.
"""

from enum import Enum
from typing import TYPE_CHECKING

from rhosocial.activerecord.backend.expression.bases import (
    BaseExpression,
    SQLQueryAndParams,
)

if TYPE_CHECKING:  # pragma: no cover
    from rhosocial.activerecord.backend.impl.postgres.dialect import PostgresDialect


class PostgresSequenceValueMode(Enum):
    """Access mode for a PostgreSQL sequence value expression."""

    NEXTVAL = "NEXTVAL"
    CURRVAL = "CURRVAL"


class PostgresSequenceValueExpression(BaseExpression):
    """PostgreSQL ``nextval('seq')`` / ``currval('seq')`` value expression.

    Args:
        dialect: the PostgreSQL dialect instance.
        sequence: the sequence name (unquoted; formatted by the dialect).
        mode: access mode; ``NEXTVAL`` (default) or ``CURRVAL``.

    Raises:
        ValueError: if ``sequence`` is empty.
        TypeError: if ``mode`` is not a ``PostgresSequenceValueMode``.
    """

    def __init__(
        self,
        dialect: "PostgresDialect",
        sequence: str,
        mode: PostgresSequenceValueMode = PostgresSequenceValueMode.NEXTVAL,
    ):
        super().__init__(dialect)
        if not isinstance(sequence, str) or not sequence.strip():
            raise ValueError("sequence must be a non-empty string")
        if not isinstance(mode, PostgresSequenceValueMode):
            raise TypeError(
                "mode must be a PostgresSequenceValueMode value, "
                f"got {type(mode).__name__}"
            )
        self.sequence = sequence
        self.mode = mode

    def to_sql(self) -> SQLQueryAndParams:
        if self.mode is PostgresSequenceValueMode.NEXTVAL:
            return self.dialect.format_nextval(self)
        return self.dialect.format_currval(self)


__all__ = [
    "PostgresSequenceValueMode",
    "PostgresSequenceValueExpression",
]
