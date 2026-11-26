# src/rhosocial/activerecord/backend/impl/postgres/protocols/extensions/pg_hint_plan.py
"""pg_hint_plan extension protocol definition."""

from typing import Iterable, Protocol, runtime_checkable


@runtime_checkable
class PostgresPgHintPlanSupport(Protocol):
    """pg_hint_plan optimizer hint support.

    Extension Information:
    - Extension name: pg_hint_plan
    - Install command: CREATE EXTENSION pg_hint_plan;
    - Documentation: https://github.com/ossc-db/pg_hint_plan
    """

    def supports_pg_hint_plan(self) -> bool:
        """Whether optimizer hints via ``/*+ ... */`` are available."""
        ...

    def build_hinted_query(self, query, hints: Iterable[str]):
        """Prefix a statement expression with a validated hint comment."""
        ...
