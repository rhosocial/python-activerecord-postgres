# postgres/protocols/policy_generated.py
"""Auto-generated protocol declarations (P7, 2026-09-01).

Functional-group principle: every public format_*/supports_* on a
backend mixin is declared here so dialect users can program against
the capability contract.  Regenerate via scripts/p7_generate_protocols.py
when mixins gain new public rendering methods.
"""

from typing import Any, Dict, List, Optional, Tuple

from typing import Protocol

class PostgresPolicySupport(Protocol):
    """Auto-generated capability protocol (P7)."""

    def format_table_ref(self, schema: Optional[str], table: str) -> str:
        ...  # pragma: no cover
    def format_role_list(self, roles: List[str]) -> Tuple[str, tuple]:
        ...  # pragma: no cover
    def format_predicate(self, expr: SQLPredicate, clause_keyword: str) -> Tuple[str, tuple]:
        ...  # pragma: no cover
