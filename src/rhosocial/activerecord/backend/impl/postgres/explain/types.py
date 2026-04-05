# src/rhosocial/activerecord/backend/impl/postgres/explain/types.py
"""
PostgreSQL-specific EXPLAIN result types.

PostgreSQL's EXPLAIN output (default TEXT format) produces one row per
plan line, each row having a single column named ``QUERY PLAN``.  The
text in that column uses indentation and keywords like ``Seq Scan``,
``Index Scan``, and ``Index Only Scan`` to describe the query plan.
"""
from typing import List

from pydantic import BaseModel

from rhosocial.activerecord.backend.explain.types import BaseExplainResult

try:
    from typing import Literal
except ImportError:  # Python 3.8
    from typing_extensions import Literal  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Index-usage label (mirrors SQLite / MySQL for a consistent API)
# ---------------------------------------------------------------------------
IndexUsage = Literal["full_scan", "index_with_lookup", "covering_index", "unknown"]


class PostgresExplainPlanLine(BaseModel):
    """One line from PostgreSQL's plain-text EXPLAIN output.

    The column returned by psycopg is named ``"QUERY PLAN"`` (case-sensitive).
    The field is aliased as ``line`` for convenience.
    """

    line: str

    model_config = {"populate_by_name": True}


class PostgresExplainResult(BaseExplainResult):
    """PostgreSQL EXPLAIN result (default TEXT format).

    Contains index-usage analysis helpers that mirror the SQLite and MySQL
    equivalents for a consistent cross-backend API.

    Analysis is performed via text-matching on the plan lines, looking for
    PostgreSQL-specific keywords:
    - ``Seq Scan``       → full table scan
    - ``Index Scan``     → index lookup with table access
    - ``Index Only Scan`` → covering index (no heap access)
    - ``Bitmap`` patterns → treated as index_with_lookup
    """

    rows: List[PostgresExplainPlanLine] = []

    # ------------------------------------------------------------------
    # Index-usage analysis
    # ------------------------------------------------------------------

    def analyze_index_usage(self) -> IndexUsage:
        """Analyse index usage from the EXPLAIN output lines.

        Returns one of: ``"full_scan"``, ``"index_with_lookup"``,
        ``"covering_index"``, or ``"unknown"``.
        """
        if not self.rows:
            return "unknown"

        combined = " ".join(r.line for r in self.rows).upper()

        # Covering index is the most specific — check first
        if "INDEX ONLY SCAN" in combined:
            return "covering_index"

        # Any index scan (with heap access)
        if "INDEX SCAN" in combined or "BITMAP INDEX SCAN" in combined or "BITMAP HEAP SCAN" in combined:
            return "index_with_lookup"

        # Full sequential scan
        if "SEQ SCAN" in combined:
            return "full_scan"

        return "unknown"

    @property
    def is_full_scan(self) -> bool:
        """``True`` when no index is used (sequential scan)."""
        return self.analyze_index_usage() == "full_scan"

    @property
    def is_index_used(self) -> bool:
        """``True`` when any index is used."""
        return self.analyze_index_usage() in ("index_with_lookup", "covering_index")

    @property
    def is_covering_index(self) -> bool:
        """``True`` when an index-only scan eliminates heap access."""
        return self.analyze_index_usage() == "covering_index"
