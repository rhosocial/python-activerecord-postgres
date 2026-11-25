# src/rhosocial/activerecord/backend/impl/postgres/suggestion/__init__.py
"""PostgreSQL backend-suggestion implementations.

Suggestions are not expressions: the immutable suggestion values live in
``rhosocial.activerecord.backend.suggestion``. This package holds PostgreSQL's
concrete answer mixins -- which the dialect installs ahead of the core mixins
so no portable default can shadow them. The eight partition answer mixins each
live in their own module and share the helpers on
:class:`PostgresPartitionSuggestionBase`; the four migration change-point
mixins (column reorder, constraint/index change, partition scheme change) answer
the frozen points added by core's change planner.
"""

from ._common import PostgresPartitionSuggestionBase
from .column_reorder_suggestion import PostgresColumnReorderSuggestionMixin
from .data_type_suggestions import (
    POSTGRES_DATA_TYPE_SUGGESTIONS,
    POSTGRES_EXTRA_DATA_TYPE_SUGGESTIONS,
    PostgresDataTypeSuggestionsMixin,
)
from .general_suggestions import (
    PostgresCreateTableLayoutSuggestionMixin,
    PostgresStringConcatenationSuggestionMixin,
)
from .generated_value_suggestion import PostgresGeneratedValueSuggestionMixin
from .partition_add_suggestion import PostgresPartitionAddSuggestionMixin
from .partition_build_suggestion import PostgresPartitionBuildSuggestionMixin
from .partition_drop_suggestion import PostgresPartitionDropSuggestionMixin
from .partition_merge_suggestion import PostgresPartitionMergeSuggestionMixin
from .partition_retention_suggestion import PostgresPartitionRetentionSuggestionMixin
from .partition_split_suggestion import PostgresPartitionSplitSuggestionMixin
from .partition_table_alter_suggestion import PostgresPartitionTableAlterSuggestionMixin
from .partition_table_drop_suggestion import PostgresPartitionTableDropSuggestionMixin
from .partition_scheme_change_suggestion import PostgresPartitionSchemeChangeSuggestionMixin
from .table_constraint_change_suggestion import PostgresTableConstraintChangeSuggestionMixin
from .table_index_change_suggestion import PostgresTableIndexChangeSuggestionMixin

__all__ = [
    "POSTGRES_DATA_TYPE_SUGGESTIONS",
    "POSTGRES_EXTRA_DATA_TYPE_SUGGESTIONS",
    "PostgresColumnReorderSuggestionMixin",
    "PostgresDataTypeSuggestionsMixin",
    "PostgresCreateTableLayoutSuggestionMixin",
    "PostgresStringConcatenationSuggestionMixin",
    "PostgresGeneratedValueSuggestionMixin",
    "PostgresPartitionSuggestionBase",
    "PostgresPartitionAddSuggestionMixin",
    "PostgresPartitionBuildSuggestionMixin",
    "PostgresPartitionDropSuggestionMixin",
    "PostgresPartitionMergeSuggestionMixin",
    "PostgresPartitionRetentionSuggestionMixin",
    "PostgresPartitionSplitSuggestionMixin",
    "PostgresPartitionTableAlterSuggestionMixin",
    "PostgresPartitionTableDropSuggestionMixin",
    "PostgresPartitionSchemeChangeSuggestionMixin",
    "PostgresTableConstraintChangeSuggestionMixin",
    "PostgresTableIndexChangeSuggestionMixin",
]
