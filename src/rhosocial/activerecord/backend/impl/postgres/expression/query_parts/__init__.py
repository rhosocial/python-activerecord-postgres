# rhosocial/activerecord/backend/impl/postgres/expression/query_parts/__init__.py
"""Expression namespace: query_parts (hints)."""

import importlib

# Public name -> defining module. Private names here stay reachable
# through this package but are not part of __all__.
_EXPORTS = {
    'CorrectionKind': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'Enum': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'ForeignTable': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'HINT_FIELDS': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'HintGroup': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'HintKind': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'HintPayload': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'HintTarget': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'MaterializedView': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'NamedRelationRef': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'Optional': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'SchemaObject': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'Sequence': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'Table': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'Tuple': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'TypedDict': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'Union': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'View': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_HINT_COMMENT_END': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_REGEXP_KINDS': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_REQUIRES_TABLES': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_assert_hint_pattern': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_assert_identifier': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_is_hint_name': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_normalise_names': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_normalise_sequence_item': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    '_render_name': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'assert_hint_payload': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'cast': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'hint_payload': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'hint_target': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'make_hint_payload': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'normalise_hints': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'resolve_correction_kind': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
    'resolve_hint_kind': 'rhosocial.activerecord.backend.impl.postgres.expression.query_parts.hint',
}

__all__ = [
    'CorrectionKind',
    'Enum',
    'ForeignTable',
    'HINT_FIELDS',
    'HintGroup',
    'HintKind',
    'HintPayload',
    'HintTarget',
    'MaterializedView',
    'NamedRelationRef',
    'Optional',
    'SchemaObject',
    'Sequence',
    'Table',
    'Tuple',
    'TypedDict',
    'Union',
    'View',
    'assert_hint_payload',
    'cast',
    'hint_payload',
    'hint_target',
    'make_hint_payload',
    'normalise_hints',
    'resolve_correction_kind',
    'resolve_hint_kind',
]


def __getattr__(name):
    """Resolve a public name from this namespace lazily (no import cycles)."""
    module_name = _EXPORTS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = importlib.import_module(module_name)
    value = vars(module)[name]
    globals()[name] = value
    return value
