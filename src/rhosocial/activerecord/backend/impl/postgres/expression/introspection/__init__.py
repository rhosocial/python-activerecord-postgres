# rhosocial/activerecord/backend/impl/postgres/expression/introspection/__init__.py
"""Expression namespace: introspection (catalog / server query nodes)."""

import importlib

# Public name -> defining module. Private names here stay reachable
# through this package but are not part of __all__.
_EXPORTS = {
    'BaseExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.enum_',
    'IntrospectionExpression': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'List': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'Optional': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.enum_',
    'PostgresActiveReplicationCountQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresActivityCountQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresArchiveStatsQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresCheckpointCountQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresCheckpointLsnQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresClientAddressQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresConstraintStateQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresCurrentDatabaseQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresCurrentSchemaQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresCurrentUserQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresDatabaseListQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresDatabaseSizeQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresEnumTypeExistsQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.enum_',
    'PostgresEnumTypeValuesQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.enum_',
    'PostgresExtensionListQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresIndexStateQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresMaterializedViewInfoQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresMaterializedViewListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresOperatorClassListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresOperatorFamilyListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresOperatorListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresPartitionListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresRecoveryStateQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresReplicationLagQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresReplicationSlotCountQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresRoleListQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresRoutineListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresSchemaListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresSchemaStatsQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresSequenceInfoQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresSequenceListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresSequenceStateQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresSettingsQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresSslSessionQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresTableStateQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresTableStorageQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresTypeListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'PostgresWalFileCountQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'PostgresWalPositionQuery': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'Sequence': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.status',
    'TYPE_CHECKING': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.enum_',
    'ViewInfoQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
    'ViewListQueryStatement': 'rhosocial.activerecord.backend.impl.postgres.expression.introspection.introspection',
}

__all__ = [
    'BaseExpression',
    'IntrospectionExpression',
    'List',
    'Optional',
    'PostgresActiveReplicationCountQuery',
    'PostgresActivityCountQuery',
    'PostgresArchiveStatsQuery',
    'PostgresCheckpointCountQuery',
    'PostgresCheckpointLsnQuery',
    'PostgresClientAddressQuery',
    'PostgresConstraintStateQueryStatement',
    'PostgresCurrentDatabaseQuery',
    'PostgresCurrentSchemaQuery',
    'PostgresCurrentUserQuery',
    'PostgresDatabaseListQuery',
    'PostgresDatabaseSizeQueryStatement',
    'PostgresEnumTypeExistsQuery',
    'PostgresEnumTypeValuesQuery',
    'PostgresExtensionListQuery',
    'PostgresIndexStateQueryStatement',
    'PostgresMaterializedViewInfoQueryStatement',
    'PostgresMaterializedViewListQueryStatement',
    'PostgresOperatorClassListQueryStatement',
    'PostgresOperatorFamilyListQueryStatement',
    'PostgresOperatorListQueryStatement',
    'PostgresPartitionListQueryStatement',
    'PostgresRecoveryStateQuery',
    'PostgresReplicationLagQuery',
    'PostgresReplicationSlotCountQuery',
    'PostgresRoleListQuery',
    'PostgresRoutineListQueryStatement',
    'PostgresSchemaListQueryStatement',
    'PostgresSchemaStatsQuery',
    'PostgresSequenceInfoQueryStatement',
    'PostgresSequenceListQueryStatement',
    'PostgresSequenceStateQueryStatement',
    'PostgresSettingsQuery',
    'PostgresSslSessionQuery',
    'PostgresTableStateQueryStatement',
    'PostgresTableStorageQueryStatement',
    'PostgresTypeListQueryStatement',
    'PostgresWalFileCountQuery',
    'PostgresWalPositionQuery',
    'Sequence',
    'TYPE_CHECKING',
    'ViewInfoQueryStatement',
    'ViewListQueryStatement',
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
