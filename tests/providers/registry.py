# tests/providers/registry.py
"""
Test Provider Registry for postgres Backend

This module registers the concrete implementations of the test suite provider
interfaces for the postgres backend.
"""
from rhosocial.activerecord.testsuite.core.registry import ProviderRegistry
from .basic import BasicSyncProvider, BasicAsyncProvider
from .events import EventsSyncProvider, EventsAsyncProvider
from .mixins import MixinsSyncProvider, MixinsAsyncProvider
from .query import QuerySyncProvider, QueryAsyncProvider
from .relation import RelationSyncProvider, RelationAsyncProvider
from .basic_connection import BasicConnectionProvider
from .query_connection import QueryConnectionProvider
from .crud_benchmark import CrudBenchmarkProvider
from .fastapi_benchmark import FastAPIBenchmarkProvider
from .mixin_benchmark import MixinBenchmarkProvider
from .query_benchmark import QueryBenchmarkProvider
from .transaction_benchmark import TransactionBenchmarkProvider

# Create a single, global instance of the ProviderRegistry for this backend.
provider_registry = ProviderRegistry()

# Register the concrete implementations as the providers for the
# interfaces defined in the testsuite.
provider_registry.register("feature.basic.IBasicProvider", BasicSyncProvider)
provider_registry.register("feature.basic.IBasicSyncProvider", BasicSyncProvider)
provider_registry.register("feature.basic.IBasicAsyncProvider", BasicAsyncProvider)

provider_registry.register("feature.events.IEventsProvider", EventsSyncProvider)
provider_registry.register("feature.events.IEventsSyncProvider", EventsSyncProvider)
provider_registry.register("feature.events.IEventsAsyncProvider", EventsAsyncProvider)

provider_registry.register("feature.mixins.IMixinsProvider", MixinsSyncProvider)
provider_registry.register("feature.mixins.IMixinsSyncProvider", MixinsSyncProvider)
provider_registry.register("feature.mixins.IMixinsAsyncProvider", MixinsAsyncProvider)

provider_registry.register("feature.query.IQueryProvider", QuerySyncProvider)
provider_registry.register("feature.query.IQuerySyncProvider", QuerySyncProvider)
provider_registry.register("feature.query.IQueryAsyncProvider", QueryAsyncProvider)

provider_registry.register("feature.relation.IRelationProvider", RelationSyncProvider)
provider_registry.register("feature.relation.IRelationSyncProvider", RelationSyncProvider)
provider_registry.register("feature.relation.IRelationAsyncProvider", RelationAsyncProvider)

# Register connection pool context awareness providers
provider_registry.register(
    "feature.basic.connection.IBasicConnectionProvider",
    BasicConnectionProvider,
)
provider_registry.register(
    "feature.query.connection.IQueryConnectionProvider",
    QueryConnectionProvider,
)

# Register benchmark providers.
provider_registry.register("benchmark.crud.ICrudBenchmarkProvider", CrudBenchmarkProvider)
provider_registry.register("benchmark.query.IQueryBenchmarkProvider", QueryBenchmarkProvider)
provider_registry.register(
    "benchmark.transaction.ITransactionBenchmarkProvider",
    TransactionBenchmarkProvider,
)
provider_registry.register("benchmark.mixin.IMixinBenchmarkProvider", MixinBenchmarkProvider)
provider_registry.register("benchmark.fastapi.IFastAPIBenchmarkProvider", FastAPIBenchmarkProvider)
