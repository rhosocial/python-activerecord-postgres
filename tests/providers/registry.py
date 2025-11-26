# tests/providers/registry.py
"""
Test Provider Registry for postgres Backend

This module registers the concrete implementations of the test suite provider
interfaces for the postgres backend.
"""
from rhosocial.activerecord.testsuite.core.registry import ProviderRegistry
from .basic import BasicProvider
from .events import EventsProvider
from .mixins import MixinsProvider
from .query import QueryProvider

# Create a single, global instance of the ProviderRegistry for this backend.
provider_registry = ProviderRegistry()

# Register the concrete implementations as the providers for the
# interfaces defined in the testsuite.
provider_registry.register("feature.basic.IBasicProvider", BasicProvider)
provider_registry.register("feature.events.IEventsProvider", EventsProvider)
provider_registry.register("feature.mixins.IMixinsProvider", MixinsProvider)
provider_registry.register("feature.query.IQueryProvider", QueryProvider)
