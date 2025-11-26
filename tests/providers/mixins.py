"""
This file provides the concrete implementation of the `IMixinsProvider` interface
that is defined in the `rhosocial-activerecord-testsuite` package.

Its main responsibilities are:
1.  Reporting which test scenarios (database configurations) are available.
2.  Setting up the database environment for a given test. This includes:
    - Getting the correct database configuration for the scenario.
    - Configuring the ActiveRecord model with a database connection.
    - Dropping any old tables and creating the necessary table schema.
3.  Cleaning up any resources after a test runs.
"""
import os
from typing import Type, List

from rhosocial.activerecord.model import ActiveRecord
from rhosocial.activerecord.testsuite.feature.mixins.interfaces import IMixinsProvider
from rhosocial.activerecord.testsuite.feature.mixins.fixtures.models import TimestampedPost, VersionedProduct, Task, CombinedArticle
# ...and the scenarios are defined specifically for this backend.
from .scenarios import get_enabled_scenarios, get_scenario


class MixinsProvider(IMixinsProvider):
    """
    This is the postgres backend's implementation for the mixins test group.
    It connects the generic tests in the testsuite with the actual postgres database.
    """

    def __init__(self):
        # This list will track the backend instances created during the setup phase.
        self._active_backends = []

    def get_test_scenarios(self) -> List[str]:
        """Returns a list of names for all enabled scenarios for this backend."""
        return list(get_enabled_scenarios().keys())

    def _setup_model(self, model_class: Type[ActiveRecord], scenario_name: str, table_name: str) -> Type[ActiveRecord]:
        """A generic helper method to handle the setup for any given model."""
        # 1. Get the backend class (PostgresBackend) and connection config for the requested scenario.
        backend_class, config = get_scenario(scenario_name)

        # 2. Configure the generic model class with our specific backend and config.
        model_class.configure(config, backend_class)

        backend_instance = model_class.__backend__
        if backend_instance not in self._active_backends:
            self._active_backends.append(backend_instance)

        # 3. Prepare the database schema.
        try:
            # Drop the table if it exists, with cascade to handle dependencies.
            model_class.__backend__.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
        except Exception as e:
            # Ignore errors if the table didn't exist, but print for debugging.
            print(f"Could not drop table {table_name}: {e}")
            pass

        # Execute the schema SQL to create the table.
        schema_sql = self._load_postgres_schema(f"{table_name}.sql")
        model_class.__backend__.execute(schema_sql)

        return model_class

    # --- Implementation of the IMixinsProvider interface ---







    def setup_timestamped_post_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for timestamps model tests."""

        return self._setup_model(TimestampedPost, scenario_name, "timestamped_posts")

    def setup_versioned_product_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for optimistic lock model tests."""

        return self._setup_model(VersionedProduct, scenario_name, "versioned_products")

    def setup_task_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for soft delete model tests."""

        return self._setup_model(Task, scenario_name, "tasks")

    def setup_combined_article_model(self, scenario_name: str) -> Type[ActiveRecord]:
        """Sets up the database for combined article model tests."""

        return self._setup_model(CombinedArticle, scenario_name, "combined_articles")



    def _load_postgres_schema(self, filename: str) -> str:
        """Helper to load a SQL schema file from this project's fixtures."""
        # Schemas are stored in the centralized location for mixins feature.
        schema_dir = os.path.join(os.path.dirname(__file__), "..", "rhosocial", "activerecord_postgres_test", "feature", "mixins", "schema")
        schema_path = os.path.join(schema_dir, filename)

        with open(schema_path, 'r', encoding='utf-8') as f:
            return f.read()

    def cleanup_after_test(self, scenario_name: str):
        """
        Performs cleanup after a test. This now iterates through the backends
        that were created during setup, drops tables, and explicitly disconnects them.
        """
        for backend_instance in self._active_backends:
            try:
                # Drop all tables that might have been created for mixins tests
                for table_name in ['timestamped_posts', 'versioned_products', 'tasks',
                                   'combined_articles']:
                    try:
                        backend_instance.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    except Exception:
                        # Continue with other tables if one fails
                        pass
            finally:
                # Always disconnect the backend instance that was used in the test
                try:
                    backend_instance.disconnect()
                except:
                    # Ignore errors during disconnect
                    pass

        # Clear the list of active backends for the next test
        self._active_backends.clear()