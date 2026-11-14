# tests/rhosocial/activerecord_postgres_test/feature/backend/execute/errors/test_error_taxonomy_contract.py
"""Run the shared error taxonomy contract against this backend.

The testsuite module imports only the core taxonomy, so no PostgreSQL
connection is needed. Importing the module is the whole wrapper: pytest
collects the classes and methods it defines.
"""

from rhosocial.activerecord.testsuite.feature.backend.errors.test_error_taxonomy import *  # noqa: F401,F403
