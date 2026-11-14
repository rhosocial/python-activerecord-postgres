# tests/rhosocial/activerecord_postgres_test/feature/basic/core/test_basic_mapped_models.py
"""
This module serves as a bridge to execute the generic basic mapped models tests
defined in the `rhosocial-activerecord-testsuite` package against the PostgreSQL backend.

The fixtures those cases request (including ``mapped_models_fixtures``) come
from the shared ``basic`` topic conftest inherited from the parent directory.
"""

from rhosocial.activerecord.testsuite.feature.basic.fields.test_example_basic_fixtures import *  # noqa: F403
