# tests/rhosocial/activerecord_postgres_test/feature/basic/types_long_content/conftest.py
"""Use the shared ``basic/types_long_content`` test set.

The shared ``LongRow`` / ``AsyncLongRow`` fixtures declare ``id: Optional[int]``
but no ``__primary_key__``. Since the primary-key-declaration refactor, a bare
``ActiveRecord`` subclass is keyless (``primary_key() is None``), so ``save()``
no longer writes back the generated id and ``find_one()`` refuses to address
the row. PostgreSQL is one of the few dialects that renders all four content
concepts (text/blob/json/xml), so this group actually runs its round-trip tests
here and is the only backend affected.

The declaration belongs in the shared fixture, but until it is added there the
postgres bridge restores it locally. Setting the class attribute (rather than
subclassing) keeps the shared fixtures and the imported test module operating
on the very same model objects.
"""

from rhosocial.activerecord.testsuite.feature.basic.types_long_content.conftest import *  # noqa: F401,F403
from rhosocial.activerecord.testsuite.feature.basic.fixtures.types_long_content_models import (
    AsyncLongRow,
    LongRow,
)

for _long_row_class in (LongRow, AsyncLongRow):
    if _long_row_class.__primary_key__ is None:
        _long_row_class.__primary_key__ = "id"

del _long_row_class
