# tests/rhosocial/activerecord_postgres_test/feature/basic/fixtures/models.py
"""
PostgreSQL-specific model definitions for basic feature tests.

These models are tailored for PostgreSQL's native type support:
- Uses TEXT[] for array fields (no need for string serialization adapters)
- Uses native JSONB for JSON fields
"""
import json
from datetime import datetime
from decimal import Decimal
from typing import ClassVar, Dict, List, Optional, Any

from pydantic import Field

from rhosocial.activerecord.model import ActiveRecord, AsyncActiveRecord
from rhosocial.activerecord.base.field_proxy import FieldProxy
from rhosocial.activerecord.base.fields import UseColumn, UseAdapter
from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated


class JsonToStringAdapter(BaseSQLTypeAdapter):
    """Converts a Python dictionary to a JSON string for the DB."""

    def __init__(self):
        super().__init__()
        self._register_type(dict, str)

    def _do_to_database(self, value: Dict, target_type: type, options: Optional[Dict[str, Any]] = None) -> Optional[str]:
        if value is None:
            return None
        return json.dumps(value)

    def _do_from_database(self, value: str, target_type: type, options: Optional[Dict[str, Any]] = None) -> Optional[Dict]:
        if value is None:
            return None
        if isinstance(value, dict):
            return value
        return json.loads(value)


class PostgresMixedAnnotationModel(ActiveRecord):
    """
    PostgreSQL-specific model with native array support.
    
    Unlike the generic MixedAnnotationModel in testsuite:
    - tags: Uses native TEXT[] (no adapter needed)
    - metadata: Uses JSONB (adapter handles dict <-> str)
    """
    __table_name__ = "mixed_annotation_items"
    __primary_key__ = "id"

    name: str
    item_id: Annotated[Optional[int], UseColumn("id")] = None
    
    tags: Optional[List[str]] = None

    metadata: Annotated[Optional[Dict], UseColumn("meta"), UseAdapter(JsonToStringAdapter(), str)] = None
    description: Optional[str] = None
    status: str = "active"


class AsyncPostgresMixedAnnotationModel(AsyncActiveRecord):
    """Async version of PostgresMixedAnnotationModel."""
    __table_name__ = "mixed_annotation_items"
    __primary_key__ = "id"
    c: ClassVar[FieldProxy] = FieldProxy()

    name: str
    item_id: Annotated[Optional[int], UseColumn("id")] = None
    
    tags: Optional[List[str]] = None

    metadata: Annotated[Optional[Dict], UseColumn("meta"), UseAdapter(JsonToStringAdapter(), str)] = None
    description: Optional[str] = None
    status: str = "active"
