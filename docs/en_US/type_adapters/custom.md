# Custom Type Adapters

## Creating Custom Adapters

```python
from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter
from typing import Type, Dict, List, Any, Optional

class IPAddressAdapter(SQLTypeAdapter):
    """Adapter for PostgreSQL INET type."""
    
    @property
    def supported_types(self) -> Dict[Type, List[Any]]:
        return {str: ['INET']}
    
    def to_database(self, value: str, target_type: Type, 
                   options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        # Convert Python string to PostgreSQL INET
        return value
    
    def from_database(self, value: Any, target_type: Type,
                     options: Optional[Dict[str, Any]] = None) -> str:
        if value is None:
            return None
        return str(value)

# Register adapter
backend.register_type_adapter(IPAddressAdapter())
```

## Using Custom Adapters

```python
class Server(ActiveRecord):
    __table_name__ = "servers"
    name: str
    ip_address: str  # Will use IPAddressAdapter
```

## Adapter Priority

Custom adapters take precedence over built-in adapters when registered.

💡 *AI Prompt:* "When should I create a custom type adapter?"
