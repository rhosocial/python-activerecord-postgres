# 自定义类型适配器

## 创建自定义适配器

```python
from rhosocial.activerecord.backend.type_adapter import SQLTypeAdapter
from typing import Type, Dict, List, Any, Optional

class IPAddressAdapter(SQLTypeAdapter):
    """PostgreSQL INET 类型的适配器。"""
    
    @property
    def supported_types(self) -> Dict[Type, List[Any]]:
        return {str: ['INET']}
    
    def to_database(self, value: str, target_type: Type, 
                   options: Optional[Dict[str, Any]] = None) -> Any:
        if value is None:
            return None
        # 将 Python 字符串转换为 PostgreSQL INET
        return value
    
    def from_database(self, value: Any, target_type: Type,
                     options: Optional[Dict[str, Any]] = None) -> str:
        if value is None:
            return None
        return str(value)

# 注册适配器
backend.register_type_adapter(IPAddressAdapter())
```

## 使用自定义适配器

```python
class Server(ActiveRecord):
    __table_name__ = "servers"
    name: str
    ip_address: str  # 将使用 IPAddressAdapter
```

## 适配器优先级

注册时，自定义适配器优先于内置适配器。

💡 *AI 提示词：* "什么时候应该创建自定义类型适配器？"
