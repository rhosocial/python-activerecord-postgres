# 自定义类型适配器

## 概述

类型适配器在 Python 对象和数据库值之间进行转换。当内置适配器无法满足需求时，您可以为专门的数据类型创建自定义适配器。

## 适配器协议

类型适配器实现 `rhosocial.activerecord.backend.type_adapter` 中的 `SQLTypeAdapter` 协议：

```python
@runtime_checkable
class SQLTypeAdapter(Protocol):
    def to_database(self, value, target_type, options=None) -> Any: ...
    def from_database(self, value, target_type, options=None) -> Any: ...

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]: ...
```

## 创建自定义适配器

继承 `BaseSQLTypeAdapter` 以获得便利：

```python
from rhosocial.activerecord.backend.type_adapter import BaseSQLTypeAdapter


class Color:
    """简单的 RGB 颜色类。"""
    def __init__(self, r: int, g: int, b: int):
        self.r = r
        self.g = g
        self.b = b

    @classmethod
    def from_hex(cls, hex_str: str) -> 'Color':
        hex_str = hex_str.lstrip('#')
        return cls(
            int(hex_str[0:2], 16),
            int(hex_str[2:4], 16),
            int(hex_str[4:6], 16),
        )

    def to_hex(self) -> str:
        return f"#{self.r:02x}{self.g:02x}{self.b:02x}"


class ColorAdapter(BaseSQLTypeAdapter):
    """在 Python Color 对象和数据库十六进制字符串之间转换。"""

    def __init__(self):
        super().__init__()
        # 注册：{python_type: {db_types}}
        self._register_type(Color, str)

    def _do_to_database(self, value, target_type, options):
        """将 Color 转换为十六进制字符串以供数据库存储。"""
        if isinstance(value, Color):
            return value.to_hex()
        raise TypeError(f"Cannot convert {type(value).__name__} to Color")

    def _do_from_database(self, value, target_type, options):
        """将数据库中的十六进制字符串转换为 Color。"""
        if isinstance(value, str):
            return Color.from_hex(value)
        raise TypeError(f"Cannot convert {type(value).__name__} from database")
```

## 注册适配器

将适配器注册到后端的类型注册表：

```python
from rhosocial.activerecord.backend.type_registry import TypeRegistry

# 创建注册表（或使用后端现有的注册表）
registry = TypeRegistry()

# 注册：adapter, python_type, db_type
registry.register(ColorAdapter(), Color, str, allow_override=False)
```

或通过后端的 `_register_default_adapters()` 方法注册：

```python
class MyBackend(PostgreSQLBackend):
    def _register_default_adapters(self):
        super()._register_default_adapters()
        self._type_registry.register(
            ColorAdapter(), Color, str, allow_override=False
        )
```

## 在模型中使用自定义适配器

注册后，当您声明带有自定义类型的字段时，适配器会自动使用：

```python
class Product(ActiveRecord):
    name: str
    color: Color  # 自动使用 ColorAdapter

    @classmethod
    def table_name(cls) -> str:
        return 'products'

# 适配器透明地处理转换
product = Product(name='Widget', color=Color(255, 0, 0))
product.save()  # Color 转换为 '#ff0000' 以供存储

loaded = Product.find_one(product.id)
print(loaded.color)  # Color(255, 0, 0) — 从 '#ff0000' 转换回来
```

## 适配器选项

适配器可以接受配置选项：

```python
class JsonAdapter(BaseSQLTypeAdapter):
    def _do_to_database(self, value, target_type, options):
        indent = options.get('indent', None) if options else None
        return json.dumps(value, indent=indent)

    def _do_from_database(self, value, target_type, options):
        return json.loads(value)
```

## 另请参阅

- [类型适配器](../type_adapters/README.md) — 类型转换系统
- [自定义数据类型](custom_types.md) — 定义新的 DataType 子类
- [类型映射](../type_adapters/mapping.md) — 内置类型映射
- [自定义适配器](../type_adapters/custom.md) — 更多适配器示例

💡 *AI 提示词：* "如何在数据库列中存储带有自定义序列化的 Python 对象？"
