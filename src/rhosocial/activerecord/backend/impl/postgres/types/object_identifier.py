# src/rhosocial/activerecord/backend/impl/postgres/types/object_identifier.py
"""
PostgreSQL Object Identifier (OID) types representation.

This module provides Python classes for PostgreSQL object identifier types:

Object Identifier Types:
- OID: Unsigned 4-byte integer for internal object identification
- RegClass: Relation (table/view/sequence) name
- RegType: Data type name
- RegProc/RegProcedure: Function name
- RegOper/RegOperator: Operator name
- RegConfig: Text search configuration name
- RegDictionary: Text search dictionary name
- RegNamespace: Namespace (schema) name
- RegRole: Role (user/group) name
- RegCollation: Collation name

Transaction and Command Identifiers:
- XID: 32-bit transaction identifier
- XID8: 64-bit transaction identifier (PostgreSQL 13+)
- CID: Command identifier

Tuple Identifier:
- TID: Tuple identifier (block number, offset)

Version requirements:
- Most OID types: PostgreSQL 8.0+
- regcollation: PostgreSQL 9.1+
- regnamespace/regrole: PostgreSQL 9.5+
- XID8: PostgreSQL 13+

For type adapters (conversion between Python and database),
see adapters.object_identifier module.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class OID:
    """PostgreSQL OID type wrapper.

    OID (Object Identifier) is a 4-byte unsigned integer used internally
    by PostgreSQL as a primary key for system catalogs.

    Note: While OID can be used as a regular unsigned integer, it's primarily
    intended for internal system use. User tables should generally not use OIDs.

    Attributes:
        value: The OID value (0 to 4,294,967,295)

    Examples:
        OID(16384)  # System catalog OID
        OID(0)  # Invalid OID (often used as NULL equivalent)
    """
    value: int

    def __post_init__(self):
        """Validate OID value is in valid range."""
        if not 0 <= self.value <= 4294967295:
            raise ValueError(f"OID must be between 0 and 4294967295, got {self.value}")

    def __int__(self) -> int:
        return self.value

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"OID({self.value})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, OID):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)


@dataclass(frozen=True)
class RegClass:
    """PostgreSQL regclass type wrapper.

    regclass is an alias type that represents a relation (table, view,
    sequence, etc.) by name. PostgreSQL automatically resolves names to OIDs
    and vice versa.

    Attributes:
        name: Relation name (may include schema like 'schema.table')
        oid: Resolved OID (optional, set when retrieved from database)

    Examples:
        RegClass('users')  # Table in default schema
        RegClass('public.users')  # Table with explicit schema
        RegClass('users', 16384)  # With known OID
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regclass has a name."""
        if not self.name:
            raise ValueError("RegClass name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegClass('{self.name}', oid={self.oid})"
        return f"RegClass('{self.name}')"


@dataclass(frozen=True)
class RegType:
    """PostgreSQL regtype type wrapper.

    regtype is an alias type that represents a data type by name.
    PostgreSQL automatically resolves type names to OIDs.

    Attributes:
        name: Type name (may include schema like 'schema.type')
        oid: Resolved OID (optional)

    Examples:
        RegType('integer')
        RegType('varchar')
        RegType('public.my_type')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regtype has a name."""
        if not self.name:
            raise ValueError("RegType name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegType('{self.name}', oid={self.oid})"
        return f"RegType('{self.name}')"


@dataclass(frozen=True)
class RegProc:
    """PostgreSQL regproc type wrapper.

    regproc is an alias type that represents a function by name.
    It uses a simple function name (no arguments) for lookup.

    Attributes:
        name: Function name (may include schema)
        oid: Resolved OID (optional)

    Examples:
        RegProc('now')
        RegProc('pg_catalog.now')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regproc has a name."""
        if not self.name:
            raise ValueError("RegProc name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegProc('{self.name}', oid={self.oid})"
        return f"RegProc('{self.name}')"


@dataclass(frozen=True)
class RegProcedure:
    """PostgreSQL regprocedure type wrapper.

    regprocedure is an alias type that represents a function with its
    argument types, allowing precise identification of overloaded functions.

    Attributes:
        name: Function signature (name with argument types)
        oid: Resolved OID (optional)

    Examples:
        RegProcedure('sum(integer)')
        RegProcedure('pg_catalog.sum(bigint)')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regprocedure has a name."""
        if not self.name:
            raise ValueError("RegProcedure name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegProcedure('{self.name}', oid={self.oid})"
        return f"RegProcedure('{self.name}')"


@dataclass(frozen=True)
class RegOper:
    """PostgreSQL regoper type wrapper.

    regoper is an alias type that represents an operator by name.

    Attributes:
        name: Operator name (symbol or name)
        oid: Resolved OID (optional)

    Examples:
        RegOper('+')
        RegOper('pg_catalog.||')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regoper has a name."""
        if not self.name:
            raise ValueError("RegOper name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegOper('{self.name}', oid={self.oid})"
        return f"RegOper('{self.name}')"


@dataclass(frozen=True)
class RegOperator:
    """PostgreSQL regoperator type wrapper.

    regoperator is an alias type that represents an operator with its
    argument types, allowing precise identification of overloaded operators.

    Attributes:
        name: Operator signature with argument types
        oid: Resolved OID (optional)

    Examples:
        RegOperator('+(integer,integer)')
        RegOperator('||(text,text)')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regoperator has a name."""
        if not self.name:
            raise ValueError("RegOperator name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegOperator('{self.name}', oid={self.oid})"
        return f"RegOperator('{self.name}')"


@dataclass(frozen=True)
class RegConfig:
    """PostgreSQL regconfig type wrapper.

    regconfig is an alias type that represents a text search configuration.

    Attributes:
        name: Configuration name
        oid: Resolved OID (optional)

    Examples:
        RegConfig('english')
        RegConfig('pg_catalog.english')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regconfig has a name."""
        if not self.name:
            raise ValueError("RegConfig name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegConfig('{self.name}', oid={self.oid})"
        return f"RegConfig('{self.name}')"


@dataclass(frozen=True)
class RegDictionary:
    """PostgreSQL regdictionary type wrapper.

    regdictionary is an alias type that represents a text search dictionary.

    Attributes:
        name: Dictionary name
        oid: Resolved OID (optional)

    Examples:
        RegDictionary('english_stem')
        RegDictionary('pg_catalog.english_stem')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regdictionary has a name."""
        if not self.name:
            raise ValueError("RegDictionary name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegDictionary('{self.name}', oid={self.oid})"
        return f"RegDictionary('{self.name}')"


@dataclass(frozen=True)
class RegNamespace:
    """PostgreSQL regnamespace type wrapper.

    regnamespace is an alias type that represents a namespace (schema).

    Attributes:
        name: Namespace name
        oid: Resolved OID (optional)

    Examples:
        RegNamespace('public')
        RegNamespace('app')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regnamespace has a name."""
        if not self.name:
            raise ValueError("RegNamespace name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegNamespace('{self.name}', oid={self.oid})"
        return f"RegNamespace('{self.name}')"


@dataclass(frozen=True)
class RegRole:
    """PostgreSQL regrole type wrapper.

    regrole is an alias type that represents a role (user or group).

    Attributes:
        name: Role name
        oid: Resolved OID (optional)

    Examples:
        RegRole('postgres')
        RegRole('app_user')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regrole has a name."""
        if not self.name:
            raise ValueError("RegRole name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegRole('{self.name}', oid={self.oid})"
        return f"RegRole('{self.name}')"


@dataclass(frozen=True)
class RegCollation:
    """PostgreSQL regcollation type wrapper.

    regcollation is an alias type that represents a collation.

    Attributes:
        name: Collation name
        oid: Resolved OID (optional)

    Examples:
        RegCollation('en_US')
        RegCollation('C')
        RegCollation('pg_catalog.default')
    """
    name: str
    oid: Optional[int] = None

    def __post_init__(self):
        """Validate regcollation has a name."""
        if not self.name:
            raise ValueError("RegCollation name cannot be empty")

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        if self.oid:
            return f"RegCollation('{self.name}', oid={self.oid})"
        return f"RegCollation('{self.name}')"


@dataclass(frozen=True)
class XID:
    """PostgreSQL xid type wrapper.

    XID (Transaction Identifier) is a 32-bit unsigned integer used to
    identify transactions internally. XIDs are assigned sequentially and
    wrap around at approximately 4 billion transactions.

    Attributes:
        value: The transaction ID (0 to 4,294,967,295)

    Examples:
        XID(1000)  # Transaction 1000
        XID(0)  # Frozen transaction ID
    """
    value: int

    def __post_init__(self):
        """Validate XID value is in valid range."""
        if not 0 <= self.value <= 4294967295:
            raise ValueError(f"XID must be between 0 and 4294967295, got {self.value}")

    def __int__(self) -> int:
        return self.value

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"XID({self.value})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, XID):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)


@dataclass(frozen=True)
class XID8:
    """PostgreSQL xid8 type wrapper.

    XID8 is a 64-bit transaction identifier introduced in PostgreSQL 13.
    Unlike regular XID, XID8 does not wrap around, making it suitable
    for long-term transaction tracking.

    Attributes:
        value: The full transaction ID (0 to 18,446,744,073,709,551,615)

    Examples:
        XID8(1000000000)  # Full transaction ID
    """
    value: int

    def __post_init__(self):
        """Validate XID8 value is in valid range."""
        if not 0 <= self.value <= 18446744073709551615:
            raise ValueError(f"XID8 must be between 0 and 18446744073709551615, got {self.value}")

    def __int__(self) -> int:
        return self.value

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"XID8({self.value})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, XID8):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)


@dataclass(frozen=True)
class CID:
    """PostgreSQL cid type wrapper.

    CID (Command Identifier) is a 32-bit unsigned integer used to
    identify commands within a transaction. Each command in a transaction
    gets a sequential CID starting from 0.

    Attributes:
        value: The command ID (0 to 4,294,967,295)

    Examples:
        CID(0)  # First command in transaction
        CID(5)  # Sixth command in transaction
    """
    value: int

    def __post_init__(self):
        """Validate CID value is in valid range."""
        if not 0 <= self.value <= 4294967295:
            raise ValueError(f"CID must be between 0 and 4294967295, got {self.value}")

    def __int__(self) -> int:
        return self.value

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"CID({self.value})"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, CID):
            return self.value == other.value
        if isinstance(other, int):
            return self.value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)


@dataclass(frozen=True)
class TID:
    """PostgreSQL tid type wrapper.

    TID (Tuple Identifier) identifies a physical location of a tuple
    within a table. It consists of a block number and an offset within
    that block.

    Attributes:
        block: Block number (0 to 4,294,967,295)
        offset: Tuple offset within block (0 to 65,535)

    Examples:
        TID(0, 1)  # First block, first tuple
        TID(100, 50)  # Block 100, tuple at offset 50
    """
    block: int
    offset: int

    def __post_init__(self):
        """Validate TID values are in valid ranges."""
        if not 0 <= self.block <= 4294967295:
            raise ValueError(f"Block number must be between 0 and 4294967295, got {self.block}")
        if not 0 <= self.offset <= 65535:
            raise ValueError(f"Offset must be between 0 and 65535, got {self.offset}")

    def __str__(self) -> str:
        return f"({self.block},{self.offset})"

    def __repr__(self) -> str:
        return f"TID(block={self.block}, offset={self.offset})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TID):
            return NotImplemented
        return self.block == other.block and self.offset == other.offset

    def __hash__(self) -> int:
        return hash((self.block, self.offset))

    def to_postgres_string(self) -> str:
        """Convert to PostgreSQL tid literal format.

        Returns:
            PostgreSQL tid literal string like '(0,1)'
        """
        return f"({self.block},{self.offset})"

    @classmethod
    def from_postgres_string(cls, value: str) -> 'TID':
        """Parse PostgreSQL tid string.

        Args:
            value: PostgreSQL tid string like '(0,1)'

        Returns:
            TID instance

        Raises:
            ValueError: If string format is invalid
        """
        value = value.strip()
        if value.startswith('(') and value.endswith(')'):
            value = value[1:-1]
        parts = value.split(',')
        if len(parts) != 2:
            raise ValueError(f"Invalid tid format: {value}")
        return cls(int(parts[0].strip()), int(parts[1].strip()))


__all__ = [
    'OID', 'RegClass', 'RegType', 'RegProc', 'RegProcedure',
    'RegOper', 'RegOperator', 'RegConfig', 'RegDictionary',
    'RegNamespace', 'RegRole', 'RegCollation',
    'XID', 'XID8', 'CID', 'TID'
]
