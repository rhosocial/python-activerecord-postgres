# src/rhosocial/activerecord/backend/impl/postgres/adapters/object_identifier.py
"""
PostgreSQL Object Identifier Types Adapters.

This module provides type adapters for PostgreSQL object identifier types.

PostgreSQL Documentation: https://www.postgresql.org/docs/current/datatype-oid.html

Object Identifier Types:
- oid: Unsigned 4-byte integer for internal object identification
- regclass: Relation (table/view/sequence) name
- regtype: Data type name
- regproc/regprocedure: Function name
- regoper/regoperator: Operator name
- regconfig: Text search configuration name
- regdictionary: Text search dictionary name
- regnamespace: Namespace (schema) name
- regrole: Role (user/group) name
- regcollation: Collation name

Transaction and Command Identifiers:
- xid: 32-bit transaction identifier (XID)
- xid8: 64-bit transaction identifier (full XID, PostgreSQL 13+)
- cid: Command identifier

Tuple Identifier:
- tid: Tuple identifier (block number, offset)
"""
from typing import Any, Dict, List, Optional, Set, Type, Union

from ..types.object_identifier import (
    OID,
    RegClass,
    RegType,
    RegProc,
    RegProcedure,
    RegOper,
    RegOperator,
    RegConfig,
    RegDictionary,
    RegNamespace,
    RegRole,
    RegCollation,
    XID,
    XID8,
    CID,
    TID,
)


class PostgresOidAdapter:
    """PostgreSQL OID and registry type adapter.

    This adapter handles conversion between Python values and PostgreSQL
    object identifier types, including OID and all reg* types.

    Supported types:
    - oid: Unsigned 4-byte integer
    - regclass: Relation name
    - regtype: Data type name
    - regproc/regprocedure: Function name
    - regoper/regoperator: Operator name
    - regconfig: Text search configuration
    - regdictionary: Text search dictionary
    - regnamespace: Namespace name
    - regrole: Role name
    - regcollation: Collation name
    """

    REGISTRY_TYPES: Dict[str, Type] = {
        'regclass': RegClass,
        'regtype': RegType,
        'regproc': RegProc,
        'regprocedure': RegProcedure,
        'regoper': RegOper,
        'regoperator': RegOperator,
        'regconfig': RegConfig,
        'regdictionary': RegDictionary,
        'regnamespace': RegNamespace,
        'regrole': RegRole,
        'regcollation': RegCollation,
    }

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            OID: {int, str},
            RegClass: {str},
            RegType: {str},
            RegProc: {str},
            RegProcedure: {str},
            RegOper: {str},
            RegOperator: {str},
            RegConfig: {str},
            RegDictionary: {str},
            RegNamespace: {str},
            RegRole: {str},
            RegCollation: {str},
        }

    def to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[Union[int, str]]:
        """Convert Python value to PostgreSQL OID type.

        Args:
            value: OID wrapper class, int, or str
            target_type: Target type (not used directly)
            options: Optional conversion options

        Returns:
            Integer OID or name string, or None
        """
        if value is None:
            return None

        if isinstance(value, OID):
            return value.value

        if isinstance(value, int):
            if not 0 <= value <= 4294967295:
                raise ValueError(f"OID value out of range: {value}")
            return value

        for reg_type in self.REGISTRY_TYPES.values():
            if isinstance(value, reg_type):
                return value.name

        if isinstance(value, str):
            return value

        raise TypeError(f"Cannot convert {type(value).__name__} to OID type")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Convert PostgreSQL OID value to Python object.

        Args:
            value: OID or name string from database
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            OID wrapper instance or str/int
        """
        if value is None:
            return None

        if target_type == OID:
            if isinstance(value, int):
                return OID(value)
            if isinstance(value, str):
                return OID(int(value))

        if target_type in self.REGISTRY_TYPES.values():
            if isinstance(value, str):
                return target_type(value)
            if isinstance(value, int):
                return target_type(str(value), oid=value)

        if target_type == int:
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                return int(value)

        if target_type == str:
            return str(value)

        raise TypeError(f"Cannot convert {type(value).__name__} to {target_type.__name__}")

    def to_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


class PostgresXidAdapter:
    """PostgreSQL transaction and command identifier adapter.

    This adapter handles conversion between Python values and PostgreSQL
    transaction identifier types (xid, xid8, cid).

    Supported types:
    - xid: 32-bit transaction identifier
    - xid8: 64-bit transaction identifier (PostgreSQL 13+)
    - cid: Command identifier
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            XID: {int, str},
            XID8: {int, str},
            CID: {int, str},
        }

    def to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[int]:
        """Convert Python value to PostgreSQL transaction ID.

        Args:
            value: XID/XID8/CID wrapper or int
            target_type: Target type
            options: Optional conversion options

        Returns:
            Integer transaction/command ID, or None
        """
        if value is None:
            return None

        if isinstance(value, (XID, XID8, CID)):
            return value.value

        if isinstance(value, int):
            return value

        if isinstance(value, str):
            return int(value)

        raise TypeError(f"Cannot convert {type(value).__name__} to transaction ID type")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Convert PostgreSQL transaction ID to Python object.

        Args:
            value: Integer or string from database
            target_type: Target Python type (XID, XID8, or CID)
            options: Optional conversion options

        Returns:
            XID/XID8/CID wrapper instance
        """
        if value is None:
            return None

        if target_type in (XID, XID8, CID):
            if isinstance(value, int):
                return target_type(value)
            if isinstance(value, str):
                return target_type(int(value))

        if target_type == int:
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                return int(value)

        raise TypeError(f"Cannot convert {type(value).__name__} to {target_type.__name__}")

    def to_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


class PostgresTidAdapter:
    """PostgreSQL tuple identifier adapter.

    This adapter handles conversion between Python values and PostgreSQL
    tuple identifier type (tid).

    Supported types:
    - tid: Tuple identifier (block number, offset)
    """

    @property
    def supported_types(self) -> Dict[Type, Set[Type]]:
        """Return supported type mappings."""
        return {
            TID: {str, tuple},
        }

    def to_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Optional[str]:
        """Convert Python value to PostgreSQL tid.

        Args:
            value: TID wrapper, tuple, or str
            target_type: Target type
            options: Optional conversion options

        Returns:
            PostgreSQL tid literal string, or None
        """
        if value is None:
            return None

        if isinstance(value, TID):
            return value.to_postgres_string()

        if isinstance(value, str):
            return value

        if isinstance(value, (tuple, list)):
            if len(value) == 2:
                block, offset = value
                return TID(int(block), int(offset)).to_postgres_string()

        raise TypeError(f"Cannot convert {type(value).__name__} to TID")

    def from_database(
        self,
        value: Any,
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Convert PostgreSQL tid to Python object.

        Args:
            value: Tid string from database
            target_type: Target Python type
            options: Optional conversion options

        Returns:
            TID wrapper instance or tuple
        """
        if value is None:
            return None

        if target_type == TID:
            if isinstance(value, TID):
                return value
            if isinstance(value, str):
                return TID.from_postgres_string(value)
            if isinstance(value, (tuple, list)):
                return TID(int(value[0]), int(value[1]))

        if target_type == tuple:
            if isinstance(value, TID):
                return (value.block, value.offset)
            if isinstance(value, str):
                tid = TID.from_postgres_string(value)
                return (tid.block, tid.offset)

        if target_type == str:
            if isinstance(value, TID):
                return value.to_postgres_string()
            return str(value)

        raise TypeError(f"Cannot convert {type(value).__name__} to {target_type.__name__}")

    def to_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values to database format."""
        return [self.to_database(v, target_type, options) for v in values]

    def from_database_batch(
        self,
        values: List[Any],
        target_type: Type,
        options: Optional[Dict[str, Any]] = None
    ) -> List[Any]:
        """Batch convert values from database format."""
        return [self.from_database(v, target_type, options) for v in values]


__all__ = ['PostgresOidAdapter', 'PostgresXidAdapter', 'PostgresTidAdapter']
