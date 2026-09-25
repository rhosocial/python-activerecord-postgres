# src/rhosocial/activerecord/backend/impl/postgres/expression/registry.py
"""PostgreSQL DDL expression registration."""

from typing import Tuple, Type

from rhosocial.activerecord.backend.expression.bases import BaseExpression
from rhosocial.activerecord.backend.expression.serialization import ExpressionRegistry
from .ddl.domain import (
    _LegacySqlExpression,
    _UnsupportedDomainAction,
    PostgresAddDomainCheckAction,
    PostgresAlterDomainExpression,
    PostgresChangeDomainOwnerAction,
    PostgresCreateDomainExpression,
    PostgresDropDomainCheckAction,
    PostgresDropDomainExpression,
    PostgresRenameDomainConstraintAction,
    PostgresSetDomainSchemaAction,
    PostgresValidateDomainConstraintAction,
)
from .copy import PostgresCopyFromExpression, PostgresCopyToExpression
from .ddl.multirange import (
    CreateMultirangeTypeExpression,
    MultirangeAggFunctionExpression,
)
from .ddl.repack import PostgresRepackExpression
from .ddl.constraint import PostgresAlterConstraint, PostgresValidateConstraint
from .ddl.type import (
    AlterEnumAddValueExpression,
    CreateEnumTypeExpression,
    DropEnumTypeExpression,
    EnumTypeNameExpression,
    EnumValuesExpression,
    PostgresAddEnumValueAction,
    PostgresAddTypeAttributeAction,
    PostgresAlterEnumAddValueExpression,
    PostgresAlterEnumTypeAddValueExpression,
    PostgresAlterEnumTypeRenameValueExpression,
    PostgresAlterTypeAttributeAction,
    PostgresBaseTypeDefinition,
    PostgresChangeTypeOwnerAction,
    PostgresCompositeTypeDefinition,
    PostgresCreateEnumTypeExpression,
    PostgresCreateRangeTypeExpression,
    PostgresDropEnumTypeExpression,
    PostgresDropTypeAttributeAction,
    PostgresDropTypeExpression,
    PostgresEnumTypeDefinition,
    PostgresRangeTypeDefinition,
    PostgresRenameEnumValueAction,
    PostgresRenameTypeAction,
    PostgresRenameTypeAttributeAction,
    PostgresSetTypePropertiesAction,
    PostgresSetTypeSchemaAction,
    PostgresShellTypeDefinition,
)


_POSTGRES_DDL_EXPRESSION_CLASSES: Tuple[Type[BaseExpression], ...] = (
    PostgresCopyFromExpression,
    PostgresCopyToExpression,
    PostgresRepackExpression,
    PostgresAlterConstraint,
    PostgresValidateConstraint,
    PostgresCompositeTypeDefinition,
    PostgresEnumTypeDefinition,
    PostgresRangeTypeDefinition,
    PostgresBaseTypeDefinition,
    PostgresShellTypeDefinition,
    PostgresRenameTypeAction,
    PostgresSetTypeSchemaAction,
    PostgresChangeTypeOwnerAction,
    PostgresRenameTypeAttributeAction,
    PostgresAddTypeAttributeAction,
    PostgresDropTypeAttributeAction,
    PostgresAlterTypeAttributeAction,
    PostgresAddEnumValueAction,
    PostgresRenameEnumValueAction,
    PostgresSetTypePropertiesAction,
    PostgresDropTypeExpression,
    PostgresCreateEnumTypeExpression,
    PostgresDropEnumTypeExpression,
    PostgresAlterEnumAddValueExpression,
    PostgresAlterEnumTypeAddValueExpression,
    PostgresAlterEnumTypeRenameValueExpression,
    PostgresCreateRangeTypeExpression,
    EnumTypeNameExpression,
    EnumValuesExpression,
    CreateEnumTypeExpression,
    DropEnumTypeExpression,
    AlterEnumAddValueExpression,
    _LegacySqlExpression,
    _UnsupportedDomainAction,
    PostgresAddDomainCheckAction,
    PostgresDropDomainCheckAction,
    PostgresRenameDomainConstraintAction,
    PostgresValidateDomainConstraintAction,
    PostgresChangeDomainOwnerAction,
    PostgresSetDomainSchemaAction,
    PostgresCreateDomainExpression,
    PostgresAlterDomainExpression,
    PostgresDropDomainExpression,
    CreateMultirangeTypeExpression,
    MultirangeAggFunctionExpression,
)


def register_postgres_ddl_expressions() -> None:
    for expression_class in _POSTGRES_DDL_EXPRESSION_CLASSES:
        ExpressionRegistry.register(expression_class)


__all__ = ["register_postgres_ddl_expressions"]
