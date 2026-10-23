# src/rhosocial/activerecord/backend/impl/postgres/mixins/function.py
"""PostgreSQL function support detection mixin."""

from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.impl.postgres.function_versions import (
        FunctionSupportInfo,
    )


class PostgresFunctionMixin:
    """PostgreSQL function support detection."""

    def supports_functions(self) -> Dict[str, "FunctionSupportInfo"]:
        """Return supported SQL functions with detailed support information.

        This method combines:
        1. Core functions from rhosocial.activerecord.backend.expression.functions
        2. PostgreSQL-specific functions from rhosocial.activerecord.backend.impl.postgres.functions

        Each function is mapped to a FunctionSupportInfo indicating:
        - Whether the function is supported
        - If not, the reason why (PG version, extension status, etc.)

        Returns:
            Dict mapping function names to FunctionSupportInfo.

        """
        from ..function_versions import FunctionSupportInfo
        from rhosocial.activerecord.backend.expression.functions import (
            __all__ as core_functions,
        )
        from rhosocial.activerecord.backend.impl.postgres import functions as postgres_functions

        expression_constructors = {
            "xmlagg",
            "xmlattributes",
            "xmlcomment",
            "xmlconcat",
            "xmlelement",
            "xmlexists",
            "xmlforest",
            "xmlparse",
            "xmlpi",
            "xmlquery",
            "xmlroot",
            "xmlserialize",
            "xmltable",
        }
        result: Dict[str, FunctionSupportInfo] = {}
        for func_name in core_functions:
            if func_name not in expression_constructors:
                result[func_name] = self._check_function_support(func_name)

        postgres_funcs = getattr(postgres_functions, "__all__", [])
        for func_name in postgres_funcs:
            if func_name not in result:
                result[func_name] = self._check_function_support(func_name)

        return result

    def _check_function_support(self, func_name: str) -> "FunctionSupportInfo":
        """Check function support status and return detailed information.

        Args:
            func_name: Name of the function to check

        Returns:
            FunctionSupportInfo with support status and reason if unsupported

        """
        from ..function_versions import FunctionSupportInfo

        requirement = self._POSTGRES_FUNCTION_VERSIONS.get(func_name)
        if requirement is None:
            return FunctionSupportInfo(supported=True)

        # Check PostgreSQL server version
        if requirement.min_pg_version is not None and self.version < requirement.min_pg_version:
            return FunctionSupportInfo(supported=False, reason="pg_version_too_low")
        if requirement.max_pg_version is not None and self.version > requirement.max_pg_version:
            return FunctionSupportInfo(supported=False, reason="pg_version_too_high")

        # Check extension requirements
        if requirement.extension is not None:
            if not hasattr(self, "_extensions"):
                return FunctionSupportInfo(supported=False, reason="extension_not_probed")

            if requirement.ext_feature is not None:
                if not self.check_extension_feature(requirement.extension, requirement.ext_feature):
                    if not self.is_extension_installed(requirement.extension):
                        return FunctionSupportInfo(supported=False, reason="extension_not_installed")
                    return FunctionSupportInfo(supported=False, reason="extension_version_insufficient")
            else:
                if not self.is_extension_installed(requirement.extension):
                    return FunctionSupportInfo(supported=False, reason="extension_not_installed")
                if requirement.min_ext_version is not None:
                    installed = self.get_extension_version(requirement.extension)
                    if installed is None or self._compare_versions(installed, requirement.min_ext_version) < 0:
                        return FunctionSupportInfo(supported=False, reason="extension_version_insufficient")

        return FunctionSupportInfo(supported=True)
