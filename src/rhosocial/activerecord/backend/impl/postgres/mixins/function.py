# src/rhosocial/activerecord/backend/impl/postgres/mixins/function.py
"""PostgreSQL function support detection mixin."""

from typing import Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from rhosocial.activerecord.backend.impl.postgres.function_versions import (
        FunctionSupportInfo,
        FunctionVersionRequirement,
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

    def _is_postgres_function_supported(self, func_name: str) -> bool:
        """Check if a PostgreSQL function is supported based on version and extensions.

        Checks:
        1. PostgreSQL server version (for built-in and version-gated functions)
        2. Extension installation and version (for extension-provided functions)

        For extension functions, requires _extensions to have been populated
        via introspect_and_adapt(). If _extensions is not available,
        extension functions return False (cannot confirm availability).

        Args:
            func_name: Name of the PostgreSQL function

        Returns:
            True if supported, False otherwise

        """
        requirement = self._POSTGRES_FUNCTION_VERSIONS.get(func_name)
        if requirement is None:
            return True  # Unregistered functions default to supported

        # Step 1: Check PostgreSQL server version
        if requirement.min_pg_version is not None and self.version < requirement.min_pg_version:
            return False
        if requirement.max_pg_version is not None and self.version > requirement.max_pg_version:
            return False

        # Step 2: Check extension requirements
        if requirement.extension is not None:
            return self._check_extension_requirement(requirement)

        return True

    def _check_extension_requirement(
        self, requirement: "FunctionVersionRequirement"
    ) -> bool:
        """Check extension requirements based on introspect_and_adapt() results.

        Requires _extensions to have been populated via introspect_and_adapt().
        If _extensions is not available, returns False (cannot confirm
        extension availability without probing the database).
        """
        ext_name = requirement.extension

        # _extensions is populated by introspect_and_adapt()
        # If the attribute doesn't exist, introspect_and_adapt() hasn't been called
        if not hasattr(self, "_extensions"):
            return False

        # Use feature-level check if ext_feature is specified
        if requirement.ext_feature is not None:
            return self.check_extension_feature(ext_name, requirement.ext_feature)

        # Check extension installed
        if not self.is_extension_installed(ext_name):
            return False

        # Check extension version
        if requirement.min_ext_version is not None:
            installed = self.get_extension_version(ext_name)
            if installed is None:
                return False
            return self._compare_versions(installed, requirement.min_ext_version) >= 0

        return True
