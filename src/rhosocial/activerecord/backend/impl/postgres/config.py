# src/rhosocial/activerecord/backend/impl/postgres/config.py
"""postgres connection configuration."""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from rhosocial.activerecord.backend.config import ConnectionConfig, ConnectionPoolMixin


@dataclass
class PostgresSSLMixin:
    """Mixin implementing postgres-specific SSL/TLS options."""
    sslmode: Optional[str] = None  # 'disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'
    sslcert: Optional[str] = None
    sslkey: Optional[str] = None
    sslrootcert: Optional[str] = None
    sslcrl: Optional[str] = None
    sslcompression: Optional[bool] = None


@dataclass
class PostgresConnectionMixin:
    """Mixin implementing postgres-specific connection options."""
    application_name: Optional[str] = None
    target_session_attrs: Optional[str] = None  # 'any', 'primary', 'standby', 'prefer-standby'
    connect_timeout: Optional[int] = None
    client_encoding: Optional[str] = None
    service: Optional[str] = None
    gssencmode: Optional[str] = None
    channel_binding: Optional[str] = None


@dataclass
class PostgresConnectionConfig(ConnectionConfig, ConnectionPoolMixin, PostgresSSLMixin, PostgresConnectionMixin):
    """postgres-specific connection configuration with dedicated postgres options.

    This class extends the base ConnectionConfig with postgres-specific
    parameters and functionality.
    """

    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary, including postgres-specific parameters."""
        # Get base config
        config_dict = super().to_dict()
        
        # Add postgres-specific parameters
        pg_params = {
            'sslmode': self.sslmode,
            'sslcert': self.sslcert,
            'sslkey': self.sslkey,
            'sslrootcert': self.sslrootcert,
            'sslcrl': self.sslcrl,
            'sslcompression': self.sslcompression,
            'application_name': self.application_name,
            'target_session_attrs': self.target_session_attrs,
            'connect_timeout': self.connect_timeout,
            'client_encoding': self.client_encoding,
            'service': self.service,
            'gssencmode': self.gssencmode,
            'channel_binding': self.channel_binding,
        }
        
        # Only include non-None values
        for key, value in pg_params.items():
            if value is not None:
                config_dict[key] = value
        
        return config_dict

    def to_connection_string(self) -> str:
        """Convert configuration to postgres connection URI.

        Returns:
            postgres connection URI string

        Example:
            postgres://user:password@localhost:5432/mydb?sslmode=require
        """
        # Build basic connection string
        if self.username and self.password:
            conn_str = f"postgres://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        elif self.username:
            conn_str = f"postgres://{self.username}@{self.host}:{self.port}/{self.database}"
        else:
            conn_str = f"postgres://{self.host}:{self.port}/{self.database}"

        # Add query parameters from postgres-specific parameters
        params = []
        if self.sslmode:
            params.append(f"sslmode={self.sslmode}")
        if self.sslcert:
            params.append(f"sslcert={self.sslcert}")
        if self.sslkey:
            params.append(f"sslkey={self.sslkey}")
        if self.sslrootcert:
            params.append(f"sslrootcert={self.sslrootcert}")
        if self.sslcrl:
            params.append(f"sslcrl={self.sslcrl}")
        if self.sslcompression is not None:
            params.append(f"sslcompression={self.sslcompression}")
        if self.application_name:
            params.append(f"application_name={self.application_name}")
        if self.target_session_attrs:
            params.append(f"target_session_attrs={self.target_session_attrs}")
        if self.connect_timeout:
            params.append(f"connect_timeout={self.connect_timeout}")
        if self.client_encoding:
            params.append(f"client_encoding={self.client_encoding}")
        if self.service:
            params.append(f"service={self.service}")
        if self.gssencmode:
            params.append(f"gssencmode={self.gssencmode}")
        if self.channel_binding:
            params.append(f"channel_binding={self.channel_binding}")
        
        # Add any additional options
        if self.options:
            for key, value in self.options.items():
                if key not in params:  # Avoid duplicates
                    params.append(f"{key}={value}")

        if params:
            conn_str += "?" + "&".join(params)

        return conn_str

    def validate(self) -> bool:
        """Validate postgres connection configuration.

        Returns:
            True if configuration is valid

        Raises:
            ValueError: If configuration is invalid
        """
        if not self.host:
            raise ValueError("Host is required for postgres connection")

        if not self.database:
            raise ValueError("Database name is required for postgres connection")

        if self.port is not None and (self.port <= 0 or self.port > 65535):
            raise ValueError(f"Invalid port number: {self.port}")

        # Validate SSL mode if specified
        if self.sslmode:
            valid_ssl_modes = {
                "disable", "allow", "prefer", "require",
                "verify-ca", "verify-full"
            }
            if self.sslmode not in valid_ssl_modes:
                raise ValueError(
                    f"Invalid sslmode: {self.sslmode}. "
                    f"Must be one of: {', '.join(valid_ssl_modes)}"
                )
        
        # Validate target_session_attrs if specified
        if self.target_session_attrs:
            valid_session_attrs = {
                "any", "primary", "standby", "prefer-standby", "read-write", "read-only"
            }
            if self.target_session_attrs not in valid_session_attrs:
                raise ValueError(
                    f"Invalid target_session_attrs: {self.target_session_attrs}. "
                    f"Must be one of: {', '.join(valid_session_attrs)}"
                )

        return True

    def get_pool_config(self) -> Dict[str, Any]:
        """Get connection pool configuration for postgres.

        Returns:
            Dictionary of pool configuration parameters
        """
        pool_config = {
            "min_size": self.pool_min_size or 1,
            "max_size": self.pool_max_size or 10,
            "timeout": self.pool_timeout or 30.0,
        }

        return pool_config