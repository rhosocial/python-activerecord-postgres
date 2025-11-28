
import pytest
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig

def test_pg_config_to_dict():
    """Test that postgres-specific parameters are added to the dictionary."""
    config = PostgresConnectionConfig(
        database="test_db",
        sslmode="require",
        application_name="my_app"
    )
    
    config_dict = config.to_dict()
    
    # Test that the pg-specific parameters are present
    assert config_dict['sslmode'] == 'require'
    assert config_dict['application_name'] == 'my_app'
    
    # Test that parameters with default value None are not present
    assert 'sslcert' not in config_dict

def test_pg_config_to_connection_string_with_params():
    """Test connection string builds correctly with various pg parameters."""
    config = PostgresConnectionConfig(
        host="localhost",
        port=5432,
        database="test_db",
        username="user",
        sslmode="verify-full",
        application_name="my_app",
        connect_timeout=20,
        options={'custom_opt': 'custom_val'}
    )
    
    conn_str = config.to_connection_string()
    base_url, params_str = conn_str.split('?')
    params = dict(p.split('=') for p in params_str.split('&'))
    
    assert base_url == "postgres://user@localhost:5432/test_db" # Password is None, so it's omitted
    assert params['sslmode'] == 'verify-full'
    assert params['application_name'] == 'my_app'
    assert params['connect_timeout'] == '20'
    assert params['custom_opt'] == 'custom_val'

def test_pg_config_to_connection_string_no_auth():
    """Test connection string with no username or password."""
    config = PostgresConnectionConfig(host="localhost", port=5432, database="test_db")
    assert config.to_connection_string() == "postgres://localhost:5432/test_db"

def test_validation_missing_host():
    """Test validation fails if host is missing."""
    config = PostgresConnectionConfig(database="test_db", host=None)
    with pytest.raises(ValueError, match="Host is required"):
        config.validate()

def test_validation_missing_database():
    """Test validation fails if database is missing."""
    config = PostgresConnectionConfig(host="localhost", database=None)
    with pytest.raises(ValueError, match="Database name is required"):
        config.validate()

@pytest.mark.parametrize("invalid_port", [0, -1, 65536])
def test_validation_invalid_port(invalid_port):
    """Test validation fails for invalid port numbers."""
    config = PostgresConnectionConfig(host="localhost", database="test_db", port=invalid_port)
    with pytest.raises(ValueError, match=f"Invalid port number: {invalid_port}"):
        config.validate()

def test_validation_invalid_sslmode():
    """Test validation fails for an invalid sslmode."""
    config = PostgresConnectionConfig(host="localhost", database="test_db", sslmode="invalid_mode")
    with pytest.raises(ValueError, match="Invalid sslmode"):
        config.validate()

def test_validation_invalid_session_attrs():
    """Test validation fails for invalid target_session_attrs."""
    config = PostgresConnectionConfig(
        host="localhost",
        database="test_db",
        target_session_attrs="invalid_attr",
    )
    with pytest.raises(ValueError, match="Invalid target_session_attrs"):
        config.validate()
        
@pytest.mark.parametrize("valid_session_attr", ["any", "primary", "standby", "prefer-standby", "read-write", "read-only"])
def test_validation_valid_session_attrs(valid_session_attr):
    """Test validation succeeds for valid target_session_attrs."""
    config = PostgresConnectionConfig(
        host="localhost",
        database="test_db",
        target_session_attrs=valid_session_attr,
    )
    assert config.validate() is True

def test_validation_success():
    """Test a valid configuration passes validation."""
    config = PostgresConnectionConfig(
        host="localhost",
        database="test_db",
        port=5432,
        sslmode="require"
    )
    assert config.validate() is True

def test_get_pool_config_defaults():
    """Test getting pool config with default values."""
    config = PostgresConnectionConfig(host="localhost", database="test_db")
    pool_config = config.get_pool_config()
    assert pool_config == {
        "min_size": 1,
        "max_size": 10,
        "timeout": 30.0,
    }

def test_get_pool_config_custom():
    """Test getting pool config with custom values from the mixin."""
    config = PostgresConnectionConfig(
        host="localhost",
        database="test_db",
        pool_min_size=5,
        pool_max_size=20,
        pool_timeout=60.5,
    )
    pool_config = config.get_pool_config()
    assert pool_config == {
        "min_size": 5,
        "max_size": 20,
        "timeout": 60.5,
    }

def test_connection_string_password_behavior():
    """Test that connection string correctly handles password."""
    # With password
    config = PostgresConnectionConfig(host="h", database="d", username="u", password="p", port=5432)
    assert config.to_connection_string() == "postgres://u:p@h:5432/d"
    
    # With username but no password
    config = PostgresConnectionConfig(host="h", database="d", username="u", port=5432)
    assert config.to_connection_string() == "postgres://u@h:5432/d"

def test_connection_string_all_pg_params():
    """Test that all pg-specific boolean and string params are in the conn string."""
    config = PostgresConnectionConfig(
        host="h", database="d", port=1,
        sslmode='require',
        sslcert='cert.pem',
        sslkey='key.pem',
        sslrootcert='root.crt',
        sslcrl='crl.pem',
        sslcompression=True,
        application_name='app',
        target_session_attrs='primary',
        connect_timeout=15,
        client_encoding='latin1',
        service='my_service',
        gssencmode='prefer',
        channel_binding='require'
    )
    conn_str = config.to_connection_string()
    
    assert "sslmode=require" in conn_str
    assert "sslcert=cert.pem" in conn_str
    assert "sslkey=key.pem" in conn_str
    assert "sslrootcert=root.crt" in conn_str
    assert "sslcrl=crl.pem" in conn_str
    assert "sslcompression=True" in conn_str
    assert "application_name=app" in conn_str
    assert "target_session_attrs=primary" in conn_str
    assert "connect_timeout=15" in conn_str
    assert "client_encoding=latin1" in conn_str
    assert "service=my_service" in conn_str
    assert "gssencmode=prefer" in conn_str
    assert "channel_binding=require" in conn_str

