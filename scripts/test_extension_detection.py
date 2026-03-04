#!/usr/bin/env python3
# scripts/test_extension_detection.py
"""
Test script to detect PostgreSQL extensions.

This script connects to a PostgreSQL database using configuration from
tests/config/postgres_scenarios.yaml and displays installed extensions.
"""
import yaml
import sys
import os

# Add src to PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python-activerecord', 'src'))

from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


def load_scenario_config(yaml_path: str, scenario_name: str = None):
    """Load connection configuration from YAML file.
    
    Also loads commented-out scenarios (lines starting with '# postgres_')
    """
    with open(yaml_path, 'r') as f:
        content = f.read()
    
    # Parse the YAML content
    config_data = yaml.safe_load(content)
    scenarios = config_data.get('scenarios', {})
    
    # Also parse commented scenarios
    import re
    # Match commented scenario blocks
    lines = content.split('\n')
    current_scenario = None
    current_config = {}
    
    for line in lines:
        # Check for commented scenario header: # postgres_XX:
        scenario_match = re.match(r'#\s*(postgres_\d+):\s*$', line)
        if scenario_match:
            # Save previous scenario if exists
            if current_scenario and current_config:
                if current_scenario not in scenarios:
                    scenarios[current_scenario] = current_config
            current_scenario = scenario_match.group(1)
            current_config = {}
            continue
        
        # Parse config lines within commented scenarios
        if current_scenario:
            # Skip pure comment lines (not key: value)
            if line.strip().startswith('#') and ':' in line:
                # Remove the leading #
                config_line = line.strip()[1:].strip()
                # Skip lines that are just comments
                if config_line.startswith('#') or 'Replace with your password' in config_line:
                    continue
                # Parse key: value
                if ':' in config_line:
                    key, value = config_line.split(':', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key:
                        current_config[key] = value
    
    # Save last scenario
    if current_scenario and current_config:
        if current_scenario not in scenarios:
            scenarios[current_scenario] = current_config
    
    # If no scenario specified, use the first active one
    if scenario_name is None:
        scenario_name = list(scenarios.keys())[0]
        print(f"No scenario specified, using: {scenario_name}")
    
    if scenario_name not in scenarios:
        print(f"Error: Scenario '{scenario_name}' not found")
        print(f"Available scenarios: {list(scenarios.keys())}")
        sys.exit(1)
    
    return scenario_name, scenarios[scenario_name]


def main():
    # Path to config file
    config_path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'tests',
        'config',
        'postgres_scenarios.yaml'
    )
    
    # Allow command line argument to specify scenario
    scenario_name = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Load configuration
    scenario_name, scenario_config = load_scenario_config(config_path, scenario_name)
    
    print(f"\n{'='*60}")
    print(f"Connecting to PostgreSQL scenario: {scenario_name}")
    print(f"{'='*60}")
    print(f"Host: {scenario_config['host']}")
    print(f"Port: {scenario_config['port']}")
    print(f"Database: {scenario_config['database']}")
    print(f"Username: {scenario_config['username']}")
    print(f"{'='*60}\n")
    
    # Create connection config
    conn_config = PostgresConnectionConfig(
        host=scenario_config['host'],
        port=scenario_config['port'],
        database=scenario_config['database'],
        username=scenario_config['username'],
        password=scenario_config['password'],
        sslmode=scenario_config.get('sslmode', 'prefer')
    )
    
    # Create backend
    backend = PostgresBackend(connection_config=conn_config)
    
    try:
        # Connect to database
        backend.connect()
        print("Connected successfully!\n")
        
        # Get server version
        version = backend.get_server_version()
        print(f"PostgreSQL Version: {'.'.join(map(str, version))}")
        
        # Introspect and detect extensions
        print("\nDetecting extensions...")
        backend.introspect_and_adapt()
        
        # Get extension information
        dialect = backend.dialect
        extensions = dialect._extensions
        
        print(f"\n{'='*60}")
        print("Extension Detection Results")
        print(f"{'='*60}")
        
        # Separate installed and not installed extensions
        installed = []
        not_installed = []
        
        for name, info in extensions.items():
            if info.installed:
                installed.append((name, info.version, info.schema))
            else:
                not_installed.append(name)
        
        if installed:
            print("\nInstalled Extensions:")
            print("-" * 40)
            for name, version, schema in installed:
                print(f"  {name}")
                print(f"    Version: {version}")
                print(f"    Schema: {schema}")
        else:
            print("\nNo extensions installed.")
        
        if not_installed:
            print(f"\nKnown but not installed extensions:")
            print("-" * 40)
            for name in not_installed:
                print(f"  {name}")
        
        # Test extension check methods
        print(f"\n{'='*60}")
        print("Extension Support Tests")
        print(f"{'='*60}")
        
        print(f"\nPostGIS (spatial):")
        print(f"  is_extension_installed('postgis'): {dialect.is_extension_installed('postgis')}")
        print(f"  supports_geometry_type(): {dialect.supports_geometry_type()}")
        print(f"  supports_spatial_functions(): {dialect.supports_spatial_functions()}")
        
        print(f"\npgvector (vector):")
        print(f"  is_extension_installed('vector'): {dialect.is_extension_installed('vector')}")
        print(f"  supports_vector_type(): {dialect.supports_vector_type()}")
        print(f"  supports_hnsw_index(): {dialect.supports_hnsw_index()}")
        
        print(f"\npg_trgm (trigram):")
        print(f"  is_extension_installed('pg_trgm'): {dialect.is_extension_installed('pg_trgm')}")
        print(f"  supports_trigram_similarity(): {dialect.supports_trigram_similarity()}")
        
        print(f"\nhstore (key-value):")
        print(f"  is_extension_installed('hstore'): {dialect.is_extension_installed('hstore')}")
        print(f"  supports_hstore_type(): {dialect.supports_hstore_type()}")
        
        print(f"\n{'='*60}")
        
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        # Disconnect
        if backend._connection:
            backend.disconnect()
            print("\nDisconnected successfully!")


if __name__ == '__main__':
    main()
