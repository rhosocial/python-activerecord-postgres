#!/usr/bin/env python3
# scripts/extension_comparison.py
"""
Generate a comparison table of PostgreSQL extensions across versions.

This script connects to all PostgreSQL servers defined in the config file
and generates a comparison table showing installed extensions.
"""
import yaml
import sys
import os

# Add src to PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'python-activerecord', 'src'))

from rhosocial.activerecord.backend.impl.postgres import PostgresBackend
from rhosocial.activerecord.backend.impl.postgres.config import PostgresConnectionConfig


def load_all_scenarios(yaml_path: str):
    """Load all scenarios from YAML file, including commented ones."""
    import re
    with open(yaml_path, 'r') as f:
        content = f.read()
    
    config_data = yaml.safe_load(content)
    scenarios = config_data.get('scenarios', {})
    
    # Parse commented scenarios
    lines = content.split('\n')
    current_scenario = None
    current_config = {}
    
    for line in lines:
        scenario_match = re.match(r'#\s*(postgres_\d+):\s*$', line)
        if scenario_match:
            if current_scenario and current_config:
                if current_scenario not in scenarios:
                    scenarios[current_scenario] = current_config
            current_scenario = scenario_match.group(1)
            current_config = {}
            continue
        
        if current_scenario:
            if line.strip().startswith('#') and ':' in line:
                config_line = line.strip()[1:].strip()
                if config_line.startswith('#') or 'Replace with your password' in config_line:
                    continue
                if ':' in config_line:
                    key, value = config_line.split(':', 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")
                    if key:
                        current_config[key] = value
    
    if current_scenario and current_config:
        if current_scenario not in scenarios:
            scenarios[current_scenario] = current_config
    
    return scenarios


def get_extension_info(scenario_config):
    """Connect to database and get extension info."""
    conn_config = PostgresConnectionConfig(
        host=scenario_config['host'],
        port=scenario_config['port'],
        database=scenario_config['database'],
        username=scenario_config['username'],
        password=scenario_config['password'],
        sslmode=scenario_config.get('sslmode', 'prefer')
    )
    
    backend = PostgresBackend(connection_config=conn_config)
    try:
        backend.connect()
        backend.introspect_and_adapt()
        
        version = backend.get_server_version()
        extensions = backend.dialect._extensions
        
        installed = {}
        for name, info in extensions.items():
            if info.installed:
                installed[name] = {
                    'version': info.version,
                    'schema': info.schema
                }
        
        return version, installed
    finally:
        if backend._connection:
            backend.disconnect()


def main():
    config_path = os.path.join(
        os.path.dirname(__file__),
        '..',
        'tests',
        'config',
        'postgres_scenarios.yaml'
    )
    
    scenarios = load_all_scenarios(config_path)
    
    # Define the extensions we care about
    extensions_of_interest = ['plpgsql', 'postgis', 'vector', 'pg_trgm', 'hstore', 'uuid-ossp', 'pgcrypto']
    
    # Collect data from all scenarios
    results = {}
    for scenario_name in sorted(scenarios.keys(), key=lambda x: int(x.split('_')[1])):
        print(f"Checking {scenario_name}...", file=sys.stderr)
        scenario_config = scenarios[scenario_name]
        try:
            version, installed = get_extension_info(scenario_config)
            results[scenario_name] = {
                'version': version,
                'extensions': installed
            }
        except Exception as e:
            print(f"Error checking {scenario_name}: {e}", file=sys.stderr)
            results[scenario_name] = {'error': str(e)}
    
    # Generate comparison table
    print("\n" + "=" * 80)
    print("PostgreSQL Extension Comparison Table")
    print("=" * 80)
    
    # Header
    header = "| Version | PG Version | " + " | ".join(ext.ljust(10) for ext in extensions_of_interest) + " |"
    separator = "|" + "-" * 9 + "|" + "-" * 13 + "|" + "|".join("-" * 11 for _ in extensions_of_interest) + "|"
    
    print(header)
    print(separator)
    
    # Rows
    for scenario_name in sorted(results.keys(), key=lambda x: int(x.split('_')[1])):
        data = results[scenario_name]
        if 'error' in data:
            print(f"| {scenario_name} | ERROR | {data['error'][:60]}...")
            continue
        
        version_str = '.'.join(map(str, data['version']))
        row = f"| {scenario_name} | {version_str.ljust(11)} |"
        
        for ext in extensions_of_interest:
            if ext in data['extensions']:
                info = data['extensions'][ext]
                cell = f"{info['version']}"
            else:
                cell = "-"
            row += f" {cell.ljust(10)} |"
        
        print(row)
    
    print(separator)
    
    # Summary
    print("\nSummary:")
    print("-" * 40)
    
    # Count installed extensions per type
    ext_counts = {}
    for ext in extensions_of_interest:
        count = sum(1 for data in results.values() 
                    if 'extensions' in data and ext in data['extensions'])
        ext_counts[ext] = count
    
    for ext, count in ext_counts.items():
        total = len(results)
        print(f"  {ext}: {count}/{total} servers ({count*100//total}%)")
    
    # Print detailed info for installed extensions
    print("\nDetailed Extension Information:")
    print("-" * 60)
    
    for scenario_name in sorted(results.keys(), key=lambda x: int(x.split('_')[1])):
        data = results[scenario_name]
        if 'extensions' in data and data['extensions']:
            version_str = '.'.join(map(str, data['version']))
            print(f"\n{scenario_name} (PostgreSQL {version_str}):")
            for ext, info in sorted(data['extensions'].items()):
                print(f"  - {ext}: version={info['version']}, schema={info['schema']}")


if __name__ == '__main__':
    main()
