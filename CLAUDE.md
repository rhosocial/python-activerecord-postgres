# Project Overview: rhosocial-activerecord-postgres

## Project Name
- **Repository Name**: python-activerecord-postgres
- **Python Package Name**: rhosocial-activerecord-postgres

## Project Purpose

This project is a PostgreSQL backend implementation for the `rhosocial-activerecord` Python package. It provides PostgreSQL database support with the elegant ActiveRecord pattern interface.

## Key Design Principles

1. **Backend Implementation**: Extends core ActiveRecord with PostgreSQL-specific features
2. **Driver**: Uses `psycopg` (version 3) for database connectivity
3. **Namespace Package**: Integrates with the rhosocial namespace package architecture

## Current Status

This project is under active development. Key features implemented:

- Basic CRUD operations
- Connection management
- Transaction support
- Schema introspection
- PostgreSQL-specific data types
- Array types
- JSON/JSONB
- Range types
- PostGIS spatial types
- UUID types
- HStore
- Full-text search

## Version Control and Changelog

This project adheres to the same version control, branching, commit message, and changelog management standards as the main `python-activerecord` project.