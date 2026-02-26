"""Configuration constants for Oracle Lakebridge Extractor."""

from typing import Dict, Any

# Supported Oracle object types and their output configuration
OBJECT_TYPES: Dict[str, Dict[str, str]] = {
    'TABLE': {'folder': 'tables', 'ext': '.sql'},
    'VIEW': {'folder': 'views', 'ext': '.sql'},
    'MATERIALIZED_VIEW': {'folder': 'materialized_views', 'ext': '.sql'},
    'PROCEDURE': {'folder': 'procedures', 'ext': '.sql'},
    'FUNCTION': {'folder': 'functions', 'ext': '.sql'},
    'PACKAGE': {'folder': 'packages', 'ext': '.sql'},
    'TRIGGER': {'folder': 'triggers', 'ext': '.sql'},
    'SEQUENCE': {'folder': 'sequences', 'ext': '.sql'},
    'SYNONYM': {'folder': 'synonyms', 'ext': '.sql'},
    'TYPE': {'folder': 'types', 'ext': '.sql'},
    'INDEX': {'folder': 'indexes', 'ext': '.sql'},
    'DATABASE LINK': {'folder': 'db_links', 'ext': '.sql'},
}

# DBMS_METADATA transform parameters for clean DDL output
TRANSFORM_PARAMS: Dict[str, Any] = {
    'SEGMENT_ATTRIBUTES': False,  # Remove tablespace, storage clauses
    'STORAGE': False,             # Remove storage-specific settings
    'TABLESPACE': False,          # Remove tablespace references
    'PRETTY': True,               # Format output nicely
    'SQLTERMINATOR': True,        # Add semicolons
    'CONSTRAINTS_AS_ALTER': False, # Keep constraints inline
    'REF_CONSTRAINTS': True,      # Include foreign keys
}

# DDL patterns to remove during cleanup
DDL_CLEANUP_PATTERNS = [
    r'\s+PCTFREE\s+\d+',
    r'\s+PCTUSED\s+\d+',
    r'\s+INITRANS\s+\d+',
    r'\s+MAXTRANS\s+\d+',
    r'\s+COMPRESS(\s+\d+)?',
    r'\s+NOCOMPRESS',
    r'\s+LOGGING',
    r'\s+NOLOGGING',
    r'\s+CACHE',
    r'\s+NOCACHE',
    r'\s+PARALLEL(\s+\d+)?',
    r'\s+NOPARALLEL',
    r'\s+MONITORING',
    r'\s+NOMONITORING',
    r'\s+SEGMENT\s+CREATION\s+(IMMEDIATE|DEFERRED)',
    r'\s+FLASH_CACHE\s+\w+',
    r'\s+CELL_FLASH_CACHE\s+\w+',
    r'\s+ENABLE\s+ROW\s+MOVEMENT',
    r'\s+DISABLE\s+ROW\s+MOVEMENT',
]

# Oracle to Databricks type mappings (reference)
TYPE_MAPPINGS = {
    'NUMBER': 'DECIMAL or BIGINT',
    'NUMBER(p,s)': 'DECIMAL(p,s)',
    'VARCHAR2(n)': 'STRING',
    'CHAR(n)': 'STRING',
    'DATE': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMP',
    'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP',
    'CLOB': 'STRING',
    'BLOB': 'BINARY',
    'RAW': 'BINARY',
    'LONG': 'STRING',
    'LONG RAW': 'BINARY',
    'XMLTYPE': 'STRING',
    'BOOLEAN': 'BOOLEAN',
    'BINARY_FLOAT': 'FLOAT',
    'BINARY_DOUBLE': 'DOUBLE',
    'INTERVAL': 'STRING',
    'ROWID': 'STRING',
}

# Default connection settings
DEFAULT_PORT = 1521
DEFAULT_ENCODING = 'UTF-8'
