"""DDL cleanup utilities for removing Oracle-specific storage and tablespace clauses."""

import re
from typing import List

from ..config import DDL_CLEANUP_PATTERNS


def clean_ddl(ddl: str) -> str:
    """
    Clean up Oracle DDL by removing storage-specific clauses.

    Removes Oracle-specific patterns that are not relevant for migration analysis:
    - PCTFREE, PCTUSED, INITRANS, MAXTRANS
    - COMPRESS/NOCOMPRESS
    - LOGGING/NOLOGGING
    - CACHE/NOCACHE
    - PARALLEL/NOPARALLEL
    - MONITORING/NOMONITORING
    - SEGMENT CREATION clauses
    - FLASH_CACHE settings
    - ROW MOVEMENT settings

    Args:
        ddl: The raw DDL string from Oracle DBMS_METADATA

    Returns:
        Cleaned DDL string with Oracle-specific storage clauses removed
    """
    if not ddl:
        return ""

    cleaned = ddl

    # Apply all cleanup patterns
    for pattern in DDL_CLEANUP_PATTERNS:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)

    # Remove TABLESPACE clauses (including quoted tablespace names)
    cleaned = re.sub(
        r'\s+TABLESPACE\s+"?[A-Za-z_][A-Za-z0-9_$#]*"?',
        '',
        cleaned,
        flags=re.IGNORECASE
    )

    # Remove USING INDEX TABLESPACE clauses
    cleaned = re.sub(
        r'\s+USING\s+INDEX\s+TABLESPACE\s+"?[A-Za-z_][A-Za-z0-9_$#]*"?',
        '',
        cleaned,
        flags=re.IGNORECASE
    )

    # Remove storage clauses with parentheses
    cleaned = re.sub(
        r'\s+STORAGE\s*\([^)]*\)',
        '',
        cleaned,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Remove LOB storage clauses
    cleaned = re.sub(
        r'\s+LOB\s*\([^)]*\)\s+STORE\s+AS\s+[^(]*\([^)]*\)',
        '',
        cleaned,
        flags=re.IGNORECASE | re.DOTALL
    )

    # Clean up multiple consecutive whitespace
    cleaned = re.sub(r'\n\s*\n\s*\n', '\n\n', cleaned)
    cleaned = re.sub(r'[ \t]+', ' ', cleaned)
    cleaned = re.sub(r' +\n', '\n', cleaned)

    # Clean up empty parentheses that might result from removal
    cleaned = re.sub(r'\(\s*\)', '', cleaned)

    # Ensure proper line endings
    cleaned = cleaned.strip()
    if cleaned and not cleaned.endswith(';'):
        cleaned += ';'

    return cleaned


def remove_schema_prefix(ddl: str, schema: str) -> str:
    """
    Optionally remove schema prefix from object references.

    Args:
        ddl: The DDL string
        schema: The schema name to remove

    Returns:
        DDL with schema prefix removed from object names
    """
    if not ddl or not schema:
        return ddl

    # Remove schema prefix (handles both quoted and unquoted)
    pattern = rf'"{schema}"\.|{schema}\.'
    return re.sub(pattern, '', ddl, flags=re.IGNORECASE)


def normalize_whitespace(ddl: str) -> str:
    """
    Normalize whitespace in DDL for consistent formatting.

    Args:
        ddl: The DDL string

    Returns:
        DDL with normalized whitespace
    """
    if not ddl:
        return ""

    # Normalize line endings
    ddl = ddl.replace('\r\n', '\n').replace('\r', '\n')

    # Remove trailing whitespace from each line
    lines = [line.rstrip() for line in ddl.split('\n')]

    # Remove excessive blank lines (more than 2 consecutive)
    result = []
    blank_count = 0
    for line in lines:
        if not line:
            blank_count += 1
            if blank_count <= 2:
                result.append(line)
        else:
            blank_count = 0
            result.append(line)

    return '\n'.join(result)


def clean_package_ddl(spec_ddl: str, body_ddl: str) -> str:
    """
    Combine and clean package specification and body DDL.

    Args:
        spec_ddl: Package specification DDL
        body_ddl: Package body DDL

    Returns:
        Combined and cleaned package DDL
    """
    parts = []

    if spec_ddl:
        cleaned_spec = clean_ddl(spec_ddl)
        parts.append("-- Package Specification")
        parts.append(cleaned_spec)

    if body_ddl:
        cleaned_body = clean_ddl(body_ddl)
        if parts:
            parts.append("\n")
        parts.append("-- Package Body")
        parts.append(cleaned_body)

    return '\n'.join(parts)


def clean_type_ddl(type_ddl: str, body_ddl: str = None) -> str:
    """
    Combine and clean type specification and body DDL.

    Args:
        type_ddl: Type specification DDL
        body_ddl: Type body DDL (if exists)

    Returns:
        Combined and cleaned type DDL
    """
    parts = []

    if type_ddl:
        cleaned_type = clean_ddl(type_ddl)
        parts.append(cleaned_type)

    if body_ddl:
        cleaned_body = clean_ddl(body_ddl)
        if parts:
            parts.append("\n")
        parts.append("-- Type Body")
        parts.append(cleaned_body)

    return '\n'.join(parts)
