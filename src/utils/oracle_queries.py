"""Oracle system queries for extracting database object metadata."""

from typing import Dict


class OracleQueries:
    """Collection of Oracle system queries for metadata extraction."""

    # Query to get tables (excluding nested, secondary, and recycled)
    TABLES = """
        SELECT owner, table_name as object_name
        FROM all_tables
        WHERE owner = :schema
        AND nested = 'NO'
        AND secondary = 'N'
        AND table_name NOT LIKE 'BIN$%'
        ORDER BY table_name
    """

    # Query to get views
    VIEWS = """
        SELECT owner, view_name as object_name
        FROM all_views
        WHERE owner = :schema
        ORDER BY view_name
    """

    # Query to get standalone procedures (not packaged)
    PROCEDURES = """
        SELECT owner, object_name
        FROM all_procedures
        WHERE owner = :schema
        AND object_type = 'PROCEDURE'
        AND procedure_name IS NULL
        ORDER BY object_name
    """

    # Query to get standalone functions (not packaged)
    FUNCTIONS = """
        SELECT owner, object_name
        FROM all_procedures
        WHERE owner = :schema
        AND object_type = 'FUNCTION'
        AND procedure_name IS NULL
        ORDER BY object_name
    """

    # Query to get packages
    PACKAGES = """
        SELECT owner, object_name
        FROM all_objects
        WHERE owner = :schema
        AND object_type = 'PACKAGE'
        ORDER BY object_name
    """

    # Query to get indexes (excluding LOB and system-generated)
    INDEXES = """
        SELECT owner, index_name as object_name
        FROM all_indexes
        WHERE owner = :schema
        AND index_type NOT IN ('LOB')
        AND index_name NOT LIKE 'SYS_%'
        AND generated = 'N'
        ORDER BY index_name
    """

    # Query to get sequences
    SEQUENCES = """
        SELECT sequence_owner as owner, sequence_name as object_name
        FROM all_sequences
        WHERE sequence_owner = :schema
        ORDER BY sequence_name
    """

    # Query to get triggers
    TRIGGERS = """
        SELECT owner, trigger_name as object_name
        FROM all_triggers
        WHERE owner = :schema
        ORDER BY trigger_name
    """

    # Query to get types
    TYPES = """
        SELECT owner, type_name as object_name
        FROM all_types
        WHERE owner = :schema
        ORDER BY type_name
    """

    # Query to get materialized views
    MATERIALIZED_VIEWS = """
        SELECT owner, mview_name as object_name
        FROM all_mviews
        WHERE owner = :schema
        ORDER BY mview_name
    """

    # Query to get synonyms
    SYNONYMS = """
        SELECT owner, synonym_name as object_name
        FROM all_synonyms
        WHERE owner = :schema
        ORDER BY synonym_name
    """

    # Query to get database links
    DATABASE_LINKS = """
        SELECT owner, db_link as object_name
        FROM all_db_links
        WHERE owner = :schema
        ORDER BY db_link
    """

    # Query to get table details for inventory
    TABLE_DETAILS = """
        SELECT
            t.table_name,
            t.num_rows,
            t.blocks,
            t.avg_row_len,
            t.last_analyzed,
            (SELECT COUNT(*) FROM all_tab_columns c WHERE c.owner = t.owner AND c.table_name = t.table_name) as column_count,
            (SELECT COUNT(*) FROM all_indexes i WHERE i.owner = t.owner AND i.table_name = t.table_name) as index_count,
            (SELECT COUNT(*) FROM all_constraints c WHERE c.owner = t.owner AND c.table_name = t.table_name AND c.constraint_type = 'R') as fk_count
        FROM all_tables t
        WHERE t.owner = :schema
        AND t.nested = 'NO'
        AND t.secondary = 'N'
        AND t.table_name NOT LIKE 'BIN$%'
        ORDER BY t.table_name
    """

    # Query to get procedure/function details
    PROCEDURE_DETAILS = """
        SELECT
            p.object_name,
            p.object_type,
            p.created,
            p.last_ddl_time,
            p.status,
            (SELECT COUNT(*) FROM all_source s
             WHERE s.owner = p.owner AND s.name = p.object_name AND s.type = p.object_type) as line_count
        FROM all_objects p
        WHERE p.owner = :schema
        AND p.object_type IN ('PROCEDURE', 'FUNCTION')
        ORDER BY p.object_type, p.object_name
    """

    # Query to get source code metrics
    SOURCE_CODE_METRICS = """
        SELECT
            type,
            name,
            COUNT(*) as line_count,
            SUM(CASE WHEN REGEXP_LIKE(text, '^\s*--') THEN 1 ELSE 0 END) as comment_lines
        FROM all_source
        WHERE owner = :schema
        GROUP BY type, name
        ORDER BY type, name
    """

    # Query to get package details including body
    PACKAGE_DETAILS = """
        SELECT
            o.object_name,
            o.created,
            o.last_ddl_time,
            o.status,
            (SELECT COUNT(*) FROM all_source s
             WHERE s.owner = o.owner AND s.name = o.object_name AND s.type = 'PACKAGE') as spec_lines,
            (SELECT COUNT(*) FROM all_source s
             WHERE s.owner = o.owner AND s.name = o.object_name AND s.type = 'PACKAGE BODY') as body_lines,
            (SELECT status FROM all_objects b
             WHERE b.owner = o.owner AND b.object_name = o.object_name AND b.object_type = 'PACKAGE BODY') as body_status
        FROM all_objects o
        WHERE o.owner = :schema
        AND o.object_type = 'PACKAGE'
        ORDER BY o.object_name
    """

    # Query to check if package body exists
    PACKAGE_BODY_EXISTS = """
        SELECT COUNT(*) as body_exists
        FROM all_objects
        WHERE owner = :schema
        AND object_name = :package_name
        AND object_type = 'PACKAGE BODY'
    """

    # Query to check if type body exists
    TYPE_BODY_EXISTS = """
        SELECT COUNT(*) as body_exists
        FROM all_objects
        WHERE owner = :schema
        AND object_name = :type_name
        AND object_type = 'TYPE BODY'
    """

    # Query mapping for object types
    QUERY_MAP: Dict[str, str] = {
        'TABLE': TABLES,
        'VIEW': VIEWS,
        'PROCEDURE': PROCEDURES,
        'FUNCTION': FUNCTIONS,
        'PACKAGE': PACKAGES,
        'INDEX': INDEXES,
        'SEQUENCE': SEQUENCES,
        'TRIGGER': TRIGGERS,
        'TYPE': TYPES,
        'MATERIALIZED_VIEW': MATERIALIZED_VIEWS,
        'SYNONYM': SYNONYMS,
        'DATABASE LINK': DATABASE_LINKS,
    }

    @classmethod
    def get_query(cls, object_type: str) -> str:
        """
        Get the appropriate query for an object type.

        Args:
            object_type: The Oracle object type

        Returns:
            The SQL query string for that object type

        Raises:
            KeyError: If the object type is not supported
        """
        if object_type not in cls.QUERY_MAP:
            raise KeyError(f"Unsupported object type: {object_type}")
        return cls.QUERY_MAP[object_type]
