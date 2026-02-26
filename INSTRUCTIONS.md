# INSTRUCTIONS.md - Oracle Lakebridge Extractor Repository Specification

## Overview

Build a complete GitHub repository for extracting Oracle database objects (DDL and metadata) to stage for the Databricks Lakebridge Analyzer. This tool helps organizations migrate from Oracle to Databricks by providing clean, well-organized SQL artifacts for complexity analysis.

---

## Repository Structure

```
oracle-lakebridge-extractor/
├── README.md                           # Main repository documentation
├── LICENSE                             # Apache 2.0 or MIT license
├── .gitignore                          # Python, Docker, IDE ignores
├── requirements.txt                    # Python dependencies
├── pyproject.toml                      # Modern Python packaging (optional)
│
├── src/
│   ├── __init__.py
│   ├── oracle_lakebridge_extractor.py  # Main extraction script
│   ├── config.py                       # Configuration constants
│   └── utils/
│       ├── __init__.py
│       ├── ddl_cleaner.py              # DDL cleanup utilities
│       ├── oracle_queries.py           # Oracle system queries
│       └── inventory.py                # Inventory/manifest generation
│
├── docker/
│   ├── docker-compose.yml              # Oracle container definitions
│   ├── setup.sh                        # Quick start script
│   ├── .env.example                    # Environment variables template
│   └── init-scripts/
│       └── init_healthcare_schema.sql  # Sample healthcare test schema
│
├── docs/
│   ├── QUICKSTART.md                   # Quick start guide
│   ├── ORACLE_SETUP.md                 # Detailed Oracle Docker setup
│   ├── LAKEBRIDGE_GUIDE.md             # Lakebridge Analyzer usage
│   └── TROUBLESHOOTING.md              # Common issues and solutions
│
├── examples/
│   ├── sample_output/                  # Example extraction output structure
│   │   └── .gitkeep
│   └── sample_config.json              # Example configuration file
│
└── tests/
    ├── __init__.py
    ├── test_extractor.py               # Unit tests for extractor
    ├── test_ddl_cleaner.py             # Unit tests for DDL cleanup
    └── conftest.py                     # Pytest fixtures
```

---

## Background Context: Lakebridge Analyzer

### What is Lakebridge?

Lakebridge is Databricks' free, open-source migration toolkit that helps organizations migrate from legacy data warehouses to Databricks SQL. It has three main components:

1. **Analyzer**: Scans legacy SQL/ETL assets and generates complexity reports
2. **Transpiler/Converter**: Converts legacy SQL (including PL/SQL) to Databricks SQL
3. **Reconciler**: Validates data integrity between source and target systems

### Lakebridge Oracle Support

| Feature | Support Level | Notes |
|---------|---------------|-------|
| Analyzer | ✅ Full | Select source tech "oracle" (option 21) |
| Transpiler | ✅ Full | PL/SQL → Databricks SQL via BladeBridge engine |
| Reconciler | ✅ Full | Direct Oracle connectivity for data validation |

### What the Analyzer Produces

The Lakebridge Analyzer generates an Excel report (.xlsx) with multiple worksheets:

- **Summary**: Object counts, complexity distribution (LOW/MEDIUM/HIGH/VERY_HIGH)
- **Tables**: Table inventory with estimated row counts
- **Views**: View definitions and complexity scores
- **Procedures**: Stored procedure analysis with line counts
- **Functions**: Function analysis
- **Packages**: Package complexity scores (spec + body combined)
- **Dependencies**: Object interdependency mapping
- **Complexity**: Detailed breakdown by construct type (cursors, loops, dynamic SQL, etc.)

### Analyzer Best Practices

1. **One SQL artifact per file** for granular analysis
2. **Organized folder structure** by object type
3. **Clean DDL** without Oracle-specific storage clauses
4. **Consistent file naming**: lowercase, `.sql` extension
5. **Include all object types**: tables, views, procedures, functions, packages, triggers, sequences, types, indexes, materialized views, synonyms

### Running the Analyzer

```bash
# Install Lakebridge
databricks labs install lakebridge

# Run analyzer
databricks labs lakebridge analyze \
    --source-directory /path/to/extracted/files \
    --source-tech oracle \
    --report-file /path/to/output/analysis_report
```

---

## Main Python Script Specification

### File: `src/oracle_lakebridge_extractor.py`

#### Core Functionality

1. **Three Operation Modes**:
   - **Direct extraction**: Connect via `oracledb` driver, extract DDL directly
   - **SQL*Plus script generation**: For environments without Python DB connectivity
   - **PL/SQL script generation**: For individual file exports via UTL_FILE

2. **Supported Oracle Object Types**:
   ```python
   OBJECT_TYPES = {
       'TABLE': {'folder': 'tables', 'ext': '.sql'},
       'VIEW': {'folder': 'views', 'ext': '.sql'},
       'MATERIALIZED_VIEW': {'folder': 'materialized_views', 'ext': '.sql'},
       'PROCEDURE': {'folder': 'procedures', 'ext': '.sql'},
       'FUNCTION': {'folder': 'functions', 'ext': '.sql'},
       'PACKAGE': {'folder': 'packages', 'ext': '.sql'},  # Include spec + body
       'TRIGGER': {'folder': 'triggers', 'ext': '.sql'},
       'SEQUENCE': {'folder': 'sequences', 'ext': '.sql'},
       'SYNONYM': {'folder': 'synonyms', 'ext': '.sql'},
       'TYPE': {'folder': 'types', 'ext': '.sql'},  # Include type body
       'INDEX': {'folder': 'indexes', 'ext': '.sql'},
       'DATABASE LINK': {'folder': 'db_links', 'ext': '.sql'},
   }
   ```

3. **DBMS_METADATA Transform Parameters** (for clean DDL):
   ```python
   TRANSFORM_PARAMS = {
       'SEGMENT_ATTRIBUTES': False,  # Remove tablespace, storage clauses
       'STORAGE': False,             # Remove storage-specific settings
       'TABLESPACE': False,          # Remove tablespace references
       'PRETTY': True,               # Format output nicely
       'SQLTERMINATOR': True,        # Add semicolons
       'CONSTRAINTS_AS_ALTER': False, # Keep constraints inline
       'REF_CONSTRAINTS': True,      # Include foreign keys
   }
   ```

4. **DDL Cleanup** - Remove Oracle-specific patterns:
   - `PCTFREE`, `PCTUSED`, `INITRANS`, `MAXTRANS`
   - `COMPRESS`/`NOCOMPRESS`
   - `LOGGING`/`NOLOGGING`
   - `CACHE`/`NOCACHE`
   - `PARALLEL`/`NOPARALLEL`
   - `MONITORING`/`NOMONITORING`
   - Excess whitespace

#### CLI Interface

```bash
# Direct extraction
python -m src.oracle_lakebridge_extractor \
    --host oracle.example.com \
    --port 1521 \
    --service ORCL \
    --user migration_user \
    --password secret \
    --schemas HR,FINANCE,CLINICAL \
    --output /data/lakebridge_staging \
    --verbose

# Generate SQL*Plus script
python -m src.oracle_lakebridge_extractor \
    --generate-sqlplus \
    --schemas HR,FINANCE \
    --output /data/lakebridge_staging

# Generate PL/SQL individual export script
python -m src.oracle_lakebridge_extractor \
    --generate-plsql \
    --schemas HR,FINANCE \
    --output /data/lakebridge_staging
```

#### Key Functions to Implement

```python
# Database connection context manager
class OracleConnection:
    def __init__(self, config: Dict): ...
    def __enter__(self): ...
    def __exit__(self, exc_type, exc_val, exc_tb): ...

# Setup DBMS_METADATA transform parameters
def setup_transform_params(cursor) -> None: ...

# Get list of objects for a schema
def get_schema_objects(cursor, schema: str, object_type: str) -> List[Tuple[str, str]]: ...

# Extract DDL using DBMS_METADATA.GET_DDL
def extract_ddl(cursor, object_type: str, object_name: str, schema: str) -> Optional[str]: ...

# Extract dependent DDL (indexes, constraints)
def extract_dependent_ddl(cursor, base_object_type: str, base_object_name: str, 
                          schema: str, dependent_type: str) -> Optional[str]: ...

# Extract package body separately
def extract_package_body(cursor, package_name: str, schema: str) -> Optional[str]: ...

# Clean up DDL output
def clean_ddl(ddl: str) -> str: ...

# Get table details for inventory
def get_table_details(cursor, schema: str) -> List[Dict]: ...

# Get procedure/function details
def get_procedure_details(cursor, schema: str) -> List[Dict]: ...

# Get source code metrics (line counts)
def get_source_code_metrics(cursor, schema: str) -> Dict: ...

# Main extractor class
class LakebridgeExtractor:
    def __init__(self, config: Dict, output_dir: str, schemas: List[str]): ...
    def extract_all(self) -> None: ...
    def _create_directory_structure(self) -> None: ...
    def _extract_schema(self, cursor, schema: str) -> None: ...
    def _extract_and_save_object(self, cursor, obj_type: str, obj_name: str, 
                                  schema: str, obj_config: Dict) -> bool: ...
    def _write_inventory(self) -> None: ...
    def _print_summary(self) -> None: ...

# Generate SQL*Plus export script
def generate_sqlplus_script(schemas: List[str], output_dir: str) -> str: ...

# Generate PL/SQL individual export script
def generate_individual_export_script(schemas: List[str], output_dir: str) -> str: ...
```

#### Oracle System Queries

```sql
-- Tables (exclude nested, secondary, recycled)
SELECT owner, table_name as object_name 
FROM all_tables 
WHERE owner = :schema
AND nested = 'NO'
AND secondary = 'N'
AND table_name NOT LIKE 'BIN$%';

-- Views
SELECT owner, view_name as object_name 
FROM all_views 
WHERE owner = :schema;

-- Procedures (standalone only)
SELECT owner, object_name 
FROM all_procedures 
WHERE owner = :schema 
AND object_type = 'PROCEDURE'
AND procedure_name IS NULL;

-- Functions (standalone only)
SELECT owner, object_name 
FROM all_procedures 
WHERE owner = :schema 
AND object_type = 'FUNCTION'
AND procedure_name IS NULL;

-- Packages
SELECT owner, object_name 
FROM all_objects 
WHERE owner = :schema 
AND object_type = 'PACKAGE';

-- Indexes (exclude LOB, system-generated)
SELECT owner, index_name as object_name 
FROM all_indexes 
WHERE owner = :schema
AND index_type NOT IN ('LOB')
AND index_name NOT LIKE 'SYS_%';

-- Sequences
SELECT sequence_owner as owner, sequence_name as object_name 
FROM all_sequences 
WHERE sequence_owner = :schema;

-- Triggers
SELECT owner, trigger_name as object_name 
FROM all_triggers 
WHERE owner = :schema;

-- Types
SELECT owner, type_name as object_name 
FROM all_types 
WHERE owner = :schema;

-- Materialized Views
SELECT owner, mview_name as object_name 
FROM all_mviews 
WHERE owner = :schema;

-- Synonyms
SELECT owner, synonym_name as object_name 
FROM all_synonyms 
WHERE owner = :schema;

-- Database Links
SELECT owner, db_link as object_name 
FROM all_db_links 
WHERE owner = :schema;
```

#### Output Structure

```
output_directory/
├── schema_name/
│   ├── tables/
│   │   ├── employees.sql
│   │   ├── departments.sql
│   │   └── ...
│   ├── views/
│   │   └── vw_employee_summary.sql
│   ├── procedures/
│   │   └── sp_update_salary.sql
│   ├── functions/
│   │   └── fn_calculate_age.sql
│   ├── packages/
│   │   └── pkg_hr_utils.sql        # Contains spec + body
│   ├── triggers/
│   │   └── trg_audit.sql
│   ├── sequences/
│   │   └── emp_seq.sql
│   ├── indexes/
│   │   └── idx_emp_name.sql
│   ├── types/
│   │   └── address_type.sql
│   ├── materialized_views/
│   │   └── mv_employee_summary.sql
│   └── synonyms/
│       └── emp_syn.sql
└── lakebridge_inventory.json       # Extraction manifest
```

#### Inventory JSON Format

```json
{
  "extraction_date": "2024-01-15T10:30:00.000000",
  "source_database": "ORCL",
  "schemas": {
    "HR": {
      "objects_extracted": {
        "TABLE": 10,
        "VIEW": 5,
        "PROCEDURE": 8,
        "FUNCTION": 3,
        "PACKAGE": 2,
        "TRIGGER": 4,
        "SEQUENCE": 6,
        "INDEX": 15
      },
      "objects_failed": {
        "PROCEDURE": 1
      },
      "total_files": 53,
      "table_details": [...],
      "procedure_details": [...],
      "source_metrics": {...}
    }
  },
  "summary": {
    "total_schemas": 1,
    "total_objects": 53,
    "objects_by_type": {...}
  }
}
```

---

## Docker Test Environment Specification

### File: `docker/docker-compose.yml`

Three Oracle container options:

#### Option 1: gvenzl/oracle-xe (Recommended for testing)
- **Image**: `gvenzl/oracle-xe:21-slim`
- **Size**: ~3GB
- **Startup**: ~2 minutes
- **Login Required**: No
- **Service Name**: XEPDB1
- **Mount**: `./init-scripts:/container-entrypoint-initdb.d`

#### Option 2: Oracle XE 21c (Official)
- **Image**: `container-registry.oracle.com/database/express:latest`
- **Size**: ~8GB
- **Startup**: ~5 minutes
- **Login Required**: Yes (Oracle SSO)
- **Service Name**: XEPDB1
- **Mount**: `./init-scripts:/opt/oracle/scripts/startup`
- **Requires**: `shm_size: '2g'`

#### Option 3: Oracle 23ai Free (Newest)
- **Image**: `container-registry.oracle.com/database/free:latest`
- **Size**: ~8GB
- **Startup**: ~5 minutes
- **Login Required**: Yes (Oracle SSO)
- **Service Name**: FREEPDB1
- **Mount**: `./init-scripts:/opt/oracle/scripts/startup`

#### Common Configuration

```yaml
environment:
  - ORACLE_PWD=LakebridgeTest123!
  - ORACLE_CHARACTERSET=AL32UTF8
ports:
  - "1521:1521"
  - "5500:5500"  # Enterprise Manager (optional)
healthcheck:
  test: ["CMD", "sqlplus", "-L", "sys/${ORACLE_PWD}@//localhost:1521/${SERVICE} as sysdba", "@/dev/null"]
  interval: 30s
  timeout: 10s
  retries: 10
```

Use Docker Compose profiles to select which container to run:
- `--profile slim` for gvenzl
- `--profile xe` for Oracle XE
- `--profile 23ai` for Oracle 23ai Free

### File: `docker/setup.sh`

Shell script with commands:

| Command | Description |
|---------|-------------|
| `start` | Start Oracle using slim profile (default) |
| `start-xe` | Start Oracle XE 21c |
| `start-23ai` | Start Oracle 23ai Free |
| `stop` | Stop all Oracle containers |
| `status` | Show container status |
| `logs` | Follow container logs |
| `connect` | Connect with sqlplus |
| `init` | Initialize healthcare test schema |
| `test` | Run extraction script against test DB |
| `clean` | Remove containers and volumes |

Include a `wait_for_oracle()` function that polls until the database is ready.

### File: `docker/.env.example`

```env
ORACLE_PWD=LakebridgeTest123!
ORACLE_CHARACTERSET=AL32UTF8
HEALTHCARE_USER=healthcare
HEALTHCARE_PWD=healthcare123
```

---

## Healthcare Test Schema Specification

### File: `docker/init-scripts/init_healthcare_schema.sql`

Create a realistic healthcare schema that exercises various Oracle features.

#### Schema Setup

```sql
-- Create user
CREATE USER healthcare IDENTIFIED BY healthcare123
  DEFAULT TABLESPACE USERS
  QUOTA UNLIMITED ON USERS;

-- Grant privileges
GRANT CONNECT, RESOURCE, CREATE VIEW, CREATE PROCEDURE, 
      CREATE SEQUENCE, CREATE TRIGGER, CREATE TYPE,
      CREATE MATERIALIZED VIEW, CREATE SYNONYM TO healthcare;
GRANT SELECT ANY DICTIONARY TO healthcare;
```

#### Tables (10 tables)

1. **patients** - Core patient demographics
   - patient_id (PK), mrn, first_name, last_name, date_of_birth, gender, ssn_last_four
   - address fields, insurance_id, pcp_provider_id (FK), active_flag, timestamps

2. **providers** - Healthcare providers
   - provider_id (PK), npi, first_name, last_name, specialty, department, license info

3. **encounters** - Patient visits
   - encounter_id (PK), patient_id (FK), provider_id (FK), encounter_type
   - admit_date, discharge_date, facility_code, department_code, primary_diagnosis, status

4. **diagnoses** - ICD-10 diagnosis codes
   - diagnosis_id (PK), encounter_id (FK), patient_id (FK), icd10_code
   - diagnosis_desc, diagnosis_type (PRIMARY/SECONDARY/ADMITTING), onset_date, resolved_date

5. **medications** - Prescriptions
   - medication_id (PK), patient_id (FK), encounter_id (FK)
   - rxnorm_code, ndc_code, medication_name, dosage, route, frequency, dates, status

6. **lab_results** - Lab test results
   - result_id (PK), patient_id (FK), encounter_id (FK)
   - loinc_code, test_name, result_value, result_unit, reference_range, abnormal_flag

7. **clinical_notes** - Clinical documentation (includes CLOB)
   - note_id (PK), patient_id (FK), encounter_id (FK)
   - note_type, note_title, note_text (CLOB), author_id, note_date, signed_date, status

8. **hedis_measures** - HEDIS measure definitions
   - measure_id (PK), measure_name, measure_year, measure_type
   - denominator_desc, numerator_desc, exclusion_desc, value_set_oid

9. **audit_log** - Audit trail (includes CLOB, TIMESTAMP)
   - audit_id (PK), table_name, operation, record_id, old_values (CLOB), new_values (CLOB), changed_by, changed_date

#### Sequences (8)
- patient_seq, provider_seq, encounter_seq, diagnosis_seq
- medication_seq, lab_result_seq, note_seq, audit_seq

#### Indexes (12+)
- Primary key indexes (automatic)
- Secondary indexes on commonly queried columns:
  - patients: mrn, name, dob
  - encounters: patient_id+admit_date, dates
  - diagnoses: icd10_code, patient_id
  - medications: patient_id+status
  - lab_results: patient_id+specimen_date, loinc_code
  - clinical_notes: patient_id+note_date
  - audit_log: table_name+changed_date

#### Views (3)

1. **vw_patient_summary** - Patient overview with calculated age, PCP name, encounter count
2. **vw_active_medications** - Currently active medications with patient and prescriber info
3. **vw_hedis_diabetes_eligible** - Patients eligible for diabetes HEDIS measures (Type 2, age 18-75)

#### Object Types (2)

1. **address_type** - Object type with line1, line2, city, state, zip_code
   - Include MEMBER FUNCTION full_address()

2. **diagnosis_code_list** - TABLE OF VARCHAR2(10) for diagnosis code collections

#### Packages (2)

1. **pkg_patient_mgmt** - Patient management
   - Constants: c_active, c_inactive
   - Exceptions: patient_not_found, invalid_mrn
   - Functions: get_patient_age(), get_patient_by_mrn(), calculate_bmi()
   - Procedures: create_patient(), update_patient_status(), get_patient_summary()

2. **pkg_hedis_calculations** - HEDIS calculations
   - Functions: get_measure_eligible() returning SYS_REFCURSOR, check_numerator_compliance()
   - Procedures: calculate_measure_rate() with OUT parameters

#### Standalone Procedures (2)

1. **sp_process_clinical_note** - Insert clinical note with CLOB handling
2. **sp_get_diagnosis_hierarchy** - Uses CONNECT BY for hierarchical query (Oracle-specific)

#### Standalone Functions (3)

1. **fn_calculate_age** - Calculate age from DOB (DETERMINISTIC)
2. **fn_get_age_group** - Uses DECODE (Oracle-specific) to categorize age
3. **fn_format_phone** - Uses NVL2 (Oracle-specific) for phone formatting

#### Triggers (2)

1. **trg_patients_audit** - AFTER INSERT/UPDATE/DELETE audit trigger
2. **trg_patients_timestamp** - BEFORE UPDATE timestamp trigger

#### Materialized View (1)

**mv_patient_encounter_summary** - Patient encounter aggregates
- BUILD IMMEDIATE, REFRESH COMPLETE ON DEMAND

#### Synonyms (2)
- patients_syn → patients
- encounters_syn → encounters

#### Sample Data

Insert a few records:
- 2-3 providers
- 3-5 patients (use pkg_patient_mgmt.create_patient)
- 2-3 HEDIS measure definitions

#### Oracle-Specific Patterns to Include

These patterns test Lakebridge's Oracle analysis capabilities:

| Pattern | Location | Purpose |
|---------|----------|---------|
| `CONNECT BY` | sp_get_diagnosis_hierarchy | Hierarchical queries |
| `DECODE` | fn_get_age_group | Oracle function |
| `NVL2` | fn_format_phone | Oracle function |
| `CLOB` handling | clinical_notes, audit_log | Large text |
| Cursor loops | pkg_hedis_calculations | PL/SQL iteration |
| `SYS_REFCURSOR` | pkg_hedis_calculations | Cursor return |
| OUT parameters | pkg_patient_mgmt, pkg_hedis_calculations | Procedure outputs |
| `%ROWTYPE` | pkg_patient_mgmt | PL/SQL typing |
| Package spec+body | Both packages | Code organization |
| Object types | address_type | OOP in Oracle |
| Collection types | diagnosis_code_list | Nested tables |
| Audit triggers | trg_patients_audit | DML auditing |
| `DETERMINISTIC` | Functions | Optimization hint |
| Materialized views | mv_patient_encounter_summary | Precomputed results |

---

## Documentation Specifications

### README.md

Structure:
1. **Header**: Project name, badges (Python version, License, Lakebridge version)
2. **Overview**: What this tool does, why it exists
3. **Features**: Bullet list of capabilities
4. **Quick Start**: 5-step guide
5. **Installation**: pip install, Docker setup
6. **Usage**: CLI examples for all modes
7. **Output Structure**: Directory layout
8. **Configuration**: Environment variables, CLI options
9. **Running Lakebridge Analyzer**: Commands and expected output
10. **Healthcare Test Schema**: What's included
11. **Oracle-Specific Patterns**: Migration considerations
12. **Troubleshooting**: Common issues
13. **Contributing**: How to contribute
14. **License**: Apache 2.0 or MIT
15. **Resources**: Links to Lakebridge docs, Oracle docs

### docs/QUICKSTART.md

- Prerequisites (Docker, Python 3.10+, Databricks CLI)
- 5-minute setup with copy-paste commands
- Expected output at each step

### docs/ORACLE_SETUP.md

- Detailed Docker Oracle setup
- Comparison of three image options
- Networking configuration
- Persistence and volumes
- Troubleshooting connectivity

### docs/LAKEBRIDGE_GUIDE.md

- What is Lakebridge
- Installing Lakebridge
- Running the Analyzer
- Interpreting the report
- Next steps (Transpiler, Reconciler)
- Common HEDIS/healthcare migration patterns

### docs/TROUBLESHOOTING.md

Common issues:
- ORA-00942: table or view does not exist → Grant SELECT ANY DICTIONARY
- ORA-31603: object not found → Object doesn't exist or no privileges
- CLOB truncation → SET LONG 2000000
- Wrapped PL/SQL → Cannot extract encrypted code
- Container won't start → Check Docker resources (2GB+ RAM)
- Connection refused → Wait for Oracle startup (2-5 min)
- oracledb import error → pip install oracledb

---

## Python Dependencies

### requirements.txt

```
oracledb>=2.0.0
```

### Optional (for development)

```
pytest>=7.0.0
pytest-cov>=4.0.0
black>=23.0.0
ruff>=0.1.0
mypy>=1.0.0
```

---

## Testing Specification

### tests/conftest.py

Pytest fixtures:
- `mock_cursor` - Mock Oracle cursor
- `mock_connection` - Mock Oracle connection
- `sample_ddl` - Sample DDL strings
- `temp_output_dir` - Temporary output directory

### tests/test_extractor.py

Test cases:
- `test_get_schema_objects_tables` - Query returns table list
- `test_get_schema_objects_procedures` - Query returns procedure list
- `test_extract_ddl_success` - DDL extraction works
- `test_extract_ddl_not_found` - Handles missing objects
- `test_directory_structure_creation` - Creates correct folders
- `test_file_naming` - Files named correctly (lowercase, .sql)
- `test_inventory_generation` - JSON inventory created correctly

### tests/test_ddl_cleaner.py

Test cases:
- `test_remove_storage_clauses` - PCTFREE, etc. removed
- `test_remove_tablespace` - TABLESPACE references removed
- `test_preserve_constraints` - Constraints kept intact
- `test_preserve_column_definitions` - Column types preserved
- `test_whitespace_cleanup` - Extra whitespace removed

---

## Oracle to Databricks Type Mappings (Reference)

Include this in documentation for user reference:

| Oracle | Databricks SQL |
|--------|----------------|
| `NUMBER` | `DECIMAL` or `BIGINT` |
| `NUMBER(p,s)` | `DECIMAL(p,s)` |
| `VARCHAR2(n)` | `STRING` |
| `CHAR(n)` | `STRING` |
| `DATE` | `TIMESTAMP` |
| `TIMESTAMP` | `TIMESTAMP` |
| `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` |
| `CLOB` | `STRING` |
| `BLOB` | `BINARY` |
| `RAW` | `BINARY` |
| `LONG` | `STRING` |
| `LONG RAW` | `BINARY` |
| `XMLTYPE` | `STRING` |
| `BOOLEAN` (PL/SQL only) | `BOOLEAN` |
| `BINARY_FLOAT` | `FLOAT` |
| `BINARY_DOUBLE` | `DOUBLE` |
| `INTERVAL` | `STRING` (manual handling) |
| `ROWID` | `STRING` |

---

## Oracle PL/SQL to Databricks Patterns (Reference)

Include this in documentation:

| Oracle Pattern | Databricks Equivalent | Complexity |
|----------------|----------------------|------------|
| `CONNECT BY` | Recursive CTE | MEDIUM |
| Cursor FOR loops | DataFrame operations | HIGH |
| `BULK COLLECT` | Spark parallelism | MEDIUM |
| `FORALL` | DataFrame writes | MEDIUM |
| `DBMS_OUTPUT.PUT_LINE` | `print()` in notebooks | LOW |
| `UTL_FILE` | Databricks file APIs | MEDIUM |
| Autonomous transactions | Separate jobs/notebooks | HIGH |
| Global temp tables | Session-scoped views | MEDIUM |
| `MERGE` | Delta `MERGE INTO` | LOW |
| Analytic functions | Window functions | LOW |
| `DECODE` | `CASE WHEN` | LOW |
| `NVL` | `COALESCE` | LOW |
| `NVL2` | `CASE WHEN` | LOW |
| `ROWNUM` | `ROW_NUMBER()` | LOW |
| `SYSDATE` | `CURRENT_TIMESTAMP()` | LOW |
| `TO_DATE` | `TO_DATE` | LOW |
| `TO_CHAR` | `DATE_FORMAT` | LOW |
| Packages | Python modules or SQL UDFs | MEDIUM |
| Object types | Structs or separate tables | MEDIUM |
| Collections | Arrays | MEDIUM |
| REF CURSOR | DataFrames | MEDIUM |
| Exception handling | Try/except in Python | MEDIUM |

---

## Git Configuration

### .gitignore

```gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
venv/
ENV/
.venv/
*.egg-info/
dist/
build/

# IDE
.idea/
.vscode/
*.swp
*.swo
.DS_Store

# Testing
.pytest_cache/
.coverage
htmlcov/
.tox/

# Docker
docker/.env

# Output
lakebridge_output/
*.xlsx
lakebridge_inventory.json

# Logs
*.log

# Secrets (never commit)
*.pem
*.key
secrets.json
credentials.json
```

---

## CI/CD Considerations (Optional)

GitHub Actions workflow for:
- Python linting (ruff, black)
- Type checking (mypy)
- Unit tests (pytest)
- Integration tests (with Docker Oracle)

---

## Final Checklist

Before publishing, ensure:

- [ ] All Python code passes linting
- [ ] All tests pass
- [ ] README has complete documentation
- [ ] Docker setup works with `./setup.sh start && ./setup.sh init && ./setup.sh test`
- [ ] Extraction script produces valid output for Lakebridge
- [ ] Lakebridge Analyzer runs successfully on output
- [ ] Healthcare schema contains all specified Oracle patterns
- [ ] License file included
- [ ] .gitignore configured
- [ ] No secrets or credentials in repository

---

## Summary

This repository provides a complete solution for extracting Oracle database objects for Lakebridge Analyzer. The key value propositions are:

1. **Clean DDL output** optimized for migration analysis
2. **One artifact per file** following Lakebridge best practices
3. **Comprehensive object coverage** including all PL/SQL constructs
4. **Easy local testing** with Docker Oracle environment
5. **Healthcare-focused test schema** demonstrating realistic patterns
6. **Multiple extraction modes** for different environments

The extracted files can be directly fed to `databricks labs lakebridge analyze` for complexity assessment before migrating Oracle workloads to Databricks.
