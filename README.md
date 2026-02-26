# Oracle Lakebridge Extractor

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Lakebridge](https://img.shields.io/badge/Databricks-Lakebridge-orange.svg)](https://github.com/databrickslabs/lakebridge)

Extract Oracle database objects (DDL and metadata) for Databricks Lakebridge Analyzer migration assessment.

## Overview

Oracle Lakebridge Extractor helps organizations migrate from Oracle to Databricks by extracting database objects into clean, well-organized SQL artifacts. These artifacts are optimized for analysis by the [Databricks Lakebridge Analyzer](https://github.com/databrickslabs/lakebridge), which assesses migration complexity and effort.

### Key Features

- **Direct extraction** via Python `oracledb` driver
- **SQL*Plus script generation** for environments without Python connectivity
- **PL/SQL UTL_FILE export** for individual file generation on the database server
- **Clean DDL output** with Oracle-specific storage clauses removed
- **One artifact per file** following Lakebridge best practices
- **Comprehensive object coverage**: tables, views, procedures, functions, packages, triggers, sequences, indexes, types, materialized views, synonyms, database links
- **Extraction inventory** in JSON format with metadata
- **Docker test environment** with healthcare sample schema

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-org/oracle-lakebridge-extractor.git
cd oracle-lakebridge-extractor

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Start Docker Oracle test environment (optional)
cd docker && ./setup.sh start && ./setup.sh init && cd ..

# 4. Run extraction
python -m src.oracle_lakebridge_extractor \
    --host localhost \
    --port 1521 \
    --service XEPDB1 \
    --user healthcare \
    --password healthcare123 \
    --schemas healthcare \
    --output ./lakebridge_output \
    --verbose

# 5. Run Lakebridge Analyzer on extracted files
databricks labs lakebridge analyze \
    --source-directory ./lakebridge_output \
    --source-tech oracle \
    --report-file ./analysis_report
```

## Installation

### Prerequisites

- Python 3.10 or higher
- Access to Oracle database (or Docker for local testing)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) (for running Lakebridge)

### Install from source

```bash
git clone https://github.com/your-org/oracle-lakebridge-extractor.git
cd oracle-lakebridge-extractor
pip install -r requirements.txt
```

### Docker Test Environment

For local testing without an existing Oracle database:

```bash
cd docker
./setup.sh start      # Start Oracle container (~2 min startup)
./setup.sh init       # Initialize healthcare test schema
./setup.sh test       # Run extraction test
```

See [docs/ORACLE_SETUP.md](docs/ORACLE_SETUP.md) for detailed Docker configuration.

## Usage

### Direct Extraction (Recommended)

Connect directly to Oracle and extract DDL:

```bash
python -m src.oracle_lakebridge_extractor \
    --host oracle.example.com \
    --port 1521 \
    --service ORCL \
    --user migration_user \
    --password secret \
    --schemas HR,FINANCE,CLINICAL \
    --output /data/lakebridge_staging \
    --verbose
```

### SQL*Plus Script Generation

For environments where Python database connectivity is not available:

```bash
python -m src.oracle_lakebridge_extractor \
    --generate-sqlplus \
    --schemas HR,FINANCE \
    --output /data/lakebridge_staging

# Then run on the database server:
sqlplus user/password@database @/data/lakebridge_staging/extract_ddl.sql
```
```

### CLI Options

| Option | Description |
|--------|-------------|
| `--host` | Oracle database host |
| `--port` | Oracle port (default: 1521) |
| `--service` | Oracle service name |
| `--sid` | Oracle SID (alternative to --service) |
| `--user` | Oracle username |
| `--password` | Oracle password |
| `--schemas` | Comma-separated list of schemas to extract |
| `--output` | Output directory for extracted files |
| `--verbose`, `-v` | Enable verbose output |
| `--generate-sqlplus` | Generate SQL*Plus script instead of direct extraction |
| `--generate-plsql` | Generate PL/SQL UTL_FILE export script |

## Output Structure

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
│   ├── synonyms/
│   │   └── emp_syn.sql
│   └── db_links/
│       └── remote_db.sql
└── lakebridge_inventory.json       # Extraction manifest
```

## Healthcare Test Schema

The included test schema demonstrates Oracle-specific patterns:

| Pattern | Location | Purpose |
|---------|----------|---------|
| `CONNECT BY` | sp_get_diagnosis_hierarchy | Hierarchical queries |
| `DECODE` | fn_get_age_group | Oracle function |
| `NVL2` | fn_format_phone | Oracle function |
| `CLOB` handling | clinical_notes, audit_log | Large text |
| Cursor loops | pkg_hedis_calculations | PL/SQL iteration |
| `SYS_REFCURSOR` | pkg_hedis_calculations | Cursor return |
| OUT parameters | pkg_patient_mgmt | Procedure outputs |
| `%ROWTYPE` | pkg_patient_mgmt | PL/SQL typing |
| Package spec+body | Both packages | Code organization |
| Object types | address_type | OOP in Oracle |
| Collection types | diagnosis_code_list | Nested tables |
| Audit triggers | trg_patients_audit | DML auditing |
| `DETERMINISTIC` | Functions | Optimization hint |
| Materialized views | mv_patient_encounter_summary | Precomputed results |