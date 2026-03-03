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

### macOS / Linux

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

### Windows (PowerShell)

```powershell
# 1. Clone the repository
git clone https://github.com/your-org/oracle-lakebridge-extractor.git
cd oracle-lakebridge-extractor

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Run extraction (connect to your Oracle server)
python -m src.oracle_lakebridge_extractor `
    --host your-oracle-host `
    --port 1521 `
    --service ORCL `
    --user your_user `
    --password your_password `
    --schemas YOUR_SCHEMA `
    --output .\lakebridge_output `
    --verbose

# 4. Run Lakebridge Analyzer on extracted files
databricks labs lakebridge analyze `
    --source-directory .\lakebridge_output `
    --source-tech oracle `
    --report-file .\analysis_report
```

### Windows (Command Prompt)

```cmd
REM 1. Clone the repository
git clone https://github.com/your-org/oracle-lakebridge-extractor.git
cd oracle-lakebridge-extractor

REM 2. Install Python dependencies
pip install -r requirements.txt

REM 3. Run extraction (connect to your Oracle server)
python -m src.oracle_lakebridge_extractor ^
    --host your-oracle-host ^
    --port 1521 ^
    --service ORCL ^
    --user your_user ^
    --password your_password ^
    --schemas YOUR_SCHEMA ^
    --output .\lakebridge_output ^
    --verbose

REM 4. Run Lakebridge Analyzer on extracted files
databricks labs lakebridge analyze ^
    --source-directory .\lakebridge_output ^
    --source-tech oracle ^
    --report-file .\analysis_report
```

## Installation

### Prerequisites

- Python 3.10 or higher
- Access to Oracle database (or Docker for local testing)
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) (for running Lakebridge)
- Git (for cloning the repository)

> **Note:** The `oracledb` Python driver uses **thin mode** by default, which means **no Oracle Client installation is required**. This works on Windows, macOS, and Linux without any additional Oracle software.

### Install from source

**macOS / Linux:**

```bash
git clone https://github.com/your-org/oracle-lakebridge-extractor.git
cd oracle-lakebridge-extractor
pip install -r requirements.txt
```

**Windows (PowerShell):**

```powershell
git clone https://github.com/your-org/oracle-lakebridge-extractor.git
cd oracle-lakebridge-extractor
pip install -r requirements.txt
```

If you have multiple Python versions installed on Windows, you may need to use `py -3` instead of `python` and `py -3 -m pip install -r requirements.txt` instead of `pip install`.

### Docker Test Environment (macOS / Linux)

For local testing without an existing Oracle database:

```bash
cd docker
./setup.sh start      # Start Oracle container (~2 min startup)
./setup.sh init       # Initialize healthcare test schema
./setup.sh test       # Run extraction test
```

See [docs/ORACLE_SETUP.md](docs/ORACLE_SETUP.md) for detailed Docker configuration.

> **Windows users:** The Docker test environment uses bash scripts. If you need local Oracle testing on Windows, connect to your own Oracle instance directly using the extraction commands below.

## Usage

### Direct Extraction (Recommended)

Connect directly to Oracle and extract DDL:

**macOS / Linux:**

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

**Windows (PowerShell):**

```powershell
python -m src.oracle_lakebridge_extractor `
    --host oracle.example.com `
    --port 1521 `
    --service ORCL `
    --user migration_user `
    --password secret `
    --schemas HR,FINANCE,CLINICAL `
    --output C:\data\lakebridge_staging `
    --verbose
```

**Windows (Command Prompt):**

```cmd
python -m src.oracle_lakebridge_extractor ^
    --host oracle.example.com ^
    --port 1521 ^
    --service ORCL ^
    --user migration_user ^
    --password secret ^
    --schemas HR,FINANCE,CLINICAL ^
    --output C:\data\lakebridge_staging ^
    --verbose
```

> **Tip:** You can also run the entire command on a single line on any platform without line-continuation characters.

### SQL*Plus Script Generation

For environments where Python database connectivity is not available:

**macOS / Linux:**

```bash
python -m src.oracle_lakebridge_extractor \
    --generate-sqlplus \
    --schemas HR,FINANCE \
    --output /data/lakebridge_staging

# Then run on the database server:
sqlplus user/password@database @/data/lakebridge_staging/extract_ddl.sql
```

**Windows (PowerShell):**

```powershell
python -m src.oracle_lakebridge_extractor `
    --generate-sqlplus `
    --schemas HR,FINANCE `
    --output C:\data\lakebridge_staging

# Then run on the database server:
sqlplus user/password@database "@C:\data\lakebridge_staging\extract_ddl.sql"
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
| `--thick-mode` | Use thick mode (requires Oracle Instant Client). Needed for older Oracle databases. |
| `--lib-dir` | Path to Oracle Instant Client directory (for `--thick-mode`) |

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

## Platform Notes

### Windows

- **No Oracle Client needed** in most cases. The `oracledb` driver runs in thin mode by default, connecting directly over TCP.
- **Older Oracle databases** may require `--thick-mode` with Oracle Instant Client if you get a `DPY-3015` password verifier error (see [Troubleshooting](#troubleshooting) below).
- **Python:** Download from [python.org](https://www.python.org/downloads/). During installation, check "Add Python to PATH". If you have multiple versions, use `py -3` instead of `python`.
- **Git:** Download from [git-scm.com](https://git-scm.com/download/win).
- **Line continuation:** PowerShell uses backtick (`` ` ``), Command Prompt uses caret (`^`), or just put the entire command on one line.
- **Paths:** Use `.\lakebridge_output` or `C:\data\lakebridge_staging` style paths. Forward slashes (`/`) also work in Python.

### macOS / Linux

- **No Oracle Client needed.** Same thin mode driver, no additional setup.
- **Python:** Use your system package manager or [python.org](https://www.python.org/downloads/).
- **Line continuation:** Use backslash (`\`) for multi-line commands.

## Troubleshooting

### DPY-3015: password verifier type is not supported in thin mode

This error occurs when the Oracle database uses an older password authentication protocol that the pure-Python thin mode driver cannot handle. This is common with Oracle 11g/12c or accounts with legacy password hashes.

**Fix: Use `--thick-mode` with Oracle Instant Client.**

1. Download [Oracle Instant Client Basic](https://www.oracle.com/database/technologies/instant-client/winx64-64-downloads.html) for your platform (Windows x64, macOS, or Linux)
2. Extract it to a directory (e.g., `C:\oracle\instantclient_21_12` on Windows)
3. Add `--thick-mode` and `--lib-dir` to your command:

**Windows (PowerShell):**

```powershell
python -m src.oracle_lakebridge_extractor `
    --host your-oracle-host `
    --port 1521 `
    --service ORCL `
    --user your_user `
    --password your_password `
    --schemas YOUR_SCHEMA `
    --output .\lakebridge_output `
    --thick-mode `
    --lib-dir "C:\oracle\instantclient_21_12" `
    --verbose
```

**macOS / Linux:**

```bash
python -m src.oracle_lakebridge_extractor \
    --host your-oracle-host \
    --port 1521 \
    --service ORCL \
    --user your_user \
    --password your_password \
    --schemas YOUR_SCHEMA \
    --output ./lakebridge_output \
    --thick-mode \
    --lib-dir /opt/oracle/instantclient_21_12 \
    --verbose
```

**Alternative (DBA fix):** Ask your Oracle DBA to reset the user's password on Oracle 12c+ so it generates a newer password verifier (12C or later). This avoids needing thick mode entirely:

```sql
ALTER USER migration_user IDENTIFIED BY new_password;
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