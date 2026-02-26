"""Pytest fixtures for Oracle Lakebridge Extractor tests."""

import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock
import tempfile
import shutil


@pytest.fixture
def mock_cursor():
    """Create a mock Oracle cursor."""
    cursor = Mock()
    cursor.execute = Mock()
    cursor.fetchone = Mock(return_value=None)
    cursor.fetchall = Mock(return_value=[])
    cursor.description = []
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    """Create a mock Oracle connection."""
    connection = Mock()
    connection.cursor = Mock(return_value=mock_cursor)
    connection.close = Mock()
    return connection


@pytest.fixture
def sample_table_ddl():
    """Sample table DDL with Oracle-specific clauses."""
    return '''
  CREATE TABLE "HR"."EMPLOYEES"
   (    "EMPLOYEE_ID" NUMBER(6,0),
        "FIRST_NAME" VARCHAR2(20),
        "LAST_NAME" VARCHAR2(25) CONSTRAINT "EMP_LAST_NAME_NN" NOT NULL ENABLE,
        "EMAIL" VARCHAR2(25) CONSTRAINT "EMP_EMAIL_NN" NOT NULL ENABLE,
        "PHONE_NUMBER" VARCHAR2(20),
        "HIRE_DATE" DATE CONSTRAINT "EMP_HIRE_DATE_NN" NOT NULL ENABLE,
        "JOB_ID" VARCHAR2(10) CONSTRAINT "EMP_JOB_NN" NOT NULL ENABLE,
        "SALARY" NUMBER(8,2),
        "COMMISSION_PCT" NUMBER(2,2),
        "MANAGER_ID" NUMBER(6,0),
        "DEPARTMENT_ID" NUMBER(4,0),
         CONSTRAINT "EMP_EMP_ID_PK" PRIMARY KEY ("EMPLOYEE_ID")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS"  ENABLE
   ) SEGMENT CREATION IMMEDIATE
  PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS"
'''


@pytest.fixture
def sample_clean_table_ddl():
    """Expected cleaned table DDL."""
    return '''CREATE TABLE "HR"."EMPLOYEES"
   (    "EMPLOYEE_ID" NUMBER(6,0),
        "FIRST_NAME" VARCHAR2(20),
        "LAST_NAME" VARCHAR2(25) CONSTRAINT "EMP_LAST_NAME_NN" NOT NULL ENABLE,
        "EMAIL" VARCHAR2(25) CONSTRAINT "EMP_EMAIL_NN" NOT NULL ENABLE,
        "PHONE_NUMBER" VARCHAR2(20),
        "HIRE_DATE" DATE CONSTRAINT "EMP_HIRE_DATE_NN" NOT NULL ENABLE,
        "JOB_ID" VARCHAR2(10) CONSTRAINT "EMP_JOB_NN" NOT NULL ENABLE,
        "SALARY" NUMBER(8,2),
        "COMMISSION_PCT" NUMBER(2,2),
        "MANAGER_ID" NUMBER(6,0),
        "DEPARTMENT_ID" NUMBER(4,0),
         CONSTRAINT "EMP_EMP_ID_PK" PRIMARY KEY ("EMPLOYEE_ID")
  USING INDEX ENABLE
   )
;'''


@pytest.fixture
def sample_procedure_ddl():
    """Sample procedure DDL."""
    return '''
CREATE OR REPLACE PROCEDURE "HR"."UPDATE_SALARY"
(
    p_employee_id IN NUMBER,
    p_new_salary IN NUMBER
)
AS
BEGIN
    UPDATE employees
    SET salary = p_new_salary
    WHERE employee_id = p_employee_id;
    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;
'''


@pytest.fixture
def sample_package_spec_ddl():
    """Sample package specification DDL."""
    return '''
CREATE OR REPLACE PACKAGE "HR"."PKG_EMPLOYEE"
AS
    -- Constants
    c_max_salary CONSTANT NUMBER := 100000;

    -- Exceptions
    invalid_salary EXCEPTION;

    -- Functions
    FUNCTION get_employee_name(p_emp_id IN NUMBER) RETURN VARCHAR2;

    -- Procedures
    PROCEDURE update_employee_salary(
        p_emp_id IN NUMBER,
        p_salary IN NUMBER
    );
END PKG_EMPLOYEE;
'''


@pytest.fixture
def sample_package_body_ddl():
    """Sample package body DDL."""
    return '''
CREATE OR REPLACE PACKAGE BODY "HR"."PKG_EMPLOYEE"
AS
    FUNCTION get_employee_name(p_emp_id IN NUMBER) RETURN VARCHAR2
    IS
        v_name VARCHAR2(100);
    BEGIN
        SELECT first_name || ' ' || last_name
        INTO v_name
        FROM employees
        WHERE employee_id = p_emp_id;
        RETURN v_name;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RETURN NULL;
    END get_employee_name;

    PROCEDURE update_employee_salary(
        p_emp_id IN NUMBER,
        p_salary IN NUMBER
    )
    IS
    BEGIN
        IF p_salary > c_max_salary THEN
            RAISE invalid_salary;
        END IF;

        UPDATE employees
        SET salary = p_salary
        WHERE employee_id = p_emp_id;

        COMMIT;
    END update_employee_salary;
END PKG_EMPLOYEE;
'''


@pytest.fixture
def sample_view_ddl():
    """Sample view DDL."""
    return '''
CREATE OR REPLACE VIEW "HR"."VW_EMPLOYEE_SUMMARY" AS
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name AS full_name,
    d.department_name,
    j.job_title,
    e.salary,
    m.first_name || ' ' || m.last_name AS manager_name
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id
LEFT JOIN jobs j ON e.job_id = j.job_id
LEFT JOIN employees m ON e.manager_id = m.employee_id
'''


@pytest.fixture
def sample_index_ddl():
    """Sample index DDL with storage clauses."""
    return '''
CREATE INDEX "HR"."IDX_EMP_NAME" ON "HR"."EMPLOYEES" ("LAST_NAME", "FIRST_NAME")
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "USERS"
'''


@pytest.fixture
def sample_sequence_ddl():
    """Sample sequence DDL."""
    return '''
CREATE SEQUENCE "HR"."EMPLOYEE_SEQ"
  MINVALUE 1 MAXVALUE 9999999999999999999999999999
  INCREMENT BY 1 START WITH 1000 CACHE 20 NOORDER NOCYCLE
'''


@pytest.fixture
def sample_trigger_ddl():
    """Sample trigger DDL."""
    return '''
CREATE OR REPLACE TRIGGER "HR"."TRG_EMP_AUDIT"
AFTER INSERT OR UPDATE OR DELETE ON "HR"."EMPLOYEES"
FOR EACH ROW
DECLARE
    v_operation VARCHAR2(10);
BEGIN
    IF INSERTING THEN
        v_operation := 'INSERT';
    ELSIF UPDATING THEN
        v_operation := 'UPDATE';
    ELSIF DELETING THEN
        v_operation := 'DELETE';
    END IF;

    INSERT INTO audit_log (table_name, operation, changed_by, changed_date)
    VALUES ('EMPLOYEES', v_operation, USER, SYSDATE);
END;
'''


@pytest.fixture
def temp_output_dir():
    """Create a temporary output directory."""
    temp_dir = tempfile.mkdtemp(prefix='lakebridge_test_')
    yield temp_dir
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_objects_list():
    """Sample list of database objects."""
    return [
        ('HR', 'EMPLOYEES'),
        ('HR', 'DEPARTMENTS'),
        ('HR', 'JOBS'),
        ('HR', 'LOCATIONS'),
    ]


@pytest.fixture
def sample_table_details():
    """Sample table details for inventory."""
    return [
        {
            'table_name': 'EMPLOYEES',
            'num_rows': 107,
            'blocks': 5,
            'avg_row_len': 69,
            'last_analyzed': '2024-01-15',
            'column_count': 11,
            'index_count': 3,
            'fk_count': 2
        },
        {
            'table_name': 'DEPARTMENTS',
            'num_rows': 27,
            'blocks': 1,
            'avg_row_len': 21,
            'last_analyzed': '2024-01-15',
            'column_count': 4,
            'index_count': 1,
            'fk_count': 1
        }
    ]


@pytest.fixture
def sample_config():
    """Sample extraction configuration."""
    return {
        'host': 'localhost',
        'port': 1521,
        'service': 'XEPDB1',
        'user': 'test_user',
        'password': 'test_password',
        'use_sid': False
    }
