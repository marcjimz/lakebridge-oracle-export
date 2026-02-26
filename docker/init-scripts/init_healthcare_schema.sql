-- ============================================================================
-- Oracle Lakebridge Extractor - Healthcare Test Schema
-- ============================================================================
-- This schema demonstrates various Oracle-specific patterns for testing
-- Lakebridge Analyzer's complexity assessment capabilities.
--
-- Patterns included:
--   - CONNECT BY hierarchical queries
--   - DECODE, NVL, NVL2 functions
--   - CLOB handling
--   - Cursor loops and SYS_REFCURSOR
--   - OUT parameters
--   - %ROWTYPE declarations
--   - Package spec + body
--   - Object types with member functions
--   - Collection types
--   - Audit triggers
--   - DETERMINISTIC functions
--   - Materialized views
-- ============================================================================

SET SERVEROUTPUT ON;

-- ============================================================================
-- SCHEMA SETUP
-- ============================================================================

-- Create user (if not exists via APP_USER in gvenzl container)
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count FROM dba_users WHERE username = 'HEALTHCARE';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE 'CREATE USER healthcare IDENTIFIED BY healthcare123 DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS';
    END IF;
END;
/

-- Grant privileges
GRANT CONNECT, RESOURCE TO healthcare;
GRANT CREATE VIEW, CREATE PROCEDURE, CREATE SEQUENCE TO healthcare;
GRANT CREATE TRIGGER, CREATE TYPE, CREATE MATERIALIZED VIEW TO healthcare;
GRANT CREATE SYNONYM TO healthcare;
GRANT SELECT ANY DICTIONARY TO healthcare;
GRANT SELECT ON sys.v_$session TO healthcare;

ALTER SESSION SET CURRENT_SCHEMA = healthcare;

-- ============================================================================
-- SEQUENCES
-- ============================================================================

CREATE SEQUENCE patient_seq START WITH 1000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE provider_seq START WITH 100 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE encounter_seq START WITH 10000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE diagnosis_seq START WITH 50000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE medication_seq START WITH 60000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE lab_result_seq START WITH 70000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE note_seq START WITH 80000 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE audit_seq START WITH 1 INCREMENT BY 1 NOCACHE;

-- ============================================================================
-- OBJECT TYPES
-- ============================================================================

-- Address type with member function
CREATE OR REPLACE TYPE address_type AS OBJECT (
    line1       VARCHAR2(100),
    line2       VARCHAR2(100),
    city        VARCHAR2(50),
    state       VARCHAR2(2),
    zip_code    VARCHAR2(10),

    MEMBER FUNCTION full_address RETURN VARCHAR2
);
/

CREATE OR REPLACE TYPE BODY address_type AS
    MEMBER FUNCTION full_address RETURN VARCHAR2 IS
    BEGIN
        RETURN line1 ||
               CASE WHEN line2 IS NOT NULL THEN ', ' || line2 ELSE '' END ||
               ', ' || city || ', ' || state || ' ' || zip_code;
    END full_address;
END;
/

-- Collection type for diagnosis codes
CREATE OR REPLACE TYPE diagnosis_code_list AS TABLE OF VARCHAR2(10);
/

-- ============================================================================
-- TABLES
-- ============================================================================

-- Providers table
CREATE TABLE providers (
    provider_id         NUMBER(10)      PRIMARY KEY,
    npi                 VARCHAR2(10)    NOT NULL UNIQUE,
    first_name          VARCHAR2(50)    NOT NULL,
    last_name           VARCHAR2(50)    NOT NULL,
    specialty           VARCHAR2(100),
    department          VARCHAR2(100),
    license_number      VARCHAR2(50),
    license_state       VARCHAR2(2),
    license_expiry      DATE,
    email               VARCHAR2(100),
    phone               VARCHAR2(20),
    active_flag         CHAR(1)         DEFAULT 'Y' CHECK (active_flag IN ('Y', 'N')),
    created_date        DATE            DEFAULT SYSDATE,
    modified_date       DATE
);

-- Patients table
CREATE TABLE patients (
    patient_id          NUMBER(10)      PRIMARY KEY,
    mrn                 VARCHAR2(20)    NOT NULL UNIQUE,
    first_name          VARCHAR2(50)    NOT NULL,
    last_name           VARCHAR2(50)    NOT NULL,
    date_of_birth       DATE            NOT NULL,
    gender              VARCHAR2(10)    CHECK (gender IN ('Male', 'Female', 'Other', 'Unknown')),
    ssn_last_four       VARCHAR2(4),
    address_line1       VARCHAR2(100),
    address_line2       VARCHAR2(100),
    city                VARCHAR2(50),
    state               VARCHAR2(2),
    zip_code            VARCHAR2(10),
    phone_home          VARCHAR2(20),
    phone_mobile        VARCHAR2(20),
    email               VARCHAR2(100),
    insurance_id        VARCHAR2(50),
    insurance_name      VARCHAR2(100),
    pcp_provider_id     NUMBER(10)      REFERENCES providers(provider_id),
    active_flag         CHAR(1)         DEFAULT 'Y' CHECK (active_flag IN ('Y', 'N')),
    created_date        DATE            DEFAULT SYSDATE,
    modified_date       DATE
);

-- Encounters table
CREATE TABLE encounters (
    encounter_id        NUMBER(10)      PRIMARY KEY,
    patient_id          NUMBER(10)      NOT NULL REFERENCES patients(patient_id),
    provider_id         NUMBER(10)      REFERENCES providers(provider_id),
    encounter_type      VARCHAR2(50)    NOT NULL,
    admit_date          DATE            NOT NULL,
    discharge_date      DATE,
    facility_code       VARCHAR2(10),
    department_code     VARCHAR2(10),
    primary_diagnosis   VARCHAR2(10),
    status              VARCHAR2(20)    DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'COMPLETED', 'CANCELLED')),
    created_date        DATE            DEFAULT SYSDATE,
    modified_date       DATE
);

-- Diagnoses table
CREATE TABLE diagnoses (
    diagnosis_id        NUMBER(10)      PRIMARY KEY,
    encounter_id        NUMBER(10)      REFERENCES encounters(encounter_id),
    patient_id          NUMBER(10)      NOT NULL REFERENCES patients(patient_id),
    icd10_code          VARCHAR2(10)    NOT NULL,
    diagnosis_desc      VARCHAR2(500),
    diagnosis_type      VARCHAR2(20)    CHECK (diagnosis_type IN ('PRIMARY', 'SECONDARY', 'ADMITTING')),
    onset_date          DATE,
    resolved_date       DATE,
    created_date        DATE            DEFAULT SYSDATE,
    modified_date       DATE
);

-- Medications table
CREATE TABLE medications (
    medication_id       NUMBER(10)      PRIMARY KEY,
    patient_id          NUMBER(10)      NOT NULL REFERENCES patients(patient_id),
    encounter_id        NUMBER(10)      REFERENCES encounters(encounter_id),
    prescriber_id       NUMBER(10)      REFERENCES providers(provider_id),
    rxnorm_code         VARCHAR2(20),
    ndc_code            VARCHAR2(20),
    medication_name     VARCHAR2(200)   NOT NULL,
    dosage              VARCHAR2(50),
    dosage_unit         VARCHAR2(20),
    route               VARCHAR2(50),
    frequency           VARCHAR2(50),
    start_date          DATE            NOT NULL,
    end_date            DATE,
    status              VARCHAR2(20)    DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'COMPLETED', 'DISCONTINUED', 'ON_HOLD')),
    created_date        DATE            DEFAULT SYSDATE,
    modified_date       DATE
);

-- Lab results table
CREATE TABLE lab_results (
    result_id           NUMBER(10)      PRIMARY KEY,
    patient_id          NUMBER(10)      NOT NULL REFERENCES patients(patient_id),
    encounter_id        NUMBER(10)      REFERENCES encounters(encounter_id),
    ordering_provider   NUMBER(10)      REFERENCES providers(provider_id),
    loinc_code          VARCHAR2(20)    NOT NULL,
    test_name           VARCHAR2(200)   NOT NULL,
    result_value        VARCHAR2(100),
    result_numeric      NUMBER(18,4),
    result_unit         VARCHAR2(50),
    reference_range_low NUMBER(18,4),
    reference_range_high NUMBER(18,4),
    reference_range_text VARCHAR2(100),
    abnormal_flag       VARCHAR2(10)    CHECK (abnormal_flag IN ('N', 'L', 'H', 'LL', 'HH', 'A')),
    specimen_date       DATE,
    result_date         DATE            NOT NULL,
    status              VARCHAR2(20)    DEFAULT 'FINAL' CHECK (status IN ('PENDING', 'PRELIMINARY', 'FINAL', 'CORRECTED')),
    created_date        DATE            DEFAULT SYSDATE
);

-- Clinical notes table (includes CLOB)
CREATE TABLE clinical_notes (
    note_id             NUMBER(10)      PRIMARY KEY,
    patient_id          NUMBER(10)      NOT NULL REFERENCES patients(patient_id),
    encounter_id        NUMBER(10)      REFERENCES encounters(encounter_id),
    author_id           NUMBER(10)      REFERENCES providers(provider_id),
    note_type           VARCHAR2(50)    NOT NULL,
    note_title          VARCHAR2(200),
    note_text           CLOB,
    note_date           DATE            NOT NULL,
    signed_date         DATE,
    status              VARCHAR2(20)    DEFAULT 'DRAFT' CHECK (status IN ('DRAFT', 'SIGNED', 'AMENDED', 'ADDENDUM')),
    created_date        DATE            DEFAULT SYSDATE,
    modified_date       DATE
);

-- HEDIS measures table
CREATE TABLE hedis_measures (
    measure_id          VARCHAR2(20)    PRIMARY KEY,
    measure_name        VARCHAR2(200)   NOT NULL,
    measure_year        NUMBER(4)       NOT NULL,
    measure_type        VARCHAR2(50),
    denominator_desc    VARCHAR2(1000),
    numerator_desc      VARCHAR2(1000),
    exclusion_desc      VARCHAR2(1000),
    value_set_oid       VARCHAR2(100),
    active_flag         CHAR(1)         DEFAULT 'Y',
    created_date        DATE            DEFAULT SYSDATE
);

-- Audit log table (includes CLOB and TIMESTAMP)
CREATE TABLE audit_log (
    audit_id            NUMBER(10)      PRIMARY KEY,
    table_name          VARCHAR2(50)    NOT NULL,
    operation           VARCHAR2(10)    NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    record_id           NUMBER(10),
    old_values          CLOB,
    new_values          CLOB,
    changed_by          VARCHAR2(50),
    changed_date        TIMESTAMP       DEFAULT SYSTIMESTAMP
);

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Patients indexes
CREATE INDEX idx_patients_mrn ON patients(mrn);
CREATE INDEX idx_patients_name ON patients(last_name, first_name);
CREATE INDEX idx_patients_dob ON patients(date_of_birth);
CREATE INDEX idx_patients_pcp ON patients(pcp_provider_id);

-- Encounters indexes
CREATE INDEX idx_encounters_patient ON encounters(patient_id, admit_date);
CREATE INDEX idx_encounters_dates ON encounters(admit_date, discharge_date);
CREATE INDEX idx_encounters_provider ON encounters(provider_id);
CREATE INDEX idx_encounters_status ON encounters(status);

-- Diagnoses indexes
CREATE INDEX idx_diagnoses_icd10 ON diagnoses(icd10_code);
CREATE INDEX idx_diagnoses_patient ON diagnoses(patient_id);
CREATE INDEX idx_diagnoses_encounter ON diagnoses(encounter_id);

-- Medications indexes
CREATE INDEX idx_medications_patient_status ON medications(patient_id, status);
CREATE INDEX idx_medications_rxnorm ON medications(rxnorm_code);

-- Lab results indexes
CREATE INDEX idx_lab_results_patient_date ON lab_results(patient_id, specimen_date);
CREATE INDEX idx_lab_results_loinc ON lab_results(loinc_code);

-- Clinical notes indexes
CREATE INDEX idx_clinical_notes_patient_date ON clinical_notes(patient_id, note_date);
CREATE INDEX idx_clinical_notes_type ON clinical_notes(note_type);

-- Audit log indexes
CREATE INDEX idx_audit_log_table_date ON audit_log(table_name, changed_date);

-- ============================================================================
-- VIEWS
-- ============================================================================

-- Patient summary view
CREATE OR REPLACE VIEW vw_patient_summary AS
SELECT
    p.patient_id,
    p.mrn,
    p.first_name || ' ' || p.last_name AS patient_name,
    p.date_of_birth,
    FLOOR(MONTHS_BETWEEN(SYSDATE, p.date_of_birth) / 12) AS age,
    p.gender,
    p.city || ', ' || p.state || ' ' || p.zip_code AS location,
    pr.first_name || ' ' || pr.last_name AS pcp_name,
    pr.specialty AS pcp_specialty,
    (SELECT COUNT(*) FROM encounters e WHERE e.patient_id = p.patient_id) AS encounter_count,
    (SELECT MAX(e.admit_date) FROM encounters e WHERE e.patient_id = p.patient_id) AS last_visit_date,
    p.insurance_name,
    p.active_flag
FROM patients p
LEFT JOIN providers pr ON p.pcp_provider_id = pr.provider_id;

-- Active medications view
CREATE OR REPLACE VIEW vw_active_medications AS
SELECT
    m.medication_id,
    m.patient_id,
    p.mrn,
    p.first_name || ' ' || p.last_name AS patient_name,
    m.medication_name,
    m.dosage || ' ' || m.dosage_unit AS dosage_info,
    m.route,
    m.frequency,
    m.start_date,
    m.end_date,
    pr.first_name || ' ' || pr.last_name AS prescriber_name,
    m.rxnorm_code,
    m.ndc_code
FROM medications m
JOIN patients p ON m.patient_id = p.patient_id
LEFT JOIN providers pr ON m.prescriber_id = pr.provider_id
WHERE m.status = 'ACTIVE'
AND (m.end_date IS NULL OR m.end_date >= SYSDATE);

-- HEDIS Diabetes eligible patients view
CREATE OR REPLACE VIEW vw_hedis_diabetes_eligible AS
SELECT DISTINCT
    p.patient_id,
    p.mrn,
    p.first_name || ' ' || p.last_name AS patient_name,
    FLOOR(MONTHS_BETWEEN(SYSDATE, p.date_of_birth) / 12) AS age,
    d.icd10_code,
    d.diagnosis_desc,
    d.onset_date
FROM patients p
JOIN diagnoses d ON p.patient_id = d.patient_id
WHERE d.icd10_code LIKE 'E11%'  -- Type 2 Diabetes
AND FLOOR(MONTHS_BETWEEN(SYSDATE, p.date_of_birth) / 12) BETWEEN 18 AND 75
AND p.active_flag = 'Y';

-- ============================================================================
-- STANDALONE FUNCTIONS
-- ============================================================================

-- Calculate age function (DETERMINISTIC)
CREATE OR REPLACE FUNCTION fn_calculate_age(
    p_dob IN DATE,
    p_as_of IN DATE DEFAULT SYSDATE
) RETURN NUMBER
DETERMINISTIC
IS
    v_age NUMBER;
BEGIN
    IF p_dob IS NULL THEN
        RETURN NULL;
    END IF;

    v_age := FLOOR(MONTHS_BETWEEN(p_as_of, p_dob) / 12);

    RETURN v_age;
END fn_calculate_age;
/

-- Get age group function (uses DECODE - Oracle-specific)
CREATE OR REPLACE FUNCTION fn_get_age_group(
    p_age IN NUMBER
) RETURN VARCHAR2
DETERMINISTIC
IS
BEGIN
    RETURN DECODE(
        SIGN(p_age - 18), -1, 'Pediatric',
        DECODE(
            SIGN(p_age - 40), -1, 'Young Adult',
            DECODE(
                SIGN(p_age - 65), -1, 'Adult',
                'Senior'
            )
        )
    );
END fn_get_age_group;
/

-- Format phone function (uses NVL2 - Oracle-specific)
CREATE OR REPLACE FUNCTION fn_format_phone(
    p_phone IN VARCHAR2
) RETURN VARCHAR2
DETERMINISTIC
IS
    v_clean VARCHAR2(20);
BEGIN
    -- Remove non-numeric characters
    v_clean := REGEXP_REPLACE(p_phone, '[^0-9]', '');

    -- Format as (XXX) XXX-XXXX if 10 digits
    RETURN NVL2(
        CASE WHEN LENGTH(v_clean) = 10 THEN v_clean ELSE NULL END,
        '(' || SUBSTR(v_clean, 1, 3) || ') ' || SUBSTR(v_clean, 4, 3) || '-' || SUBSTR(v_clean, 7, 4),
        p_phone
    );
END fn_format_phone;
/

-- ============================================================================
-- STANDALONE PROCEDURES
-- ============================================================================

-- Process clinical note procedure (CLOB handling)
CREATE OR REPLACE PROCEDURE sp_process_clinical_note(
    p_patient_id    IN NUMBER,
    p_encounter_id  IN NUMBER,
    p_author_id     IN NUMBER,
    p_note_type     IN VARCHAR2,
    p_note_title    IN VARCHAR2,
    p_note_text     IN CLOB,
    p_note_id       OUT NUMBER
)
IS
BEGIN
    p_note_id := note_seq.NEXTVAL;

    INSERT INTO clinical_notes (
        note_id, patient_id, encounter_id, author_id,
        note_type, note_title, note_text, note_date, status
    ) VALUES (
        p_note_id, p_patient_id, p_encounter_id, p_author_id,
        p_note_type, p_note_title, p_note_text, SYSDATE, 'DRAFT'
    );

    COMMIT;
END sp_process_clinical_note;
/

-- Get diagnosis hierarchy procedure (uses CONNECT BY - Oracle-specific)
CREATE OR REPLACE PROCEDURE sp_get_diagnosis_hierarchy(
    p_icd_prefix    IN VARCHAR2,
    p_result        OUT SYS_REFCURSOR
)
IS
BEGIN
    -- This demonstrates CONNECT BY for hierarchical queries
    -- ICD-10 codes have a hierarchical structure (e.g., E11 -> E11.0 -> E11.01)
    OPEN p_result FOR
        SELECT
            LEVEL as hierarchy_level,
            LPAD(' ', (LEVEL - 1) * 2) || icd10_code AS formatted_code,
            icd10_code,
            diagnosis_desc,
            COUNT(*) OVER (PARTITION BY SUBSTR(icd10_code, 1, 3)) as category_count
        FROM (
            SELECT DISTINCT icd10_code, diagnosis_desc
            FROM diagnoses
            WHERE icd10_code LIKE p_icd_prefix || '%'
        )
        CONNECT BY PRIOR icd10_code = SUBSTR(icd10_code, 1, LENGTH(icd10_code) - 1)
        START WITH LENGTH(icd10_code) = LENGTH(p_icd_prefix)
        ORDER SIBLINGS BY icd10_code;
END sp_get_diagnosis_hierarchy;
/

-- ============================================================================
-- PACKAGES
-- ============================================================================

-- Package: Patient Management
CREATE OR REPLACE PACKAGE pkg_patient_mgmt AS
    -- Constants
    c_active    CONSTANT CHAR(1) := 'Y';
    c_inactive  CONSTANT CHAR(1) := 'N';

    -- Exceptions
    patient_not_found EXCEPTION;
    invalid_mrn       EXCEPTION;
    PRAGMA EXCEPTION_INIT(patient_not_found, -20001);
    PRAGMA EXCEPTION_INIT(invalid_mrn, -20002);

    -- Record type using %ROWTYPE
    TYPE patient_rec IS RECORD (
        patient_id      patients.patient_id%TYPE,
        mrn             patients.mrn%TYPE,
        full_name       VARCHAR2(101),
        age             NUMBER,
        pcp_name        VARCHAR2(101)
    );

    -- Functions
    FUNCTION get_patient_age(p_patient_id IN NUMBER) RETURN NUMBER;
    FUNCTION get_patient_by_mrn(p_mrn IN VARCHAR2) RETURN patient_rec;
    FUNCTION calculate_bmi(p_weight_kg IN NUMBER, p_height_cm IN NUMBER) RETURN NUMBER;

    -- Procedures
    PROCEDURE create_patient(
        p_mrn           IN VARCHAR2,
        p_first_name    IN VARCHAR2,
        p_last_name     IN VARCHAR2,
        p_dob           IN DATE,
        p_gender        IN VARCHAR2,
        p_patient_id    OUT NUMBER
    );

    PROCEDURE update_patient_status(
        p_patient_id    IN NUMBER,
        p_active        IN CHAR
    );

    PROCEDURE get_patient_summary(
        p_patient_id    IN NUMBER,
        p_summary       OUT SYS_REFCURSOR
    );
END pkg_patient_mgmt;
/

CREATE OR REPLACE PACKAGE BODY pkg_patient_mgmt AS

    FUNCTION get_patient_age(p_patient_id IN NUMBER) RETURN NUMBER IS
        v_dob DATE;
    BEGIN
        SELECT date_of_birth INTO v_dob
        FROM patients
        WHERE patient_id = p_patient_id;

        RETURN fn_calculate_age(v_dob);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE patient_not_found;
    END get_patient_age;

    FUNCTION get_patient_by_mrn(p_mrn IN VARCHAR2) RETURN patient_rec IS
        v_patient patient_rec;
    BEGIN
        IF p_mrn IS NULL OR LENGTH(p_mrn) < 3 THEN
            RAISE invalid_mrn;
        END IF;

        SELECT
            p.patient_id,
            p.mrn,
            p.first_name || ' ' || p.last_name,
            fn_calculate_age(p.date_of_birth),
            NVL(pr.first_name || ' ' || pr.last_name, 'Unassigned')
        INTO v_patient
        FROM patients p
        LEFT JOIN providers pr ON p.pcp_provider_id = pr.provider_id
        WHERE p.mrn = p_mrn;

        RETURN v_patient;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE patient_not_found;
    END get_patient_by_mrn;

    FUNCTION calculate_bmi(p_weight_kg IN NUMBER, p_height_cm IN NUMBER) RETURN NUMBER IS
    BEGIN
        IF p_weight_kg IS NULL OR p_height_cm IS NULL OR p_height_cm = 0 THEN
            RETURN NULL;
        END IF;
        RETURN ROUND(p_weight_kg / POWER(p_height_cm / 100, 2), 1);
    END calculate_bmi;

    PROCEDURE create_patient(
        p_mrn           IN VARCHAR2,
        p_first_name    IN VARCHAR2,
        p_last_name     IN VARCHAR2,
        p_dob           IN DATE,
        p_gender        IN VARCHAR2,
        p_patient_id    OUT NUMBER
    ) IS
    BEGIN
        IF p_mrn IS NULL OR LENGTH(p_mrn) < 3 THEN
            RAISE invalid_mrn;
        END IF;

        p_patient_id := patient_seq.NEXTVAL;

        INSERT INTO patients (
            patient_id, mrn, first_name, last_name, date_of_birth, gender,
            active_flag, created_date
        ) VALUES (
            p_patient_id, p_mrn, p_first_name, p_last_name, p_dob, p_gender,
            c_active, SYSDATE
        );

        COMMIT;
    END create_patient;

    PROCEDURE update_patient_status(
        p_patient_id    IN NUMBER,
        p_active        IN CHAR
    ) IS
    BEGIN
        UPDATE patients
        SET active_flag = p_active,
            modified_date = SYSDATE
        WHERE patient_id = p_patient_id;

        IF SQL%ROWCOUNT = 0 THEN
            RAISE patient_not_found;
        END IF;

        COMMIT;
    END update_patient_status;

    PROCEDURE get_patient_summary(
        p_patient_id    IN NUMBER,
        p_summary       OUT SYS_REFCURSOR
    ) IS
    BEGIN
        OPEN p_summary FOR
            SELECT
                p.patient_id,
                p.mrn,
                p.first_name,
                p.last_name,
                p.date_of_birth,
                fn_calculate_age(p.date_of_birth) AS age,
                fn_get_age_group(fn_calculate_age(p.date_of_birth)) AS age_group,
                p.gender,
                p.insurance_name,
                (SELECT COUNT(*) FROM encounters e WHERE e.patient_id = p.patient_id) AS total_encounters,
                (SELECT COUNT(*) FROM medications m WHERE m.patient_id = p.patient_id AND m.status = 'ACTIVE') AS active_medications
            FROM patients p
            WHERE p.patient_id = p_patient_id;
    END get_patient_summary;

END pkg_patient_mgmt;
/

-- Package: HEDIS Calculations
CREATE OR REPLACE PACKAGE pkg_hedis_calculations AS
    -- Get eligible patients for a measure
    FUNCTION get_measure_eligible(
        p_measure_id    IN VARCHAR2,
        p_measure_year  IN NUMBER
    ) RETURN SYS_REFCURSOR;

    -- Check numerator compliance
    FUNCTION check_numerator_compliance(
        p_patient_id    IN NUMBER,
        p_measure_id    IN VARCHAR2,
        p_measure_year  IN NUMBER
    ) RETURN CHAR;

    -- Calculate measure rate with OUT parameters
    PROCEDURE calculate_measure_rate(
        p_measure_id        IN VARCHAR2,
        p_measure_year      IN NUMBER,
        p_denominator       OUT NUMBER,
        p_numerator         OUT NUMBER,
        p_exclusions        OUT NUMBER,
        p_rate              OUT NUMBER
    );
END pkg_hedis_calculations;
/

CREATE OR REPLACE PACKAGE BODY pkg_hedis_calculations AS

    FUNCTION get_measure_eligible(
        p_measure_id    IN VARCHAR2,
        p_measure_year  IN NUMBER
    ) RETURN SYS_REFCURSOR IS
        v_cursor SYS_REFCURSOR;
    BEGIN
        -- Example: CDC (Comprehensive Diabetes Care) measure eligibility
        IF p_measure_id = 'CDC' THEN
            OPEN v_cursor FOR
                SELECT DISTINCT
                    p.patient_id,
                    p.mrn,
                    p.first_name || ' ' || p.last_name AS patient_name,
                    fn_calculate_age(p.date_of_birth) AS age
                FROM patients p
                JOIN diagnoses d ON p.patient_id = d.patient_id
                WHERE d.icd10_code LIKE 'E11%'
                AND fn_calculate_age(p.date_of_birth) BETWEEN 18 AND 75
                AND p.active_flag = 'Y'
                ORDER BY p.last_name, p.first_name;
        ELSE
            -- Generic query for other measures
            OPEN v_cursor FOR
                SELECT patient_id, mrn, first_name || ' ' || last_name AS patient_name,
                       fn_calculate_age(date_of_birth) AS age
                FROM patients
                WHERE active_flag = 'Y';
        END IF;

        RETURN v_cursor;
    END get_measure_eligible;

    FUNCTION check_numerator_compliance(
        p_patient_id    IN NUMBER,
        p_measure_id    IN VARCHAR2,
        p_measure_year  IN NUMBER
    ) RETURN CHAR IS
        v_count NUMBER;
    BEGIN
        -- Example: Check for HbA1c test in the measurement year
        IF p_measure_id = 'CDC' THEN
            SELECT COUNT(*) INTO v_count
            FROM lab_results lr
            WHERE lr.patient_id = p_patient_id
            AND lr.loinc_code IN ('4548-4', '4549-2')  -- HbA1c LOINC codes
            AND EXTRACT(YEAR FROM lr.result_date) = p_measure_year;

            RETURN CASE WHEN v_count > 0 THEN 'Y' ELSE 'N' END;
        END IF;

        RETURN 'N';
    END check_numerator_compliance;

    PROCEDURE calculate_measure_rate(
        p_measure_id        IN VARCHAR2,
        p_measure_year      IN NUMBER,
        p_denominator       OUT NUMBER,
        p_numerator         OUT NUMBER,
        p_exclusions        OUT NUMBER,
        p_rate              OUT NUMBER
    ) IS
        v_cursor    SYS_REFCURSOR;
        v_patient_id NUMBER;
        v_mrn       VARCHAR2(20);
        v_name      VARCHAR2(101);
        v_age       NUMBER;
    BEGIN
        p_denominator := 0;
        p_numerator := 0;
        p_exclusions := 0;

        -- Get eligible patients using cursor loop (Oracle pattern)
        v_cursor := get_measure_eligible(p_measure_id, p_measure_year);

        LOOP
            FETCH v_cursor INTO v_patient_id, v_mrn, v_name, v_age;
            EXIT WHEN v_cursor%NOTFOUND;

            p_denominator := p_denominator + 1;

            IF check_numerator_compliance(v_patient_id, p_measure_id, p_measure_year) = 'Y' THEN
                p_numerator := p_numerator + 1;
            END IF;
        END LOOP;

        CLOSE v_cursor;

        -- Calculate rate
        IF p_denominator > 0 THEN
            p_rate := ROUND((p_numerator / (p_denominator - p_exclusions)) * 100, 2);
        ELSE
            p_rate := 0;
        END IF;
    END calculate_measure_rate;

END pkg_hedis_calculations;
/

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- Audit trigger for patients table
CREATE OR REPLACE TRIGGER trg_patients_audit
AFTER INSERT OR UPDATE OR DELETE ON patients
FOR EACH ROW
DECLARE
    v_operation VARCHAR2(10);
    v_old_values CLOB;
    v_new_values CLOB;
BEGIN
    IF INSERTING THEN
        v_operation := 'INSERT';
        v_new_values := 'MRN=' || :NEW.mrn || ', Name=' || :NEW.first_name || ' ' || :NEW.last_name;
    ELSIF UPDATING THEN
        v_operation := 'UPDATE';
        v_old_values := 'MRN=' || :OLD.mrn || ', Name=' || :OLD.first_name || ' ' || :OLD.last_name ||
                        ', Active=' || :OLD.active_flag;
        v_new_values := 'MRN=' || :NEW.mrn || ', Name=' || :NEW.first_name || ' ' || :NEW.last_name ||
                        ', Active=' || :NEW.active_flag;
    ELSIF DELETING THEN
        v_operation := 'DELETE';
        v_old_values := 'MRN=' || :OLD.mrn || ', Name=' || :OLD.first_name || ' ' || :OLD.last_name;
    END IF;

    INSERT INTO audit_log (audit_id, table_name, operation, record_id, old_values, new_values, changed_by)
    VALUES (audit_seq.NEXTVAL, 'PATIENTS', v_operation,
            NVL(:NEW.patient_id, :OLD.patient_id), v_old_values, v_new_values, USER);
END trg_patients_audit;
/

-- Timestamp trigger for patients table
CREATE OR REPLACE TRIGGER trg_patients_timestamp
BEFORE UPDATE ON patients
FOR EACH ROW
BEGIN
    :NEW.modified_date := SYSDATE;
END trg_patients_timestamp;
/

-- ============================================================================
-- MATERIALIZED VIEW
-- ============================================================================

CREATE MATERIALIZED VIEW mv_patient_encounter_summary
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    p.patient_id,
    p.mrn,
    p.first_name || ' ' || p.last_name AS patient_name,
    COUNT(e.encounter_id) AS total_encounters,
    COUNT(CASE WHEN e.encounter_type = 'INPATIENT' THEN 1 END) AS inpatient_count,
    COUNT(CASE WHEN e.encounter_type = 'OUTPATIENT' THEN 1 END) AS outpatient_count,
    COUNT(CASE WHEN e.encounter_type = 'EMERGENCY' THEN 1 END) AS emergency_count,
    MIN(e.admit_date) AS first_visit,
    MAX(e.admit_date) AS last_visit,
    COUNT(DISTINCT d.icd10_code) AS unique_diagnoses,
    COUNT(DISTINCT m.medication_id) AS total_medications
FROM patients p
LEFT JOIN encounters e ON p.patient_id = e.patient_id
LEFT JOIN diagnoses d ON p.patient_id = d.patient_id
LEFT JOIN medications m ON p.patient_id = m.patient_id
GROUP BY p.patient_id, p.mrn, p.first_name, p.last_name;

-- ============================================================================
-- SYNONYMS
-- ============================================================================

CREATE SYNONYM patients_syn FOR patients;
CREATE SYNONYM encounters_syn FOR encounters;

-- ============================================================================
-- SAMPLE DATA
-- ============================================================================

-- Insert providers
INSERT INTO providers (provider_id, npi, first_name, last_name, specialty, department, active_flag)
VALUES (provider_seq.NEXTVAL, '1234567890', 'John', 'Smith', 'Internal Medicine', 'Primary Care', 'Y');

INSERT INTO providers (provider_id, npi, first_name, last_name, specialty, department, active_flag)
VALUES (provider_seq.NEXTVAL, '2345678901', 'Sarah', 'Johnson', 'Endocrinology', 'Specialty Care', 'Y');

INSERT INTO providers (provider_id, npi, first_name, last_name, specialty, department, active_flag)
VALUES (provider_seq.NEXTVAL, '3456789012', 'Michael', 'Williams', 'Family Medicine', 'Primary Care', 'Y');

-- Insert patients using the package
DECLARE
    v_patient_id NUMBER;
    v_provider1 NUMBER;
    v_provider2 NUMBER;
BEGIN
    -- Get provider IDs
    SELECT MIN(provider_id) INTO v_provider1 FROM providers WHERE specialty = 'Internal Medicine';
    SELECT MIN(provider_id) INTO v_provider2 FROM providers WHERE specialty = 'Family Medicine';

    -- Create patients
    pkg_patient_mgmt.create_patient('MRN001', 'Robert', 'Anderson', DATE '1965-03-15', 'Male', v_patient_id);
    UPDATE patients SET pcp_provider_id = v_provider1, insurance_name = 'Blue Cross' WHERE patient_id = v_patient_id;

    pkg_patient_mgmt.create_patient('MRN002', 'Maria', 'Garcia', DATE '1978-07-22', 'Female', v_patient_id);
    UPDATE patients SET pcp_provider_id = v_provider1, insurance_name = 'Aetna' WHERE patient_id = v_patient_id;

    pkg_patient_mgmt.create_patient('MRN003', 'James', 'Wilson', DATE '1952-11-08', 'Male', v_patient_id);
    UPDATE patients SET pcp_provider_id = v_provider2, insurance_name = 'Medicare' WHERE patient_id = v_patient_id;

    pkg_patient_mgmt.create_patient('MRN004', 'Emily', 'Brown', DATE '1990-01-30', 'Female', v_patient_id);
    UPDATE patients SET pcp_provider_id = v_provider2, insurance_name = 'United Healthcare' WHERE patient_id = v_patient_id;

    pkg_patient_mgmt.create_patient('MRN005', 'David', 'Martinez', DATE '1945-09-12', 'Male', v_patient_id);
    UPDATE patients SET pcp_provider_id = v_provider1, insurance_name = 'Medicare' WHERE patient_id = v_patient_id;
END;
/

-- Insert HEDIS measure definitions
INSERT INTO hedis_measures (measure_id, measure_name, measure_year, measure_type, denominator_desc, numerator_desc)
VALUES ('CDC', 'Comprehensive Diabetes Care', 2024, 'HEDIS',
        'Patients 18-75 with diabetes (E11.x)', 'Patients with HbA1c test in measurement year');

INSERT INTO hedis_measures (measure_id, measure_name, measure_year, measure_type, denominator_desc, numerator_desc)
VALUES ('BCS', 'Breast Cancer Screening', 2024, 'HEDIS',
        'Women 50-74 years of age', 'Mammogram in past 2 years');

INSERT INTO hedis_measures (measure_id, measure_name, measure_year, measure_type, denominator_desc, numerator_desc)
VALUES ('COL', 'Colorectal Cancer Screening', 2024, 'HEDIS',
        'Adults 45-75 years of age', 'Appropriate screening completed');

-- Insert sample encounters
DECLARE
    v_patient_id NUMBER;
    v_provider_id NUMBER;
    v_encounter_id NUMBER;
BEGIN
    SELECT MIN(patient_id) INTO v_patient_id FROM patients WHERE mrn = 'MRN001';
    SELECT MIN(provider_id) INTO v_provider_id FROM providers;

    v_encounter_id := encounter_seq.NEXTVAL;
    INSERT INTO encounters (encounter_id, patient_id, provider_id, encounter_type, admit_date, status)
    VALUES (v_encounter_id, v_patient_id, v_provider_id, 'OUTPATIENT', DATE '2024-01-15', 'COMPLETED');

    -- Add diagnosis (Type 2 Diabetes)
    INSERT INTO diagnoses (diagnosis_id, encounter_id, patient_id, icd10_code, diagnosis_desc, diagnosis_type)
    VALUES (diagnosis_seq.NEXTVAL, v_encounter_id, v_patient_id, 'E11.9', 'Type 2 diabetes without complications', 'PRIMARY');

    INSERT INTO diagnoses (diagnosis_id, encounter_id, patient_id, icd10_code, diagnosis_desc, diagnosis_type)
    VALUES (diagnosis_seq.NEXTVAL, v_encounter_id, v_patient_id, 'I10', 'Essential hypertension', 'SECONDARY');

    -- Add medication
    INSERT INTO medications (medication_id, patient_id, encounter_id, prescriber_id, rxnorm_code, medication_name, dosage, dosage_unit, frequency, start_date, status)
    VALUES (medication_seq.NEXTVAL, v_patient_id, v_encounter_id, v_provider_id, '860975', 'Metformin 500mg', '500', 'mg', 'BID', DATE '2024-01-15', 'ACTIVE');

    -- Add lab result
    INSERT INTO lab_results (result_id, patient_id, encounter_id, ordering_provider, loinc_code, test_name, result_value, result_numeric, result_unit, abnormal_flag, specimen_date, result_date, status)
    VALUES (lab_result_seq.NEXTVAL, v_patient_id, v_encounter_id, v_provider_id, '4548-4', 'Hemoglobin A1c', '7.2', 7.2, '%', 'H', DATE '2024-01-15', DATE '2024-01-16', 'FINAL');
END;
/

-- Refresh materialized view
BEGIN
    DBMS_MVIEW.REFRESH('MV_PATIENT_ENCOUNTER_SUMMARY', 'C');
END;
/

COMMIT;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SET SERVEROUTPUT ON;

DECLARE
    v_count NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('Healthcare Schema Initialization Complete');
    DBMS_OUTPUT.PUT_LINE('=========================================');

    SELECT COUNT(*) INTO v_count FROM user_tables;
    DBMS_OUTPUT.PUT_LINE('Tables created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_views;
    DBMS_OUTPUT.PUT_LINE('Views created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_sequences;
    DBMS_OUTPUT.PUT_LINE('Sequences created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_procedures WHERE object_type = 'PROCEDURE';
    DBMS_OUTPUT.PUT_LINE('Procedures created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_procedures WHERE object_type = 'FUNCTION';
    DBMS_OUTPUT.PUT_LINE('Functions created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_objects WHERE object_type = 'PACKAGE';
    DBMS_OUTPUT.PUT_LINE('Packages created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_triggers;
    DBMS_OUTPUT.PUT_LINE('Triggers created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_types;
    DBMS_OUTPUT.PUT_LINE('Types created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_indexes WHERE generated = 'N';
    DBMS_OUTPUT.PUT_LINE('Indexes created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_mviews;
    DBMS_OUTPUT.PUT_LINE('Materialized views created: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM user_synonyms;
    DBMS_OUTPUT.PUT_LINE('Synonyms created: ' || v_count);

    DBMS_OUTPUT.PUT_LINE('');

    SELECT COUNT(*) INTO v_count FROM providers;
    DBMS_OUTPUT.PUT_LINE('Sample providers: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM patients;
    DBMS_OUTPUT.PUT_LINE('Sample patients: ' || v_count);

    SELECT COUNT(*) INTO v_count FROM hedis_measures;
    DBMS_OUTPUT.PUT_LINE('HEDIS measures: ' || v_count);
END;
/
