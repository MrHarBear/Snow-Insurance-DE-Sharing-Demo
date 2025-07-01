/*
================================================================================
MOUNTAINPEAK INSURANCE - AUTOMATED PIPELINE SETUP
================================================================================
Demo:    Progressive Insurance Data Sharing with Automated Pipelines
Purpose: Foundation setup for automated data ingestion and real-time analytics
Data:    CSV files with automated loading and Dynamic Table refresh
Location: Insurance_Data_Sharing_Demo folder - separate from main setup
================================================================================
*/

-- Enable Cortex for classification capabilities
USE ROLE ACCOUNTADMIN;

-- Clean start for demo consistency
DROP DATABASE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB;

/*
================================================================================
DATABASE & WAREHOUSE CREATION
================================================================================
*/

-- Create main database with comprehensive schema structure
CREATE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB
    COMMENT = 'MountainPeak Insurance - Automated Pipeline & Progressive Governance Demo';

CREATE SCHEMA MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA 
    COMMENT = 'Raw insurance data from CSV sources';

CREATE SCHEMA MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS 
    COMMENT = 'Automated analytics with Dynamic Tables';
    
CREATE SCHEMA MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE 
    COMMENT = 'Data governance policies and controls';
    
CREATE SCHEMA MOUNTAINPEAK_INSURANCE_PIPELINE_DB.SHARING 
    COMMENT = 'Secure views for external data sharing';

-- Set working context
USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
USE SCHEMA RAW_DATA;

-- Create optimized warehouses for automated operations
CREATE OR REPLACE WAREHOUSE INSURANCE_PIPELINE_COMPUTE_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    RESOURCE_CONSTRAINT = 'STANDARD_GEN_2'
    COMMENT = 'Main compute for automated insurance analytics';

CREATE OR REPLACE WAREHOUSE INSURANCE_PIPELINE_OPS_WH
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 30
    AUTO_RESUME = TRUE
    COMMENT = 'Dedicated warehouse for pipeline operations';

USE WAREHOUSE INSURANCE_PIPELINE_COMPUTE_WH;

/*
================================================================================
RBAC SETUP FOR DEMO
================================================================================
*/

-- Create analyst role for governance demonstration
USE ROLE USERADMIN;
CREATE OR REPLACE ROLE MOUNTAINPEAK_PIPELINE_ANALYST
    COMMENT = 'Analyst role demonstrating progressive governance controls';

-- Grant comprehensive privileges
USE ROLE SECURITYADMIN;
GRANT USAGE, OPERATE ON WAREHOUSE INSURANCE_PIPELINE_COMPUTE_WH TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT USAGE, OPERATE ON WAREHOUSE INSURANCE_PIPELINE_OPS_WH TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;

GRANT USAGE ON DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;

-- Grant object creation privileges for demo operations
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE ON SCHEMA MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT CREATE TABLE, CREATE VIEW, CREATE DYNAMIC TABLE ON SCHEMA MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT CREATE VIEW ON SCHEMA MOUNTAINPEAK_INSURANCE_PIPELINE_DB.SHARING TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;

-- Grant analyst role to current user
SET MY_USER_ID = CURRENT_USER();
GRANT ROLE MOUNTAINPEAK_PIPELINE_ANALYST TO USER identifier($MY_USER_ID);

USE ROLE ACCOUNTADMIN;

/*
================================================================================
AUTOMATED PIPELINE INFRASTRUCTURE  
================================================================================
*/

-- Create Git integration for repository data access
CREATE OR REPLACE API INTEGRATION INSURANCE_PIPELINE_GIT_INTEGRATION
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com')
    ENABLED = TRUE
    COMMENT = 'Git integration for automated data pipeline';

-- Connect to demo repository
CREATE OR REPLACE GIT REPOSITORY INSURANCE_PIPELINE_DEMO_REPO
    API_INTEGRATION = INSURANCE_PIPELINE_GIT_INTEGRATION
    ORIGIN = 'https://github.com/MrHarBear/Snow-Insurance-DE-Sharing-Demo.git'
    GIT_CREDENTIALS = NULL
    COMMENT = 'Repository with insurance demo data';

-- Refresh repository to access latest files
ALTER GIT REPOSITORY INSURANCE_PIPELINE_DEMO_REPO FETCH;
-- List files to verify the repository connection
SHOW GIT BRANCHES IN GIT REPOSITORY INSURANCE_PIPELINE_DEMO_REPO;
LS @INSURANCE_PIPELINE_DEMO_REPO/branches/main;

-- Create pipeline stages for automated operations
CREATE OR REPLACE STAGE INSURANCE_PIPELINE_CSV_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Stage for original CSV data from Git';

CREATE OR REPLACE STAGE INSURANCE_PIPELINE_LANDING_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Landing stage for new file drops during demo';

CREATE OR REPLACE STAGE INSURANCE_PIPELINE_WORK_STAGE
    DIRECTORY = ( ENABLE = true )
    ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    COMMENT = 'Working stage for data processing operations';

/*
================================================================================
INITIAL DATA LOADING FROM GIT
================================================================================
*/

-- Load original CSV files from Git repository
COPY FILES
    INTO @INSURANCE_PIPELINE_CSV_STAGE
    FROM '@INSURANCE_PIPELINE_DEMO_REPO/branches/main/sample_data/'
    pattern='.*CLAIMS_DATA.csv';
-- Load original CSV files from Git repository
COPY FILES
    INTO @INSURANCE_PIPELINE_CSV_STAGE
    FROM '@INSURANCE_PIPELINE_DEMO_REPO/branches/main/sample_data/'
    pattern='.*CUSTOMER_DATA.csv';
-- Verify files are staged
LIST @INSURANCE_PIPELINE_CSV_STAGE;

/*
================================================================================
RAW DATA TABLES WITH AUTOMATED PIPELINE SUPPORT
================================================================================
*/

-- Create claims table optimized for automated loading
CREATE OR REPLACE TABLE RAW_DATA.CLAIMS_RAW (
    POLICY_NUMBER VARCHAR(50),
    INCIDENT_DATE TIMESTAMP_NTZ,
    INCIDENT_TYPE VARCHAR(100),
    INCIDENT_SEVERITY VARCHAR(50),
    AUTHORITIES_CONTACTED VARCHAR(50),
    INCIDENT_HOUR_OF_THE_DAY NUMBER,
    NUMBER_OF_VEHICLES_INVOLVED NUMBER,
    BODILY_INJURIES NUMBER,
    WITNESSES NUMBER,
    POLICE_REPORT_AVAILABLE VARCHAR(10),
    CLAIM_AMOUNT NUMBER(10,2),
    FRAUD_REPORTED BOOLEAN,
    -- Automated pipeline tracking columns
    LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME STRING DEFAULT 'INITIAL_LOAD'
) COMMENT = 'Raw claims data with automated pipeline tracking';

-- Create customer table optimized for automated loading
CREATE OR REPLACE TABLE RAW_DATA.CUSTOMER_RAW (
    POLICY_NUMBER VARCHAR(50),
    AGE NUMBER,
    POLICY_START_DATE TIMESTAMP_NTZ,
    POLICY_LENGTH_MONTH NUMBER,
    POLICY_DEDUCTABLE NUMBER(10,2),
    POLICY_ANNUAL_PREMIUM NUMBER(10,2),
    INSURED_SEX VARCHAR(10),
    INSURED_EDUCATION_LEVEL VARCHAR(50),
    INSURED_OCCUPATION VARCHAR(100),
    -- Automated pipeline tracking columns
    LOAD_TIMESTAMP TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME STRING DEFAULT 'INITIAL_LOAD'
) COMMENT = 'Raw customer data with automated pipeline tracking';

-- Create FILE FORMAT object for CSV processing
CREATE OR REPLACE FILE FORMAT INSURANCE_CSV_FORMAT
    TYPE = CSV
    PARSE_HEADER = TRUE
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    REPLACE_INVALID_CHARACTERS = TRUE
    DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
    COMMENT = 'Standard CSV format for insurance data loading';

COPY INTO RAW_DATA.CLAIMS_RAW 
FROM @INSURANCE_PIPELINE_CSV_STAGE/CLAIMS_DATA.csv
FILE_FORMAT = (FORMAT_NAME = 'INSURANCE_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

COPY INTO RAW_DATA.CUSTOMER_RAW
FROM @INSURANCE_PIPELINE_CSV_STAGE/CUSTOMER_DATA.csv
FILE_FORMAT = (FORMAT_NAME = 'INSURANCE_CSV_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

/*
================================================================================
SNOWPIPE SETUP FOR AUTOMATED DATA LOADING
================================================================================
*/

-- Create Snowpipe for automatic claims data loading
CREATE OR REPLACE PIPE CLAIMS_DATA_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO RAW_DATA.CLAIMS_RAW 
    FROM @INSURANCE_PIPELINE_LANDING_STAGE
    PATTERN = '.*CLAIMS_DATA.*\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'INSURANCE_CSV_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE;

-- Create Snowpipe for automatic customer data loading
CREATE OR REPLACE PIPE CUSTOMER_DATA_PIPE
    AUTO_INGEST = TRUE
    AS
    COPY INTO RAW_DATA.CUSTOMER_RAW
    FROM @INSURANCE_PIPELINE_LANDING_STAGE
    PATTERN = '.*CUSTOMER_DATA.*\.csv'
    FILE_FORMAT = (FORMAT_NAME = 'INSURANCE_CSV_FORMAT')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
    ON_ERROR = CONTINUE;
-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('CLAIMS_DATA_PIPE') as CLAIMS_PIPE_STATUS;
SELECT SYSTEM$PIPE_STATUS('CUSTOMER_DATA_PIPE') as CUSTOMER_PIPE_STATUS;

-- Manually refresh the pipes to process any existing files
ALTER PIPE CLAIMS_DATA_PIPE REFRESH;
ALTER PIPE CUSTOMER_DATA_PIPE REFRESH;

/*
================================================================================
INITIAL PRIVILEGE GRANTS
================================================================================
*/
USE SECONDARY ROLES NONE;
USE ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
SELECT * FROM RAW_DATA.CLAIMS_RAW;

USE ROLE ACCOUNTADMIN;
SELECT * FROM RAW_DATA.CLAIMS_RAW;
select count(1) from RAW_DATA.CLAIMS_RAW;
USE SECONDARY ROLES ALL;
-- Grant analyst access to all pipeline components
GRANT SELECT, INSERT ON TABLE RAW_DATA.CLAIMS_RAW TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT SELECT, INSERT ON TABLE RAW_DATA.CUSTOMER_RAW TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;

GRANT READ ON STAGE INSURANCE_PIPELINE_CSV_STAGE TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT READ, WRITE ON STAGE INSURANCE_PIPELINE_LANDING_STAGE TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT READ, WRITE ON STAGE INSURANCE_PIPELINE_WORK_STAGE TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
/*
================================================================================
DATA VALIDATION & PIPELINE STATUS
================================================================================
*/

-- Validate initial data loading
SELECT 
    'INITIAL LOAD STATUS' as CHECK_TYPE,
    COUNT(*) as TOTAL_CLAIMS,
    COUNT(DISTINCT POLICY_NUMBER) as UNIQUE_POLICIES,
    MIN(LOAD_TIMESTAMP) as FIRST_LOADED,
    MAX(LOAD_TIMESTAMP) as LAST_LOADED
FROM RAW_DATA.CLAIMS_RAW;

SELECT 
    'INITIAL LOAD STATUS' as CHECK_TYPE,
    COUNT(*) as TOTAL_CUSTOMERS,
    COUNT(DISTINCT POLICY_NUMBER) as UNIQUE_POLICIES,
    MIN(LOAD_TIMESTAMP) as FIRST_LOADED,
    MAX(LOAD_TIMESTAMP) as LAST_LOADED
FROM RAW_DATA.CUSTOMER_RAW;

/*
================================================================================
ADD NEW DATA TO SEE PIPELINE AUTOMATION
================================================================================
*/
-- Load original CSV files from Git repository
-- COPY FILES
--     INTO @INSURANCE_PIPELINE_LANDING_STAGE
--     FROM '@INSURANCE_PIPELINE_DEMO_REPO/branches/main/sample_data/'
--     pattern='.*CLAIMS_DATA_BATCH.*\.csv';
-- -- Load original CSV files from Git repository
-- COPY FILES
--     INTO @INSURANCE_PIPELINE_LANDING_STAGE
--     FROM '@INSURANCE_PIPELINE_DEMO_REPO/branches/main/sample_data/'
--     pattern='.*CUSTOMER_DATA_.*\.csv';

-- drop table RAW_DATA.CLAIMS_RAW;
-- drop table RAW_DATA.CUSTOMER_RAW;
-- drop stage INSURANCE_PIPELINE_CSV_STAGE;
-- drop stage INSURANCE_PIPELINE_LANDING_STAGE;