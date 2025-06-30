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
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

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
    ORIGIN = 'https://github.com/MrHarBear/Snowflake-Insurance-data-sharing.git'
    GIT_CREDENTIALS = NULL
    COMMENT = 'Repository with insurance demo data';

-- Refresh repository to access latest files
ALTER GIT REPOSITORY INSURANCE_PIPELINE_DEMO_REPO FETCH;

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
    FROM '@INSURANCE_PIPELINE_DEMO_REPO/branches/main/'
    PATTERN='CLAIMS_DATA.csv';

COPY FILES
    INTO @INSURANCE_PIPELINE_CSV_STAGE
    FROM '@INSURANCE_PIPELINE_DEMO_REPO/branches/main/'
    PATTERN='CUSTOMER_DATA.csv';

-- Load additional demo batch files
COPY FILES
    INTO @INSURANCE_PIPELINE_CSV_STAGE
    FROM '@INSURANCE_PIPELINE_DEMO_REPO/branches/main/Insurance_Data_Sharing_Demo/sample_data/'
    PATTERN='*.csv';

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

-- Load initial data with pipeline tracking
COPY INTO RAW_DATA.CLAIMS_RAW 
    (POLICY_NUMBER, INCIDENT_DATE, INCIDENT_TYPE, INCIDENT_SEVERITY, 
     AUTHORITIES_CONTACTED, INCIDENT_HOUR_OF_THE_DAY, NUMBER_OF_VEHICLES_INVOLVED,
     BODILY_INJURIES, WITNESSES, POLICE_REPORT_AVAILABLE, CLAIM_AMOUNT, 
     FRAUD_REPORTED, FILE_NAME)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 
           METADATA$FILENAME as FILE_NAME
    FROM @INSURANCE_PIPELINE_CSV_STAGE/CLAIMS_DATA.csv
)
FILE_FORMAT = (
    TYPE = CSV,
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    TRIM_SPACE = TRUE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
    REPLACE_INVALID_CHARACTERS = TRUE,
    DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3',
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
);

COPY INTO RAW_DATA.CUSTOMER_RAW
    (POLICY_NUMBER, AGE, POLICY_START_DATE, POLICY_LENGTH_MONTH,
     POLICY_DEDUCTABLE, POLICY_ANNUAL_PREMIUM, INSURED_SEX,
     INSURED_EDUCATION_LEVEL, INSURED_OCCUPATION, FILE_NAME)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9,
           METADATA$FILENAME as FILE_NAME
    FROM @INSURANCE_PIPELINE_CSV_STAGE/CUSTOMER_DATA.csv
)
FILE_FORMAT = (
    TYPE = CSV,
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    RECORD_DELIMITER = '\n',
    TRIM_SPACE = TRUE,
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE,
    REPLACE_INVALID_CHARACTERS = TRUE,
    DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3',
    TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
);



/*
================================================================================
INITIAL PRIVILEGE GRANTS
================================================================================
*/

-- Grant analyst access to all pipeline components
GRANT SELECT, INSERT ON TABLE RAW_DATA.CLAIMS_RAW TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT SELECT, INSERT ON TABLE RAW_DATA.CUSTOMER_RAW TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;

GRANT USAGE, READ ON STAGE INSURANCE_PIPELINE_CSV_STAGE TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT USAGE, READ, WRITE ON STAGE INSURANCE_PIPELINE_LANDING_STAGE TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;
GRANT USAGE, READ, WRITE ON STAGE INSURANCE_PIPELINE_WORK_STAGE TO ROLE MOUNTAINPEAK_PIPELINE_ANALYST;



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

-- Pipeline health check
SELECT 
    'PIPELINE READINESS' as STATUS,
    CURRENT_DATABASE() as ACTIVE_DATABASE,
    CURRENT_SCHEMA() as ACTIVE_SCHEMA,
    CURRENT_WAREHOUSE() as ACTIVE_WAREHOUSE,
    CURRENT_ROLE() as ACTIVE_ROLE;

-- Environment summary for demo
SELECT 
    'PIPELINE INFRASTRUCTURE' as COMPONENT,
    'Automated stages and Dynamic Table ready' as STATUS
UNION ALL
SELECT 
    'DATA LOADING',
    'Initial CSV data loaded with tracking'

UNION ALL
SELECT 
    'DEMO READINESS',
    'Ready for progressive analytics and governance';

/*
================================================================================
SETUP COMPLETE - AUTOMATED PIPELINE READY
================================================================================
Environment Ready:
  • Database: MOUNTAINPEAK_INSURANCE_PIPELINE_DB with automated pipeline support
  • Warehouses: Optimized for real-time operations
  • Stages: Landing zone ready for file drops
  • Tables: Initial data loaded with pipeline tracking
  • RBAC: MOUNTAINPEAK_PIPELINE_ANALYST role configured

Pipeline Demo Ready:
  1. Drop additional CSV files into INSURANCE_PIPELINE_LANDING_STAGE
  2. Execute simple COPY INTO commands
  3. Watch Dynamic Tables refresh automatically
  4. See governance policies apply to new data

Demo Files Available in Repository:
  - Insurance_Data_Sharing_Demo/sample_data/CLAIMS_DATA_BATCH2.csv
  - Insurance_Data_Sharing_Demo/sample_data/CUSTOMER_DATA_BATCH2.csv
  - Insurance_Data_Sharing_Demo/sample_data/CLAIMS_DATA_BATCH3.csv

Next Steps:
  1. Execute Insurance_Data_Sharing_Demo/01_DATA_QUALITY.sql for monitoring setup
  2. Execute Insurance_Data_Sharing_Demo/02_RISK_ANALYTICS.sql for Dynamic Tables
  3. Create governance demo script for progressive security
  
Quick Demo Commands:
  -- Load additional batches using standard COPY INTO commands
  -- Files are already staged and ready for loading
================================================================================
*/ 