/*
================================================================================
MOUNTAINPEAK INSURANCE - DEMO CLEANUP SCRIPT
================================================================================
Purpose: Complete cleanup of all demo assets from the Snowflake account
Order:   Remove policies first, then tables, then other objects to avoid errors
================================================================================
*/

USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE INSURANCE_PIPELINE_COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

-- Show current state before cleanup
SELECT 'Starting cleanup of MountainPeak Insurance Demo' as STATUS;

/*
================================================================================
STEP 1: REMOVE GOVERNANCE POLICIES & CLASSIFICATION
================================================================================
*/

-- Remove auto-classification from schema
ALTER SCHEMA MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS 
    UNSET CLASSIFICATION_PROFILE;

-- Remove masking policies from tables
ALTER TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX 
    MODIFY COLUMN CLAIM_AMOUNT_FILLED 
    UNSET MASKING POLICY;

-- Remove row access policies from tables
ALTER TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX
    DROP ROW ACCESS POLICY MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.ALPINE_BROKER_ACCESS;

-- Drop masking policies (no IF EXISTS for policies)
DROP MASKING POLICY MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.MASK_SENSITIVE_DATA;

-- Drop row access policies (no IF EXISTS for policies)
DROP ROW ACCESS POLICY MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.ALPINE_BROKER_ACCESS;

-- Drop classification profile (no IF EXISTS for classification profiles)
DROP CLASSIFICATION_PROFILE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.INSURANCE_CLASSIFICATION_PROFILE;

SELECT 'Governance policies removed' as STATUS;

/*
================================================================================
STEP 2: REMOVE DATA SHARING ASSETS
================================================================================
*/

-- Drop shares (only ALPINE_RISK_SHARE is actually created)
DROP SHARE ALPINE_RISK_SHARE;

SELECT 'Data sharing assets removed' as STATUS;

/*
================================================================================
STEP 3: REMOVE DATA METRIC FUNCTIONS
================================================================================
*/

-- Remove DMFs from tables (from 01_DATA_QUALITY.sql)
ALTER TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW 
    DROP DATA METRIC FUNCTION MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INVALID_CLAIM_AMOUNT_COUNT ON (CLAIM_AMOUNT);

ALTER TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW 
    DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (POLICY_NUMBER);

ALTER TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW 
    DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (POLICY_NUMBER);

ALTER TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW 
    DROP DATA METRIC FUNCTION MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INVALID_CUSTOMER_AGE_COUNT ON (AGE);

ALTER TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW 
    DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (POLICY_NUMBER);

ALTER TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW 
    DROP DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

-- Drop custom DMF functions (no IF EXISTS for functions)
DROP FUNCTION MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INVALID_CLAIM_AMOUNT_COUNT(TABLE(CLAIM_AMOUNT NUMBER));
DROP FUNCTION MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INVALID_CUSTOMER_AGE_COUNT(TABLE(AGE NUMBER));

SELECT 'Data metric functions removed' as STATUS;

/*
================================================================================
STEP 4: REMOVE SNOWPIPES
================================================================================
*/

-- Drop Snowpipes (from 00_AUTOMATED_PIPELINE_SETUP.sql)
DROP PIPE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_DATA_PIPE;
DROP PIPE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_DATA_PIPE;

SELECT 'Snowpipes removed' as STATUS;

/*
================================================================================
STEP 5: REMOVE DYNAMIC TABLES AND VIEWS
================================================================================
*/

-- Drop secure views (from 02_RISK_ANALYTICS.sql)
DROP VIEW MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.BROKER_RISK_VIEW;

-- Drop Dynamic Tables in dependency order (from 02_RISK_ANALYTICS.sql)
DROP DYNAMIC TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX;
DROP DYNAMIC TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CUSTOMER_CLAIMS_INTEGRATED;

SELECT 'Dynamic tables and views removed' as STATUS;

/*
================================================================================
STEP 6: REMOVE REGULAR TABLES
================================================================================
*/

-- Drop regular tables (from 00_AUTOMATED_PIPELINE_SETUP.sql)
DROP TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW;
DROP TABLE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW;

SELECT 'Tables removed' as STATUS;

/*
================================================================================
STEP 7: REMOVE FUNCTIONS (UDFs)
================================================================================
*/

-- Drop Python UDFs (from 02_RISK_ANALYTICS.sql) - no IF EXISTS for functions
DROP FUNCTION MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CATEGORIZE_DRIVER_AGE(NUMBER);
DROP FUNCTION MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CALCULATE_TOTAL_RISK_SCORE(NUMBER, NUMBER, BOOLEAN);

-- Drop SQL UDFs (from 02_RISK_ANALYTICS.sql) - no IF EXISTS for functions
DROP FUNCTION MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CALCULATE_AGE_RISK_SCORE(NUMBER);

SELECT 'User-defined functions removed' as STATUS;

/*
================================================================================
STEP 8: REMOVE STAGES AND FILE FORMATS
================================================================================
*/

-- Drop stages (from 00_AUTOMATED_PIPELINE_SETUP.sql)
DROP STAGE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_PIPELINE_CSV_STAGE;
DROP STAGE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_PIPELINE_LANDING_STAGE;
DROP STAGE MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_PIPELINE_WORK_STAGE;

-- Drop file formats (from 00_AUTOMATED_PIPELINE_SETUP.sql)
DROP FILE FORMAT MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_CSV_FORMAT;

SELECT 'Stages and file formats removed' as STATUS;

/*
================================================================================
STEP 9: REMOVE GIT INTEGRATION
================================================================================
*/

-- Drop Git repository (from 00_AUTOMATED_PIPELINE_SETUP.sql)
DROP GIT REPOSITORY MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_PIPELINE_DEMO_REPO;

-- Drop Git API integration (from 00_AUTOMATED_PIPELINE_SETUP.sql)
DROP API INTEGRATION INSURANCE_PIPELINE_GIT_INTEGRATION;

SELECT 'Git integration removed' as STATUS;

/*
================================================================================
STEP 10: REMOVE WAREHOUSES
================================================================================
*/

-- Drop warehouses (from 00_AUTOMATED_PIPELINE_SETUP.sql)
DROP WAREHOUSE INSURANCE_PIPELINE_COMPUTE_WH;
DROP WAREHOUSE INSURANCE_PIPELINE_OPS_WH;

SELECT 'Warehouses removed' as STATUS;

/*
================================================================================
STEP 11: REMOVE ROLES AND PRIVILEGES
================================================================================
*/

-- Note: Be careful with role cleanup - only remove if you're sure
-- Remove role assignments from current user (optional)
-- REVOKE ROLE MOUNTAINPEAK_PIPELINE_ANALYST FROM USER identifier(current_user());

-- Drop custom roles (from 00_AUTOMATED_PIPELINE_SETUP.sql)
USE ROLE USERADMIN;
DROP ROLE MOUNTAINPEAK_PIPELINE_ANALYST;

USE ROLE ACCOUNTADMIN;
SELECT 'Roles removed' as STATUS;

/*
================================================================================
STEP 12: REMOVE DATABASE (FINAL STEP)
================================================================================
*/

-- Drop the entire database (this removes all remaining objects)
-- From 00_AUTOMATED_PIPELINE_SETUP.sql
DROP DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;

SELECT 'Database removed - Demo cleanup complete!' as STATUS;

/*
================================================================================
CLEANUP VERIFICATION
================================================================================
*/

-- Verify cleanup
SHOW DATABASES LIKE 'MOUNTAINPEAK%';
SHOW WAREHOUSES LIKE 'INSURANCE%';
SHOW ROLES LIKE 'MOUNTAINPEAK%';
SHOW SHARES LIKE 'ALPINE%';

SELECT 'Demo cleanup completed successfully!' as FINAL_STATUS;

/*
================================================================================
CLEANUP COMPLETE
================================================================================
All MountainPeak Insurance demo assets have been removed from the account:

✅ Auto-classification profile and masking/row access policies (02_RISK_ANALYTICS.sql)
✅ Data sharing assets - ALPINE_RISK_SHARE (02_RISK_ANALYTICS.sql)
✅ Data metric functions - custom and system DMFs (01_DATA_QUALITY.sql)
✅ Snowpipes - automated data loading (00_AUTOMATED_PIPELINE_SETUP.sql)
✅ Dynamic Tables - RISK_SCORE_MATRIX, CUSTOMER_CLAIMS_INTEGRATED (02_RISK_ANALYTICS.sql)
✅ Secure Views - BROKER_RISK_VIEW (02_RISK_ANALYTICS.sql)
✅ Raw data tables - CLAIMS_RAW, CUSTOMER_RAW (00_AUTOMATED_PIPELINE_SETUP.sql)
✅ User-defined functions - 3 UDFs for risk calculation (02_RISK_ANALYTICS.sql)
✅ Stages and file formats - data loading infrastructure (00_AUTOMATED_PIPELINE_SETUP.sql)
✅ Git integration and repositories (00_AUTOMATED_PIPELINE_SETUP.sql)
✅ Warehouses - INSURANCE_PIPELINE_COMPUTE_WH, INSURANCE_PIPELINE_OPS_WH (00_AUTOMATED_PIPELINE_SETUP.sql)
✅ Custom roles - MOUNTAINPEAK_PIPELINE_ANALYST (00_AUTOMATED_PIPELINE_SETUP.sql)
✅ Database - MOUNTAINPEAK_INSURANCE_PIPELINE_DB (00_AUTOMATED_PIPELINE_SETUP.sql)

The account has been restored to its pre-demo state.
Only assets from the main demo files (00, 01, 02) are removed - no reference demo artifacts.
================================================================================
*/ 