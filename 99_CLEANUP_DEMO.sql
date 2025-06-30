/*
================================================================================
MOUNTAINPEAK INSURANCE - DEMO CLEANUP SCRIPT
================================================================================
Purpose: Complete cleanup of all demo assets from the Snowflake account
Order:   Remove policies first, then tables, then other objects to avoid errors
================================================================================
*/

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

-- Remove tag-based masking policies
ALTER TAG IF EXISTS SNOWFLAKE.CORE.SEMANTIC_CATEGORY 
    UNSET MASKING POLICY;

-- Remove masking policies from tables
ALTER TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX 
    MODIFY COLUMN CLAIM_AMOUNT_FILLED 
    UNSET MASKING POLICY;

-- Remove row access policies from tables
ALTER TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX
    DROP ROW ACCESS POLICY IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.ALPINE_BROKER_ACCESS;

-- Drop masking policies
DROP MASKING POLICY IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.MASK_SENSITIVE_DATA;
DROP MASKING POLICY IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.MASK_CLAIM_AMOUNT;

-- Drop row access policies
DROP ROW ACCESS POLICY IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.ALPINE_BROKER_ACCESS;

-- Drop aggregation policies (from reference demo)
DROP AGGREGATION POLICY IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.MIN_GROUP_POLICY;

-- Drop projection policies (from reference demo)
DROP PROJECTION POLICY IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.HIDE_FRAUD_INDICATOR;

-- Drop classification profile
DROP CLASSIFICATION_PROFILE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.INSURANCE_CLASSIFICATION_PROFILE;

SELECT 'Governance policies removed' as STATUS;

/*
================================================================================
STEP 2: REMOVE DATA SHARING ASSETS
================================================================================
*/

-- Drop shares
DROP SHARE IF EXISTS ALPINE_RISK_SHARE;
DROP SHARE IF EXISTS BROKER_DATA_SHARE;

-- Drop external listings
DROP LISTING IF EXISTS BROKER_DATA_LISTING;
DROP LISTING IF EXISTS DOCAI_CLAIM_CONSUMER_LISTING;

SELECT 'Data sharing assets removed' as STATUS;

/*
================================================================================
STEP 3: REMOVE DATA METRIC FUNCTIONS
================================================================================
*/

-- Remove DMFs from tables
ALTER TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW 
    DROP DATA METRIC FUNCTION IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INVALID_CLAIM_AMOUNT_COUNT ON (CLAIM_AMOUNT);

ALTER TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW 
    DROP DATA METRIC FUNCTION IF EXISTS SNOWFLAKE.CORE.NULL_COUNT ON (POLICY_NUMBER);

ALTER TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW 
    DROP DATA METRIC FUNCTION IF EXISTS SNOWFLAKE.CORE.DUPLICATE_COUNT ON (POLICY_NUMBER);

ALTER TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW 
    DROP DATA METRIC FUNCTION IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INVALID_CUSTOMER_AGE_COUNT ON (AGE);

ALTER TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW 
    DROP DATA METRIC FUNCTION IF EXISTS SNOWFLAKE.CORE.NULL_COUNT ON (POLICY_NUMBER);

ALTER TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW 
    DROP DATA METRIC FUNCTION IF EXISTS SNOWFLAKE.CORE.ROW_COUNT ON ();

-- Drop custom DMF functions
DROP FUNCTION IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INVALID_CLAIM_AMOUNT_COUNT(TABLE(CLAIM_AMOUNT NUMBER));
DROP FUNCTION IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INVALID_CUSTOMER_AGE_COUNT(TABLE(AGE NUMBER));

SELECT 'Data metric functions removed' as STATUS;

/*
================================================================================
STEP 4: REMOVE SNOWPIPES AND STREAMS
================================================================================
*/

-- Drop Snowpipes
DROP PIPE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_DATA_PIPE;
DROP PIPE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_DATA_PIPE;

SELECT 'Snowpipes removed' as STATUS;

/*
================================================================================
STEP 5: REMOVE DYNAMIC TABLES AND VIEWS
================================================================================
*/

-- Drop secure views
DROP VIEW IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.BROKER_RISK_VIEW;
DROP VIEW IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.VW_RISK_LEVEL_MATRIX;

-- Drop Dynamic Tables (in dependency order)
DROP DYNAMIC TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX;
DROP DYNAMIC TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CUSTOMER_CLAIMS_INTEGRATED;
DROP DYNAMIC TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_LEVEL_MATRIX;

SELECT 'Dynamic tables and views removed' as STATUS;

/*
================================================================================
STEP 6: REMOVE REGULAR TABLES
================================================================================
*/

-- Drop regular tables
DROP TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW;
DROP TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW;
DROP TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CUSTOMER_CLAIMS_JOINED;
DROP TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.SHARING.BROKER_DATA_CLONE;
DROP TABLE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.BROKER_TERRITORY_MAP;

SELECT 'Tables removed' as STATUS;

/*
================================================================================
STEP 7: REMOVE FUNCTIONS (UDFs)
================================================================================
*/

-- Drop Python UDFs
DROP FUNCTION IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CATEGORIZE_DRIVER_AGE(NUMBER);
DROP FUNCTION IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CALCULATE_TOTAL_RISK_SCORE(NUMBER, NUMBER, BOOLEAN);

-- Drop SQL UDFs
DROP FUNCTION IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.CALCULATE_AGE_RISK_SCORE(NUMBER);

SELECT 'User-defined functions removed' as STATUS;

/*
================================================================================
STEP 8: REMOVE STAGES AND FILE FORMATS
================================================================================
*/

-- Drop stages
DROP STAGE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_PIPELINE_CSV_STAGE;
DROP STAGE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_PIPELINE_LANDING_STAGE;
DROP STAGE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_PIPELINE_WORK_STAGE;

-- Drop file formats
DROP FILE FORMAT IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_CSV_FORMAT;

SELECT 'Stages and file formats removed' as STATUS;

/*
================================================================================
STEP 9: REMOVE GIT INTEGRATION
================================================================================
*/

-- Drop Git repository
DROP GIT REPOSITORY IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.INSURANCE_PIPELINE_DEMO_REPO;

-- Drop Git API integration
DROP API INTEGRATION IF EXISTS INSURANCE_PIPELINE_GIT_INTEGRATION;

SELECT 'Git integration removed' as STATUS;

/*
================================================================================
STEP 10: REMOVE WAREHOUSES
================================================================================
*/

-- Drop warehouses
DROP WAREHOUSE IF EXISTS INSURANCE_PIPELINE_COMPUTE_WH;
DROP WAREHOUSE IF EXISTS INSURANCE_PIPELINE_OPS_WH;
DROP WAREHOUSE IF EXISTS INSURANCE_COMPUTE_WH;

SELECT 'Warehouses removed' as STATUS;

/*
================================================================================
STEP 11: REMOVE ROLES AND PRIVILEGES
================================================================================
*/

-- Note: Be careful with role cleanup - only remove if you're sure
-- Remove role assignments from current user
-- REVOKE ROLE MOUNTAINPEAK_PIPELINE_ANALYST FROM USER identifier(current_user());

-- Drop custom roles
USE ROLE USERADMIN;
DROP ROLE IF EXISTS MOUNTAINPEAK_PIPELINE_ANALYST;
DROP ROLE IF EXISTS MOUNTAINPEAK_ANALYST;

USE ROLE ACCOUNTADMIN;
SELECT 'Roles removed' as STATUS;

/*
================================================================================
STEP 12: REMOVE DATABASE (FINAL STEP)
================================================================================
*/

-- Drop the entire database (this removes all remaining objects)
DROP DATABASE IF EXISTS MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
DROP DATABASE IF EXISTS MOUNTAINPEAK_INSURANCE_DB;

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
SHOW SHARES LIKE '%ALPINE%';
SHOW SHARES LIKE '%BROKER%';

SELECT 'Demo cleanup completed successfully!' as FINAL_STATUS;

/*
================================================================================
CLEANUP COMPLETE
================================================================================
All MountainPeak Insurance demo assets have been removed from the account:

✅ Governance policies (masking, row access, aggregation, projection)
✅ Data sharing assets (shares, listings)
✅ Data metric functions (custom and system DMFs)
✅ Snowpipes and automation
✅ Dynamic Tables and views
✅ Regular tables and data
✅ User-defined functions (Python and SQL)
✅ Stages and file formats
✅ Git integration and repositories
✅ Warehouses
✅ Custom roles
✅ Databases

The account has been restored to its pre-demo state.
================================================================================
*/ 