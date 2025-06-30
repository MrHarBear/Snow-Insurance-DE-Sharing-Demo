/*
================================================================================
MOUNTAINPEAK INSURANCE - DATA QUALITY & MONITORING
================================================================================
Demo:    Streamlined DMF showcase for automated pipeline monitoring
Purpose: Essential data quality checks with Snowflake DMFs
Data:    Claims and customer data with automated quality scoring
================================================================================
*/

USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
USE SCHEMA RAW_DATA;
USE WAREHOUSE INSURANCE_PIPELINE_COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

/*
================================================================================
STREAMLINED DATA METRIC FUNCTIONS - SHOWCASE SETUP
================================================================================
*/

-- Custom DMF for Claims: Validate claim amounts are reasonable
CREATE OR REPLACE DATA METRIC FUNCTION RAW_DATA.INVALID_CLAIM_AMOUNT_COUNT(
    INPUT_TABLE TABLE(CLAIM_AMOUNT NUMBER)
)
RETURNS NUMBER
LANGUAGE SQL    -- can mix and match languages
COMMENT = 'Count of invalid claim amounts (outside $100-$500,000 range)'
AS
'SELECT COUNT_IF(
    CLAIM_AMOUNT IS NOT NULL 
    AND (CLAIM_AMOUNT < 100 OR CLAIM_AMOUNT > 500000)
) FROM INPUT_TABLE';

-- Custom DMF for Customer: Validate customer age range
CREATE OR REPLACE DATA METRIC FUNCTION RAW_DATA.INVALID_CUSTOMER_AGE_COUNT(
    INPUT_TABLE TABLE(AGE NUMBER)
)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Count of invalid customer ages (outside 18-85 range)'
AS
'SELECT COUNT_IF(
    AGE IS NOT NULL 
    AND (AGE < 18 OR AGE > 85)
) FROM INPUT_TABLE';

/*
================================================================================
APPLY DATA METRIC FUNCTIONS TO TABLES
================================================================================
*/

-- Set up automated monitoring schedule
ALTER TABLE RAW_DATA.CLAIMS_RAW SET DATA_METRIC_SCHEDULE = '5 minute';
ALTER TABLE RAW_DATA.CUSTOMER_RAW SET DATA_METRIC_SCHEDULE = '5 minute';

-- CLAIMS TABLE: 1 Custom + 2 System DMFs
-- Custom function we've created
ALTER TABLE RAW_DATA.CLAIMS_RAW ADD DATA METRIC FUNCTION RAW_DATA.INVALID_CLAIM_AMOUNT_COUNT ON (CLAIM_AMOUNT);
-- Built-in funtions
ALTER TABLE RAW_DATA.CLAIMS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (POLICY_NUMBER);
ALTER TABLE RAW_DATA.CLAIMS_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.DUPLICATE_COUNT ON (POLICY_NUMBER);

-- CUSTOMER TABLE: 1 Custom + 2 System DMFs
-- Custom function we've created
ALTER TABLE RAW_DATA.CUSTOMER_RAW ADD DATA METRIC FUNCTION RAW_DATA.INVALID_CUSTOMER_AGE_COUNT ON (AGE);
-- Built-in funtions
ALTER TABLE RAW_DATA.CUSTOMER_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (POLICY_NUMBER);
ALTER TABLE RAW_DATA.CUSTOMER_RAW ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.ROW_COUNT ON ();

/*
================================================================================
MONITOR DATA METRIC FUNCTION RESULTS
================================================================================
*/

-- Review active data metric functions
SELECT 
    metric_name, 
    ref_entity_name, 
    schedule, 
    schedule_status 
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    ref_entity_name => 'MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CLAIMS_RAW', 
    ref_entity_domain => 'TABLE'
))
UNION ALL
SELECT 
    metric_name, 
    ref_entity_name, 
    schedule, 
    schedule_status 
FROM TABLE(INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    ref_entity_name => 'MOUNTAINPEAK_INSURANCE_PIPELINE_DB.RAW_DATA.CUSTOMER_RAW', 
    ref_entity_domain => 'TABLE'
));

-- View real-time data quality monitoring results
SELECT 
    change_commit_time,
    measurement_time,
    table_database,
    table_schema,
    table_name,
    metric_name,
    value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
WHERE table_database = 'MOUNTAINPEAK_INSURANCE_PIPELINE_DB'
    AND table_schema = 'RAW_DATA'
    AND table_name IN ('CLAIMS_RAW', 'CUSTOMER_RAW')
ORDER BY change_commit_time DESC, table_name, metric_name;

/*
================================================================================
DMF SHOWCASE COMPLETE - STREAMLIT READY
================================================================================
Streamlined Setup Complete:
  • 6 Data Metric Functions (3 per table)
  • 2 Custom DMFs showcasing business logic validation
  • 4 System DMFs showcasing Snowflake capabilities
  • Real-time automated monitoring every 5 minutes

DMF Showcase:
  Claims Table:
    - INVALID_CLAIM_AMOUNT_COUNT (Custom): Business rule validation
    - NULL_COUNT on POLICY_NUMBER (System): Data completeness
    - DUPLICATE_COUNT on POLICY_NUMBER (System): Data uniqueness
  
  Customer Table:
    - INVALID_CUSTOMER_AGE_COUNT (Custom): Business rule validation  
    - NULL_COUNT on POLICY_NUMBER (System): Data completeness
    - ROW_COUNT (System): Volume monitoring

Ready for: Streamlit Dashboard monitoring with real-time DMF results
================================================================================
*/ 