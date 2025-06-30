/*
================================================================================
MOUNTAINPEAK INSURANCE - RISK ANALYTICS & PROGRESSIVE GOVERNANCE
================================================================================
Demo: Real-time risk analytics with Python UDF and progressive data sharing
Purpose: 2-level Dynamic Tables, governance policies, and secure broker access
Target: Alpine Risk Brokers risk intelligence with data protection
================================================================================
*/

USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE INSURANCE_PIPELINE_COMPUTE_WH;
USE ROLE ACCOUNTADMIN;

/*
================================================================================
SECTION 1: PYTHON UDF FOR AGE CATEGORIZATION
================================================================================
*/

-- SQL UDF for age-based risk scoring - more efficient for simple logic
CREATE OR REPLACE FUNCTION ANALYTICS.CALCULATE_AGE_RISK_SCORE(AGE NUMBER)
RETURNS NUMBER
LANGUAGE SQL
COMMENT = 'Returns numeric risk score based on driver age'
AS
$$
    CASE 
        WHEN AGE IS NULL THEN 0
        WHEN AGE < 25 THEN 25
        WHEN AGE < 35 THEN 15 
        WHEN AGE < 55 THEN 5
        ELSE 10
    END
$$;

-- Python UDF for standardized driver age categorization
CREATE OR REPLACE FUNCTION ANALYTICS.CATEGORIZE_DRIVER_AGE(AGE NUMBER)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'categorize_age'
COMMENT = 'Categorizes driver age into risk-based groups for insurance analytics'
AS
$$
def categorize_age(age):
    """
    Categorizes driver age into standard insurance risk groups
    Returns: Young Driver, Middle Age, Mature Driver, or Senior Driver
    """
    if age is None:
        return 'Unknown'
    elif age < 25:
        return 'Young Driver'
    elif age < 45:
        return 'Middle Age'
    elif age < 65:
        return 'Mature Driver'
    else:
        return 'Senior Driver'
$$;

-- Python UDF for comprehensive risk scoring
CREATE OR REPLACE FUNCTION ANALYTICS.CALCULATE_TOTAL_RISK_SCORE(AGE NUMBER, CLAIM_AMOUNT NUMBER, FRAUD_REPORTED BOOLEAN)
RETURNS NUMBER
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
HANDLER = 'calculate_total_risk'
COMMENT = 'Calculates comprehensive risk score from age, claim amount, and fraud history'
AS
$$
def calculate_total_risk(age, claim_amount, fraud_reported):
    """
    Calculates total risk score combining multiple factors
    Returns: Total risk score (10-115)
    """
    # Age risk component
    if age is None:
        age_risk = 0
    elif age < 25:
        age_risk = 25
    elif age < 35:
        age_risk = 15
    elif age < 55:
        age_risk = 5
    else:
        age_risk = 10
    
    # Claim amount risk component
    if claim_amount is None:
        claim_risk = 5
    elif claim_amount > 75000:
        claim_risk = 40
    elif claim_amount > 25000:
        claim_risk = 20
    else:
        claim_risk = 5
    
    # Fraud risk component
    fraud_risk = 50 if fraud_reported else 0
    
    return age_risk + claim_risk + fraud_risk
$$;

-- Test the Python UDFs
SELECT 
    'Python UDF Test' as TEST_TYPE,
    ANALYTICS.CATEGORIZE_DRIVER_AGE(22) as YOUNG_DRIVER_CATEGORY,
    ANALYTICS.CALCULATE_AGE_RISK_SCORE(22) as YOUNG_DRIVER_SCORE,
    ANALYTICS.CALCULATE_TOTAL_RISK_SCORE(22, 80000, TRUE) as HIGH_RISK_TOTAL,
    ANALYTICS.CALCULATE_TOTAL_RISK_SCORE(45, 15000, FALSE) as LOW_RISK_TOTAL;

/*
================================================================================
SECTION 2: LEVEL 1 DYNAMIC TABLE - CUSTOMER CLAIMS INTEGRATION
================================================================================
*/

-- Level 1: Foundational customer-claims data integration
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.CUSTOMER_CLAIMS_INTEGRATED
    TARGET_LAG = '1 minute'
    WAREHOUSE = INSURANCE_PIPELINE_COMPUTE_WH
    COMMENT = 'Level 1: Customer and claims data integration with derived fields'
    AS
SELECT 
    -- Customer core data
    c.POLICY_NUMBER,
    c.AGE,
    c.INSURED_SEX,
    c.INSURED_EDUCATION_LEVEL,
    c.INSURED_OCCUPATION,
    c.POLICY_START_DATE,
    c.POLICY_LENGTH_MONTH,
    c.POLICY_DEDUCTABLE,
    c.POLICY_ANNUAL_PREMIUM,
    
    -- Claims data (NULL if no claims)
    cl.INCIDENT_DATE,
    cl.INCIDENT_TYPE,
    cl.INCIDENT_SEVERITY,
    cl.AUTHORITIES_CONTACTED,
    cl.INCIDENT_HOUR_OF_THE_DAY,
    cl.NUMBER_OF_VEHICLES_INVOLVED,
    cl.BODILY_INJURIES,
    cl.WITNESSES,
    cl.POLICE_REPORT_AVAILABLE,
    cl.CLAIM_AMOUNT,
    cl.FRAUD_REPORTED,
    
    -- Derived business fields
    CASE WHEN cl.POLICY_NUMBER IS NOT NULL THEN 1 ELSE 0 END as HAS_CLAIM,
    COALESCE(cl.CLAIM_AMOUNT, 0) as CLAIM_AMOUNT_FILLED,
    COALESCE(cl.FRAUD_REPORTED, FALSE) as FRAUD_REPORTED_FILLED,
    
    -- Simulated customer geography for row access demo
    CASE 
        WHEN MOD(HASH(c.POLICY_NUMBER), 10) <= 2 THEN 'Colorado'
        WHEN MOD(HASH(c.POLICY_NUMBER), 10) <= 4 THEN 'Utah'  
        WHEN MOD(HASH(c.POLICY_NUMBER), 10) <= 6 THEN 'Wyoming'
        ELSE 'Other States'
    END as CUSTOMER_STATE,
    
    -- Data lineage tracking
    GREATEST(c.LOAD_TIMESTAMP, COALESCE(cl.LOAD_TIMESTAMP, c.LOAD_TIMESTAMP)) as LAST_UPDATED
    
FROM RAW_DATA.CUSTOMER_RAW c
LEFT JOIN RAW_DATA.CLAIMS_RAW cl ON c.POLICY_NUMBER = cl.POLICY_NUMBER;

/*
================================================================================
SECTION 3: LEVEL 2 DYNAMIC TABLE - RISK SCORING WITH PYTHON UDF
================================================================================
*/

-- Level 2: Risk scoring using Python UDFs for efficient calculation
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.RISK_SCORE_MATRIX
    TARGET_LAG = '1 minute'
    WAREHOUSE = INSURANCE_PIPELINE_COMPUTE_WH
    COMMENT = 'Level 2: Risk scoring with Python UDFs for comprehensive analysis'
    AS
SELECT 
    -- Core identifiers and data
    POLICY_NUMBER,
    AGE,
    INSURED_SEX,
    INSURED_EDUCATION_LEVEL,
    INSURED_OCCUPATION,
    POLICY_ANNUAL_PREMIUM,
    CLAIM_AMOUNT_FILLED,
    FRAUD_REPORTED_FILLED,
    CUSTOMER_STATE,
    
    -- Python UDF results - efficient single calculations
    ANALYTICS.CATEGORIZE_DRIVER_AGE(AGE) as AGE_CATEGORY,
    ANALYTICS.CALCULATE_AGE_RISK_SCORE(AGE) as AGE_RISK_SCORE,
    ANALYTICS.CALCULATE_TOTAL_RISK_SCORE(AGE, CLAIM_AMOUNT_FILLED, FRAUD_REPORTED_FILLED) as TOTAL_RISK_SCORE,
    
    -- Risk level classification using UDF result
    CASE 
        WHEN ANALYTICS.CALCULATE_TOTAL_RISK_SCORE(AGE, CLAIM_AMOUNT_FILLED, FRAUD_REPORTED_FILLED) >= 80 THEN 'HIGH'
        WHEN ANALYTICS.CALCULATE_TOTAL_RISK_SCORE(AGE, CLAIM_AMOUNT_FILLED, FRAUD_REPORTED_FILLED) >= 40 THEN 'MEDIUM'
        ELSE 'LOW'
    END as RISK_LEVEL,
    
    -- Risk factors for transparency using efficient logic
    ARRAY_CONSTRUCT_COMPACT(
        CASE WHEN AGE < 25 THEN 'Young Driver' END,
        CASE WHEN AGE > 65 THEN 'Senior Driver' END,
        CASE WHEN CLAIM_AMOUNT_FILLED > 75000 THEN 'High Claim Amount' END,
        CASE WHEN FRAUD_REPORTED_FILLED = TRUE THEN 'Fraud History' END
    ) as RISK_FACTORS,
    
    LAST_UPDATED
    
FROM ANALYTICS.CUSTOMER_CLAIMS_INTEGRATED;

select * from ANALYTICS.RISK_SCORE_MATRIX;

/*
================================================================================
SECTION 4: AUTOMATIC SENSITIVE DATA CLASSIFICATION
================================================================================
*/

-- Create governance schema for policies
USE SCHEMA GOVERNANCE;

-- Create a classification profile for automatic sensitive data detection
CREATE OR REPLACE SNOWFLAKE.DATA_PRIVACY.CLASSIFICATION_PROFILE INSURANCE_CLASSIFICATION_PROFILE(
    {
        'minimum_object_age_for_classification_days': 0,
        'maximum_classification_validity_days': 30,
        'auto_tag': true
    }
);

-- Set the classification profile on the Analytics schema
ALTER SCHEMA ANALYTICS SET CLASSIFICATION_PROFILE = 'MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.INSURANCE_CLASSIFICATION_PROFILE';

-- Run classification on our risk scoring table
CALL SYSTEM$CLASSIFY(
    'MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX',
    'MOUNTAINPEAK_INSURANCE_PIPELINE_DB.GOVERNANCE.INSURANCE_CLASSIFICATION_PROFILE'
);

-- View classification results
CALL SYSTEM$GET_CLASSIFICATION_RESULT('MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX');

-- Check what tags were automatically applied
SELECT 
    COLUMN_NAME,
    TAG_NAME,
    TAG_VALUE
FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES('MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX', 'COLUMN'));

/*
================================================================================
SECTION 5: PROGRESSIVE GOVERNANCE - DATA MASKING WITH CLASSIFICATION
================================================================================
*/

-- Tag-based masking policy leveraging auto-classification results
CREATE OR REPLACE MASKING POLICY GOVERNANCE.MASK_SENSITIVE_DATA AS 
    (sensitive_value NUMBER) RETURNS NUMBER ->
    CASE
        -- Internal roles get full access
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN sensitive_value  
        -- External consumers get masked values for classified sensitive data
        WHEN CURRENT_ROLE() IN ('MOUNTAINPEAK_PIPELINE_ANALYST') OR 
             CURRENT_ACCOUNT_NAME() LIKE '%CONSUMER%' THEN FLOOR(sensitive_value / 10000) * 10000
        ELSE FLOOR(sensitive_value / 10000) * 10000
    END
    COMMENT = 'Masks sensitive financial data based on auto-classification';

-- Apply tag-based masking to any columns classified as sensitive
-- Note: This will automatically apply to columns with SNOWFLAKE.CORE.SEMANTIC_CATEGORY tag
ALTER TAG SNOWFLAKE.CORE.SEMANTIC_CATEGORY 
    SET MASKING POLICY GOVERNANCE.MASK_SENSITIVE_DATA;

use role mountainpeak_pipeline_analyst;
select * from ANALYTICS.RISK_SCORE_MATRIX;
use role accountadmin;
select * from ANALYTICS.RISK_SCORE_MATRIX;
/*
================================================================================
SECTION 6: PROGRESSIVE GOVERNANCE - ROW ACCESS CONTROL
================================================================================
*/

-- Row access policy for geographic territory restrictions
CREATE OR REPLACE ROW ACCESS POLICY GOVERNANCE.ALPINE_BROKER_ACCESS AS
    (customer_state STRING) RETURNS BOOLEAN ->
    CASE
        -- Internal roles see all states
        WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN TRUE
        -- Analyst role and external consumers limited to broker territories
        WHEN CURRENT_ROLE() IN ('MOUNTAINPEAK_PIPELINE_ANALYST') OR
             CURRENT_ACCOUNT_NAME() LIKE '%CONSUMER%' 
            THEN customer_state IN ('Colorado', 'Utah', 'Wyoming')
        ELSE FALSE
    END
    COMMENT = 'Restricts broker access to CO/UT/WY territories only';

-- Apply row access policy to risk scoring table
ALTER TABLE ANALYTICS.RISK_SCORE_MATRIX
    ADD ROW ACCESS POLICY GOVERNANCE.ALPINE_BROKER_ACCESS ON (CUSTOMER_STATE);
use role mountainpeak_pipeline_analyst;
select * from ANALYTICS.RISK_SCORE_MATRIX;
select distinct customer_state from ANALYTICS.RISK_SCORE_MATRIX;
use role accountadmin;
select distinct customer_state from ANALYTICS.RISK_SCORE_MATRIX;
select * from ANALYTICS.RISK_SCORE_MATRIX;


/*
================================================================================
SECTION 7: SECURE DATA SHARING - BROKER RISK VIEW
================================================================================
*/

USE SCHEMA ANALYTICS;

-- Create curated view for Alpine Risk Brokers
CREATE OR REPLACE SECURE VIEW ANALYTICS.BROKER_RISK_VIEW 
    COMMENT = 'Curated risk intelligence for Alpine Risk Brokers with governance applied'
    AS
SELECT 
    POLICY_NUMBER,
    AGE_CATEGORY,  -- Python UDF result
    INSURED_OCCUPATION,
    POLICY_ANNUAL_PREMIUM,
    CLAIM_AMOUNT_FILLED,  -- Masked to $10K increments
    CUSTOMER_STATE,       -- Row access limited to CO/UT/WY
    TOTAL_RISK_SCORE,
    RISK_LEVEL,
    RISK_FACTORS,
    LAST_UPDATED
FROM ANALYTICS.RISK_SCORE_MATRIX
WHERE CUSTOMER_STATE IS NOT NULL;  -- Ensure clean data for sharing

use role mountainpeak_pipeline_analyst;
select * from ANALYTICS.BROKER_RISK_VIEW;
select distinct customer_state from ANALYTICS.BROKER_RISK_VIEW;
use role accountadmin;
select distinct customer_state from ANALYTICS.BROKER_RISK_VIEW;
select count(1) from analytics.broker_risk_view;
select * from ANALYTICS.BROKER_RISK_VIEW;

-- Create data share for Alpine Risk Brokers
CREATE OR REPLACE SHARE ALPINE_RISK_SHARE
    COMMENT = 'Risk intelligence share for Alpine Risk Brokers';

GRANT USAGE ON DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB TO SHARE ALPINE_RISK_SHARE;
GRANT USAGE ON SCHEMA ANALYTICS TO SHARE ALPINE_RISK_SHARE;
GRANT SELECT ON VIEW ANALYTICS.BROKER_RISK_VIEW TO SHARE ALPINE_RISK_SHARE;

/*
================================================================================
RISK ANALYTICS & GOVERNANCE COMPLETE
================================================================================
Implementation Complete:
  • Python UDFs: Efficient risk calculation with 3 specialized functions
    - CATEGORIZE_DRIVER_AGE: Standardized age groups  
    - CALCULATE_AGE_RISK_SCORE: Numeric age risk scoring
    - CALCULATE_TOTAL_RISK_SCORE: Comprehensive risk calculation
  • Level 1 Dynamic Table: Customer-claims integration (1 min refresh)
  • Level 2 Dynamic Table: Optimized risk scoring with Python UDFs (1 min refresh)
  • Progressive Governance: Masking (claim amounts) + Row access (geography)
  • Secure Sharing: BROKER_RISK_VIEW with applied governance policies

Business Value Delivered:
  • Real-time risk intelligence refreshing every minute
  • Python-powered comprehensive risk analytics with efficient calculation
  • Territory-appropriate data access for Alpine Risk Brokers
  • Protected sensitive data while preserving analytical utility
  • Maintainable code with centralized risk logic in UDFs

Ready for: Live demonstration of automated pipeline with optimized governance
================================================================================
*/ 
