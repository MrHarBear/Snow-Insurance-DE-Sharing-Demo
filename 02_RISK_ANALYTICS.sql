/*
================================================================================
MOUNTAINPEAK INSURANCE - AUTOMATED RISK ANALYTICS
================================================================================
Demo:    Real-time risk analytics with Dynamic Tables
Purpose: Automated data transformations and risk scoring with live refresh
Data:    Claims and customer data with continuous analytics updates
================================================================================
*/

USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE INSURANCE_PIPELINE_COMPUTE_WH;
USE ROLE MOUNTAINPEAK_PIPELINE_ANALYST;

/*
================================================================================
FOUNDATIONAL DATA INTEGRATION
================================================================================
*/

-- Create integrated customer-claims dataset for analytics
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.CUSTOMER_CLAIMS_INTEGRATED
    TARGET_LAG = '1 minute'
    WAREHOUSE = INSURANCE_PIPELINE_COMPUTE_WH
    AS
SELECT 
    -- Customer information
    c.POLICY_NUMBER,
    c.AGE,
    c.POLICY_START_DATE,
    c.POLICY_LENGTH_MONTH,
    c.POLICY_DEDUCTABLE,
    c.POLICY_ANNUAL_PREMIUM,
    c.INSURED_SEX,
    c.INSURED_EDUCATION_LEVEL,
    c.INSURED_OCCUPATION,
    c.LOAD_TIMESTAMP as CUSTOMER_LOAD_TIME,
    
    -- Claims information
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
    cl.LOAD_TIMESTAMP as CLAIM_LOAD_TIME,
    
    -- Derived analytics fields
    DATEDIFF('YEAR', c.POLICY_START_DATE, cl.INCIDENT_DATE) as YEARS_POLICY_TO_INCIDENT,
    CASE 
        WHEN cl.CLAIM_AMOUNT > 75000 THEN 'High Value'
        WHEN cl.CLAIM_AMOUNT > 25000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as CLAIM_VALUE_CATEGORY,
    
    CASE 
        WHEN c.AGE < 25 THEN 'Young Driver'
        WHEN c.AGE < 45 THEN 'Middle Age'
        WHEN c.AGE < 65 THEN 'Mature Driver'
        ELSE 'Senior Driver'
    END as AGE_CATEGORY,
    
    -- Risk indicators
    CASE 
        WHEN cl.INCIDENT_HOUR_OF_THE_DAY BETWEEN 22 AND 6 THEN TRUE 
        ELSE FALSE 
    END as LATE_NIGHT_INCIDENT,
    
    CASE 
        WHEN cl.NUMBER_OF_VEHICLES_INVOLVED > 2 THEN TRUE
        ELSE FALSE
    END as MULTI_VEHICLE_COMPLEX,
    
    -- Processing metadata
    GREATEST(c.LOAD_TIMESTAMP, cl.LOAD_TIMESTAMP) as LAST_UPDATED
    
FROM RAW_DATA.CUSTOMER_RAW c
INNER JOIN RAW_DATA.CLAIMS_RAW cl 
    ON c.POLICY_NUMBER = cl.POLICY_NUMBER;

/*
================================================================================
REAL-TIME RISK SCORING ENGINE
================================================================================
*/

-- Create comprehensive risk scoring with automatic refresh
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.RISK_SCORE_MATRIX
    TARGET_LAG = '1 minute'
    WAREHOUSE = INSURANCE_PIPELINE_COMPUTE_WH
    AS
SELECT 
    POLICY_NUMBER,
    AGE,
    AGE_CATEGORY,
    INSURED_OCCUPATION,
    CLAIM_AMOUNT,
    CLAIM_VALUE_CATEGORY,
    INCIDENT_TYPE,
    INCIDENT_SEVERITY,
    FRAUD_REPORTED,
    
    -- Risk scoring components
    CASE 
        WHEN AGE < 25 THEN 25
        WHEN AGE < 35 THEN 15 
        WHEN AGE < 55 THEN 5
        ELSE 10
    END as AGE_RISK_SCORE,
    
    CASE 
        WHEN CLAIM_AMOUNT > 100000 THEN 40
        WHEN CLAIM_AMOUNT > 50000 THEN 25
        WHEN CLAIM_AMOUNT > 25000 THEN 15
        ELSE 5
    END as AMOUNT_RISK_SCORE,
    
    CASE 
        WHEN INCIDENT_TYPE = 'Vehicle Theft' THEN 30
        WHEN INCIDENT_TYPE = 'Multi-vehicle Collision' THEN 20
        WHEN INCIDENT_TYPE = 'Single Vehicle Collision' THEN 15
        ELSE 10
    END as INCIDENT_TYPE_RISK_SCORE,
    
    CASE 
        WHEN INCIDENT_SEVERITY = 'Total Loss' THEN 35
        WHEN INCIDENT_SEVERITY = 'Major Damage' THEN 25
        WHEN INCIDENT_SEVERITY = 'Minor Damage' THEN 10
        ELSE 5
    END as SEVERITY_RISK_SCORE,
    
    CASE 
        WHEN FRAUD_REPORTED = TRUE THEN 50
        ELSE 0
    END as FRAUD_RISK_SCORE,
    
    CASE 
        WHEN LATE_NIGHT_INCIDENT = TRUE THEN 15
        ELSE 0
    END as TIME_RISK_SCORE,
    
    CASE 
        WHEN MULTI_VEHICLE_COMPLEX = TRUE THEN 20
        ELSE 0
    END as COMPLEXITY_RISK_SCORE,
    
    -- Composite risk calculation
    (CASE WHEN AGE < 25 THEN 25 WHEN AGE < 35 THEN 15 WHEN AGE < 55 THEN 5 ELSE 10 END +
     CASE WHEN CLAIM_AMOUNT > 100000 THEN 40 WHEN CLAIM_AMOUNT > 50000 THEN 25 WHEN CLAIM_AMOUNT > 25000 THEN 15 ELSE 5 END +
     CASE WHEN INCIDENT_TYPE = 'Vehicle Theft' THEN 30 WHEN INCIDENT_TYPE = 'Multi-vehicle Collision' THEN 20 WHEN INCIDENT_TYPE = 'Single Vehicle Collision' THEN 15 ELSE 10 END +
     CASE WHEN INCIDENT_SEVERITY = 'Total Loss' THEN 35 WHEN INCIDENT_SEVERITY = 'Major Damage' THEN 25 WHEN INCIDENT_SEVERITY = 'Minor Damage' THEN 10 ELSE 5 END +
     CASE WHEN FRAUD_REPORTED = TRUE THEN 50 ELSE 0 END +
     CASE WHEN LATE_NIGHT_INCIDENT = TRUE THEN 15 ELSE 0 END +
     CASE WHEN MULTI_VEHICLE_COMPLEX = TRUE THEN 20 ELSE 0 END) as TOTAL_RISK_SCORE,
    
    -- Risk classification
    CASE 
        WHEN (CASE WHEN AGE < 25 THEN 25 WHEN AGE < 35 THEN 15 WHEN AGE < 55 THEN 5 ELSE 10 END +
              CASE WHEN CLAIM_AMOUNT > 100000 THEN 40 WHEN CLAIM_AMOUNT > 50000 THEN 25 WHEN CLAIM_AMOUNT > 25000 THEN 15 ELSE 5 END +
              CASE WHEN INCIDENT_TYPE = 'Vehicle Theft' THEN 30 WHEN INCIDENT_TYPE = 'Multi-vehicle Collision' THEN 20 WHEN INCIDENT_TYPE = 'Single Vehicle Collision' THEN 15 ELSE 10 END +
              CASE WHEN INCIDENT_SEVERITY = 'Total Loss' THEN 35 WHEN INCIDENT_SEVERITY = 'Major Damage' THEN 25 WHEN INCIDENT_SEVERITY = 'Minor Damage' THEN 10 ELSE 5 END +
              CASE WHEN FRAUD_REPORTED = TRUE THEN 50 ELSE 0 END +
              CASE WHEN LATE_NIGHT_INCIDENT = TRUE THEN 15 ELSE 0 END +
              CASE WHEN MULTI_VEHICLE_COMPLEX = TRUE THEN 20 ELSE 0 END) >= 80 THEN 'HIGH RISK'
        WHEN (CASE WHEN AGE < 25 THEN 25 WHEN AGE < 35 THEN 15 WHEN AGE < 55 THEN 5 ELSE 10 END +
              CASE WHEN CLAIM_AMOUNT > 100000 THEN 40 WHEN CLAIM_AMOUNT > 50000 THEN 25 WHEN CLAIM_AMOUNT > 25000 THEN 15 ELSE 5 END +
              CASE WHEN INCIDENT_TYPE = 'Vehicle Theft' THEN 30 WHEN INCIDENT_TYPE = 'Multi-vehicle Collision' THEN 20 WHEN INCIDENT_TYPE = 'Single Vehicle Collision' THEN 15 ELSE 10 END +
              CASE WHEN INCIDENT_SEVERITY = 'Total Loss' THEN 35 WHEN INCIDENT_SEVERITY = 'Major Damage' THEN 25 WHEN INCIDENT_SEVERITY = 'Minor Damage' THEN 10 ELSE 5 END +
              CASE WHEN FRAUD_REPORTED = TRUE THEN 50 ELSE 0 END +
              CASE WHEN LATE_NIGHT_INCIDENT = TRUE THEN 15 ELSE 0 END +
              CASE WHEN MULTI_VEHICLE_COMPLEX = TRUE THEN 20 ELSE 0 END) >= 50 THEN 'MEDIUM RISK'
        ELSE 'LOW RISK'
    END as RISK_CATEGORY,
    
    LAST_UPDATED
    
FROM ANALYTICS.CUSTOMER_CLAIMS_INTEGRATED;

/*
================================================================================
AUTOMATED BUSINESS INTELLIGENCE VIEWS
================================================================================
*/

-- Real-time fraud detection summary
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.FRAUD_DETECTION_SUMMARY
    TARGET_LAG = '2 minutes'
    WAREHOUSE = INSURANCE_PIPELINE_COMPUTE_WH
    AS
SELECT 
    FRAUD_REPORTED,
    RISK_CATEGORY,
    COUNT(*) as CASE_COUNT,
    ROUND(AVG(CLAIM_AMOUNT), 2) as AVG_CLAIM_AMOUNT,
    ROUND(SUM(CLAIM_AMOUNT), 2) as TOTAL_CLAIM_AMOUNT,
    ROUND(AVG(TOTAL_RISK_SCORE), 2) as AVG_RISK_SCORE,
    COUNT(DISTINCT POLICY_NUMBER) as UNIQUE_POLICIES,
    
    -- Fraud indicators
    COUNT(CASE WHEN LATE_NIGHT_INCIDENT = TRUE THEN 1 END) as LATE_NIGHT_CASES,
    COUNT(CASE WHEN MULTI_VEHICLE_COMPLEX = TRUE THEN 1 END) as COMPLEX_CASES,
    
    -- Statistical measures
    ROUND(STDDEV(CLAIM_AMOUNT), 2) as CLAIM_AMOUNT_STDDEV,
    ROUND(MEDIAN(CLAIM_AMOUNT), 2) as MEDIAN_CLAIM_AMOUNT,
    
    MAX(LAST_UPDATED) as LAST_REFRESHED
    
FROM ANALYTICS.RISK_SCORE_MATRIX
GROUP BY FRAUD_REPORTED, RISK_CATEGORY
ORDER BY FRAUD_REPORTED DESC, RISK_CATEGORY;

-- Real-time demographic risk analysis
CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.DEMOGRAPHIC_RISK_ANALYSIS
    TARGET_LAG = '2 minutes'
    WAREHOUSE = INSURANCE_PIPELINE_COMPUTE_WH
    AS
SELECT 
    AGE_CATEGORY,
    INSURED_SEX,
    INSURED_EDUCATION_LEVEL,
    INSURED_OCCUPATION,
    
    -- Risk metrics
    COUNT(*) as TOTAL_CLAIMS,
    ROUND(AVG(TOTAL_RISK_SCORE), 2) as AVG_RISK_SCORE,
    ROUND(AVG(CLAIM_AMOUNT), 2) as AVG_CLAIM_AMOUNT,
    
    -- Distribution analysis
    COUNT(CASE WHEN RISK_CATEGORY = 'HIGH RISK' THEN 1 END) as HIGH_RISK_COUNT,
    COUNT(CASE WHEN RISK_CATEGORY = 'MEDIUM RISK' THEN 1 END) as MEDIUM_RISK_COUNT,
    COUNT(CASE WHEN RISK_CATEGORY = 'LOW RISK' THEN 1 END) as LOW_RISK_COUNT,
    
    -- Fraud correlation
    COUNT(CASE WHEN FRAUD_REPORTED = TRUE THEN 1 END) as FRAUD_CASES,
    ROUND((COUNT(CASE WHEN FRAUD_REPORTED = TRUE THEN 1 END) * 100.0) / COUNT(*), 2) as FRAUD_RATE_PCT,
    
    -- Financial impact
    ROUND(SUM(CLAIM_AMOUNT), 2) as TOTAL_EXPOSURE,
    ROUND(MAX(CLAIM_AMOUNT), 2) as MAX_CLAIM_AMOUNT,
    
    MAX(LAST_UPDATED) as LAST_REFRESHED
    
FROM ANALYTICS.RISK_SCORE_MATRIX
GROUP BY AGE_CATEGORY, INSURED_SEX, INSURED_EDUCATION_LEVEL, INSURED_OCCUPATION
HAVING COUNT(*) >= 2  -- Only show groups with multiple claims
ORDER BY AVG_RISK_SCORE DESC, TOTAL_EXPOSURE DESC;

/*
================================================================================
AUTOMATED MONITORING & ALERTING VIEWS
================================================================================
*/

-- High-risk claims requiring immediate attention
CREATE OR REPLACE VIEW ANALYTICS.HIGH_RISK_ALERTS AS
SELECT 
    POLICY_NUMBER,
    CLAIM_AMOUNT,
    TOTAL_RISK_SCORE,
    RISK_CATEGORY,
    FRAUD_REPORTED,
    INCIDENT_TYPE,
    INCIDENT_SEVERITY,
    AGE_CATEGORY,
    
    -- Alert reasons
    CASE 
        WHEN FRAUD_REPORTED = TRUE AND TOTAL_RISK_SCORE >= 100 THEN 'CRITICAL: Fraud + High Risk Score'
        WHEN FRAUD_REPORTED = TRUE THEN 'HIGH: Fraud Reported'
        WHEN TOTAL_RISK_SCORE >= 100 THEN 'HIGH: Risk Score >= 100'
        WHEN CLAIM_AMOUNT >= 100000 THEN 'HIGH: Claim Amount >= $100K'
        ELSE 'MEDIUM: Multiple Risk Factors'
    END as ALERT_REASON,
    
    -- Priority scoring
    CASE 
        WHEN FRAUD_REPORTED = TRUE AND TOTAL_RISK_SCORE >= 100 THEN 1
        WHEN FRAUD_REPORTED = TRUE THEN 2
        WHEN TOTAL_RISK_SCORE >= 100 THEN 3
        WHEN CLAIM_AMOUNT >= 100000 THEN 4
        ELSE 5
    END as ALERT_PRIORITY,
    
    LAST_UPDATED
    
FROM ANALYTICS.RISK_SCORE_MATRIX
WHERE RISK_CATEGORY = 'HIGH RISK' 
   OR FRAUD_REPORTED = TRUE 
   OR CLAIM_AMOUNT >= 75000
ORDER BY ALERT_PRIORITY, TOTAL_RISK_SCORE DESC;

-- Pipeline performance summary
CREATE OR REPLACE VIEW ANALYTICS.PIPELINE_PERFORMANCE_SUMMARY AS
SELECT 
    'REAL-TIME ANALYTICS STATUS' as METRIC_TYPE,
    COUNT(DISTINCT r.POLICY_NUMBER) as POLICIES_ANALYZED,
    COUNT(*) as TOTAL_RISK_ASSESSMENTS,
    ROUND(AVG(r.TOTAL_RISK_SCORE), 2) as AVG_RISK_SCORE,
    
    -- Risk distribution
    COUNT(CASE WHEN r.RISK_CATEGORY = 'HIGH RISK' THEN 1 END) as HIGH_RISK_CASES,
    COUNT(CASE WHEN r.RISK_CATEGORY = 'MEDIUM RISK' THEN 1 END) as MEDIUM_RISK_CASES,
    COUNT(CASE WHEN r.RISK_CATEGORY = 'LOW RISK' THEN 1 END) as LOW_RISK_CASES,
    
    -- Financial metrics
    ROUND(SUM(r.CLAIM_AMOUNT), 2) as TOTAL_CLAIM_VALUE,
    ROUND(AVG(r.CLAIM_AMOUNT), 2) as AVG_CLAIM_VALUE,
    
    -- Fraud detection
    COUNT(CASE WHEN r.FRAUD_REPORTED = TRUE THEN 1 END) as FRAUD_CASES_DETECTED,
    ROUND((COUNT(CASE WHEN r.FRAUD_REPORTED = TRUE THEN 1 END) * 100.0) / COUNT(*), 2) as FRAUD_DETECTION_RATE,
    
    -- Data freshness
    MAX(r.LAST_UPDATED) as LAST_ANALYTICS_UPDATE,
    DATEDIFF('MINUTE', MAX(r.LAST_UPDATED), CURRENT_TIMESTAMP()) as MINUTES_SINCE_UPDATE,
    
    -- Pipeline health indicator
    CASE 
        WHEN DATEDIFF('MINUTE', MAX(r.LAST_UPDATED), CURRENT_TIMESTAMP()) <= 5 THEN 'EXCELLENT'
        WHEN DATEDIFF('MINUTE', MAX(r.LAST_UPDATED), CURRENT_TIMESTAMP()) <= 15 THEN 'GOOD'
        WHEN DATEDIFF('MINUTE', MAX(r.LAST_UPDATED), CURRENT_TIMESTAMP()) <= 60 THEN 'ACCEPTABLE'
        ELSE 'NEEDS ATTENTION'
    END as PIPELINE_HEALTH_STATUS
    
FROM ANALYTICS.RISK_SCORE_MATRIX r;

/*
================================================================================
ANALYTICAL QUERIES FOR INSIGHTS
================================================================================
*/

-- Top risk factors analysis
SELECT 
    'RISK FACTOR ANALYSIS' as ANALYSIS_TYPE,
    'Current top risk contributors' as DESCRIPTION;

SELECT 
    INCIDENT_TYPE,
    INCIDENT_SEVERITY,
    COUNT(*) as CASE_COUNT,
    ROUND(AVG(TOTAL_RISK_SCORE), 2) as AVG_RISK_SCORE,
    ROUND(AVG(CLAIM_AMOUNT), 2) as AVG_CLAIM_AMOUNT,
    COUNT(CASE WHEN FRAUD_REPORTED = TRUE THEN 1 END) as FRAUD_CASES
FROM ANALYTICS.RISK_SCORE_MATRIX
GROUP BY INCIDENT_TYPE, INCIDENT_SEVERITY
ORDER BY AVG_RISK_SCORE DESC, AVG_CLAIM_AMOUNT DESC;

-- Real-time claims value analysis by risk category
SELECT 
    'CLAIMS VALUE DISTRIBUTION' as ANALYSIS_TYPE,
    RISK_CATEGORY,
    COUNT(*) as CLAIM_COUNT,
    ROUND(SUM(CLAIM_AMOUNT), 2) as TOTAL_VALUE,
    ROUND(AVG(CLAIM_AMOUNT), 2) as AVG_VALUE,
    ROUND(MIN(CLAIM_AMOUNT), 2) as MIN_VALUE,
    ROUND(MAX(CLAIM_AMOUNT), 2) as MAX_VALUE,
    ROUND(MEDIAN(CLAIM_AMOUNT), 2) as MEDIAN_VALUE
FROM ANALYTICS.RISK_SCORE_MATRIX
GROUP BY RISK_CATEGORY
ORDER BY TOTAL_VALUE DESC;

-- Fraud correlation analysis
SELECT 
    'FRAUD CORRELATION ANALYSIS' as ANALYSIS_TYPE,
    FRAUD_REPORTED,
    RISK_CATEGORY,
    COUNT(*) as CASES,
    ROUND(AVG(TOTAL_RISK_SCORE), 2) as AVG_RISK_SCORE,
    ROUND(SUM(CLAIM_AMOUNT), 2) as TOTAL_EXPOSURE,
    ROUND(AVG(AGE), 2) as AVG_CUSTOMER_AGE
FROM ANALYTICS.RISK_SCORE_MATRIX
GROUP BY FRAUD_REPORTED, RISK_CATEGORY
ORDER BY FRAUD_REPORTED DESC, AVG_RISK_SCORE DESC;

-- Pipeline latency check
SELECT 
    'DYNAMIC TABLE REFRESH STATUS' as STATUS_TYPE,
    CURRENT_TIMESTAMP() as CHECK_TIME,
    DATEDIFF('MINUTE', MAX(LAST_UPDATED), CURRENT_TIMESTAMP()) as MINUTES_SINCE_LAST_UPDATE,
    COUNT(DISTINCT POLICY_NUMBER) as ACTIVE_POLICIES,
    COUNT(*) as ACTIVE_RISK_ASSESSMENTS
FROM ANALYTICS.RISK_SCORE_MATRIX;

/*
================================================================================
RISK ANALYTICS COMPLETE - DYNAMIC TABLES ACTIVE
================================================================================
Automated Analytics Ready:
  • Dynamic Tables refreshing every 1-2 minutes
  • Real-time risk scoring for all claims
  • Automated fraud detection analysis
  • Demographic risk profiling with live updates
  • High-risk alerts and monitoring views

Analytics Capabilities:
  • Composite risk scoring algorithm
  • Real-time fraud correlation analysis
  • Demographic risk segmentation
  • Automated alert generation
  • Pipeline performance monitoring

Next Steps:
  1. Execute 03_GOVERNANCE_DEMO.sql for progressive security
  2. Load additional data to see Dynamic Tables refresh
  3. Monitor automated risk assessments

Ready for: Real-time risk analytics with automated refresh
================================================================================
*/ 