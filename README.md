# MountainPeak Insurance - Automated Data Pipeline & Progressive Governance Demo

## Overview

This demo showcases Snowflake's end-to-end capabilities for building automated data pipelines, real-time quality monitoring, risk analytics with Python UDFs, and progressive data sharing with governance controls. The scenario demonstrates how **MountainPeak Insurance** creates an automated risk analytics pipeline and shares curated risk scores with **Alpine Risk Brokers** while progressively protecting sensitive information.

## Demo Flow

**Key Message**: "Automate first, analyze intelligently, then share securely"

### 1. Automated Pipeline Setup (10 minutes) - `00_AUTOMATED_PIPELINE_SETUP.sql`
- Complete environment creation (database, schemas, warehouses, roles)
- Git integration for automated CSV data loading
- Snowpipe setup for continuous data ingestion
- Role-based access control configuration
- Initial data loading (1,202 customers + 1,002 claims)

### 2. Data Quality Monitoring (8 minutes) - `01_DATA_QUALITY.sql` + Dashboard
- **Data Metric Functions (DMFs)**: Custom and system DMFs for automated quality monitoring
- **Real-time Dashboard**: Streamlit dashboard showing quality metrics and pipeline health
- **Automated Alerts**: Continuous monitoring with quality scoring and trend analysis
- **Business Rules**: Custom validation for claim amounts and customer age ranges

### 3. Risk Analytics & Progressive Governance (15 minutes) - `02_RISK_ANALYTICS.sql`
- **Level 1 Dynamic Table**: Foundational customer-claims data integration
- **Level 2 Dynamic Table**: Risk scoring with Python UDF for age categorization
- **Progressive Data Masking**: Claim amounts protected but analytically useful
- **Row Access Policies**: Geographic territory restrictions for brokers
- **Secure Data Sharing**: Curated risk score views for Alpine Risk Brokers
- **Real-time Updates**: Automated refresh every 1-2 minutes

## Files

1. **`00_AUTOMATED_PIPELINE_SETUP.sql`** - Complete automated pipeline foundation
2. **`01_DATA_QUALITY.sql`** - DMF setup for quality monitoring
3. **`01_DATA_QUALITY_DASHBOARD.py`** - Streamlit dashboard for real-time monitoring
4. **`02_RISK_ANALYTICS.sql`** - Dynamic Tables, Python UDF, governance, and sharing
5. **`02_RISK_ANALYTICS_DASHBOARD.py`** - Risk analytics visualization with Snowflake branding
6. **`REFERENCE_01_INSURANCE_DEMO.sql`** - Reference implementation for governance patterns
7. **`sample_data/`** - CSV files for pipeline demonstration
   - `CLAIMS_DATA.csv` - Initial claims data (1,002 records)
   - `CUSTOMER_DATA.csv` - Initial customer data (1,202 records)
   - `CLAIMS_DATA_BATCH2.csv` - Additional claims for pipeline testing
   - `CUSTOMER_DATA_BATCH2.csv` - Additional customers for pipeline testing
   - `CLAIMS_DATA_BATCH3.csv` - Third batch for continuous loading demo

## Architecture

```
MOUNTAINPEAK_INSURANCE_PIPELINE_DB
├── RAW_DATA (Schema)
│   ├── CLAIMS_RAW (automated loading + DMF monitoring)
│   ├── CUSTOMER_RAW (automated loading + DMF monitoring)
│   └── Custom/System DMFs (quality validation)
├── ANALYTICS (Schema)
│   ├── CUSTOMER_CLAIMS_INTEGRATED (Level 1 Dynamic Table)
│   ├── RISK_SCORE_MATRIX (Level 2 Dynamic Table + Python UDF)
│   └── BROKER_RISK_VIEW (Secure curated view for sharing)
├── GOVERNANCE (Schema)
│   ├── MASK_CLAIM_AMOUNT (dynamic masking policy)
│   ├── ALPINE_BROKER_ACCESS (row access policy)
│   └── Data sharing configurations
├── VISUALIZATION LAYER
│   ├── Data Quality Dashboard (Streamlit + DMF monitoring)
│   └── Risk Analytics Dashboard (Streamlit + Snowflake branding)
└── Git Integration + Snowpipe (Automated pipeline)
```

## Key Demo Points

### Automated Pipeline Excellence
- **Git Integration**: Direct connection to repository for data source management
- **Snowpipe**: Automatic ingestion on file arrival
- **Dynamic Tables**: Real-time transformations with minimal latency
- **Python UDFs**: Advanced analytics with familiar programming languages

### Quality Assurance at Scale
- **Data Metric Functions**: Both custom business rules and system-provided validations
- **Real-time Monitoring**: Live dashboard showing pipeline health and data quality
- **Automated Alerting**: Proactive issue detection with trend analysis

### Intelligent Risk Analytics
- **Two-Tier Dynamic Tables**: Simplified, maintainable transformation layers
- **Python Integration**: Age categorization using Python UDF for advanced logic
- **Real-time Scoring**: Continuous risk assessment with automated refresh

### Progressive Data Governance
- **Dynamic Masking**: Claim amounts floored to $10K increments for partner access
- **Row Access Control**: Geographic restrictions ensuring brokers see only relevant territories
- **Secure Sharing**: Curated views providing business value while protecting sensitive data
- **Account-Aware Policies**: Different access levels based on user roles and external accounts

## Technical Features Demonstrated

- **Git Integration**: Repository-based data source management
- **Snowpipe**: Event-driven data loading
- **Dynamic Tables**: Automated materialized view management
- **Data Metric Functions (DMFs)**: Custom and system quality validations
- **Python UDFs**: Advanced analytics in familiar programming language
- **Progressive Governance**: Layered security without breaking functionality
- **Secure Data Sharing**: Cross-account data distribution
- **Role-Based Access Control**: Granular permission management
- **Real-time Analytics**: Sub-minute data freshness

## Business Value Delivered

### For MountainPeak Insurance
- **Automated Operations**: Reduced manual data pipeline management
- **Quality Assurance**: Proactive data quality monitoring and alerting
- **Advanced Analytics**: Python-powered risk scoring with real-time updates
- **Secure Partnerships**: Controlled data sharing maintaining competitive advantage

### For Alpine Risk Brokers
- **Risk Intelligence**: Access to sophisticated risk scoring algorithms
- **Geographic Focus**: Territory-appropriate data access
- **Analytical Capability**: Masked data that preserves analytical utility
- **Real-time Insights**: Current risk assessments for portfolio management

## Success Metrics

- ✅ Automated pipeline loading 1,200+ customers and 1,000+ claims
- ✅ 6 Data Metric Functions monitoring quality (2 custom + 4 system)
- ✅ Real-time Streamlit dashboards with quality scoring and risk analytics
- ✅ 2-level Dynamic Table architecture refreshing every 1-2 minutes
- ✅ Python UDF for advanced age categorization logic
- ✅ Progressive governance with masking and row access policies
- ✅ Secure broker access with curated risk intelligence
- ✅ Cross-role validation demonstrating different access levels
- ✅ Professional Snowflake-branded visualizations for risk insights

---

**Duration**: 35 minutes total (10 min setup + 8 min quality + 15 min analytics/governance + 2 min demo)
**Audience**: Data engineering and analytics teams evaluating automated pipeline capabilities
**Complexity**: Intermediate to Advanced (automated pipelines + governance) 