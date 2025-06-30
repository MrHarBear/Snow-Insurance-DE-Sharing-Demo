import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from datetime import datetime, timedelta
import time

# -----------------------------------------------------------------------------
# MOUNTAINPEAK INSURANCE - DATA QUALITY MONITORING DASHBOARD
# -----------------------------------------------------------------------------
# Purpose: Real-time data quality monitoring for automated insurance pipeline
# Features: Live quality metrics, trend analysis, pipeline health monitoring
# -----------------------------------------------------------------------------

# Page configuration
st.set_page_config(
    page_title="MountainPeak Insurance - Data Quality Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f0f8ff;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #29B5E8;
        margin: 10px 0;
    }
    .quality-excellent { color: #28a745; font-weight: bold; }
    .quality-good { color: #17a2b8; font-weight: bold; }
    .quality-fair { color: #ffc107; font-weight: bold; }
    .quality-poor { color: #dc3545; font-weight: bold; }
    .header-style {
        color: #11567F;
        font-size: 24px;
        font-weight: bold;
        padding: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# SNOWFLAKE CONNECTION & DATA FUNCTIONS
# -----------------------------------------------------------------------------

@st.cache_data(ttl=60)  # Cache for 60 seconds for real-time feel
def get_connection():
    """Create Snowflake connection using Streamlit secrets"""
    try:
        conn = snowflake.connector.connect(
            **st.secrets["snowflake"]
        )
        return conn
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

@st.cache_data(ttl=30)  # Refresh every 30 seconds for pipeline monitoring
def get_dmf_results():
    """Fetch latest DMF monitoring results"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
    USE SCHEMA RAW_DATA;
    
    SELECT 
        change_commit_time,
        measurement_time,
        table_database,
        table_schema,
        table_name,
        metric_name,
        value,
        CASE 
            WHEN metric_name LIKE '%NULL_COUNT' AND value = 0 THEN 'EXCELLENT'
            WHEN metric_name LIKE '%NULL_COUNT' AND value <= 5 THEN 'GOOD'
            WHEN metric_name LIKE '%NULL_COUNT' AND value <= 20 THEN 'FAIR'
            WHEN metric_name LIKE '%NULL_COUNT' THEN 'NEEDS ATTENTION'
            WHEN metric_name LIKE '%DUPLICATE_COUNT' AND value = 0 THEN 'EXCELLENT'
            WHEN metric_name LIKE '%DUPLICATE_COUNT' AND value <= 2 THEN 'GOOD'
            WHEN metric_name LIKE '%DUPLICATE_COUNT' THEN 'NEEDS ATTENTION'
            WHEN metric_name LIKE 'INVALID_%' AND value = 0 THEN 'EXCELLENT'
            WHEN metric_name LIKE 'INVALID_%' AND value <= 3 THEN 'GOOD'
            WHEN metric_name LIKE 'INVALID_%' THEN 'NEEDS ATTENTION'
            ELSE 'MONITORING'
        END as quality_status,
        CASE
            WHEN metric_name LIKE '%NULL_COUNT' THEN 'Completeness'
            WHEN metric_name LIKE '%DUPLICATE_COUNT' THEN 'Uniqueness'
            WHEN metric_name LIKE 'INVALID_%' THEN 'Validity'
            WHEN metric_name = 'ROW_COUNT' THEN 'Volume'
            ELSE 'Other'
        END as check_type
    FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
    WHERE table_database = 'MOUNTAINPEAK_INSURANCE_PIPELINE_DB'
        AND table_schema = 'RAW_DATA'
        AND table_name IN ('CLAIMS_RAW', 'CUSTOMER_RAW')
    ORDER BY change_commit_time DESC, table_name, metric_name;
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching DMF results: {str(e)}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=30)
def get_pipeline_status():
    """Fetch pipeline health and processing status"""
    conn = get_connection()
    if not conn:
        return {}
    
    queries = {
        'pipeline_summary': """
            SELECT 
                COUNT(DISTINCT POLICY_NUMBER) as UNIQUE_POLICIES,
                SUM(CASE WHEN FRAUD_REPORTED = TRUE THEN 1 ELSE 0 END) as FRAUD_CASES,
                ROUND(AVG(CLAIM_AMOUNT), 2) as AVG_CLAIM_AMOUNT,
                MAX(LOAD_TIMESTAMP) as LAST_PIPELINE_RUN,
                COUNT(DISTINCT FILE_NAME) as FILES_PROCESSED
            FROM CLAIMS_RAW;
        """,
        'data_freshness': """
            SELECT 
                DATEDIFF('MINUTE', MAX(LOAD_TIMESTAMP), CURRENT_TIMESTAMP()) as MINUTES_SINCE_LOAD,
                CASE 
                    WHEN DATEDIFF('MINUTE', MAX(LOAD_TIMESTAMP), CURRENT_TIMESTAMP()) <= 30 THEN 'FRESH'
                    WHEN DATEDIFF('MINUTE', MAX(LOAD_TIMESTAMP), CURRENT_TIMESTAMP()) <= 120 THEN 'ACCEPTABLE'
                    ELSE 'STALE'
                END as FRESHNESS_STATUS,
                MAX(LOAD_TIMESTAMP) as LAST_LOAD_TIME
            FROM CLAIMS_RAW;
        """
    }
    
    results = {}
    try:
        for key, query in queries.items():
            results[key] = pd.read_sql(f"USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB; USE SCHEMA RAW_DATA; {query}", conn)
        conn.close()
        return results
    except Exception as e:
        st.error(f"Error fetching pipeline status: {str(e)}")
        conn.close()
        return {}

@st.cache_data(ttl=60)
def get_dmf_active_functions():
    """Fetch active DMF functions for monitoring"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
    USE SCHEMA RAW_DATA;
    
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
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching DMF functions: {str(e)}")
        conn.close()
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# DASHBOARD HEADER
# -----------------------------------------------------------------------------

st.markdown('<p class="header-style">🏔️ MountainPeak Insurance - Data Quality Monitor</p>', unsafe_allow_html=True)
st.markdown("**Real-time monitoring of automated insurance data pipeline quality**")

# Auto-refresh controls
col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=True)
with col2:
    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()
with col3:
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%H:%M:%S')}")

# Auto-refresh logic
if auto_refresh:
    time.sleep(30)
    st.rerun()

# -----------------------------------------------------------------------------
# PIPELINE STATUS OVERVIEW
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown("## 📈 Pipeline Status Overview")

# Fetch pipeline data
pipeline_data = get_pipeline_status()
dmf_data = get_dmf_results()

# Calculate overall DMF quality status
overall_score = 0
quality_rating = "NO DATA"
if not dmf_data.empty:
    excellent_count = len(dmf_data[dmf_data['quality_status'] == 'EXCELLENT'])
    good_count = len(dmf_data[dmf_data['quality_status'] == 'GOOD'])
    total_checks = len(dmf_data)
    
    if total_checks > 0:
        overall_score = ((excellent_count * 100) + (good_count * 85)) / total_checks
        if overall_score >= 95:
            quality_rating = "EXCELLENT"
        elif overall_score >= 85:
            quality_rating = "GOOD"
        elif overall_score >= 70:
            quality_rating = "FAIR"
        else:
            quality_rating = "NEEDS ATTENTION"

if pipeline_data:
    pipeline_summary = pipeline_data.get('pipeline_summary', pd.DataFrame()) 
    freshness = pipeline_data.get('data_freshness', pd.DataFrame())
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        color_class = f"quality-{quality_rating.lower().replace(' ', '-')}"
        st.markdown(f"""
        <div class="metric-card">
            <h4>DMF Quality Score</h4>
            <h2 class="{color_class}">{overall_score:.1f}%</h2>
            <p>Status: {quality_rating}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        if not pipeline_summary.empty:
            policies = pipeline_summary.iloc[0]['UNIQUE_POLICIES']
            fraud_cases = pipeline_summary.iloc[0]['FRAUD_CASES']
            st.markdown(f"""
            <div class="metric-card">
                <h4>Policies Processed</h4>
                <h2 style="color: #11567F;">{policies:,}</h2>
                <p>Fraud Cases: {fraud_cases}</p>
            </div>
            """, unsafe_allow_html=True)
    
    with col3:
        if not pipeline_summary.empty:
            avg_claim = pipeline_summary.iloc[0]['AVG_CLAIM_AMOUNT']
            files = pipeline_summary.iloc[0]['FILES_PROCESSED']
            st.markdown(f"""
            <div class="metric-card">
                <h4>Avg Claim Amount</h4>
                <h2 style="color: #11567F;">${avg_claim:,.2f}</h2>
                <p>Files: {files}</p>
            </div>
            """, unsafe_allow_html=True)
    
    with col4:
        if not freshness.empty:
            minutes = freshness.iloc[0]['MINUTES_SINCE_LOAD']
            status = freshness.iloc[0]['FRESHNESS_STATUS']
            status_color = {"FRESH": "#28a745", "ACCEPTABLE": "#ffc107", "STALE": "#dc3545"}
            st.markdown(f"""
            <div class="metric-card">
                <h4>Data Freshness</h4>
                <h2 style="color: {status_color.get(status, '#11567F')};">{minutes} min</h2>
                <p>Status: {status}</p>
            </div>
            """, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# QUALITY METRICS DETAIL
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown("## 🔍 DMF Quality Metrics Detail")

if not dmf_data.empty:
    # Quality summary by table and check type
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Quality Status by Data Source")
        table_summary = dmf_data.groupby('table_name').apply(lambda x: pd.Series({
            'total_checks': len(x),
            'excellent': len(x[x['quality_status'] == 'EXCELLENT']),
            'good': len(x[x['quality_status'] == 'GOOD']),
            'needs_attention': len(x[x['quality_status'] == 'NEEDS ATTENTION'])
        })).reset_index()
        
        # Create stacked bar chart
        fig_bar = px.bar(
            table_summary,
            x='table_name',
            y=['excellent', 'good', 'needs_attention'],
            title="Quality Status Distribution by Table",
            labels={'table_name': 'Table Name', 'value': 'Count of Checks'},
            color_discrete_map={'excellent': '#28a745', 'good': '#17a2b8', 'needs_attention': '#dc3545'}
        )
        fig_bar.update_layout(height=300)
        st.plotly_chart(fig_bar, use_container_width=True)
    
    with col2:
        st.markdown("### DMF Check Type Distribution")
        check_summary = dmf_data['check_type'].value_counts().reset_index()
        
        # Create donut chart for check types
        fig_donut = px.pie(
            check_summary,
            values='count',
            names='check_type',
            hole=0.4,
            title="DMF Check Type Distribution"
        )
        fig_donut.update_layout(height=300)
        st.plotly_chart(fig_donut, use_container_width=True)

    # Detailed DMF metrics table
    st.markdown("### Detailed DMF Results")
    
    # Add filters
    col1, col2, col3 = st.columns(3)
    with col1:
        table_filter = st.selectbox("Filter by Table", ["All"] + list(dmf_data['table_name'].unique()))
    with col2:
        check_filter = st.selectbox("Filter by Check Type", ["All"] + list(dmf_data['check_type'].unique()))
    with col3:
        status_filter = st.selectbox("Filter by Status", ["All"] + list(dmf_data['quality_status'].unique()))
    
    # Apply filters
    filtered_df = dmf_data.copy()
    if table_filter != "All":
        filtered_df = filtered_df[filtered_df['table_name'] == table_filter]
    if check_filter != "All":
        filtered_df = filtered_df[filtered_df['check_type'] == check_filter]
    if status_filter != "All":
        filtered_df = filtered_df[filtered_df['quality_status'] == status_filter]
    
    # Format and display table
    display_df = filtered_df[['metric_name', 'table_name', 'check_type', 'value', 
                             'quality_status', 'measurement_time']].copy()
    display_df.columns = ['DMF Function', 'Table', 'Check Type', 'Value', 'Status', 'Measurement Time']
    
    # Color coding for quality status
    def color_quality_status(val):
        if val == 'EXCELLENT':
            return 'background-color: #d4edda; color: #155724'  # Green
        elif val == 'GOOD':
            return 'background-color: #d1ecf1; color: #0c5460'  # Blue
        elif val == 'NEEDS ATTENTION':
            return 'background-color: #f8d7da; color: #721c24'  # Red
        else:
            return 'background-color: #fff3cd; color: #856404'  # Yellow
    
    styled_df = display_df.style.applymap(color_quality_status, subset=['Status'])
    st.dataframe(styled_df, use_container_width=True)

# -----------------------------------------------------------------------------
# DMF FUNCTIONS AND ANALYSIS
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown("## 📊 DMF Functions & Analysis")

dmf_functions = get_dmf_active_functions()

if not dmf_functions.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Active DMF Functions")
        # Display active functions with their schedules
        for _, row in dmf_functions.iterrows():
            table_name = row['ref_entity_name'].split('.')[-1]
            function_name = row['metric_name']
            schedule_status = row['schedule_status']
            
            status_color = "#28a745" if schedule_status == "STARTED" else "#dc3545"
            st.markdown(f"""
            <div style="padding: 10px; margin: 5px 0; border-left: 4px solid {status_color}; background-color: #f8f9fa;">
                <strong>{function_name}</strong><br>
                Table: {table_name}<br>
                Status: <span style="color: {status_color};">{schedule_status}</span>
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("### DMF Value Trends")
        if not dmf_data.empty:
            # Show recent DMF values over time
            fig_trends = px.line(
                dmf_data,
                x='measurement_time',
                y='value',
                color='metric_name',
                facet_col='table_name',
                title="DMF Values Over Time",
                labels={'measurement_time': 'Time', 'value': 'DMF Value'}
            )
            fig_trends.update_layout(height=400)
            st.plotly_chart(fig_trends, use_container_width=True)

# Quality Analysis Summary
if not dmf_data.empty:
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Quality Status Summary")
        status_summary = dmf_data['quality_status'].value_counts()
        fig_status = px.bar(
            x=status_summary.index,
            y=status_summary.values,
            color=status_summary.index,
            color_discrete_map={'EXCELLENT': '#28a745', 'GOOD': '#17a2b8', 
                               'NEEDS ATTENTION': '#dc3545', 'MONITORING': '#ffc107'},
            title="Current Quality Status Distribution"
        )
        fig_status.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_status, use_container_width=True)
    
    with col2:
        st.markdown("### DMF Values Distribution")
        fig_values = px.histogram(
            dmf_data,
            x='value',
            color='check_type',
            title="DMF Values Distribution by Check Type",
            nbins=15
        )
        fig_values.update_layout(height=300)
        st.plotly_chart(fig_values, use_container_width=True)

# -----------------------------------------------------------------------------
# ALERTS AND RECOMMENDATIONS
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown("## ⚠️ DMF Quality Alerts & Recommendations")

if not dmf_data.empty:
    # Identify issues requiring attention
    issues = dmf_data[dmf_data['quality_status'] == 'NEEDS ATTENTION']
    
    if not issues.empty:
        st.markdown("### Issues Requiring Attention")
        
        for _, row in issues.iterrows():
            severity = "🔴 Critical" if row['value'] > 10 else "🟡 Warning"
            function_desc = {
                'INVALID_CLAIM_AMOUNT_COUNT': 'Claims with invalid amounts (outside $100-$500K range)',
                'INVALID_CUSTOMER_AGE_COUNT': 'Customers with invalid ages (outside 18-100 range)',
                'NULL_COUNT': f'Records with missing {row["metric_name"].split(".")[-1]} values',
                'DUPLICATE_COUNT': f'Duplicate records found in {row["metric_name"].split(".")[-1]}'
            }
            
            desc = function_desc.get(row['metric_name'].split('.')[-1], f'Quality issue in {row["metric_name"]}')
            
            st.markdown(f"""
            **{severity}**: {desc}
            - Table: {row['table_name']} | DMF: {row['metric_name']}
            - Issue Count: {row['value']} | Status: {row['quality_status']}
            - Last Measured: {row['measurement_time']}
            """)
    else:
        st.success("✅ All DMF quality checks are excellent! No issues requiring immediate attention.")

# -----------------------------------------------------------------------------
# PIPELINE HEALTH RECOMMENDATIONS
# -----------------------------------------------------------------------------

st.markdown("### 🔧 Pipeline Health Recommendations")

if pipeline_data:
    recommendations = []
    
    # Check DMF quality status
    if not dmf_data.empty:
        needs_attention_count = len(dmf_data[dmf_data['quality_status'] == 'NEEDS ATTENTION'])
        if needs_attention_count > 0:
            recommendations.append(f"🔍 {needs_attention_count} DMF checks need attention - investigate data quality issues")
    
    # Check data freshness
    if not freshness.empty and freshness.iloc[0]['FRESHNESS_STATUS'] == 'STALE':
        recommendations.append("⏰ Data pipeline may need attention - last update was over 2 hours ago")
    
    # Check DMF function status
    if not dmf_functions.empty:
        stopped_functions = dmf_functions[dmf_functions['schedule_status'] != 'STARTED']
        if not stopped_functions.empty:
            recommendations.append(f"⚙️ {len(stopped_functions)} DMF functions are not running - check monitoring schedule")
    
    # Overall health check
    if overall_score < 85:
        recommendations.append("📊 Overall DMF quality score is below optimal - review data ingestion processes")
    
    if len(recommendations) == 0:
        recommendations.append("✅ Pipeline is running optimally with excellent DMF monitoring - continue monitoring")
    
    for rec in recommendations:
        st.markdown(f"- {rec}")

# -----------------------------------------------------------------------------
# FOOTER
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #5B5B5B; padding: 20px;'>
    <p><strong>MountainPeak Insurance - DMF Quality Monitor</strong></p>
    <p>Powered by Snowflake DMFs • Real-time Pipeline Monitoring • Automated Quality Assurance</p>
    <p>Monitoring: 6 Data Metric Functions (2 Custom + 4 System) across Claims & Customer tables</p>
</div>
""", unsafe_allow_html=True) 