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
def get_quality_metrics():
    """Fetch latest data quality metrics"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
    USE SCHEMA RAW_DATA;
    
    SELECT 
        CHECK_ID,
        TABLE_NAME,
        CHECK_TYPE,
        CHECK_DESCRIPTION,
        RECORD_COUNT,
        FAILED_COUNT,
        PASS_RATE,
        QUALITY_SCORE,
        CHECK_TIMESTAMP
    FROM DATA_QUALITY_METRICS
    ORDER BY CHECK_TIMESTAMP DESC;
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching quality metrics: {str(e)}")
        conn.close()
        return pd.DataFrame()

@st.cache_data(ttl=30)
def get_pipeline_status():
    """Fetch pipeline health and processing status"""
    conn = get_connection()
    if not conn:
        return {}
    
    queries = {
        'overall_status': """
            SELECT 
                ROUND(AVG(QUALITY_SCORE), 2) as OVERALL_SCORE,
                CASE 
                    WHEN AVG(QUALITY_SCORE) >= 95 THEN 'EXCELLENT'
                    WHEN AVG(QUALITY_SCORE) >= 90 THEN 'GOOD' 
                    WHEN AVG(QUALITY_SCORE) >= 80 THEN 'FAIR'
                    ELSE 'NEEDS ATTENTION'
                END as QUALITY_RATING,
                COUNT(*) as TOTAL_CHECKS,
                SUM(FAILED_COUNT) as TOTAL_FAILURES
            FROM DATA_QUALITY_METRICS;
        """,
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
def get_quality_trends():
    """Fetch quality score trends for visualization"""
    conn = get_connection()
    if not conn:
        return pd.DataFrame()
    
    query = """
    USE DATABASE MOUNTAINPEAK_INSURANCE_PIPELINE_DB;
    USE SCHEMA RAW_DATA;
    
    SELECT 
        CHECK_TYPE,
        TABLE_NAME,
        QUALITY_SCORE,
        CHECK_TIMESTAMP,
        FAILED_COUNT,
        RECORD_COUNT
    FROM DATA_QUALITY_METRICS
    ORDER BY CHECK_TIMESTAMP;
    """
    
    try:
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error fetching quality trends: {str(e)}")
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

if pipeline_data:
    # Overall quality status
    overall = pipeline_data.get('overall_status', pd.DataFrame())
    pipeline_summary = pipeline_data.get('pipeline_summary', pd.DataFrame()) 
    freshness = pipeline_data.get('data_freshness', pd.DataFrame())
    
    if not overall.empty:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            score = overall.iloc[0]['OVERALL_SCORE']
            rating = overall.iloc[0]['QUALITY_RATING']
            color_class = f"quality-{rating.lower().replace(' ', '-')}"
            st.markdown(f"""
            <div class="metric-card">
                <h4>Overall Quality Score</h4>
                <h2 class="{color_class}">{score}%</h2>
                <p>Status: {rating}</p>
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
st.markdown("## 🔍 Quality Metrics Detail")

# Fetch quality metrics
quality_df = get_quality_metrics()

if not quality_df.empty:
    # Quality summary by table and check type
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Quality Score by Data Source")
        table_summary = quality_df.groupby('TABLE_NAME').agg({
            'QUALITY_SCORE': 'mean',
            'FAILED_COUNT': 'sum',
            'RECORD_COUNT': 'sum'
        }).round(2)
        
        # Create bar chart for quality scores by table
        fig_bar = px.bar(
            x=table_summary.index,
            y=table_summary['QUALITY_SCORE'],
            color=table_summary['QUALITY_SCORE'],
            color_continuous_scale='RdYlGn',
            title="Average Quality Score by Table",
            labels={'x': 'Table Name', 'y': 'Quality Score (%)'}
        )
        fig_bar.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)
    
    with col2:
        st.markdown("### Quality Check Distribution")
        check_summary = quality_df.groupby('CHECK_TYPE')['QUALITY_SCORE'].mean().reset_index()
        
        # Create donut chart for check types
        fig_donut = px.pie(
            check_summary,
            values='QUALITY_SCORE',
            names='CHECK_TYPE',
            hole=0.4,
            title="Quality Distribution by Check Type"
        )
        fig_donut.update_layout(height=300)
        st.plotly_chart(fig_donut, use_container_width=True)

    # Detailed quality metrics table
    st.markdown("### Detailed Quality Metrics")
    
    # Add filters
    col1, col2, col3 = st.columns(3)
    with col1:
        table_filter = st.selectbox("Filter by Table", ["All"] + list(quality_df['TABLE_NAME'].unique()))
    with col2:
        check_filter = st.selectbox("Filter by Check Type", ["All"] + list(quality_df['CHECK_TYPE'].unique()))
    with col3:
        score_filter = st.slider("Min Quality Score", 0, 100, 0)
    
    # Apply filters
    filtered_df = quality_df.copy()
    if table_filter != "All":
        filtered_df = filtered_df[filtered_df['TABLE_NAME'] == table_filter]
    if check_filter != "All":
        filtered_df = filtered_df[filtered_df['CHECK_TYPE'] == check_filter]
    filtered_df = filtered_df[filtered_df['QUALITY_SCORE'] >= score_filter]
    
    # Format and display table
    display_df = filtered_df[['CHECK_ID', 'TABLE_NAME', 'CHECK_TYPE', 'CHECK_DESCRIPTION', 
                             'QUALITY_SCORE', 'FAILED_COUNT', 'RECORD_COUNT', 'CHECK_TIMESTAMP']]
    
    # Color coding for quality scores
    def color_quality_score(val):
        if val >= 95:
            return 'background-color: #d4edda; color: #155724'  # Green
        elif val >= 90:
            return 'background-color: #d1ecf1; color: #0c5460'  # Blue
        elif val >= 80:
            return 'background-color: #fff3cd; color: #856404'  # Yellow
        else:
            return 'background-color: #f8d7da; color: #721c24'  # Red
    
    styled_df = display_df.style.applymap(color_quality_score, subset=['QUALITY_SCORE'])
    st.dataframe(styled_df, use_container_width=True)

# -----------------------------------------------------------------------------
# QUALITY TRENDS VISUALIZATION
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown("## 📊 Quality Trends Analysis")

trends_df = get_quality_trends()

if not trends_df.empty:
    # Quality score trends over time
    fig_trends = px.line(
        trends_df,
        x='CHECK_TIMESTAMP',
        y='QUALITY_SCORE',
        color='CHECK_TYPE',
        facet_col='TABLE_NAME',
        title="Quality Score Trends Over Time",
        labels={'CHECK_TIMESTAMP': 'Time', 'QUALITY_SCORE': 'Quality Score (%)'}
    )
    fig_trends.update_layout(height=400)
    st.plotly_chart(fig_trends, use_container_width=True)
    
    # Failure analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Failure Rate Analysis")
        trends_df['FAILURE_RATE'] = (trends_df['FAILED_COUNT'] / trends_df['RECORD_COUNT']) * 100
        
        fig_failures = px.scatter(
            trends_df,
            x='RECORD_COUNT',
            y='FAILURE_RATE',
            color='CHECK_TYPE',
            size='FAILED_COUNT',
            hover_data=['TABLE_NAME', 'CHECK_TIMESTAMP'],
            title="Failure Rate vs Record Count"
        )
        fig_failures.update_layout(height=300)
        st.plotly_chart(fig_failures, use_container_width=True)
    
    with col2:
        st.markdown("### Quality Score Distribution")
        fig_hist = px.histogram(
            trends_df,
            x='QUALITY_SCORE',
            color='TABLE_NAME',
            nbins=20,
            title="Quality Score Distribution",
            labels={'QUALITY_SCORE': 'Quality Score (%)', 'count': 'Frequency'}
        )
        fig_hist.update_layout(height=300)
        st.plotly_chart(fig_hist, use_container_width=True)

# -----------------------------------------------------------------------------
# ALERTS AND RECOMMENDATIONS
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown("## ⚠️ Quality Alerts & Recommendations")

if not quality_df.empty:
    # Identify issues requiring attention
    low_quality = quality_df[quality_df['QUALITY_SCORE'] < 95]
    
    if not low_quality.empty:
        st.markdown("### Issues Requiring Attention")
        
        for _, row in low_quality.iterrows():
            severity = "🔴 Critical" if row['QUALITY_SCORE'] < 80 else "🟡 Warning"
            st.markdown(f"""
            **{severity}**: {row['CHECK_DESCRIPTION']}
            - Table: {row['TABLE_NAME']} | Check: {row['CHECK_TYPE']}
            - Quality Score: {row['QUALITY_SCORE']}% | Failed Records: {row['FAILED_COUNT']}/{row['RECORD_COUNT']}
            - Timestamp: {row['CHECK_TIMESTAMP']}
            """)
    else:
        st.success("✅ All quality checks are passing! No issues requiring immediate attention.")

# -----------------------------------------------------------------------------
# PIPELINE HEALTH RECOMMENDATIONS
# -----------------------------------------------------------------------------

st.markdown("### 🔧 Pipeline Health Recommendations")

if pipeline_data and not pipeline_data.get('overall_status', pd.DataFrame()).empty:
    overall_score = pipeline_data['overall_status'].iloc[0]['OVERALL_SCORE']
    
    recommendations = []
    
    if overall_score < 90:
        recommendations.append("📊 Consider reviewing data ingestion processes for quality improvement")
    
    if not freshness.empty and freshness.iloc[0]['FRESHNESS_STATUS'] == 'STALE':
        recommendations.append("⏰ Data pipeline may need attention - last update was over 2 hours ago")
    
    if not quality_df.empty and quality_df['FAILED_COUNT'].sum() > 0:
        recommendations.append("🔍 Investigate failed quality checks to identify root causes")
    
    if len(recommendations) == 0:
        recommendations.append("✅ Pipeline is running optimally - continue monitoring")
    
    for rec in recommendations:
        st.markdown(f"- {rec}")

# -----------------------------------------------------------------------------
# FOOTER
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #5B5B5B; padding: 20px;'>
    <p><strong>MountainPeak Insurance - Data Quality Monitor</strong></p>
    <p>Powered by Snowflake • Real-time Pipeline Monitoring • Automated Quality Assurance</p>
</div>
""", unsafe_allow_html=True) 