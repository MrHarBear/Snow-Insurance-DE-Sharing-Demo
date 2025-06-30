import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time
from snowflake.snowpark.context import get_active_session

# -----------------------------------------------------------------------------
# MOUNTAINPEAK INSURANCE - RISK ANALYTICS DASHBOARD
# -----------------------------------------------------------------------------
# Purpose: Real-time risk analytics visualization for insurance portfolio
# Features: Risk scoring, geographic analysis, governance validation
# -----------------------------------------------------------------------------

# Page configuration
st.set_page_config(
    page_title="MountainPeak Insurance - Risk Analytics",
    page_icon="🏔️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflake Brand Colors from README
COLORS = {
    'main': '#29B5E8',           # Snowflake Blue
    'midnight': '#000000',       # Titles
    'mid_blue': '#11567F',       # Sections
    'medium_gray': '#5B5B5B',    # Body
    'star_blue': '#75CDD7',      # Accents
    'valencia_orange': '#FF9F36', # Accents
    'first_light': '#D45B90',    # Accents
    'purple_moon': '#7254A3'     # Accents
}

# Custom CSS with Snowflake branding
st.markdown(f"""
<style>
    .main-header {{
        color: {COLORS['midnight']};
        font-size: 28px;
        font-weight: bold;
        text-align: center;
        padding: 20px 0;
        border-bottom: 3px solid {COLORS['main']};
        margin-bottom: 30px;
    }}
    .section-header {{
        color: {COLORS['mid_blue']};
        font-size: 20px;
        font-weight: bold;
        margin: 20px 0 10px 0;
        padding: 10px 0;
        border-left: 4px solid {COLORS['main']};
        padding-left: 15px;
    }}
    .metric-card {{
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 20px;
        border-radius: 10px;
        border-left: 5px solid {COLORS['main']};
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }}
    .risk-high {{ color: {COLORS['first_light']}; font-weight: bold; }}
    .risk-medium {{ color: {COLORS['valencia_orange']}; font-weight: bold; }}
    .risk-low {{ color: {COLORS['star_blue']}; font-weight: bold; }}
    .governance-note {{
        background-color: #f0f8ff;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid {COLORS['purple_moon']};
        margin: 15px 0;
        color: {COLORS['medium_gray']};
    }}
</style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# DATA ACCESS USING SNOWFLAKE SESSION
# -----------------------------------------------------------------------------

# Get the active Snowflake session
session = get_active_session()

@st.cache_data(ttl=30)  # Refresh every 30 seconds for real-time feel
def get_risk_analytics_data():
    """Fetch risk analytics results from Dynamic Tables using Snowflake session"""
    
    try:
        results = {}
        
        # Risk overview query
        results['risk_overview'] = session.sql("""
            SELECT 
                COUNT(*) as TOTAL_CUSTOMERS,
                COUNT(CASE WHEN RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_COUNT,
                COUNT(CASE WHEN RISK_LEVEL = 'MEDIUM' THEN 1 END) as MEDIUM_RISK_COUNT,
                COUNT(CASE WHEN RISK_LEVEL = 'LOW' THEN 1 END) as LOW_RISK_COUNT,
                ROUND(AVG(TOTAL_RISK_SCORE), 1) as AVG_RISK_SCORE,
                ROUND(AVG(CLAIM_AMOUNT_FILLED), 0) as AVG_CLAIM_AMOUNT,
                MAX(LAST_UPDATED) as LAST_REFRESH
            FROM MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX
        """).to_pandas()
        
        # Age category analysis
        results['age_category_analysis'] = session.sql("""
            SELECT 
                AGE_CATEGORY,
                COUNT(*) as CUSTOMER_COUNT,
                ROUND(AVG(AGE_RISK_SCORE), 1) as AVG_AGE_RISK_SCORE,
                ROUND(AVG(TOTAL_RISK_SCORE), 1) as AVG_TOTAL_RISK_SCORE,
                COUNT(CASE WHEN RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_COUNT
            FROM MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX
            GROUP BY AGE_CATEGORY
            ORDER BY AVG_TOTAL_RISK_SCORE DESC
        """).to_pandas()
        
        # Geographic distribution
        results['geographic_distribution'] = session.sql("""
            SELECT 
                CUSTOMER_STATE,
                COUNT(*) as CUSTOMER_COUNT,
                ROUND(AVG(TOTAL_RISK_SCORE), 1) as AVG_RISK_SCORE,
                COUNT(CASE WHEN RISK_LEVEL = 'HIGH' THEN 1 END) as HIGH_RISK_COUNT,
                ROUND(AVG(CLAIM_AMOUNT_FILLED), 0) as AVG_CLAIM_AMOUNT
            FROM MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX
            GROUP BY CUSTOMER_STATE
            ORDER BY CUSTOMER_COUNT DESC
        """).to_pandas()
        
        # Risk factors analysis
        results['risk_factors_analysis'] = session.sql("""
            SELECT 
                RISK_LEVEL,
                COUNT(*) as COUNT,
                ROUND(AVG(TOTAL_RISK_SCORE), 1) as AVG_SCORE,
                ROUND(AVG(CLAIM_AMOUNT_FILLED), 0) as AVG_CLAIM,
                ROUND(AVG(POLICY_ANNUAL_PREMIUM), 0) as AVG_PREMIUM
            FROM MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.RISK_SCORE_MATRIX
            GROUP BY RISK_LEVEL
            ORDER BY AVG_SCORE DESC
        """).to_pandas()
        
        return results
        
    except Exception as e:
        st.error(f"Error fetching analytics data: {str(e)}")
        return {}

@st.cache_data(ttl=60)
def get_governance_validation():
    """Test governance policies using current session context"""
    
    try:
        results = {}
        
        # Current role view (should see governed data based on role)
        results['current_role_view'] = session.sql("""
            SELECT 
                CURRENT_ROLE() as ROLE_NAME,
                COUNT(*) as VISIBLE_RECORDS,
                COUNT(DISTINCT CUSTOMER_STATE) as VISIBLE_STATES,
                ROUND(AVG(CLAIM_AMOUNT_FILLED), 0) as AVG_CLAIM_AMOUNT,
                MAX(CLAIM_AMOUNT_FILLED) as MAX_CLAIM_AMOUNT
            FROM MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.BROKER_RISK_VIEW
        """).to_pandas()
        
        # Sample of actual data visible to current role
        results['sample_data'] = session.sql("""
            SELECT 
                POLICY_NUMBER,
                AGE_CATEGORY,
                CUSTOMER_STATE,
                RISK_LEVEL,
                CLAIM_AMOUNT_FILLED,
                TOTAL_RISK_SCORE
            FROM MOUNTAINPEAK_INSURANCE_PIPELINE_DB.ANALYTICS.BROKER_RISK_VIEW
            LIMIT 10
        """).to_pandas()
        
        return results
        
    except Exception as e:
        st.error(f"Error testing governance: {str(e)}")
        return {}

# -----------------------------------------------------------------------------
# DASHBOARD HEADER
# -----------------------------------------------------------------------------

st.markdown('<div class="main-header">🏔️ MountainPeak Insurance - Risk Analytics Dashboard</div>', 
           unsafe_allow_html=True)

col1, col2, col3 = st.columns([2, 1, 1])
with col1:
    st.markdown("**Real-time risk intelligence powered by Dynamic Tables and Python UDFs**")
with col2:
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=False)
with col3:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Auto-refresh logic
if auto_refresh:
    time.sleep(30)
    st.rerun()

# -----------------------------------------------------------------------------
# FETCH DATA
# -----------------------------------------------------------------------------

analytics_data = get_risk_analytics_data()
governance_data = get_governance_validation()

if not analytics_data:
    st.error("Unable to load risk analytics data. Please check your session context.")
    st.stop()

# -----------------------------------------------------------------------------
# RISK OVERVIEW SECTION
# -----------------------------------------------------------------------------

st.markdown('<div class="section-header">📊 Risk Portfolio Overview</div>', unsafe_allow_html=True)

if 'risk_overview' in analytics_data:
    overview = analytics_data['risk_overview'].iloc[0]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: {COLORS['mid_blue']}; margin: 0;">Total Customers</h3>
            <h1 style="color: {COLORS['midnight']}; margin: 10px 0;">{overview['TOTAL_CUSTOMERS']:,}</h1>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Active Policies</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        high_pct = (overview['HIGH_RISK_COUNT'] / overview['TOTAL_CUSTOMERS'] * 100)
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: {COLORS['mid_blue']}; margin: 0;">High Risk</h3>
            <h1 style="color: {COLORS['first_light']}; margin: 10px 0;">{overview['HIGH_RISK_COUNT']:,}</h1>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">{high_pct:.1f}% of portfolio</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: {COLORS['mid_blue']}; margin: 0;">Avg Risk Score</h3>
            <h1 style="color: {COLORS['valencia_orange']}; margin: 10px 0;">{overview['AVG_RISK_SCORE']}</h1>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Portfolio Average</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <h3 style="color: {COLORS['mid_blue']}; margin: 0;">Avg Claim Amount</h3>
            <h1 style="color: {COLORS['star_blue']}; margin: 10px 0;">${overview['AVG_CLAIM_AMOUNT']:,.0f}</h1>
            <p style="color: {COLORS['medium_gray']}; margin: 0;">Last Updated: {overview['LAST_REFRESH'].strftime('%H:%M:%S')}</p>
        </div>
        """, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# ANALYTICS VISUALIZATIONS
# -----------------------------------------------------------------------------

col1, col2 = st.columns(2)

with col1:
    st.markdown('<div class="section-header">👥 Python UDF Age Analysis</div>', unsafe_allow_html=True)
    
    if 'age_category_analysis' in analytics_data:
        age_data = analytics_data['age_category_analysis']
        
        # Age category risk distribution
        fig_age = px.bar(
            age_data,
            x='AGE_CATEGORY',
            y='AVG_TOTAL_RISK_SCORE',
            color='AVG_TOTAL_RISK_SCORE',
            color_continuous_scale=[[0, COLORS['star_blue']], [0.5, COLORS['valencia_orange']], [1, COLORS['first_light']]],
            title="Average Risk Score by Age Category",
            labels={'AVG_TOTAL_RISK_SCORE': 'Average Risk Score', 'AGE_CATEGORY': 'Age Category'}
        )
        fig_age.update_layout(
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_age, use_container_width=True)
        
        # Display age category details
        st.dataframe(
            age_data.style.format({
                'AVG_AGE_RISK_SCORE': '{:.1f}',
                'AVG_TOTAL_RISK_SCORE': '{:.1f}'
            }),
            use_container_width=True
        )

with col2:
    st.markdown('<div class="section-header">🗺️ Geographic Distribution</div>', unsafe_allow_html=True)
    
    if 'geographic_distribution' in analytics_data:
        geo_data = analytics_data['geographic_distribution']
        
        # Geographic risk pie chart
        fig_geo = px.pie(
            geo_data,
            values='CUSTOMER_COUNT',
            names='CUSTOMER_STATE',
            color_discrete_sequence=[COLORS['main'], COLORS['star_blue'], COLORS['valencia_orange'], COLORS['purple_moon']],
            title="Customer Distribution by State"
        )
        fig_geo.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=350
        )
        st.plotly_chart(fig_geo, use_container_width=True)
        
        # Display geographic details
        st.dataframe(
            geo_data.style.format({
                'AVG_RISK_SCORE': '{:.1f}',
                'AVG_CLAIM_AMOUNT': '${:,.0f}'
            }),
            use_container_width=True
        )

# -----------------------------------------------------------------------------
# RISK ANALYSIS SECTION
# -----------------------------------------------------------------------------

st.markdown('<div class="section-header">⚠️ Risk Level Analysis</div>', unsafe_allow_html=True)

if 'risk_factors_analysis' in analytics_data:
    risk_data = analytics_data['risk_factors_analysis']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Risk level distribution donut chart
        fig_risk = px.pie(
            risk_data,
            values='COUNT',
            names='RISK_LEVEL',
            hole=0.4,
            color='RISK_LEVEL',
            color_discrete_map={
                'HIGH': COLORS['first_light'],
                'MEDIUM': COLORS['valencia_orange'], 
                'LOW': COLORS['star_blue']
            },
            title="Risk Level Distribution"
        )
        fig_risk.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_risk, use_container_width=True)
    
    with col2:
        # Risk vs Premium analysis
        fig_premium = px.scatter(
            risk_data,
            x='AVG_PREMIUM',
            y='AVG_SCORE',
            size='COUNT',
            color='RISK_LEVEL',
            color_discrete_map={
                'HIGH': COLORS['first_light'],
                'MEDIUM': COLORS['valencia_orange'],
                'LOW': COLORS['star_blue']
            },
            title="Risk Score vs Premium Analysis",
            labels={'AVG_PREMIUM': 'Average Premium ($)', 'AVG_SCORE': 'Average Risk Score'}
        )
        fig_premium.update_layout(
            title_font_color=COLORS['mid_blue'],
            height=400
        )
        st.plotly_chart(fig_premium, use_container_width=True)

# -----------------------------------------------------------------------------
# GOVERNANCE VALIDATION
# -----------------------------------------------------------------------------

st.markdown('<div class="section-header">🔐 Progressive Governance Validation</div>', unsafe_allow_html=True)

st.markdown(f"""
<div class="governance-note">
<strong>Governance in Action:</strong> This section shows how data masking and row access policies 
affect what you can see based on your current role. Claim amounts are masked and geographic access is restricted.
</div>
""", unsafe_allow_html=True)

if governance_data and 'current_role_view' in governance_data:
    current_role = governance_data['current_role_view'].iloc[0]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Current Session View**")
        st.markdown(f"""
        - **Active Role:** {current_role['ROLE_NAME']}
        - **Visible Records:** {current_role['VISIBLE_RECORDS']:,}
        - **Visible States:** {current_role['VISIBLE_STATES']}
        - **Avg Claim Amount:** ${current_role['AVG_CLAIM_AMOUNT']:,.0f}
        - **Max Claim Amount:** ${current_role['MAX_CLAIM_AMOUNT']:,.0f}
        """)
        
        # Governance policies explanation
        st.markdown("**Applied Governance Policies:**")
        st.markdown("""
        - 🎭 **Data Masking**: Claim amounts floored to $10K increments
        - 🗺️ **Row Access**: Limited to CO/UT/WY territories  
        - 🔒 **Account-Aware**: Different access based on role/account
        """)
    
    with col2:
        st.markdown("**Sample Governed Data**")
        if 'sample_data' in governance_data:
            sample_data = governance_data['sample_data']
            
            # Display sample with formatting
            st.dataframe(
                sample_data.style.format({
                    'CLAIM_AMOUNT_FILLED': '${:,.0f}',
                    'TOTAL_RISK_SCORE': '{:.1f}'
                }),
                use_container_width=True
            )
            
            st.markdown("**Notice:**")
            st.markdown("""
            - Claim amounts are masked (rounded to $10K)
            - Only CO/UT/WY states visible
            - All data respects governance policies
            """)

# Governance summary metrics
if governance_data and 'current_role_view' in governance_data:
    current_data = governance_data['current_role_view'].iloc[0]
    
    governance_summary = pd.DataFrame({
        'Governance Feature': [
            'Data Masking',
            'Row Access Control', 
            'Geographic Restriction',
            'Real-time Enforcement'
        ],
        'Status': [
            '✅ Active (Claim amounts masked)',
            '✅ Active (Territory-based)',
            f"✅ Active ({current_data['VISIBLE_STATES']} states only)",
            '✅ Active (Policy-enforced)'
        ],
        'Business Impact': [
            'Protects sensitive financial data',
            'Ensures appropriate data access',
            'Maintains broker territory boundaries', 
            'Zero configuration for consumers'
        ]
    })
    
    st.dataframe(governance_summary, use_container_width=True)

# -----------------------------------------------------------------------------
# FOOTER
# -----------------------------------------------------------------------------

st.markdown("---")
st.markdown(f"""
<div style='text-align: center; color: {COLORS['medium_gray']}; padding: 20px;'>
    <p><strong>MountainPeak Insurance - Risk Analytics Dashboard</strong></p>
    <p>Powered by Snowflake Dynamic Tables • Python UDFs • Progressive Governance</p>
    <p>Real-time risk intelligence with automated refresh • Secure broker data sharing</p>
</div>
""", unsafe_allow_html=True) 