import streamlit as st
import pandas as pd
import uuid
from database import (
    create_revenue_table, 
    get_daily_revenue, 
    clear_session_data, 
    get_mau_data, 
    get_wau_data,
    get_dau_data,
    get_mrr_data,
    get_wrr_data,
    get_monthly_retention_data,
    get_weekly_retention_data,
    get_daily_retention_data,
    get_drr_data,
    get_monthly_revenue_retention_data,
    get_weekly_revenue_retention_data,
    get_daily_revenue_retention_data,
    get_monthly_quick_ratio_data,
    get_monthly_revenue_quick_ratio_data,
    get_weekly_revenue_quick_ratio_data,
    get_weekly_quick_ratio_data,
    get_daily_quick_ratio_data,
    get_daily_revenue_quick_ratio_data,
    get_monthly_cohorts_data,
    get_initial_monthly_data,
    get_weekly_cohorts_data,
    get_daily_cohorts_data,
    refresh_views
)
from visuals.mau import plot_mau
from visuals.wau import plot_wau
from visuals.dau import plot_dau
from visuals.mrr import plot_mrr
from visuals.wrr import plot_wrr
from visuals.retention import plot_retention_rates
from visuals.drr import plot_drr
from visuals.quick_ratio import plot_quick_ratio
from visuals.cohorts import plot_cohorts
from visuals.ltv_cohorts import plot_ltv_cohorts
from datetime import datetime
import time
from metrics import MetricsLogger
from logger import ErrorLogger
from st_supabase_connection import SupabaseConnection

# Page config must be the first Streamlit command
st.set_page_config(
    page_title="Growth, Retention and LTV Calculator",
    page_icon="📊",
    layout="wide"
)

def load_css():
    with open('src/styles/main.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Load font and CSS after page config
st.markdown("""
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
""", unsafe_allow_html=True)
load_css()

# Initialize session ID if not exists
if 'session_id' not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

# Constants
MAX_FILE_SIZE_MB = 20
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

# Add credit text as footer
st.markdown("""
<style>
.footer {
    position: fixed;
    bottom: 0;
    left: 0;
    padding: 10px;
    font-size: 0.8em;
    color: #888888;
}
</style>
<div class="footer">made by f.camara</div>
""", unsafe_allow_html=True)

st.title("📊 Growth, Retention and LTV Calculator")

# Create tabs
tab1, tab2, tab3 = st.tabs([
    "1️⃣ Upload",
    "2️⃣ Visualize", 
    "3️⃣ Go Further"
])

# Initialize loggers
supabase = st.connection("supabase", type=SupabaseConnection)
metrics = MetricsLogger(supabase)
error_logger = ErrorLogger(supabase)

with tab1:
    st.subheader("Upload Data")
    
    # Create main columns for the section with thin spacer column
    main_col1, spacer, main_col2 = st.columns([1, 0.2, 2])
    
    # First column: Instructions and example download
    with main_col1:
        st.markdown("##### ℹ️ Instructions")
        st.markdown("""
            - Maximum file size: 20MB
            - Required CSV columns: date, transaction id, revenue, user id
            - Filters only apply after clicking "Apply filters"
            - See example file below to know the expected format, or to test the app with it
        """)
        
        # Download button
        col1, col2, col3 = st.columns([0.8, 3.5, 0.8])
        with col2:
            with open('2024_template_unit_economics.csv', 'r') as file:
                st.download_button(
                    label="📥 Download example CSV",
                    data=file,
                    file_name="template_unit_economics.csv",
                    mime="text/csv",
                    type="secondary",
                    use_container_width=True
                )
    
    # Second column: File upload and preview
    with main_col2:
        st.markdown("##### 📂 Choose CSV file")
        uploaded_file = st.file_uploader(
            "",  # Empty label since we're using the header above
            type=['csv'],
            label_visibility="collapsed"
        )
        
        if uploaded_file is not None:
            # Check file size
            file_size = len(uploaded_file.getvalue())
            if file_size > MAX_FILE_SIZE_BYTES:
                st.error(f"File size exceeds {MAX_FILE_SIZE_MB}MB limit. Please upload a smaller file.")
                metrics.log_upload(file_size, 0, False, "File size exceeds limit")
            else:
                try:
                    start_time = time.time()
                    df = pd.read_csv(uploaded_file)
                    
                    # Clean up date format - remove time if it's all zeros
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                        if df['date'].str.contains('00:00:00').all():
                            df['date'] = df['date'].str.replace(' 00:00:00', '')
                    
                    st.markdown("##### 👀 Data Preview:")
                    st.dataframe(
                        df.head(),
                        hide_index=True,
                        column_config={
                            "date": st.column_config.TextColumn("date"),
                            "id": st.column_config.TextColumn("id"),
                            "revenue": st.column_config.NumberColumn(
                                "revenue",
                                format="$%d"
                            ),
                            "user_id": st.column_config.TextColumn("user_id"),
                        },
                        use_container_width=False,
                    )
                    
                    # Validate required columns
                    required_columns = ['date', 'id', 'revenue', 'user_id']
                    if not all(col in df.columns for col in required_columns):
                        st.error("CSV must contain these columns: date, id, revenue, user_id")
                    else:
                        # Add buttons for actions
                        col1, col2, col3 = st.columns([0.3, 0.2, 0.65])
                        with col1:
                            if st.button("Generate Charts", use_container_width=True):
                                try:
                                    with st.spinner('Clearing existing data...'):
                                        clear_session_data()
                                    
                                    # Start timing
                                    start_time = time.time()
                                    
                                    # Progress bar and storage logic...
                                    progress_bar = st.progress(0)
                                    status_text = st.empty()
                                    metrics_text = st.empty()
                                    
                                    total_rows = len(df)
                                    chunk_size = max(1, total_rows // 100) 
                                    processed_rows = 0
                                    
                                    # Process data in chunks
                                    for i in range(0, total_rows, chunk_size):
                                        chunk = df[i:i + chunk_size]
                                        create_revenue_table(chunk)
                                        
                                        processed_rows += len(chunk)
                                        progress = min(processed_rows / total_rows, 1.0)
                                        progress_bar.progress(progress)
                                        status_text.text(f"{progress:.1%} Stored {processed_rows:,} of {total_rows:,} rows")
                                    
                                    # Refresh views once after all data is loaded
                                    with st.spinner('Loading views...'):
                                        refresh_views(st.session_state.session_id)
                                    
                                    # Store success message in session state
                                    st.session_state.upload_success = f"Success! Stored {total_rows:,} records."
                                    
                                    # Set flags to automatically apply filters on initial data load
                                    st.session_state.filters_applied = True
                                    st.session_state.period_data = get_initial_monthly_data()
                                    
                                    # Force a rerun to show the visualization
                                    st.rerun()
                                        
                                except Exception as e:
                                    st.error(f"Error storing data: {str(e)}")
                            
                            # Move the success message display outside the button click handler
                            # and after any potential rerun
                            if 'upload_success' in st.session_state:
                                st.success(st.session_state.upload_success)
                                # Clear the message after displaying it
                                del st.session_state.upload_success
                        with col2:
                            if st.button("Clear Data"):
                                try:
                                    with st.spinner('Clearing data...'):
                                        result = clear_session_data()
                                        if result is not None:
                                            st.success("All data cleared!")
                                            # Reset the session state
                                            st.session_state.data_generated = False
                                            # Force a rerun to refresh the page
                                            st.rerun()
                                        else:
                                            st.info("No data to clear")
                                except Exception as e:
                                    st.error(f"Error clearing data: {str(e)}")
                
                    processing_time = (time.time() - start_time) * 1000  # Convert to milliseconds
                    metrics.log_upload(file_size, processing_time, True)
                
                except Exception as e:
                    metrics.log_upload(file_size, 0, False, str(e))
                    st.error(f"Error processing file: {str(e)}")

with tab2:
    
    # Create filters section
    st.markdown("### Filters")
    filter_container = st.container()
    
    # Initialize session states if not exists
    if 'filters_applied' not in st.session_state:
        st.session_state.filters_applied = False
    if 'period_data' not in st.session_state:
        st.session_state.period_data = None
    
    with filter_container:
        # Add period selector
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        
        with col1:
            st.markdown("Select Period")
            period = st.selectbox(
                "",
                options=["Monthly", "Weekly", "Daily"],
                key="period_selector",
                label_visibility="collapsed"
            )
        
        with col2:
            st.markdown("From")  # Label above input
            start = st.date_input("", 
                value=datetime.now().date() - pd.DateOffset(months=6),
                key="period_start_date", 
                label_visibility="collapsed"
            )
        
        with col3:
            st.markdown("To")  # Label above input
            end = st.date_input("", 
                value=datetime.now().date(),
                key="period_end_date", 
                label_visibility="collapsed"
            )
        
        with col4:
            st.markdown("&nbsp;")  # Empty space to align with date inputs
            if st.button("Apply filters", key="period_apply", use_container_width=True):
                # Get data based on selected period
                if period == "Monthly":
                    results = get_mau_data()
                    mrr_results = get_mrr_data()
                    retention_results = get_monthly_retention_data()
                    revenue_retention_results = get_monthly_revenue_retention_data()
                    quick_ratio_results = get_monthly_quick_ratio_data()
                    revenue_quick_ratio_results = get_monthly_revenue_quick_ratio_data()
                    cohorts_results = get_monthly_cohorts_data()
                    st.session_state.period_data = {
                        'results': results,
                        'revenue_results': mrr_results,
                        'retention_results': retention_results,
                        'revenue_retention_results': revenue_retention_results,
                        'quick_ratio_results': quick_ratio_results,
                        'revenue_quick_ratio_results': revenue_quick_ratio_results,
                        'cohorts_results': cohorts_results,
                        'period': "Monthly"
                    }
                elif period == "Weekly":
                    results = get_wau_data()
                    wrr_results = get_wrr_data()
                    retention_results = get_weekly_retention_data()
                    revenue_retention_results = get_weekly_revenue_retention_data()
                    quick_ratio_results = get_weekly_quick_ratio_data()
                    revenue_quick_ratio_results = get_weekly_revenue_quick_ratio_data()
                    cohorts_results = get_weekly_cohorts_data()
                    st.session_state.period_data = {
                        'results': results,
                        'revenue_results': wrr_results,
                        'retention_results': retention_results,
                        'revenue_retention_results': revenue_retention_results,
                        'quick_ratio_results': quick_ratio_results,
                        'revenue_quick_ratio_results': revenue_quick_ratio_results,
                        'cohorts_results': cohorts_results,
                        'period': "Weekly"
                    }
                else:  # Daily
                    results = get_dau_data()
                    retention_results = get_daily_retention_data()
                    drr_results = get_drr_data()
                    revenue_retention_results = get_daily_revenue_retention_data()
                    quick_ratio_results = get_daily_quick_ratio_data()
                    revenue_quick_ratio_results = get_daily_revenue_quick_ratio_data()
                    cohorts_results = get_daily_cohorts_data()
                    st.session_state.period_data = {
                        'results': results,
                        'revenue_results': drr_results,
                        'retention_results': retention_results,
                        'revenue_retention_results': revenue_retention_results,
                        'quick_ratio_results': quick_ratio_results,
                        'revenue_quick_ratio_results': revenue_quick_ratio_results,
                        'cohorts_results': cohorts_results,
                        'period': "Daily"
                    }
                
                st.session_state.filters_applied = True
                st.rerun()
    
    # Display data from session state
    if st.session_state.period_data:
        data = st.session_state.period_data
        
        # Static title for all views
        st.markdown("### Growth Trends")
        
        if data['results'].data:
            df = pd.DataFrame(data['results'].data)
            if data['period'] == "Monthly":
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    plot_mau(df)
                with col2:
                    st.markdown("##### Revenue")
                    if data['revenue_results'].data:
                        df_mrr = pd.DataFrame(data['revenue_results'].data)
                        plot_mrr(df_mrr)
            
                # Add Retention over Period section (only for Monthly)
                st.markdown("### Retention over Period")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    if data['retention_results'].data:
                        df_retention = pd.DataFrame(data['retention_results'].data)
                        plot_retention_rates(df_retention, "month")
                    else:
                        st.info("No user retention data available.")
                        
                with col2:
                    st.markdown("##### Revenue", help="Shows only revenue retained (does not include contraction or expansion)")
                    if data['revenue_retention_results'].data:
                        df_revenue_retention = pd.DataFrame(data['revenue_retention_results'].data)
                        plot_retention_rates(df_revenue_retention, "month")
                    else:
                        st.info("No revenue retention data available.")
            
                # Add Quick Ratio section
                st.markdown("### Quick Ratio")
                
                # Add benchmark explanations
                st.write("**Benchmark Guidelines:**")
                benchmark_col1, benchmark_col2, benchmark_col3 = st.columns(3)
                with benchmark_col1:
                    st.write("- <span style='color: #95A5A6'>**1**: You're gaining more users than losing</span>", unsafe_allow_html=True)
                with benchmark_col2:
                    st.write("- <span style='color: #27AE60'>**2**: Good benchmark for consumer companies</span>", unsafe_allow_html=True)
                with benchmark_col3:
                    st.write("- <span style='color: #E67E22'>**4**: Good benchmark for SaaS companies</span>", unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    if (data.get('quick_ratio_results') and 
                        data['quick_ratio_results'].data):
                        df_quick_ratio = pd.DataFrame(data['quick_ratio_results'].data)
                        plot_quick_ratio(df_quick_ratio, "month")
                    else:
                        st.info("No quick ratio data available.")
                
                with col2:
                    st.markdown("##### Revenue")
                    if (data.get('revenue_quick_ratio_results') and 
                        data['revenue_quick_ratio_results'].data):
                        df_revenue_quick_ratio = pd.DataFrame(data['revenue_quick_ratio_results'].data)
                        plot_quick_ratio(df_revenue_quick_ratio, "month")
                    else:
                        st.info("No revenue quick ratio data available.")
            
                # Add Cohorts section
                st.markdown("### Cohorts")

                # Add cohort analysis notes
                st.markdown("""
                <div style='font-size: 0.9em; color: #888888;'>
                <strong>Note:</strong><br/>
                • Monthly cohorts are limited to 24 months since first purchase
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("<br/>", unsafe_allow_html=True)  # Extra whitespace

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("##### User Retention")
                    if (data.get('cohorts_results') and 
                        hasattr(data['cohorts_results'], 'data') and 
                        data['cohorts_results'].data):
                        df_cohorts = pd.DataFrame(data['cohorts_results'].data)
                        plot_cohorts(df_cohorts, "month")
                    else:
                        st.info("No cohorts data available.")

                with col2:
                    st.markdown("##### User LTV")
                    if (data.get('cohorts_results') and 
                        hasattr(data['cohorts_results'], 'data') and 
                        data['cohorts_results'].data):
                        df_cohorts = pd.DataFrame(data['cohorts_results'].data)
                        plot_ltv_cohorts(df_cohorts, "month")
                    else:
                        st.info("No cohorts data available.")
            
            elif data['period'] == "Weekly":
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    plot_wau(df)
                with col2:
                    st.markdown("##### Revenue")
                    if data['revenue_results'].data:
                        df_wrr = pd.DataFrame(data['revenue_results'].data)
                        plot_wrr(df_wrr)
            
                # Update Retention over Period section for Weekly
                st.markdown("### Retention over Period")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    if data['retention_results'].data:
                        df_retention = pd.DataFrame(data['retention_results'].data)
                        plot_retention_rates(df_retention, "week")
                    else:
                        st.info("No user retention data available.")
                        
                with col2:
                    st.markdown("##### Revenue", help="Shows only revenue retained (does not include contraction or expansion)")
                    if data['revenue_retention_results'].data:
                        df_revenue_retention = pd.DataFrame(data['revenue_retention_results'].data)
                        plot_retention_rates(df_revenue_retention, "week")
                    else:
                        st.info("No revenue retention data available.")
            
                # Add Quick Ratio section
                st.markdown("### Quick Ratio")
                
                # Add benchmark explanations
                st.write("**Benchmark Guidelines:**")
                benchmark_col1, benchmark_col2, benchmark_col3 = st.columns(3)
                with benchmark_col1:
                    st.write("- <span style='color: #95A5A6'>**1**: You're gaining more users than losing</span>", unsafe_allow_html=True)
                with benchmark_col2:
                    st.write("- <span style='color: #27AE60'>**2**: Good benchmark for consumer companies</span>", unsafe_allow_html=True)
                with benchmark_col3:
                    st.write("- <span style='color: #E67E22'>**4**: Good benchmark for SaaS companies</span>", unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    if (data.get('quick_ratio_results') and 
                        data['quick_ratio_results'].data):
                        df_quick_ratio = pd.DataFrame(data['quick_ratio_results'].data)
                        plot_quick_ratio(df_quick_ratio, "week")
                    else:
                        st.info("No quick ratio data available.")
                
                with col2:
                    st.markdown("##### Revenue")
                    if (data.get('revenue_quick_ratio_results') and 
                        data['revenue_quick_ratio_results'].data):
                        df_revenue_quick_ratio = pd.DataFrame(data['revenue_quick_ratio_results'].data)
                        plot_quick_ratio(df_revenue_quick_ratio, "week")
                    else:
                        st.info("No revenue quick ratio data available.")
            
                # Add Cohorts section
                st.markdown("### Cohorts")

                # Add cohort analysis notes
                st.markdown("""
                <div style='font-size: 0.9em; color: #888888;'>
                <strong>Note:</strong><br/>
                • Weekly cohorts are limited to 52 weeks since first purchase (weeks start on Sunday)
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("<br/>", unsafe_allow_html=True)  # Extra whitespace

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("##### User Retention")
                    if (data.get('cohorts_results') and 
                        hasattr(data['cohorts_results'], 'data') and 
                        data['cohorts_results'].data):
                        df_cohorts = pd.DataFrame(data['cohorts_results'].data)
                        plot_cohorts(df_cohorts, "week")
                    else:
                        st.info("No cohorts data available.")

                with col2:
                    st.markdown("##### User LTV")
                    if (data.get('cohorts_results') and 
                        hasattr(data['cohorts_results'], 'data') and 
                        data['cohorts_results'].data):
                        df_cohorts = pd.DataFrame(data['cohorts_results'].data)
                        plot_ltv_cohorts(df_cohorts, "week")
                    else:
                        st.info("No cohorts data available.")
            else:
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    plot_dau(df)
                with col2:
                    st.markdown("##### Revenue")
                    if data['revenue_results'].data:
                        df_drr = pd.DataFrame(data['revenue_results'].data)
                        plot_drr(df_drr)
                        
                # Update Retention over Period section for Daily
                st.markdown("### Retention over Period")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    if data['retention_results'].data:
                        df_retention = pd.DataFrame(data['retention_results'].data)
                        plot_retention_rates(df_retention, "day")
                    else:
                        st.info("No user retention data available.")
                        
                with col2:
                    st.markdown("##### Revenue", help="Shows only revenue retained (does not include contraction or expansion)")
                    if data['revenue_retention_results'].data:
                        df_revenue_retention = pd.DataFrame(data['revenue_retention_results'].data)
                        plot_retention_rates(df_revenue_retention, "day")
                    else:
                        st.info("No revenue retention data available.")
            
                # Add Quick Ratio section
                st.markdown("### Quick Ratio")
                
                # Add benchmark explanations
                st.write("**Benchmark Guidelines:**")
                benchmark_col1, benchmark_col2, benchmark_col3 = st.columns(3)
                with benchmark_col1:
                    st.write("- <span style='color: #95A5A6'>**1**: You're gaining more users than losing</span>", unsafe_allow_html=True)
                with benchmark_col2:
                    st.write("- <span style='color: #27AE60'>**2**: Good benchmark for consumer companies</span>", unsafe_allow_html=True)
                with benchmark_col3:
                    st.write("- <span style='color: #E67E22'>**4**: Good benchmark for SaaS companies</span>", unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown("##### User")
                    if (data.get('quick_ratio_results') and 
                        data['quick_ratio_results'].data):
                        df_quick_ratio = pd.DataFrame(data['quick_ratio_results'].data)
                        plot_quick_ratio(df_quick_ratio, "day")
                    else:
                        st.info("No quick ratio data available.")
                
                with col2:
                    st.markdown("##### Revenue")
                    if (data.get('revenue_quick_ratio_results') and 
                        data['revenue_quick_ratio_results'].data):
                        df_revenue_quick_ratio = pd.DataFrame(data['revenue_quick_ratio_results'].data)
                        plot_quick_ratio(df_revenue_quick_ratio, "day")
                    else:
                        st.info("No revenue quick ratio data available.")
            
                # Add Cohorts section
                st.markdown("### Cohorts")

                # Add cohort analysis notes
                st.markdown("""
                <div style='font-size: 0.9em; color: #888888;'>
                <strong>Note:</strong><br/>
                • Daily cohorts are limited to 90 days since first purchase
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("<br/>", unsafe_allow_html=True)  # Extra whitespace

                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("##### User Retention")
                    if (data.get('cohorts_results') and 
                        hasattr(data['cohorts_results'], 'data') and 
                        data['cohorts_results'].data):
                        df_cohorts = pd.DataFrame(data['cohorts_results'].data)
                        plot_cohorts(df_cohorts, "day")
                    else:
                        st.info("No cohorts data available.")

                with col2:
                    st.markdown("##### User LTV")
                    if (data.get('cohorts_results') and 
                        hasattr(data['cohorts_results'], 'data') and 
                        data['cohorts_results'].data):
                        df_cohorts = pd.DataFrame(data['cohorts_results'].data)
                        plot_ltv_cohorts(df_cohorts, "day")
                    else:
                        st.info("No cohorts data available.")
        else:
            st.info(f"No {data['period'].lower()} data available. Please upload data in the Upload tab.")
    else:
        st.info("Select filters and click 'Apply filters' to view the data")

    try:
        # Only log metrics if the data is actually loaded
        if st.session_state.period_data and st.session_state.period_data.get('results'):
            metrics.log_user_action("view_chart", "visualize", "cohorts")
    except Exception as e:
        error_logger.log_error(e, {"tab": "visualize", "action": "view_chart"})
        # Don't show error to user since metrics logging is non-critical
        pass

with tab3:
    # Create a column that's almost full width
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.subheader("Want to Go Further? 🚀")
        st.markdown("""
        This tool is inspired by Jonathan Hsu's influential 2015 series of Medium posts about startup evaluation through growth due diligence at Social Capital, a prominent Silicon Valley venture capital firm.
        
        If you want to implement these metrics in your own data warehouse, I've created a repository with all the necessary SQL queries for Google BigQuery. The queries are designed to be flexible, allowing you to add your own filters like:
        - Acquisition source, medium, etc...
        - Sign up source, medium, etc...
        - User demographics
        - And much more!
        
        #### 🔗 Check out the repository:
        [github.com/fcamara1109/ltv_and_retention](https://github.com/fcamara1109/ltv_and_retention)
        
        #### 📚 Original Medium Series:
        1. [Diligence at Social Capital, Part 1: Accounting for User Growth](https://medium.com/swlh/diligence-at-social-capital-part-1-accounting-for-user-growth-4a8a449fddfc)
        2. [Part 2: Accounting for Revenue Growth](https://medium.com/swlh/diligence-at-social-capital-part-2-accounting-for-revenue-growth-551fa07dd972)
        3. [Part 3: Cohorts and Revenue LTV](https://medium.com/swlh/diligence-at-social-capital-part-3-cohorts-and-revenue-ltv-ab65a07464e1)
        4. [Part 4: Cohorts and Engagement LTV](https://medium.com/swlh/diligence-at-social-capital-part-4-cohorts-and-engagement-ltv-80b4fa7f8e41)
        5. [Part 5: Depth of Usage and Quality of Revenue](https://medium.com/swlh/diligence-at-social-capital-part-5-depth-of-usage-and-quality-of-revenue-b4dd96b47ca6)
        6. [Epilogue: Introducing the 8-ball and GAAP for Startups](https://medium.com/swlh/diligence-at-social-capital-epilogue-introducing-the-8-ball-and-gaap-for-startups-7ab215c378bc)
        
        #### 📧 Questions or Issues?
        Feel free to contact me at [f.camara1109@gmail.com](mailto:f.camara1109@gmail.com)
        """)