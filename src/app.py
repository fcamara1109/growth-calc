import streamlit as st
import pandas as pd
import plotly.express as px
from database import (
    get_db_connection, 
    get_mau_data,
    SessionManager,
    init_session_tables,
    get_wau_data,
    get_dau_data,
    get_monthly_retention_rates,
    get_weekly_retention_rates,
    get_daily_retention_rates,
    get_monthly_quick_ratio,
    get_weekly_quick_ratio,
    get_daily_quick_ratio,
    get_mrr_data,
    get_wrr_data,
    get_drr_data,
    get_monthly_revenue_retention_rates,
    get_weekly_revenue_retention_rates,
    get_daily_revenue_retention_rates,
    get_monthly_revenue_quick_ratio,
    get_weekly_revenue_quick_ratio,
    get_daily_revenue_quick_ratio,
    get_monthly_cohorts,
    get_weekly_cohorts,
    get_daily_cohorts,
    clear_all_data
)
from datetime import datetime
import io
from visuals.mau import plot_mau
from visuals.wau import plot_wau
from visuals.dau import plot_dau
from visuals.retention import plot_retention_rates
from visuals.quick_ratio import plot_quick_ratio
from visuals.mrr import plot_mrr
from visuals.wrr import plot_wrr
from visuals.drr import plot_drr
from visuals.cohorts import plot_cohorts
from visuals.ltv_cohorts import plot_ltv_cohorts
from visuals.weekly_cohorts import plot_weekly_retention_cohorts
from visuals.weekly_ltv_cohorts import plot_weekly_ltv_cohorts
from visuals.daily_cohorts import plot_daily_retention_cohorts
from visuals.daily_ltv_cohorts import plot_daily_ltv_cohorts
import os

st.set_page_config(
    page_title="Growth, Retention and LTV Calculator",
    page_icon="📊",
    layout="wide"
)

def load_custom_css():
    with open('src/styles/main.css') as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
    
    # Add JetBrains Mono font
    st.markdown("""
        <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
    """, unsafe_allow_html=True)

# Add this right after your st.set_page_config()
load_custom_css()

# Initialize session state
if 'session_id' not in st.session_state:
    session_manager = SessionManager()
    st.session_state.session_id = session_manager.session_id
    init_session_tables(st.session_state.session_id)

# Add this constant at the top of the file
MAX_FILE_SIZE_MB = 20
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

# Add at the beginning of the file, after other session state initialization
if 'should_apply_filters' not in st.session_state:
    st.session_state.should_apply_filters = False

# Add this to your session state initialization section
if 'data_generated' not in st.session_state:
    st.session_state.data_generated = False

def validate_csv(df):
    """Validate CSV data format and content"""
    required_columns = {'date', 'id', 'revenue', 'user_id'}
    
    # Check for required columns
    if not all(col in df.columns for col in required_columns):
        return False, "Missing required columns. Please ensure your CSV has: date, id, revenue, user_id"
    
    # Validate data types
    try:
        df['date'] = pd.to_datetime(df['date'])
        df['revenue'] = pd.to_numeric(df['revenue'])
        df['id'] = df['id'].astype(str)
        df['user_id'] = df['user_id'].astype(str)
    except Exception as e:
        return False, f"Data type validation failed: {str(e)}"
    
    return True, df

def store_data(df):
    """Store validated data in PostgreSQL"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        values = [
            (row['date'], row['id'], float(row['revenue']), row['user_id'])
            for _, row in df.iterrows()
        ]
        
        from psycopg2.extras import execute_values
        execute_values(
            cur,
            f"""
            INSERT INTO revenue_data_{st.session_state.session_id} 
            (transaction_date, transaction_id, revenue, user_id)
            VALUES %s
            """,
            values
        )
        
        conn.commit()
        return True, f"Successfully inserted {len(values)} records"
    except Exception as e:
        conn.rollback()
        return False, f"Error storing data: {str(e)}"
    finally:
        cur.close()
        conn.close()

def clear_session_data(session_id):
    """Clear all data for the current session"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
        # Drop the table
        cur.execute(f"DROP TABLE IF EXISTS revenue_data_{session_id}")
        conn.commit()
        
        # Reinitialize the table
        init_session_tables(session_id)
        
        return True, "Data cleared successfully"
    except Exception as e:
        conn.rollback()
        return False, f"Error clearing data: {str(e)}"
    finally:
        cur.close()
        conn.close()

def get_date_filters(key_prefix):
    """Create date filters with 6-month default range"""
    end_date = datetime.now().date() - pd.Timedelta(days=1)  # Yesterday
    start_date = end_date - pd.DateOffset(months=6)
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        st.markdown("From")  # Label above input
        start = st.date_input("", value=start_date, key=f"{key_prefix}_start_date", label_visibility="collapsed")
    with col2:
        st.markdown("To")  # Label above input
        end = st.date_input("", value=end_date, key=f"{key_prefix}_end_date", label_visibility="collapsed")
    with col3:
        st.markdown("&nbsp;")  # Empty space to align with date inputs
        apply_filters = st.button("Apply filters", key=f"{key_prefix}_apply", use_container_width=True)
    
    return start, end, apply_filters

def main():
    st.title("📊 Growth, Retention and LTV Calculator")
    
    tab1, tab2, tab3 = st.tabs([
        "1️⃣ Upload",
        "2️⃣ Visualize",
        "3️⃣ Go Further"
    ])
    
    with tab1:
        st.subheader("Upload Data")
        
        # Create main columns for the entire section
        main_col1, main_col2 = st.columns([1, 2])
        
        # First column: Instructions and example download
        with main_col1:
            st.markdown("##### ℹ️ Instructions")
            st.markdown("""
                - Maximum file size: 20MB
                - Required CSV columns: date, transaction id, revenue, user id
                - Filters only apply after clicking "Apply filters"
                - See example file below to know the expected format, or to test the app with it
            """)
            
            # Single download button with emoji
            col1, col2, col3 = st.columns([1, 3, 1])
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
                label_visibility="collapsed"  # This hides the default label completely
            )
            
            if uploaded_file is not None:
                # Check file size
                file_size = len(uploaded_file.getvalue())
                if file_size > MAX_FILE_SIZE_BYTES:
                    st.error(f"File size exceeds {MAX_FILE_SIZE_MB}MB limit. Please upload a smaller file.")
                    return
                
                try:
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
                            "date": st.column_config.TextColumn(
                                "date",
                            ),
                            "id": st.column_config.TextColumn(
                                "id",
                            ),
                            "revenue": st.column_config.NumberColumn(
                                "revenue",
                                format="$%d"
                            ),
                            "user_id": st.column_config.TextColumn(
                                "user_id",
                            ),
                        },
                        use_container_width=False,  # This will make the table only as wide as it needs to be
                    )
                    
                    is_valid, result = validate_csv(df)
                    
                    if is_valid:
                        # Adjusted column widths to prevent button text wrapping
                        col1, col2, col3 = st.columns([0.25, 0.2, 0.65])  # Increased first column width
                        with col1:
                            generate_button = st.button("Generate Charts", use_container_width=True)
                        with col2:
                            clear_button = st.button("Clear Data")
                        
                        if generate_button:
                            try:
                                with st.spinner('Clearing existing data...'):
                                    clear_all_data(st.session_state.session_id)
                                
                                # Progress bar and storage logic...
                                progress_bar = st.progress(0)
                                status_text = st.empty()
                                
                                total_rows = len(result)
                                chunk_size = max(1, total_rows // 100)
                                
                                processed_rows = 0
                                chunks = [result[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]
                                
                                for chunk in chunks:
                                    with st.spinner('Storing data...'):
                                        success, _ = store_data(chunk)
                                        if not success:
                                            st.error("Error storing data chunk")
                                            break
                                        
                                        processed_rows += len(chunk)
                                        progress = min(processed_rows / total_rows, 1.0)
                                        progress_bar.progress(progress)
                                        status_text.text(f"{progress:.1%} Stored {processed_rows:,} of {total_rows:,} rows")
                                
                                if processed_rows == total_rows:
                                    progress_bar.empty()
                                    status_text.empty()
                                    st.success(f"Successfully stored {total_rows:,} records, charts will be generated shortly!")
                                    # Set flags to automatically apply filters on initial data load
                                    st.session_state.should_apply_filters = True
                                    st.session_state.data_generated = True
                            
                            except Exception as e:
                                st.error(f"Error storing data: {str(e)}")
                        
                        if clear_button:
                            success, message = clear_session_data(st.session_state.session_id)
                            if success:
                                st.success(message)
                            else:
                                st.error(message)
                    else:
                        st.error(result)
                        
                except Exception as e:
                    st.error(f"Error processing file: {str(e)}")
    
    with tab2:
        st.subheader("Filters")
        
        # Create a container for filters that's left-aligned
        filter_container_col1, filter_container_col2 = st.columns([1, 1])
        
        with filter_container_col1:
            # Create a single row for all filters with adjusted widths
            filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1.25, 0.9, 0.9, 1.25])
            
            with filter_col1:
                st.markdown("Select Period", help="Choose the time period for analysis")
                frequency = st.selectbox(
                    "",
                    options=["Monthly", "Weekly", "Daily"],
                    key="combined_frequency",
                    label_visibility="collapsed"
                )
            
            with filter_col2:
                st.markdown("From")
                start_date = st.date_input(
                    "",
                    value=datetime.now().date() - pd.DateOffset(months=6),
                    key="viz_start_date",
                    label_visibility="collapsed"
                )
            
            with filter_col3:
                st.markdown("To")
                end_date = st.date_input(
                    "",
                    value=datetime.now().date() - pd.Timedelta(days=1),
                    key="viz_end_date",
                    label_visibility="collapsed"
                )
            
            with filter_col4:
                st.markdown("&nbsp;")  # Empty space for alignment
                apply_filters = st.button(
                    "Apply filters",
                    key="viz_apply",
                    use_container_width=True
                )

        # Store the data in session state to avoid reloading when only heatmap values change
        if (st.session_state.should_apply_filters and st.session_state.data_generated) or \
           (st.session_state.data_generated and apply_filters):
            
            # Store the selected frequency
            st.session_state.last_frequency = frequency
            
            # Store all the data we need in session state
            if frequency == "Monthly":
                st.session_state.monthly_data = {
                    'cohorts': get_monthly_cohorts(st.session_state.session_id, start_date, end_date),
                    'retention': get_monthly_retention_rates(st.session_state.session_id, start_date, end_date),
                    'revenue_retention': get_monthly_revenue_retention_rates(st.session_state.session_id, start_date, end_date),
                    'quick_ratio': get_monthly_quick_ratio(st.session_state.session_id, start_date, end_date),
                    'revenue_quick_ratio': get_monthly_revenue_quick_ratio(st.session_state.session_id, start_date, end_date),
                    'growth': get_mrr_data(st.session_state.session_id, start_date, end_date),
                    'users': get_mau_data(st.session_state.session_id, start_date, end_date)
                }
            elif frequency == "Weekly":
                st.session_state.weekly_data = {
                    'cohorts': get_weekly_cohorts(st.session_state.session_id, start_date, end_date),
                    'retention': get_weekly_retention_rates(st.session_state.session_id, start_date, end_date),
                    'revenue_retention': get_weekly_revenue_retention_rates(st.session_state.session_id, start_date, end_date),
                    'quick_ratio': get_weekly_quick_ratio(st.session_state.session_id, start_date, end_date),
                    'revenue_quick_ratio': get_weekly_revenue_quick_ratio(st.session_state.session_id, start_date, end_date),
                    'growth': get_wrr_data(st.session_state.session_id, start_date, end_date),
                    'users': get_wau_data(st.session_state.session_id, start_date, end_date)
                }
            else:  # Daily
                st.session_state.daily_data = {
                    'cohorts': get_daily_cohorts(st.session_state.session_id, start_date, end_date),
                    'retention': get_daily_retention_rates(st.session_state.session_id, start_date, end_date),
                    'revenue_retention': get_daily_revenue_retention_rates(st.session_state.session_id, start_date, end_date),
                    'quick_ratio': get_daily_quick_ratio(st.session_state.session_id, start_date, end_date),
                    'revenue_quick_ratio': get_daily_revenue_quick_ratio(st.session_state.session_id, start_date, end_date),
                    'growth': get_drr_data(st.session_state.session_id, start_date, end_date),
                    'users': get_dau_data(st.session_state.session_id, start_date, end_date)
                }

        if st.session_state.data_generated and hasattr(st.session_state, 'last_frequency'):
            current_freq = st.session_state.last_frequency
            data_key = f"{current_freq.lower()}_data"
            
            if data_key in st.session_state:
                data = st.session_state[data_key]
                
                # Growth Section
                st.markdown("### Growth Trends")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("##### Revenue")
                    plot_mrr(data['growth']) if current_freq == "Monthly" else \
                    plot_wrr(data['growth']) if current_freq == "Weekly" else \
                    plot_drr(data['growth'])
                
                with col2:
                    st.markdown("##### Users")
                    plot_mau(data['users']) if current_freq == "Monthly" else \
                    plot_wau(data['users']) if current_freq == "Weekly" else \
                    plot_dau(data['users'])

                # Retention Section
                st.markdown("### Retention over Period")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("##### User")
                    period = "month" if current_freq == "Monthly" else "week" if current_freq == "Weekly" else "day"
                    plot_retention_rates(data['retention'], period)
                
                with col2:
                    st.markdown("##### Revenue")
                    plot_retention_rates(data['revenue_retention'], period)

                # Quick Ratio Section
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
                    plot_quick_ratio(data['quick_ratio'], period)
                
                with col2:
                    st.markdown("##### Revenue")
                    plot_quick_ratio(data['revenue_quick_ratio'], period)

                # Cohorts Section
                st.markdown("### Cohorts")
                
                # Show appropriate note based on frequency
                if current_freq == "Monthly":
                    st.info("Note: Limited to 24 months since first purchase")
                elif current_freq == "Weekly":
                    st.info("Notes:\n"
                           "- Weeks start on Sunday\n"
                           "- Limited to 52 weeks since first purchase")
                else:  # Daily
                    st.info("Note: Limited to 90 days since first purchase")

                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("##### Retention")
                    if current_freq == "Monthly":
                        plot_cohorts(data['cohorts'])
                    elif current_freq == "Weekly":
                        plot_weekly_retention_cohorts(data['cohorts'])
                    else:  # Daily
                        plot_daily_retention_cohorts(data['cohorts'])
                
                with col2:
                    st.markdown("##### LTV")
                    if current_freq == "Monthly":
                        plot_ltv_cohorts(data['cohorts'])
                    elif current_freq == "Weekly":
                        plot_weekly_ltv_cohorts(data['cohorts'])
                    else:  # Daily
                        plot_daily_ltv_cohorts(data['cohorts'])

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

# After the first render with generated data, reset the should_apply_filters flag
if st.session_state.should_apply_filters and st.session_state.data_generated:
    st.session_state.should_apply_filters = False

if __name__ == "__main__":
    main()
