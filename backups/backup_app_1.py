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
    page_title="Unit Economics Generator",
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
        apply_filters = st.button("Confirm filters", key=f"{key_prefix}_apply", use_container_width=True)
    
    return start, end, apply_filters

def main():
    st.title("📊 Unit Economics Generator")
    
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8, tab9 = st.tabs([
        "Data Upload", 
        "Revenue Growth",
        "User Growth",
        "User Ret. over Period",
        "Revenue Ret. over Period",
        "User Quick Ratio",
        "Revenue Quick Ratio",
        "Retention Cohort",
        "LTV Cohort"
    ])
    
    with tab1:
        st.subheader("Upload Data")
        
        # Create main columns for the entire section
        main_col1, main_col2 = st.columns([1, 3])
        
        # First column: Instructions and example download
        with main_col1:
            st.markdown("##### Instructions")
            st.markdown("""
                - Maximum file size: 20MB
                - Required CSV columns: date, id, revenue, user_id
                - Download example file below to see the expected format
            """)
            
            # Single download button with emoji
            with open('2024_template_unit_economics.csv', 'r') as file:
                st.download_button(
                    label="📥 Download example CSV",
                    data=file,
                    file_name="template_unit_economics.csv",
                    mime="text/csv",
                    type="secondary",
                    use_container_width=False
                )
        
        # Second column: File upload and preview
        with main_col2:
            st.markdown("##### Choose CSV file")
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
                    
                    st.markdown("##### Data Preview:")
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
                        col1, col2, col3 = st.columns([0.2, 0.15, 0.65])  # Increased first column width
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
                                    # Set flag to automatically apply filters
                                    st.session_state.should_apply_filters = True
                            
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
        st.subheader("Revenue Analysis")
        
        revenue_frequency = st.selectbox(
            "Select Analysis Frequency",
            options=["Monthly Recurring Revenue", "Weekly Recurring Revenue", "Daily Recurring Revenue"],
            key="revenue_frequency"
        )
        
        start_date, end_date, apply_filters = get_date_filters("revenue")
        
        # Only fetch and display data when filters are confirmed
        if apply_filters or st.session_state.should_apply_filters:
            if revenue_frequency == "Monthly Recurring Revenue":
                data = get_mrr_data(st.session_state.session_id, start_date, end_date)
                plot_mrr(data)
            elif revenue_frequency == "Weekly Recurring Revenue":
                data = get_wrr_data(st.session_state.session_id, start_date, end_date)
                plot_wrr(data)
            else:  # Daily Recurring Revenue
                data = get_drr_data(st.session_state.session_id, start_date, end_date)
                plot_drr(data)
    
    with tab3:
        st.subheader("User Analysis")
        
        frequency = st.selectbox(
            "Select Analysis Frequency",
            options=["Monthly Active Users", "Weekly Active Users", "Daily Active Users"],
            key="frequency"
        )
        
        start_date, end_date, apply_filters = get_date_filters("users")
        
        # Only fetch and display data when filters are confirmed
        if apply_filters or st.session_state.should_apply_filters:
            if frequency == "Monthly Active Users":
                data = get_mau_data(st.session_state.session_id, start_date, end_date)
                plot_mau(data)
            elif frequency == "Weekly Active Users":
                data = get_wau_data(st.session_state.session_id, start_date, end_date)
                plot_wau(data)
            else:  # Daily Active Users
                data = get_dau_data(st.session_state.session_id, start_date, end_date)
                plot_dau(data)
    
    with tab4:
        st.subheader("Retention Analysis")
        
        retention_frequency = st.selectbox(
            "Select Analysis Frequency",
            options=["Monthly Retention", "Weekly Retention", "Daily Retention"],
            key="retention_frequency"
        )
        
        start_date, end_date, apply_filters = get_date_filters("retention")
        
        if apply_filters or st.session_state.should_apply_filters:
            if retention_frequency == "Monthly Retention":
                data = get_monthly_retention_rates(st.session_state.session_id, start_date, end_date)
                plot_retention_rates(data, "month")
            elif retention_frequency == "Weekly Retention":
                data = get_weekly_retention_rates(st.session_state.session_id, start_date, end_date)
                plot_retention_rates(data, "week")
            else:  # Daily Retention
                data = get_daily_retention_rates(st.session_state.session_id, start_date, end_date)
                plot_retention_rates(data, "day")
    
    with tab5:
        st.subheader("Revenue Retention Analysis")
        
        revenue_retention_frequency = st.selectbox(
            "Select Analysis Frequency",
            options=["Monthly Revenue Retention", "Weekly Revenue Retention", "Daily Revenue Retention"],
            key="revenue_retention_frequency"
        )
        
        start_date, end_date, apply_filters = get_date_filters("revenue_retention")
        
        if apply_filters or st.session_state.should_apply_filters:
            if revenue_retention_frequency == "Monthly Revenue Retention":
                data = get_monthly_revenue_retention_rates(st.session_state.session_id, start_date, end_date)
                plot_retention_rates(data, "month")
            elif revenue_retention_frequency == "Weekly Revenue Retention":
                data = get_weekly_revenue_retention_rates(st.session_state.session_id, start_date, end_date)
                plot_retention_rates(data, "week")
            else:  # Daily Revenue Retention
                data = get_daily_revenue_retention_rates(st.session_state.session_id, start_date, end_date)
                plot_retention_rates(data, "day")
    
    with tab6:
        st.subheader("Quick Ratio Analysis")
        
        quick_ratio_frequency = st.selectbox(
            "Select Analysis Frequency",
            options=["Monthly Quick Ratio", "Weekly Quick Ratio", "Daily Quick Ratio"],
            key="quick_ratio_frequency"
        )
        
        start_date, end_date, apply_filters = get_date_filters("quick_ratio")
        
        if apply_filters or st.session_state.should_apply_filters:
            if quick_ratio_frequency == "Monthly Quick Ratio":
                data = get_monthly_quick_ratio(st.session_state.session_id, start_date, end_date)
                plot_quick_ratio(data, "month")
            elif quick_ratio_frequency == "Weekly Quick Ratio":
                data = get_weekly_quick_ratio(st.session_state.session_id, start_date, end_date)
                plot_quick_ratio(data, "week")
            else:  # Daily Quick Ratio
                data = get_daily_quick_ratio(st.session_state.session_id, start_date, end_date)
                plot_quick_ratio(data, "day")
    
    with tab7:
        st.subheader("Revenue Quick Ratio Analysis")
        
        revenue_quick_ratio_frequency = st.selectbox(
            "Select Analysis Frequency",
            options=["Monthly Revenue Quick Ratio", "Weekly Revenue Quick Ratio", "Daily Revenue Quick Ratio"],
            key="revenue_quick_ratio_frequency"
        )
        
        start_date, end_date, apply_filters = get_date_filters("revenue_quick_ratio")
        
        if apply_filters or st.session_state.should_apply_filters:
            if revenue_quick_ratio_frequency == "Monthly Revenue Quick Ratio":
                data = get_monthly_revenue_quick_ratio(st.session_state.session_id, start_date, end_date)
                plot_quick_ratio(data, "month")
            elif revenue_quick_ratio_frequency == "Weekly Revenue Quick Ratio":
                data = get_weekly_revenue_quick_ratio(st.session_state.session_id, start_date, end_date)
                plot_quick_ratio(data, "week")
            else:  # Daily Revenue Quick Ratio
                data = get_daily_revenue_quick_ratio(st.session_state.session_id, start_date, end_date)
                plot_quick_ratio(data, "day")
    
    with tab8:
        st.subheader("Cohort Analysis")
        
        cohort_frequency = st.selectbox(
            "Select Analysis Frequency",
            options=["Monthly Retention", "Weekly Retention", "Daily Retention"],
            key="cohort_frequency"
        )
        
        start_date, end_date, apply_filters = get_date_filters("cohorts")
        
        if apply_filters or st.session_state.should_apply_filters:
            if cohort_frequency == "Monthly Retention":
                monthly_data = get_monthly_cohorts(st.session_state.session_id, start_date, end_date)
                plot_cohorts(monthly_data)
            elif cohort_frequency == "Weekly Retention":
                weekly_data = get_weekly_cohorts(st.session_state.session_id, start_date, end_date)
                plot_weekly_retention_cohorts(weekly_data)
            else:  # Daily Retention
                daily_data = get_daily_cohorts(st.session_state.session_id, start_date, end_date)
                plot_daily_retention_cohorts(daily_data)
    
    with tab9:
        st.subheader("LTV Cohort Analysis")
        
        ltv_cohort_frequency = st.selectbox(
            "Select Analysis Frequency",
            options=["Monthly LTV", "Weekly LTV", "Daily LTV"],
            key="ltv_cohort_frequency"
        )
        
        start_date, end_date, apply_filters = get_date_filters("ltv_cohorts")
        
        if apply_filters or st.session_state.should_apply_filters:
            if ltv_cohort_frequency == "Monthly LTV":
                data = get_monthly_cohorts(st.session_state.session_id, start_date, end_date)
                plot_ltv_cohorts(data)
            elif ltv_cohort_frequency == "Weekly LTV":
                data = get_weekly_cohorts(st.session_state.session_id, start_date, end_date)
                plot_weekly_ltv_cohorts(data)
            else:  # Daily LTV
                data = get_daily_cohorts(st.session_state.session_id, start_date, end_date)
                plot_daily_ltv_cohorts(data)

if __name__ == "__main__":
    main()
