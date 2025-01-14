from st_supabase_connection import SupabaseConnection, execute_query
import streamlit as st
import pandas as pd
from time import sleep
from typing import List
import math

def init_connection():
    """Initialize Supabase connection"""
    return st.connection("supabase", type=SupabaseConnection)

def create_revenue_table_batch(df_chunk):
    """Insert a single batch of data with retry logic"""
    max_retries = 3
    retry_delay = 1  # seconds
    
    for attempt in range(max_retries):
        try:
            conn = init_connection()
            
            # Prepare data
            df_chunk = df_chunk.copy()
            df_chunk = df_chunk.rename(columns={
                'date': 'transaction_date',
                'id': 'transaction_id'
            })
            df_chunk['session_id'] = st.session_state.session_id
            
            # Insert data
            records = df_chunk.to_dict('records')
            result = execute_query(
                conn.table("revenue_data").upsert(records),
                ttl=0
            )
            
            return result
            
        except Exception as e:
            if attempt == max_retries - 1:  # Last attempt
                raise e
            sleep(retry_delay * (attempt + 1))  # Exponential backoff
            continue

def create_revenue_table(df):
    """Insert data into revenue_data table with batching"""
    # Calculate optimal batch size based on total rows
    total_rows = len(df)
    batch_size = min(1000, math.ceil(total_rows / 50))  # Max 1000 rows per batch, min 50 batches
    
    # Process data in smaller batches
    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        chunk = df[start_idx:end_idx]
        
        try:
            create_revenue_table_batch(chunk)
            
            # Refresh materialized views periodically (every 5000 rows or at the end)
            if (end_idx % 5000 == 0) or (end_idx == total_rows):
                conn = init_connection()
                execute_query(
                    conn.table("refresh_trigger").insert({"created_at": "now()"}),
                    ttl=0
                )
                
        except Exception as e:
            raise Exception(f"Error processing rows {start_idx}-{end_idx}: {str(e)}")
    
    return True

def get_daily_revenue():
    """Get all revenue data for current session"""
    conn = init_connection()
    
    # Build base query
    query = (conn.table("revenue_data")
            .select("*")
            .eq('session_id', st.session_state.session_id))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('transaction_date', desc=True),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def clear_session_data():
    """Delete all data for current session"""
    conn = init_connection()
    try:
        # First verify the session exists
        result = execute_query(
            conn.table("revenue_data")
            .select("count")  # Use PostgreSQL count
            .eq('session_id', st.session_state.session_id),
            ttl=0
        )
        
        if result.data:
            # Execute delete with explicit session check
            result = execute_query(
                conn.table("revenue_data")
                .delete()
                .eq('session_id', st.session_state.session_id),
                ttl=0
            )
            return result
        return None
    except Exception as e:
        st.error(f"Error clearing data: {str(e)}")
        raise

def get_mau_data():
    """Get MAU data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("mau_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('month', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('month', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('month'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_wau_data():
    """Get WAU data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("wau_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('week', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('week', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('week'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_dau_data():
    """Get DAU data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("dau_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('day', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('day', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('day'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_mrr_data():
    """Get MRR data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("mrr_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('month', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('month', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('month'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_wrr_data():
    """Get WRR data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("wrr_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('week', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('week', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('week'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_monthly_retention_data():
    """Get monthly retention data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("monthly_retention_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('month', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('month', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('month'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_weekly_retention_data():
    """Get weekly retention data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("weekly_retention_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('week', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('week', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('week'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_daily_retention_data():
    """Get daily retention data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("daily_retention_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('day', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('day', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('day'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_drr_data():
    """Get DRR data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("drr_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('day', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('day', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('day'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_monthly_revenue_retention_data():
    """Get monthly revenue retention data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("monthly_revenue_retention_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('month', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('month', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('month'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_weekly_revenue_retention_data():
    """Get weekly revenue retention data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("weekly_revenue_retention_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('week', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('week', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('week'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_daily_revenue_retention_data():
    """Get daily revenue retention data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("daily_revenue_retention_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('day', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('day', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('day'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_monthly_quick_ratio_data():
    """Get monthly quick ratio data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("monthly_quick_ratio_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('month', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('month', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('month'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_monthly_revenue_quick_ratio_data():
    """Get monthly revenue quick ratio data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("monthly_revenue_quick_ratio_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('month', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('month', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('month'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_weekly_revenue_quick_ratio_data():
    """Get weekly revenue quick ratio data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("weekly_revenue_quick_ratio_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('week', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('week', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('week'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_weekly_quick_ratio_data():
    """Get weekly quick ratio data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("weekly_quick_ratio_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('week', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('week', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('week'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_daily_quick_ratio_data():
    """Get daily quick ratio data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("daily_quick_ratio_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('day', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('day', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('day'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_daily_revenue_quick_ratio_data():
    """Get daily revenue quick ratio data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("daily_revenue_quick_ratio_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('day', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('day', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('day'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_monthly_cohorts_data():
    """Get monthly cohorts data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("monthly_cohorts_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('first_month', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('first_month', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('first_month')
            .order('active_month'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

def get_initial_monthly_data():
    """Get all monthly data for initial load"""
    conn = init_connection()
    
    def paginated_query(query):
        all_data = []
        page_size = 1000
        current_range = 0
        
        while True:
            result = execute_query(
                query.range(current_range, current_range + page_size - 1),
                ttl=0
            )
            
            if not result.data:
                break
                
            all_data.extend(result.data)
            
            if len(result.data) < page_size:
                break
                
            current_range += page_size
        
        result.data = all_data
        return result
    
    # Execute queries sequentially with pagination
    results = {
        'results': paginated_query(
            conn.table("mau_view")
            .select("*")
            .eq('session_id', st.session_state.session_id)
            .order('month')
        ),
        'revenue_results': paginated_query(
            conn.table("mrr_view")
            .select("*")
            .eq('session_id', st.session_state.session_id)
            .order('month')
        ),
        'retention_results': paginated_query(
            conn.table("monthly_retention_view")
            .select("*")
            .eq('session_id', st.session_state.session_id)
            .order('month')
        ),
        'revenue_retention_results': paginated_query(
            conn.table("monthly_revenue_retention_view")
            .select("*")
            .eq('session_id', st.session_state.session_id)
            .order('month')
        ),
        'quick_ratio_results': paginated_query(
            conn.table("monthly_quick_ratio_view")
            .select("*")
            .eq('session_id', st.session_state.session_id)
            .order('month')
        ),
        'revenue_quick_ratio_results': paginated_query(
            conn.table("monthly_revenue_quick_ratio_view")
            .select("*")
            .eq('session_id', st.session_state.session_id)
            .order('month')
        )
    }
    
    # Add cohorts data with proper structure and pagination
    results['cohorts_results'] = paginated_query(
        conn.table("monthly_cohorts_view")
        .select("*")
        .eq('session_id', st.session_state.session_id)
        .order('first_month')
        .order('active_month')
    )
    
    # Add period identifier
    results['period'] = "Monthly"
    
    return results

def get_weekly_cohorts_data():
    """Get weekly cohorts data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("weekly_cohorts_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('first_week', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('first_week', end_date.strftime('%Y-%m-%d'))
    
    return execute_query(
        query.order('first_week').order('active_week'),
        ttl=0
    )

def get_daily_cohorts_data():
    """Get daily cohorts data for current session with date filters"""
    conn = init_connection()
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    query = conn.table("daily_cohorts_view").select("*").eq('session_id', st.session_state.session_id)
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        query = query.gte('first_dt', start_date.strftime('%Y-%m-%d'))
    if end_date and st.session_state.get('filters_applied'):
        query = query.lte('first_dt', end_date.strftime('%Y-%m-%d'))
    
    # Initialize variables for pagination
    all_data = []
    page_size = 1000
    current_range = 0
    
    while True:
        # Get next page of results using range
        result = execute_query(
            query.range(current_range, current_range + page_size - 1)
            .order('first_dt')
            .order('active_day'),
            ttl=0
        )
        
        # If no more data, break
        if not result.data:
            break
            
        # Add this page's data
        all_data.extend(result.data)
        
        # If we got less than a full page, we're done
        if len(result.data) < page_size:
            break
            
        # Move to next page
        current_range += page_size
    
    # Return results in same format as before
    result.data = all_data
    return result

