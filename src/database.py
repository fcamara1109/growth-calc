from st_supabase_connection import SupabaseConnection, execute_query
import streamlit as st
import pandas as pd
from time import sleep
from typing import List
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

def init_connection():
    """Initialize Supabase connection"""
    return st.connection("supabase", type=SupabaseConnection)

def create_revenue_table_batch(df_chunk, session_id):
    """Insert a single batch of data with retry logic"""
    max_retries = 5  # Increased retries
    retry_delay = 0.5  # Reduced initial delay
    
    for attempt in range(max_retries):
        try:
            conn = init_connection()
            
            # Prepare data
            df_chunk = df_chunk.copy()
            df_chunk = df_chunk.rename(columns={
                'date': 'transaction_date',
                'id': 'transaction_id'
            })
            df_chunk['session_id'] = session_id  # Use passed session_id
            
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
            sleep(retry_delay * (2 ** attempt))  # True exponential backoff
            continue

def create_revenue_table(df):
    """Insert data into revenue_data table with parallel processing"""
    # Get session_id once before parallel processing
    session_id = st.session_state.session_id
    
    # Much smaller batch size for better parallelization
    batch_size = 1000  # Smaller batches for better distribution
    total_rows = len(df)
    num_batches = math.ceil(total_rows / batch_size)
    
    # Create batches
    batches = []
    for i in range(0, total_rows, batch_size):
        chunk = df[i:min(i + batch_size, total_rows)]
        batches.append(chunk)
    
    # Process batches in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        # Submit all tasks with session_id
        future_to_batch = {
            executor.submit(create_revenue_table_batch, batch, session_id): i 
            for i, batch in enumerate(batches)
        }
        
        # Process completed tasks
        for future in as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                future.result()  # This will raise any exceptions that occurred
            except Exception as e:
                # If a batch fails, raise the error with batch information
                raise Exception(f"Error processing batch {batch_idx}: {str(e)}")
    
    return True

def refresh_views(session_id):
    """Refresh all views for the given session"""
    conn = init_connection()
    
    try:
        # First refresh the non-materialized views by querying them
        views_to_refresh = [
            "mau_view", "wau_view", "dau_view", 
            "mrr_view", "wrr_view", "drr_view"
        ]
        
        for view in views_to_refresh:
            try:
                execute_query(
                    conn.table(view).select("count").eq('session_id', session_id),
                    ttl=0
                )
                sleep(0.5)  # Small delay between views
            except Exception as e:
                st.warning(f"Warning: {view} refresh failed, but continuing... ({str(e)})")
                continue
        
        # Then trigger materialized view refreshes one at a time
        materialized_views = ['daily', 'weekly', 'monthly']
        for view in materialized_views:
            try:
                execute_query(
                    conn.table("refresh_trigger")
                    .insert({
                        "created_at": "now()", 
                        "view_name": view,
                        "session_id": session_id
                    }),
                    ttl=0
                )
                sleep(2)  # Wait between refreshes
            except Exception as e:
                st.warning(f"Warning: {view} refresh failed, but continuing... ({str(e)})")
        
        return True
        
    except Exception as e:
        raise Exception(f"View refresh failed: {str(e)}")

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
    
    # Get filter dates from session state
    start_date = st.session_state.get('period_start_date')
    end_date = st.session_state.get('period_end_date')
    
    # Build base queries with session_id filter
    base_queries = {
        'mau': conn.table("mau_view").select("*").eq('session_id', st.session_state.session_id),
        'mrr': conn.table("mrr_view").select("*").eq('session_id', st.session_state.session_id),
        'retention': conn.table("monthly_retention_view").select("*").eq('session_id', st.session_state.session_id),
        'revenue_retention': conn.table("monthly_revenue_retention_view").select("*").eq('session_id', st.session_state.session_id),
        'quick_ratio': conn.table("monthly_quick_ratio_view").select("*").eq('session_id', st.session_state.session_id),
        'revenue_quick_ratio': conn.table("monthly_revenue_quick_ratio_view").select("*").eq('session_id', st.session_state.session_id),
        'cohorts': conn.table("monthly_cohorts_view").select("*").eq('session_id', st.session_state.session_id)
    }
    
    # Apply date filters if they exist and filters were applied
    if start_date and st.session_state.get('filters_applied'):
        for key in base_queries:
            if key == 'cohorts':
                base_queries[key] = base_queries[key].gte('first_month', start_date.strftime('%Y-%m-%d'))
            else:
                base_queries[key] = base_queries[key].gte('month', start_date.strftime('%Y-%m-%d'))
    
    if end_date and st.session_state.get('filters_applied'):
        for key in base_queries:
            if key == 'cohorts':
                base_queries[key] = base_queries[key].lte('first_month', end_date.strftime('%Y-%m-%d'))
            else:
                base_queries[key] = base_queries[key].lte('month', end_date.strftime('%Y-%m-%d'))
    
    # Add ordering
    for key in base_queries:
        if key == 'cohorts':
            base_queries[key] = base_queries[key].order('first_month').order('active_month')
        else:
            base_queries[key] = base_queries[key].order('month')
    
    # Execute queries and structure results
    results = {
        'results': paginated_query(base_queries['mau']),
        'revenue_results': paginated_query(base_queries['mrr']),
        'retention_results': paginated_query(base_queries['retention']),
        'revenue_retention_results': paginated_query(base_queries['revenue_retention']),
        'quick_ratio_results': paginated_query(base_queries['quick_ratio']),
        'revenue_quick_ratio_results': paginated_query(base_queries['revenue_quick_ratio']),
        'cohorts_results': paginated_query(base_queries['cohorts']),
        'period': "Monthly"
    }
    
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

