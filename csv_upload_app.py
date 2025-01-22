import streamlit as st
import pandas as pd
from uuid import uuid4
from st_supabase_connection import SupabaseConnection
from datetime import datetime
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
from time import sleep

# Page config
st.set_page_config(page_title="Revenue Data Upload", layout="wide")

# Initialize connection
supabase = st.connection("supabase", type=SupabaseConnection)

# Initialize session state
if "session_id" not in st.session_state:
    st.session_state.session_id = uuid4()

def validate_data(df):
    """Validate uploaded CSV data"""
    required_columns = {'date', 'id', 'revenue', 'user_id'}
    
    if not all(col in df.columns for col in required_columns):
        return False, "Missing required columns. Please ensure your CSV has: date, id, revenue, user_id"
    
    # Convert date column to datetime
    try:
        df['transaction_date'] = pd.to_datetime(df['date']).dt.date
        df = df.drop('date', axis=1)  # Remove old date column
    except:
        return False, "Invalid date format in date column"
    
    # Rename id column to transaction_id
    df = df.rename(columns={'id': 'transaction_id'})
    
    # Validate revenue is numeric
    if not pd.to_numeric(df['revenue'], errors='coerce').notnull().all():
        return False, "Revenue column contains non-numeric values"
    
    return True, df

def upload_batch_to_supabase(batch, session_id, batch_idx, total_batches):
    """Upload a single batch with retry logic"""
    max_retries = 5
    retry_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            # Add session_id to all records in batch
            for record in batch:
                record['session_id'] = session_id
            
            # Upload batch
            supabase.table('revenue_data').upsert(batch).execute()
            return True
            
        except Exception as e:
            if attempt == max_retries - 1:
                raise Exception(f"Batch {batch_idx}/{total_batches} failed: {str(e)}")
            sleep(retry_delay * (2 ** attempt))
            continue

def upload_to_supabase(df):
    """Upload dataframe to Supabase with parallel processing"""
    # Optimize data types
    df['transaction_date'] = df['transaction_date'].astype(str)
    df['revenue'] = pd.to_numeric(df['revenue'], downcast='float')
    df['transaction_id'] = df['transaction_id'].astype(str)
    df['user_id'] = df['user_id'].astype(str)
    
    # Prepare data for parallel upload
    batch_size = 1000  # Smaller batches for better distribution
    records = df.to_dict('records')
    total_records = len(records)
    batches = [
        records[i:i + batch_size] 
        for i in range(0, total_records, batch_size)
    ]
    total_batches = len(batches)
    
    # Create progress tracking
    progress_bar = st.progress(0)
    status_text = st.empty()
    timer_text = st.empty()
    start_time = datetime.now()
    completed_records = 0
    
    session_id = str(st.session_state.session_id)
    
    try:
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all batches for parallel processing
            future_to_batch = {
                executor.submit(
                    upload_batch_to_supabase, 
                    batch, 
                    session_id,
                    idx + 1,
                    total_batches
                ): (idx, len(batch))
                for idx, batch in enumerate(batches)
            }
            
            # Process completed batches
            for future in as_completed(future_to_batch):
                batch_idx, batch_size = future_to_batch[future]
                try:
                    future.result()
                    completed_records += batch_size
                    
                    # Update progress
                    progress = completed_records / total_records
                    progress_bar.progress(progress)
                    
                    # Update status
                    elapsed_time = (datetime.now() - start_time).total_seconds()
                    upload_speed = completed_records / elapsed_time if elapsed_time > 0 else 0
                    status_text.text(
                        f"Uploading... {completed_records} of {total_records} records "
                        f"(Speed: {upload_speed:.0f} records/sec)"
                    )
                    timer_text.text(f"Time elapsed: {elapsed_time:.2f} seconds")
                    
                except Exception as e:
                    raise Exception(f"Upload failed: {str(e)}")
        
        # Ensure 100% completion
        final_time = (datetime.now() - start_time).total_seconds()
        final_speed = total_records / final_time if final_time > 0 else 0
        status_text.text(
            f"Completed! {total_records} records uploaded in {final_time:.2f} seconds "
            f"(Avg speed: {final_speed:.0f} records/sec)"
        )
        timer_text.empty()
        
        return True, f"Successfully uploaded {total_records} records in {final_time:.2f} seconds"
        
    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        timer_text.empty()
        return False, f"Error uploading data: {str(e)}"

def main():
    st.title("Revenue Data Upload")
    
    # File uploader
    uploaded_file = st.file_uploader("Upload your CSV file", type=['csv'])
    
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
            st.write("Preview of uploaded data:")
            st.dataframe(df.head())
            
            # Show total records to be uploaded
            st.info(f"Total records to upload: {len(df)}")
            
            # Validate data
            is_valid, result = validate_data(df)
            
            if is_valid:
                if st.button("Upload to Database"):
                    success, message = upload_to_supabase(result)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
            else:
                st.error(result)
                
        except Exception as e:
            st.error(f"Error reading CSV file: {str(e)}")
    
    # Show current session data
    if st.button("View My Uploaded Data"):
        data = supabase.table('revenue_data').select("*").eq('session_id', st.session_state.session_id).execute()
        if data.data:
            st.write("Your uploaded data:")
            df = pd.DataFrame(data.data)
            # Convert date strings back to datetime for display
            df['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
            # Reorder columns for better display
            columns = ['transaction_date', 'transaction_id', 'revenue', 'user_id', 'created_at']
            st.dataframe(df[columns])
        else:
            st.info("No data uploaded in this session yet")
    
    # Clear session data
    if st.button("Clear My Data"):
        supabase.table('revenue_data').delete().eq('session_id', st.session_state.session_id).execute()
        st.success("Data cleared successfully")
        st.rerun()

if __name__ == "__main__":
    main() 