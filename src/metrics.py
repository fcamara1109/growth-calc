from datetime import datetime
import streamlit as st
from st_supabase_connection import SupabaseConnection

class MetricsLogger:
    def __init__(self, supabase_client: SupabaseConnection):
        self.client = supabase_client
        
    def log_upload(self, file_size: int, processing_time: float, success: bool, error: str = None):
        self.client.table("metrics_uploads").insert({
            "timestamp": datetime.now().isoformat(),
            "session_id": st.session_state.session_id,
            "file_size_bytes": file_size,
            "processing_time_ms": processing_time * 1000,
            "success": success,
            "error": error
        }).execute()