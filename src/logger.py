import streamlit as st
import logging
from datetime import datetime
from st_supabase_connection import SupabaseConnection

class ErrorLogger:
    def __init__(self, supabase_client: SupabaseConnection):
        self.client = supabase_client
        
    def log_error(self, error: Exception, context: dict = None):
        self.client.table("error_logs").insert({
            "timestamp": datetime.now().isoformat(),
            "session_id": st.session_state.session_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "stack_trace": getattr(error, '__traceback__', None).__str__(),
            "context": context
        }).execute() 