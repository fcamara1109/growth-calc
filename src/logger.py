import streamlit as st
import logging
from datetime import datetime
from st_supabase_connection import SupabaseConnection

class MetricsLogger:
    def __init__(self, supabase: SupabaseConnection):
        self.supabase = supabase

    def log_user_action(self, action: str, tab: str, component: str):
        """Log user actions to metrics table"""
        try:
            result = self.supabase.table("metrics").insert({
                "created_at": datetime.now().isoformat(),
                "action": action,
                "tab": tab,
                "component": component,
                "session_id": st.session_state.session_id
            }).execute()
            return result
        except Exception as e:
            # Silently fail for metrics logging
            pass

    def log_upload(self, file_size: int, processing_time: float, success: bool, error: str = None):
        """Log file upload metrics"""
        try:
            result = self.supabase.table("metrics_uploads").insert({
                "timestamp": datetime.now().isoformat(),
                "session_id": st.session_state.session_id,
                "file_size_bytes": file_size,
                "processing_time_ms": processing_time,
                "success": success,
                "error": error
            }).execute()
            return result
        except Exception as e:
            # Silently fail for metrics logging
            pass

class ErrorLogger:
    def __init__(self, supabase: SupabaseConnection):
        self.supabase = supabase
    
    def log_error(self, error: Exception, context: dict = None):
        """Log errors to errors table"""
        try:
            error_data = {
                "created_at": datetime.now().isoformat(),
                "error_message": str(error),
                "error_type": type(error).__name__,
                "traceback": str(error.__traceback__),
                "session_id": st.session_state.session_id
            }
            
            if context:
                error_data["context"] = context
                
            result = self.supabase.table("errors").insert(error_data).execute()
            return result
        except Exception as e:
            # If error logging fails, print to console as last resort
            print(f"Error logging failed: {str(e)}") 