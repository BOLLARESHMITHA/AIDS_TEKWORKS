from supabase import create_client
import os

# Load environment variables (optional: use python-dotenv)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://hvyumvthapjuqkvkoaxg.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh2eXVtdnRoYXBqdXFrdmtvYXhnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTk3NDQ0NzMsImV4cCI6MjA3NTMyMDQ3M30.CaGAhkgX1lvQS-MBc1JRwov1HZYAqEMN1jW_JL6zcWI")

_supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_supabase():
    """Return the Supabase client instance."""
    return _supabase
