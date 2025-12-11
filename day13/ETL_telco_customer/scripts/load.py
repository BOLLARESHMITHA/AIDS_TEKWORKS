# ===========================
# load.py (modified)
# ===========================
# Purpose: Load transformed dataset into Supabase using Supabase client
#
import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Initialize Supabase client
def get_supabase_client():
    """Initialize and return Supabase client."""
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    supabase = create_client(url, key)
    print("ℹ️  Supabase client initialized. Skipping raw CREATE TABLE (not supported in all setups).")
    return supabase

# # ------------------------------------------------------
# # Step 1: Create table if not exists (best-effort)
# # ------------------------------------------------------
def create_table_if_not_exists():
    """
    Attempts to create table by calling RPC (if available).
    If RPC is not available, prints CREATE TABLE SQL for manual run in Supabase SQL editor.
    """
    create_table_sql = """
CREATE TABLE IF NOT EXISTS public.telco_customer (
    id BIGSERIAL PRIMARY KEY,
    tenure INTEGER,
    monthlycharges FLOAT,
    totalcharges FLOAT,
    churn TEXT,
    internetservice TEXT,
    contract TEXT,
    paymentmethod TEXT,
    tenure_group TEXT,
    monthly_charge_segment TEXT,
    has_internet_service INTEGER,
    is_multi_line_user INTEGER,
    contract_type_code INTEGER
);
"""
    try:
        supabase = get_supabase_client()
        try:
            # Original approach - many Supabase setups don't expose execute_sql RPC
            supabase.rpc('execute_sql', {'query': create_table_sql}).execute()
            print("✅ Table 'telco_customer' created or already exists (via RPC).")
            return
        except Exception as e:
            # RPC not available — fall back to telling user how to create table
            print(f"ℹ️  Note: {e}")
            print("ℹ️  Could not create table via RPC. Please create the table manually in Supabase SQL editor using the SQL below (or run once in your DB):\n")
            print(create_table_sql)
            print("\nℹ️  Continuing and attempting inserts — if the table doesn't exist with matching lowercased column names, inserts will fail.")
            return
    except Exception as e:
        print(f"⚠️  Error checking/creating table: {e}")
        print("ℹ️  Trying to continue with data insertion...")

# ------------------------------------------------------
# Step 2: Load CSV data into Supabase table
# ------------------------------------------------------
def load_to_supabase(staged_path: str, table_name: str = "telco_customer_data"):

    # Convert to absolute path (relative to script)
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(os.path.join(os.path.dirname(__file__), staged_path))

    print(f"🔍 Looking for data file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"❌ Error: File not found at {staged_path}")
        print("ℹ️  Please run transform.py first to generate the transformed data")
        return

    try:
        # Initialize Supabase client
        supabase = get_supabase_client()

        # Read the CSV fully (staged file) and normalize columns to lowercase
        df = pd.read_csv(staged_path)
        total_rows = len(df)

        # Normalize column names to lowercase to match the CREATE TABLE SQL above
        # (Postgres column names are usually lowercase; your earlier error showed missing 'Churn' column)
        df.columns = [c.strip() for c in df.columns]  # remove stray spaces
        df.columns = [c.lower() for c in df.columns]

        print(f"📊 Loading {total_rows} rows into '{table_name}' (sending lowercased column names)...")

        # Ensure we only send the columns that exist in the target table (best-effort).
        # If you used the CREATE TABLE SQL printed above, expected columns are:
        expected_cols = [
            "tenure",
            "monthlycharges",
            "totalcharges",
            "churn",
            "internetservice",
            "contract",
            "paymentmethod",
            "tenure_group",  # NOTE: transform should produce 'tenure_group' lowercase
            "monthly_charge_segment",
            "has_internet_service",
            "is_multi_line_user",
            "contract_type_code"
        ]
        # Keep intersection of df columns and expected (so we don't send unexpected columns)
        send_cols = [c for c in df.columns if c in expected_cols]
        if not send_cols:
            print("⚠️  Warning: none of the dataframe columns match expected table columns. Check column names.")
            print(f"⚠️  Dataframe columns: {list(df.columns)}")
            print(f"⚠️  Expected (sample): {expected_cols}")
            return

        # Reorder and keep only send_cols
        df_send = df[send_cols].copy()

        # Process in batches
        batch_size = 200  # Reduced batch size for better reliability
        for i in range(0, total_rows, batch_size):
            batch = df_send.iloc[i:i + batch_size].copy()
            # Convert NaN to None for proper NULL handling
            batch = batch.where(pd.notnull(batch), None)
            records = batch.to_dict('records')

            try:
                # Use the standard table insert API
                response = supabase.table(table_name).insert(records).execute()

                # Different client versions return different shapes; handle common ones
                if isinstance(response, dict) and response.get("error"):
                    print(f"⚠️  Error in batch {i//batch_size + 1}: {response.get('error')}")
                else:
                    # If response has 'status_code' or has .error attribute, handle gracefully
                    if hasattr(response, "error") and response.error:
                        print(f"⚠️  Error in batch {i//batch_size + 1}: {response.error}")
                    else:
                        end = min(i + batch_size, total_rows)
                        print(f"✅ Inserted rows {i+1}-{end} of {total_rows}")
            except Exception as e:
                # Many Supabase errors include a dict-like payload; print helpful info
                print(f"⚠️  Error in batch {i//batch_size + 1}: {str(e)}")
                # If it looks like a schema issue, provide guidance
                if "Could not find the" in str(e) or "PGRST204" in str(e):
                    print("ℹ️  This looks like a schema mismatch. Make sure the 'telco_customer' table exists")
                    print("and that column names are lowercased (e.g. 'churn', 'monthlycharges', 'totalcharges').")
                    print("Run the CREATE TABLE SQL printed earlier in the Supabase SQL editor, then re-run this script.")
                continue

        print(f"🎯 Finished loading data into '{table_name}'.")

    except Exception as e:
        print(f"❌ Error loading data: {e}")

# ------------------------------------------------------
# Step 3: Run as standalone script
# ------------------------------------------------------
if __name__ == "__main__":
    # Path relative to the script location - ensure this matches your transform.py output filename
    staged_csv_path = os.path.join("..", "data", "staged", "telco_customers_transformed.csv")
    create_table_if_not_exists()  # Best-effort create (or print SQL to run)
    load_to_supabase(staged_csv_path)