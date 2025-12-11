'''VALIDATION SCRIPT (validate.py)
After load, write a script that checks:
No missing values in:
tenure, MonthlyCharges, TotalCharges
Unique count of rows = original dataset
Row count matches Supabase table
All segments (tenure_group, monthly_charge_segment) exist
Contract codes are only {0,1,2}
Print a validation summary.'''

import os
import pandas as pd
import numpy as np
from supabase import create_client
from dotenv import load_dotenv

def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


# ----------------------------------------
# Load dataset from Supabase
# ----------------------------------------
def load_dataset():
    supabase = get_supabase_client()
    response = supabase.table("telco_customer_data").select("*").execute()
    df = pd.DataFrame(response.data)
    return df


# ----------------------------------------
# Validation checks
# ----------------------------------------
def run_validation(df: pd.DataFrame):


    # ----------------------------------------
    # 1. Missing value checks
    # ----------------------------------------
    missing_tenure = df[df["tenure"].isna()]
    missing_mc = df[df["monthlycharges"].isna()]
    missing_tc = df[df["totalcharges"].isna()]

    print("🔹 Missing Tenure:", len(missing_tenure))
    print("🔹 Missing MonthlyCharges:", len(missing_mc))
    print("🔹 Missing TotalCharges:", len(missing_tc))

    if len(missing_tenure) == 0 and len(missing_mc) == 0 and len(missing_tc) == 0:
        print("✅ No missing required fields.\n")
    else:
        print("⚠️ Missing values detected! Fix required.\n")

    # ----------------------------------------
    # 2. Duplicate ID check
    # ----------------------------------------
    unique_ok = df["id"].unique() == len(df)
    if unique_ok:
        print("✅ All IDs are unique.")
    else:
        print("❌ Duplicate IDs found!")

    # ----------------------------------------
    # 3. Required columns exist
    # ----------------------------------------
    required_cols = ["tenure_group", "monthly_charge_segment"]

    missing = [c for c in required_cols if c not in df.columns]

    if len(missing) == 0:
        print("✅ All required segment columns exist.")
    else:
        print("❌ Missing columns:", missing)

    # ----------------------------------------
    # 4. Contract code validation {0,1,2}
    # ----------------------------------------
    valid_codes = {0, 1, 2}

    invalid = set(df["contract_type_code"].unique()) - valid_codes

    print("\n🔍 Contract type codes found:", df["contract_type_code"].unique())

    if len(invalid) == 0:
        print("✅ Contract codes valid (only 0,1,2 used).")
    else:
        print("❌ Invalid contract codes found:", invalid)

    # ----------------------------------------
    # 5. Row count summary
    # ----------------------------------------
    print("\n📌 Total rows in Supabase:", len(df))

if __name__ == "__main__":
    df = load_dataset()
    run_validation(df)
