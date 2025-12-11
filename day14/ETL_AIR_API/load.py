'''

3️⃣ Load (load.py) – Supabase
Create table:
air_quality_data (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    time TIMESTAMP,
    pm10 FLOAT,
    pm2_5 FLOAT,
    carbon_monoxide FLOAT,
    nitrogen_dioxide FLOAT,
    sulphur_dioxide FLOAT,
    ozone FLOAT,
    uv_index FLOAT,
    aqi_category TEXT,
    severity_score FLOAT,
    risk_flag TEXT,
    hour INTEGER
)
Load Requirements
Batch insert records (batch size = 200)
Auto-convert NaN → NULL
Convert datetime to ISO formatted strings
Retry failed batches (2 retries)
Print summary of inserted rows
🟩 4️⃣ Analysis (etl_analysis.py)
Read the loaded data from Supabase and perform:
A. KPI Metrics
City with highest average PM2.5
City with the highest severity score
Percentage of High/Moderate/Low risk hours
Hour of day with worst AQI
B. City Pollution Trend Report
For each city:
time → pm2_5, pm10, ozone
C. Export Outputs
Save the following CSVs into data/processed/:
summary_metrics.csv
city_risk_distribution.csv
pollution_trends.csv
D. Visualizations
Save the following PNG plots:
Histogram of PM2.5
Bar chart of risk flags per city
Line chart of hourly PM2.5 trends
Scatter: severity_score vs pm2_5
 
Docstring for ETL_WEATHER_2APIS.load
'''

# load.py

import os
from pathlib import Path
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client
from time import sleep

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ Please set SUPABASE_URL and SUPABASE_KEY in your .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"


def clean_inf_nan(df: pd.DataFrame) -> pd.DataFrame:
    """Remove ALL invalid JSON values so Supabase will accept."""
    # Replace Python/numpy infinities
    df = df.replace([np.inf, -np.inf, float("inf"), float("-inf")], None)

    # Convert NaN → None
    df = df.where(pd.notnull(df), None)

    # Replace string versions of Infinity if present
    df = df.replace(["Infinity", "-Infinity", "inf", "-inf"], None)

    return df


def create_table_if_not_exists():
    print("🔧 Checking table creation...")
    print("➡️ RPC skipped (Supabase disallows execute_sql).")
    print("👉 Create this table manually inside Supabase SQL Editor:\n")

    print("""
CREATE TABLE IF NOT EXISTS public.air_quality_data (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    time TIMESTAMP,
    pm10 FLOAT,
    pm2_5 FLOAT,
    carbon_monoxide FLOAT,
    nitrogen_dioxide FLOAT,
    sulphur_dioxide FLOAT,
    ozone FLOAT,
    uv_index FLOAT,
    aqi_category TEXT,
    severity_score FLOAT,
    risk_flag TEXT,
    hour INTEGER
);
""")


def load_to_supabase(staged_csv_path: str, batch_size: int = 200):

    df = pd.read_csv(staged_csv_path)

    # Convert timestamps
    df["time"] = pd.to_datetime(df["time"], errors="coerce").astype(str)

    # CLEAN DATA COMPLETELY
    df = clean_inf_nan(df)

    # Convert to list of dictionaries
    records = df.to_dict(orient="records")

    total = len(records)
    print(f"📦 Loading {total} records into Supabase...\n")

    successful = 0

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]

        # CLEAN AGAIN at row level to prevent unexpected errors
        for row in batch:
            for key, value in row.items():
                if isinstance(value, float) and (np.isnan(value)):
                    row[key] = None

        retry = 0
        while retry < 3:
            try:
                supabase.table(TABLE_NAME).insert(batch).execute()
                print(f"✅ Inserted rows {i+1}-{min(i+batch_size, total)}")
                successful += len(batch)
                break

            except Exception as e:
                print(f"⚠️ Batch {i//batch_size + 1} failed: {e}")
                retry += 1
                if retry < 3:
                    print("⏳ Retrying...")
                    sleep(2)
                else:
                    print("❌ Giving up on this batch.\n")

    print("\n🎯 LOAD SUMMARY")
    print(f"Total Rows:      {total}")
    print(f"Successfully Loaded: {successful}")
    print(f"Failed:              {total - successful}")
    print("\n🚀 Load stage complete.")
    

if __name__ == "__main__":
    staged_files = sorted([str(p) for p in STAGED_DIR.glob("air_quality_transformed.csv")])
    if not staged_files:
        raise SystemExit("❌ No staged CSV found.")

    create_table_if_not_exists()
    load_to_supabase(staged_files[-1])
