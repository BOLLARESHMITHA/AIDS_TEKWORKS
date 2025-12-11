'''

2️⃣ Transform (transform.py)
Each city’s JSON must be flattened into tabular format with one row per hour.
A. Required Columns
city
time
pm10
pm2_5
carbon_monoxide
nitrogen_dioxide
sulphur_dioxide
ozone
uv_index
B. Derived Features (Feature Engineering)
1. AQI based on PM2.5
0–50     → Good
51–100   → Moderate
101–200  → Unhealthy
201–300  → Very Unhealthy
>300     → Hazardous 
2. Pollution Severity Score
Use weighted pollutants:
severity = (pm2_5 * 5) + (pm10 * 3) +
           (nitrogen_dioxide * 4) + (sulphur_dioxide * 4) +
           (carbon_monoxide * 2) + (ozone * 3)
3. Risk Classification
severity > 400 → "High Risk"severity > 200 → "Moderate Risk" else           → "Low Risk" 
4. Temperature Hour-of-Day Feature (Optional)
Extract hour:
hour = time.hour
C. Transform Requirements
Convert timestamps into datetime format
Convert all pollutant values to numeric
Remove records where all pollutant readings are missing
Save transformed data into:
data/staged/air_quality_transformed.csv
 
Docstring for ETL_WEATHER_2APIS.transform
'''

# transform.py
# transform.py

import json
from pathlib import Path
import pandas as pd
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
STAGED_DIR = BASE_DIR / "data" / "staged"

STAGED_DIR.mkdir(parents=True, exist_ok=True)


def flatten_air_quality_json(json_path: str) -> pd.DataFrame:
    """
    Flatten Open-Meteo Air Quality JSON into tabular hourly data.
    Required columns:
    city, time, pm10, pm2_5, carbon_monoxide, nitrogen_dioxide,
    sulphur_dioxide, ozone, uv_index
    """
    with open(json_path, "r") as f:
        payload = json.load(f)

    hourly = payload.get("hourly", {})

    # Extract pollutant arrays
    times = hourly.get("time", [])
    pm10 = hourly.get("pm10", [])
    pm2_5 = hourly.get("pm2_5", [])
    carbon_monoxide = hourly.get("carbon_monoxide", [])
    nitrogen_dioxide = hourly.get("nitrogen_dioxide", [])
    sulphur_dioxide = hourly.get("sulphur_dioxide", [])
    ozone = hourly.get("ozone", [])
    uv_index = hourly.get("uv_index", [])

    # Extract city name from filename
    city = Path(json_path).stem.split("_")[0].title()

    rows = []
    for i, t in enumerate(times):
        rows.append({
            "city": city,
            "time": t,
            "pm10": pm10[i] if i < len(pm10) else None,
            "pm2_5": pm2_5[i] if i < len(pm2_5) else None,
            "carbon_monoxide": carbon_monoxide[i] if i < len(carbon_monoxide) else None,
            "nitrogen_dioxide": nitrogen_dioxide[i] if i < len(nitrogen_dioxide) else None,
            "sulphur_dioxide": sulphur_dioxide[i] if i < len(sulphur_dioxide) else None,
            "ozone": ozone[i] if i < len(ozone) else None,
            "uv_index": uv_index[i] if i < len(uv_index) else None
        })

    return pd.DataFrame(rows)


def pm25_to_aqi_category(pm2_5):
    if pm2_5 is None or pd.isna(pm2_5):
        return None
    if 0 <= pm2_5 <= 50:
        return "Good"
    elif 51 <= pm2_5 <= 100:
        return "Moderate"
    elif 101 <= pm2_5 <= 200:
        return "Unhealthy"
    elif 201 <= pm2_5 <= 300:
        return "Very Unhealthy"
    elif pm2_5 > 300:
        return "Hazardous"
    return None


def compute_severity(row):
    return (
        (row["pm2_5"] or 0) * 5 +
        (row["pm10"] or 0) * 3 +
        (row["nitrogen_dioxide"] or 0) * 4 +
        (row["sulphur_dioxide"] or 0) * 4 +
        (row["carbon_monoxide"] or 0) * 2 +
        (row["ozone"] or 0) * 3
    )


def classify_risk(severity):
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"


def transform_all():
    print("🔄 Transforming all raw air-quality JSON files...")

    json_files = sorted(RAW_DIR.glob("*.json"))
    if not json_files:
        raise SystemExit("❌ No raw JSON files found!")

    dfs = []
    for file in json_files:
        print(f"📌 Processing {file} ...")
        df = flatten_air_quality_json(file)
        dfs.append(df)

    # Merge all cities
    df = pd.concat(dfs, ignore_index=True)

    # Convert time column
    df["time"] = pd.to_datetime(df["time"])

    # Convert pollutants to numeric safely
    pollutant_cols = ["pm10", "pm2_5", "carbon_monoxide",
                      "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index"]

    for col in pollutant_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with all pollutant values missing
    df = df.dropna(subset=pollutant_cols, how="all")

    # Feature Engineering
    df["aqi_category"] = df["pm2_5"].apply(pm25_to_aqi_category)
    df["severity_score"] = df.apply(compute_severity, axis=1)
    df["risk_flag"] = df["severity_score"].apply(classify_risk)
    df["hour"] = df["time"].dt.hour  # optional feature

    # Save transformed CSV
    output_file = STAGED_DIR / "air_quality_transformed.csv"
    df.to_csv(output_file, index=False)

    print(f"✅ Transformed file saved → {output_file}")
    return output_file


if __name__ == "__main__":
    transform_all()
