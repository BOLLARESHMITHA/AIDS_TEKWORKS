# etl_analysis.py
"""
Air Quality ETL - Analysis Stage
--------------------------------
Reads loaded data from Supabase and produces:

A. KPI Metrics
   - City with highest average PM2.5
   - City with highest severity score
   - Percentage of risk levels (High / Moderate / Low)
   - Hour of the day with worst AQI

B. City Pollution Trend Report
   - time vs pm2_5, pm10, ozone for each city

C. Save CSV outputs:
   - summary_metrics.csv
   - city_risk_distribution.csv
   - pollution_trends.csv

D. Save PNG visualizations:
   - Histogram of PM2.5
   - Bar chart of risk flags per city
   - Line chart of hourly PM2.5 trends
   - Scatter plot: severity_score vs pm2_5
"""

import os
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client
import matplotlib.pyplot as plt

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PLOTS_DIR = BASE_DIR / "plots"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ Missing Supabase credentials in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"


def fetch_data_from_supabase() -> pd.DataFrame:
    """Fetch entire Air Quality table."""
    print("📥 Fetching data from Supabase...")
    data = supabase.table(TABLE_NAME).select("*").execute()

    if hasattr(data, "error") and data.error:
        raise RuntimeError(f"Supabase Error: {data.error}")

    df = pd.DataFrame(data.data)

    if df.empty:
        raise SystemExit("❌ No data found in Supabase table.")

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df


def compute_kpi_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Compute summary metrics."""
    print("📊 Computing KPI metrics...")

    metrics = {
        "city_highest_pm25": df.groupby("city")["pm2_5"].mean().idxmax(),
        "city_highest_severity": df.groupby("city")["severity_score"].mean().idxmax(),
        "percent_high_risk": (df[df["risk_flag"] == "High Risk"].shape[0] / df.shape[0]) * 100,
        "percent_mod_risk": (df[df["risk_flag"] == "Moderate Risk"].shape[0] / df.shape[0]) * 100,
        "percent_low_risk": (df[df["risk_flag"] == "Low Risk"].shape[0] / df.shape[0]) * 100,
        "worst_hour_pm25": df.groupby("hour")["pm2_5"].mean().idxmax(),
    }

    df_out = pd.DataFrame([metrics])
    df_out.to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)

    print("✅ Saved: summary_metrics.csv")
    return df_out


def compute_risk_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Risk distribution per city."""
    print("📊 Computing risk distribution...")

    risk_df = (
        df.groupby(["city", "risk_flag"])
        .size()
        .reset_index(name="count")
    )

    risk_df.to_csv(PROCESSED_DIR / "city_risk_distribution.csv", index=False)
    print("✅ Saved: city_risk_distribution.csv")

    return risk_df


def compute_pollution_trends(df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-city pollution trends over time."""
    print("📈 Computing pollution trends...")

    trend_df = df[["city", "time", "pm2_5", "pm10", "ozone"]].sort_values("time")
    trend_df.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)

    print("✅ Saved: pollution_trends.csv")
    return trend_df


def generate_plots(df: pd.DataFrame):
    """Generate PNG visualizations."""
    print("📊 Generating plots...")

    # --- Histogram PM2.5 ---
    plt.figure(figsize=(8, 5))
    plt.hist(df["pm2_5"].dropna(), bins=30)
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5 (µg/m³)")
    plt.ylabel("Frequency")
    plt.savefig(PLOTS_DIR / "hist_pm25.png")
    plt.close()

    # --- Bar chart of risk flags per city ---
    plt.figure(figsize=(10, 6))
    risk_counts = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
    risk_counts.plot(kind="bar", figsize=(12, 6))
    plt.title("Risk Category Count by City")
    plt.ylabel("Count")
    plt.savefig(PLOTS_DIR / "bar_risk_by_city.png")
    plt.close()

    # --- Line chart of hourly PM2.5 trends ---
    plt.figure(figsize=(12, 6))
    for city in df["city"].unique():
        city_df = df[df["city"] == city].sort_values("time")
        plt.plot(city_df["time"], city_df["pm2_5"], label=city)
    plt.legend()
    plt.title("Hourly PM2.5 Trend by City")
    plt.xlabel("Time")
    plt.ylabel("PM2.5 (µg/m³)")
    plt.savefig(PLOTS_DIR / "line_pm25_trends.png")
    plt.close()

    # --- Scatter severity vs PM2.5 ---
    plt.figure(figsize=(8, 5))
    plt.scatter(df["pm2_5"], df["severity_score"], alpha=0.5)
    plt.title("Severity Score vs PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.savefig(PLOTS_DIR / "scatter_severity_vs_pm25.png")
    plt.close()

    print("🎨 All plots saved!")


def run_analysis():
    df = fetch_data_from_supabase()
    compute_kpi_metrics(df)
    compute_risk_distribution(df)
    compute_pollution_trends(df)
    generate_plots(df)
    print("🎉 Analysis completed successfully.")


if __name__ == "__main__":
    run_analysis()
