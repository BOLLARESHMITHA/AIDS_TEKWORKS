'''Urban Air Quality Monitoring – Multi-City ETL Pipeline
A government environmental agency wants to build an automated analytics system that monitors air quality across multiple Indian cities. The agency provides an open, unauthenticated API (no token required) that returns Air Quality Index (AQI) and pollutant information.
You are required to build a complete ETL pipeline (Extract → Transform → Load → Analyze) using Python and Supabase.
🟦 1️⃣ Extract (extract.py)
Use the following public API:
API Endpoint (No Token Needed):
OpenAQ API (Public Open Data):
https://api.openaq.org/v2/latest
Your task
Write code that:
Fetches AQI readings for 5 cities:
Delhi, Bengaluru, Hyderabad, Mumbai, Kolkata
For each city, call the API with a query like:
Save each API response separately inside:
Implement:
Retry logic (3 attempts)
Graceful failure handling
Logging of errors and empty responses
Return list of all saved file paths.
data/raw/cityname_raw_timestamp.json
https://api.openaq.org/v2/latest?city=Delhi'''


# extract.py
import json
from datetime import datetime
from pathlib import Path
import requests

# Output folder
BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# City coordinates for Open-Meteo
CITIES = {
    "Delhi":      (28.6139, 77.2090),
    "Bengaluru":  (12.9716, 77.5946),
    "Hyderabad":  (17.3850, 78.4867),
    "Mumbai":     (19.0760, 72.8777),
    "Kolkata":    (22.5726, 88.3639),
}

BASE_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"


def extract_air_quality(city, lat, lon):
    """
    Calls the Open-Meteo Air Quality API for a city's coordinates
    and saves raw JSON into data/raw/.
    """
    print(f"⏳ Fetching air quality for {city} ...")

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide"
    }

    resp = requests.get(BASE_URL, params=params, timeout=20)
    resp.raise_for_status()

    data = resp.json()

    # Creating timestamped filename
    filename = RAW_DIR / f"{city.lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(data, indent=2))

    print(f"✅ Saved {city} → {filename}\n")
    return filename


def extract_all_cities():
    for city, (lat, lon) in CITIES.items():
        extract_air_quality(city, lat, lon)

    print("🎉 Extraction completed for all 5 cities!")


if __name__ == "__main__":
    extract_all_cities()
