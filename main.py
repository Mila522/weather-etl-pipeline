import requests
import pandas as pd
from datetime import datetime
import logging
from typing import List, Dict
from sqlalchemy import create_engine, text
import urllib.parse


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


API_URL = "https://api.open-meteo.com/v1/forecast"
LAT = -25.746
LON = 28.188
PARAMS = {
    "latitude": LAT,
    "longitude": LON,
    "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,pressure_msl,precipitation",
    "timezone": "Africa/Johannesburg"
}


SERVER   = r".\SQLEXPRESS"
DATABASE = "WeatherDB"
DRIVER   = "{ODBC Driver 17 for SQL Server}"

master_conn = urllib.parse.quote_plus(
    f"DRIVER={DRIVER};SERVER={SERVER};DATABASE=master;Trusted_Connection=yes;TrustServerCertificate=yes;"
)
engine_master = create_engine(f"mssql+pyodbc:///?odbc_connect={master_conn}")

log.info("Ensuring WeatherDB exists...")
with engine_master.connect() as conn:
    conn.execute(text("IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'WeatherDB') CREATE DATABASE WeatherDB"))
    conn.commit()


db_conn = urllib.parse.quote_plus(
    f"DRIVER={DRIVER};SERVER={SERVER};DATABASE=WeatherDB;Trusted_Connection=yes;TrustServerCertificate=yes;"
)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={db_conn}")

try:
    with engine.connect() as test:
        ver = test.execute(text("SELECT @@VERSION")).scalar()
        log.info("SQL Connected: %s", ver[:60])
except Exception as e:
    log.error("SQL Error: %s", e)
    raise


def extract_weather_data() -> Dict:
    log.info("Fetching from Open-Meteo...")
    r = requests.get(API_URL, params=PARAMS, timeout=10)
    r.raise_for_status()
    data = r.json()
    if "current" not in data:
        raise ValueError("No current weather data")
    log.info("Extracted current weather.")
    return data["current"]


def transform_data(raw: Dict) -> pd.DataFrame:
    now = datetime.now()
    row = {
        "station_name": "Open-Meteo Pretoria",
        "station_code": "OM-PTA",
        "temperature": round(raw["temperature_2m"], 1),
        "humidity": raw["relative_humidity_2m"],
        "wind_speed": round(raw["wind_speed_10m"], 1),
        "wind_direction": None,
        "pressure": round(raw["pressure_msl"], 1),
        "rainfall": raw["precipitation"],
        "measurement_time": datetime.fromisoformat(raw["time"].replace("Z", "+00:00")),
        "etl_run_time": now
    }
    df = pd.DataFrame([row])
    log.info("Transformed 1 record.")
    return df


def load_to_sql(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    with engine.begin() as conn:
        conn.execute(text("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SAWS_Weather')
            BEGIN
                CREATE TABLE SAWS_Weather (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    station_name NVARCHAR(100),
                    station_code NVARCHAR(50),
                    temperature FLOAT,
                    humidity FLOAT,
                    wind_speed FLOAT,
                    wind_direction NVARCHAR(50),
                    pressure FLOAT,
                    rainfall FLOAT,
                    measurement_time DATETIME,
                    etl_run_time DATETIME
                )
            END
        """))
    rows = df.to_sql("SAWS_Weather", engine, if_exists="append", index=False, method="multi")
    log.info("Inserted %d row(s).", rows)
    return rows


if __name__ == "__main__":
    try:
        log.info("=== OPEN-METEO ETL START ===")
        raw = extract_weather_data()
        df = transform_data(raw)
        print("\n--- DATA ---")
        print(df)
        inserted = load_to_sql(df)
        log.info("=== DONE – %d row written ===", inserted)
    except Exception as e:
        log.error("Failed: %s", e)
        raise