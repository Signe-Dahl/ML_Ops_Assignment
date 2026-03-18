import sqlite3
from pathlib import Path

import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry


DATA_DIR = Path(__file__).resolve().parent / "data"
DATA_DIR.mkdir(exist_ok=True)  # create folder if it doesn't exist

DB_PATH = DATA_DIR / "weather.db"

API_URL = "https://api.open-meteo.com/v1/forecast"
LOCATIONS = [
    {"name": "Aalborg", "latitude": 57.03, "longitude": 9.55},
    {"name": "Copenhagen", "latitude": 55.68, "longitude": 12.57},
    {"name": "Vodskov", "latitude": 57.10, "longitude": 10.02},
]
HOURLY_VARIABLES = ["temperature_2m", "cloud_cover", "relative_humidity_2m"]
TIMEZONE = "Europe/Copenhagen"
FORECAST_DAYS = 2


def get_client() -> openmeteo_requests.Client:
    """Create an Open-Meteo client with caching and retry support."""
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def init_db(connection: sqlite3.Connection) -> None:
    """Create the forecast table if it does not exist."""
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS forecasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location_name TEXT NOT NULL,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            forecast_datetime TEXT NOT NULL,
            temperature_2m REAL,
            cloud_cover REAL,
            relative_humidity_2m REAL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(location_name, forecast_datetime)
        )
        """
    )
    connection.commit()


def fetch_location_forecast(
    client: openmeteo_requests.Client,
    location: dict,
) -> pd.DataFrame:
    """Fetch tomorrow's hourly forecast data for a single location."""
    params = {
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "hourly": HOURLY_VARIABLES,
        "timezone": TIMEZONE,
        "forecast_days": FORECAST_DAYS,
    }

    response = client.weather_api(API_URL, params=params)[0]
    hourly = response.Hourly()

    data = {
        "forecast_datetime": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        ),
        "location_name": location["name"],
        "latitude": response.Latitude(),
        "longitude": response.Longitude(),
        "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
        "cloud_cover": hourly.Variables(1).ValuesAsNumpy(),
        "relative_humidity_2m": hourly.Variables(2).ValuesAsNumpy(),
    }

    df = pd.DataFrame(data)

    # Convert timestamps to local timezone and keep only tomorrow
    df["forecast_datetime"] = df["forecast_datetime"].dt.tz_convert(TIMEZONE)

    tomorrow = pd.Timestamp.now(tz=TIMEZONE).normalize() + pd.Timedelta(days=1)
    df = df[df["forecast_datetime"].dt.date == tomorrow.date()].copy()

    # Store datetime 
    df["forecast_datetime"] = df["forecast_datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

    return df


def save_forecasts(connection: sqlite3.Connection, dataframe: pd.DataFrame) -> int:
    """Insert forecast rows into SQLite, ignoring duplicates."""
    records = list(
        dataframe[
            [
                "location_name",
                "latitude",
                "longitude",
                "forecast_datetime",
                "temperature_2m",
                "cloud_cover",
                "relative_humidity_2m",
            ]
        ].itertuples(index=False, name=None)
    )

    cursor = connection.executemany(
        """
        INSERT OR IGNORE INTO forecasts (
            location_name,
            latitude,
            longitude,
            forecast_datetime,
            temperature_2m,
            cloud_cover,
            relative_humidity_2m
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        records,
    )
    connection.commit()
    return cursor.rowcount


def main() -> None:
    client = get_client()
    connection = sqlite3.connect(DB_PATH)

    try:
        init_db(connection)

        all_frames = []
        for location in LOCATIONS:
            print(f"Fetching forecast for {location['name']}...")
            df = fetch_location_forecast(client, location)
            all_frames.append(df)

        combined_df = pd.concat(all_frames, ignore_index=True)
        inserted_rows = save_forecasts(connection, combined_df)

        print(f"Saved {inserted_rows} new forecast rows to {DB_PATH.name}")
        print(combined_df.head())
    finally:
        connection.close()


if __name__ == "__main__":
    main()