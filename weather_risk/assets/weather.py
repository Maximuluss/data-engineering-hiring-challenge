import json
from datetime import datetime, timezone
import time

import requests
from dagster import AssetExecutionContext, Failure, MaterializeResult, MetadataValue, asset
from dagster_duckdb import DuckDBResource

# Open-Meteo forecast API docs: https://open-meteo.com/en/docs
# Endpoint: https://api.open-meteo.com/v1/forecast
# No API key required.

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

HOURLY = [
    "weather_code",               # instant, WMO code
    "temperature_2m",             # instant, °C
    "precipitation",              # preceding-hour sum, mm
    "precipitation_probability",  # preceding-hour probability of >0.1 mm, %
    "wind_gusts_10m",             # preceding-hour max, km/h
]

MAX_ATTEMPTS = 3

def fetch_forecast(latitude: float, longitude: float, tz: str) -> dict:
    """Call Open-Meteo for one location's hourly forecast. Retry network errors, 429 and 5xx.

    With `timeformat=unixtime` every timestamp is seconds since epoch in UTC, so the
    hours are unambiguous even across DST changes (Open-Meteo ignores DST inside a
    response for iso8601 local times). `timezone` still makes the series start at
    local midnight today, so the first local day is complete when dbt aggregates it.
    """
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(HOURLY),
        "timezone": tz,
        "forecast_days": 16,
        "timeformat": "unixtime",
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = requests.get(FORECAST_URL, params=params, timeout=30)
            if resp.status_code == 429 or resp.status_code >= 500:
                raise requests.HTTPError(f"retryable status {resp.status_code}")
            resp.raise_for_status()   # 400 etc. are not retried
            return {"url": resp.url, "body": resp.json()}
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            retryable = not (
                isinstance(exc, requests.HTTPError)
                and exc.response is not None
                and 400 <= exc.response.status_code < 500
                and exc.response.status_code != 429
            )
            if attempt == MAX_ATTEMPTS or not retryable:
                raise
            time.sleep(2 ** attempt)  # 2s, 4s

@asset
def raw_weather_forecast(context: AssetExecutionContext, duckdb: DuckDBResource):
    """
    Fetch the 16-day hourly forecast from Open-Meteo for every location.
    Daily aggregation (cut at each location's local midnight) happens in dbt.
    Store results in DuckDB as raw.raw_weather_forecast (append-only: one row per
    location per fetch, so every forecast is kept).

    Read the API docs carefully — some parameters affect the correctness
    of the daily forecast data, not just what fields are returned.
    """
    fetched_at = datetime.now(timezone.utc)

    with duckdb.get_connection() as con:
        con.execute("create schema if not exists raw")
        con.execute("""
            create table if not exists raw.raw_weather_forecast (
                location_id varchar,
                fetched_at  timestamptz,
                request_url varchar,
                response    json
            )
        """)
        locations = con.execute(
            "select location_id, latitude::double, longitude::double, timezone from stg.stg_locations"
        ).fetchall()

        failures = []
        saved = 0
        for location_id, lat, lng, tz in locations:
            try:
                result = fetch_forecast(lat, lng, tz)
            except Exception as exc:  # keep going; report at the end
                context.log.error(f"{location_id}: {exc}")
                failures.append(location_id)
                continue
            con.execute(
                "insert into raw.raw_weather_forecast values (?, ?, ?, ?)",
                [location_id, fetched_at, result["url"], json.dumps(result["body"])],
            )
            saved += 1

    if failures:
        raise Failure(f"{saved} saved, {len(failures)} failed: {failures}")

    return MaterializeResult(
        metadata={"locations_fetched": saved, "fetched_at": MetadataValue.text(fetched_at.isoformat())}
    )
