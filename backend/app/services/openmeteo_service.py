# app/services/openmeteo_service.py
from __future__ import annotations

import math
import json
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.services.cache import cache  # cache 10 phút (TTLCache custom)

OPEN_METEO_AIR_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"

HOURLY_FIELDS: List[str] = [
    "pm2_5",
    "pm10",
    "nitrogen_dioxide",
    "ozone",
    "carbon_monoxide",
]


class OpenMeteoError(RuntimeError):
    pass


def _validate_lat_lon(lat: float, lon: float) -> Tuple[float, float]:
    if not (-90 <= lat <= 90):
        raise ValueError("lat must be between -90 and 90")
    if not (-180 <= lon <= 180):
        raise ValueError("lon must be between -180 and 180")
    return float(lat), float(lon)


def _clamp_int(x: Any, default: int, lo: int, hi: int) -> int:
    try:
        v = int(x)
    except Exception:
        v = default
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _forecast_days_from_hours(hours: int) -> int:
    return max(1, int(math.ceil(hours / 24)))


def _normalize_hourly(hourly: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
    times = hourly.get("time") or []
    if not isinstance(times, list) or len(times) == 0:
        raise OpenMeteoError("Open-Meteo response missing hourly.time")

    out = {"time": times}
    n = len(times)
    for f in fields:
        arr = hourly.get(f)
        out[f] = arr if isinstance(arr, list) else [None] * n
    return out


def _cache_key(
    lat: float,
    lon: float,
    hours: int,
    past_days: int,
    timezone: str,
    fields: List[str],
) -> str:
    # round để tránh cache explode vì float noise
    key_obj = {
        "lat": round(lat, 4),
        "lon": round(lon, 4),
        "hours": hours,
        "past_days": past_days,
        "tz": timezone,
        "fields": fields,
    }
    return "openmeteo:" + json.dumps(key_obj, sort_keys=True, ensure_ascii=False)


def fetch_hourly(
    lat: float,
    lon: float,
    hours: int,
    past_days: int = 1,
    timezone: str = "UTC",
    hourly_fields: Optional[List[str]] = None,
) -> Dict[str, Any]:
    lat, lon = _validate_lat_lon(lat, lon)
    hours = _clamp_int(hours, default=24, lo=1, hi=168)
    past_days = _clamp_int(past_days, default=1, lo=0, hi=7)

    fields = hourly_fields or HOURLY_FIELDS
    forecast_days = _forecast_days_from_hours(hours)

    ck = _cache_key(lat, lon, hours, past_days, timezone, fields)
    cached = cache.get(ck)
    if cached is not None:
        return cached

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(fields),
        "timezone": timezone,
        "timeformat": "iso8601",
        "past_days": past_days,
        "forecast_days": forecast_days,
    }

    timeout = httpx.Timeout(20.0, connect=10.0)
    try:
        with httpx.Client(timeout=timeout) as client:
            r = client.get(OPEN_METEO_AIR_URL, params=params)
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPStatusError as e:
        raise OpenMeteoError(f"Open-Meteo HTTP error: {e.response.status_code}") from e
    except Exception as e:
        raise OpenMeteoError(f"Open-Meteo request failed: {e}") from e

    hourly = _normalize_hourly(data.get("hourly") or {}, fields)

    out = {
        "source": "open-meteo",
        "latitude": data.get("latitude", lat),
        "longitude": data.get("longitude", lon),
        "timezone": data.get("timezone", timezone),
        "hours_requested": hours,
        "past_days": past_days,
        "hourly": hourly,
    }

    cache.set(ck, out, ttl=600)
    return out


async def fetch_hourly_async(
    lat: float,
    lon: float,
    hours: int,
    past_days: int = 1,
    timezone: str = "UTC",
    hourly_fields: Optional[List[str]] = None,
) -> Dict[str, Any]:
    lat, lon = _validate_lat_lon(lat, lon)
    hours = _clamp_int(hours, default=24, lo=1, hi=168)
    past_days = _clamp_int(past_days, default=1, lo=0, hi=7)

    fields = hourly_fields or HOURLY_FIELDS
    forecast_days = _forecast_days_from_hours(hours)

    ck = _cache_key(lat, lon, hours, past_days, timezone, fields)
    cached = cache.get(ck)
    if cached is not None:
        return cached

    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(fields),
        "timezone": timezone,
        "timeformat": "iso8601",
        "past_days": past_days,
        "forecast_days": forecast_days,
    }

    timeout = httpx.Timeout(20.0, connect=10.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            r = await client.get(OPEN_METEO_AIR_URL, params=params)
            r.raise_for_status()
            data = r.json()
    except httpx.HTTPStatusError as e:
        raise OpenMeteoError(f"Open-Meteo HTTP error: {e.response.status_code}") from e
    except Exception as e:
        raise OpenMeteoError(f"Open-Meteo request failed: {e}") from e

    hourly = _normalize_hourly(data.get("hourly") or {}, fields)

    out = {
        "source": "open-meteo",
        "latitude": data.get("latitude", lat),
        "longitude": data.get("longitude", lon),
        "timezone": data.get("timezone", timezone),
        "hours_requested": hours,
        "past_days": past_days,
        "hourly": hourly,
    }

    cache.set(ck, out, ttl=600)
    return out
