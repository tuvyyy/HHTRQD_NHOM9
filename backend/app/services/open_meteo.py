# app/services/open_meteo.py
from __future__ import annotations

from typing import Dict, Any

# Wrapper để tương thích code cũ.
# TỪ GIỜ: không gọi Open-Meteo trực tiếp nữa, chỉ gọi service chuẩn openmeteo_service.
from app.services.openmeteo_service import fetch_hourly_async


HOURLY_FIELDS = ["pm2_5", "pm10", "nitrogen_dioxide", "ozone", "carbon_monoxide"]


async def fetch_air_quality(lat: float, lon: float, hours: int) -> Dict[str, Any]:
    """
    Backward-compatible API:
    - Trả về format gần giống Open-Meteo raw (có hourly/time + fields)
    - Nhưng dữ liệu lấy từ openmeteo_service để thống nhất behavior/caching/timeformat.
    """
    pack = await fetch_hourly_async(
        lat=lat,
        lon=lon,
        hours=hours,
        past_days=1,
        timezone="UTC",
        hourly_fields=HOURLY_FIELDS,
    )

    # Trả về giống kiểu data raw hay dùng trước đây
    return {
        "latitude": pack.get("latitude", lat),
        "longitude": pack.get("longitude", lon),
        "timezone": pack.get("timezone", "UTC"),
        "hourly": pack.get("hourly", {}),
    }
