from fastapi import APIRouter, Query, HTTPException
import httpx
from datetime import datetime

router = APIRouter(tags=["Air Quality (Open-Meteo)"])

OPEN_METEO_AQ = "https://air-quality-api.open-meteo.com/v1/air-quality"

# Map tên biến hệ thống của bạn -> open-meteo field
HOURLY_FIELDS = [
    "pm2_5",
    "pm10",
    "nitrogen_dioxide",
    "ozone",
    "carbon_monoxide",
]

@router.get("/air-quality")
async def get_air_quality(
    lat: float = Query(...),
    lon: float = Query(...),
    hours: int = Query(24, ge=1, le=168),  # tối đa 7 ngày demo
):
    """
    Return hourly air-quality series from Open-Meteo.
    Shape trả về:
    {
      "lat":..,"lon":..,"hours":..,
      "time": [...],
      "hourly": {
         "pm2_5":[...],
         "pm10":[...],
         "no2":[...],
         "o3":[...],
         "co":[...]
      },
      "units": {...}
    }
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(HOURLY_FIELDS),
        "timezone": "UTC",
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(OPEN_METEO_AQ, params=params)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo error: {e}")

    hourly = data.get("hourly") or {}
    time_list = hourly.get("time") or []
    if not time_list:
        raise HTTPException(status_code=502, detail="Open-Meteo returned no hourly time series")

    # Cắt đúng số giờ yêu cầu (lấy từ đầu danh sách)
    cut = min(hours, len(time_list))

    def cut_list(key: str):
        arr = hourly.get(key) or []
        return arr[:cut] if isinstance(arr, list) else []

    out = {
"lat": lat,
"lon": lon,
"hours": hours,
"time": time_list[:cut],
"hourly": {
"time": time_list[:cut],
"pm2_5": cut_list("pm2_5"),
"pm10": cut_list("pm10"),
"no2": cut_list("nitrogen_dioxide"),
"o3": cut_list("ozone"),
"co": cut_list("carbon_monoxide"),
},
"units": (data.get("hourly_units") or {}),
}
    return out
