from __future__ import annotations
import os
from typing import Any, Dict, List
import httpx

AQICN_BASE = "https://api.waqi.info"

class AqicnError(RuntimeError):
    pass

def _token() -> str:
    t = os.getenv("AQICN_TOKEN", "").strip()
    if not t:
        raise AqicnError("Missing AQICN_TOKEN in environment (.env)")
    return t

async def aqicn_map_bounds(min_lat: float, min_lon: float, max_lat: float, max_lon: float) -> List[Dict[str, Any]]:
    """
    List stations in bounding box. Returns items with uid, aqi, lat, lon, station.name
    """
    token = _token()
    url = f"{AQICN_BASE}/map/bounds/"
    params = {"latlng": f"{min_lat},{min_lon},{max_lat},{max_lon}", "token": token}

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
    data = r.json()
    if data.get("status") != "ok":
        raise AqicnError(f"AQICN map/bounds failed: {data}")
    return data.get("data", []) or []

async def aqicn_feed_uid(uid: int) -> Dict[str, Any]:
    """
    Station detail by uid. Includes iaqi + aqi + time + station geo/name.
    """
    token = _token()
    url = f"{AQICN_BASE}/feed/@{int(uid)}/"
    params = {"token": token}

    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
    data = r.json()
    if data.get("status") != "ok":
        raise AqicnError(f"AQICN feed failed: {data}")
    return data.get("data") or {}