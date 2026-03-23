# app/routers/risk_grid.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, confloat, conint
from typing import Dict, List, Any, Optional
import asyncio

from app.services.grid import generate_grid_points
from app.services.openmeteo_service import fetch_hourly_async, HOURLY_FIELDS, OpenMeteoError
from app.services.risk_scoring import compute_score_0_100

router = APIRouter(prefix="/risk", tags=["risk-grid"])


class BBox(BaseModel):
    minLat: confloat(ge=-90, le=90)
    minLon: confloat(ge=-180, le=180)
    maxLat: confloat(ge=-90, le=90)
    maxLon: confloat(ge=-180, le=180)


class GridScoreRequest(BaseModel):
    bbox: BBox
    step_km: confloat(gt=0.1, le=50) = 2.0
    max_points: conint(ge=1, le=5000) = 500
    hours: conint(ge=1, le=168) = 24
    weights: Dict[str, confloat(ge=0.0, le=1.0)]


def latest_non_null(arr: List[Optional[float]]) -> Optional[float]:
    for v in reversed(arr):
        if v is not None:
            return float(v)
    return None


def extract_latest(hourly: Dict[str, Any]) -> Dict[str, Optional[float]]:
    latest = {}
    for k in HOURLY_FIELDS:
        latest[k] = latest_non_null(hourly.get(k, []))
    return latest


@router.post("/grid-score")
async def grid_score(req: GridScoreRequest):
    bbox_dict = req.bbox.model_dump()
    points = generate_grid_points(bbox_dict, step_km=float(req.step_km), max_points=int(req.max_points))
    if not points:
        raise HTTPException(status_code=400, detail="No points generated. Check bbox/step_km.")

    if len(points) <= 50:
        sem = asyncio.Semaphore(20)
    elif len(points) <= 150:
        sem = asyncio.Semaphore(12)
    else:
        sem = asyncio.Semaphore(8)

    async def score_one(lat: float, lon: float):
        async with sem:
            try:
                pack = await fetch_hourly_async(lat, lon, req.hours, past_days=0, timezone="UTC")
            except (OpenMeteoError, ValueError) as e:
                raise HTTPException(status_code=502, detail=f"Open-Meteo error: {type(e).__name__}: {e}")

            hourly = pack.get("hourly", {}) or {}
            latest_values = extract_latest(hourly)

            values_for_scoring = {
                "pm2_5": float(latest_values.get("pm2_5") or 0.0),
                "pm10": float(latest_values.get("pm10") or 0.0),
                "no2": float(latest_values.get("nitrogen_dioxide") or 0.0),
                "o3": float(latest_values.get("ozone") or 0.0),
                "co": float(latest_values.get("carbon_monoxide") or 0.0),
            }
            out = compute_score_0_100(values_for_scoring, req.weights)
            score100 = float(out["score_0_100"])
            level = out["level"]

            return {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "lat": lat,
                    "lon": lon,
                    "latest_values": latest_values,
                    "score_0_100": score100,
                    "level": level,
                },
            }

    tasks = [score_one(lat, lon) for (lat, lon) in points]
    features = await asyncio.gather(*tasks)

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "count": len(features),
            "step_km": req.step_km,
            "hours": req.hours,
            "bbox": bbox_dict,
        },
    }