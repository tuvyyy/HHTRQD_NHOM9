# app/routers/dss_run.py
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field, conint, confloat
from typing import Any, Dict, List, Optional
import json
import asyncio
from sqlalchemy import text

from app.core.db import engine
from app.services.openmeteo_service import fetch_hourly_async
from app.services.risk import compute_risk_from_hourly
from app.services.risk_scoring import compute_score_0_100

# grid helpers (re-use service)
from app.services.grid import generate_grid_points

router = APIRouter(prefix="/dss", tags=["DSS"])

DEFAULT_TZ = "Asia/Ho_Chi_Minh"
DEFAULT_OFFSET = "+07:00"


def _iso_with_offset(t: str, tz: str) -> str:
    """Chuẩn hóa time string: thêm +07:00 cho Asia/Ho_Chi_Minh nếu chưa có offset."""
    if not isinstance(t, str):
        return t
    tail = t[-6:]
    if t.endswith("Z") or ("+" in tail) or ("-" in tail):
        return t
    if tz in ("UTC", "GMT"):
        return t + "Z"
    if tz == "Asia/Ho_Chi_Minh":
        return t + DEFAULT_OFFSET
    return t


class BBox(BaseModel):
    minLat: confloat(ge=-90, le=90)
    minLon: confloat(ge=-180, le=180)
    maxLat: confloat(ge=-90, le=90)
    maxLon: confloat(ge=-180, le=180)


class RunDSSRequest(BaseModel):
    lat: float
    lon: float
    hours: conint(ge=1, le=168) = 24
    weights: Dict[str, float] = Field(default_factory=dict)

    # early-warning params
    threshold: float = Field(60.0, ge=0.0, le=100.0)
    delta_threshold: float = Field(15.0, ge=0.0, le=100.0)
    delta_window: conint(ge=1, le=24) = 3

    # grid optional
    include_grid: bool = False
    bbox: Optional[BBox] = None
    step_km: confloat(gt=0.1, le=50) = 5.0
    max_points: conint(ge=1, le=5000) = 200


@router.post("/run")
async def run_dss(req: RunDSSRequest):
    """
    Run DSS in ONE call:
    - fetch Open-Meteo hourly once for the point
    - compute risk
    - save AlertHistory
    - compute early-warning series from the same hourly
    - optional: grid-score (bbox)
    """
    # 1) Fetch hourly ONCE for the location
    try:
        pack = await fetch_hourly_async(
            lat=req.lat,
            lon=req.lon,
            hours=req.hours,
            past_days=1,
            timezone=DEFAULT_TZ,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo error: {type(e).__name__}: {e}")

    tz = pack.get("timezone", DEFAULT_TZ)
    hourly = pack.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        raise HTTPException(status_code=502, detail="Open-Meteo returned no hourly time series")

    # 2) Risk score (same logic as /risk/score)
    risk_result = compute_risk_from_hourly(hourly, req.weights)

    # 3) Save DB (same columns as AlertHistory)
    latest = risk_result.get("latest_values", {}) or {}
    detail_rows = risk_result.get("detail", []) or []

    sql = text("""
        INSERT INTO AlertHistory
            (Lat, Lon, Score, Level, PM25, PM10, NO2, O3, CO, WeightsJson, DetailJson)
        OUTPUT INSERTED.Id
        VALUES
            (:Lat, :Lon, :Score, :Level, :PM25, :PM10, :NO2, :O3, :CO, :WeightsJson, :DetailJson);
    """)

    payload = {
        "Lat": float(req.lat),
        "Lon": float(req.lon),
        "Score": float(risk_result["score_0_100"]),
        "Level": risk_result["level"],
        "PM25": latest.get("pm2_5"),
        "PM10": latest.get("pm10"),
        "NO2": latest.get("nitrogen_dioxide"),
        "O3": latest.get("ozone"),
        "CO": latest.get("carbon_monoxide"),
        "WeightsJson": json.dumps(req.weights, ensure_ascii=False),
        "DetailJson": json.dumps(detail_rows, ensure_ascii=False),
    }

    try:
        with engine.begin() as conn:
            saved_id = conn.execute(sql, payload).scalar()
        if saved_id is None:
            raise HTTPException(status_code=500, detail="Insert OK but no inserted Id returned.")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB insert failed: {type(e).__name__}: {e}")

    # 4) Early-warning from SAME hourly (no extra API call)
    cut = min(req.hours, len(times))
    times_cut = times[:cut]

    def arr(key: str) -> List[Optional[float]]:
        a = hourly.get(key) or []
        return a[:cut] if isinstance(a, list) else [None] * cut

    pm2_5 = arr("pm2_5")
    pm10 = arr("pm10")
    no2 = arr("nitrogen_dioxide")
    o3 = arr("ozone")
    co = arr("carbon_monoxide")

    series: List[Dict[str, Any]] = []
    scores_only: List[float] = []

    max_score = -1.0
    max_level = "Xanh"
    time_of_max = times_cut[0]

    for i in range(cut):
        values = {
            "pm2_5": float(pm2_5[i] or 0.0),
            "pm10": float(pm10[i] or 0.0),
            "no2": float(no2[i] or 0.0),
            "o3": float(o3[i] or 0.0),
            "co": float(co[i] or 0.0),
        }
        out = compute_score_0_100(values, req.weights)
        s = float(out["score_0_100"])
        lv = out["level"]

        scores_only.append(s)
        series.append(
            {
                "time": _iso_with_offset(times_cut[i], tz),
                "score_0_100": round(s, 2),
                "level": lv,
            }
        )

        if s > max_score:
            max_score = s
            max_level = lv
            time_of_max = times_cut[i]

    # rules
    warning = False
    reason = ""

    if max_score >= req.threshold:
        warning = True
        reason = f"maxScore >= threshold ({round(max_score,2)} >= {req.threshold})"
    else:
        w = min(req.delta_window, len(scores_only) - 1)
        if w >= 1:
            deltas = []
            start_idx = len(scores_only) - (w + 1)
            for j in range(start_idx + 1, len(scores_only)):
                deltas.append(scores_only[j] - scores_only[j - 1])
            max_delta = max(deltas) if deltas else 0.0
            if max_delta >= req.delta_threshold:
                warning = True
                reason = f"rapid increase detected (max Δ={round(max_delta,2)} >= {req.delta_threshold})"

    if not reason:
        reason = "no rule triggered"

    early_warning = {
        "lat": req.lat,
        "lon": req.lon,
        "hours": req.hours,
        "timezone": tz,
        "warning": warning,
        "reason": reason,
        "threshold": req.threshold,
        "maxScore": round(float(max_score), 2),
        "maxLevel": max_level,
        "timeOfMax": _iso_with_offset(time_of_max, tz),
        "series": series,
    }

    # 5) Optional grid-score (bbox)
    grid = None
    if req.include_grid:
        if req.bbox is None:
            raise HTTPException(status_code=400, detail="include_grid=true requires bbox")

        bbox_dict = req.bbox.model_dump()
        points = generate_grid_points(bbox_dict, step_km=float(req.step_km), max_points=int(req.max_points))
        if not points:
            raise HTTPException(status_code=400, detail="No points generated. Check bbox/step_km.")

        # nhẹ nhàng concurrency (grid demo)
        if len(points) <= 50:
            sem = asyncio.Semaphore(20)
        elif len(points) <= 150:
            sem = asyncio.Semaphore(12)
        else:
            sem = asyncio.Semaphore(8)

        async def score_one(lat: float, lon: float):
            async with sem:
                pack2 = await fetch_hourly_async(
                    lat=lat,
                    lon=lon,
                    hours=req.hours,
                    past_days=1,
                    timezone=DEFAULT_TZ,
                )
                r2 = compute_risk_from_hourly(pack2.get("hourly") or {}, req.weights)
                return {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": {
                        "lat": lat,
                        "lon": lon,
                        "latest_values": r2.get("latest_values", {}),
                        "score_0_100": r2.get("score_0_100"),
                        "level": r2.get("level"),
                    },
                }

        features = await asyncio.gather(*[score_one(lat, lon) for (lat, lon) in points])

        grid = {
            "type": "FeatureCollection",
            "features": features,
            "meta": {
                "count": len(features),
                "step_km": req.step_km,
                "hours": req.hours,
                "bbox": bbox_dict,
            },
        }

    # 6) Response bundle
    score_payload = {
        "lat": req.lat,
        "lon": req.lon,
        "hours": req.hours,
        "timezone": tz,
        **risk_result,
    }

    return {
        "timezone": tz,
        "score": score_payload,
        "saved": {
            "saved_id": int(saved_id),
            "db": "DSS_AirQuality",
            "lat": req.lat,
            "lon": req.lon,
            "hours": req.hours,
            "timezone": tz,
            **risk_result,
        },
        "early_warning": early_warning,
        "grid": grid,  # null nếu include_grid=false
    }
