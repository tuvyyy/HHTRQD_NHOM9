# app/routers/risk.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict

from app.services.risk import compute_risk_from_hourly
from app.services.openmeteo_service import fetch_hourly

router = APIRouter(tags=["Risk Score"])

DEFAULT_TZ = "Asia/Ho_Chi_Minh"


class RiskRequest(BaseModel):
    lat: float
    lon: float
    weights: Dict[str, float]
    hours: int = Field(default=24, ge=1, le=168)


@router.post("/risk/score")
def risk_score(req: RiskRequest):
    try:
        pack = fetch_hourly(
            req.lat,
            req.lon,
            req.hours,
            past_days=1,
            timezone=DEFAULT_TZ,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo error: {type(e).__name__}: {e}")

    result = compute_risk_from_hourly(pack["hourly"], req.weights)

    # ✅ schema thống nhất: luôn có lat/lon/hours/timezone + result
    return {
        "lat": req.lat,
        "lon": req.lon,
        "hours": req.hours,
        "timezone": pack.get("timezone", DEFAULT_TZ),
        **result,
    }
