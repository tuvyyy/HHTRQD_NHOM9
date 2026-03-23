from __future__ import annotations
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional

from app.services.aqicn_service import aqicn_feed_uid, AqicnError
from app.services.risk_scoring import compute_score_0_100

router = APIRouter(tags=["Risk Score (Station)"])

class RiskStationReq(BaseModel):
    uid: int
    weights: Dict[str, float]

def _v(iaqi: Dict[str, Any], key: str) -> Optional[float]:
    obj = iaqi.get(key)
    if not isinstance(obj, dict):
        return None
    v = obj.get("v")
    try:
        return float(v)
    except:
        return None

@router.post("/risk/score-station")
async def score_station(req: RiskStationReq):
    try:
        feed = await aqicn_feed_uid(req.uid)
    except AqicnError as e:
        raise HTTPException(status_code=502, detail=str(e))

    iaqi = feed.get("iaqi") or {}
    # AQICN keys: pm25, pm10, no2, o3, co
    pm25 = _v(iaqi, "pm25")
    pm10 = _v(iaqi, "pm10")
    no2  = _v(iaqi, "no2")
    o3   = _v(iaqi, "o3")
    co   = _v(iaqi, "co")

    values_for_scoring = {
        "pm2_5": float(pm25 or 0.0),
        "pm10": float(pm10 or 0.0),
        "no2": float(no2 or 0.0),
        "o3": float(o3 or 0.0),
        "co": float(co or 0.0),
    }

    out = compute_score_0_100(values_for_scoring, req.weights)

    # trả đúng schema FE đang dùng
    latest_values = {
        "pm2_5": pm25,
        "pm10": pm10,
        "nitrogen_dioxide": no2,
        "ozone": o3,
        "carbon_monoxide": co,
    }

    st = feed.get("station") or {}
    geo = st.get("geo") or []

    return {
        "source": "AQICN",
        "uid": req.uid,
        "station_name": st.get("name"),
        "station_geo": geo,  # [lat, lon]
        "aqi": feed.get("aqi"),
        "time": (feed.get("time") or {}).get("s"),
        "latest_values": latest_values,
        "score_0_100": out["score_0_100"],
        "level": out["level"],
        "subscores": out.get("subscores"),
        "weights_normalized": out.get("weights"),
    }