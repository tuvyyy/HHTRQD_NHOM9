# app/routers/risk_save.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict
import json
from sqlalchemy import text

from app.core.db import engine
from app.services.risk import compute_risk_from_hourly
from app.services.openmeteo_service import fetch_hourly

router = APIRouter(tags=["Risk Save"])

DEFAULT_TZ = "Asia/Ho_Chi_Minh"


class RiskSaveRequest(BaseModel):
    lat: float
    lon: float
    weights: Dict[str, float]
    hours: int = Field(default=24, ge=1, le=168)  # ✅ thêm hours để thống nhất schema


@router.post("/risk/score-and-save")
def score_and_save(req: RiskSaveRequest):
    # 1) Lấy dữ liệu Open-Meteo (chuẩn hoá: dùng openmeteo_service)
    try:
        pack = fetch_hourly(
            lat=req.lat,
            lon=req.lon,
            hours=req.hours,
            past_days=1,
            timezone=DEFAULT_TZ,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Open-Meteo error: {type(e).__name__}: {e}")

    hourly = pack.get("hourly", {}) or {}

    # 2) Tính risk score (đã kèm explain + recommendation)
    result = compute_risk_from_hourly(hourly, req.weights)

    latest = result.get("latest_values", {}) or {}
    detail_rows = result.get("detail", []) or []

    # 3) Lưu DB
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
        "Score": float(result["score_0_100"]),
        "Level": result["level"],
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
            new_id = conn.execute(sql, payload).scalar()

        if new_id is None:
            raise HTTPException(status_code=500, detail="Insert OK but no inserted Id returned.")

        tz = pack.get("timezone", DEFAULT_TZ)

        # ✅ schema thống nhất:
        # - top-level có lat/lon/hours/timezone + result fields
        # - vẫn giữ "result" để FE cũ không gãy
        return {
            "saved_id": int(new_id),
            "db": "DSS_AirQuality",
            "lat": req.lat,
            "lon": req.lon,
            "hours": req.hours,
            "timezone": tz,
            **result,
            "result": {
                "lat": req.lat,
                "lon": req.lon,
                "hours": req.hours,
                "timezone": tz,
                **result
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB insert failed: {type(e).__name__}: {e}")
