# app/routers/early_warning.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, List, Optional

from app.services.risk_scoring import compute_score_0_100
from app.services.openmeteo_service import fetch_hourly_async

router = APIRouter(prefix="/alerts", tags=["Alerts"])

DEFAULT_TZ = "Asia/Ho_Chi_Minh"
DEFAULT_OFFSET = "+07:00"


def _iso_with_offset(t: str, tz: str) -> str:
    """
    Open-Meteo thường trả "YYYY-MM-DDTHH:MM" (không offset).
    Chuẩn hoá: thêm Z hoặc +07:00.
    """
    if not isinstance(t, str):
        return t
    # Nếu đã có offset hoặc Z thì giữ nguyên
    tail = t[-6:]
    if t.endswith("Z") or ("+" in tail) or ("-" in tail):
        return t
    if tz in ("UTC", "GMT"):
        return t + "Z"
    if tz == "Asia/Ho_Chi_Minh":
        return t + DEFAULT_OFFSET
    return t


class EarlyWarningRequest(BaseModel):
    lat: float
    lon: float
    hours: int = Field(24, ge=1, le=168)
    weights: Dict[str, float] = Field(default_factory=dict)
    threshold: float = Field(60.0, ge=0.0, le=100.0)
    delta_threshold: float = Field(15.0, ge=0.0, le=100.0)
    delta_window: int = Field(3, ge=1, le=24)


class SeriesPoint(BaseModel):
    time: str
    score_0_100: float
    level: str


class EarlyWarningResponse(BaseModel):
    # ✅ thêm fields để thống nhất schema
    lat: float
    lon: float
    hours: int
    timezone: str

    warning: bool
    reason: str
    threshold: float
    maxScore: float
    maxLevel: str
    timeOfMax: str
    series: List[SeriesPoint]


@router.post("/early-warning", response_model=EarlyWarningResponse)
async def early_warning(req: EarlyWarningRequest):
    # 1) gọi Open-Meteo qua service chuẩn
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

    hourly = pack.get("hourly") or {}
    times = hourly.get("time") or []
    if not times:
        raise HTTPException(status_code=502, detail="Open-Meteo returned no hourly time series")

    tz = pack.get("timezone", DEFAULT_TZ)

    # 2) cắt đúng số giờ
    cut = min(req.hours, len(times))
    times = times[:cut]

    def arr(key: str):
        a = hourly.get(key) or []
        return a[:cut] if isinstance(a, list) else [None] * cut

    pm2_5 = arr("pm2_5")
    pm10 = arr("pm10")
    no2 = arr("nitrogen_dioxide")
    o3 = arr("ozone")
    co = arr("carbon_monoxide")

    # 3) tính score cho từng giờ
    series: List[SeriesPoint] = []
    max_score = -1.0
    max_level = "Xanh"
    time_of_max = times[0]

    scores_only: List[float] = []

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
            SeriesPoint(
                time=_iso_with_offset(times[i], tz),
                score_0_100=s,
                level=lv,
            )
        )

        if s > max_score:
            max_score = s
            max_level = lv
            time_of_max = times[i]

    # 4) rule cảnh báo
    reason = ""
    warning = False

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

    return EarlyWarningResponse(
        lat=req.lat,
        lon=req.lon,
        hours=req.hours,
        timezone=tz,
        warning=warning,
        reason=reason,
        threshold=req.threshold,
        maxScore=float(round(max_score, 2)),
        maxLevel=max_level,
        timeOfMax=_iso_with_offset(time_of_max, tz),
        series=series,
    )
