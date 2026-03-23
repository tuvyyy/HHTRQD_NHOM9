from __future__ import annotations
from typing import Dict, Optional, List

from app.services.risk_scoring import compute_score_0_100
from app.services.decision_explain import build_explain, build_recommendation


def latest_non_null(arr: List[Optional[float]]) -> Optional[float]:
    for v in reversed(arr):
        if v is not None:
            return float(v)
    return None


def compute_risk_from_hourly(hourly: Dict, weights: Dict[str, float]) -> Dict:
    """
    hourly: dict từ Open-Meteo (hourly arrays)
    weights: dict theo label: {"PM2.5":..., "PM10":..., "NO2":..., "O3":..., "CO":...}
    """
    latest = {}
    for k in ["pm2_5", "pm10", "nitrogen_dioxide", "ozone", "carbon_monoxide"]:
        latest[k] = latest_non_null(hourly.get(k, []))

    # ✅ Dùng chung công thức với Early-Warning
    values_for_scoring = {
        "pm2_5": float(latest.get("pm2_5") or 0.0),
        "pm10": float(latest.get("pm10") or 0.0),
        "no2": float(latest.get("nitrogen_dioxide") or 0.0),
        "o3": float(latest.get("ozone") or 0.0),
        "co": float(latest.get("carbon_monoxide") or 0.0),
    }

    out = compute_score_0_100(values_for_scoring, weights)
    score100 = float(out["score_0_100"])
    level = out["level"]

    # Giữ schema detail tương thích FE/DB cũ:
    subs = out.get("subscores") or {}
    detail_rows = [
        {
            "label": "PM2.5",
            "weight": float(weights.get("PM2.5", 0.0)),
            "value": latest.get("pm2_5"),
            "normalized": round(float(subs.get("pm2_5", 0.0)) / 100.0, 6),
            "subscore_0_100": float(subs.get("pm2_5", 0.0)),
        },
        {
            "label": "PM10",
            "weight": float(weights.get("PM10", 0.0)),
            "value": latest.get("pm10"),
            "normalized": round(float(subs.get("pm10", 0.0)) / 100.0, 6),
            "subscore_0_100": float(subs.get("pm10", 0.0)),
        },
        {
            "label": "NO2",
            "weight": float(weights.get("NO2", 0.0)),
            "value": latest.get("nitrogen_dioxide"),
            "normalized": round(float(subs.get("no2", 0.0)) / 100.0, 6),
            "subscore_0_100": float(subs.get("no2", 0.0)),
        },
        {
            "label": "O3",
            "weight": float(weights.get("O3", 0.0)),
            "value": latest.get("ozone"),
            "normalized": round(float(subs.get("o3", 0.0)) / 100.0, 6),
            "subscore_0_100": float(subs.get("o3", 0.0)),
        },
        {
            "label": "CO",
            "weight": float(weights.get("CO", 0.0)),
            "value": latest.get("carbon_monoxide"),
            "normalized": round(float(subs.get("co", 0.0)) / 100.0, 6),
            "subscore_0_100": float(subs.get("co", 0.0)),
        },
    ]

    latest_values = latest

    explain = build_explain(latest_values, weights, score100)
    recommendation = build_recommendation(level, warning=False)

    return {
        "latest_values": latest_values,
        "detail": detail_rows,
        "score_0_100": score100,
        "level": level,
        "explain": explain,
        "recommendation": recommendation,
    }