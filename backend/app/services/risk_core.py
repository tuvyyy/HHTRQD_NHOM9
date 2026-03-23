# app/services/risk_core.py
from __future__ import annotations
from typing import Dict, Any, Optional, Tuple, List

# Ngưỡng demo (để ra điểm 0..1). Bro có thể chỉnh sau theo tài liệu/chuẩn.
# Ý tưởng: value <= good => 0 (tốt), value >= bad => 1 (xấu), nội suy tuyến tính giữa.
THRESHOLDS = {
    "pm2_5": (15.0, 55.0),               # µg/m3 (demo)
    "pm10": (45.0, 155.0),               # µg/m3 (demo)
    "nitrogen_dioxide": (40.0, 200.0),   # µg/m3 (demo)
    "ozone": (80.0, 180.0),              # µg/m3 (demo)
    "carbon_monoxide": (4000.0, 15000.0) # µg/m3 (demo)
}

def clamp01(x: float) -> float:
    return 0.0 if x < 0 else 1.0 if x > 1 else x

def normalize(value: float, good: float, bad: float) -> float:
    if bad == good:
        return 0.0
    return clamp01((value - good) / (bad - good))

def level_from_score(score_0_100: float) -> str:
    if score_0_100 < 25:
        return "Xanh"
    if score_0_100 < 50:
        return "Vàng"
    if score_0_100 < 75:
        return "Cam"
    return "Đỏ"

def compute_risk_score(
    latest_values: Dict[str, Optional[float]],
    weights: Dict[str, float]
) -> Tuple[float, str, Dict[str, Any]]:
    """
    latest_values: {
      "pm2_5": float|None,
      "pm10": float|None,
      "nitrogen_dioxide": float|None,
      "ozone": float|None,
      "carbon_monoxide": float|None
    }

    weights: dict theo LABEL (đúng như code hiện tại của bạn):
      {"PM2.5":0.49,"PM10":...,"NO2":...,"O3":...,"CO":...}

    return: (score_0_100, level, detail_dict)
    """
    label_to_key = {
        "PM2.5": "pm2_5",
        "PM10": "pm10",
        "NO2": "nitrogen_dioxide",
        "O3": "ozone",
        "CO": "carbon_monoxide"
    }

    # Chuẩn hoá từng tiêu chí về 0..1
    norm: Dict[str, Optional[float]] = {}
    for key, val in latest_values.items():
        if val is None:
            norm[key] = None
            continue
        good, bad = THRESHOLDS[key]
        norm[key] = normalize(float(val), good, bad)

    # Tính điểm tổng theo AHP
    total = 0.0
    used = 0.0
    detail_rows: List[Dict[str, Any]] = []

    for label, key in label_to_key.items():
        w = float(weights.get(label, 0.0))
        v = norm.get(key)
        if v is None:
            detail_rows.append({"label": label, "weight": w, "value": latest_values.get(key), "normalized": None})
            continue
        total += w * v
        used += w
        detail_rows.append({"label": label, "weight": w, "value": latest_values.get(key), "normalized": round(v, 6)})

    score01 = (total / used) if used > 0 else 0.0
    score100 = round(score01 * 100, 2)
    level = level_from_score(score100)

    detail = {
        "latest_values": latest_values,
        "detail": detail_rows
    }
    return score100, level, detail
