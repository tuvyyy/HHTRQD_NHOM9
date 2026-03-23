from __future__ import annotations
from typing import Dict, Tuple

# ---- (1) Breakpoints để đổi pollutant -> subscore 0..100
# Bạn có thể chỉnh lại cho giống công thức hiện tại của bạn nếu đang dùng.
# Đây là bộ ngưỡng demo "dễ hiểu + ổn khi làm báo cáo".

def _piecewise_score(x: float, bps: Tuple[Tuple[float, float], ...]) -> float:
    """
    bps: ((value, score), ...), value tăng dần.
    Nội suy tuyến tính giữa các mốc.
    """
    if x is None:
        return 0.0
    x = float(x)

    # dưới mốc đầu
    if x <= bps[0][0]:
        return float(bps[0][1])

    # giữa các mốc
    for i in range(1, len(bps)):
        x0, s0 = bps[i - 1]
        x1, s1 = bps[i]
        if x <= x1:
            # linear interpolation
            t = (x - x0) / (x1 - x0) if x1 != x0 else 0.0
            return float(s0 + t * (s1 - s0))

    # trên mốc cuối
    return float(bps[-1][1])


def pollutant_subscores(values: Dict[str, float]) -> Dict[str, float]:
    """
    values keys: pm2_5, pm10, no2, o3, co
    units (Open-Meteo thường):
      pm2_5, pm10: µg/m³
      no2, o3: µg/m³
      co: µg/m³
    """
    pm25 = values.get("pm2_5", 0.0)
    pm10 = values.get("pm10", 0.0)
    no2 = values.get("no2", 0.0)
    o3 = values.get("o3", 0.0)
    co = values.get("co", 0.0)

    # Các ngưỡng demo (có thể chỉnh):
    s_pm25 = _piecewise_score(pm25, ((0, 0), (12, 20), (35, 50), (55, 75), (150, 100)))
    s_pm10 = _piecewise_score(pm10, ((0, 0), (25, 20), (50, 50), (100, 75), (200, 100)))
    s_no2  = _piecewise_score(no2,  ((0, 0), (40, 20), (80, 50), (150, 75), (300, 100)))
    s_o3   = _piecewise_score(o3,   ((0, 0), (60, 20), (120, 50), (180, 75), (240, 100)))
    s_co   = _piecewise_score(co,   ((0, 0), (2000, 20), (4000, 50), (8000, 75), (15000, 100)))

    return {
        "pm2_5": round(s_pm25, 2),
        "pm10": round(s_pm10, 2),
        "no2": round(s_no2, 2),
        "o3": round(s_o3, 2),
        "co": round(s_co, 2),
    }


def normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    """
    weights input keys bạn đang dùng: PM2.5, PM10, NO2, O3, CO hoặc pm2_5,... tùy FE
    -> chuẩn hóa về pm2_5, pm10, no2, o3, co
    """
    # nhận cả 2 kiểu key
    mapping = {
        "pm2_5": "pm2_5",
        "pm25": "pm2_5",
        "PM2.5": "pm2_5",

        "pm10": "pm10",
        "PM10": "pm10",

        "no2": "no2",
        "NO2": "no2",

        "o3": "o3",
        "O3": "o3",

        "co": "co",
        "CO": "co",
    }

    w = {"pm2_5": 0.0, "pm10": 0.0, "no2": 0.0, "o3": 0.0, "co": 0.0}
    for k, v in (weights or {}).items():
        kk = mapping.get(k)
        if kk:
            try:
                w[kk] = float(v)
            except:
                w[kk] = 0.0

    s = sum(w.values())
    if s <= 0:
        # fallback weight đều
        return {"pm2_5": 0.2, "pm10": 0.2, "no2": 0.2, "o3": 0.2, "co": 0.2}
    return {k: (v / s) for k, v in w.items()}


def compute_score_0_100(values: Dict[str, float], weights: Dict[str, float]) -> Dict:
    """
    Output:
      { "score_0_100": float, "level": str, "subscores": {...}, "weights": {...} }
    """
    w = normalize_weights(weights)
    subs = pollutant_subscores(values)
    score = (
        subs["pm2_5"] * w["pm2_5"]
        + subs["pm10"] * w["pm10"]
        + subs["no2"] * w["no2"]
        + subs["o3"] * w["o3"]
        + subs["co"] * w["co"]
    )
    score = float(round(score, 2))
    level = level_from_score(score)
    return {"score_0_100": score, "level": level, "subscores": subs, "weights": w}


def level_from_score(score: float) -> str:
    # Bạn đang dùng Xanh/Vàng/Cam/Đỏ
    if score < 25:
        return "Xanh"
    if score < 50:
        return "Vàng"
    if score < 75:
        return "Cam"
    return "Đỏ"
