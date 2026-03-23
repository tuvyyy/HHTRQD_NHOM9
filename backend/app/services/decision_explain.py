from __future__ import annotations
from typing import Dict, Any, List, Optional

# Quy ước unit theo Open-Meteo Air Quality
UNITS = {
    "pm2_5": "µg/m³",
    "pm10": "µg/m³",
    "nitrogen_dioxide": "µg/m³",
    "ozone": "µg/m³",
    "carbon_monoxide": "µg/m³",
}

# Map label FE -> key trong latest_values
LABEL_TO_KEY = {
    "PM2.5": "pm2_5",
    "PM10": "pm10",
    "NO2": "nitrogen_dioxide",
    "O3": "ozone",
    "CO": "carbon_monoxide",
}

def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def severity_score(key: str, value: Optional[float]) -> float:
    """
    Chuyển giá trị pollutant -> điểm 0..100 (để giải thích contribution).
    Lưu ý: đây là thang "giải thích" (proxy) để nói top factor, không thay đổi score hệ thống.
    """
    if value is None:
        return 0.0

    v = float(value)

    # Ngưỡng đơn giản (demo + báo cáo), đủ để giải thích
    # Bạn có thể chỉnh lại theo tiêu chuẩn bạn muốn (WHO/VN AQI/US AQI)
    if key == "pm2_5":
        # 0-15 tốt, 15-35 TB, 35-55 xấu, >55 rất xấu
        if v <= 15: return (v / 15) * 25
        if v <= 35: return 25 + ((v - 15) / 20) * 25
        if v <= 55: return 50 + ((v - 35) / 20) * 25
        return _clamp(75 + ((v - 55) / 45) * 25, 0, 100)

    if key == "pm10":
        # 0-50 tốt, 50-100 TB, 100-150 xấu, >150 rất xấu
        if v <= 50: return (v / 50) * 25
        if v <= 100: return 25 + ((v - 50) / 50) * 25
        if v <= 150: return 50 + ((v - 100) / 50) * 25
        return _clamp(75 + ((v - 150) / 150) * 25, 0, 100)

    if key == "nitrogen_dioxide":
        # 0-40 tốt, 40-80 TB, 80-150 xấu, >150 rất xấu
        if v <= 40: return (v / 40) * 25
        if v <= 80: return 25 + ((v - 40) / 40) * 25
        if v <= 150: return 50 + ((v - 80) / 70) * 25
        return _clamp(75 + ((v - 150) / 150) * 25, 0, 100)

    if key == "ozone":
        # 0-60 tốt, 60-120 TB, 120-180 xấu, >180 rất xấu
        if v <= 60: return (v / 60) * 25
        if v <= 120: return 25 + ((v - 60) / 60) * 25
        if v <= 180: return 50 + ((v - 120) / 60) * 25
        return _clamp(75 + ((v - 180) / 180) * 25, 0, 100)

    if key == "carbon_monoxide":
        # CO theo µg/m³, ngưỡng demo: 0-2000 tốt, 2000-5000 TB, 5000-10000 xấu, >10000 rất xấu
        if v <= 2000: return (v / 2000) * 25
        if v <= 5000: return 25 + ((v - 2000) / 3000) * 25
        if v <= 10000: return 50 + ((v - 5000) / 5000) * 25
        return _clamp(75 + ((v - 10000) / 10000) * 25, 0, 100)

    return 0.0

def build_recommendation(level: str, warning: bool = False) -> Dict[str, Any]:
    lv = (level or "").lower()
    actions: List[str] = []

    if warning:
        actions.append("Cảnh báo sớm: theo dõi giờ đỉnh và hạn chế hoạt động ngoài trời trong khung giờ đó.")

    if "đỏ" in lv or "do" in lv:
        actions += [
            "Hạn chế ra ngoài; tránh vận động mạnh ngoài trời.",
            "Nhóm nhạy cảm (trẻ em, người già, bệnh hô hấp) nên ở trong nhà.",
            "Đeo khẩu trang lọc bụi (N95/KN95) khi bắt buộc phải ra ngoài.",
        ]
    elif "cam" in lv:
        actions += [
            "Hạn chế hoạt động ngoài trời kéo dài.",
            "Nhóm nhạy cảm nên giảm thời gian ra ngoài.",
        ]
    elif "vàng" in lv or "vang" in lv:
        actions += [
            "Theo dõi chất lượng không khí; giảm vận động mạnh ngoài trời nếu có triệu chứng khó chịu.",
        ]
    else:
        actions += [
            "Chất lượng tương đối tốt; duy trì theo dõi định kỳ.",
        ]

    return {
        "level": level,
        "warning": bool(warning),
        "actions": actions,
        "note": "Khuyến nghị mang tính hỗ trợ ra quyết định (DSS), có thể tinh chỉnh theo tiêu chuẩn/đối tượng.",
    }

def build_explain(latest_values: Dict[str, Any], weights: Dict[str, float], total_score_0_100: Optional[float] = None) -> Dict[str, Any]:
    # Normalize weights
    ws = sum(float(v or 0) for v in weights.values()) or 1.0

    criteria = []
    total_weighted = 0.0

    for label, w in weights.items():
        key = LABEL_TO_KEY.get(label)
        if not key:
            continue
        value = latest_values.get(key)
        sev = severity_score(key, value)  # 0..100
        weighted = (float(w or 0) / ws) * sev
        total_weighted += weighted

        criteria.append({
            "label": label,
            "key": key,
            "value": value,
            "unit": UNITS.get(key, ""),
            "weight": round(float(w or 0), 6),
            "severity_0_100": round(sev, 2),
            "weighted_part": round(weighted, 2),
        })

    # % đóng góp theo weighted_part
    denom = total_weighted if total_weighted > 0 else 1.0
    for c in criteria:
        c["pct_of_explain"] = round((c["weighted_part"] / denom) * 100.0, 1)

    criteria_sorted = sorted(criteria, key=lambda x: x["weighted_part"], reverse=True)
    top_factors = [
        {
            "label": c["label"],
            "value": c["value"],
            "unit": c["unit"],
            "pct": c["pct_of_explain"],
        }
        for c in criteria_sorted[:3]
    ]

    return {
        "method": "weights × severity(proxy)",
        "note": "Explain dùng thang severity đơn giản để chỉ ra yếu tố đóng góp chính; KHÔNG thay đổi công thức score hiện tại của hệ thống.",
        "total_score_input": None if total_score_0_100 is None else round(float(total_score_0_100), 2),
        "explain_score_estimated": round(total_weighted, 2),
        "criteria": criteria_sorted,
        "top_factors": top_factors,
    }
