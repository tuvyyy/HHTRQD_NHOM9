# backend/app/services/gee_service.py
from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import ee

# ============ 1) EE init (dùng project bạn đã đăng ký) ============
_EE_INITIALIZED = False


def _ensure_ee():
    global _EE_INITIALIZED
    if _EE_INITIALIZED:
        return
    project = os.getenv("GEE_PROJECT", "").strip()
    if project:
        ee.Initialize(project=project)
    else:
        # nếu bạn đã authenticate + register project rồi nhưng chưa set env,
        # ee sẽ vẫn chạy (tuỳ môi trường), nhưng mình khuyên set GEE_PROJECT
        ee.Initialize()
    _EE_INITIALIZED = True


def _parse_date(d: str):
    datetime.strptime(d, "%Y-%m-%d")


def _parse_bbox(bbox: str) -> Tuple[float, float, float, float]:
    # bbox = "minLon,minLat,maxLon,maxLat"
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox phải có dạng minLon,minLat,maxLon,maxLat")
    minLon, minLat, maxLon, maxLat = map(float, parts)
    return minLon, minLat, maxLon, maxLat


# ============ 2) Mask ranh hành chính từ GAUL level2 ============
# Dataset: ee.FeatureCollection("FAO/GAUL/2015/level2")
# Có field ADM0_NAME / ADM1_NAME / ADM2_NAME (docs của Earth Engine).
# (Lưu ý: GAUL là bộ ranh tham chiếu, không phải ranh “mới nhất sau sáp nhập” 100%.)

# Danh sách “nội thành” theo cách hiểu phổ biến:
# - gồm các quận + TP Thủ Đức
# - loại các huyện: Củ Chi, Hóc Môn, Bình Chánh, Nhà Bè, Cần Giờ
# (Nếu GAUL đặt tên khác, filter có thể hụt -> mình fallback về toàn TP.HCM)
HCM_INNER_HINTS: List[str] = [
    # dạng tiếng Anh hay gặp
    "District 1", "District 3", "District 4", "District 5", "District 6",
    "District 7", "District 8", "District 10", "District 11", "District 12",
    "Binh Thanh", "Go Vap", "Phu Nhuan", "Tan Binh", "Tan Phu", "Binh Tan",
    "Thu Duc",
    # dạng VN không dấu hay gặp
    "Quan 1", "Quan 3", "Quan 4", "Quan 5", "Quan 6",
    "Quan 7", "Quan 8", "Quan 10", "Quan 11", "Quan 12",
]


def get_hcm_geometry(mask: str) -> ee.Geometry:
    """
    mask:
      - "hcm": ranh toàn TP.HCM
      - "hcm_inner": ranh nội thành (ước lượng theo danh sách quận + Thủ Đức)
    """
    _ensure_ee()

    gaul2 = ee.FeatureCollection("FAO/GAUL/2015/level2")

    # lọc Việt Nam
    vn = gaul2.filter(ee.Filter.eq("ADM0_NAME", "Viet Nam"))

    # lọc ADM1 là TP.HCM (tên có thể là "Ho Chi Minh City" / "Ho Chi Minh")
    hcm_fc = vn.filter(ee.Filter.stringContains("ADM1_NAME", "Ho Chi Minh"))

    if mask == "hcm_inner":
        # thử lọc theo danh sách tên quận/thủ đức (có thể không match 100%)
        inner_fc = hcm_fc.filter(ee.Filter.inList("ADM2_NAME", HCM_INNER_HINTS))

        # nếu không match được gì -> fallback về toàn HCM
        # (mình kiểm tra size = 0 bằng cách getInfo; với demo local OK)
        try:
            size = int(inner_fc.size().getInfo())
        except Exception:
            size = 0

        use_fc = inner_fc if size > 0 else hcm_fc
    else:
        use_fc = hcm_fc

    # union thành 1 polygon
    geom = use_fc.union(1).geometry()
    return geom


# ============ 3) Tile NO2 Sentinel-5P ============
def get_s5p_no2_tile(start: str, end: str, bbox: Optional[str] = None, mask: Optional[str] = None) -> Dict[str, Any]:
    """
    start/end: YYYY-MM-DD
    bbox: minLon,minLat,maxLon,maxLat (optional)
    mask: "hcm" | "hcm_inner" (optional)
    """
    _ensure_ee()

    _parse_date(start)
    _parse_date(end)

    # Sentinel-5P NO2 column density (mol/m^2) — proxy
    col = ee.ImageCollection("COPERNICUS/S5P/NRTI/L3_NO2") \
        .filterDate(start, end) \
        .select("NO2_column_number_density")

    img = col.mean()

    # 1) ưu tiên clip theo mask hành chính nếu có
    if mask in ("hcm", "hcm_inner"):
        geom = get_hcm_geometry(mask)
        img = img.clip(geom)
    # 2) nếu không có mask mà có bbox -> clip bbox
    elif bbox:
        minLon, minLat, maxLon, maxLat = _parse_bbox(bbox)
        rect = ee.Geometry.Rectangle([minLon, minLat, maxLon, maxLat])
        img = img.clip(rect)

    vis = {
        "min": 0,
        "max": 0.0002,
        "palette": [
            "000004", "1B0C41", "4A0C6B", "781C6D",
            "A52C60", "CF4446", "ED6925", "FB9A06", "F7D13D"
        ]
    }

    map_id = ee.Image(img).getMapId(vis)
    tile_url = map_id["tile_fetcher"].url_format

    return {
        "layer": "s5p_no2",
        "start": start,
        "end": end,
        "tile_url": tile_url,
        "vis": vis,
        "legend": {
            "title": "Sentinel-5P NO2 column (mol/m²)",
            "min": vis["min"],
            "max": vis["max"],
            "palette": vis["palette"],
            "note": "NO2 là column density (proxy), không phải nồng độ mặt đất."
        },
        "mask": mask,
        "bbox": bbox
    }
