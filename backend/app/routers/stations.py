from __future__ import annotations

import math
from fastapi import APIRouter, HTTPException, Query

from app.services.aqicn_service import aqicn_map_bounds, aqicn_feed_uid, AqicnError

router = APIRouter(tags=["Stations (AQICN)"])


@router.get("/stations/ping")
async def stations_ping():
    """Quick check AQICN token + network.

    Trả về count trạm của bbox nhỏ quanh trung tâm HCM.
    """
    try:
        # bbox nhỏ quanh Q1 (có thể 0 nếu AQICN public ít, nhưng token OK vẫn trả status=ok)
        items = await aqicn_map_bounds(10.72, 106.62, 10.83, 106.77)
        return {"ok": True, "count": len(items)}
    except AqicnError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/stations/around")
async def stations_around(
    lat: float = Query(...),
    lon: float = Query(...),
    radius_km: float = Query(200.0, ge=5.0, le=1500.0),
    expand_if_empty: bool = Query(True, description="Nếu 0 trạm thì tự mở rộng vùng tìm kiếm"),
    max_km: float = Query(350.0, ge=5.0, le=1500.0),
    step_km: float = Query(50.0, ge=5.0, le=500.0),
    max_tries: int = Query(8, ge=1, le=30),
):
    """Helper endpoint: tìm trạm quanh 1 điểm theo bán kính (km).

    FE gọi đơn giản hơn so với tự tính bbox.
    Tận dụng logic /stations/bounds (bao gồm auto expand).
    """
    d_lat = radius_km / 111.0
    d_lon = radius_km / (111.0 * max(0.2, math.cos(math.radians(lat))))
    return await stations_bounds(
        minLat=lat - d_lat,
        minLon=lon - d_lon,
        maxLat=lat + d_lat,
        maxLon=lon + d_lon,
        expand_if_empty=expand_if_empty,
        max_km=max_km,
        step_km=step_km,
        max_tries=max_tries,
    )

@router.get("/stations/bounds")
async def stations_bounds(
    minLat: float = Query(...),
    minLon: float = Query(...),
    maxLat: float = Query(...),
    maxLon: float = Query(...),
    expand_if_empty: bool = Query(True, description="Nếu bbox trả 0 trạm thì tự mở rộng vùng tìm kiếm"),
    max_km: float = Query(350.0, ge=5.0, le=1500.0, description="Bán kính tối đa (km) khi auto expand"),
    step_km: float = Query(50.0, ge=5.0, le=500.0, description="Bước tăng bán kính (km) mỗi lần retry"),
    max_tries: int = Query(8, ge=1, le=30, description="Số lần retry tối đa khi auto expand"),
):
    try:
        def bbox_around(lat: float, lon: float, km: float):
            d_lat = km / 111.0
            d_lon = km / (111.0 * max(0.2, math.cos(math.radians(lat))))
            return (lat - d_lat, lon - d_lon, lat + d_lat, lon + d_lon)

        async def fetch(min_lat: float, min_lon: float, max_lat: float, max_lon: float):
            return await aqicn_map_bounds(min_lat, min_lon, max_lat, max_lon)

        items = await fetch(minLat, minLon, maxLat, maxLon)

        # ✅ Fallback: nếu bbox "nội thành" quá chặt / AQICN public ít trạm
        attempts = 1
        used_km = None
        if expand_if_empty and (not items):
            center_lat = (minLat + maxLat) / 2.0
            center_lon = (minLon + maxLon) / 2.0

            # ước lượng bán kính hiện tại từ bbox (km) để bắt đầu expand hợp lý
            base_km = max(abs(maxLat - minLat) * 111.0, abs(maxLon - minLon) * 111.0) / 2.0
            km = max(50.0, base_km)

            while attempts < max_tries and km <= max_km:
                km = min(max_km, km + step_km)
                (a, b, c, d) = bbox_around(center_lat, center_lon, km)
                items = await fetch(a, b, c, d)
                attempts += 1
                used_km = km
                if items:
                    break
        # normalize output nhẹ cho FE
        out = []
        for it in items:
            st = it.get("station") or {}
            out.append({
                "uid": it.get("uid"),
                "aqi": it.get("aqi"),
                "lat": it.get("lat"),
                "lon": it.get("lon"),
                "name": st.get("name"),
            })
        return {
            "items": out,
            "count": len(out),
            "meta": {
                "attempts": attempts,
                "expanded": bool(expand_if_empty and attempts > 1),
                "used_km": used_km,
                "input_bbox": {"minLat": minLat, "minLon": minLon, "maxLat": maxLat, "maxLon": maxLon},
            },
        }
    except AqicnError as e:
        raise HTTPException(status_code=502, detail=str(e))

@router.get("/stations/{uid}")
async def station_detail(uid: int):
    try:
        data = await aqicn_feed_uid(uid)
        return data
    except AqicnError as e:
        raise HTTPException(status_code=502, detail=str(e))