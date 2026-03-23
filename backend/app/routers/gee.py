# backend/app/routers/gee.py
from fastapi import APIRouter, Query, HTTPException
from app.services.gee_service import get_s5p_no2_tile

router = APIRouter(tags=["GEE"])

@router.get("/gee/tiles")
def gee_tiles(
    layer: str = Query("no2"),
    start: str = Query(...),
    end: str = Query(...),
    bbox: str | None = Query(None, description="minLon,minLat,maxLon,maxLat (optional)"),
    mask: str | None = Query(None, description="hcm | hcm_inner (optional)")
):
    try:
        if layer.lower() == "no2":
            return get_s5p_no2_tile(start=start, end=end, bbox=bbox, mask=mask)
        raise HTTPException(status_code=400, detail="layer chưa hỗ trợ (hiện chỉ demo no2)")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GEE error: {e}")
