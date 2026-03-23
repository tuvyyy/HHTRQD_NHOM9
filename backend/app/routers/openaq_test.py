# backend/app/routers/openaq_test.py

from fastapi import APIRouter, HTTPException, Query
import os, json
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.services.nominatim_reverse import reverse_district

router = APIRouter(prefix="/api/openaq", tags=["openaq"])

OPENAQ_BASE = "https://api.openaq.org"


def _get_json(url: str) -> dict:
    api_key = os.getenv("OPENAQ_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing OPENAQ_API_KEY")

    req = Request(
        url,
        headers={
            "X-API-Key": api_key,
            "Accept": "application/json",
            "User-Agent": "ra-quyet-dinh-hcm-dss/1.0",
        },
    )
    with urlopen(req, timeout=20) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _provider_name(item: dict) -> str:
    provider = item.get("provider")
    if isinstance(provider, dict):
        name = provider.get("name")
        if isinstance(name, str) and name.strip():
            return name.strip()

    providers = item.get("providers")
    if isinstance(providers, list):
        for p in providers:
            if isinstance(p, dict):
                name = p.get("name") or p.get("providerName")
                if isinstance(name, str) and name.strip():
                    return name.strip()
            elif isinstance(p, str) and p.strip():
                return p.strip()

    owner = item.get("owner")
    if isinstance(owner, dict):
        owner_name = owner.get("name")
        if isinstance(owner_name, str) and owner_name.strip():
            return owner_name.strip()

    return "OpenAQ"


def _is_inner_hcm(district: str) -> bool:
    d = (district or "").strip().lower()

    # Nominatim hay trả "Thành phố Hồ Chí Minh" / "Thành phố Thủ Đức"
    if "thành phố hồ chí minh" in d or "ho chi minh" in d:
        return True
    if "thủ đức" in d or "thu duc" in d:
        return True

    # loại ngoại thành (HCM)
    if any(
        x in d
        for x in [
            "củ chi", "cu chi",
            "hóc môn", "hoc mon",
            "bình chánh", "binh chanh",
            "nhà bè", "nha be",
            "cần giờ", "can gio",
        ]
    ):
        return False

    # Quận số
    if any(
        q in d
        for q in [
            "quận 1", "quan 1",
            "quận 3", "quan 3",
            "quận 4", "quan 4",
            "quận 5", "quan 5",
            "quận 6", "quan 6",
            "quận 7", "quan 7",
            "quận 8", "quan 8",
            "quận 10", "quan 10",
            "quận 11", "quan 11",
            "quận 12", "quan 12",
        ]
    ):
        return True

    # Quận tên
    if any(
        x in d
        for x in [
            "bình thạnh", "binh thanh",
            "phú nhuận", "phu nhuan",
            "gò vấp", "go vap",
            "tân bình", "tan binh",
            "tân phú", "tan phu",
            "bình tân", "binh tan",
        ]
    ):
        return True

    return False


@router.get("/debug-reverse")
def debug_reverse(
    lat: float = Query(...),
    lon: float = Query(...),
):
    """
    Debug nhanh Nominatim reverse geocode cho 1 điểm.
    """
    try:
        district = reverse_district(float(lat), float(lon))
        return {
            "lat": lat,
            "lon": lon,
            "district": district,
            "is_inner": _is_inner_hcm(district),
        }
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Reverse error: {e}")


@router.get("/test")
def test_locations_bbox(
    
    minLat: float = Query(10.60),
    minLon: float = Query(106.55),
    maxLat: float = Query(10.95),
    maxLon: float = Query(106.90),
    limit: int = Query(50, ge=1, le=1000),
):
    try:
        bbox = f"{minLon:.4f},{minLat:.4f},{maxLon:.4f},{maxLat:.4f}"  # OpenAQ bbox: lon,lat
        params = {
            "bbox": bbox,
            "limit": str(limit),
            "page": "1",
            "iso": "VN",
            "order_by": "id",
            "sort_order": "asc",
        }
        url = f"{OPENAQ_BASE}/v3/locations?" + urlencode(params)
        data = _get_json(url)
        results = data.get("results") or []

        items = []
        for r in results:
            coords = r.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")
            if lat is None or lon is None:
                continue

            district = reverse_district(float(lat), float(lon))
            items.append(
                {
                    "id": r.get("id"),
                    "name": r.get("name"),
                    "lat": lat,
                    "lon": lon,
                    "provider": _provider_name(r),
                    "district": district,
                    "is_inner": _is_inner_hcm(district),
                }
            )

        inner = [x for x in items if x["is_inner"]]

        return {
            "count_raw": len(items),
            "count_inner": len(inner),
            "sample_inner": inner[:10],
            "sample_raw": items[:10],
        }

    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenAQ error: {e}")


@router.get("/stations")
def get_inner_stations(
    minLat: float = Query(10.60),
    minLon: float = Query(106.55),
    maxLat: float = Query(10.95),
    maxLon: float = Query(106.90),
    limit: int = Query(200, ge=1, le=1000),
):
    """
    Trả danh sách trạm trong nội thành (tạm bằng reverse + rule).
    Khi có GeoJSON sẽ thay rule bằng point-in-polygon.
    """
    try:
        bbox = f"{minLon:.4f},{minLat:.4f},{maxLon:.4f},{maxLat:.4f}"
        params = {
            "bbox": bbox,
            "limit": str(limit),
            "page": "1",
            "iso": "VN",
            "order_by": "id",
            "sort_order": "asc",
        }
        url = f"{OPENAQ_BASE}/v3/locations?" + urlencode(params)
        data = _get_json(url)
        results = data.get("results") or []

        inner = []
        for r in results:
            coords = r.get("coordinates") or {}
            lat = coords.get("latitude")
            lon = coords.get("longitude")
            if lat is None or lon is None:
                continue

            district = reverse_district(float(lat), float(lon))
            if not _is_inner_hcm(district):
                continue

            inner.append(
                {
                    "id": r.get("id"),
                    "name": r.get("name"),
                    "lat": float(lat),
                    "lon": float(lon),
                    "provider": _provider_name(r),
                    "district": district,
                }
            )

        return {"count": len(inner), "stations": inner}

    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenAQ error: {e}")
