from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Query

from app.services.nominatim_reverse import reverse_admin

router = APIRouter(tags=["Geocode"])


def _district_from_address(addr: Dict[str, Any]) -> str:
    return str(
        addr.get("city_district")
        or addr.get("county")
        or addr.get("city")
        or addr.get("state_district")
        or addr.get("state")
        or ""
    ).strip()


def _display_name_from_address(addr: Dict[str, Any]) -> str:
    ordered_keys: List[str] = [
        "house_number",
        "road",
        "neighbourhood",
        "suburb",
        "quarter",
        "city_district",
        "county",
        "city",
        "state",
        "postcode",
        "country",
    ]
    parts: List[str] = []
    seen = set()
    for key in ordered_keys:
        value = str(addr.get(key) or "").strip()
        if not value:
            continue
        lowered = value.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        parts.append(value)
    return ", ".join(parts)


@router.get("/geocode/reverse")
def geocode_reverse(
    lat: float = Query(..., ge=-90.0, le=90.0),
    lon: float = Query(..., ge=-180.0, le=180.0),
) -> Dict[str, Any]:
    """
    Reverse geocode nhẹ cho frontend AddressSearch / Map click.
    Trả về display_name + district (best effort). Nếu Nominatim tạm lỗi thì trả rỗng.
    """
    try:
        addr = reverse_admin(float(lat), float(lon)) or {}
    except Exception:
        addr = {}

    district = _district_from_address(addr)
    display_name = _display_name_from_address(addr)
    return {
        "display_name": display_name or None,
        "district": district or None,
        "address": addr,
    }

