# backend/app/services/nominatim_reverse.py
import json, time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

USER_AGENT = "ra-quyet-dinh-hcm-dss/1.0 (dev local)"

_CACHE = {}
_LAST_CALL = 0.0
MIN_INTERVAL = 1.0  # tránh bị Nominatim block


def reverse_admin(lat: float, lon: float) -> dict:
    """
    Trả về dict address của Nominatim (có cache).
    """
    global _LAST_CALL
    key = f"{lat:.5f},{lon:.5f}"
    if key in _CACHE:
        return _CACHE[key]

    now = time.monotonic()
    wait = MIN_INTERVAL - (now - _LAST_CALL)
    if wait > 0:
        time.sleep(wait)

    params = {
        "format": "jsonv2",
        "lat": f"{lat:.6f}",
        "lon": f"{lon:.6f}",
        "zoom": "12",
        "addressdetails": "1",
        "accept-language": "vi",
    }
    url = "https://nominatim.openstreetmap.org/reverse?" + urlencode(params)

    try:
        req = Request(url, headers={"User-Agent": USER_AGENT})
        with urlopen(req, timeout=25) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception:
        _CACHE[key] = {}
        return {}

    _LAST_CALL = time.monotonic()
    addr = data.get("address") or {}
    _CACHE[key] = addr
    return addr


def reverse_district(lat: float, lon: float) -> str:
    """
    Trả về admin cấp "quận/huyện/TP Thủ Đức/TPHCM" để lọc nội thành.
    Ưu tiên city_district/county, nếu không có thì lấy city.
    """
    addr = reverse_admin(lat, lon)
    if not addr:
        return ""

    # ƯU TIÊN CẤP QUẬN/HUYỆN nếu có
    district = (addr.get("city_district") or addr.get("county") or "")
    district = str(district).strip()
    if district:
        return district

    # Nếu không có quận/huyện thì lấy city (vd: "Thành phố Thủ Đức", "Thành phố Hồ Chí Minh")
    city = str(addr.get("city") or "").strip()
    if city:
        return city

    # fallback cuối: state_district/state
    fallback = str(addr.get("state_district") or addr.get("state") or "").strip()
    return fallback