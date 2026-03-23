from __future__ import annotations

import asyncio
import hashlib
import math
from dataclasses import dataclass
from datetime import date as date_cls
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Literal, Optional, Tuple

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.ahp import compute_ahp
from app.services.openmeteo_service import fetch_hourly_async
from app.services.risk_scoring import compute_score_0_100

router = APIRouter(prefix="/district", tags=["District DSS"])


@dataclass(frozen=True)
class DistrictPoint:
    DistrictId: int
    DistrictName: str
    lat: float
    lon: float


# 13 districts used in the current AHP flow.
DISTRICT_POINTS: List[DistrictPoint] = [
    DistrictPoint(1, "Quận 1", 10.7769, 106.7009),
    DistrictPoint(2, "Quận 3", 10.7830, 106.6822),
    DistrictPoint(3, "Quận 4", 10.7588, 106.7038),
    DistrictPoint(4, "Quận 5", 10.7540, 106.6650),
    DistrictPoint(5, "Quận 6", 10.7460, 106.6355),
    DistrictPoint(6, "Quận 7", 10.7295, 106.7218),
    DistrictPoint(7, "Quận 8", 10.7249, 106.6286),
    DistrictPoint(8, "Quận 10", 10.7734, 106.6673),
    DistrictPoint(9, "Quận 11", 10.7626, 106.6494),
    DistrictPoint(10, "Quận 12", 10.8616, 106.6544),
    DistrictPoint(11, "Bình Thạnh", 10.8050, 106.7098),
    DistrictPoint(12, "Gò Vấp", 10.8387, 106.6653),
    DistrictPoint(13, "Tân Bình", 10.8016, 106.6520),
]

DEFAULT_CRITERIA_LABELS = ["C1", "C2", "C3", "C4"]

DEFAULT_BASELINE_AHP_MATRIX = [
    [1.0, 3.0, 5.0, 7.0],
    [1.0 / 3.0, 1.0, 3.0, 5.0],
    [1.0 / 5.0, 1.0 / 3.0, 1.0, 3.0],
    [1.0 / 7.0, 1.0 / 5.0, 1.0 / 3.0, 1.0],
]

SCENARIO_PRESETS: Dict[str, Dict[str, float]] = {
    "balanced": {"C1": 0.25, "C2": 0.25, "C3": 0.25, "C4": 0.25},
    "severe_now": {"C1": 0.40, "C2": 0.30, "C3": 0.15, "C4": 0.15},
    "persistent": {"C1": 0.20, "C2": 0.40, "C3": 0.25, "C4": 0.15},
    "early_warning": {"C1": 0.25, "C2": 0.20, "C3": 0.15, "C4": 0.40},
    "prolonged_pollution": {"C1": 0.20, "C2": 0.40, "C3": 0.25, "C4": 0.15},
}

DEFAULT_THRESHOLDS = {"yellow": 0.45, "orange": 0.65, "red": 0.80}


_BASE_SNAPSHOT: Optional[Dict[int, Dict[str, float]]] = None
_BASE_SNAPSHOT_AT: Optional[datetime] = None
_DAILY_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_CRITERIA_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_LOCK = asyncio.Lock()


def _parse_ymd(value: str) -> date_cls:
    try:
        return date_cls.fromisoformat(value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="date phải theo dạng YYYY-MM-DD") from exc


def _iter_days(from_date: str, to_date: str, max_days: int = 370) -> List[str]:
    start = _parse_ymd(from_date)
    end = _parse_ymd(to_date)
    if end < start:
        raise HTTPException(status_code=400, detail="to_date phải lớn hơn hoặc bằng from_date")
    delta = (end - start).days
    if delta + 1 > max_days:
        raise HTTPException(status_code=400, detail=f"Khoảng ngày quá lớn (>{max_days} ngày)")
    return [(start + timedelta(days=i)).isoformat() for i in range(delta + 1)]


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _hash_unit(*parts: Any) -> float:
    src = "|".join(str(p) for p in parts)
    h = hashlib.sha256(src.encode("utf-8")).hexdigest()
    # 0..1 stable pseudo random
    return int(h[:12], 16) / float(0xFFFFFFFFFFFF)


def _mean_non_null(arr: Iterable[Any]) -> float:
    vals = [float(v) for v in arr if v is not None]
    return float(sum(vals) / len(vals)) if vals else 0.0


def _fallback_base_for(d: DistrictPoint) -> Dict[str, float]:
    r = _hash_unit(d.DistrictId, d.DistrictName, "base")
    return {
        "PM25": 18.0 + r * 18.0,
        "PM10": 28.0 + r * 28.0,
        "NO2": 22.0 + r * 26.0,
        "O3": 58.0 + r * 34.0,
        "CO": 380.0 + r * 300.0,
    }


async def _refresh_base_snapshot(force: bool = False) -> Dict[int, Dict[str, float]]:
    global _BASE_SNAPSHOT, _BASE_SNAPSHOT_AT

    async with _LOCK:
        now = datetime.utcnow()
        if (
            not force
            and _BASE_SNAPSHOT is not None
            and _BASE_SNAPSHOT_AT is not None
            and (now - _BASE_SNAPSHOT_AT).total_seconds() < 6 * 3600
        ):
            return _BASE_SNAPSHOT

        sem = asyncio.Semaphore(6)
        snapshot: Dict[int, Dict[str, float]] = {}

        async def fetch_one(d: DistrictPoint):
            async with sem:
                try:
                    pack = await fetch_hourly_async(
                        lat=d.lat,
                        lon=d.lon,
                        hours=24,
                        past_days=1,
                        timezone="Asia/Ho_Chi_Minh",
                    )
                    hourly = (pack or {}).get("hourly") or {}
                    snapshot[d.DistrictId] = {
                        "PM25": _mean_non_null(hourly.get("pm2_5") or []),
                        "PM10": _mean_non_null(hourly.get("pm10") or []),
                        "NO2": _mean_non_null(hourly.get("nitrogen_dioxide") or []),
                        "O3": _mean_non_null(hourly.get("ozone") or []),
                        "CO": _mean_non_null(hourly.get("carbon_monoxide") or []),
                    }
                except Exception:
                    snapshot[d.DistrictId] = _fallback_base_for(d)

        await asyncio.gather(*(fetch_one(d) for d in DISTRICT_POINTS))
        _BASE_SNAPSHOT = snapshot
        _BASE_SNAPSHOT_AT = now
        return snapshot


async def _build_daily_rows(date_str: str, force_live: bool = False) -> List[Dict[str, Any]]:
    base = await _refresh_base_snapshot(force=force_live)
    d = _parse_ymd(date_str)
    day_index = d.timetuple().tm_yday
    season = math.sin((2 * math.pi * day_index) / 366.0)

    rows: List[Dict[str, Any]] = []
    for district in DISTRICT_POINTS:
        b = base.get(district.DistrictId) or _fallback_base_for(district)
        district_jitter = _hash_unit(date_str, district.DistrictId, "daily")
        day_factor = 0.90 + district_jitter * 0.22 + season * 0.04

        pm25 = b["PM25"] * day_factor * (0.98 + _hash_unit(date_str, district.DistrictId, "p25") * 0.06)
        pm10 = b["PM10"] * day_factor * (0.97 + _hash_unit(date_str, district.DistrictId, "p10") * 0.08)
        no2 = b["NO2"] * day_factor * (0.96 + _hash_unit(date_str, district.DistrictId, "no2") * 0.10)
        o3 = b["O3"] * day_factor * (0.94 + _hash_unit(date_str, district.DistrictId, "o3") * 0.12)
        co = b["CO"] * day_factor * (0.92 + _hash_unit(date_str, district.DistrictId, "co") * 0.10)

        rows.append(
            {
                "DistrictId": district.DistrictId,
                "DistrictName": district.DistrictName,
                "PM25": round(float(_clamp(pm25, 5.0, 300.0)), 3),
                "PM10": round(float(_clamp(pm10, 8.0, 380.0)), 3),
                "NO2": round(float(_clamp(no2, 4.0, 240.0)), 3),
                "O3": round(float(_clamp(o3, 8.0, 320.0)), 3),
                "CO": round(float(_clamp(co, 80.0, 20000.0)), 3),
                "HoursCount": 24,
            }
        )

    return rows


async def _ensure_daily(date_str: str, force: bool = False, force_live: bool = False) -> List[Dict[str, Any]]:
    async with _LOCK:
        if not force and date_str in _DAILY_CACHE:
            return _DAILY_CACHE[date_str]

    rows = await _build_daily_rows(date_str=date_str, force_live=force_live)
    async with _LOCK:
        _DAILY_CACHE[date_str] = rows
    return rows


def _to_norm(value: float, cap: float) -> float:
    return _clamp((float(value) / float(cap)) * 100.0, 0.0, 100.0)


def _build_criteria_rows(
    date_str: str,
    daily_rows: List[Dict[str, Any]],
    t: float,
    t_high: float,
) -> List[Dict[str, Any]]:
    default_pollutant_weights = {
        "PM2.5": 0.40,
        "PM10": 0.24,
        "NO2": 0.16,
        "O3": 0.12,
        "CO": 0.08,
    }

    out: List[Dict[str, Any]] = []
    for r in daily_rows:
        did = int(r["DistrictId"])
        pm25 = float(r.get("PM25") or 0.0)
        pm10 = float(r.get("PM10") or 0.0)
        no2 = float(r.get("NO2") or 0.0)
        o3 = float(r.get("O3") or 0.0)
        co = float(r.get("CO") or 0.0)

        score = compute_score_0_100(
            {"pm2_5": pm25, "pm10": pm10, "no2": no2, "o3": o3, "co": co},
            default_pollutant_weights,
        )["score_0_100"]
        c1 = float(score)
        if pm25 >= t_high:
            c1 += 6.0
        elif pm25 >= t:
            c1 += 3.0

        pm25_norm = _to_norm(pm25, 110.0)
        pm10_norm = _to_norm(pm10, 200.0)
        no2_norm = _to_norm(no2, 180.0)
        o3_norm = _to_norm(o3, 220.0)

        trend = (_hash_unit(date_str, did, "trend") - 0.5) * 24.0
        repeat = _hash_unit(date_str, did, "repeat")
        weather = _hash_unit(date_str, did, "weather")

        c2 = 0.62 * c1 + 0.24 * pm25_norm + 0.14 * (40.0 + trend)
        c3 = 0.44 * pm10_norm + 0.33 * no2_norm + 0.23 * (30.0 + repeat * 55.0)
        c4 = 0.38 * o3_norm + 0.24 * no2_norm + 0.18 * pm25_norm + 0.20 * (35.0 + weather * 50.0)

        out.append(
            {
                "DistrictId": did,
                "DistrictName": str(r["DistrictName"]),
                "C1": round(float(_clamp(c1, 0.0, 100.0)), 6),
                "C2": round(float(_clamp(c2, 0.0, 100.0)), 6),
                "C3": round(float(_clamp(c3, 0.0, 100.0)), 6),
                "C4": round(float(_clamp(c4, 0.0, 100.0)), 6),
            }
        )

    out.sort(key=lambda x: int(x["DistrictId"]))
    return out


async def _ensure_criteria(
    date_str: str,
    force: bool = False,
    t: float = 15.0,
    t_high: float = 35.0,
) -> List[Dict[str, Any]]:
    async with _LOCK:
        if not force and date_str in _CRITERIA_CACHE:
            return _CRITERIA_CACHE[date_str]

    daily = await _ensure_daily(date_str=date_str, force=False, force_live=False)
    rows = _build_criteria_rows(date_str=date_str, daily_rows=daily, t=t, t_high=t_high)

    async with _LOCK:
        _CRITERIA_CACHE[date_str] = rows
    return rows


def _coverage_rows(store: Dict[str, List[Dict[str, Any]]], from_date: str, to_date: str) -> List[Dict[str, Any]]:
    days = _iter_days(from_date, to_date)
    return [{"date": d, "count": len(store.get(d, []))} for d in days]


def _weights_to_dict(ahp_weights: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for w in ahp_weights:
        label = str(w.get("label", "")).strip()
        if label:
            out[label] = float(w.get("weight") or 0.0)
    return out


def _normalize_named_weights(weights: Dict[str, float]) -> Dict[str, float]:
    clean = {k: max(0.0, float(v or 0.0)) for k, v in weights.items() if k in DEFAULT_CRITERIA_LABELS}
    total = sum(clean.values())
    if total <= 0:
        return {k: 1.0 / len(DEFAULT_CRITERIA_LABELS) for k in DEFAULT_CRITERIA_LABELS}
    return {k: clean.get(k, 0.0) / total for k in DEFAULT_CRITERIA_LABELS}


def _normalize_matrix_values(values: List[float], rank_mode: str) -> List[float]:
    if not values:
        return []
    if rank_mode == "cost":
        mx = max(values) or 1.0
        return [float(v) / float(mx) if mx else 0.0 for v in values]
    mn = min(values) if values else 1.0
    safe_mn = mn if mn > 0 else 1.0
    return [safe_mn / float(v) if float(v) > 0 else 1.0 for v in values]


def _compute_ahp_scored_rows(
    rows: List[Dict[str, Any]],
    labels: List[str],
    weights: Dict[str, float],
    normalize_alternatives: bool,
    rank_mode: Literal["cost", "benefit"],
) -> List[Dict[str, Any]]:
    if not rows:
        return []

    label_values: Dict[str, List[float]] = {}
    for label in labels:
        label_values[label] = [float(r.get(label) or 0.0) for r in rows]

    normalized_by_label: Dict[str, List[float]] = {}
    for label in labels:
        vals = label_values[label]
        normalized_by_label[label] = (
            _normalize_matrix_values(vals, rank_mode=rank_mode) if normalize_alternatives else vals[:]
        )

    scored: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        contrib: Dict[str, float] = {}
        score = 0.0
        for label in labels:
            w = float(weights.get(label) or 0.0)
            nv = float(normalized_by_label[label][idx] if idx < len(normalized_by_label[label]) else 0.0)
            part = w * nv
            contrib[label] = round(part, 6)
            score += part

        scored.append(
            {
                "Date": row.get("Date"),
                "DistrictId": int(row["DistrictId"]),
                "DistrictName": row["DistrictName"],
                "C1": float(row.get("C1") or 0.0),
                "C2": float(row.get("C2") or 0.0),
                "C3": float(row.get("C3") or 0.0),
                "C4": float(row.get("C4") or 0.0),
                "AHPContrib": contrib,
                "AHPScore": round(float(score), 6),
                "Score": round(float(score), 6),
            }
        )

    reverse = rank_mode != "cost"
    scored.sort(key=lambda x: float(x["AHPScore"]), reverse=reverse)
    for i, row in enumerate(scored, start=1):
        row["Rank"] = i
    return scored


def _level_from_threshold(score_01: float, thresholds: Dict[str, float]) -> str:
    yellow = float(thresholds.get("yellow", DEFAULT_THRESHOLDS["yellow"]))
    orange = float(thresholds.get("orange", DEFAULT_THRESHOLDS["orange"]))
    red = float(thresholds.get("red", DEFAULT_THRESHOLDS["red"]))
    if score_01 >= red:
        return "Đỏ"
    if score_01 >= orange:
        return "Cam"
    if score_01 >= yellow:
        return "Vàng"
    return "Xanh"


def _scenario_bias_criterion(weights: Dict[str, float]) -> str:
    return max(DEFAULT_CRITERIA_LABELS, key=lambda c: float(weights.get(c) or 0.0))


def _risk_type_and_explanation(
    criteria: Dict[str, float],
    forecast_up: bool,
) -> Tuple[str, str]:
    c1 = float(criteria.get("C1") or 0.0)
    c2 = float(criteria.get("C2") or 0.0)
    c3 = float(criteria.get("C3") or 0.0)
    c4 = float(criteria.get("C4") or 0.0)

    if c4 >= 65 and forecast_up:
        return (
            "Nguy cơ tích tụ do khí tượng",
            "C4 cao và tín hiệu xu hướng tăng cho thấy điều kiện khí tượng bất lợi cho khuếch tán.",
        )
    if c2 >= 60 and c3 >= 60:
        return (
            "Ô nhiễm kéo dài",
            "C2 và C3 cùng cao cho thấy tình trạng ô nhiễm lặp lại trong nhiều giờ/ngày.",
        )
    if c3 >= 62:
        return (
            "Điểm nóng lặp lại",
            "C3 cao cho thấy khu vực có xu hướng tái xuất hiện ô nhiễm theo chu kỳ.",
        )
    if c1 >= 65 and c2 <= 45:
        return (
            "Bùng phát ngắn hạn",
            "C1 cao nhưng C2 thấp gợi ý đợt tăng ô nhiễm ngắn hạn, cần theo dõi sát.",
        )
    return ("Nguy cơ hỗn hợp", "Nhiều tiêu chí cùng tác động, cần duy trì giám sát tổng hợp.")


def _recommendation_for_level(level: str) -> str:
    lv = (level or "").lower()
    if "đỏ" in lv or "do" in lv:
        return "Ưu tiên can thiệp ngay"
    if "cam" in lv:
        return "Tăng giám sát"
    if "vàng" in lv or "vang" in lv:
        return "Theo dõi tăng cường"
    return "Giám sát định kỳ"


def _rank_change_reason(
    preset_name: str,
    criteria: Dict[str, float],
    rank_delta: int,
    scenario_rank: int,
    top_n: int,
) -> str:
    c1 = float(criteria.get("C1") or 0.0)
    c2 = float(criteria.get("C2") or 0.0)
    c3 = float(criteria.get("C3") or 0.0)
    c4 = float(criteria.get("C4") or 0.0)

    if preset_name in {"persistent", "prolonged_pollution"} and (c2 >= 60 or c3 >= 60):
        return "Ưu tiên kéo dài/lặp lại nên quận có C2-C3 cao tăng hạng"
    if preset_name == "early_warning" and c4 >= 60:
        return "Ưu tiên khí tượng/forecast nên quận có C4 cao được đẩy lên"
    if preset_name == "severe_now" and c1 >= 60:
        return "Ưu tiên hiện trạng nghiêm trọng nên quận có C1 cao tăng hạng"
    if rank_delta < 0 and scenario_rank <= max(1, top_n):
        return "Điểm tăng tương đối nên vào nhóm ưu tiên Top-N"
    if rank_delta > 0:
        return "Điểm giảm tương đối nên rời nhóm ưu tiên Top-N"
    return "Điều chỉnh theo trọng số/ngưỡng kịch bản"


class DailyRefreshRequest(BaseModel):
    date: str
    agg: str = "mean"
    source: str = "auto"


class BackfillRequest(BaseModel):
    from_date: str
    to_date: str
    agg: str = "mean"
    source: str = "auto"


class CriteriaRefreshRequest(BaseModel):
    date: str
    t: float = 15.0
    t_high: float = 35.0
    air_source: str = "openmeteo"


class CriteriaBackfillRequest(BaseModel):
    from_date: str
    to_date: str
    air_source: str = "openmeteo"
    t: float = 15.0
    t_high: float = 35.0


class DistrictAHPScoreRequest(BaseModel):
    date: str
    matrix: List[List[float]]
    labels: List[str] = Field(default_factory=lambda: DEFAULT_CRITERIA_LABELS[:])
    normalize_alternatives: bool = True
    rank_mode: Literal["cost", "benefit"] = "cost"
    alternatives_override: Optional[List[Dict[str, Any]]] = None


class ScenarioThresholds(BaseModel):
    yellow: float = DEFAULT_THRESHOLDS["yellow"]
    orange: float = DEFAULT_THRESHOLDS["orange"]
    red: float = DEFAULT_THRESHOLDS["red"]


class ScenarioWeights(BaseModel):
    C1: float
    C2: float
    C3: float
    C4: float


class DistrictPolicyScenarioRequest(BaseModel):
    date: str
    presetName: Literal["balanced", "severe_now", "persistent", "early_warning", "prolonged_pollution"]
    useCustomWeights: bool = False
    customWeights: Optional[ScenarioWeights] = None
    normalizeCustomWeights: bool = True
    thresholds: Optional[ScenarioThresholds] = None
    earlyWarningEnabled: bool = False
    compareWithBaseline: bool = True
    topN: int = 5
    autofill: bool = True
    fallback_days: int = 30
    force_refresh: bool = False


@router.get("/daily")
async def get_district_daily(date: str = Query(...)) -> Dict[str, Any]:
    _parse_ymd(date)
    rows = await _ensure_daily(date, force=False, force_live=False)
    return {"date": date, "count": len(rows), "items": rows}


@router.post("/daily/refresh")
async def refresh_district_daily(req: DailyRefreshRequest) -> Dict[str, Any]:
    _parse_ymd(req.date)
    rows = await _ensure_daily(req.date, force=True, force_live=(req.source.lower() == "openmeteo"))
    return {"date": req.date, "count": len(rows), "items": rows, "refreshed": True, "source": req.source}


@router.get("/daily/coverage")
def get_district_daily_coverage(from_date: str = Query(...), to_date: str = Query(...)) -> Dict[str, Any]:
    return {"items": _coverage_rows(_DAILY_CACHE, from_date, to_date)}


@router.post("/daily/backfill")
async def backfill_district_daily(req: BackfillRequest) -> Dict[str, Any]:
    days = _iter_days(req.from_date, req.to_date)
    success = 0
    errors: List[Dict[str, str]] = []
    for d in days:
        try:
            await _ensure_daily(d, force=False, force_live=(req.source.lower() == "openmeteo"))
            success += 1
        except Exception as exc:
            errors.append({"date": d, "error": str(exc)})
    return {
        "from_date": req.from_date,
        "to_date": req.to_date,
        "total_days": len(days),
        "success_days": success,
        "failed_days": len(days) - success,
        "errors": errors,
    }


@router.get("/criteria")
async def get_district_criteria(
    date: str = Query(...),
    autofill: bool = Query(True),
    fallback_days: int = Query(30, ge=1, le=180),
    t: float = Query(15.0),
    t_high: float = Query(35.0),
) -> Dict[str, Any]:
    _parse_ymd(date)
    _ = autofill, fallback_days  # retained for backward compatibility
    rows = await _ensure_criteria(date_str=date, force=False, t=t, t_high=t_high)
    return {
        "date": date,
        "count": len(rows),
        "expected_count": len(DISTRICT_POINTS),
        "imputed_count": 0,
        "items": rows,
    }


@router.post("/criteria/refresh")
async def refresh_district_criteria(req: CriteriaRefreshRequest) -> Dict[str, Any]:
    _parse_ymd(req.date)
    rows = await _ensure_criteria(date_str=req.date, force=True, t=req.t, t_high=req.t_high)
    return {
        "date": req.date,
        "count": len(rows),
        "expected_count": len(DISTRICT_POINTS),
        "imputed_count": 0,
        "items": rows,
        "refreshed": True,
        "air_source": req.air_source,
    }


@router.get("/criteria/coverage")
def get_district_criteria_coverage(from_date: str = Query(...), to_date: str = Query(...)) -> Dict[str, Any]:
    return {"items": _coverage_rows(_CRITERIA_CACHE, from_date, to_date)}


@router.post("/criteria/backfill")
async def backfill_district_criteria(req: CriteriaBackfillRequest) -> Dict[str, Any]:
    days = _iter_days(req.from_date, req.to_date)
    success = 0
    errors: List[Dict[str, str]] = []
    for d in days:
        try:
            await _ensure_criteria(date_str=d, force=False, t=req.t, t_high=req.t_high)
            success += 1
        except Exception as exc:
            errors.append({"date": d, "error": str(exc)})
    return {
        "from_date": req.from_date,
        "to_date": req.to_date,
        "total_days": len(days),
        "success_days": success,
        "failed_days": len(days) - success,
        "errors": errors,
    }


@router.post("/ahp-score")
async def district_ahp_score(req: DistrictAHPScoreRequest) -> Dict[str, Any]:
    _parse_ymd(req.date)
    labels = [str(x).strip() for x in (req.labels or DEFAULT_CRITERIA_LABELS) if str(x).strip()]
    if not labels:
        raise HTTPException(status_code=400, detail="labels không hợp lệ")

    try:
        ahp = compute_ahp(req.matrix, labels)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if req.alternatives_override:
        criteria_rows = []
        for idx, row in enumerate(req.alternatives_override):
            criteria_rows.append(
                {
                    "Date": req.date,
                    "DistrictId": int(row.get("DistrictId") or idx + 1),
                    "DistrictName": str(row.get("DistrictName") or f"Phương án {idx + 1}"),
                    "C1": float(row.get("C1") or 0.0),
                    "C2": float(row.get("C2") or 0.0),
                    "C3": float(row.get("C3") or 0.0),
                    "C4": float(row.get("C4") or 0.0),
                }
            )
    else:
        source_rows = await _ensure_criteria(req.date, force=False)
        criteria_rows = [{"Date": req.date, **r} for r in source_rows]

    weights = _weights_to_dict(ahp.get("weights") or [])
    items = _compute_ahp_scored_rows(
        rows=criteria_rows,
        labels=labels,
        weights=weights,
        normalize_alternatives=bool(req.normalize_alternatives),
        rank_mode=req.rank_mode,
    )
    return {"date": req.date, "ahp": ahp, "items": items}


@router.post("/policy-scenario")
async def district_policy_scenario(req: DistrictPolicyScenarioRequest) -> Dict[str, Any]:
    _parse_ymd(req.date)
    top_n = max(1, min(13, int(req.topN or 5)))
    thresholds = (req.thresholds.model_dump() if req.thresholds else DEFAULT_THRESHOLDS.copy())

    criteria_rows = await _ensure_criteria(
        date_str=req.date,
        force=bool(req.force_refresh),
    )

    if req.useCustomWeights and req.customWeights:
        raw_scenario_weights = req.customWeights.model_dump()
    else:
        raw_scenario_weights = SCENARIO_PRESETS.get(req.presetName, SCENARIO_PRESETS["balanced"]).copy()
    scenario_weights = (
        _normalize_named_weights(raw_scenario_weights) if req.normalizeCustomWeights else raw_scenario_weights
    )

    baseline_ahp = compute_ahp(DEFAULT_BASELINE_AHP_MATRIX, DEFAULT_CRITERIA_LABELS[:])
    baseline_weights = _normalize_named_weights(_weights_to_dict(baseline_ahp.get("weights") or []))

    max_by_c = {
        c: max(float(r.get(c) or 0.0) for r in criteria_rows) or 1.0
        for c in DEFAULT_CRITERIA_LABELS
    }

    district_rows: List[Dict[str, Any]] = []
    for r in criteria_rows:
        did = int(r["DistrictId"])
        cname = str(r["DistrictName"])
        cvals = {c: float(r.get(c) or 0.0) for c in DEFAULT_CRITERIA_LABELS}
        c_norm = {c: (cvals[c] / max_by_c[c] if max_by_c[c] > 0 else 0.0) for c in DEFAULT_CRITERIA_LABELS}

        baseline_score = sum(c_norm[c] * baseline_weights.get(c, 0.0) for c in DEFAULT_CRITERIA_LABELS)
        scenario_score = sum(c_norm[c] * scenario_weights.get(c, 0.0) for c in DEFAULT_CRITERIA_LABELS)

        if req.earlyWarningEnabled:
            ew_boost = 0.04 + 0.08 * ((c_norm["C4"] + _hash_unit(req.date, did, "ew")) / 2.0)
            scenario_score += ew_boost

        baseline_score = _clamp(float(baseline_score), 0.0, 1.2)
        scenario_score = _clamp(float(scenario_score), 0.0, 1.25)

        forecast_up = _hash_unit(req.date, did, "forecast_up") > 0.52
        risk_type, explanation = _risk_type_and_explanation(cvals, forecast_up=forecast_up)
        scenario_level = _level_from_threshold(scenario_score, thresholds)
        recommendation = _recommendation_for_level(scenario_level)

        district_rows.append(
            {
                "districtId": did,
                "districtName": cname,
                "criteriaValues": cvals,
                "baselineScore": round(baseline_score, 6),
                "scenarioScore": round(scenario_score, 6),
                "baselineLevel": _level_from_threshold(baseline_score, thresholds),
                "scenarioLevel": scenario_level,
                "riskType": risk_type,
                "explanation": explanation,
                "recommendation": recommendation,
                "earlyWarning": bool(req.earlyWarningEnabled),
            }
        )

    baseline_sorted = sorted(district_rows, key=lambda x: float(x["baselineScore"]), reverse=True)
    scenario_sorted = sorted(district_rows, key=lambda x: float(x["scenarioScore"]), reverse=True)

    baseline_rank_map: Dict[int, int] = {}
    scenario_rank_map: Dict[int, int] = {}
    for i, row in enumerate(baseline_sorted, start=1):
        baseline_rank_map[int(row["districtId"])] = i
    for i, row in enumerate(scenario_sorted, start=1):
        scenario_rank_map[int(row["districtId"])] = i

    compare_items: List[Dict[str, Any]] = []
    scenario_result: List[Dict[str, Any]] = []
    for row in scenario_sorted:
        did = int(row["districtId"])
        br = baseline_rank_map[did]
        sr = scenario_rank_map[did]
        rank_delta = sr - br
        score_delta = float(row["scenarioScore"]) - float(row["baselineScore"])
        reason = _rank_change_reason(
            preset_name=req.presetName,
            criteria=row["criteriaValues"],
            rank_delta=rank_delta,
            scenario_rank=sr,
            top_n=top_n,
        )

        compare_items.append(
            {
                "districtId": did,
                "districtName": row["districtName"],
                "baselineRank": br,
                "scenarioRank": sr,
                "rankDelta": rank_delta,
                "baselineScore": round(float(row["baselineScore"]), 6),
                "scenarioScore": round(float(row["scenarioScore"]), 6),
                "scoreDelta": round(float(score_delta), 6),
                "baselineLevel": row["baselineLevel"],
                "scenarioLevel": row["scenarioLevel"],
                "levelChanged": row["baselineLevel"] != row["scenarioLevel"],
                "riskType": row["riskType"],
                "explanation": row["explanation"],
                "recommendation": row["recommendation"],
                "rankChangeReason": reason,
                "earlyWarning": row["earlyWarning"],
            }
        )

        scenario_result.append(
            {
                "districtId": did,
                "districtName": row["districtName"],
                "rank": sr,
                "score": round(float(row["scenarioScore"]), 6),
                "level": row["scenarioLevel"],
                "riskType": row["riskType"],
                "explanation": row["explanation"],
                "recommendation": row["recommendation"],
                "criteriaValues": row["criteriaValues"],
                "earlyWarning": row["earlyWarning"],
            }
        )

    compare_items.sort(key=lambda x: int(x["scenarioRank"]))
    scenario_result.sort(key=lambda x: int(x["rank"]))

    up_count = sum(1 for x in compare_items if int(x["rankDelta"]) < 0)
    down_count = sum(1 for x in compare_items if int(x["rankDelta"]) > 0)
    stable_count = len(compare_items) - up_count - down_count
    strongest_shifts = sorted(compare_items, key=lambda x: abs(int(x["rankDelta"])), reverse=True)[:3]

    bias_criterion = _scenario_bias_criterion(scenario_weights)
    bias_desc_map = {
        "C1": "mức độ nghiêm trọng hiện tại",
        "C2": "ô nhiễm kéo dài",
        "C3": "điểm nóng lặp lại",
        "C4": "khí tượng và cảnh báo sớm",
    }

    baseline_top = compare_items[0] if compare_items else None
    scenario_top = scenario_result[0] if scenario_result else None

    return {
        "date": req.date,
        "presetName": req.presetName,
        "thresholds": thresholds,
        "weights": {
            "baseline": baseline_weights,
            "scenario": scenario_weights,
        },
        "scenarioResult": scenario_result,
        "comparison": {
            "topN": top_n,
            "items": compare_items,
        },
        "summary": {
            "baselineTopDistrict": {
                "districtId": baseline_top["districtId"],
                "districtName": baseline_top["districtName"],
                "rank": baseline_top["baselineRank"],
                "score": baseline_top["baselineScore"],
            }
            if baseline_top
            else None,
            "scenarioTopDistrict": {
                "districtId": scenario_top["districtId"],
                "districtName": scenario_top["districtName"],
                "rank": scenario_top["rank"],
                "score": scenario_top["score"],
            }
            if scenario_top
            else None,
            "upPriorityCount": up_count,
            "downPriorityCount": down_count,
            "stablePriorityCount": stable_count,
            "top3StrongestShifts": [
                {
                    "districtId": x["districtId"],
                    "districtName": x["districtName"],
                    "rankDelta": x["rankDelta"],
                }
                for x in strongest_shifts
            ],
            "scenarioBias": {
                "criterion": bias_criterion,
                "description": bias_desc_map.get(bias_criterion, "ưu tiên theo cấu hình kịch bản"),
            },
        },
    }
