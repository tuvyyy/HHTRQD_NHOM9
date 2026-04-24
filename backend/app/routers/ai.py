from __future__ import annotations

import os
import re
import unicodedata
from datetime import date as date_cls
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Literal, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.routers.district import (
    DEFAULT_BASELINE_AHP_MATRIX,
    DEFAULT_CRITERIA_LABELS,
    _compute_ahp_scored_rows,
    _compute_forecast_rank_rows,
    _ensure_criteria,
    _weights_to_dict,
)
from app.services.ahp import compute_ahp
from app.services.openmeteo_service import fetch_hourly_async
from app.services.risk_scoring import compute_score_0_100

router = APIRouter(prefix="/ai", tags=["AI"])

DEFAULT_TZ = "Asia/Ho_Chi_Minh"
VN_OFFSET = "+07:00"
LOCAL_TZ = timezone(timedelta(hours=7))

DEFAULT_POLLUTANT_WEIGHTS = {
    "PM2.5": 0.50,
    "PM10": 0.25,
    "NO2": 0.12,
    "O3": 0.08,
    "CO": 0.05,
}

CRITERIA_LABELS: Dict[str, str] = {
    "C1": "mức vượt chuẩn (bụi mịn/chất ô nhiễm chính)",
    "C2": "thời gian duy trì ô nhiễm cao",
    "C3": "tần suất vượt ngưỡng lặp lại",
    "C4": "điều kiện khí tượng bất lợi",
}


class AIForecastRequest(BaseModel):
    lat: float
    lon: float
    horizon_hours: int = Field(24, ge=6, le=168)
    weights: Dict[str, float] = Field(default_factory=dict)
    threshold: float = Field(60.0, ge=0.0, le=100.0)
    model: Optional[str] = "openmeteo_baseline"


class AIForecastPoint(BaseModel):
    time: str
    risk_score_0_100: float


class AIForecastResponse(BaseModel):
    warning: bool
    max_risk_score: float
    time_of_max: str
    current_risk_score: Optional[float] = None
    current_level: Optional[str] = None
    current_time: Optional[str] = None
    confidence_label: str
    confidence_0_100: float
    series: List[AIForecastPoint]
    baseline_series: List[AIForecastPoint]


class AIChatMessage(BaseModel):
    role: Literal["system", "user", "assistant"]
    content: str


class AIChatDistrictRow(BaseModel):
    districtName: str
    rank: int
    score: float
    C1: Optional[float] = None
    C2: Optional[float] = None
    C3: Optional[float] = None
    C4: Optional[float] = None


class AIChatForecastPoint(BaseModel):
    time: str
    risk_score_0_100: float


class AIChatRequest(BaseModel):
    messages: List[AIChatMessage] = Field(default_factory=list)
    lat: Optional[float] = None
    lon: Optional[float] = None
    hours: Optional[int] = Field(default=None, ge=1, le=168)
    weights: Optional[Dict[str, float]] = None
    decision_date: Optional[str] = None
    ranking_source: Optional[str] = None
    district_rows: Optional[List[AIChatDistrictRow]] = None
    district_forecast_rows: Optional[List[AIChatDistrictRow]] = None
    district_forecast_horizon_hours: Optional[int] = None
    forecast_series: Optional[List[AIChatForecastPoint]] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    temperature: float = Field(default=0.3, ge=0.0, le=1.5)


class AIChatResponse(BaseModel):
    provider: str
    model: str
    reply: str


def _iso_with_offset(raw: str, tz: str = DEFAULT_TZ) -> str:
    if not isinstance(raw, str):
        return str(raw)
    text = raw.strip()
    if not text:
        return text
    if text.endswith("Z"):
        return text
    tail = text[-6:]
    if "+" in tail or "-" in tail:
        return text
    if tz == DEFAULT_TZ:
        return text + VN_OFFSET
    return text


def _normalize_for_match(text: str) -> str:
    source = (text or "").strip().lower()
    if not source:
        return ""
    source = source.replace("đ", "d")
    source = unicodedata.normalize("NFKD", source)
    source = "".join(ch for ch in source if unicodedata.category(ch) != "Mn")
    source = re.sub(r"[^a-z0-9\s]", " ", source)
    source = re.sub(r"\s+", " ", source).strip()
    return source


def _parse_iso_time(raw: str) -> Optional[datetime]:
    text = (raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except Exception:
        return None


def _to_local_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(LOCAL_TZ).replace(tzinfo=None)


def _now_local_hour() -> datetime:
    return datetime.now(LOCAL_TZ).replace(minute=0, second=0, microsecond=0, tzinfo=None)


def _last_user_message(messages: List[AIChatMessage]) -> str:
    for msg in reversed(messages or []):
        if msg.role == "user" and (msg.content or "").strip():
            return msg.content.strip()
    return ""


def _score_level(score: float) -> str:
    if score < 25:
        return "Xanh"
    if score < 50:
        return "Vàng"
    if score < 75:
        return "Cam"
    return "Đỏ"


INTENT_PRIORITY: List[str] = [
    "emergency_health",
    "sensor_vs_reality",
    "system_dependency",
    "data_lineage",
    "data_reliability",
    "warning_vs_ahp_comparison",
    "warning_mechanism",
    "short_duration_outdoor_advice",
    "district_health_precaution",
    "operational_advice",
    "short_term_risk",
    "ahp_current",
    "general_explanation",
]

INTENT_PATTERNS: Dict[str, List[str]] = {
    "emergency_health": [
        "kho tho",
        "dau nguc",
        "chong mat",
        "buon non",
        "sap xiu",
        "muon xiu",
        "xay xam",
        "met la",
        "khong tho duoc",
    ],
    "sensor_vs_reality": [
        "ngoai duong ngot ngat hon",
        "nhin mu mit hon du lieu",
        "thuc te xau hon man hinh",
        "mat thay khac cam bien",
        "cam giac ngoai duong khac app",
        "app chua bao xau",
        "man hinh chua theo kip ngoai duong",
        "ngoai duong kho chiu hon app",
    ],
    "system_dependency": [
        "du lieu moi co anh huong",
        "du lieu moi nay co anh huong",
        "co anh huong toi du bao",
        "co anh huong toi du doan",
        "anh huong den du bao 3 ngay",
        "anh huong den du doan 3 ngay",
        "du lieu ahp moi co tac dong",
        "ahp moi co anh huong",
        "du lieu nay co ke thua sang du bao",
        "xep hang ahp co anh huong du bao",
    ],
    "data_lineage": [
        "du lieu cua ban lay tu dau",
        "du lieu cua ai lay tu dau",
        "lay du lieu tu sau khi co ahp",
        "du lieu co phai sau ahp",
        "ai co dung ket qua ahp khong",
        "ahp co phai dau vao cho ai",
        "ai va ahp lien quan nhu the nao",
        "du lieu ahp va ai",
        "nguon du lieu cua ai",
        "du lieu lay sau khi co ahp",
    ],
    "data_reliability": [
        "du lieu tre",
        "du lieu thieu",
        "cam bien cham",
        "cap nhat cham",
        "man hinh chua theo kip",
        "forecast cu",
        "timestamp cu",
        "con dung duoc toi muc nao",
        "dang tin toi muc nao",
        "doc ket qua kieu nao khi du lieu cham",
        "du lieu chua cap nhat",
        "do tin cay",
    ],
    "warning_mechanism": [
        "he thong canh bao som theo cach nao",
        "co che canh bao",
        "canh bao som dua vao gi",
        "phan biet kieu gi giua cho dang o nhiem",
        "he thong phan biet giua cho dang o nhiem",
        "he thong dua vao yeu to nao",
        "cach he thong nhan dien nguy co tang",
    ],
    "warning_vs_ahp_comparison": [
        "ahp khac gi voi canh bao som",
        "ahp va ai khac nhau",
        "hien trang va ngan han khac nhau",
        "chi xem ahp thi khac gi",
        "phan biet ahp voi canh bao som",
        "cho dang xau bay gio va cho sap tang",
        "ahp va canh bao som",
        "ahp giup toi biet",
        "con ai giup toi biet",
        "ahp va ai",
    ],
    "operational_advice": [
        "nen lam gi",
        "can lam gi",
        "can chu y gi",
        "luu y gi",
        "giu cho on",
        "tu giu the nao",
        "co gi can can than khong",
        "lam sao cho an toan",
        "do bi phoi nhiem",
        "giam phoi nhiem",
        "bao ve suc khoe",
        "dam bao suc khoe",
        "phong tranh",
        "neu phai ra ngoai",
        "co nen",
    ],
    "district_health_precaution": [
        "o quan",
        "tai quan",
        "khu vuc quan",
        "o khu vuc",
        "o q",
        "tai q",
    ],
    "short_duration_outdoor_advice": [
        "20 30 phut",
        "20-30 phut",
        "20 phut",
        "30 phut",
        "khong o lau",
        "ngoai troi mot luc",
        "ra ngoai mot chut",
        "chay qua vai viec",
        "nua gio",
        "it phut",
    ],
    "short_term_risk": [
        "6 gio toi",
        "24 gio toi",
        "trong vai gio toi",
        "ngan han",
        "co nguy co tang khong",
        "khung gio nao nguy co cao",
        "xu huong 3 gio toi",
        "du bao gan",
        "tuan toi",
        "7 ngay toi",
    ],
    "ahp_current": [
        "hien tai quan nao",
        "top 3 hien tai",
        "xep hang hien tai",
        "diem ahp hien tai",
        "vi sao quan",
        "tieu chi nao dang chi phoi",
        "chi nhin ahp",
        "uu tien theo doi nhat hien tai",
        "it o nhiem nhat",
        "it o nhiem",
        "o nhiem nhat",
        "dung thu",
        "hang thu",
        "thu may",
    ],
    "general_explanation": [
        "he thong nay la gi",
        "he thong nay ho tro gi",
        "giai thich de hieu",
        "tong quan he thong",
        "he thong phan biet the nao",
    ],
}

DATA_QUALITY_WORDS = (
    "du lieu tre",
    "du lieu thieu",
    "cam bien cham",
    "forecast cu",
    "timestamp cu",
    "cap nhat cham",
    "lech thoi gian",
    "tre",
    "thieu",
    "do tin cay",
    "dang tin",
)

ADVICE_WORDS = (
    "nen",
    "can",
    "luu y",
    "chu y",
    "giam phoi nhiem",
    "bao ve suc khoe",
    "giu cho on",
    "an toan",
    "phong tranh",
)

AHp_WORDS = (
    "ahp",
    "xep hang",
    "hang",
    "diem",
    "hien trang",
    "top",
)

SHORT_TERM_WORDS = (
    "ngan han",
    "6 gio",
    "24 gio",
    "canh bao som",
    "xu huong",
    "sap tang",
    "du bao",
)

WARNING_WORDS = (
    "canh bao",
    "canh bao som",
    "co che",
    "phan biet",
)

SENSOR_REALITY_LOCAL_WORDS = (
    "ngoai duong",
    "thuc te",
    "mat thay",
    "nhin",
    "cam giac",
)

SENSOR_REALITY_MISMATCH_WORDS = (
    "app",
    "man hinh",
    "cam bien",
    "he thong",
    "chua bao xau",
    "khac",
    "ngot ngat",
    "mu mit",
)


def _normalized_district_rows(rows: Optional[List[AIChatDistrictRow]]) -> List[AIChatDistrictRow]:
    normalized: List[AIChatDistrictRow] = []
    for idx, row in enumerate(rows or []):
        name = (row.districtName or "").strip()
        if not name:
            continue
        rank = int(row.rank or 0)
        if rank <= 0:
            rank = 999 + idx
        normalized.append(
            AIChatDistrictRow(
                districtName=name,
                rank=rank,
                score=float(row.score or 0.0),
                C1=float(row.C1 or 0.0),
                C2=float(row.C2 or 0.0),
                C3=float(row.C3 or 0.0),
                C4=float(row.C4 or 0.0),
            )
        )
    normalized.sort(key=lambda item: (item.rank, -item.score))
    return normalized


async def _load_rank_rows_for_date(date_text: str) -> List[AIChatDistrictRow]:
    raw_date = (date_text or "").strip()
    if not raw_date:
        return []
    try:
        date_cls.fromisoformat(raw_date)
    except Exception:
        return []

    criteria_rows = await _ensure_criteria(date_str=raw_date, force=False)
    if not criteria_rows:
        return []

    base_ahp = compute_ahp(DEFAULT_BASELINE_AHP_MATRIX, DEFAULT_CRITERIA_LABELS[:])
    weights = _weights_to_dict(base_ahp.get("weights") or [])
    rows = [{"Date": raw_date, **r} for r in criteria_rows]
    scored = _compute_ahp_scored_rows(
        rows=rows,
        labels=DEFAULT_CRITERIA_LABELS[:],
        weights=weights,
        normalize_alternatives=True,
        rank_mode="cost",
    )

    out: List[AIChatDistrictRow] = []
    for row in scored:
        out.append(
            AIChatDistrictRow(
                districtName=str(row.get("DistrictName") or "").strip(),
                rank=int(row.get("Rank") or 0),
                score=float(row.get("AHPScore") or row.get("Score") or 0.0),
                C1=float(row.get("C1") or 0.0),
                C2=float(row.get("C2") or 0.0),
                C3=float(row.get("C3") or 0.0),
                C4=float(row.get("C4") or 0.0),
            )
        )
    return _normalized_district_rows(out)


def _safe_hourly_value(arr: List[Optional[float]], idx: int) -> float:
    try:
        val = arr[idx]
    except Exception:
        return 0.0
    return float(val or 0.0)


def _score_from_hourly_index(
    hourly: Dict[str, List[Optional[float]]],
    idx: int,
    weights: Dict[str, float],
) -> float:
    values = {
        "pm2_5": _safe_hourly_value(hourly.get("pm2_5", []), idx),
        "pm10": _safe_hourly_value(hourly.get("pm10", []), idx),
        "no2": _safe_hourly_value(hourly.get("nitrogen_dioxide", []), idx),
        "o3": _safe_hourly_value(hourly.get("ozone", []), idx),
        "co": _safe_hourly_value(hourly.get("carbon_monoxide", []), idx),
    }
    scored = compute_score_0_100(values, weights or {})
    return float(scored.get("score_0_100") or 0.0)


def _future_hour_indexes(times: List[str], horizon_hours: int) -> List[int]:
    start = _now_local_hour()
    indexes: List[int] = []
    for idx, raw in enumerate(times):
        dt = _parse_iso_time(str(raw))
        if dt is None:
            continue
        if _to_local_naive(dt) >= start:
            indexes.append(idx)
    if not indexes:
        count = max(1, min(horizon_hours, len(times)))
        return list(range(max(0, len(times) - count), len(times)))
    return indexes[: max(1, horizon_hours)]


async def _build_forecast_payload(
    lat: float,
    lon: float,
    horizon_hours: int,
    weights: Dict[str, float],
) -> Tuple[List[AIForecastPoint], List[AIForecastPoint]]:
    pack = await fetch_hourly_async(
        lat=float(lat),
        lon=float(lon),
        hours=max(24, int(horizon_hours)),
        past_days=1,
        timezone=DEFAULT_TZ,
    )
    hourly = pack.get("hourly") or {}
    times: List[str] = list(hourly.get("time") or [])
    if not times:
        return [], []

    idxs = _future_hour_indexes(times, horizon_hours=horizon_hours)
    if not idxs:
        return [], []

    weighted_series: List[AIForecastPoint] = []
    baseline_series: List[AIForecastPoint] = []
    for idx in idxs:
        t = _iso_with_offset(str(times[idx]), tz=DEFAULT_TZ)
        score = _score_from_hourly_index(hourly, idx, weights or {})
        base_score = _score_from_hourly_index(hourly, idx, DEFAULT_POLLUTANT_WEIGHTS)
        weighted_series.append(AIForecastPoint(time=t, risk_score_0_100=round(score, 2)))
        baseline_series.append(AIForecastPoint(time=t, risk_score_0_100=round(base_score, 2)))
    return weighted_series, baseline_series


def _confidence_from_series(series: List[AIForecastPoint]) -> Tuple[str, float]:
    n = len(series or [])
    if n >= 24:
        return "Cao", 88.0
    if n >= 12:
        return "Khá", 78.0
    if n >= 6:
        return "Trung bình", 66.0
    if n >= 2:
        return "Thấp", 54.0
    return "Rất thấp", 40.0


@router.post("/forecast", response_model=AIForecastResponse)
async def ai_forecast(req: AIForecastRequest):
    try:
        series, baseline_series = await _build_forecast_payload(
            lat=req.lat,
            lon=req.lon,
            horizon_hours=req.horizon_hours,
            weights=req.weights or {},
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Không lấy được forecast: {exc}") from exc

    if not series:
        raise HTTPException(status_code=502, detail="Forecast rỗng, chưa đủ dữ liệu để tính.")

    max_point = max(series, key=lambda p: float(p.risk_score_0_100))
    current_point = series[0]
    confidence_label, confidence_score = _confidence_from_series(series)

    return AIForecastResponse(
        warning=float(max_point.risk_score_0_100) >= float(req.threshold),
        max_risk_score=round(float(max_point.risk_score_0_100), 2),
        time_of_max=max_point.time,
        current_risk_score=round(float(current_point.risk_score_0_100), 2),
        current_level=_score_level(float(current_point.risk_score_0_100)),
        current_time=current_point.time,
        confidence_label=confidence_label,
        confidence_0_100=round(confidence_score, 1),
        series=series,
        baseline_series=baseline_series,
    )


async def _auto_attach_forecast_series(req: AIChatRequest) -> None:
    if req.forecast_series and len(req.forecast_series) >= 2:
        return
    if req.lat is None or req.lon is None:
        return
    hours = int(req.hours or 24)
    hours = max(6, min(168, hours))
    try:
        series, _ = await _build_forecast_payload(
            lat=float(req.lat),
            lon=float(req.lon),
            horizon_hours=hours,
            weights=req.weights or {},
        )
    except Exception:
        return
    req.forecast_series = [
        AIChatForecastPoint(time=item.time, risk_score_0_100=item.risk_score_0_100)
        for item in series
    ]


def _district_number_from_name(name: str) -> Optional[str]:
    key = _normalize_for_match(name)
    if not key:
        return None
    m = re.search(r"\bquan\s*(\d{1,2})\b", key)
    if m:
        return str(int(m.group(1)))
    return None


def _district_aliases(name: str) -> List[str]:
    key = _normalize_for_match(name)
    if not key:
        return []
    aliases = {key}
    num = _district_number_from_name(name)
    if num:
        aliases.update({f"quan {num}", f"q {num}", f"q{num}"})
    # Do not add bare numeric alias ("3", "10", "11", ...) to avoid collisions
    # with time expressions like "3 ngày", "10 giờ", etc.
    if key.startswith("quan "):
        tail = key.replace("quan ", "", 1).strip()
        if tail and not tail.isdigit():
            aliases.add(tail)
    aliases.add(key.replace(" ", ""))
    return [a for a in aliases if a]


def _extract_target_districts(text: str, rows: List[AIChatDistrictRow]) -> List[AIChatDistrictRow]:
    if not rows:
        return []

    q = _normalize_for_match(text)
    if not q:
        return []

    by_number: Dict[str, AIChatDistrictRow] = {}
    alias_entries: List[Tuple[str, AIChatDistrictRow]] = []

    for row in rows:
        num = _district_number_from_name(row.districtName)
        if num:
            by_number[num] = row
        for alias in _district_aliases(row.districtName):
            alias_entries.append((alias, row))

    hits: List[Tuple[int, AIChatDistrictRow]] = []

    number_patterns = (
        r"\bq\s*\.?\s*(\d{1,2})\b",
        r"\bquan\s*(\d{1,2})\b",
    )
    for pattern in number_patterns:
        for m in re.finditer(pattern, q):
            num = str(int(m.group(1)))
            row = by_number.get(num)
            if row is not None:
                hits.append((m.start(), row))

    # Exact alias matching with word boundaries to avoid Quận 11 => Quận 1 collision.
    for alias, row in sorted(alias_entries, key=lambda item: len(item[0]), reverse=True):
        pattern = rf"\b{re.escape(alias)}\b"
        for m in re.finditer(pattern, q):
            hits.append((m.start(), row))

    hits.sort(key=lambda item: item[0])
    uniq: List[AIChatDistrictRow] = []
    seen: set[str] = set()
    for _, row in hits:
        key = _normalize_for_match(row.districtName)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(row)
    return uniq


def _extract_target_district(text: str, rows: List[AIChatDistrictRow]) -> Optional[AIChatDistrictRow]:
    found = _extract_target_districts(text, rows)
    return found[0] if found else None


def _extract_duration_minutes(text: str) -> Optional[int]:
    q = _normalize_for_match(text)
    if not q:
        return None
    if "nua gio" in q:
        return 30
    m_range = re.search(r"\b(\d{1,3})\s*-\s*(\d{1,3})\s*phut\b", q)
    if m_range:
        lo = int(m_range.group(1))
        hi = int(m_range.group(2))
        return max(1, min(180, int((lo + hi) / 2)))
    m = re.search(r"\b(\d{1,3})\s*phut\b", q)
    if not m:
        return None
    value = int(m.group(1))
    return max(1, min(180, value))


def _extract_time_scope(text: str) -> Dict[str, Optional[object]]:
    q = _normalize_for_match(text)
    duration = _extract_duration_minutes(text)
    if any(token in q for token in ("7 ngay", "tuan toi", "trong tuan", "ca tuan", "7 ngay toi")):
        return {"kind": "weekly", "duration_minutes": duration, "days": 7, "hours": 168}
    m_day = re.search(r"\b(\d{1,2})\s*ngay\b", q)
    if m_day:
        days = max(1, min(14, int(m_day.group(1))))
        if days >= 7:
            return {"kind": "weekly", "duration_minutes": duration, "days": days, "hours": days * 24}
        return {"kind": "days", "duration_minutes": duration, "days": days, "hours": days * 24}
    m_hour = re.search(r"\b(\d{1,2})\s*gio\b", q)
    if m_hour:
        hours = max(1, min(168, int(m_hour.group(1))))
        return {"kind": "hours", "duration_minutes": duration, "days": None, "hours": hours}
    if duration is not None:
        return {"kind": "minutes", "duration_minutes": duration, "days": None, "hours": max(1, duration // 60)}
    return {"kind": None, "duration_minutes": duration, "days": None, "hours": None}


def _pattern_score(q: str, patterns: List[str]) -> float:
    score = 0.0
    for phrase in patterns:
        if not phrase:
            continue
        if phrase in q:
            words = len(phrase.split())
            score += 1.0 + min(1.5, words * 0.2)
    return score


def _extract_entities(question: str, rows: List[AIChatDistrictRow]) -> Dict[str, object]:
    q = _normalize_for_match(question)
    districts = _extract_target_districts(question, rows)
    max_rank_value = 0
    if rows:
        max_rank_value = max(int(r.rank or 0) for r in rows)
    requested_rank = _extract_requested_rank(question, max(max_rank_value, len(rows), 1))
    duration_minutes = _extract_duration_minutes(question)
    time_scope = _extract_time_scope(question)

    has_symptom = any(token in q for token in INTENT_PATTERNS["emergency_health"])
    has_data_quality = any(token in q for token in DATA_QUALITY_WORDS)
    has_advice_words = any(token in q for token in ADVICE_WORDS)
    has_ahp_words = any(token in q for token in AHp_WORDS)
    has_short_words = any(token in q for token in SHORT_TERM_WORDS)
    has_warning_words = any(token in q for token in WARNING_WORDS)
    asks_compare = any(token in q for token in ("khac gi", "so sanh", "phan biet", "so voi"))
    scope_kind = str(time_scope.get("kind") or "")
    scope_hours = int(time_scope.get("hours") or 0)
    asks_weekly = scope_kind == "weekly"
    asks_multi_day = scope_kind in {"days", "weekly"} or scope_hours >= 48

    has_short_duration_hint = any(token in q for token in ("khong o lau", "vai viec", "mot luc", "it phut"))

    has_local_words = any(token in q for token in SENSOR_REALITY_LOCAL_WORDS)
    has_mismatch_words = any(token in q for token in SENSOR_REALITY_MISMATCH_WORDS)
    has_sensor_discrepancy = has_local_words and has_mismatch_words

    asks_warning_vs_ahp = (
        (has_ahp_words and has_warning_words and asks_compare)
        or (has_ahp_words and " ai " in f" {q} " and ("con ai" in q or "ahp giup" in q))
        or "cho dang o nhiem bay gio" in q
        or "cho sap tang" in q
    )
    lineage_tokens = (
        "du lieu cua ban",
        "du lieu cua ai",
        "lay tu dau",
        "sau khi co ahp",
        "sau khi co tieu chi ahp",
        "du lieu sau khi co ahp",
        "sau ahp",
        "ahp co phai dau vao",
        "ai co dung ket qua ahp",
        "nguon du lieu",
    )
    asks_data_lineage = (
        any(token in q for token in lineage_tokens)
        or bool(re.search(r"sau khi co .*ahp", q))
        or (
            ("lay" in q or "dua" in q)
            and "du lieu" in q
            and "ahp" in q
            and ("tu dau" in q or "co phai" in q or "sau khi co" in q or "dung ket qua" in q)
        )
    ) and ("ahp" in q or "ai" in q or "forecast" in q or "du lieu" in q)
    asks_warning_mechanism = (
        has_warning_words
        and ("the nao" in q or "ra sao" in q or "dua vao" in q or "phan biet kieu gi" in q)
        and not asks_warning_vs_ahp
    )
    asks_ahp_current = has_ahp_words or any(
        token in q
        for token in (
            "top 3",
            "hang 1",
            "hang",
            "thu",
            "dung thu",
            "xep hang hien tai",
            "o nhiem nhat",
            "it o nhiem nhat",
            "it o nhiem",
            "tieu chi nao chi phoi",
            "uu tien theo doi nhat hien tai",
        )
    )
    asks_short_term = asks_multi_day or has_short_words or any(
        token in q for token in ("6 gio toi", "24 gio toi", "trong vai gio toi", "xu huong 3 gio toi")
    )

    return {
        "normalized": q,
        "districts": districts,
        "has_district": bool(districts),
        "duration_minutes": duration_minutes,
        "time_scope": time_scope,
        "has_symptom": has_symptom,
        "has_data_quality": has_data_quality,
        "has_sensor_discrepancy": has_sensor_discrepancy,
        "has_advice_words": has_advice_words,
        "asks_data_lineage": asks_data_lineage,
        "asks_warning_vs_ahp": asks_warning_vs_ahp,
        "asks_warning_mechanism": asks_warning_mechanism,
        "asks_ahp_current": asks_ahp_current,
        "requested_rank": requested_rank,
        "asks_short_term": asks_short_term,
        "asks_multi_day": asks_multi_day,
        "asks_weekly": asks_weekly,
        "has_short_duration_hint": has_short_duration_hint,
    }


def _classify_chat_intent(text: str, rows: Optional[List[AIChatDistrictRow]] = None) -> Dict[str, object]:
    safe_rows = rows or []
    entities = _extract_entities(text, safe_rows)
    q = str(entities["normalized"])
    raw_q = (text or "").strip().lower()

    scores: Dict[str, float] = {
        intent: _pattern_score(q, patterns) for intent, patterns in INTENT_PATTERNS.items()
    }

    if entities["has_symptom"]:
        scores["emergency_health"] += 5.0

    if entities["has_sensor_discrepancy"]:
        scores["sensor_vs_reality"] += 7.0
        scores["data_reliability"] += 1.5

    if entities.get("asks_data_lineage"):
        scores["data_lineage"] += 7.0
        scores["data_reliability"] += 0.5
        scores["warning_vs_ahp_comparison"] += 1.0

    if entities["has_data_quality"]:
        scores["data_reliability"] += 5.0

    if (
        ("ahp" in q and ("anh huong" in q or "tac dong" in q) and ("du bao" in q or "du doan" in q))
        or ("ahp" in raw_q and ("ảnh hưởng" in raw_q or "tác động" in raw_q) and ("dự báo" in raw_q or "dự đoán" in raw_q))
    ):
        scores["system_dependency"] += 7.0

    if (
        (
            ("du lieu moi" in q or "du lieu nay" in q)
            and ("anh huong" in q or "tac dong" in q)
            and ("du bao" in q or "du doan" in q or "3 ngay" in q or "7 ngay" in q)
        )
        or (
            ("dữ liệu mới" in raw_q or "dữ liệu này" in raw_q)
            and ("ảnh hưởng" in raw_q or "tác động" in raw_q)
            and ("dự báo" in raw_q or "dự đoán" in raw_q or "3 ngày" in raw_q or "7 ngày" in raw_q)
        )
    ):
        scores["system_dependency"] += 8.0

    if entities["asks_warning_vs_ahp"]:
        scores["warning_vs_ahp_comparison"] += 6.0

    if entities["asks_warning_mechanism"]:
        scores["warning_mechanism"] += 5.0

    duration_minutes = entities.get("duration_minutes")
    if duration_minutes is not None and int(duration_minutes) <= 60:
        scores["short_duration_outdoor_advice"] += 5.0
    elif entities.get("has_short_duration_hint"):
        scores["short_duration_outdoor_advice"] += 4.0

    if entities["has_district"] and entities["has_advice_words"]:
        scores["district_health_precaution"] += 5.0

    if entities["has_advice_words"] and not entities["has_district"]:
        scores["operational_advice"] += 3.0

    if entities["asks_short_term"]:
        scores["short_term_risk"] += 4.0
    if entities.get("asks_multi_day"):
        scores["short_term_risk"] += 5.0

    if entities["asks_ahp_current"] and not entities["asks_warning_vs_ahp"]:
        scores["ahp_current"] += 4.0
    if entities.get("requested_rank") is not None:
        scores["ahp_current"] += 3.0

    if entities["asks_weekly"]:
        scores["short_term_risk"] += 2.0

    if max(scores.values() or [0.0]) <= 0.0:
        scores["general_explanation"] = 1.0

    best_intent = "general_explanation"
    best_score = -1.0
    for intent in INTENT_PRIORITY:
        value = float(scores.get(intent, 0.0))
        if value > best_score:
            best_intent = intent
            best_score = value

    strong_signal = (
        entities["has_sensor_discrepancy"]
        or entities["has_data_quality"]
        or bool(entities.get("asks_data_lineage"))
        or entities["asks_warning_vs_ahp"]
        or entities["has_district"]
        or entities["has_symptom"]
    )
    if best_score < 2.0 and not strong_signal:
        best_intent = "general_explanation"
        best_score = float(scores.get("general_explanation", 1.0))

    return {
        "intent": best_intent,
        "score": round(best_score, 3),
        "scores": scores,
        "entities": entities,
    }


def _driver_breakdown(row: AIChatDistrictRow) -> List[Tuple[str, float]]:
    vals = {
        "C1": float(row.C1 or 0.0),
        "C2": float(row.C2 or 0.0),
        "C3": float(row.C3 or 0.0),
        "C4": float(row.C4 or 0.0),
    }
    total = sum(max(v, 0.0) for v in vals.values()) or 1.0
    pairs = []
    for key, value in vals.items():
        pairs.append((key, max(value, 0.0) / total))
    pairs.sort(key=lambda item: item[1], reverse=True)
    return pairs


def _top_drivers_text(row: AIChatDistrictRow, limit: int = 2) -> str:
    parts = []
    for key, share in _driver_breakdown(row)[: max(1, limit)]:
        parts.append(f"**{CRITERIA_LABELS.get(key, key)}** (~{share * 100:.1f}%)")
    return ", ".join(parts)


def _rank_bucket(row: AIChatDistrictRow) -> str:
    if row.rank <= 3:
        return "cao"
    if row.rank <= 7:
        return "trung bình"
    return "thấp"


def _sorted_rows(rows: List[AIChatDistrictRow]) -> List[AIChatDistrictRow]:
    return sorted(rows, key=lambda item: (item.rank, -item.score))


def _forecast_quality(req: AIChatRequest) -> Dict[str, object]:
    points = list(req.forecast_series or [])
    if len(points) < 2:
        return {
            "status": "missing",
            "note": "Chưa có chuỗi **forecast** đủ mới, nên đánh giá **ngắn hạn** chỉ ở mức tham khảo.",
            "latest": None,
        }

    parsed = [_parse_iso_time(str(item.time)) for item in points]
    parsed = [dt for dt in parsed if dt is not None]
    if not parsed:
        return {
            "status": "missing",
            "note": "Dữ liệu **forecast** không đọc được mốc thời gian.",
            "latest": None,
        }

    latest = max(_to_local_naive(dt) for dt in parsed)
    now_local = datetime.now().replace(microsecond=0)

    stale_by_lag = (now_local - latest) > timedelta(hours=8)
    stale_by_date = latest.date() < now_local.date()
    stale_by_decision = False
    if (req.decision_date or "").strip():
        try:
            d0 = date_cls.fromisoformat(str(req.decision_date).strip())
            stale_by_decision = latest.date() < d0
        except Exception:
            stale_by_decision = False

    is_stale = stale_by_lag or stale_by_date or stale_by_decision
    if is_stale:
        return {
            "status": "stale",
            "note": "Dữ liệu **forecast** đang chậm/không đồng bộ thời gian; chỉ nên dùng như **thông tin hỗ trợ quyết định**.",
            "latest": latest,
        }
    return {
        "status": "fresh",
        "note": "Dữ liệu **forecast** còn mới cho phân tích **ngắn hạn**.",
        "latest": latest,
    }


def _future_points(points: Optional[List[AIChatForecastPoint]]) -> List[Tuple[datetime, float]]:
    out: List[Tuple[datetime, float]] = []
    now_ref = _now_local_hour()
    for point in points or []:
        dt = _parse_iso_time(str(point.time))
        if dt is None:
            continue
        local_dt = _to_local_naive(dt)
        if local_dt >= now_ref:
            out.append((local_dt, float(point.risk_score_0_100 or 0.0)))
    if out:
        out.sort(key=lambda item: item[0])
        return out

    for point in points or []:
        dt = _parse_iso_time(str(point.time))
        if dt is None:
            continue
        out.append((_to_local_naive(dt), float(point.risk_score_0_100 or 0.0)))
    out.sort(key=lambda item: item[0])
    return out


def _short_term_trend(req: AIChatRequest, horizon_hours: int = 6) -> Optional[Dict[str, object]]:
    future = _future_points(req.forecast_series)
    if len(future) < 2:
        return None

    start_time = future[0][0]
    end_limit = start_time + timedelta(hours=max(1, horizon_hours))
    window = [item for item in future if item[0] <= end_limit]
    if len(window) < 2:
        window = future[: min(len(future), max(2, horizon_hours))]
    if len(window) < 2:
        return None

    first = window[0][1]
    last = window[-1][1]
    delta = last - first
    if delta >= 2.0:
        trend = "tăng"
    elif delta <= -2.0:
        trend = "giảm"
    else:
        trend = "đi ngang"

    peak_time, peak_value = max(window, key=lambda item: item[1])
    return {
        "trend": trend,
        "delta": round(delta, 2),
        "start": round(first, 2),
        "end": round(last, 2),
        "peak_time": peak_time,
        "peak_value": round(peak_value, 2),
        "horizon_hours": horizon_hours,
    }


def _format_latest_time(req: AIChatRequest) -> str:
    quality = _forecast_quality(req)
    latest = quality.get("latest")
    if isinstance(latest, datetime):
        return latest.strftime("%Y-%m-%d %H:%M")
    return "không có"


def _compose_sections(sections: List[Tuple[str, str]]) -> str:
    cleaned: List[Tuple[str, str]] = []
    for title, body in sections:
        t = (title or "").strip()
        b = (body or "").strip()
        if not b:
            continue
        cleaned.append((t, b))

    if not cleaned:
        return ""

    # Natural, conversational rendering (no rigid section labels in output).
    intro = ""
    basis = ""
    limit = ""
    advice = ""
    extras: List[str] = []

    for title, body in cleaned:
        key = _normalize_for_match(title)
        if "danh gia" in key and not intro:
            intro = body
        elif "co so" in key and not basis:
            basis = body
        elif "gioi han" in key and not limit:
            limit = body
        elif ("khuyen nghi" in key or "goi y" in key) and not advice:
            advice = body
        else:
            extras.append(body)

    if not intro:
        intro = cleaned[0][1]

    blocks: List[str] = [intro]
    seen = {intro.strip()}
    if basis:
        line = f"Mình dựa trên dữ liệu này: {basis}"
        if line.strip() not in seen:
            blocks.append(line)
            seen.add(line.strip())
    if limit:
        line = f"Lưu ý thêm: {limit}"
        if line.strip() not in seen:
            blocks.append(line)
            seen.add(line.strip())
    if advice:
        line = f"Gợi ý thực tế: {advice}"
        if line.strip() not in seen:
            blocks.append(line)
            seen.add(line.strip())
    for item in extras:
        if item.strip() in seen:
            continue
        blocks.append(item)
        seen.add(item.strip())
    return "\n\n".join(blocks).strip()


def _ahp_summary_line(row: AIChatDistrictRow) -> str:
    return f"**{row.districtName}** đang ở hạng {row.rank} theo **AHP**, điểm {row.score:.6f}."


def _select_row_from_question(question: str, rows: List[AIChatDistrictRow]) -> Optional[AIChatDistrictRow]:
    target = _extract_target_district(question, rows)
    if target is not None:
        return target
    ranked = _sorted_rows(rows)
    return ranked[0] if ranked else None


def _has_contextual_district_ref(question: str) -> bool:
    q = _normalize_for_match(question)
    return any(token in q for token in ("quan nay", "khu nay", "cho nay", "noi nay"))


def _target_from_chat_context(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> Optional[AIChatDistrictRow]:
    # Walk backward to find the latest explicit district mention.
    for msg in reversed(req.messages or []):
        text = (msg.content or "").strip()
        if not text:
            continue
        found = _extract_target_district(text, rows)
        if found is not None:
            return found
    return None


def _is_lowest_query(question: str) -> bool:
    q = _normalize_for_match(question)
    return any(
        token in q
        for token in (
            "it o nhiem nhat",
            "it o nhiem",
            "thap nhat",
            "an toan hon",
            "it nguy co nhat",
            "nguy co thap",
        )
    )


def _is_top3_query(question: str) -> bool:
    q = _normalize_for_match(question)
    return "top 3" in q or "ba quan" in q


def _is_ahp_refresh_update_query(question: str) -> bool:
    raw = (question or "").strip().lower()
    q = _normalize_for_match(question)
    if not raw and not q:
        return False

    compact = re.sub(r"\s+", "", q)
    raw_compact = re.sub(r"\s+", "", raw)

    has_ahp_context = any(
        token in q for token in ("ahp", "xep hang", "top 3", "thu tu")
    ) or any(
        token in raw for token in ("ahp", "xếp hạng", "top 3", "thứ tự")
    ) or any(
        token in compact for token in ("xephang", "top3", "thutu")
    ) or any(
        token in raw_compact for token in ("xếphạng", "top3", "thứtự")
    )

    has_update_intent = any(
        token in q
        for token in (
            "tu dong cap nhat",
            "co tu cap nhat",
            "co cap nhat",
            "cap nhat lai",
            "nap them",
            "nhap them",
            "nap moi",
            "nhap moi",
            "sau khi nap",
            "sau khi nhap",
            "chay lai",
            "tinh lai",
            "luu lai",
        )
    ) or any(
        token in raw
        for token in (
            "tự động cập nhật",
            "có tự cập nhật",
            "có cập nhật",
            "cập nhật lại",
            "nạp thêm",
            "nhập thêm",
            "nạp mới",
            "nhập mới",
            "sau khi nạp",
            "sau khi nhập",
            "chạy lại",
            "tính lại",
            "lưu lại",
        )
    ) or any(
        token in compact
        for token in ("tudongcapnhat", "capnhat", "napthem", "nhapthem", "chaylai", "tinhlai")
    ) or any(
        token in raw_compact
        for token in ("tựđộngcậpnhật", "cậpnhật", "nạpthêm", "nhậpthêm", "chạylại", "tínhlại")
    )

    if has_ahp_context and has_update_intent:
        return True

    # Fallback for mojibake-like inputs where vowels are lost after bad encoding.
    broken_update = bool(re.search(r"c\s*p\s*n\s*h\s*t", q)) or bool(re.search(r"n\s*p\s*t\s*h\s*m", q))
    broken_context = ("top 3" in q) or bool(re.search(r"x\s*p\s*h\s*n\s*g", q))
    return broken_context and broken_update


def _is_yes_no_query(question: str) -> bool:
    q = _normalize_for_match(question)
    return ("co nen" in q) or ("duoc khong" in q) or ("nen hay khong" in q) or ("an toan khong" in q)


def _extract_requested_rank(question: str, max_rank: int) -> Optional[int]:
    q = _normalize_for_match(question)
    if not q or max_rank <= 0:
        return None

    candidates: List[Tuple[int, int]] = []
    patterns = [
        r"\bdung thu\s*(\d{1,2})\b",
        r"\bhang\s*thu\s*(\d{1,2})\b",
        r"\bhang\s*(\d{1,2})\b",
        r"\bthu\s*(\d{1,2})\b",
    ]
    for pattern in patterns:
        for m in re.finditer(pattern, q):
            n = int(m.group(1))
            if 1 <= n <= max_rank:
                candidates.append((m.start(), n))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _build_ahp_current_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = _sorted_rows(rows)
    if not ranked:
        return _compose_sections(
            [
                ("Đánh giá", "Mình chưa có bảng xếp hạng **AHP** cho phiên hiện tại."),
                ("Giới hạn", "Bạn cần nạp dữ liệu tiêu chí C1–C4 hoặc chọn ngày có dữ liệu."),
                ("Khuyến nghị tham khảo", "Sau khi có bảng AHP, mình sẽ giải thích quận nào đang đáng lo nhất."),
            ]
        )

    if _is_ahp_refresh_update_query(question):
        return _compose_sections(
            [
                (
                    "Đánh giá",
                    "Có. Top 3 sẽ cập nhật sau khi bạn nạp/lưu dữ liệu AHP mới và bấm tính lại xếp hạng.",
                ),
                (
                    "Cơ sở",
                    "Nếu chỉ nhập dữ liệu nhưng chưa tính lại (hoặc chưa lưu thành công), bảng hiện tại vẫn giữ thứ tự cũ.",
                ),
                (
                    "Khuyến nghị tham khảo",
                    "Sau khi cập nhật, hãy tải lại bảng kết quả để xác nhận thứ tự mới.",
                ),
            ]
        )

    if _is_top3_query(question):
        top3 = ranked[:3]
        top_text = "; ".join(
            [f"**{row.districtName}** (hạng {row.rank}, điểm {row.score:.6f})" for row in top3]
        )
        return _compose_sections(
            [
                ("Đánh giá", f"Top ưu tiên hiện trạng theo **AHP**: {top_text}."),
                (
                    "Cơ sở",
                    "Đây là xếp hạng theo dữ liệu hiện tại của C1–C4 ở cấp quận, không phải dự báo tương lai.",
                ),
                ("Khuyến nghị tham khảo", "Ưu tiên theo dõi quận đứng đầu trước, sau đó mở rộng sang hạng 2–3."),
            ]
        )

    max_rank_value = 0
    if ranked:
        max_rank_value = max(int(r.rank or 0) for r in ranked)
    requested_rank = _extract_requested_rank(question, max(max_rank_value, len(ranked), 1))
    if requested_rank is not None:
        target = next((r for r in ranked if int(r.rank) == int(requested_rank)), None)
        if target is None and 1 <= requested_rank <= len(ranked):
            target = ranked[requested_rank - 1]
        if target is not None:
            return _compose_sections(
                [
                    (
                        "Đánh giá",
                        f"Theo **AHP** hiện tại, quận đứng hạng **{requested_rank}** là **{target.districtName}** (điểm {target.score:.6f}).",
                    ),
                    (
                        "Cơ sở",
                        f"Tiêu chí chi phối chính: {_top_drivers_text(target)}.",
                    ),
                    (
                        "Giới hạn",
                        "Đây là xếp hạng hiện trạng tại thời điểm dữ liệu; chưa phải kết luận xu hướng **ngắn hạn**.",
                    ),
                    (
                        "Khuyến nghị tham khảo",
                        "Nếu cần quyết định theo giờ, xem thêm cảnh báo ngắn hạn để tránh diễn giải quá mức.",
                    ),
                ]
            )

    if _is_lowest_query(question):
        row = ranked[-1]
        return _compose_sections(
            [
                ("Đánh giá", f"Trong bảng **AHP** hiện tại, **{row.districtName}** đang có mức rủi ro thấp nhất."),
                ("Cơ sở", f"Hạng {row.rank}, điểm {row.score:.6f}."),
                (
                    "Giới hạn",
                    "Kết luận này phản ánh hiện trạng tại thời điểm dữ liệu, không suy rộng thành xu hướng **ngắn hạn**.",
                ),
                ("Khuyến nghị tham khảo", "Vẫn duy trì giám sát định kỳ để phát hiện biến động mới."),
            ]
        )

    row = _select_row_from_question(question, ranked)
    if row is None:
        row = ranked[0]

    return _compose_sections(
        [
            ("Đánh giá", _ahp_summary_line(row)),
            (
                "Cơ sở",
                f"Tiêu chí chi phối chính: {_top_drivers_text(row)}.",
            ),
            (
                "Giới hạn",
                "**AHP** dùng để đọc hiện trạng theo quận ở thời điểm đang xét; chưa phải kết luận xu hướng **ngắn hạn**.",
            ),
            (
                "Khuyến nghị tham khảo",
                "Nếu cần quyết định vài giờ tới, nên xem thêm phần **cảnh báo sớm** để tránh tăng phơi nhiễm đột ngột.",
            ),
        ]
    )


def _build_warning_mechanism_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _forecast_quality(req)
    trend = _short_term_trend(req, horizon_hours=6)

    trend_text = "Chưa đủ chuỗi forecast để mô tả biến động trong vài giờ tới."
    if trend is not None:
        trend_text = (
            f"Xu hướng **ngắn hạn** hiện đang **{trend['trend']}** "
            f"(từ {trend['start']:.2f} lên {trend['end']:.2f} trong khoảng {trend['horizon_hours']} giờ)."
        )

    return _compose_sections(
        [
            (
                "Đánh giá",
                "Hệ thống tách rõ 2 lớp: **AHP** cho hiện trạng theo quận, và **cảnh báo sớm** cho xu hướng **ngắn hạn**.",
            ),
            (
                "Cơ sở",
                "Lớp cảnh báo ngắn hạn dùng tổ hợp: hiện trạng hiện tại + xu hướng gần + độ kéo dài ô nhiễm + hỗ trợ khí tượng/forecast khi có. "
                + trend_text,
            ),
            ("Giới hạn", str(quality["note"])),
            (
                "Khuyến nghị tham khảo",
                "Dùng AHP để biết khu vực đang đáng lo ngay lúc này; dùng cảnh báo sớm để quyết định hành động trong vài giờ kế tiếp.",
            ),
        ]
    )


def _build_warning_vs_ahp_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = _sorted_rows(rows)
    top_line = "Chưa có bảng AHP hiện trạng."
    if ranked:
        top_line = _ahp_summary_line(ranked[0])

    quality = _forecast_quality(req)
    trend = _short_term_trend(req, horizon_hours=6)
    trend_line = "Chưa đủ dữ liệu forecast để kết luận chắc về xu hướng vài giờ tới."
    if trend is not None:
        trend_line = (
            f"Phần **AI/cảnh báo sớm** cho thấy xu hướng **{trend['trend']}** "
            f"trong khoảng {trend['horizon_hours']} giờ tới."
        )

    return _compose_sections(
        [
            ("Đánh giá", f"Đúng, bạn hiểu chuẩn: **AHP** và **AI ngắn hạn** phục vụ hai câu hỏi khác nhau. {top_line}"),
            (
                "Cơ sở",
                "**AHP** trả lời 'đang xấu ở đâu ngay bây giờ'; **cảnh báo sớm** trả lời 'nơi nào có nguy cơ tăng thêm trong vài giờ tới'. "
                + trend_line,
            ),
            ("Giới hạn", str(quality["note"])),
            (
                "Khuyến nghị tham khảo",
                "Ra quyết định vận hành nên dùng cả hai lớp: ưu tiên theo AHP trước, sau đó tinh chỉnh theo cảnh báo sớm.",
            ),
        ]
    )


def _build_data_lineage_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = _sorted_rows(rows)
    top_line = _ahp_summary_line(ranked[0]) if ranked else "Hiện chưa có bảng AHP của phiên này."
    quality = _forecast_quality(req)

    assessment = (
        "Nguồn để xét tiêu chí **AHP (C1-C4)** là dữ liệu môi trường theo giờ từ nguồn quan trắc/khí tượng của hệ thống "
        "(trục chính hiện tại: **Open-Meteo** cho PM2.5, PM10, NO2, O3, CO theo tọa độ quận)."
    )
    basis = (
        "Luồng xử lý là: lấy chuỗi giờ -> tổng hợp theo ngày/phiên -> tính **C1-C4** -> chạy ma trận **AHP** để ra rank/score. "
        "Sau đó lớp **AI** mới dùng kết quả AHP + xu hướng gần để diễn giải nguy cơ ngắn hạn. "
        + top_line
    )
    limit = (
        "AHP phản ánh hiện trạng tại thời điểm dữ liệu. "
        f"{quality['note']} Vì vậy phần AI là **thông tin hỗ trợ quyết định**, không phải dự báo dài hạn chắc chắn."
    )
    recommendation = (
        "Khi cần quyết định nhanh: đọc bảng AHP để biết quận nào đang xấu trước, "
        "rồi xem lớp AI để biết nguy cơ vài giờ tới tăng hay giảm."
    )
    return _compose_sections(
        [
            ("Đánh giá", assessment),
            ("Cơ sở", basis),
            ("Giới hạn", limit),
            ("Khuyến nghị tham khảo", recommendation),
        ]
    )


def _build_system_dependency_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    scope = _extract_time_scope(question)
    days = int(scope.get("days") or 0)
    horizon_text = f"{days} ng?y t?i" if days > 0 else "khung d? b?o ng?n h?n"
    quality = _forecast_quality(req)

    return _compose_sections(
        [
            (
                "??nh gi?",
                f"Kh?ng tr?c ti?p. B?ng AHP m?i b?n n?p s? ??i th? t? **hi?n tr?ng**, c?n d? b?o {horizon_text} ch?y theo chu?i forecast t??ng lai theo qu?n.",
            ),
            (
                "C? s?",
                "Lu?ng hi?n t?i: AHP = x?p h?ng hi?n tr?ng theo C1-C4; d? b?o 3 ng?y = m? ph?ng t? d? li?u forecast 72h theo qu?n, kh?ng copy nguy?n h?ng AHP hi?n t?i.",
            ),
            (
                "Gi?i h?n",
                f"{quality['note']} N?u forecast ??i th? d? b?o 3 ng?y s? ??i, k? c? khi b?ng AHP hi?n tr?ng ch?a ??i.",
            ),
            (
                "Khuy?n ngh? tham kh?o",
                "D?ng AHP cho quy?t ??nh ngay b?y gi?, v? d?ng b?ng d? b?o 3 ng?y ?? chu?n b? tr??c ngu?n l?c v?n h?nh.",
            ),
        ]
    )


def _build_data_reliability_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _forecast_quality(req)
    status = str(quality.get("status") or "missing")

    if status == "fresh":
        assessment = (
            "Khi cảm biến cập nhật tốt, phần **cảnh báo sớm ngắn hạn** có thể dùng khá tin cậy cho quyết định vận hành."
        )
        limit = "Dù vậy, đây vẫn là **thông tin hỗ trợ quyết định**, không phải dự báo chắc chắn tuyệt đối."
    elif status == "stale":
        assessment = (
            "Nếu cảm biến/forecast cập nhật chậm, lớp **AHP hiện trạng** vẫn còn giá trị nền, "
            "nhưng phần **cảnh báo sớm ngắn hạn** sẽ giảm độ chắc chắn trước."
        )
        limit = "Dữ liệu đang lệch thời gian, nên đọc kết quả theo hướng thận trọng hơn."
    else:
        assessment = (
            "Khi thiếu chuỗi forecast, hệ thống vẫn đọc được **AHP hiện trạng**, "
            "nhưng chưa đủ cơ sở để kết luận mạnh về rủi ro vài giờ tới."
        )
        limit = "Thiếu dữ liệu ngắn hạn nên không nên diễn giải quá tự tin."

    return _compose_sections(
        [
            ("Đánh giá", assessment),
            (
                "Cơ sở",
                "Mốc forecast mới nhất trong phiên: "
                f"**{_format_latest_time(req)}**. "
                "Độ chậm dữ liệu ảnh hưởng trực tiếp đến độ nhạy của cảnh báo ngắn hạn.",
            ),
            ("Giới hạn", f"{quality['note']} {limit}"),
            (
                "Khuyến nghị tham khảo",
                "Trong lúc dữ liệu chậm, ưu tiên dùng AHP để xác định khu vực đang xấu, và giảm mức chắc chắn khi dùng cảnh báo vài giờ tới.",
            ),
        ]
    )


def _build_sensor_vs_reality_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _forecast_quality(req)
    row = _select_row_from_question(question, _sorted_rows(rows)) if rows else None
    district_line = (
        f"Tại **{row.districtName}**, "
        if row is not None
        else "Ở khu vực bạn đang đứng, "
    )
    return _compose_sections(
        [
            (
                "Đánh giá",
                district_line
                + "nếu bạn quan sát thấy ngột ngạt hơn app, hãy xem dữ liệu hệ thống là **thông tin hỗ trợ quyết định** chứ không phải chân lý tuyệt đối.",
            ),
            (
                "Cơ sở",
                "Sai khác có thể đến từ điểm nóng cục bộ (kẹt xe, công trình, hẻm hẹp, giờ cao điểm) và độ trễ cập nhật giữa hiện trường với dữ liệu tổng hợp.",
            ),
            ("Giới hạn", str(quality["note"])),
            (
                "Khuyến nghị tham khảo",
                "Trong tình huống thực địa xấu hơn app, nên **giảm phơi nhiễm** ngay: rút ngắn thời gian ngoài trời, ưu tiên tuyến thoáng và dùng khẩu trang lọc bụi mịn.",
            ),
        ]
    )


def _build_district_health_precaution_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = _sorted_rows(rows)
    target = _extract_target_district(question, ranked)
    if target is None and _has_contextual_district_ref(question):
        target = _target_from_chat_context(req, ranked)
    if target is None:
        target = ranked[0] if ranked else None
    if target is None:
        return _build_operational_advice_reply(req, rows, question)

    quality = _forecast_quality(req)
    bucket = _rank_bucket(target)
    if bucket == "cao":
        action = "hạn chế ở ngoài trời quá lâu, ưu tiên khẩu trang lọc bụi mịn và tuyến đường ít xe."
    elif bucket == "trung bình":
        action = "giữ thời gian ngoài trời ở mức vừa phải, tránh khung giờ kẹt xe và theo dõi cập nhật mới."
    else:
        action = "duy trì thói quen phòng tránh cơ bản và tiếp tục theo dõi biến động."

    assessment = (
        f"Với **{target.districtName}**, mức ưu tiên hiện trạng theo **AHP** đang ở nhóm {bucket} (hạng {target.rank}, điểm {target.score:.6f})."
    )
    if _is_yes_no_query(question):
        if bucket == "cao":
            assessment = (
                f"Trả lời nhanh: **chưa an toàn** nếu ở ngoài lâu tại **{target.districtName}** lúc này "
                f"(hạng {target.rank}, điểm {target.score:.6f})."
            )
        elif bucket == "trung bình":
            assessment = (
                f"Trả lời nhanh: **tương đối an toàn nếu ở ngắn** tại **{target.districtName}**, "
                f"nhưng vẫn cần phòng tránh (hạng {target.rank}, điểm {target.score:.6f})."
            )
        else:
            assessment = (
                f"Trả lời nhanh: **khá an toàn** cho hoạt động ngắn tại **{target.districtName}**, "
                f"vẫn nên giữ biện pháp phòng tránh cơ bản (hạng {target.rank}, điểm {target.score:.6f})."
            )

    return _compose_sections(
        [
            ("Đánh giá", assessment),
            (
                "Cơ sở",
                f"Yếu tố chi phối chính tại quận này: {_top_drivers_text(target)}.",
            ),
            ("Giới hạn", f"{quality['note']} Đây là **thông tin hỗ trợ quyết định**, **không phải tư vấn y khoa**."),
            (
                "Khuyến nghị tham khảo",
                f"Bạn nên {action} Mục tiêu là **giảm phơi nhiễm** trong giai đoạn dữ liệu hiện tại.",
            ),
        ]
    )


def _build_short_duration_outdoor_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = _sorted_rows(rows)
    target = _extract_target_district(question, ranked)
    if target is None and _has_contextual_district_ref(question):
        target = _target_from_chat_context(req, ranked)
    if target is None:
        target = ranked[0] if ranked else None
    duration = _extract_duration_minutes(question) or 30
    quality = _forecast_quality(req)

    district_text = f"tại **{target.districtName}** " if target is not None else ""
    base_text = (
        f"Nếu bạn chỉ ra ngoài khoảng {duration} phút {district_text}thì vẫn nên chủ động bảo vệ hô hấp."
    )
    if target is not None and target.rank <= 3:
        base_text = (
            f"Với thời lượng khoảng {duration} phút tại **{target.districtName}**, nên xem đây là mức rủi ro cao và ưu tiên giảm phơi nhiễm ngay."
        )

    return _compose_sections(
        [
            ("Đánh giá", base_text),
            (
                "Cơ sở",
                "Khuyến nghị dựa trên hiện trạng **AHP** của quận đang hỏi và tín hiệu forecast gần (nếu có).",
            ),
            ("Giới hạn", f"{quality['note']} Đây là khuyến nghị vận hành ngắn hạn, **không phải tư vấn y khoa**."),
            (
                "Khuyến nghị tham khảo",
                "Đeo N95/KF94, chọn lộ trình ít xe, hạn chế dừng lâu tại giao lộ lớn và ưu tiên vào không gian kín khi thấy không khí ngột ngạt.",
            ),
        ]
    )


def _build_operational_advice_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = _sorted_rows(rows)
    target = _extract_target_district(question, ranked)
    if target is None and _has_contextual_district_ref(question):
        target = _target_from_chat_context(req, ranked)
    if target is None and ranked:
        target = ranked[0]
    quality = _forecast_quality(req)

    if target is not None and _is_yes_no_query(question):
        if target.rank <= 3:
            decision_line = f"Mình nghiêng về **không nên** ở ngoài lâu tại **{target.districtName}** trong thời điểm này."
        else:
            decision_line = f"Bạn **có thể** ra ngoài ngắn, nhưng vẫn nên phòng tránh ở **{target.districtName}**."
    else:
        decision_line = "Bạn nên ưu tiên các hành động giúp **giảm phơi nhiễm** trong vài giờ tới."

    basis_line = "Khuyến nghị dựa trên dữ liệu hiện trạng theo **AHP**."
    if target is not None:
        basis_line = f"{basis_line} Hiện **{target.districtName}** đang hạng {target.rank}, điểm {target.score:.6f}."

    return _compose_sections(
        [
            ("Đánh giá", decision_line),
            ("Cơ sở", basis_line),
            ("Giới hạn", f"{quality['note']} Kết quả là **thông tin hỗ trợ quyết định**, **không phải tư vấn y khoa**."),
            (
                "Khuyến nghị tham khảo",
                "Giảm thời gian ngoài trời, đeo khẩu trang lọc bụi, tránh giờ cao điểm giao thông và theo dõi cập nhật mới nếu phải di chuyển tiếp.",
            ),
        ]
    )


def _build_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _forecast_quality(req)
    trend = _short_term_trend(req, horizon_hours=6)
    scope = _extract_time_scope(question)
    scope_kind = str(scope.get("kind") or "")
    scope_days = int(scope.get("days") or 0)

    ranked = _sorted_rows(rows)
    target = _extract_target_district(question, ranked)
    if target is None and ranked:
        target = ranked[0]

    # Multi-day question (e.g. 3-7 days): use district forecast ranking if available.
    multi_day_query = scope_kind in {"days", "weekly"} and scope_days >= 3
    if multi_day_query:
        forecast_ranked = _sorted_rows(req.district_forecast_rows or [])
        days_text = str(scope_days if scope_days else 7)
        if not forecast_ranked:
            district_hint = f" cho **{target.districtName}**" if target is not None else ""
            return _compose_sections(
                [
                    (
                        "Danh gia",
                        f"Hien minh **chua du co so** de ket luan on dinh thu hang trong {days_text} ngay toi{district_hint}.",
                    ),
                    (
                        "Co so",
                        f"Chuoi forecast theo quan chua san sang. Moc forecast gan nhat: **{_format_latest_time(req)}**.",
                    ),
                    (
                        "Gioi han",
                        "He thong canh bao som can chuoi du lieu tuong lai theo quan; khi chuoi nay thieu thi khong nen ket luan manh cho nhieu ngay.",
                    ),
                    (
                        "Khuyen nghi tham khao",
                        "Hay cap nhat forecast moi va hoi lai theo khung 6-24 gio neu can quyet dinh ngay.",
                    ),
                ]
            )

        target_future = _extract_target_district(question, forecast_ranked)
        if target_future is None and target is not None:
            target_future = _extract_target_district(target.districtName, forecast_ranked)

        top3_text = "; ".join(
            f"**{r.districtName}** (hang {r.rank}, diem {r.score:.6f})"
            for r in forecast_ranked[:3]
        )
        if target_future is not None:
            return _compose_sections(
                [
                    (
                        "Danh gia",
                        f"Trong khung {days_text} ngay toi, **{target_future.districtName}** du kien o hang {target_future.rank} theo mo phong AHP dua tren forecast theo quan.",
                    ),
                    (
                        "Co so",
                        f"Top 3 du kien: {top3_text}. Khung tinh toan: **{int(req.district_forecast_horizon_hours or 0)} gio** forecast gan.",
                    ),
                    (
                        "Gioi han",
                        "Day la du bao ngan den trung han dua tren chuoi forecast hien co; thu hang van co the thay doi khi du lieu moi cap nhat.",
                    ),
                    (
                        "Khuyen nghi tham khao",
                        "Neu quan muc tieu van o nhom dau, nen uu tien giam phoi nhiem va theo doi cap nhat theo gio.",
                    ),
                ]
            )

        return _compose_sections(
            [
                (
                    "Danh gia",
                    f"Trong khung {days_text} ngay toi, nhom nguy co cao du kien van tap trung o nhom dau bang xep hang du bao theo quan.",
                ),
                (
                    "Co so",
                    f"Top 3 du kien: {top3_text}. Khung tinh toan: **{int(req.district_forecast_horizon_hours or 0)} gio** forecast gan.",
                ),
                (
                    "Gioi han",
                    "Du bao nay la thong tin ho tro quyet dinh, khong phai khang dinh chac chan dai han.",
                ),
                (
                    "Khuyen nghi tham khao",
                    "Nen ket hop voi hien trang AHP ngay hom nay de uu tien dieu hanh.",
                ),
            ]
        )

    if target is None:
        return _compose_sections(
            [
                ("Danh gia", "Minh chua co du du lieu quan de ket luan nguy co ngan han."),
                ("Gioi han", str(quality["note"])),
                ("Khuyen nghi tham khao", "Hay nap du lieu AHP theo quan roi hoi lai de co ket qua ro hon."),
            ]
        )

    if trend is None:
        return _compose_sections(
            [
                (
                    "Danh gia",
                    f"Hien minh chi xac nhan duoc hien trang theo **AHP**: **{target.districtName}** (hang {target.rank}, diem {target.score:.6f}).",
                ),
                (
                    "Co so",
                    "Chua du chuoi forecast moi de khang dinh xu huong 6-24 gio toi.",
                ),
                ("Gioi han", str(quality["note"])),
                (
                    "Khuyen nghi tham khao",
                    "Tam thoi uu tien quyet dinh theo hien trang AHP va cap nhat forecast moi de doc nguy co **ngan han**.",
                ),
            ]
        )

    return _compose_sections(
        [
            (
                "Danh gia",
                f"Trong khoang **{trend['horizon_hours']} gio toi**, nguy co ngan han tai **{target.districtName}** co xu huong **{trend['trend']}**.",
            ),
            (
                "Co so",
                f"Hien trang theo **AHP**: hang {target.rank}, diem {target.score:.6f}. "
                f"Forecast gan cho thay bien dong {trend['start']:.2f} -> {trend['end']:.2f}, dinh gan nhat {trend['peak_value']:.2f} luc {trend['peak_time'].strftime('%H:%M %d-%m')}.",
            ),
            ("Gioi han", f"{quality['note']} Day la canh bao **ngan han**, khong phai du bao dai han chac chan."),
            (
                "Khuyen nghi tham khao",
                "Neu xu huong tang, nen giam hoat dong ngoai troi va theo doi cap nhat theo gio; neu di ngang/giam thi van duy tri phong tranh co ban.",
            ),
        ]
    )

def _build_emergency_health_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    return _compose_sections(
        [
            (
                "Đánh giá",
                "Mình không thể chẩn đoán y khoa. Nếu bạn đang khó thở/chóng mặt rõ rệt, hãy ưu tiên vào nơi thoáng hoặc trong nhà ngay và nhờ người hỗ trợ.",
            ),
            (
                "Cơ sở",
                "Hệ thống này dùng cho đánh giá môi trường và **thông tin hỗ trợ quyết định**, không thay thế tư vấn chuyên môn y tế.",
            ),
            (
                "Giới hạn",
                "Dữ liệu cảm biến có thể có độ trễ theo không gian/thời gian; vì vậy khi triệu chứng cơ thể tăng nhanh, cần ưu tiên an toàn thực tế.",
            ),
            (
                "Khuyến nghị tham khảo",
                "Giảm phơi nhiễm ngay (khẩu trang lọc bụi, tránh điểm đông xe), đồng thời cân nhắc liên hệ cơ sở y tế nếu triệu chứng kéo dài.",
            ),
        ]
    )


def _build_general_explanation_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _forecast_quality(req)
    top_line = ""
    ranked = _sorted_rows(rows)
    if ranked:
        top_line = f"Hiện trạng nổi bật hiện tại: {_ahp_summary_line(ranked[0])}"
    return _compose_sections(
        [
            (
                "Đánh giá",
                "Hệ thống tách 2 lớp: **AHP** để đọc hiện trạng theo quận, và **AI/cảnh báo sớm** để đọc nguy cơ **ngắn hạn**.",
            ),
            (
                "Cơ sở",
                "AHP dùng C1–C4 để xếp hạng mức đáng lo hiện tại; AI đọc thêm xu hướng gần và tín hiệu khí tượng/forecast khi có. "
                + top_line,
            ),
            ("Giới hạn", f"{quality['note']} Không nên xem kết quả như dự báo dài hạn chắc chắn."),
            (
                "Khuyến nghị tham khảo",
                "Bạn có thể hỏi theo 1 trong 3 nhóm: hiện trạng AHP, nguy cơ vài giờ tới, hoặc khuyến nghị giảm phơi nhiễm theo quận.",
            ),
        ]
    )


def _build_chat_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    info = _classify_chat_intent(question, rows)
    intent = str(info["intent"] or "general_explanation")

    builders: Dict[str, Callable[[AIChatRequest, List[AIChatDistrictRow], str], str]] = {
        "ahp_current": _build_ahp_current_reply,
        "short_term_risk": _build_short_term_risk_reply,
        "warning_mechanism": _build_warning_mechanism_reply,
        "warning_vs_ahp_comparison": _build_warning_vs_ahp_reply,
        "system_dependency": _build_system_dependency_reply,
        "data_lineage": _build_data_lineage_reply,
        "data_reliability": _build_data_reliability_reply,
        "sensor_vs_reality": _build_sensor_vs_reality_reply,
        "district_health_precaution": _build_district_health_precaution_reply,
        "short_duration_outdoor_advice": _build_short_duration_outdoor_reply,
        "operational_advice": _build_operational_advice_reply,
        "emergency_health": _build_emergency_health_reply,
        "general_explanation": _build_general_explanation_reply,
    }
    builder = builders.get(intent, _build_general_explanation_reply)
    return builder(req, rows, question)


def _should_use_llm(req: AIChatRequest) -> bool:
    # Safe default: deterministic routing.
    # Only enable LLM when explicitly requested by client/environment.
    request_provider = (req.provider or "").strip().lower()
    if request_provider in {"grounded", "rule", "deterministic"}:
        return False
    if request_provider in {"ollama", "openai"}:
        return True

    env_provider = (os.getenv("LLM_PROVIDER") or "").strip().lower()
    env_auto = (os.getenv("LLM_AUTO_ENABLE") or "").strip().lower() in {"1", "true", "yes", "on"}
    if env_auto and env_provider in {"ollama", "openai"}:
        return True
    return False


def _llm_model_name(req: AIChatRequest) -> str:
    return (req.model or os.getenv("OLLAMA_MODEL") or "llama3.1:8b").strip()


def _llm_base_url() -> str:
    return (os.getenv("OLLAMA_BASE_URL") or "http://127.0.0.1:11434").rstrip("/")


def _llm_system_prompt() -> str:
    return (
        "Bạn là lớp giải thích DSS môi trường.\n"
        "Mục tiêu: trả lời đúng câu hỏi người dùng bằng tiếng Việt tự nhiên, ngắn gọn, có hành động rõ.\n"
        "Quy tắc bắt buộc:\n"
        "1) Bám sát FACTS được cung cấp, không bịa dữ liệu mới.\n"
        "2) Nếu câu hỏi là yes/no (an toàn không, có nên không), câu đầu phải trả lời trực tiếp 'Nên/Không nên' hoặc mức an toàn rõ.\n"
        "3) Phân biệt rõ: AHP = hiện trạng theo quận; cảnh báo sớm/AI = xu hướng ngắn hạn.\n"
        "4) Nếu thiếu dữ liệu forecast thì nói rõ thiếu cơ sở cho kết luận ngắn hạn.\n"
        "5) Không dùng giọng tuyệt đối chắc chắn, không chẩn đoán y khoa.\n"
        "6) Ưu tiên câu trả lời dễ hiểu, không lặp khuôn máy móc.\n"
        "7) Dùng markdown và bôi đậm vừa phải cho cụm quan trọng."
    )


def _build_llm_facts(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    info = _classify_chat_intent(question, rows)
    intent = str(info.get("intent") or "general_explanation")
    entities = dict(info.get("entities") or {})

    ranked = _sorted_rows(rows)
    top = ranked[0] if ranked else None
    bottom = ranked[-1] if ranked else None
    target = _extract_target_district(question, ranked) if ranked else None
    requested_rank = _extract_requested_rank(question, len(ranked)) if ranked else None
    nth = None
    if requested_rank is not None and ranked:
        nth = next((r for r in ranked if int(r.rank) == int(requested_rank)), None)
        if nth is None and 1 <= requested_rank <= len(ranked):
            nth = ranked[requested_rank - 1]

    quality = _forecast_quality(req)
    trend = _short_term_trend(req, horizon_hours=6)

    ranking_lines: List[str] = []
    for row in ranked[: min(13, len(ranked))]:
        ranking_lines.append(
            f"- hạng {row.rank}: {row.districtName} | score={row.score:.6f} | "
            f"C1={float(row.C1 or 0):.6f}, C2={float(row.C2 or 0):.6f}, C3={float(row.C3 or 0):.6f}, C4={float(row.C4 or 0):.6f}"
        )

    facts = [
        f"intent_hint={intent}",
        f"question={question}",
        f"decision_date={req.decision_date or 'unknown'}",
        f"forecast_quality_status={quality.get('status')}",
        f"forecast_quality_note={quality.get('note')}",
        f"latest_forecast_time={_format_latest_time(req)}",
        f"requested_rank={requested_rank}",
        f"has_district={bool(entities.get('has_district'))}",
        f"duration_minutes={entities.get('duration_minutes')}",
    ]
    if top is not None:
        facts.append(f"top_ahp={top.districtName}|rank={top.rank}|score={top.score:.6f}")
    if bottom is not None:
        facts.append(f"bottom_ahp={bottom.districtName}|rank={bottom.rank}|score={bottom.score:.6f}")
    if target is not None:
        facts.append(
            f"target_district={target.districtName}|rank={target.rank}|score={target.score:.6f}|"
            f"C1={float(target.C1 or 0):.6f}|C2={float(target.C2 or 0):.6f}|C3={float(target.C3 or 0):.6f}|C4={float(target.C4 or 0):.6f}"
        )
    if nth is not None:
        facts.append(f"requested_rank_result={nth.districtName}|rank={nth.rank}|score={nth.score:.6f}")
    if trend is not None:
        facts.append(
            f"short_term_trend={trend['trend']}|delta={trend['delta']}|start={trend['start']}|"
            f"end={trend['end']}|peak={trend['peak_value']}|peak_time={trend['peak_time']}"
        )
    else:
        facts.append("short_term_trend=missing")

    return (
        "FACTS:\n"
        + "\n".join(facts)
        + "\n\nRANKING_TABLE:\n"
        + ("\n".join(ranking_lines) if ranking_lines else "- no ranking rows")
    )


async def _call_ollama_chat(req: AIChatRequest, user_prompt: str) -> Optional[str]:
    url = f"{_llm_base_url()}/api/chat"
    model = _llm_model_name(req)
    payload = {
        "model": model,
        "stream": False,
        "messages": [
            {"role": "system", "content": _llm_system_prompt()},
            {"role": "user", "content": user_prompt},
        ],
        "options": {"temperature": float(req.temperature or 0.3)},
    }
    try:
        async with httpx.AsyncClient(timeout=35.0) as client:
            res = await client.post(url, json=payload)
            res.raise_for_status()
            data = res.json()
    except Exception:
        return None

    msg = data.get("message") if isinstance(data, dict) else None
    if isinstance(msg, dict):
        content = (msg.get("content") or "").strip()
        return content or None
    return None


@router.post("/chat", response_model=AIChatResponse)
async def ai_chat(req: AIChatRequest):
    rows = _normalized_district_rows(req.district_rows)
    if not rows and (req.decision_date or "").strip():
        rows = await _load_rank_rows_for_date(str(req.decision_date).strip())
    await _auto_attach_forecast_series(req)
    question = _last_user_message(req.messages)
    reply = ""
    used_llm = False
    rule_info = _classify_chat_intent(question, rows)
    rule_intent = str(rule_info.get("intent") or "general_explanation")
    entities = dict(rule_info.get("entities") or {})

    # For multi-day short-term questions, attach district-level forecast ranking
    # so AI can answer with real future signals by district.
    if rule_intent == "short_term_risk" and bool(entities.get("asks_multi_day")):
        requested_days = int((entities.get("time_scope") or {}).get("days") or 0)
        horizon_hours = requested_days * 24 if requested_days > 0 else 72
        horizon_hours = max(48, min(168, int(horizon_hours)))
        try:
            payload = await _compute_forecast_rank_rows(
                horizon_hours=horizon_hours,
                threshold=60.0,
            )
            ranking_rows = list(payload.get("rankingRows") or [])
            req.district_forecast_rows = _normalized_district_rows(
                [
                    AIChatDistrictRow(
                        districtName=str(item.get("DistrictName") or ""),
                        rank=int(item.get("Rank") or 0),
                        score=float(item.get("AHPScore") or item.get("Score") or 0.0),
                        C1=float(item.get("C1") or 0.0),
                        C2=float(item.get("C2") or 0.0),
                        C3=float(item.get("C3") or 0.0),
                        C4=float(item.get("C4") or 0.0),
                    )
                    for item in ranking_rows
                ]
            )
            req.district_forecast_horizon_hours = int(payload.get("horizonHours") or horizon_hours)
        except Exception:
            req.district_forecast_rows = req.district_forecast_rows or []
            req.district_forecast_horizon_hours = horizon_hours

    strict_rule_intents = {
        "ahp_current",
        "system_dependency",
        "data_lineage",
        "district_health_precaution",
        "short_duration_outdoor_advice",
        "operational_advice",
        "emergency_health",
    }
    q_norm = _normalize_for_match(question)
    force_rule = (
        rule_intent in strict_rule_intents
        or ("o nhiem nhat" in q_norm)
        or ("it o nhiem" in q_norm)
        or (_extract_requested_rank(question, max(max([int(r.rank or 0) for r in rows], default=1), 1)) is not None)
    )

    # LLM-first for semantic understanding; deterministic engine as safe fallback.
    if _should_use_llm(req) and not force_rule:
        facts = _build_llm_facts(req, rows, question)
        llm_prompt = (
            f"{facts}\n\n"
            "YÊU CẦU TRẢ LỜI:\n"
            "- Trả lời đúng trọng tâm câu hỏi của user.\n"
            "- Nếu hỏi 'ít ô nhiễm nhất', trả đúng quận hạng thấp nhất theo bảng.\n"
            "- Nếu hỏi 'đứng thứ N', trả đúng quận hạng N.\n"
            "- Nếu hỏi so sánh AHP vs cảnh báo sớm, tách rõ hai phần.\n"
            "- Nếu dữ liệu forecast không đủ, nói rõ giới hạn.\n"
            "- Giữ câu trả lời ngắn, rõ, tự nhiên.\n"
        )
        llm_reply = await _call_ollama_chat(req, llm_prompt)
        if llm_reply:
            reply = llm_reply
            used_llm = True

    if not reply:
        reply = _build_chat_reply(req, rows, question)

    return AIChatResponse(
        provider=("ollama" if used_llm else "grounded"),
        model=(_llm_model_name(req) if used_llm else "rule-router-v2"),
        reply=reply,
    )


INTENT_VALIDATION_CASES: List[Tuple[str, str]] = [
    ("Ở Quận 11 nếu tôi phải ở ngoài trời một lúc thì cần chú ý những gì để đỡ bị phơi nhiễm?", "district_health_precaution"),
    ("Mai tôi phải chạy qua Quận 11 vài việc, không ở lâu đâu, vậy có điều gì cần tự giữ cho ổn không?", "short_duration_outdoor_advice"),
    ("Nếu cảm biến cập nhật chậm hơn thực tế thì phần cảnh báo của hệ thống còn dùng được tới mức nào?", "data_reliability"),
    ("Nếu màn hình chưa theo kịp tình hình ngoài đường thì tôi nên đọc phần cảnh báo của hệ thống theo kiểu nào?", "sensor_vs_reality"),
    ("Ngoài đường nhìn ngột ngạt hơn hẳn mà app chưa báo xấu lắm, lúc đó tôi nên hiểu kết quả hệ thống thế nào?", "sensor_vs_reality"),
    ("Vậy hiểu đơn giản là bảng AHP giúp tôi biết chỗ nào đang đáng lo, còn AI giúp tôi biết chỗ nào sắp xấu thêm, đúng không?", "warning_vs_ahp_comparison"),
    ("Nói dễ hiểu thì hệ thống phân biệt kiểu gì giữa chỗ đang ô nhiễm ngay bây giờ và chỗ sắp có nguy cơ tăng?", "warning_mechanism"),
    ("Nếu khu vực tôi đang ở có nguy cơ ô nhiễm cao thì hệ thống khuyến nghị tôi nên làm gì?", "operational_advice"),
    ("lưu ý gì để đảm bảo cho sức khỏe mỗi cá nhân khi ở quận 10", "district_health_precaution"),
    ("Ngày nào trong tuần tới là ngày sạch nhất để tôi ra đường?", "short_term_risk"),
]


def run_intent_validation(rows: Optional[List[AIChatDistrictRow]] = None) -> List[Dict[str, str]]:
    safe_rows = rows or []
    output: List[Dict[str, str]] = []
    for prompt, expected in INTENT_VALIDATION_CASES:
        intent = str(_classify_chat_intent(prompt, safe_rows)["intent"])
        output.append(
            {
                "prompt": prompt,
                "expected": expected,
                "predicted": intent,
                "ok": "yes" if intent == expected else "no",
            }
        )
    return output
