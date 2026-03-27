from __future__ import annotations

import os
from typing import Dict, List, Literal, Optional
import re
import unicodedata
from datetime import date as date_cls
from datetime import datetime, timedelta

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.openmeteo_service import fetch_hourly_async
from app.services.ahp import compute_ahp
from app.services.risk_scoring import compute_score_0_100
from app.routers.district import (
    DEFAULT_BASELINE_AHP_MATRIX,
    DEFAULT_CRITERIA_LABELS,
    _compute_ahp_scored_rows,
    _ensure_criteria,
    _weights_to_dict,
)

router = APIRouter(prefix="/ai", tags=["AI"])

DEFAULT_TZ = "Asia/Ho_Chi_Minh"
DEFAULT_OFFSET = "+07:00"


def _iso_with_offset(t: str, tz: str) -> str:
    if not isinstance(t, str):
        return t
    tail = t[-6:]
    if t.endswith("Z") or ("+" in tail) or ("-" in tail):
        return t
    if tz in ("UTC", "GMT"):
        return t + "Z"
    if tz == "Asia/Ho_Chi_Minh":
        return t + DEFAULT_OFFSET
    return t


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
    forecast_series: Optional[List[AIChatForecastPoint]] = None
    provider: Optional[str] = None
    model: Optional[str] = None
    temperature: float = Field(default=0.3, ge=0.0, le=1.5)


class AIChatResponse(BaseModel):
    provider: str
    model: str
    reply: str


CRITERIA_EXPLAIN_MAP: Dict[str, str] = {
    "C1": "Mức độ vượt chuẩn: phản ánh mức ô nhiễm tức thời cao hơn ngưỡng.",
    "C2": "Thời gian duy trì ô nhiễm cao: phản ánh mức phơi nhiễm kéo dài.",
    "C3": "Tần suất vượt ngưỡng: phản ánh tính lặp lại của các đợt ô nhiễm.",
    "C4": "Điều kiện khí tượng bất lợi: phản ánh rủi ro tích tụ do khuếch tán kém.",
}
CRITERIA_NAME_MAP: Dict[str, str] = {
    "C1": "Mức độ vượt chuẩn (bụi mịn/chất ô nhiễm chính)",
    "C2": "Thời gian duy trì ô nhiễm cao",
    "C3": "Tần suất vượt ngưỡng lặp lại",
    "C4": "Điều kiện khí tượng bất lợi",
}
CRITERIA_AGENT_MAP: Dict[str, str] = {
    "C1": "Bụi mịn PM2.5 và nhóm chất ô nhiễm đang vượt chuẩn",
    "C2": "Phơi nhiễm kéo dài do nồng độ ô nhiễm duy trì cao",
    "C3": "Các đợt vượt ngưỡng lặp lại theo giờ/ngày",
    "C4": "Khí tượng bất lợi (gió yếu, khuếch tán kém, dễ tích tụ)",
}
CRITERIA_ACTION_HINT_MAP: Dict[str, str] = {
    "C1": "Đeo N95/KF94, giảm thời gian ngoài trời gần trục giao thông lớn.",
    "C2": "Giảm phơi nhiễm dài giờ, ưu tiên không gian kín có lọc không khí.",
    "C3": "Tăng giám sát theo khung giờ lặp lại, tránh đi lại vào giờ đỉnh.",
    "C4": "Theo dõi dự báo liên tục, hạn chế mở cửa vào giờ khí tượng xấu.",
}


def _criterion_name(key: str) -> str:
    return CRITERIA_NAME_MAP.get(key, key)


def _criterion_agent(key: str) -> str:
    return CRITERIA_AGENT_MAP.get(key, "Tác nhân ô nhiễm tổng hợp")



def _criterion_action_hint(key: str) -> str:
    return CRITERIA_ACTION_HINT_MAP.get(key, "Theo dõi sát cảnh báo của hệ thống.")



def _source_note(req: AIChatRequest) -> str:
    src = (req.ranking_source or "AHP").strip()
    date_text = (req.decision_date or "").strip()
    if date_text:
        return f"Nguồn dữ liệu: {src}, ngày {date_text}."
    return f"Nguồn dữ liệu: {src}."



def _normalize_plain(text: str) -> str:
    t = (text or "").strip().lower()
    if not t:
        return ""
    # Keep Vietnamese intent matching stable:
    # map "đ/Đ" -> "d/D" before stripping combining marks.
    t = t.replace("đ", "d").replace("Đ", "D")
    t = unicodedata.normalize("NFKD", t)
    t = "".join(ch for ch in t if unicodedata.category(ch) != "Mn")
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    # common recovery for broken mojibake tokens
    t = re.sub(r"\bqu n\b", "quan", t)
    t = re.sub(r"\bq n\b", "q", t)
    return t


def _district_key(name: str) -> str:
    key = _normalize_plain(name)
    if not key:
        return ""
    key = re.sub(r"\b(quan|district)\b", " ", key)
    key = re.sub(r"\s+", " ", key).strip()
    return key


def _normalized_district_rows(rows: Optional[List[AIChatDistrictRow]]) -> List[AIChatDistrictRow]:
    out = []
    for r in rows or []:
        name = (r.districtName or "").strip()
        if not name:
            continue
        rank = int(r.rank or 0)
        if rank <= 0:
            rank = 10000 + len(out)
        out.append(
            AIChatDistrictRow(
                districtName=name,
                rank=rank,
                score=float(r.score or 0.0),
                C1=r.C1,
                C2=r.C2,
                C3=r.C3,
                C4=r.C4,
            )
        )
    out.sort(key=lambda x: (x.rank, -x.score))
    return out


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

    rows = [{"Date": raw_date, **r} for r in criteria_rows]
    ahp = compute_ahp(DEFAULT_BASELINE_AHP_MATRIX, DEFAULT_CRITERIA_LABELS[:])
    weights = _weights_to_dict(ahp.get("weights") or [])
    scored = _compute_ahp_scored_rows(
        rows=rows,
        labels=DEFAULT_CRITERIA_LABELS[:],
        weights=weights,
        normalize_alternatives=True,
        rank_mode="cost",
    )
    normalized: List[AIChatDistrictRow] = []
    for r in scored:
        normalized.append(
            AIChatDistrictRow(
                districtName=str(r.get("DistrictName") or "").strip(),
                rank=int(r.get("Rank") or 0),
                score=float(r.get("AHPScore") or r.get("Score") or 0.0),
                C1=float(r.get("C1") or 0.0),
                C2=float(r.get("C2") or 0.0),
                C3=float(r.get("C3") or 0.0),
                C4=float(r.get("C4") or 0.0),
            )
        )
    return _normalized_district_rows(normalized)


async def _auto_attach_forecast_series(req: AIChatRequest) -> None:
    if req.forecast_series and len(req.forecast_series) >= 2:
        return
    if req.lat is None or req.lon is None:
        return

    horizon = req.hours or 24
    horizon = max(6, min(24, int(horizon)))

    try:
        pack = await fetch_hourly_async(
            lat=float(req.lat),
            lon=float(req.lon),
            hours=horizon,
            past_days=1,
            timezone=DEFAULT_TZ,
        )
    except Exception:
        return

    hourly = pack.get("hourly") or {}
    all_times = list(hourly.get("time") or [])
    future_idxs = _select_future_indices(all_times, horizon)
    if not future_idxs:
        return
    times = [all_times[i] for i in future_idxs]

    def arr(key: str) -> List[Optional[float]]:
        values = hourly.get(key) or []
        if not isinstance(values, list):
            return [None] * len(times)
        return [values[i] if i < len(values) else None for i in future_idxs]

    pm2_5 = arr("pm2_5")
    pm10 = arr("pm10")
    no2 = arr("nitrogen_dioxide")
    o3 = arr("ozone")
    co = arr("carbon_monoxide")
    tz = pack.get("timezone", DEFAULT_TZ)

    series: List[AIChatForecastPoint] = []
    for idx, t in enumerate(times):
        safe_values = {
            "pm2_5": float(pm2_5[idx] or 0.0),
            "pm10": float(pm10[idx] or 0.0),
            "no2": float(no2[idx] or 0.0),
            "o3": float(o3[idx] or 0.0),
            "co": float(co[idx] or 0.0),
        }
        out = compute_score_0_100(safe_values, req.weights or {})
        score = float(out.get("score_0_100") or 0.0)
        series.append(
            AIChatForecastPoint(
                time=_iso_with_offset(str(t), str(tz)),
                risk_score_0_100=score,
            )
        )
    req.forecast_series = series


SYSTEM_PROMPT = """
BẠN LÀ CHUYÊN GIA HỖ TRỢ QUYẾT ĐỊNH (DSS) VỀ SỨC KHỎE VÀ MÔI TRƯỜNG.
MỤC TIÊU: GIÚP NGƯỜI DÙNG RA QUYẾT ĐỊNH AN TOÀN NGAY LÚC NÀY.

QUY TẮC:
1) Trả lời theo hành động trước. Nếu người dùng hỏi "có nên/không nên", câu đầu phải là "Nên" hoặc "Không nên".
2) Không trả lời rập khuôn kiểu một template cố định cho mọi câu hỏi.
3) AHP = hiện trạng theo quận; AI = diễn giải xu hướng ngắn hạn. Không trộn lẫn vai trò.
4) Nếu forecast cũ/lệch thời gian/thiếu dữ liệu thì nói rõ: độ tin cậy giảm, chưa đủ cơ sở để kết luận mạnh.
5) Với câu hỏi khẩn cấp (khó thở, đau ngực, bụi mù dày): ưu tiên khuyến nghị bảo vệ sức khỏe ngay, văn phong tự nhiên như đang hỗ trợ trực tiếp.
6) Tiếng Việt UTF-8 chuẩn, có dấu, không lỗi font.
"""


def _build_chat_system_prompt(req: AIChatRequest) -> str:
    parts: List[str] = [SYSTEM_PROMPT.strip()]

    if req.lat is not None and req.lon is not None:
        parts.append(f"Vị trí hiện tại: lat={req.lat:.5f}, lon={req.lon:.5f}.")
    if req.hours is not None:
        parts.append(f"Cửa sổ phân tích: {req.hours} giờ.")
    if req.weights:
        try:
            ws = ", ".join(f"{k}={float(v):.3f}" for k, v in req.weights.items())
            parts.append(f"Trọng số đang dùng: {ws}.")
        except Exception:
            parts.append("Có cấu hình trọng số đầu vào từ dashboard.")
    if req.forecast_series:
        fs = [p for p in req.forecast_series if p is not None]
        if fs:
            cur = float(fs[0].risk_score_0_100)
            peak = max(fs, key=lambda p: float(p.risk_score_0_100))
            parts.append(
                f"Chuỗi nguy cơ ngắn hạn hiện có: hiện tại {cur:.1f}, đỉnh {float(peak.risk_score_0_100):.1f} vào {peak.time}."
            )

    rows = _normalized_district_rows(req.district_rows)
    if rows:
        src = (req.ranking_source or "AHP").strip()
        date_text = (req.decision_date or "").strip()
        if date_text:
            parts.append(f"Bảng xếp hạng tham chiếu ({src}) ngày {date_text}.")
        else:
            parts.append(f"Bảng xếp hạng tham chiếu ({src}).")
        top3 = rows[:3]
        top3_text = ", ".join(
            f"hạng {r.rank}: {r.districtName} (score {r.score:.6f})" for r in top3
        )
        parts.append(f"Top 3 hiện tại: {top3_text}.")
        parts.append(
            "QUY TẮC BẮT BUỘC: nếu người dùng hỏi quận cao nhất/thấp nhất thì phải trả lời theo đúng bảng xếp hạng này."
        )

    parts.append("Trả lời ngắn gọn, rõ ý, bám dữ liệu phiên hiện tại.")
    return " ".join(parts)


def _detect_rank_query_intent(text: str) -> Optional[str]:
    q = _normalize_plain(text)
    if not q:
        return None

    lowest_signals = [
        "it o nhiem nhat",
        "thap nhat",
        "nguy co thap nhat",
        "an toan nhat",
        "sach nhat",
        "tot nhat",
    ]
    if any(s in q for s in lowest_signals):
        return "lowest"

    highest_signals = [
        "o nhiem nhat",
        "cao nhat",
        "nguy co cao nhat",
        "uu tien nhat",
        "top 1",
        "anh huong nhat",
        "bi anh huong nhieu nhat",
        "khu vuc anh huong nhat",
        "nghiem trong nhat",
        "diem nong nhat",
    ]
    if any(s in q for s in highest_signals):
        return "highest"
    return None


def _build_grounded_rank_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], intent: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
    if intent == "lowest":
        target = ranked[-1]
        preview_rows = list(reversed(ranked[-3:]))
        head = (
            f"Quáº­n cĂ³ má»©c Æ°u tiĂªn Ă´ nhiá»…m tháº¥p nháº¥t hiá»‡n táº¡i lĂ  {target.districtName} "
            f"(háº¡ng {target.rank}, score {target.score:.6f})."
        )
        section = "NhĂ³m Ă­t Ă´ nhiá»…m hiá»‡n táº¡i"
    else:
        target = ranked[0]
        preview_rows = ranked[:3]
        head = (
            f"Quáº­n cĂ³ má»©c Æ°u tiĂªn Ă´ nhiá»…m cao nháº¥t hiá»‡n táº¡i lĂ  {target.districtName} "
            f"(háº¡ng {target.rank}, score {target.score:.6f})."
        )
        section = "Top 3 hiá»‡n táº¡i"

    lines = [f"- Háº¡ng {idx}: {r.districtName} (score {r.score:.6f})" for idx, r in enumerate(preview_rows, start=1)]
    return head + "\n\n" + section + ":\n" + "\n".join(lines) + "\n\n" + _source_note(req)

def _is_risk_analysis_question(text: str) -> bool:
    q = _normalize_plain(text)
    if not q:
        return False
    signals = [
        "rui ro",
        "rui ra",
        "nguy co",
        "canh bao som",
        "can quan tam",
        "phan tich",
        "tai sao",
        "vi sao",
        "ly do",
        "he qua",
        "khuyen nghi",
        "khuyen cao",
        "hanh dong",
        "suc khoe",
        "diem den",
        "khung gio",
        "anh huong",
        "so lieu",
        "co so du lieu",
    ]
    return any(s in q for s in signals)


def _extract_target_district(text: str, rows: List[AIChatDistrictRow]) -> Optional[AIChatDistrictRow]:
    q = _normalize_plain(text)
    if not q:
        return None

    q_compact = q.replace(" ", "")
    for row in rows:
        dn = _normalize_plain(row.districtName)
        if not dn:
            continue
        if dn in q:
            return row
        if dn.replace(" ", "") in q_compact:
            return row

    # Há»— trá»£ há»i kiá»ƒu q7 / quan 7
    m = (
        re.search(r"\bq\s*(\d{1,2})\b", q)
        or re.search(r"\bquan\s*(\d{1,2})\b", q)
        or re.search(r"\bqu\s*n\s*(\d{1,2})\b", q)
    )
    if m:
        num = m.group(1)
        for row in rows:
            dn = _normalize_plain(row.districtName)
            if f"quan {num}" in dn:
                return row

    return None


def _extract_target_districts(text: str, rows: List[AIChatDistrictRow]) -> List[AIChatDistrictRow]:
    q = _normalize_plain(text)
    if not q:
        return []
    found: List[AIChatDistrictRow] = []
    for row in rows:
        dn = _normalize_plain(row.districtName)
        if not dn:
            continue
        if dn in q or dn.replace(" ", "") in q.replace(" ", ""):
            found.append(row)

    # Há»— trá»£ há»i kiá»ƒu: "so sĂ¡nh quáº­n 7 vá»›i quáº­n 4", "q7 vs q4"
    nums: List[str] = []
    nums.extend(re.findall(r"\bq\s*(\d{1,2})\b", q))
    nums.extend(re.findall(r"\bquan\s*(\d{1,2})\b", q))
    nums.extend(re.findall(r"\bqu\s*n\s*(\d{1,2})\b", q))
    if not nums:
        compare_hint = any(k in q for k in ["so sanh", "hay", "vs", "voi", "chon"])
        if compare_hint:
            nums.extend(re.findall(r"\b(\d{1,2})\b", q))
    if nums:
        row_by_num: Dict[str, List[AIChatDistrictRow]] = {}
        for row in rows:
            dn = _normalize_plain(row.districtName)
            m = re.search(r"\b(\d{1,2})\b", dn)
            if not m:
                continue
            row_by_num.setdefault(str(int(m.group(1))), []).append(row)
        for num in nums:
            for row in row_by_num.get(str(int(num)), []):
                found.append(row)
    uniq: List[AIChatDistrictRow] = []
    seen = set()
    for r in found:
        k = _district_key(r.districtName)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(r)
    return uniq


def _forecast_anchor_date(req: AIChatRequest) -> date_cls:
    if req.decision_date:
        try:
            return date_cls.fromisoformat(str(req.decision_date))
        except Exception:
            pass
    return date_cls.today()



def _forecast_staleness_note(req: AIChatRequest) -> str:
    pts = [p for p in (req.forecast_series or []) if p is not None]
    if not pts:
        return "Chưa có dữ liệu forecast theo giờ, AI chỉ hỗ trợ diễn giải hiện trạng nên độ tin cậy giảm."

    parsed = [_parse_iso_time(str(p.time)) for p in pts]
    parsed = [d for d in parsed if d is not None]
    if not parsed:
        return "Chuỗi forecast có định dạng thời gian chưa hợp lệ, cần đồng bộ lại nguồn forecast."

    parsed_local = [_to_local_naive(d) for d in parsed]
    max_dt = max(parsed_local)
    anchor = _forecast_anchor_date(req)
    if max_dt.date() < anchor:
        return (
            f"Dữ liệu forecast đang ở mốc cũ ({max_dt.strftime('%Y-%m-%d %H:%M')}), "
            "độ tin cậy giảm; chỉ dùng kết quả AI như hỗ trợ vận hành ngắn hạn."
        )

    if max_dt < (datetime.now() - timedelta(hours=1)):
        return (
            f"Mốc forecast mới nhất là {max_dt.strftime('%Y-%m-%d %H:%M')}, "
            "đang chậm so với thời điểm hiện tại nên độ tin cậy giảm."
        )

    return ""


_CLEAN_CRITERIA_LABELS: Dict[str, str] = {
    "C1": "Mức độ vượt chuẩn",
    "C2": "Thời gian duy trì ô nhiễm cao",
    "C3": "Tần suất vượt ngưỡng",
    "C4": "Điều kiện khí tượng bất lợi",
}


def _criterion_label_clean(key: str) -> str:
    return _CLEAN_CRITERIA_LABELS.get(str(key).upper(), str(key))


def _mojibake_score(text: str) -> int:
    if not text:
        return 0
    markers = (
        "\u00c3", "\u00c2", "\u0102", "\u00c4", "\u00e1\u00bb", "â€", "â€“", "â€”", "â€¢", "Ð", "Ñ", "�", "??"
    )
    return sum(text.count(m) for m in markers)


def _maybe_fix_mojibake(text: str) -> str:
    if not text:
        return text
    # Never transliterate or strip Vietnamese accents.
    # Only attempt byte-level round-trips that can reduce mojibake markers.
    cur = text
    for _ in range(3):
        best = cur
        for codec in ("latin1", "cp1252", "cp1258"):
            try:
                candidate = cur.encode(codec).decode("utf-8")
            except Exception:
                continue
            if candidate and _mojibake_score(candidate) < _mojibake_score(best):
                best = candidate
        if best == cur:
            break
        cur = best
    return cur



def _finalize_reply_text(text: str) -> str:
    cleaned = _maybe_fix_mojibake((text or "").strip())
    if not cleaned:
        return ""

    replacements = {
        "C? s? d? li?u": "Cơ sở dữ liệu",
        "D?a trên d? li?u": "Dựa trên dữ liệu",
        "?? tin c?y": "Độ tin cậy",
        "Khuy?n ngh?": "Khuyến nghị",
        "L?u ý": "Lưu ý",
        "R?t cao": "Rất cao",
        "Kh?": "Khá",
        "Trung b?nh": "Trung bình",
        "Th?p": "Thấp",
        "Qu?n": "Quận",
        "qu?n": "quận",
        "h?ng": "hạng",
        "ng?n h?n": "ngắn hạn",
        "hi?n tr?ng": "hiện trạng",
        "d? li?u": "dữ liệu",
        "th?i gian": "thời gian",
        "c?p nh?t": "cập nhật",
        "c?nh b?o": "cảnh báo",
        "r?i ro": "rủi ro",
        "m?c": "mức",
        "ch?a": "chưa",
        "m?i": "mới",
        "v?n": "vẫn",
    }
    for bad, good in replacements.items():
        cleaned = cleaned.replace(bad, good)

    lines = [ln.rstrip() for ln in cleaned.splitlines()]
    out: List[str] = []
    seen = set()
    blank = False
    for ln in lines:
        stripped = ln.strip()
        if not stripped:
            if not blank:
                out.append("")
                blank = True
            continue
        key = stripped.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(stripped)
        blank = False
    return "\n".join(out).strip()


def _response(
    reply: str,
    provider: str = "grounded",
    model: str = "rule-engine",
) -> AIChatResponse:
    return AIChatResponse(provider=provider, model=model, reply=_finalize_reply_text(reply))


def _detect_chat_intent(text: str) -> str:
    q = _normalize_plain(text)
    if not q:
        return "ahp_current"

    emergency_terms = (
        "kho tho",
        "dau nguc",
        "dau phoi",
        "chong mat",
        "mu mit",
        "khong thay nha doi dien",
        "khong thay nha",
        "toi sam",
        "cay mat",
        "ket xe",
        "ngat",
    )
    if any(t in q for t in emergency_terms):
        return "emergency_health"

    stale_terms = ("forecast cu", "du lieu cu", "lech thoi gian", "forecast chua moi", "timestamp cu")
    compare_terms_global = ("so sanh", "ahp va ai", "ahp voi ai", "hien trang va ngan han")
    if any(t in q for t in stale_terms) and any(t in q for t in compare_terms_global):
        return "stale_forecast_compare_question"

    high_rank_no_rise_terms = (
        "hang cao nhung",
        "rank cao nhung",
        "dung hang cao nhung",
        "nhom hang cao nhung",
        "chua thay nguy co tang",
        "khong thay nguy co tang",
        "khong tang them trong ngan han",
        "chua thay tang them",
    )
    if any(t in q for t in high_rank_no_rise_terms):
        return "high_rank_but_no_extra_short_term_risk"
    if ("hang cao" in q or "rank cao" in q or "top" in q) and ("khong tang them" in q or "chua thay tang" in q):
        return "high_rank_but_no_extra_short_term_risk"

    low_rank_watch_terms = (
        "rank chua cao",
        "hang chua cao",
        "chua cao nhung",
        "chua cao ma",
        "khuyen nghi theo doi them",
        "dang chu y hon trong ngan han",
        "danh sach theo doi bo sung",
        "nhom can giam sat ngan han",
        "theo doi bo sung",
        "watchlist",
    )
    if any(t in q for t in low_rank_watch_terms):
        return "low_rank_but_short_term_watch"
    if ("hang chua cao" in q or "rank chua cao" in q) and (
        "ngan han" in q or "theo doi them" in q or "dang chu y" in q
    ):
        return "low_rank_but_short_term_watch"
    if ("chua cao" in q and "ngan han" in q and "quan nao" in q):
        return "low_rank_but_short_term_watch"

    scenario_terms = ("scenario", "kich ban", "baseline", "policy")
    if any(t in q for t in scenario_terms):
        return "scenario_compare"

    compare_terms = ("ahp va ai", "ahp voi ai", "phan biet", "so sanh ahp", "hien trang va ngan han")
    if any(t in q for t in compare_terms):
        return "ahp_vs_short_term_compare"

    data_terms = ("do tin cay", "forecast co moi", "forecast moi", "du lieu cu", "lech thoi gian", "timestamp", "encoding")
    if any(t in q for t in data_terms):
        return "data_quality"

    short_terms = ("6 gio", "24 gio", "ngan han", "canh bao som", "xu huong", "khung gio", "trong vai gio toi")
    if any(t in q for t in short_terms):
        return "short_term_risk"

    advice_terms = ("nen", "can lam gi", "khuyen nghi", "hanh dong", "co nen", "chay bo", "di bo")
    if any(t in q for t in advice_terms):
        return "operational_advice"

    return "ahp_current"


def _is_global_short_term_compare_question(question: str) -> bool:
    q = _normalize_plain(question)
    if not q:
        return False
    compare_keywords = (
        "quan nao",
        "co quan nao",
        "rank chua cao",
        "hang chua cao",
        "hang cao",
        "rank cao",
        "so sanh",
    )
    short_keywords = ("ngan han", "6 gio", "24 gio", "canh bao som", "nguy co tang")
    return any(k in q for k in compare_keywords) and any(k in q for k in short_keywords)



def _confidence_text(score: float) -> str:
    s = max(0.0, min(100.0, float(score)))
    if s >= 90:
        label = "Rất cao"
    elif s >= 80:
        label = "Cao"
    elif s >= 70:
        label = "Khá"
    elif s >= 55:
        label = "Trung bình"
    else:
        label = "Thấp"
    return f"{label} ({s:.0f}/100)"


def _format_data_quality(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> Dict[str, object]:
    pts = [p for p in (req.forecast_series or []) if p is not None]
    parsed = [_parse_iso_time(str(p.time)) for p in pts]
    parsed = [d for d in parsed if d is not None]
    parsed_local = [_to_local_naive(d) for d in parsed]
    latest_local = max(parsed_local) if parsed_local else None
    stale_note = _forecast_staleness_note(req)
    future_points = _future_forecast_points(req.forecast_series)
    has_future = len(future_points) >= 2

    score = 85.0
    reasons: List[str] = []
    if not rows:
        score -= 20
        reasons.append("thiếu bảng xếp hạng AHP hiện tại")
    if latest_local is None:
        score -= 30
        reasons.append("chưa có chuỗi forecast theo giờ")
    elif stale_note:
        score -= 25
        reasons.append("forecast bị cũ/lệch thời gian")
    if not has_future:
        score -= 15
        reasons.append("chưa đủ mốc forecast tương lai")

    score = max(10.0, min(100.0, score))
    if score >= 90:
        label = "Rất cao"
    elif score >= 80:
        label = "Cao"
    elif score >= 70:
        label = "Khá"
    elif score >= 55:
        label = "Trung bình"
    else:
        label = "Thấp"

    return {
        "score": score,
        "label": label,
        "confidence_text": _confidence_text(score),
        "latest": latest_local.strftime("%Y-%m-%d %H:%M") if latest_local else "chưa có",
        "stale_note": stale_note,
        "has_future": has_future,
        "reasons": reasons,
    }



def _compose_sections(
    conclusion: str,
    data_basis: str,
    confidence: str,
    recommendation: str,
    note: str = "",
    stale_badge: bool = False,
) -> str:
    lines: List[str] = []
    if (conclusion or "").strip():
        lines.append(conclusion.strip())
    if (data_basis or "").strip():
        lines.append(f"Dựa trên dữ liệu: {data_basis.strip()}")
    if (confidence or "").strip():
        lines.append(f"Độ tin cậy: {confidence.strip()}")
    if (recommendation or "").strip():
        lines.append(recommendation.strip())
    if (note or "").strip():
        lines.append(f"Lưu ý: {note.strip()}")
    if stale_badge:
        lines.append("[warning_data_stale]")
    return "\n".join(lines)


def _build_data_quality_reply(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> str:
    quality = _format_data_quality(req, rows)
    reason_text = ", ".join(quality["reasons"]) if quality["reasons"] else "dữ liệu đủ mới và đủ thành phần chính"
    note = str(quality["stale_note"] or "")
    return _compose_sections(
        conclusion=f"Dữ liệu hiện tại có độ tin cậy {quality['label'].lower()}.",
        data_basis=f"Forecast mới nhất: {quality['latest']}; có chuỗi tương lai: {'có' if quality['has_future'] else 'chưa đủ'}.",
        confidence=f"{quality['label']} ({quality['score']:.0f}/100), do {reason_text}.",
        recommendation="Nếu độ tin cậy thấp/trung bình, chỉ dùng AI như hỗ trợ vận hành và ưu tiên kiểm chứng lại nguồn dữ liệu.",
        note=note,
        stale_badge=bool(note),
    )


def _build_scenario_compare_reply(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> str:
    source = (req.ranking_source or "").lower()
    if "scenario" not in source and "kich ban" not in source and "policy" not in source:
        return _compose_sections(
            conclusion="Phiên hiện tại chưa ở chế độ so sánh kịch bản.",
            data_basis="Chỉ thấy nguồn xếp hạng mặc định, chưa thấy baseline vs scenario trong dữ liệu chat.",
            confidence="Thấp (thiếu dữ liệu so sánh hai vế).",
            recommendation="Mở kết quả Scenario Compare rồi hỏi lại để hệ thống tách rõ Baseline và Scenario.",
        )

    return _compose_sections(
        conclusion="Đang ở nguồn kịch bản, nhưng phiên chat chưa có đủ cả hai vế Baseline và Scenario để tính chênh lệch chuẩn.",
        data_basis=f"Nguồn hiện tại: {req.ranking_source or 'Scenario'}; số quận nhận được: {len(rows)}.",
        confidence="Trung bình (thiếu một phần dữ liệu so sánh).",
        recommendation="Gửi đồng thời baseline + scenario vào phiên chat để AI trả đúng scoreDelta/rankDelta theo quận.",
    )


def _build_operational_advice_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    risk_mode = _detect_risk_response_mode(question)
    if rows and risk_mode == "decision_yes_no":
        ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
        target = _extract_target_district(question, rows) or ranked[0]
        return _build_grounded_risk_reply(req, rows, target, mode="decision_yes_no", user_question=question)

    short_term = _build_short_term_risk_reply(req, rows, question)
    if "ch?a ?? c? s?" in _normalize_plain(short_term):
        return short_term

    target = _extract_target_district(question, rows) if rows else None
    name = target.districtName if target else "khu v?c ?ang ch?n"
    return _compose_sections(
        conclusion=f"N?n ?u ti?n h?nh ??ng ph?ng ph?i nhi?m cho {name} trong ng?n h?n.",
        data_basis="D?a tr?n AHP hi?n tr?ng c?a qu?n v? xu h??ng forecast g?n trong phi?n hi?n t?i.",
        confidence="Ph? thu?c ?? m?i forecast; xem d?ng ?? tin c?y trong tr? l?i.",
        recommendation="?eo N95/KF94, gi?m th?i gian ngo?i tr?i gi? cao ?i?m, theo d?i c?p nh?t theo gi? ?? ?i?u ch?nh k? ho?ch.",
    )


def _build_ahp_vs_short_term_compare_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _format_data_quality(req, rows)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top_ahp = ranked[0] if ranked else None
    trend = _analyze_forecast_trend(req.forecast_series)

    if not trend or quality["stale_note"] or not quality["has_future"]:
        return _compose_sections(
            conclusion=(
                (f"AHP hi?n tr?ng ?ang ?u ti?n {top_ahp.districtName} (h?ng {top_ahp.rank}, score {top_ahp.score:.6f}). " if top_ahp else "")
                + "Ph?n AI ng?n h?n hi?n ch?a ?? c? s? ?? k?t lu?n kh?c bi?t theo qu?n."
            ),
            data_basis=f"Forecast m?i nh?t: {quality['latest']}; chu?i t??ng lai: {'??' if quality['has_future'] else 'ch?a ??'}.",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="K?t lu?n ch?c ? l?p AHP hi?n tr?ng; c?p nh?t forecast m?i tr??c khi so s?nh kh?c bi?t ng?n h?n gi?a c?c qu?n.",
            note=str(quality["stale_note"] or "Thi?u d? li?u forecast theo gi? ?? t?ch r? vai tr? AI ng?n h?n."),
            stale_badge=True,
        )

    delta = float(trend.get("delta") or 0.0)
    trend_word = str(trend.get("trend") or "?n ??nh")
    ahp_line = (
        f"AHP hi?n tr?ng: {top_ahp.districtName} ?ang h?ng {top_ahp.rank}, score {top_ahp.score:.6f}."
        if top_ahp
        else "AHP hi?n tr?ng: ch?a c? qu?n m?c ti?u."
    )
    ai_line = f"AI ng?n h?n: xu h??ng {trend_word}, hi?n t?i {float(trend['current']):.1f}, TB 3 gi? t?i {float(trend['avg_next']):.1f}."
    bridge = "D? xu h??ng ng?n h?n ?ang ?i ngang/gi?m, n?n r?i ro hi?n tr?ng v?n c? th? cao n?u qu?n ?ang h?ng ??u theo AHP."
    if delta >= 2:
        bridge = "N?n r?i ro AHP ?? cao v? xu h??ng ng?n h?n c?n t?ng, n?n c?n ?u ti?n x? l? s?m."

    return _compose_sections(
        conclusion=f"{ahp_line} {ai_line}",
        data_basis="AHP d?ng rank/score/C1-C4 theo qu?n; AI d?ng forecast g?n ?? ??c xu h??ng v?i gi? t?i.",
        confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
        recommendation=bridge,
    )




def _build_high_rank_but_no_extra_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _format_data_quality(req, rows)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top_group = ranked[:3]
    trend = _analyze_forecast_trend(req.forecast_series)
    if not top_group:
        return _compose_sections(
            conclusion="Chưa có dữ liệu AHP hiện tại để xác định nhóm hạng cao.",
            data_basis="Thiếu bảng xếp hạng theo quận.",
            confidence="Thấp (thiếu dữ liệu).",
            recommendation="Nạp kết quả AHP bước 4 trước khi hỏi so sánh ngắn hạn.",
        )

    top_text = ", ".join(f"{r.districtName} (hạng {r.rank})" for r in top_group)
    if not trend or quality["stale_note"] or not quality["has_future"]:
        return _compose_sections(
            conclusion="Trong nhóm hạng cao hiện tại, chưa đủ cơ sở để kết luận quận nào chưa có tín hiệu tăng thêm trong ngắn hạn.",
            data_basis=f"Nhóm hạng cao theo AHP: {top_text}; forecast mới nhất: {quality['latest']}.",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="Tạm bám thứ tự AHP hiện tại; cập nhật forecast mới để xác nhận tín hiệu tăng thêm.",
            note=str(quality["stale_note"] or "Thiếu chuỗi forecast ngắn hạn đủ mới."),
            stale_badge=True,
        )

    delta = float(trend["delta"])
    if delta <= 1.5:
        no_extra = ", ".join(r.districtName for r in top_group)
        return _compose_sections(
            conclusion=f"Trong nhóm hạng cao theo AHP, hiện chưa thấy tín hiệu tăng thêm rõ rệt ở: {no_extra}.",
            data_basis=f"Nhóm hạng cao: {top_text}; xu hướng điểm theo dõi: {trend['trend']} (delta 3 giờ: {delta:.1f}).",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="Giữ ưu tiên vận hành theo AHP, tiếp tục theo dõi theo giờ để phát hiện thay đổi mới.",
        )

    return _compose_sections(
        conclusion="Nhóm hạng cao theo AHP đang có tín hiệu tăng thêm trong ngắn hạn, chưa thể kết luận quận nào 'không tăng thêm'.",
        data_basis=f"Nhóm hạng cao: {top_text}; xu hướng điểm theo dõi: {trend['trend']} (delta 3 giờ: {delta:.1f}).",
        confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
        recommendation="Ưu tiên giám sát dày hơn toàn bộ nhóm hạng cao và cập nhật forecast liên tục trong 6-24 giờ tới.",
    )



def _build_low_rank_but_short_term_watch_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _format_data_quality(req, rows)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend or quality["stale_note"] or not quality["has_future"]:
        return _compose_sections(
            conclusion="Chưa đủ cơ sở để xác định rõ quận hạng chưa cao nào cần theo dõi thêm trong ngắn hạn.",
            data_basis=f"Forecast mới nhất: {quality['latest']}; chuỗi tương lai: {'đủ' if quality['has_future'] else 'chưa đủ'}.",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="Tạm thời dựa vào AHP hiện trạng; cập nhật forecast mới trước khi mở rộng danh sách theo dõi bổ sung.",
            note=str(quality["stale_note"] or "Thiếu dữ liệu forecast ngắn hạn theo quận."),
            stale_badge=True,
        )

    delta = float(trend["delta"])
    if delta < 2:
        return _compose_sections(
            conclusion="Chưa phát hiện quận hạng chưa cao nào nổi bật hơn hẳn về nguy cơ ngắn hạn.",
            data_basis=f"Xu hướng điểm theo dõi: {trend['trend']} (delta 3 giờ: {delta:.1f}).",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="Tiếp tục giám sát định kỳ; chỉ mở rộng danh sách theo dõi bổ sung khi có tín hiệu tăng rõ rệt.",
        )

    low_rank_pool = [r for r in ranked if int(r.rank) >= 7]
    if not low_rank_pool:
        return _compose_sections(
            conclusion="Chưa có đủ dữ liệu nhóm hạng chưa cao để lập danh sách theo dõi bổ sung.",
            data_basis=f"Số quận trong bảng AHP hiện tại: {len(ranked)}.",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="Bổ sung đầy đủ bảng xếp hạng AHP theo quận để mở rộng phân tích.",
        )

    candidates = sorted(
        low_rank_pool,
        key=lambda r: (float(r.C4 or 0.0) + float(r.C3 or 0.0), -float(r.score or 0.0)),
        reverse=True,
    )[:3]
    names = ", ".join(f"{r.districtName} (hạng {r.rank})" for r in candidates)
    return _compose_sections(
        conclusion="Có thể mở danh sách theo dõi bổ sung cho nhóm hạng chưa cao vì tín hiệu ngắn hạn đang tăng.",
        data_basis=f"Danh sách theo dõi bổ sung: {names}; xu hướng điểm theo dõi: {trend['trend']} (delta 3 giờ: {delta:.1f}); ưu tiên theo C4+C3 trong nhóm hạng chưa cao.",
        confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
        recommendation="Đây là nhóm cần giám sát ngắn hạn, không thay thế thứ tự ưu tiên chính của AHP.",
    )



def _build_stale_forecast_compare_question_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _format_data_quality(req, rows)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top_ahp = ranked[0] if ranked else None
    return _compose_sections(
        conclusion=(f"AHP hiện trạng vẫn dùng được và đang ưu tiên {top_ahp.districtName} (hạng {top_ahp.rank}, score {top_ahp.score:.6f}). " if top_ahp else "") + "Phần so sánh ngắn hạn chưa đủ cơ sở vì forecast đang cũ hoặc lệch thời gian.",
        data_basis=f"Forecast mới nhất: {quality['latest']}.",
        confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
        recommendation="Cập nhật forecast mới trước khi hỏi so sánh AHP và AI ngắn hạn theo quận.",
        note=str(quality["stale_note"] or "Dữ liệu forecast chưa đủ mới cho so sánh ngắn hạn."),
        stale_badge=True,
    )


def _build_operational_advice_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    risk_mode = _detect_risk_response_mode(question)
    if rows and risk_mode == "decision_yes_no":
        ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
        target = _extract_target_district(question, rows) or ranked[0]
        return _build_grounded_risk_reply(req, rows, target, mode="decision_yes_no", user_question=question)
    if rows and _is_personal_health_query(question):
        ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
        target = _extract_target_district(question, rows) or ranked[0]
        return _build_grounded_risk_reply(req, rows, target, mode="decision_yes_no", user_question=question)
    short_term = _build_short_term_risk_reply(req, rows, question)
    if "chưa đủ cơ sở" in _normalize_plain(short_term):
        return short_term
    target = _extract_target_district(question, rows) if rows else None
    name = target.districtName if target else "khu vực đang chọn"
    return (
        f"Nếu cần quyết định ngay, nên ưu tiên phương án giảm phơi nhiễm cho {name}.\n"
        "Hành động thực tế: đeo N95/KF94, hạn chế ở ngoài trời giờ cao điểm, và theo dõi cập nhật theo giờ trước khi thay đổi lịch trình."
    )


def _build_emergency_health_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    q = _normalize_plain(question)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    pair = _extract_target_districts(question, rows) if rows else []
    target = _extract_target_district(question, rows) if rows else None
    if target is None and ranked:
        target = ranked[0]

    trend = _analyze_forecast_trend(req.forecast_series)
    stale = _forecast_staleness_note(req)
    quality = _format_data_quality(req, rows)
    confidence = str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0)))

    if any(t in q for t in ["kho tho", "dau nguc", "dau phoi", "ngat", "chong mat"]):
        first = "KHÔNG NÊN ở ngoài trời lúc này. ĐEO N95 ngay và tìm chỗ trong nhà có không khí sạch."
    elif "ket xe" in q:
        first = "KHÔNG NÊN đứng lâu ngoài đường lúc này. Tấp vào nơi kín ngay để giảm phơi nhiễm."
    else:
        first = "KHÔNG NÊN tiếp tục phơi nhiễm ngoài trời lúc này."

    lines: List[str] = [first]

    if len(pair) >= 2:
        a, b = pair[0], pair[1]
        safer = a if int(a.rank) > int(b.rank) else b
        riskier = b if safer is a else a
        lines[0] = f"NÊN chọn {safer.districtName}; KHÔNG NÊN chọn {riskier.districtName} nếu ưu tiên an toàn hô hấp ngay bây giờ."

    if any(t in q for t in ["mu mit", "toi sam", "cay mat", "khong thay nha doi dien", "khong thay nha"]):
        lines.append(
            "Tôi hiểu tình huống bạn đang thấy. Cảm biến có thể sai số cục bộ hoặc cập nhật chậm; lúc này hãy tin vào quan sát thực tế và trú ẩn ngay."
        )

    if target is not None:
        cvals = _extract_c_values(target)
        top_keys = sorted(cvals.keys(), key=lambda k: float(cvals.get(k, 0.0)), reverse=True)
        driver_map = {
            "C1": "bụi mịn PM2.5",
            "C2": "khí thải tích lũy kéo dài",
            "C3": "các đợt ô nhiễm lặp lại trong ngày",
            "C4": "điều kiện thời tiết giữ ô nhiễm lại",
        }
        d1 = driver_map.get(top_keys[0], "bụi mịn PM2.5") if top_keys else "bụi mịn PM2.5"
        d2 = driver_map.get(top_keys[1], "khí thải tích lũy kéo dài") if len(top_keys) > 1 else d1
        top_score = float(ranked[0].score or 0.0) if ranked else float(target.score or 0.0)
        avg_score = sum(float(r.score or 0.0) for r in ranked) / max(1, len(ranked)) if ranked else float(target.score or 0.0)
        sev, _ = _classify_ahp_severity(float(target.score or 0.0), top_score, avg_score, int(target.rank or 999))
        lines.append(f"Tại {target.districtName}, nền ô nhiễm hiện trạng vẫn ở mức {sev.lower()}, chủ yếu do {d1} và {d2}.")

    if trend and str(trend.get("trend")) == "đi ngang":
        current = float(trend.get("current") or 0.0)
        if current >= 60:
            lines.append("“Ổn định” ở đây nghĩa là ổn định ở mức bẩn: bạn đang kẹt trong vùng ô nhiễm cao và vài giờ tới chưa khá hơn rõ.")
        else:
            lines.append("“Ổn định” ở đây là ổn định ở mức thấp, rủi ro ngắn hạn chưa tăng thêm rõ.")
    elif trend:
        lines.append(f"Xu hướng ngắn hạn hiện tại là {trend['trend']}; vì vậy vẫn nên hạn chế phơi nhiễm trong vài giờ tới.")
    else:
        lines.append("Hiện chưa đủ chuỗi forecast mới để kết luận xu hướng ngắn hạn thật chắc.")

    if stale:
        lines.append(f"Cảnh báo: Dữ liệu dự báo đang bị chậm. {stale}")

    lines.append("Việc cần làm ngay: đeo N95/KF94, tránh tuyến xe đông, vào không gian kín có lọc khí và theo dõi triệu chứng hô hấp.")
    if target is not None:
        lines.append(f"Tham chiếu phụ: {target.districtName} đang thuộc nhóm ưu tiên theo dõi cao theo AHP hiện tại.")
    lines.append(f"Độ tin cậy hiện tại: {confidence}.")

    if stale:
        lines.append("[warning_data_stale]")
    return "\n".join(lines)


def _build_grounded_rank_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], intent: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
    if intent == "lowest":
        target = ranked[-1]
        preview_rows = list(reversed(ranked[-3:]))
        head = f"Quận có mức ưu tiên ô nhiễm thấp nhất hiện tại là {target.districtName} (hạng {target.rank}, score {target.score:.6f})."
        section = "Nhóm ít ô nhiễm hiện tại"
    else:
        target = ranked[0]
        preview_rows = ranked[:3]
        head = f"Quận có mức ưu tiên ô nhiễm cao nhất hiện tại là {target.districtName} (hạng {target.rank}, score {target.score:.6f})."
        section = "Top 3 hiện tại"
    lines = [f"- Hạng {idx}: {r.districtName} (score {r.score:.6f})" for idx, r in enumerate(preview_rows, start=1)]
    return head + "\n\n" + section + ":\n" + "\n".join(lines) + "\n\n" + _source_note(req)


def _classify_ahp_severity(score: float, top_score: float, avg_score: float, rank: int) -> tuple[str, str]:
    s = float(score or 0.0)
    t = float(top_score or 0.0)
    a = float(avg_score or 0.0)
    r = int(rank or 999)
    if r <= 1 or s >= max(t * 0.8, a + 0.08):
        return "Nghiêm trọng", "Mức ưu tiên cao nhất, cần can thiệp sớm."
    if r <= 3 or s >= a + 0.04:
        return "Cao", "Cần tăng giám sát và chuẩn bị hành động ngắn hạn."
    if r <= 6 or s >= a:
        return "Trung bình", "Theo dõi tăng cường theo giờ để tránh tăng đột biến."
    return "Thấp", "Duy trì giám sát định kỳ và cập nhật khi có biến động."


def _infer_warning_windows(shares: Dict[str, float]) -> List[str]:
    windows: List[str] = []
    if float(shares.get("C1", 0.0)) >= 0.30:
        windows.extend(["06:00-09:00", "17:00-21:00"])
    if float(shares.get("C2", 0.0)) >= 0.28:
        windows.append("10:00-15:00")
    if float(shares.get("C4", 0.0)) >= 0.22:
        windows.append("21:00-06:00")
    if not windows:
        windows.append("06:00-09:00")
    out: List[str] = []
    seen = set()
    for w in windows:
        if w not in seen:
            out.append(w)
            seen.add(w)
    return out


def _build_grounded_risk_reply(
    req: AIChatRequest,
    rows: List[AIChatDistrictRow],
    target: AIChatDistrictRow,
    mode: str = "full",
    user_question: str = "",
) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
    top = ranked[0]
    avg_score = sum(float(r.score or 0.0) for r in ranked) / max(1, len(ranked))
    score = float(target.score or 0.0)
    cvals = _extract_c_values(target)
    total = sum(max(0.0, v) for v in cvals.values())
    shares = {k: ((max(0.0, v) / total) if total > 0 else 0.0) for k, v in cvals.items()}
    keys = sorted(cvals.keys(), key=lambda k: shares[k], reverse=True)
    d1 = keys[0] if keys else "C1"
    d2 = keys[1] if len(keys) > 1 else d1
    sev, sev_note = _classify_ahp_severity(score, float(top.score or 0.0), avg_score, int(target.rank or 0))
    trend = _analyze_forecast_trend(req.forecast_series)
    quality = _format_data_quality(req, rows)
    stale = _forecast_staleness_note(req)
    confidence = str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0)))
    windows = "; ".join(_infer_warning_windows(shares))

    if trend:
        trend_line = f"AI ngắn hạn: xu hướng {trend['trend']}, hiện tại {float(trend['current']):.1f}, TB 3 giờ tới {float(trend['avg_next']):.1f}, đỉnh {float(trend['peak']):.1f} lúc {trend['peak_time']}."
    else:
        trend_line = "AI ngắn hạn: chưa đủ chuỗi forecast mới để kết luận xu hướng mạnh."
    trend_meaning = ""
    if trend and str(trend.get("trend")) == "đi ngang":
        if sev in ("Nghiêm trọng", "Cao") or float(trend.get("current", 0.0)) >= 60:
            trend_meaning = (
                "“Ổn định” ở đây là ổn định ở mức bẩn, tức nền ô nhiễm vẫn cao và chưa có dấu hiệu khá hơn rõ rệt trong vài giờ tới."
            )
        else:
            trend_meaning = "“Ổn định” ở đây là ổn định ở mức thấp, rủi ro chưa tăng thêm trong vài giờ tới."

    def pack(lines: List[str], stale_badge: bool = False) -> str:
        out = [ln for ln in lines if (ln or "").strip()]
        out.append(_source_note(req))
        if stale:
            out.append(stale)
            if stale_badge:
                out.append("[warning_data_stale]")
        return "\n".join(out)

    if mode == "decision_yes_no":
        pair = _extract_target_districts(user_question, rows)
        if len(pair) >= 2:
            a, b = pair[0], pair[1]
            safer = a if int(a.rank) > int(b.rank) else b
            riskier = b if safer is a else a
            lines = [
                f"Nên chọn {safer.districtName}, không nên chọn {riskier.districtName} nếu mục tiêu là giảm phơi nhiễm ngay lúc này.",
                f"Vì {safer.districtName} đang ở mức rủi ro thấp hơn theo AHP hiện trạng.",
                trend_line,
            ]
            if trend_meaning:
                lines.append(trend_meaning)
            lines.extend(
                [
                    f"Khuyến nghị: nếu buộc phải đi, ưu tiên {safer.districtName}, đeo N95/KF94 và tránh các khung giờ {windows}.",
                    f"Độ tin cậy: {confidence}.",
                ]
            )
            return pack(lines, stale_badge=True)

        activity = _detect_activity_kind(user_question)
        hour = _extract_question_hour(user_question)
        avoid = False
        reasons: List[str] = []
        if sev == "Nghiêm trọng":
            avoid = True
            reasons.append("nền rủi ro hiện trạng cao theo AHP")
        if trend and float(trend.get("delta", 0.0)) >= 2:
            avoid = True
            reasons.append("xu hướng ngắn hạn đang tăng")
        if hour is not None and (_hour_in_range(hour, 6, 9) or _hour_in_range(hour, 17, 21)):
            avoid = True
            reasons.append(f"{hour:02d}:00 thuộc khung giờ phơi nhiễm cao")
        first = f"Không nên {activity} lúc này ở {target.districtName}." if avoid else f"Nên {activity} có kiểm soát ở {target.districtName}."
        return pack(
            [
                first,
                f"Lý do: {'; '.join(reasons) if reasons else 'mức rủi ro hiện tại chưa vượt ngưỡng cao'}.",
                f"AHP hiện trạng: {target.districtName} đang thuộc nhóm {sev.lower()}, chi phối bởi {_criterion_label_clean(d1)} và {_criterion_label_clean(d2)}.",
                trend_line,
                *( [trend_meaning] if trend_meaning else [] ),
                f"Độ tin cậy: {confidence}.",
                f"Khuyến nghị: Dù xu hướng có thể đi ngang/giảm, nếu nền AHP còn cao thì vẫn cần thận trọng. Khung giờ cần tránh: {windows}.",
            ],
            stale_badge=True,
        )

    if mode == "timeslot":
        hour = _extract_question_hour(user_question)
        if hour is None:
            return pack(["Bạn nói rõ mốc giờ (ví dụ 17:00) để mình trả lời đúng thời điểm."], stale_badge=True)
        risky = False
        if (_hour_in_range(hour, 6, 9) or _hour_in_range(hour, 17, 21)) and (
            shares.get("C1", 0.0) >= 0.28 or shares.get("C3", 0.0) >= 0.20
        ):
            risky = True
        if _hour_in_range(hour, 10, 15) and shares.get("C2", 0.0) >= 0.24:
            risky = True
        if (_hour_in_range(hour, 21, 24) or _hour_in_range(hour, 0, 6)) and shares.get("C4", 0.0) >= 0.18:
            risky = True
        if risky:
            lines = [f"Lúc {hour:02d}:00: KHÔNG NÊN ra ngoài ở {target.districtName}."]
        else:
            lines = [f"Lúc {hour:02d}:00: có thể đi nhưng vẫn nên đeo N95/KF94 và hạn chế thời gian ngoài trời."]
        if trend_meaning:
            lines.append(trend_meaning)
        lines.append(f"Khung giờ nên tránh hôm nay: {windows}.")
        return pack(lines, stale_badge=True)

    if mode == "safe_time_estimate":
        est = _estimate_safe_time_from_trend(req, safe_threshold=45.0)
        if not est:
            return pack(
                ["Tôi chưa đủ chuỗi forecast mới để chốt giờ an toàn cụ thể. Bạn cập nhật forecast rồi hỏi lại “mấy giờ an toàn?”."],
                stale_badge=True,
            )
        if bool(est.get("already_safe")):
            return pack([f"Ngay lúc này đã ở mức tương đối an toàn cho hoạt động ngoài trời tại {target.districtName}."], stale_badge=True)
        if float(est.get("eta_hours", -1.0)) < 0:
            return pack(
                ["Hiện chưa chốt được mốc giờ an toàn vì xu hướng chưa giảm đủ mạnh. Tạm thời KHÔNG NÊN ra ngoài lâu."],
                stale_badge=True,
            )
        safe_time = str(est.get("safe_time") or "").strip()
        return pack([f"Mốc tham chiếu an toàn sớm nhất: khoảng {safe_time}. Trước mốc này bạn nên hạn chế ra ngoài."], stale_badge=True)

    if mode == "compare":
        pair = _extract_target_districts(user_question, rows)
        if len(pair) >= 2:
            a, b = pair[0], pair[1]
            if _is_personal_health_query(user_question):
                chosen = a if int(a.rank) > int(b.rank) else b
                other = b if chosen is a else a
                lines = [
                    f"Mình chọn {chosen.districtName}.",
                    f"Lý do: {chosen.districtName} có mức rủi ro hiện trạng thấp hơn {other.districtName}, phù hợp hơn cho quyết định đi lại/sức khỏe.",
                    trend_line,
                ]
                if trend_meaning:
                    lines.append(trend_meaning)
                lines.append(f"Khuyến nghị: vẫn đeo N95/KF94 và tránh giờ cao điểm nếu phải di chuyển.")
                return pack(lines, stale_badge=True)

            chosen = a if int(a.rank) < int(b.rank) else b
            other = b if chosen is a else a
            lines = [
                f"Mình chọn {chosen.districtName} để ưu tiên xử lý trước.",
                f"Lý do: {chosen.districtName} đang có mức rủi ro hiện trạng cao hơn {other.districtName}.",
                trend_line,
                "Khuyến nghị: dồn nguồn lực kiểm soát cho quận này trước, quận còn lại theo dõi tăng cường.",
            ]
            if trend_meaning:
                lines.append(trend_meaning)
            return pack(lines, stale_badge=True)

    if mode == "warning":
        triggers: List[str] = []
        if shares.get("C1", 0.0) >= 0.32:
            triggers.append("mức vượt chuẩn cao")
        if shares.get("C2", 0.0) >= 0.30:
            triggers.append("ô nhiễm kéo dài")
        if shares.get("C3", 0.0) >= 0.24:
            triggers.append("điểm nóng lặp lại")
        if shares.get("C4", 0.0) >= 0.22:
            triggers.append("khí tượng bất lợi")
        if trend and float(trend.get("delta", 0.0)) >= 2:
            triggers.append("xu hướng ngắn hạn tăng")
        return pack(
            [
                f"Mức cảnh báo ngắn hạn tại {target.districtName}: {'Cao' if len(triggers) >= 2 else ('Trung bình' if triggers else 'Thấp')}.",
                f"Tín hiệu chính: {', '.join(triggers) if triggers else 'chưa có tín hiệu vượt trội'}.",
                f"AHP hiện trạng: hạng {target.rank}, score {score:.6f}.",
                trend_line,
                f"Độ tin cậy: {confidence}.",
                f"Khuyến nghị: giám sát theo các khung giờ {windows}.",
            ],
            stale_badge=True,
        )

    # default / detailed / action / evidence
    return pack(
        [
            f"{target.districtName}: {sev} (hạng {target.rank}, score {score:.6f}).",
            f"AHP hiện trạng: chi phối bởi {_criterion_label_clean(d1)} ({shares.get(d1, 0.0) * 100:.1f}%) và {_criterion_label_clean(d2)} ({shares.get(d2, 0.0) * 100:.1f}%).",
            trend_line,
            f"Độ tin cậy: {confidence}.",
            f"Khuyến nghị: {sev_note} Dù xu hướng ngắn hạn đi ngang/giảm, nền ô nhiễm hiện tại vẫn cần kiểm soát nếu AHP còn cao.",
        ],
        stale_badge=True,
    )


@router.post("/chat", response_model=AIChatResponse)
async def ai_chat(req: AIChatRequest):
    rows = _normalized_district_rows(req.district_rows)
    if not rows and req.decision_date:
        try:
            rows = await _load_rank_rows_for_date(req.decision_date)
            if rows and not (req.ranking_source or "").strip():
                req.ranking_source = "AHP bước 4 (tự động đồng bộ từ dữ liệu ngày)"
        except Exception:
            rows = []

    await _auto_attach_forecast_series(req)

    last_user_message = ""
    for m in reversed(req.messages or []):
        if m.role == "user" and (m.content or "").strip():
            last_user_message = m.content.strip()
            break

    intent = _detect_rank_query_intent(last_user_message)
    temporal_mode = _detect_temporal_query_mode(last_user_message)
    chat_intent = _detect_chat_intent(last_user_message)

    if rows and temporal_mode == "rank_change_yesterday":
        return _response(await _build_temporal_yesterday_compare_reply(req, rows))

    if temporal_mode == "weekly_7d":
        return _response(_build_temporal_weekly_reply(req))

    if chat_intent == "emergency_health":
        return _response(_build_emergency_health_reply(req, rows, last_user_message))

    if chat_intent == "data_quality":
        return _response(_build_data_quality_reply(req, rows))
    if chat_intent == "stale_forecast_compare_question":
        return _response(_build_stale_forecast_compare_question_reply(req, rows, last_user_message))
    if chat_intent in ("ahp_vs_ai_compare", "ahp_vs_short_term_compare"):
        return _response(_build_ahp_vs_short_term_compare_reply(req, rows, last_user_message))
    if chat_intent == "high_rank_but_no_extra_short_term_risk":
        return _response(_build_high_rank_but_no_extra_short_term_risk_reply(req, rows, last_user_message))
    if chat_intent == "low_rank_but_short_term_watch":
        return _response(_build_low_rank_but_short_term_watch_reply(req, rows, last_user_message))
    if chat_intent == "scenario_compare":
        return _response(_build_scenario_compare_reply(req, rows))
    if chat_intent == "short_term_risk":
        return _response(_build_short_term_risk_reply(req, rows, last_user_message))
    if chat_intent == "operational_advice":
        return _response(_build_operational_advice_reply(req, rows, last_user_message))
    if chat_intent == "ahp_current":
        return _response(_build_ahp_current_reply(req, rows, last_user_message))

    if temporal_mode == "trend_3h":
        return _response(_build_temporal_trend_reply(req))

    if rows and temporal_mode in ("peak_window", "district_peak_window"):
        ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
        district = _extract_target_district(last_user_message, rows)
        if district is None:
            district = ranked[0]
        hours = _extract_hour_window(last_user_message, default_hours=6)
        if temporal_mode == "district_peak_window":
            reply = _build_temporal_district_peak_reply(req, rows, hours)
        else:
            reply = _build_temporal_peak_window_reply(req, rows, district, hours)
        return _response(reply)

    direct_grounded_modes = {"decision_yes_no", "compare", "timeslot", "data_issue", "time_anomaly", "safe_time_estimate", "detailed"}
    risk_mode = _detect_risk_response_mode(last_user_message)
    if rows and risk_mode in direct_grounded_modes:
        ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
        district = _extract_target_district(last_user_message, rows)
        if district is None:
            district = ranked[0]
        return _response(_build_grounded_risk_reply(req, rows, district, mode=risk_mode, user_question=last_user_message))

    if rows and _is_risk_analysis_question(last_user_message):
        ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
        district = _extract_target_district(last_user_message, rows)
        if district is None and intent == "highest":
            district = ranked[0]
        if district is None and intent == "lowest":
            district = ranked[-1]
        if district is not None:
            mode = _detect_risk_response_mode(last_user_message)
            return _response(_build_grounded_risk_reply(req, rows, district, mode=mode, user_question=last_user_message))

    if rows and intent in ("highest", "lowest"):
        return _response(_build_grounded_rank_reply(req, rows, intent))

    if rows and _is_rank_dependent_question(last_user_message):
        ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
        district = _extract_target_district(last_user_message, rows)
        if district is None and intent == "lowest":
            district = ranked[-1]
        if district is None:
            district = ranked[0]
        mode = _detect_risk_response_mode(last_user_message)
        return _response(_build_grounded_risk_reply(req, rows, district, mode=mode, user_question=last_user_message))

    normalized_messages = _normalize_chat_messages(req)
    if len(normalized_messages) <= 1:
        raise HTTPException(status_code=400, detail="Vui lòng nhập nội dung chat")

    provider = (req.provider or os.getenv("LLM_PROVIDER", "ollama")).strip().lower()
    if provider == "openai":
        model = req.model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        reply = await _chat_openai(model=model, messages=normalized_messages, temperature=req.temperature)
        return _response(reply, provider="openai", model=model)

    model = req.model or os.getenv("OLLAMA_MODEL", "llama3.1:8b")
    reply = await _chat_ollama(model=model, messages=normalized_messages, temperature=req.temperature)
    return _response(reply, provider="ollama", model=model)



# ===== Recovered helper functions after UTF-8 patch =====
def _forecast_start_hour() -> datetime:
    now = datetime.now()
    return now.replace(minute=0, second=0, microsecond=0)


def _select_future_indices(times: List[str], limit: int) -> List[int]:
    start_hour = _forecast_start_hour()
    idxs: List[int] = []
    for idx, raw in enumerate(times):
        dt = _parse_iso_time(str(raw))
        if dt is None:
            continue
        if _to_local_naive(dt) >= start_hour:
            idxs.append(idx)
    if not idxs:
        return []
    return idxs[: max(1, int(limit or 1))]


def _future_forecast_points(points: Optional[List[AIChatForecastPoint]]) -> List[AIChatForecastPoint]:
    safe_points = [p for p in (points or []) if p is not None]
    if not safe_points:
        return []
    start_hour = _forecast_start_hour()
    out: List[AIChatForecastPoint] = []
    for p in safe_points:
        dt = _parse_iso_time(str(p.time))
        if dt is None:
            continue
        if _to_local_naive(dt) >= start_hour:
            out.append(p)
    return out


def _analyze_forecast_trend(points: Optional[List[AIChatForecastPoint]]) -> Optional[Dict[str, object]]:
    future = _future_forecast_points(points)
    if len(future) < 2:
        return None
    values = [float(p.risk_score_0_100 or 0.0) for p in future]
    current = values[0]
    next_vals = values[1:4] if len(values) >= 4 else values[1:]
    if not next_vals:
        return None
    avg_next = sum(next_vals) / len(next_vals)
    delta = avg_next - current
    if delta >= 2:
        trend = "tăng"
    elif delta > -2:
        trend = "đi ngang"
    else:
        trend = "giảm"
    peak_idx = max(range(len(values)), key=lambda i: values[i])
    peak = values[peak_idx]
    peak_time = str(future[peak_idx].time)
    return {
        "current": current,
        "avg_next": avg_next,
        "delta": delta,
        "trend": trend,
        "peak": peak,
        "peak_time": peak_time,
    }


def _extract_question_hour(text: str) -> Optional[int]:
    q = _normalize_plain(text)
    if not q:
        return None
    m = re.search(r"\b([01]?\d|2[0-3])[:h]([0-5]\d)?\b", q)
    if m:
        return max(0, min(23, int(m.group(1))))
    m2 = re.search(r"\b([01]?\d|2[0-3])\s*gio\b", q)
    if m2:
        return max(0, min(23, int(m2.group(1))))
    return None


def _hour_in_range(hour: int, start: int, end: int) -> bool:
    if start <= end:
        return start <= hour < end
    return hour >= start or hour < end


def _extract_hour_window(text: str, default_hours: int = 6) -> int:
    q = _normalize_plain(text)
    m = re.search(r"(\d{1,2})\s*gio", q)
    if not m:
        return default_hours
    h = int(m.group(1))
    return max(1, min(24, h))


def _detect_temporal_query_mode(text: str) -> Optional[str]:
    q = _normalize_plain(text)
    if not q:
        return None
    if "7 ngay" in q or "bay ngay" in q or "1 tuan" in q or "tuan toi" in q:
        return "weekly_7d"
    if "hom qua" in q and any(s in q for s in ["tang hang", "giam hang", "thay doi hang", "manh nhat", "so voi hom qua"]):
        return "rank_change_yesterday"
    if "xu huong" in q and "3 gio" in q:
        return "trend_3h"
    if "quan nao" in q and any(s in q for s in ["nguy co cao nhat", "cao nhat", "anh huong nhat"]) and any(s in q for s in ["gio toi", "6 gio", "khung gio"]):
        return "district_peak_window"
    if "khung gio" in q and any(s in q for s in ["nguy co cao nhat", "cao nhat", "nguy hiem"]):
        return "peak_window"
    return None


def _detect_risk_response_mode(text: str) -> str:
    q = _normalize_plain(text)
    if not q:
        return "full"
    if any(k in q for k in ["co nen", "nen", "duoc khong", "an toan khong"]) and any(s in q for s in ["chay bo", "di bo", "ra ngoai", "dap xe", "tap the duc", "di hoc", "di lam"]):
        return "decision_yes_no"
    if any(s in q for s in ["may gio", "khi nao an toan", "bao gio an toan", "sau may gio", "mo cua luc may gio"]):
        return "safe_time_estimate"
    if re.search(r"\b([01]?\d|2[0-3])\s*[:h]\s*([0-5]\d)?\b", q) or re.search(r"\b([01]?\d|2[0-3])\s*gio\b", q):
        return "timeslot"
    if ("so sanh" in q or "chenh lech" in q) and any(k in q for k in ["quan", "top", "hang", "score", "rui ro", "nguy co"]):
        return "compare"
    if any(s in q for s in ["loi du lieu", "sai du lieu", "du lieu cu", "du lieu cham"]):
        return "data_issue"
    if any(s in q for s in ["sai ngay", "loi thoi gian", "qua khu", "hom qua"]) and any(k in q for k in ["du bao", "dinh", "6 gio", "7 ngay"]):
        return "time_anomaly"
    if any(s in q for s in ["canh bao", "muc do rui ro", "nguy co", "rui ro"]):
        return "warning"
    if any(s in q for s in ["can lam gi", "nen lam gi", "khuyen nghi", "khuyen cao", "hanh dong"]):
        return "action"
    if any(s in q for s in ["bao cao chi tiet", "phan tich chi tiet", "lop 1", "lop 2", "lop 3"]):
        return "detailed"
    return "full"


def _is_rank_dependent_question(text: str) -> bool:
    q = _normalize_plain(text)
    if not q:
        return False
    signals = [
        "o nhiem nhat",
        "it o nhiem nhat",
        "khu vuc anh huong nhat",
        "diem nong nhat",
        "top 1",
        "hang 1",
        "dua vao so lieu nao",
        "co so du lieu",
        "canh bao som",
        "rui ro",
        "nguy co",
    ]
    return any(s in q for s in signals)


def _detect_activity_kind(text: str) -> str:
    q = _normalize_plain(text)
    if "chay bo" in q:
        return "?i ch?y b?"
    if "dap xe" in q:
        return "?i ??p xe"
    if "di bo" in q:
        return "?i b?"
    if "di hoc" in q:
        return "?i h?c"
    if "di lam" in q:
        return "?i l?m"
    return "ra ngo?i"


def _is_personal_health_query(text: str) -> bool:
    q = _normalize_plain(text)
    keywords = ["co nen", "nen", "an toan", "chay bo", "di bo", "dap xe", "ra ngoai", "suc khoe", "choi"]
    return any(k in q for k in keywords)


def _estimate_safe_time_from_trend(req: AIChatRequest, safe_threshold: float = 45.0) -> Optional[Dict[str, object]]:
    future = _future_forecast_points(req.forecast_series)
    if not future:
        return None
    values = [float(p.risk_score_0_100 or 0.0) for p in future]
    times = [str(p.time) for p in future]
    if values[0] <= safe_threshold:
        return {"already_safe": True, "safe_time": times[0], "eta_hours": 0.0}
    for i, v in enumerate(values[1:], start=1):
        if v <= safe_threshold:
            return {"already_safe": False, "safe_time": times[i], "eta_hours": float(i)}
    return {"already_safe": False, "safe_time": "", "eta_hours": -1.0}


def _build_temporal_trend_reply(req: AIChatRequest) -> str:
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend:
        return "T?i ch?a c? ?? d? li?u forecast m?i ?? k?t lu?n xu h??ng 3 gi? t?i."
    return (
        f"Trong 3 gi? t?i, xu h??ng r?i ro ?ang {trend['trend']}.\n"
        f"Hi?n t?i {float(trend['current']):.1f}, trung b?nh 3 gi? t?i {float(trend['avg_next']):.1f}, ch?nh {float(trend['delta']):.1f} ?i?m."
    )


def _build_temporal_peak_window_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], district: AIChatDistrictRow, hours: int) -> str:
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend:
        return f"T?i ch?a c? ?? forecast m?i ?? ch?t khung gi? nguy c? cao nh?t trong {hours} gi? t?i."
    return (
        f"Trong {hours} gi? t?i, m?c nguy c? cao nh?t ?ang r?i v?o {trend['peak_time']}.\n"
        f"Qu?n tham chi?u hi?n t?i: {district.districtName} (h?ng {district.rank}, score {district.score:.6f})."
    )


def _build_temporal_district_peak_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], hours: int) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top = ranked[0] if ranked else None
    trend = _analyze_forecast_trend(req.forecast_series)
    if not top:
        return "Ch?a c? b?ng AHP ?? x?c ??nh qu?n c?n ?u ti?n trong ng?n h?n."
    if not trend:
        return f"Theo AHP hi?n t?i, qu?n ??ng ch? ? nh?t l? {top.districtName}; hi?n ch?a ?? forecast m?i ?? so s?nh ng?n h?n theo qu?n."
    return (
        f"Trong {hours} gi? t?i, qu?n ??ng ch? ? nh?t v?n l? {top.districtName} theo AHP hi?n t?i.\n"
        f"Xu h??ng ng?n h?n t?i ?i?m theo d?i: {trend['trend']} (??nh {float(trend['peak']):.1f} l?c {trend['peak_time']})."
    )


def _build_temporal_weekly_reply(req: AIChatRequest) -> str:
    pts = _future_forecast_points(req.forecast_series)
    if len(pts) < 24:
        return "T?i ch?a c? d? li?u cho 7 ng?y t?i. Hi?n m?i ?? d? li?u ng?n h?n theo gi?."

    parsed = [_parse_iso_time(str(p.time)) for p in pts]
    parsed = [d for d in parsed if d is not None]
    if not parsed:
        return "T?i ch?a c? d? li?u cho 7 ng?y t?i do chu?i forecast ch?a h?p l?."

    start = min(_to_local_naive(d) for d in parsed)
    end = max(_to_local_naive(d) for d in parsed)
    coverage_h = (end - start).total_seconds() / 3600.0
    if coverage_h < 24 * 6.5:
        return "T?i ch?a c? d? li?u cho 7 ng?y t?i. D? li?u forecast hi?n t?i ch?a ph? ?? m?t tu?n."

    max_pt = max(pts, key=lambda p: float(p.risk_score_0_100 or 0.0))
    min_pt = min(pts, key=lambda p: float(p.risk_score_0_100 or 0.0))
    avg = sum(float(p.risk_score_0_100 or 0.0) for p in pts) / max(1, len(pts))
    return (
        f"Trong 7 ng?y t?i, m?c r?i ro cao nh?t d? ki?n v?o {max_pt.time} (m?c {float(max_pt.risk_score_0_100):.1f}/100). "
        f"M?c th?p nh?t v?o {min_pt.time} (m?c {float(min_pt.risk_score_0_100):.1f}/100), trung b?nh tu?n kho?ng {avg:.1f}/100."
    )


async def _build_temporal_yesterday_compare_reply(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> str:
    today_rows = sorted(rows, key=lambda r: (r.rank, -r.score))
    if not req.decision_date:
        return "Ch?a c? ng?y quy?t ??nh ?? so s?nh v?i h?m qua."
    try:
        cur_date = date_cls.fromisoformat(str(req.decision_date))
    except Exception:
        return "Ng?y quy?t ??nh ch?a h?p l? ?? so s?nh v?i h?m qua."

    prev_date = (cur_date - timedelta(days=1)).isoformat()
    y_rows = await _load_rank_rows_for_date(prev_date)
    if not y_rows:
        return f"T?i ch?a c? d? li?u ng?y {prev_date} ?? x?c ??nh qu?n t?ng h?ng r?i ro m?nh nh?t."

    prev_map = {_district_key(r.districtName): r for r in y_rows}
    deltas: List[tuple[float, AIChatDistrictRow, AIChatDistrictRow]] = []
    for r in today_rows:
        p = prev_map.get(_district_key(r.districtName))
        if p:
            deltas.append((float(p.rank) - float(r.rank), r, p))

    if not deltas:
        return "Kh?ng ?? c?p d? li?u giao nhau gi?a h?m nay v? h?m qua ?? so s?nh h?ng."
    best = max(deltas, key=lambda x: x[0])
    shift, cur, prev = best
    if shift <= 0:
        return f"Kh?ng c? qu?n n?o t?ng h?ng r?i ro r? so v?i h?m qua; qu?n d?n ??u hi?n t?i v?n l? {today_rows[0].districtName}."
    return (
        f"Qu?n t?ng h?ng r?i ro m?nh nh?t so v?i h?m qua l? {cur.districtName} (t?ng {int(shift)} h?ng).\n"
        f"H?m qua h?ng {prev.rank}, h?m nay h?ng {cur.rank}."
    )



# ===== Recovered core reply builders =====
def _build_ahp_current_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    if not rows:
        return _compose_sections(
            conclusion="Hiện chưa có bảng xếp hạng AHP để kết luận.",
            data_basis="Thiếu dữ liệu rank và score theo quận trong phiên.",
            confidence="Thấp (thiếu dữ liệu AHP).",
            recommendation="Nạp dữ liệu ngày hiện tại để hệ thống đồng bộ bảng AHP.",
        )

    ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
    q = _normalize_plain(question)
    pair = _extract_target_districts(question, rows)
    target = _extract_target_district(question, rows) or ranked[0]
    cvals = _extract_c_values(target)
    top_driver = max(cvals.items(), key=lambda kv: kv[1])[0]

    if ("vi sao" in q or "cao hon" in q or "thap hon" in q) and len(pair) >= 2:
        a, b = pair[0], pair[1]
        av = _extract_c_values(a)
        bv = _extract_c_values(b)
        diff_key = max(av.keys(), key=lambda k: abs(av[k] - bv[k]))
        return _compose_sections(
            conclusion=f"{a.districtName} và {b.districtName} chênh lệch chủ yếu ở {_criterion_label_clean(diff_key)}.",
            data_basis=(
                f"{a.districtName}: hạng {a.rank}, score {a.score:.6f}; "
                f"{b.districtName}: hạng {b.rank}, score {b.score:.6f}; "
                f"{_criterion_label_clean(diff_key)} chênh {abs(av[diff_key] - bv[diff_key]):.6f}."
            ),
            confidence="Cao (bám trực tiếp bảng AHP hiện trạng).",
            recommendation="Ưu tiên quận có hạng cao hơn để theo dõi/can thiệp trước.",
        )

    if "top 3" in q:
        top3 = ", ".join(f"{r.districtName} (#{r.rank}, {r.score:.6f})" for r in ranked[:3])
        return _compose_sections(
            conclusion=f"Top 3 ưu tiên hiện tại: {top3}.",
            data_basis=f"Nguồn xếp hạng: {(req.ranking_source or 'AHP').strip()}.",
            confidence="Cao (bám trực tiếp bảng AHP hiện trạng).",
            recommendation="Ưu tiên kiểm tra thực địa theo thứ tự Top 3.",
        )

    if "chi phoi" in q or "tieu chi nao" in q:
        return _compose_sections(
            conclusion=f"Tiêu chí chi phối {target.districtName} hiện tại là {_criterion_label_clean(top_driver)}.",
            data_basis=f"Hạng {target.rank}, score {target.score:.6f}; " + ", ".join(f"{k}={v:.6f}" for k, v in cvals.items()),
            confidence="Cao (bám theo C1-C4 của quận trong bảng AHP).",
            recommendation=f"Tập trung biện pháp giảm tác động theo tiêu chí {_criterion_label_clean(top_driver)}.",
        )

    intent = _detect_rank_query_intent(question)
    if intent == "lowest":
        low = ranked[-1]
        return _compose_sections(
            conclusion=f"Quận có mức ưu tiên thấp nhất hiện tại là {low.districtName} (hạng {low.rank}).",
            data_basis=f"Score {low.score:.6f}.",
            confidence="Cao (bám trực tiếp bảng AHP hiện trạng).",
            recommendation="Tiếp tục theo dõi định kỳ; chưa cần ưu tiên can thiệp trước nhóm hạng cao.",
        )

    top = ranked[0]
    return _compose_sections(
        conclusion=f"Quận cần ưu tiên theo dõi nhất hiện tại là {top.districtName} (hạng {top.rank}).",
        data_basis=f"Score {top.score:.6f}; C1={top.C1 or 0:.6f}, C2={top.C2 or 0:.6f}, C3={top.C3 or 0:.6f}, C4={top.C4 or 0:.6f}.",
        confidence="Cao (bám trực tiếp bảng AHP hiện trạng).",
        recommendation="Dùng kết quả này cho quyết định hiện trạng theo quận.",
    )


def _build_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _format_data_quality(req, rows)
    trend = _analyze_forecast_trend(req.forecast_series)
    hour_window = _extract_hour_window(question, default_hours=6)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    is_global = _is_global_short_term_compare_question(question)
    target = _extract_target_district(question, rows) if rows else None
    if target is None and ranked and not is_global:
        target = ranked[0]
    target_name = target.districtName if target else "khu v?c ?ang ch?n"
    top_ahp = ranked[0] if ranked else None

    if not trend:
        note = str(quality["stale_note"] or "Thi?u chu?i forecast theo gi? ?? k?t lu?n ng?n h?n.")
        if is_global and target is None:
            return _compose_sections(
                conclusion="Ch?a ?? c? s? ?? k?t lu?n qu?n n?o c? nguy c? ng?n h?n n?i b?t to?n c?c.",
                data_basis=(f"AHP hi?n tr?ng: {top_ahp.districtName} (h?ng {top_ahp.rank}, score {top_ahp.score:.6f}); " if top_ahp else "") + f"forecast m?i nh?t: {quality['latest']}.",
                confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
                recommendation="T?m th?i d?a v?o AHP hi?n tr?ng ?? ?u ti?n theo qu?n; c?p nh?t forecast m?i tr??c khi k?t lu?n ch?nh l?ch ng?n h?n.",
                note=note,
                stale_badge=True,
            )
        return _compose_sections(
            conclusion=f"Hi?n ch?a ?? c? s? ?? k?t lu?n m?nh nguy c? ng?n h?n cho {target_name} trong {hour_window} gi? t?i.",
            data_basis=f"AHP hi?n tr?ng: {(target_name if target else 'ch?a c? theo qu?n')}; forecast m?i nh?t: {quality['latest']}.",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="Ch? d?ng AI nh? h? tr? v?n h?nh; c?p nh?t forecast m?i tr??c khi ra quy?t ??nh m?nh.",
            note=note,
            stale_badge=True,
        )

    if is_global and target is None:
        note = str(quality["stale_note"] or "")
        return _compose_sections(
            conclusion="AI ch?a th? x?p h?ng nguy c? ng?n h?n to?n c?c theo qu?n t? d? li?u hi?n c?.",
            data_basis=(f"AHP hi?n tr?ng: {top_ahp.districtName} ?ang h?ng {top_ahp.rank}, score {top_ahp.score:.6f}; " if top_ahp else "") + f"xu h??ng t?i ?i?m theo d?i: {trend['trend']}, hi?n t?i {float(trend['current']):.1f}, trung b?nh 3 gi? t?i {float(trend['avg_next']):.1f}.",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="D?ng AHP ?? ?u ti?n theo qu?n; d?ng AI ng?n h?n ?? h? tr? v?n h?nh t?i ?i?m theo d?i hi?n t?i.",
            note=note or "?? so s?nh to?n c?c theo qu?n, c?n forecast ng?n h?n ph? theo t?ng qu?n.",
            stale_badge=bool(note),
        )

    delta = float(trend["delta"])
    level = "cao" if delta >= 2 else ("trung b?nh" if delta > -2 else "?ang gi?m")
    note = str(quality["stale_note"] or "")
    return _compose_sections(
        conclusion=f"Nguy c? ng?n h?n c?a {target_name} trong {hour_window} gi? t?i ? m?c {level}, xu h??ng {trend['trend']}.",
        data_basis=(f"AHP hi?n tr?ng ({target_name} h?ng {target.rank}, {target.score:.6f}); " if target else "AHP hi?n tr?ng: ch?a c? theo qu?n; ") + f"forecast g?n: hi?n t?i {float(trend['current']):.1f}, trung b?nh 3 gi? t?i {float(trend['avg_next']):.1f}, ??nh {float(trend['peak']):.1f} l?c {trend['peak_time']}.",
        confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
        recommendation="?u ti?n gi?m ph?i nhi?m ngo?i tr?i v? t?ng t?n su?t theo d?i theo gi?." if delta >= 2 else "Ti?p t?c gi?m s?t ng?n h?n, c?p nh?t forecast ??nh k?.",
        note=note,
        stale_badge=bool(note),
    )


def _build_ahp_vs_short_term_compare_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _format_data_quality(req, rows)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top_ahp = ranked[0] if ranked else None
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend or quality["stale_note"] or not quality["has_future"]:
        return _compose_sections(
            conclusion=(f"AHP hi?n tr?ng ?ang ?u ti?n {top_ahp.districtName} (h?ng {top_ahp.rank}, score {top_ahp.score:.6f}). " if top_ahp else "") + "Ph?n AI ng?n h?n hi?n ch?a ?? c? s? ?? k?t lu?n kh?c bi?t theo qu?n.",
            data_basis=f"Forecast m?i nh?t: {quality['latest']}; chu?i t??ng lai: {'??' if quality['has_future'] else 'ch?a ??'}.",
            confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
            recommendation="K?t lu?n ch?c ? l?p AHP hi?n tr?ng; c?p nh?t forecast m?i tr??c khi so s?nh kh?c bi?t ng?n h?n gi?a c?c qu?n.",
            note=str(quality["stale_note"] or "Thi?u d? li?u forecast theo gi? ?? t?ch r? vai tr? AI ng?n h?n."),
            stale_badge=True,
        )
    delta = float(trend.get("delta") or 0.0)
    bridge = "D? xu h??ng ng?n h?n ?ang ?i ngang ho?c gi?m, n?n r?i ro hi?n tr?ng v?n c? th? cao n?u qu?n ?ang h?ng ??u theo AHP."
    if delta >= 2:
        bridge = "N?n r?i ro AHP ?? cao v? xu h??ng ng?n h?n c?n t?ng, c?n ?u ti?n x? l? s?m."
    return _compose_sections(
        conclusion=(f"AHP hi?n tr?ng: {top_ahp.districtName} ?ang h?ng {top_ahp.rank}, score {top_ahp.score:.6f}. " if top_ahp else "AHP hi?n tr?ng: ch?a c? qu?n m?c ti?u. ") + f"AI ng?n h?n: xu h??ng {trend['trend']}, hi?n t?i {float(trend['current']):.1f}, trung b?nh 3 gi? t?i {float(trend['avg_next']):.1f}.",
        data_basis="AHP d?ng rank/score/C1-C4 theo qu?n; AI d?ng forecast g?n ?? ??c xu h??ng v?i gi? t?i.",
        confidence=str(quality.get("confidence_text") or _confidence_text(float(quality.get("score") or 0))),
        recommendation=bridge,
    )


def _extract_c_values(row: AIChatDistrictRow) -> Dict[str, float]:
    return {
        "C1": float(row.C1 or 0.0),
        "C2": float(row.C2 or 0.0),
        "C3": float(row.C3 or 0.0),
        "C4": float(row.C4 or 0.0),
    }


def _is_global_short_term_compare_question(question: str) -> bool:
    q = _normalize_plain(question)
    if not q:
        return False
    compare_keywords = ("quan nao", "co quan nao", "rank chua cao", "hang chua cao", "hang cao", "rank cao", "so sanh")
    short_keywords = ("ngan han", "6 gio", "24 gio", "canh bao som", "nguy co tang")
    return any(k in q for k in compare_keywords) and any(k in q for k in short_keywords)


def _parse_iso_time(raw: str) -> Optional[datetime]:
    if not raw:
        return None
    s = str(raw).strip()
    try:
        if s.endswith('Z'):
            s = s[:-1] + '+00:00'
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _to_local_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    try:
        return dt.astimezone().replace(tzinfo=None)
    except Exception:
        return dt.replace(tzinfo=None)



def _detect_activity_kind(text: str) -> str:
    q = _normalize_plain(text)
    if "chay bo" in q:
        return "đi chạy bộ"
    if "dap xe" in q:
        return "đi đạp xe"
    if "di bo" in q:
        return "đi bộ"
    if "di hoc" in q:
        return "đi học"
    if "di lam" in q:
        return "đi làm"
    return "ra ngoài"


def _build_temporal_trend_reply(req: AIChatRequest) -> str:
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend:
        return "Tôi chưa có đủ dữ liệu forecast mới để kết luận xu hướng 3 giờ tới."
    return (
        f"Trong 3 giờ tới, xu hướng rủi ro đang {trend['trend']}.\n"
        f"Hiện tại {float(trend['current']):.1f}, trung bình 3 giờ tới {float(trend['avg_next']):.1f}, chênh {float(trend['delta']):.1f} điểm."
    )


def _build_temporal_peak_window_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], district: AIChatDistrictRow, hours: int) -> str:
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend:
        return f"Tôi chưa có đủ forecast mới để chốt khung giờ nguy cơ cao nhất trong {hours} giờ tới."
    return (
        f"Trong {hours} giờ tới, mốc nguy cơ cao nhất đang rơi vào {trend['peak_time']}.\n"
        f"Quận tham chiếu hiện tại: {district.districtName} (hạng {district.rank}, score {district.score:.6f})."
    )


def _build_temporal_district_peak_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], hours: int) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top = ranked[0] if ranked else None
    trend = _analyze_forecast_trend(req.forecast_series)
    if not top:
        return "Chưa có bảng AHP để xác định quận cần ưu tiên trong ngắn hạn."
    if not trend:
        return f"Theo AHP hiện tại, quận đáng chú ý nhất là {top.districtName}; hiện chưa đủ forecast mới để so sánh ngắn hạn theo quận."
    return (
        f"Trong {hours} giờ tới, quận đáng chú ý nhất vẫn là {top.districtName} theo AHP hiện tại.\n"
        f"Xu hướng ngắn hạn tại điểm theo dõi: {trend['trend']} (đỉnh {float(trend['peak']):.1f} lúc {trend['peak_time']})."
    )


def _build_temporal_weekly_reply(req: AIChatRequest) -> str:
    pts = _future_forecast_points(req.forecast_series)
    if len(pts) < 24:
        return "Tôi chưa có dữ liệu cho 7 ngày tới. Hiện mới đủ dữ liệu ngắn hạn theo giờ."

    parsed = [_parse_iso_time(str(p.time)) for p in pts]
    parsed = [d for d in parsed if d is not None]
    if not parsed:
        return "Tôi chưa có dữ liệu cho 7 ngày tới do chuỗi forecast chưa hợp lệ."

    start = min(_to_local_naive(d) for d in parsed)
    end = max(_to_local_naive(d) for d in parsed)
    coverage_h = (end - start).total_seconds() / 3600.0
    if coverage_h < 24 * 6.5:
        return "Tôi chưa có dữ liệu cho 7 ngày tới. Dữ liệu forecast hiện tại chưa phủ đủ một tuần."

    max_pt = max(pts, key=lambda p: float(p.risk_score_0_100 or 0.0))
    min_pt = min(pts, key=lambda p: float(p.risk_score_0_100 or 0.0))
    avg = sum(float(p.risk_score_0_100 or 0.0) for p in pts) / max(1, len(pts))
    return (
        f"Trong 7 ngày tới, mốc rủi ro cao nhất dự kiến vào {max_pt.time} (mức {float(max_pt.risk_score_0_100):.1f}/100). "
        f"Mốc thấp nhất vào {min_pt.time} (mức {float(min_pt.risk_score_0_100):.1f}/100), trung bình tuần khoảng {avg:.1f}/100."
    )


async def _build_temporal_yesterday_compare_reply(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> str:
    today_rows = sorted(rows, key=lambda r: (r.rank, -r.score))
    if not req.decision_date:
        return "Chưa có ngày quyết định để so sánh với hôm qua."
    try:
        cur_date = date_cls.fromisoformat(str(req.decision_date))
    except Exception:
        return "Ngày quyết định chưa hợp lệ để so sánh với hôm qua."

    prev_date = (cur_date - timedelta(days=1)).isoformat()
    y_rows = await _load_rank_rows_for_date(prev_date)
    if not y_rows:
        return f"Tôi chưa có dữ liệu ngày {prev_date} để xác định quận tăng hạng rủi ro mạnh nhất."

    prev_map = {_district_key(r.districtName): r for r in y_rows}
    deltas: List[tuple[float, AIChatDistrictRow, AIChatDistrictRow]] = []
    for r in today_rows:
        p = prev_map.get(_district_key(r.districtName))
        if p:
            deltas.append((float(p.rank) - float(r.rank), r, p))

    if not deltas:
        return "Không đủ cặp dữ liệu giao nhau giữa hôm nay và hôm qua để so sánh hạng."
    best = max(deltas, key=lambda x: x[0])
    shift, cur, prev = best
    if shift <= 0:
        return f"Không có quận nào tăng hạng rủi ro rõ so với hôm qua; quận dẫn đầu hiện tại vẫn là {today_rows[0].districtName}."
    return (
        f"Quận tăng hạng rủi ro mạnh nhất so với hôm qua là {cur.districtName} (tăng {int(shift)} hạng).\n"
        f"Hôm qua hạng {prev.rank}, hôm nay hạng {cur.rank}."
    )



# ===== Human-first DSS overrides (latest) =====
def _source_note(req: AIChatRequest) -> str:
    src = (req.ranking_source or "AHP").strip()
    date_text = (req.decision_date or "").strip()
    return f"Nguồn dữ liệu đang dùng: {src}{', ngày ' + date_text if date_text else ''}."


def _detect_chat_intent(text: str) -> str:
    q = _normalize_plain(text)
    if not q:
        return "ahp_current"

    emergency_terms = (
        "kho tho", "dau nguc", "dau phoi", "chong mat", "muon xiu", "xiu", "buon non",
        "mu mit", "toi sam", "khong thay nha", "khac cam bien", "cay mat", "ngat"
    )
    if any(t in q for t in emergency_terms):
        return "emergency_health"

    stale_terms = ("forecast cu", "du lieu cu", "lech thoi gian", "forecast chua moi", "timestamp cu")
    compare_terms_global = ("so sanh", "ahp va ai", "ahp voi ai", "hien trang va ngan han")
    if any(t in q for t in stale_terms) and any(t in q for t in compare_terms_global):
        return "stale_forecast_compare_question"

    if any(t in q for t in ("hang cao nhung", "rank cao nhung", "chua thay nguy co tang", "khong thay nguy co tang", "khong tang them trong ngan han", "chua thay tang them")):
        return "high_rank_but_no_extra_short_term_risk"

    if any(t in q for t in ("rank chua cao", "hang chua cao", "chua cao nhung", "chua cao ma", "khuyen nghi theo doi them", "dang chu y hon trong ngan han", "danh sach theo doi bo sung", "nhom can giam sat ngan han", "theo doi bo sung", "watchlist")):
        return "low_rank_but_short_term_watch"

    if any(t in q for t in ("scenario", "kich ban", "baseline", "policy")):
        return "scenario_compare"

    if any(t in q for t in ("ahp va ai", "ahp voi ai", "phan biet", "so sanh ahp", "hien trang va ngan han")):
        return "ahp_vs_short_term_compare"

    if any(t in q for t in ("do tin cay", "forecast co moi", "forecast moi", "du lieu cu", "lech thoi gian", "timestamp", "encoding")):
        return "data_quality"

    if any(t in q for t in ("6 gio", "24 gio", "ngan han", "canh bao som", "xu huong", "khung gio", "trong vai gio toi")):
        return "short_term_risk"

    if any(t in q for t in ("nen", "can lam gi", "khuyen nghi", "hanh dong", "co nen", "chay bo", "di bo", "ra ngoai")):
        return "operational_advice"

    return "ahp_current"


def _wants_numbers(question: str) -> bool:
    q = _normalize_plain(question)
    return any(t in q for t in ("tai sao", "vi sao", "so lieu", "cu the", "bao nhieu", "chi so", "score", "rank", "hang", "c1", "c2", "c3", "c4"))


def _friendly_driver_name(k: str) -> str:
    mapping = {
        "C1": "bụi mịn và mức vượt chuẩn hiện tại",
        "C2": "mức ô nhiễm kéo dài theo thời gian",
        "C3": "điểm nóng lặp lại",
        "C4": "điều kiện khí tượng bất lợi",
    }
    return mapping.get(str(k).upper(), str(k))


def _build_emergency_health_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    q = _normalize_plain(question)
    msg = "VÀO QUÁN CAFE CÓ MÁY LẠNH NGAY LẬP TỨC! Đừng ráng chạy xe, sức khỏe của bạn quan trọng hơn."
    if any(t in q for t in ("khong thay nha", "mu mit", "toi sam", "khac cam bien")):
        return (
            msg
            + "\nTôi hiểu. Cảm biến chỉ là thiết bị kỹ thuật, có thể có sai số cục bộ."
            + "\nHãy tin vào mắt và phổi của bạn, tìm nơi trú ẩn ngay."
        )
    return msg + "\nĐeo N95/KF94 ngay và hạn chế vận động mạnh trong 30-60 phút tới."


def _build_ahp_current_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    if not rows:
        return "Mình chưa có bảng xếp hạng AHP hiện tại để kết luận. Bạn nạp dữ liệu ngày đang xem rồi hỏi lại giúp mình."

    ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
    q = _normalize_plain(question)

    if "top 3" in q:
        top3 = ", ".join(f"{r.districtName} (hạng {r.rank})" for r in ranked[:3])
        ans = f"Hiện tại top 3 quận cần ưu tiên theo dõi là: {top3}."
        if _wants_numbers(question):
            ans += " " + "; ".join(f"{r.districtName}: {r.score:.6f}" for r in ranked[:3]) + "."
        return ans

    target = _extract_target_district(question, rows) or ranked[0]
    cvals = _extract_c_values(target)
    key = max(cvals, key=lambda k: cvals[k]) if cvals else "C1"
    ans = f"Nếu chỉ nhìn AHP hiện tại, bạn nên xem {target.districtName} trước."
    ans += f" Yếu tố chi phối là {_friendly_driver_name(key)}."
    if _wants_numbers(question):
        ans += f" (hạng {target.rank}, score {target.score:.6f})"
    return ans


def _build_data_quality_reply(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> str:
    quality = _format_data_quality(req, rows)
    stale = str(quality.get("stale_note") or "").strip()
    if stale:
        return f"Dữ liệu forecast hiện chưa đủ mới (mốc mới nhất: {quality.get('latest')}). Vì vậy độ tin cậy đang giảm và AI chỉ nên dùng để hỗ trợ ngắn hạn."
    return f"Dữ liệu forecast hiện khá mới (mốc mới nhất: {quality.get('latest')}). Độ tin cậy đang ở mức {quality.get('confidence_text')}."


def _build_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _format_data_quality(req, rows)
    trend = _analyze_forecast_trend(req.forecast_series)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    target = _extract_target_district(question, rows) if rows else None
    if target is None and ranked:
        target = ranked[0]
    name = target.districtName if target else "khu vực đang chọn"

    if not trend or quality.get("stale_note"):
        return f"Hiện mình chưa đủ cơ sở để kết luận mạnh nguy cơ ngắn hạn cho {name}. Forecast đang cũ/thiếu, bạn nên ưu tiên quyết định theo AHP hiện trạng."

    delta = float(trend.get("delta") or 0.0)
    if delta >= 2:
        ans = f"Trong vài giờ tới, nguy cơ ở {name} có xu hướng tăng."
    elif delta > -2:
        ans = f"Trong vài giờ tới, nguy cơ ở {name} đang đi ngang."
    else:
        ans = f"Trong vài giờ tới, nguy cơ ở {name} đang giảm."

    if delta > -2 and target and int(target.rank) <= 3:
        ans += " Ổn định ở đây là ổn định ở mức bẩn, tức là bạn vẫn đang kẹt trong vùng ô nhiễm cao."
    if _wants_numbers(question):
        ans += f" (hiện tại {float(trend['current']):.1f}, trung bình 3 giờ tới {float(trend['avg_next']):.1f})"
    return ans


def _build_ahp_vs_short_term_compare_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top = ranked[0] if ranked else None
    trend = _analyze_forecast_trend(req.forecast_series)
    quality = _format_data_quality(req, rows)

    ahp_part = f"AHP cho th?y hi?n tr?ng ?ang n?ng nh?t ? {top.districtName}." if top else "AHP hi?n ch?a ?? d? li?u."
    if not trend or quality.get("stale_note"):
        ai_part = "AI ng?n h?n hi?n ch?a ?? d? li?u m?i ?? k?t lu?n ph?n v?i gi? t?i."
    else:
        ai_part = f"AI ng?n h?n cho th?y xu h??ng {trend['trend']} trong v?i gi? t?i."
    return ahp_part + " " + ai_part


def _build_high_rank_but_no_extra_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top_group = ranked[:3]
    quality = _format_data_quality(req, rows)
    trend = _analyze_forecast_trend(req.forecast_series)
    if not top_group:
        return "Mình chưa có bảng AHP hiện trạng nên chưa xác định được nhóm hạng cao."
    if not trend or quality.get("stale_note"):
        names = ", ".join(r.districtName for r in top_group)
        return f"Trong nhóm hạng cao ({names}), hiện chưa đủ cơ sở dữ liệu mới để khẳng định quận nào chưa tăng thêm."
    if float(trend.get("delta") or 0.0) <= 1.5:
        names = ", ".join(r.districtName for r in top_group)
        return f"Trong nhóm hạng cao hiện tại, chưa thấy tín hiệu tăng thêm rõ ở: {names}."
    return "Nhóm hạng cao đang có dấu hiệu tăng thêm trong ngắn hạn, nên chưa thể nói quận nào an toàn hơn hẳn."


def _build_low_rank_but_short_term_watch_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    quality = _format_data_quality(req, rows)
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend or quality.get("stale_note"):
        return "Hiện chưa đủ cơ sở để xác định rõ quận hạng chưa cao nào cần theo dõi bổ sung trong ngắn hạn."
    if float(trend.get("delta") or 0.0) < 2:
        return "Chưa phát hiện quận hạng chưa cao nào nổi bật hơn hẳn về nguy cơ ngắn hạn."
    low_pool = [r for r in ranked if int(r.rank) >= 7]
    if not low_pool:
        return "Chưa đủ dữ liệu nhóm hạng chưa cao để lập danh sách theo dõi bổ sung."
    candidates = sorted(low_pool, key=lambda r: (float(r.C4 or 0.0) + float(r.C3 or 0.0), -float(r.score or 0.0)), reverse=True)[:3]
    names = ", ".join(f"{r.districtName} (hạng {r.rank})" for r in candidates)
    return f"Có thể mở danh sách theo dõi bổ sung cho: {names}."


def _build_stale_forecast_compare_question_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top = ranked[0] if ranked else None
    if top:
        return f"AHP hi?n tr?ng v?n d?ng ???c v? ?ang ?u ti?n {top.districtName}. Nh?ng forecast ?ang c? ho?c l?ch th?i gian, n?n ch?a ?? c? s? ?? k?t lu?n kh?c bi?t ng?n h?n m?t c?ch ch?c ch?n."
    return "Forecast ?ang c? ho?c l?ch th?i gian, n?n hi?n ch?a ?? c? s? ?? k?t lu?n kh?c bi?t ng?n h?n."


def _build_operational_advice_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    q = _normalize_plain(question)
    if any(t in q for t in ("kho tho", "dau nguc", "chong mat", "xiu", "buon non", "mu mit", "khong thay nha")):
        return _build_emergency_health_reply(req, rows, question)

    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    target = _extract_target_district(question, rows) if rows else None
    if target is None and ranked:
        target = ranked[0]
    name = target.districtName if target else "khu v?c hi?n t?i"

    if "co nen" in q or "nen" in q:
        trend = _analyze_forecast_trend(req.forecast_series)
        if target and int(target.rank) <= 3:
            return f"KH?NG N?N l?m ho?t ??ng ngo?i tr?i l?c n?y ? {name}. M?c n?n hi?n tr?ng ?ang cao, ?u ti?n v?o kh?ng gian k?n v? ?eo N95/KF94."
        if trend and float(trend.get("delta") or 0.0) >= 2:
            return f"KH?NG N?N ch? quan ? {name}. Nguy c? ng?n h?n ?ang t?ng, b?n n?n gi?m ph?i nhi?m ngay."
        return f"N?N ?i l?i c? ki?m so?t ? {name}, nh?ng v?n ?eo N95/KF94 v? tr?nh khung gi? cao ?i?m."

    return _build_short_term_risk_reply(req, rows, question)


def _build_temporal_weekly_reply(req: AIChatRequest) -> str:
    pts = _future_forecast_points(req.forecast_series)
    if len(pts) < 24:
        return "R?t ti?c, t?i hi?n ch? c? d? li?u d? b?o trong h?m nay, ch?a c? d? li?u cho tu?n t?i."

    parsed = [_parse_iso_time(str(p.time)) for p in pts]
    parsed = [d for d in parsed if d is not None]
    if not parsed:
        return "R?t ti?c, chu?i d? li?u d? b?o tu?n hi?n ch?a h?p l?."

    start = min(_to_local_naive(d) for d in parsed)
    end = max(_to_local_naive(d) for d in parsed)
    coverage_h = (end - start).total_seconds() / 3600.0
    if coverage_h < 24 * 6.5:
        return "R?t ti?c, t?i hi?n ch? c? d? li?u d? b?o ng?n h?n, ch?a ?? 7 ng?y."

    min_pt = min(pts, key=lambda p: float(p.risk_score_0_100 or 0.0))
    return f"N?u nh?n trong 7 ng?y t?i theo chu?i hi?n c?, m?c s?ch nh?t ?ang r?i v?o {min_pt.time}."


# ===== Final overrides: human-first, UTF-8 safe, non-template =====
SYSTEM_PROMPT = """
BẠN LÀ TRỢ LÝ GIÁM SÁT MÔI TRƯỜNG CỦA HỆ DSS.
Ưu tiên an toàn con người, trả lời tự nhiên, không rập khuôn.

Nguyên tắc:
1) AHP = hiện trạng theo quận. AI = diễn giải xu hướng ngắn hạn. Không trộn vai trò.
2) Nếu người dùng hỏi dạng "có nên/không nên", câu đầu phải trả lời rõ "NÊN" hoặc "KHÔNG NÊN".
3) Nếu thiếu dữ liệu forecast mới hoặc lệch thời gian: nói thẳng chưa đủ cơ sở kết luận mạnh.
4) Không bịa mốc giờ cụ thể khi không có dữ liệu đủ mới.
5) Giữ tiếng Việt có dấu chuẩn UTF-8.
"""


def _finalize_reply_text(text: str) -> str:
    """Only normalize spacing and repeated lines, never strip Vietnamese accents."""
    cleaned = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not cleaned:
        return ""
    lines = [ln.rstrip() for ln in cleaned.split("\n")]
    out: List[str] = []
    last = None
    for ln in lines:
        s = ln.strip()
        if not s:
            if out and out[-1] != "":
                out.append("")
            continue
        if last and s.lower() == last.lower():
            continue
        out.append(s)
        last = s
    return "\n".join(out).strip()


def _detect_temporal_query_mode(text: str) -> Optional[str]:
    q = _normalize_plain(text)
    if not q:
        return None
    if "7 ngay" in q or "bay ngay" in q or "1 tuan" in q or "tuan toi" in q:
        return "weekly_7d"
    if "hom qua" in q and any(s in q for s in ["tang hang", "giam hang", "thay doi hang", "manh nhat", "so voi hom qua"]):
        return "rank_change_yesterday"
    if "xu huong" in q and "3 gio" in q:
        return "trend_3h"
    if "quan nao" in q and any(s in q for s in ["nguy co cao nhat", "cao nhat", "anh huong nhat"]) and any(s in q for s in ["gio toi", "6 gio", "khung gio"]):
        return "district_peak_window"
    if "khung gio" in q and any(s in q for s in ["nguy co cao nhat", "cao nhat", "nguy hiem"]):
        return "peak_window"
    return None


def _detect_chat_intent(text: str) -> str:
    q = _normalize_plain(text)
    if not q:
        return "ahp_current"

    emergency_terms = (
        "kho tho", "dau nguc", "dau phoi", "chong mat", "muon xiu", "xiu", "buon non",
        "mu mit", "toi sam", "khong thay nha", "cay mat", "ngat",
    )
    if any(t in q for t in emergency_terms):
        return "emergency_health"

    if any(t in q for t in ("forecast cu", "du lieu cu", "lech thoi gian", "forecast chua moi", "timestamp cu")) and (
        any(t in q for t in ("so sanh", "ahp va ai", "ahp voi ai", "hien trang va ngan han"))
        or ("rank" in q and "ngan han" in q)
    ):
        return "stale_forecast_compare_question"

    if any(t in q for t in (
        "hang cao nhung", "rank cao nhung", "khong thay nguy co tang", "chua thay nguy co tang",
        "khong tang them trong ngan han", "chua thay tang them",
    )):
        return "high_rank_but_no_extra_short_term_risk"

    if any(t in q for t in (
        "rank chua cao", "hang chua cao", "chua cao nhung", "chua cao ma",
        "khuyen nghi theo doi them", "theo doi bo sung", "danh sach theo doi bo sung",
        "nhom can giam sat ngan han", "watchlist",
    )):
        return "low_rank_but_short_term_watch"

    if any(t in q for t in ("scenario", "kich ban", "baseline", "policy")):
        return "scenario_compare"

    if any(t in q for t in ("ahp va ai", "ahp voi ai", "phan biet", "so sanh ahp", "hien trang va ngan han")):
        return "ahp_vs_short_term_compare"

    if any(t in q for t in ("do tin cay", "forecast co moi", "forecast moi", "du lieu cu", "lech thoi gian", "timestamp", "encoding")):
        return "data_quality"

    if any(t in q for t in ("6 gio", "24 gio", "ngan han", "canh bao som", "xu huong", "khung gio", "trong vai gio toi")):
        return "short_term_risk"

    if any(t in q for t in ("nen", "can lam gi", "khuyen nghi", "hanh dong", "co nen", "chay bo", "di bo", "dap xe", "ra ngoai")):
        return "operational_advice"

    return "ahp_current"


def _build_emergency_health_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    q = _normalize_plain(question)
    urgent = "VÀO QUÁN CAFE CÓ MÁY LẠNH NGAY LẬP TỨC! Đừng ráng chạy xe."
    if any(t in q for t in ("khong thay nha", "mu mit", "toi sam", "khac cam bien")):
        return (
            f"{urgent}\n"
            "Tôi hiểu tình huống của bạn. Cảm biến có thể sai số cục bộ hoặc cập nhật chậm.\n"
            "Hãy tin vào triệu chứng thực tế của cơ thể và tìm nơi trú ẩn ngay."
        )
    return (
        f"{urgent}\n"
        "Đeo N95/KF94 ngay, ngồi nghỉ trong không gian kín có lọc khí, uống nước ấm và tránh vận động mạnh."
    )


def _wants_numbers(question: str) -> bool:
    q = _normalize_plain(question)
    return any(k in q for k in ("tai sao", "vi sao", "so lieu", "cu the", "bao nhieu", "chi so", "score", "rank", "hang", "c1", "c2", "c3", "c4"))


def _friendly_driver_name(key: str) -> str:
    mapping = {
        "C1": "bụi mịn và mức vượt chuẩn hiện tại",
        "C2": "mức ô nhiễm kéo dài theo thời gian",
        "C3": "điểm nóng lặp lại",
        "C4": "điều kiện khí tượng bất lợi",
    }
    return mapping.get(str(key).upper(), str(key))


def _build_grounded_rank_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], intent: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
    if not ranked:
        return "Mình chưa có bảng AHP hiện tại để kết luận."
    if intent == "lowest":
        target = ranked[-1]
        preview = list(reversed(ranked[-3:]))
        head = f"Quận có mức ưu tiên ô nhiễm thấp nhất hiện tại là {target.districtName}."
    else:
        target = ranked[0]
        preview = ranked[:3]
        head = f"Quận có mức ưu tiên ô nhiễm cao nhất hiện tại là {target.districtName}."
    detail = "\n".join(f"- Hạng {r.rank}: {r.districtName} (score {r.score:.6f})" for r in preview)
    return f"{head}\n{detail}\n{_source_note(req)}"


def _build_ahp_current_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    if not rows:
        return "Mình chưa có bảng AHP hiện tại để trả lời câu này."
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
    q = _normalize_plain(question)

    if "top 3" in q:
        top3 = ranked[:3]
        ans = f"Top 3 quận cần ưu tiên theo dõi hiện tại là: {', '.join(r.districtName for r in top3)}."
        if _wants_numbers(question):
            ans += " " + "; ".join(f"{r.districtName}: hạng {r.rank}, score {r.score:.6f}" for r in top3) + "."
        return ans

    target = _extract_target_district(question, rows) or ranked[0]
    cvals = {
        "C1": float(target.C1 or 0.0),
        "C2": float(target.C2 or 0.0),
        "C3": float(target.C3 or 0.0),
        "C4": float(target.C4 or 0.0),
    }
    key = max(cvals, key=lambda k: cvals[k])
    ans = f"Nếu chỉ nhìn AHP hiện tại, bạn nên ưu tiên {target.districtName}."
    ans += f" Yếu tố chi phối chính là {_friendly_driver_name(key)}."
    if _wants_numbers(question):
        ans += f" (hạng {target.rank}, score {target.score:.6f})"
    return ans


def _build_data_quality_reply(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> str:
    quality = _format_data_quality(req, rows)
    stale_note = str(quality.get("stale_note") or "").strip()
    latest = str(quality.get("latest") or "chưa có")
    conf = str(quality.get("confidence_text") or "Trung bình")
    if stale_note:
        return f"Dữ liệu forecast hiện chưa mới (mốc gần nhất: {latest}). Vì vậy độ tin cậy đang giảm; kết quả AI chỉ nên dùng để hỗ trợ vận hành ngắn hạn."
    return f"Dữ liệu forecast hiện khá mới (mốc gần nhất: {latest}). Độ tin cậy: {conf}."


def _build_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    quality = _format_data_quality(req, rows)
    trend = _analyze_forecast_trend(req.forecast_series)
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    target = _extract_target_district(question, rows) if rows else None
    if target is None and ranked:
        target = ranked[0]
    name = target.districtName if target else "khu vực đang theo dõi"

    if not trend or quality.get("stale_note"):
        return f"Hiện chưa đủ cơ sở để kết luận mạnh nguy cơ ngắn hạn cho {name}. Forecast đang cũ/thiếu nên tạm thời bạn nên bám AHP hiện trạng để ưu tiên."

    delta = float(trend.get("delta") or 0.0)
    if delta >= 2:
        base = f"Trong vài giờ tới, nguy cơ ở {name} có xu hướng tăng."
    elif delta > -2:
        base = f"Trong vài giờ tới, nguy cơ ở {name} đang đi ngang."
    else:
        base = f"Trong vài giờ tới, nguy cơ ở {name} đang giảm."

    if delta > -2 and target and int(target.rank) <= 3:
        base += " Lưu ý: đi ngang ở đây là đi ngang ở mức ô nhiễm cao, chưa phải an toàn."
    if _wants_numbers(question):
        base += f" (hiện tại {float(trend['current']):.1f}, trung bình 3 giờ tới {float(trend['avg_next']):.1f})"
    return base


def _build_ahp_vs_short_term_compare_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top = ranked[0] if ranked else None
    trend = _analyze_forecast_trend(req.forecast_series)
    quality = _format_data_quality(req, rows)

    ahp_part = f"AHP hiện trạng cho thấy {top.districtName} đang cần ưu tiên cao nhất." if top else "AHP hiện trạng chưa đủ dữ liệu."
    if not trend or quality.get("stale_note"):
        ai_part = "AI ngắn hạn hiện chưa đủ dữ liệu mới để kết luận khác biệt trong vài giờ tới."
    else:
        ai_part = f"AI ngắn hạn cho thấy xu hướng {trend['trend']} trong vài giờ tới."
    return f"{ahp_part} {ai_part}"


def _build_high_rank_but_no_extra_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top_group = ranked[:3]
    if not top_group:
        return "Mình chưa có bảng AHP hiện trạng để xác định nhóm hạng cao."
    quality = _format_data_quality(req, rows)
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend or quality.get("stale_note"):
        names = ", ".join(r.districtName for r in top_group)
        return f"Trong nhóm hạng cao hiện tại ({names}), mình chưa đủ forecast mới để kết luận quận nào chưa tăng thêm trong ngắn hạn."
    if float(trend.get("delta") or 0.0) <= 1.5:
        names = ", ".join(r.districtName for r in top_group)
        return f"Trong nhóm hạng cao hiện tại, chưa thấy tín hiệu tăng thêm rõ ở: {names}."
    return "Nhóm hạng cao đang có dấu hiệu tăng thêm trong ngắn hạn, chưa thể khẳng định quận nào an toàn hơn hẳn."


def _build_low_rank_but_short_term_watch_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    quality = _format_data_quality(req, rows)
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend or quality.get("stale_note"):
        return "Hiện chưa đủ cơ sở để xác định rõ quận hạng chưa cao nào cần đưa vào danh sách theo dõi bổ sung."
    if float(trend.get("delta") or 0.0) < 2:
        return "Chưa phát hiện quận hạng chưa cao nào nổi bật hơn hẳn về nguy cơ ngắn hạn."
    low_pool = [r for r in ranked if int(r.rank) >= 7]
    if not low_pool:
        return "Chưa đủ dữ liệu nhóm hạng chưa cao để lập danh sách theo dõi bổ sung."
    candidates = sorted(
        low_pool,
        key=lambda r: (float(r.C4 or 0.0) + float(r.C3 or 0.0), -float(r.score or 0.0)),
        reverse=True,
    )[:3]
    names = ", ".join(f"{r.districtName} (hạng {r.rank})" for r in candidates)
    return f"Có thể đưa vào danh sách theo dõi bổ sung: {names}."


def _build_stale_forecast_compare_question_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top = ranked[0] if ranked else None
    if top:
        return (
            f"AHP hiện trạng vẫn dùng được và đang ưu tiên {top.districtName}. "
            "Nhưng forecast đang cũ/lệch thời gian nên chưa đủ cơ sở để kết luận khác biệt ngắn hạn một cách chắc chắn."
        )
    return "Forecast đang cũ/lệch thời gian nên hiện chưa đủ cơ sở để kết luận khác biệt ngắn hạn."


def _build_operational_advice_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    q = _normalize_plain(question)
    if any(t in q for t in ("kho tho", "dau nguc", "dau phoi", "chong mat", "xiu", "buon non", "mu mit", "khong thay nha", "toi sam")):
        return _build_emergency_health_reply(req, rows, question)

    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    target = _extract_target_district(question, rows) if rows else None
    if target is None and ranked:
        target = ranked[0]
    name = target.districtName if target else "khu vực hiện tại"
    trend = _analyze_forecast_trend(req.forecast_series)

    if "co nen" in q or "nen" in q:
        if target and int(target.rank) <= 3:
            return f"KHÔNG NÊN ra ngoài ở {name} lúc này. Khu vực này đang ở nhóm rủi ro cao theo hiện trạng."
        if trend and float(trend.get("delta") or 0.0) >= 2:
            return f"KHÔNG NÊN chủ quan ở {name}. Nguy cơ ngắn hạn đang tăng."
        return f"NÊN đi lại có kiểm soát ở {name}, đeo N95/KF94 và giảm thời gian ngoài trời."

    return _build_short_term_risk_reply(req, rows, question)


def _build_temporal_weekly_reply(req: AIChatRequest) -> str:
    pts = _future_forecast_points(req.forecast_series)
    if len(pts) < 24:
        return "Rất tiếc, hiện mình chỉ có dữ liệu dự báo ngắn hạn theo giờ, chưa đủ dữ liệu cho 7 ngày tới."

    parsed = [_parse_iso_time(str(p.time)) for p in pts]
    parsed = [d for d in parsed if d is not None]
    if not parsed:
        return "Rất tiếc, chuỗi dữ liệu dự báo tuần hiện chưa hợp lệ."

    start = min(_to_local_naive(d) for d in parsed)
    end = max(_to_local_naive(d) for d in parsed)
    coverage_h = (end - start).total_seconds() / 3600.0
    if coverage_h < 24 * 6.5:
        return "Rất tiếc, forecast hiện tại chưa phủ đủ 7 ngày nên chưa thể kết luận chính xác theo tuần."

    min_pt = min(pts, key=lambda p: float(p.risk_score_0_100 or 0.0))
    return f"Nếu chỉ dựa trên chuỗi forecast hiện có, mốc sạch nhất trong 7 ngày tới rơi vào {min_pt.time}."


def _normalize_chat_messages(req: AIChatRequest) -> List[Dict[str, str]]:
    msgs: List[Dict[str, str]] = [{"role": "system", "content": _build_chat_system_prompt(req)}]
    for m in req.messages or []:
        role = (m.role or "").strip().lower()
        content = (m.content or "").strip()
        if role not in {"system", "user", "assistant"}:
            continue
        if not content:
            continue
        msgs.append({"role": role, "content": content})
    return msgs


async def _chat_ollama(model: str, messages: List[Dict[str, str]], temperature: float) -> str:
    base = (os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434") or "").rstrip("/")
    url = f"{base}/api/chat"
    payload = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {"temperature": float(temperature)},
    }
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            res = await client.post(url, json=payload)
            res.raise_for_status()
            data = res.json()
        msg = (data.get("message") or {}).get("content")
        if isinstance(msg, str) and msg.strip():
            return msg.strip()
        direct = data.get("response")
        if isinstance(direct, str) and direct.strip():
            return direct.strip()
        return "Mình chưa nhận được phản hồi rõ từ mô hình AI. Bạn thử lại sau ít phút."
    except Exception:
        return "Mình chưa kết nối được mô hình AI. Bạn kiểm tra backend và dịch vụ AI rồi thử lại giúp mình."


async def _chat_openai(model: str, messages: List[Dict[str, str]], temperature: float) -> str:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return "Mình chưa có khóa OpenAI trên server nên chưa gọi được model OpenAI."
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    payload = {
        "model": model,
        "messages": messages,
        "temperature": float(temperature),
    }
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            res = await client.post(url, headers=headers, json=payload)
            res.raise_for_status()
            data = res.json()
        choices = data.get("choices") or []
        if choices and isinstance(choices[0], dict):
            content = ((choices[0].get("message") or {}).get("content") or "").strip()
            if content:
                return content
        return "Mình chưa nhận được phản hồi rõ từ OpenAI. Bạn thử lại sau."
    except Exception:
        return "Mình chưa kết nối được OpenAI trong lúc này. Bạn thử lại sau ít phút."


# ===== DSS-safe final behavior overrides =====
SYSTEM_PROMPT = """
You are the explanation and alert layer for an air-quality decision support system.

Scope:
- Evaluate current district-level air-quality condition and short-term risk warning.
- This is NOT a medical diagnosis system.
- Do NOT provide diagnosis, emergency orders, or guaranteed sensor truth.
- Do NOT claim 7-day forecast unless that forecast range truly exists.

Rules:
1) Always separate:
   - current observed condition
   - short-term risk trend
   - unsupported long-range forecast
2) Avoid absolute certainty.
3) Be honest when data is limited.
4) Reference basis in plain language:
   - latest environmental readings/signals
   - recent trend
   - duration of elevated pollution
   - optional weather-related support factors
5) If user asks beyond capability, answer narrowly and honestly.
6) Keep tone calm, supportive, and decision-support only.
"""


def _compose_dss_response(assessment: str, basis: str, limitation: str, precaution: str) -> str:
    return (
        f"Assessment: {assessment}\n"
        f"Basis: {basis}\n"
        f"Limitation: {limitation}\n"
        f"Suggested precaution: {precaution}"
    )


def _build_plain_basis(target: Optional[AIChatDistrictRow], trend: Optional[Dict[str, object]]) -> str:
    if target is None:
        rank_part = "Bảng AHP hiện tại chưa có quận mục tiêu cụ thể."
        c_part = "Chưa đủ C1-C4 để chỉ ra yếu tố chi phối."
    else:
        cvals = {
            "C1": float(target.C1 or 0.0),
            "C2": float(target.C2 or 0.0),
            "C3": float(target.C3 or 0.0),
            "C4": float(target.C4 or 0.0),
        }
        key = max(cvals, key=lambda k: cvals[k])
        rank_part = f"AHP hiện trạng: {target.districtName} đang ở hạng {target.rank} (score {target.score:.6f})."
        c_part = f"Yếu tố chi phối chính: {_friendly_driver_name(key)}; mức duy trì ô nhiễm (C2)={cvals['C2']:.4f}; hỗ trợ khí tượng (C4)={cvals['C4']:.4f}."
    if trend is None:
        trend_part = "Xu hướng gần: chưa có chuỗi forecast đủ mới để kết luận rõ."
    else:
        trend_part = (
            f"Xu hướng gần: {trend.get('trend')} (hiện tại {float(trend.get('current') or 0):.1f}, "
            f"trung bình 3 giờ tới {float(trend.get('avg_next') or 0):.1f})."
        )
    return f"{rank_part} {c_part} {trend_part}"


def _build_limit_text(req: AIChatRequest, rows: List[AIChatDistrictRow], require_forecast: bool = False) -> str:
    quality = _format_data_quality(req, rows)
    stale_note = str(quality.get("stale_note") or "").strip()
    if require_forecast and (not (req.forecast_series or []) or stale_note):
        return (
            "Hệ thống hiện chỉ hỗ trợ cảnh báo ngắn hạn; forecast đang thiếu/cũ nên chưa đủ cơ sở cho kết luận mạnh. "
            "Không hỗ trợ dự báo dài hạn trong điều kiện dữ liệu này."
        )
    return (
        "Kết luận này phục vụ hỗ trợ quyết định vận hành. "
        "Hệ thống không thay thế chẩn đoán y khoa và không bảo đảm tuyệt đối mọi cảm biến ngoài thực địa."
    )


def _build_emergency_health_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    target = ranked[0] if ranked else None
    trend = _analyze_forecast_trend(req.forecast_series)
    return _compose_dss_response(
        assessment="Bạn đang mô tả tình huống khó chịu sức khỏe; nên ưu tiên tạm dừng di chuyển và giảm phơi nhiễm ngay lúc này.",
        basis=_build_plain_basis(target, trend),
        limitation=(
            "Đây không phải hệ thống y khoa, nên mình không thể chẩn đoán hoặc đưa chỉ định điều trị. "
            + _build_limit_text(req, rows, require_forecast=True)
        ),
        precaution=(
            "Di chuyển vào nơi không khí sạch hơn, hạn chế hoạt động gắng sức, đeo khẩu trang lọc bụi khi phải ra ngoài. "
            "Nếu triệu chứng kéo dài hoặc nặng hơn, hãy liên hệ cơ sở y tế địa phương."
        ),
    )


def _build_ahp_current_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    if not rows:
        return _compose_dss_response(
            assessment="Chưa thể đánh giá hiện trạng theo quận vì chưa có bảng AHP hiện tại.",
            basis="Hệ thống cần bảng xếp hạng AHP và các tiêu chí C1-C4 của ngày đang xem.",
            limitation="Chỉ khi có AHP hiện trạng mới xác định được quận ưu tiên. Không suy luận dài hạn từ dữ liệu thiếu.",
            precaution="Nạp dữ liệu ngày cần phân tích rồi chạy lại AHP bước hiện trạng trước.",
        )
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score))
    target = _extract_target_district(question, rows) or ranked[0]
    trend = _analyze_forecast_trend(req.forecast_series)
    assessment = f"Hiện trạng quan trắc cho thấy {target.districtName} là khu vực cần ưu tiên theo dõi ở thời điểm hiện tại."
    if "top 3" in _normalize_plain(question):
        top3 = ", ".join(f"{r.districtName} (hạng {r.rank})" for r in ranked[:3])
        assessment = f"Nhóm ưu tiên hiện trạng cao nhất hiện tại gồm: {top3}."
    return _compose_dss_response(
        assessment=assessment,
        basis=_build_plain_basis(target, trend=None),
        limitation="Phần này phản ánh hiện trạng theo AHP, không phải dự báo xu hướng nhiều ngày.",
        precaution="Ưu tiên giám sát thực địa tại quận hạng cao và cập nhật dữ liệu theo chu kỳ giờ/ngày.",
    )


def _build_data_quality_reply(req: AIChatRequest, rows: List[AIChatDistrictRow]) -> str:
    quality = _format_data_quality(req, rows)
    latest = str(quality.get("latest") or "chưa có")
    stale_note = str(quality.get("stale_note") or "").strip()
    conf = str(quality.get("confidence_text") or "Trung bình")
    assessment = (
        f"Độ tin cậy hiện tại ở mức {conf}."
        if not stale_note
        else "Độ tin cậy hiện tại đang giảm do dữ liệu forecast chưa mới."
    )
    basis = f"Mốc forecast gần nhất: {latest}. Chỉ số tin cậy tổng hợp: {float(quality.get('score') or 0):.0f}/100."
    limitation = (
        stale_note
        if stale_note
        else "Dù dữ liệu khá mới, kết quả vẫn là hỗ trợ quyết định ngắn hạn, không phải khẳng định tuyệt đối."
    )
    return _compose_dss_response(
        assessment=assessment,
        basis=basis,
        limitation=limitation,
        precaution="Khi độ tin cậy giảm, ưu tiên quyết định theo AHP hiện trạng và cập nhật forecast mới trước khi kết luận xu hướng.",
    )


def _build_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    target = _extract_target_district(question, rows) if rows else None
    if target is None and ranked:
        target = ranked[0]
    trend = _analyze_forecast_trend(req.forecast_series)
    if not trend or _format_data_quality(req, rows).get("stale_note"):
        return _compose_dss_response(
            assessment=(
                f"Chưa đủ cơ sở để kết luận mạnh nguy cơ ngắn hạn cho {target.districtName if target else 'khu vực đang theo dõi'}."
            ),
            basis=_build_plain_basis(target, trend=None),
            limitation=_build_limit_text(req, rows, require_forecast=True),
            precaution="Tạm thời bám AHP hiện trạng để ưu tiên quận rủi ro cao và cập nhật forecast mới trước khi ra quyết định ngắn hạn.",
        )
    delta = float(trend.get("delta") or 0.0)
    if delta >= 2:
        assessment = f"Nguy cơ ngắn hạn có xu hướng tăng tại {target.districtName if target else 'khu vực đang theo dõi'}."
    elif delta > -2:
        assessment = f"Nguy cơ ngắn hạn đang đi ngang tại {target.districtName if target else 'khu vực đang theo dõi'}."
    else:
        assessment = f"Nguy cơ ngắn hạn có xu hướng giảm tại {target.districtName if target else 'khu vực đang theo dõi'}."
    return _compose_dss_response(
        assessment=assessment,
        basis=_build_plain_basis(target, trend),
        limitation=_build_limit_text(req, rows, require_forecast=True),
        precaution="Giảm phơi nhiễm ngoài trời trong giờ cao điểm, theo dõi cập nhật dữ liệu giờ tiếp theo để điều chỉnh kế hoạch.",
    )


def _build_ahp_vs_short_term_compare_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top = ranked[0] if ranked else None
    trend = _analyze_forecast_trend(req.forecast_series)
    quality = _format_data_quality(req, rows)
    ahp_text = f"AHP hiện trạng đang ưu tiên {top.districtName}." if top else "AHP hiện trạng chưa đủ dữ liệu."
    if not trend or quality.get("stale_note"):
        ai_text = "AI ngắn hạn chưa đủ forecast mới để kết luận khác biệt xu hướng."
    else:
        ai_text = f"AI ngắn hạn cho thấy xu hướng {trend.get('trend')} trong vài giờ tới."
    return _compose_dss_response(
        assessment=f"{ahp_text} {ai_text}",
        basis=_build_plain_basis(top, trend),
        limitation=_build_limit_text(req, rows, require_forecast=True),
        precaution="Dùng AHP để ưu tiên hiện trạng theo quận; dùng AI để hỗ trợ điều phối ngắn hạn theo giờ.",
    )


def _build_high_rank_but_no_extra_short_term_risk_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top_group = ranked[:3]
    trend = _analyze_forecast_trend(req.forecast_series)
    quality = _format_data_quality(req, rows)
    if not top_group:
        return _compose_dss_response(
            assessment="Chưa có nhóm hạng cao hiện trạng để so sánh.",
            basis="Thiếu bảng AHP hiện tại.",
            limitation="Không thể tách nhóm hạng cao khi thiếu dữ liệu hiện trạng.",
            precaution="Chạy AHP hiện trạng trước khi hỏi so sánh nhóm hạng cao.",
        )
    names = ", ".join(r.districtName for r in top_group)
    if not trend or quality.get("stale_note"):
        return _compose_dss_response(
            assessment=f"Trong nhóm hạng cao hiện tại ({names}), chưa đủ cơ sở để khẳng định quận nào không tăng thêm trong ngắn hạn.",
            basis="Có AHP hiện trạng nhưng forecast ngắn hạn chưa đủ mới.",
            limitation=_build_limit_text(req, rows, require_forecast=True),
            precaution="Tạm thời ưu tiên theo thứ hạng AHP hiện tại và đợi forecast mới để tách rủi ro ngắn hạn.",
        )
    return _compose_dss_response(
        assessment=f"Trong nhóm hạng cao hiện tại ({names}), chưa thấy tín hiệu tăng thêm rõ trong ngắn hạn.",
        basis=_build_plain_basis(top_group[0], trend),
        limitation=_build_limit_text(req, rows, require_forecast=True),
        precaution="Duy trì giám sát chặt các quận hạng cao, đặc biệt trong khung giờ giao thông cao điểm.",
    )


def _build_low_rank_but_short_term_watch_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    trend = _analyze_forecast_trend(req.forecast_series)
    quality = _format_data_quality(req, rows)
    if not trend or quality.get("stale_note"):
        return _compose_dss_response(
            assessment="Chưa đủ cơ sở để xác định quận hạng chưa cao nào cần theo dõi bổ sung trong ngắn hạn.",
            basis="AHP hiện trạng có sẵn nhưng forecast ngắn hạn chưa đủ mới để phát hiện tín hiệu tăng bất thường.",
            limitation=_build_limit_text(req, rows, require_forecast=True),
            precaution="Tạm thời theo dõi theo thứ hạng AHP hiện tại; cập nhật forecast mới để phát hiện danh sách theo dõi bổ sung.",
        )
    if float(trend.get("delta") or 0.0) < 2:
        return _compose_dss_response(
            assessment="Chưa phát hiện quận hạng chưa cao nào nổi bật hơn hẳn về nguy cơ ngắn hạn.",
            basis=_build_plain_basis(ranked[0] if ranked else None, trend),
            limitation=_build_limit_text(req, rows, require_forecast=True),
            precaution="Giữ giám sát nền theo AHP; rà soát lại khi có bản cập nhật forecast tiếp theo.",
        )
    low_pool = [r for r in ranked if int(r.rank) >= 7]
    candidates = sorted(
        low_pool,
        key=lambda r: (float(r.C4 or 0.0) + float(r.C3 or 0.0), -float(r.score or 0.0)),
        reverse=True,
    )[:3]
    names = ", ".join(f"{r.districtName} (hạng {r.rank})" for r in candidates) if candidates else "chưa xác định"
    return _compose_dss_response(
        assessment=f"Có thể theo dõi bổ sung ngắn hạn ở: {names}.",
        basis=_build_plain_basis(candidates[0] if candidates else (ranked[0] if ranked else None), trend),
        limitation=_build_limit_text(req, rows, require_forecast=True),
        precaution="Bổ sung giám sát theo giờ tại các quận trên, đồng thời đối chiếu liên tục với thứ hạng AHP hiện trạng.",
    )


def _build_stale_forecast_compare_question_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    top = ranked[0] if ranked else None
    assessment = (
        f"AHP hiện trạng vẫn cho thấy {top.districtName} là quận ưu tiên cao."
        if top
        else "AHP hiện trạng chưa đủ dữ liệu quận mục tiêu."
    )
    return _compose_dss_response(
        assessment=assessment + " Tuy vậy chưa thể kết luận chắc phần khác biệt ngắn hạn do forecast đang cũ/lệch thời gian.",
        basis=_build_plain_basis(top, trend=None),
        limitation=_build_limit_text(req, rows, require_forecast=True),
        precaution="Ưu tiên quyết định theo AHP hiện trạng trước; cập nhật forecast mới rồi mới so sánh phần ngắn hạn.",
    )


def _build_operational_advice_reply(req: AIChatRequest, rows: List[AIChatDistrictRow], question: str) -> str:
    q = _normalize_plain(question)
    if any(t in q for t in ("kho tho", "dau nguc", "dau phoi", "chong mat", "xiu", "buon non", "mu mit", "khong thay nha", "toi sam")):
        return _build_emergency_health_reply(req, rows, question)

    ranked = sorted(rows, key=lambda r: (r.rank, -r.score)) if rows else []
    target = _extract_target_district(question, rows) if rows else None
    if target is None and ranked:
        target = ranked[0]
    trend = _analyze_forecast_trend(req.forecast_series)
    quality = _format_data_quality(req, rows)

    if "co nen" in q or "nen" in q:
        if target and int(target.rank) <= 3:
            assessment = f"Khuyến nghị hiện tại: KHÔNG NÊN ưu tiên hoạt động ngoài trời tại {target.districtName}."
        elif trend and float(trend.get("delta") or 0.0) >= 2:
            assessment = f"Khuyến nghị hiện tại: NÊN giảm hoạt động ngoài trời vì nguy cơ ngắn hạn đang tăng."
        else:
            assessment = f"Khuyến nghị hiện tại: CÓ THỂ hoạt động ngoài trời ở mức kiểm soát, và nên giảm thời gian phơi nhiễm."
    else:
        return _build_short_term_risk_reply(req, rows, question)

    return _compose_dss_response(
        assessment=assessment,
        basis=_build_plain_basis(target, trend),
        limitation=_build_limit_text(req, rows, require_forecast=True),
        precaution="Ưu tiên khẩu trang lọc bụi khi di chuyển, tránh giờ cao điểm giao thông, và theo dõi cập nhật kế tiếp.",
    )


def _build_temporal_weekly_reply(req: AIChatRequest) -> str:
    pts = _future_forecast_points(req.forecast_series)
    if len(pts) < 24:
        return _compose_dss_response(
            assessment="Chưa thể cung cấp đánh giá 7 ngày tới.",
            basis="Chuỗi forecast hiện có mới ở phạm vi ngắn hạn theo giờ.",
            limitation="Hệ thống hiện chỉ hỗ trợ cảnh báo ngắn hạn trong điều kiện dữ liệu này.",
            precaution="Bạn có thể dùng đánh giá hiện trạng AHP + cảnh báo ngắn hạn, và cập nhật thêm forecast khi có dữ liệu tuần.",
        )
    parsed = [_parse_iso_time(str(p.time)) for p in pts]
    parsed = [d for d in parsed if d is not None]
    if not parsed:
        return _compose_dss_response(
            assessment="Chưa thể kết luận cho 7 ngày tới.",
            basis="Chuỗi thời gian forecast chưa hợp lệ.",
            limitation="Không đủ dữ liệu hợp lệ cho dự báo dài hơn ngắn hạn.",
            precaution="Kiểm tra lại nguồn forecast và đồng bộ lại dữ liệu trước khi hỏi theo tuần.",
        )
    start = min(_to_local_naive(d) for d in parsed)
    end = max(_to_local_naive(d) for d in parsed)
    coverage_h = (end - start).total_seconds() / 3600.0
    if coverage_h < 24 * 6.5:
        return _compose_dss_response(
            assessment="Chưa thể kết luận mốc sạch nhất cho 7 ngày tới.",
            basis=f"Forecast hiện chỉ phủ khoảng {coverage_h:.0f} giờ, chưa đủ một tuần.",
            limitation="Hệ thống không suy đoán 7 ngày khi phạm vi forecast chưa đủ.",
            precaution="Tạm thời dùng cảnh báo ngắn hạn theo giờ; cập nhật forecast dài hơn để đánh giá theo tuần.",
        )
    min_pt = min(pts, key=lambda p: float(p.risk_score_0_100 or 0.0))
    return _compose_dss_response(
        assessment=f"Trong phạm vi forecast hiện có, mốc rủi ro thấp nhất dự kiến rơi vào {min_pt.time}.",
        basis="Kết quả dựa trên chuỗi forecast nhiều ngày đã có và điểm rủi ro theo giờ.",
        limitation="Đây là hỗ trợ quyết định theo dữ liệu hiện có, không phải bảo đảm tuyệt đối điều kiện thực địa.",
        precaution="Vẫn nên theo dõi cập nhật mới nhất trước khi ra quyết định vận hành theo tuần.",
    )
