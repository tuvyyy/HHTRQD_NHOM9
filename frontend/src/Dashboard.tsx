import { Suspense, lazy, useEffect, useMemo, useRef, useState } from "react";
import {
  getAlerts,
  getAirQuality,
  gridScore,
  runDss,
  scoreRisk,
  type EarlyWarningRes,
  aiForecast,
  type AiForecastRes,
  getStationsBounds,
  scoreRiskStation,
  geocodeReverse,
  getDistrictDaily,
  refreshDistrictDaily,
  getDistrictDailyCoverage,
  backfillDistrictDaily,
  type DistrictDailyRow,
  getDistrictCriteria,
  refreshDistrictCriteria,
  getDistrictAHPScore,
  ahpWeights,
  type DistrictCriteriaResponse,
  type DistrictAHPScoredRow,
  type AhpWeightsResult,
  getDistrictCriteriaCoverage,
  backfillDistrictCriteria,
  type DistrictCoverageItem,
  type OpenAQStation,
  runDistrictPolicyScenario,
  type DistrictPolicyScenarioResponse,
  type ScenarioPresetName,
  type ScenarioThresholds,
  type ScenarioWeights,
} from "./api";
import HourlyChart from "./HourlyChart";
import "./dashboard.css";
import "./leafletIconFix";
import MapPicker, { type DistrictMapResultItem, type MapSourceStatus } from "./MapPicker";
import EarlyWarningCard from "./EarlyWarningCard";
import AddressSearch from "./AddressSearch";
import AiChatPanel from "./AiChatPanel";
import RiskScoreChart from "./RiskScoreChart";
import StationForecastPage from "./StationForecastPage";
import {
  Bar,
  BarChart,
  Cell,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from "recharts";

const VenusLanding = lazy(() => import("./VenusLanding"));

type Weights = Record<string, number>;
type CriteriaStepIndex = 1 | 2 | 3 | 4;
const ALERTS_HISTORY_LIMIT = 30;
const INTRO_SEEN_KEY = "airdss_intro_seen_v2";
const CRITERIA_TOUR_SEEN_KEY = "airdss_criteria_tour_seen_v1";
type ViewMode = "dashboard" | "map" | "history" | "ahp" | "ai";
type SourceStatusMap = Partial<Record<MapSourceStatus["source"], MapSourceStatus>>;
type TopPageMode = "home" | "news" | "map" | "criteria" | "system";
const AHP_LABELS = ["C1", "C2", "C3", "C4"] as const;
type AhpCriterionKey = (typeof AHP_LABELS)[number];
const AHP_MATRIX_DEFAULT: number[][] = [
  [1, 3, 5, 7],
  [1 / 3, 1, 3, 5],
  [1 / 5, 1 / 3, 1, 3],
  [1 / 7, 1 / 5, 1 / 3, 1],
];
const SAATY_MIN = 1;
const SAATY_ALT_MIN = 1 / 9;
const SAATY_MAX = 9;
const AHP_RI_TABLE: Record<number, number> = {
  1: 0.0,
  2: 0.0,
  3: 0.58,
  4: 0.9,
  5: 1.12,
  6: 1.24,
  7: 1.32,
  8: 1.41,
  9: 1.45,
  10: 1.49,
};
const CRITERIA_META: Array<{ key: (typeof AHP_LABELS)[number]; title: string; desc: string }> = [
  { key: "C1", title: "Mức độ vượt chuẩn", desc: "Đánh giá mức vượt ngưỡng ô nhiễm trong ngày." },
  { key: "C2", title: "Thời gian duy trì ô nhiễm cao", desc: "Tổng thời gian nồng độ ở mức bất lợi." },
  { key: "C3", title: "Tần suất vượt ngưỡng", desc: "Mức độ lặp lại các giờ vượt ngưỡng cho phép." },
  { key: "C4", title: "Điều kiện khí tượng bất lợi", desc: "Điểm bất lợi từ điều kiện khuếch tán khí thải." },
];

const SAATY_PRIORITY_GUIDE: Array<{ value: string; label: string }> = [
  { value: "1/9", label: "Vô cùng ít quan trọng" },
  { value: "1/8", label: "Giữa 1/9 và 1/7" },
  { value: "1/7", label: "Rất ít quan trọng" },
  { value: "1/6", label: "Giữa 1/7 và 1/5" },
  { value: "1/5", label: "Ít quan trọng nhiều hơn" },
  { value: "1/4", label: "Giữa 1/5 và 1/3" },
  { value: "1/3", label: "Ít quan trọng hơn" },
  { value: "1/2", label: "Giữa 1/3 và 1" },
  { value: "1", label: "Quan trọng như nhau" },
  { value: "2", label: "Giữa 1 và 3" },
  { value: "3", label: "Quan trọng hơn" },
  { value: "4", label: "Giữa 3 và 5" },
  { value: "5", label: "Quan trọng nhiều hơn" },
  { value: "6", label: "Giữa 5 và 7" },
  { value: "7", label: "Rất quan trọng hơn" },
  { value: "8", label: "Giữa 7 và 9" },
  { value: "9", label: "Vô cùng quan trọng hơn" },
];
const SAATY_RATIO_OPTIONS: Array<{ label: string; value: number }> = [
  { label: "1/9", value: 1 / 9 },
  { label: "1/8", value: 1 / 8 },
  { label: "1/7", value: 1 / 7 },
  { label: "1/6", value: 1 / 6 },
  { label: "1/5", value: 1 / 5 },
  { label: "1/4", value: 1 / 4 },
  { label: "1/3", value: 1 / 3 },
  { label: "1/2", value: 1 / 2 },
  { label: "1", value: 1 },
  { label: "2", value: 2 },
  { label: "3", value: 3 },
  { label: "4", value: 4 },
  { label: "5", value: 5 },
  { label: "6", value: 6 },
  { label: "7", value: 7 },
  { label: "8", value: 8 },
  { label: "9", value: 9 },
];
const CRITERIA_STEP_OPTIONS: Array<{ step: CriteriaStepIndex; title: string; hint: string }> = [
  { step: 1, title: "Bước 1: Nhập ma trận tiêu chí (C1-C4)", hint: "Nhập ma trận C1-C4 rồi bấm \"Tính toán bước 2\"." },
  { step: 2, title: "Bước 2: Kết quả ma trận tiêu chí", hint: "Kiểm tra CR/CI, nếu đạt thì bấm \"Tiếp tục so sánh phương án\"." },
  { step: 3, title: "Bước 3: Chuẩn bị dữ liệu phương án", hint: "Bấm \"Nạp ma trận từ C1-C4\" rồi \"Tính ma trận phương án\"." },
  { step: 4, title: "Bước 4: Kết quả tổng hợp phương án", hint: "Xem kết quả tổng hợp, biểu đồ và bảng xếp hạng 13 quận." },
];

const POLICY_SCENARIO_PRESETS: Array<{ id: ScenarioPresetName; label: string; weights: ScenarioWeights }> = [
  { id: "balanced", label: "Cân bằng", weights: { C1: 0.25, C2: 0.25, C3: 0.25, C4: 0.25 } },
  { id: "severe_now", label: "Nghiêm trọng hiện tại", weights: { C1: 0.4, C2: 0.3, C3: 0.15, C4: 0.15 } },
  { id: "persistent", label: "Kéo dài/lặp lại", weights: { C1: 0.2, C2: 0.4, C3: 0.25, C4: 0.15 } },
  { id: "early_warning", label: "Cảnh báo sớm", weights: { C1: 0.25, C2: 0.2, C3: 0.15, C4: 0.4 } },
];
const POLICY_SCENARIO_DEFAULT_THRESHOLDS: ScenarioThresholds = {
  yellow: 0.45,
  orange: 0.65,
  red: 0.8,
};
const POLICY_SCENARIO_PRESET_DESCRIPTIONS: Record<string, string> = {
  balanced: "Cân bằng: cân bằng 4 tiêu chí.",
  severe_now: "Nghiêm trọng hiện tại: ưu tiên khu đang ô nhiễm nặng hiện tại.",
  persistent: "Kéo dài/lặp lại: ưu tiên khu ô nhiễm kéo dài hoặc lặp lại.",
  early_warning: "Cảnh báo sớm: ưu tiên cảnh báo sớm theo khí tượng và xu hướng tăng.",
  prolonged_pollution: "Kéo dài/lặp lại: ưu tiên khu ô nhiễm kéo dài hoặc lặp lại.",
};
const POLICY_SCENARIO_PRESET_PRIORITY_HINT: Record<string, string> = {
  balanced: "cân bằng 4 tiêu chí (C1-C4)",
  severe_now: "mức độ nghiêm trọng hiện tại (C1)",
  persistent: "ô nhiễm kéo dài và lặp lại (C2-C3)",
  early_warning: "khí tượng bất lợi và xu hướng tăng (C4)",
  prolonged_pollution: "ô nhiễm kéo dài và lặp lại (C2-C3)",
};
const POLICY_SCENARIO_CRITERION_HINT: Record<string, string> = {
  C1: "mức độ nghiêm trọng hiện tại (C1)",
  C2: "ô nhiễm kéo dài theo thời gian (C2)",
  C3: "điểm nóng lặp lại (C3)",
  C4: "khí tượng và cảnh báo sớm (C4)",
};

const STEP_COLORS = ["#22c55e", "#f59e0b", "#f97316", "#ef4444"] as const;
type CriteriaInputRow = {
  DistrictId: number;
  DistrictName: string;
  C1: number;
  C2: number;
  C3: number;
  C4: number;
};

function buildAltMatricesFromRows(rows: CriteriaInputRow[]) {
  const buildFor = (criterion: AhpCriterionKey) => {
    const n = rows.length;
    const m = buildSaatyIdentity(n);
    if (n < 2) return m;

    const values = rows.map((r) => {
      const v = Number(r[criterion]);
      return Number.isFinite(v) ? v : 0;
    });
    const pairs: Array<{ i: number; j: number; diff: number }> = [];
    for (let i = 0; i < n; i += 1) {
      for (let j = i + 1; j < n; j += 1) {
        const vi = values[i];
        const vj = values[j];
        const denom = Math.max(Math.abs(vi), Math.abs(vj), 1e-9);
        const diff = Math.abs(vi - vj) / denom;
        pairs.push({ i, j, diff });
      }
    }

    const maxDiff = pairs.reduce((mx, p) => Math.max(mx, p.diff), 0);
    if (maxDiff <= 1e-9) return m;

    const sortedDiffs = pairs.map((p) => p.diff).sort((a, b) => a - b);
    const diffCount = sortedDiffs.length;
    const mapDiffToSaatyMagnitude = (diff: number) => {
      if (diff <= 1e-9 || diffCount === 0) return 1;
      if (diffCount === 1) return 9;
      let idx = 0;
      while (idx + 1 < diffCount && sortedDiffs[idx + 1] <= diff + 1e-12) idx += 1;
      const q = idx / (diffCount - 1);
      return Math.min(SAATY_MAX, Math.max(1, 1 + Math.round(q * 8)));
    };

    for (const p of pairs) {
      const magnitude = mapDiffToSaatyMagnitude(p.diff);
      const ratio = values[p.i] >= values[p.j] ? magnitude : 1 / magnitude;
      m[p.i][p.j] = Number(ratio.toFixed(10));
      m[p.j][p.i] = Number((1 / ratio).toFixed(10));
    }

    // Ổn định ma trận tự sinh: nếu CR còn cao thì giảm dần độ chênh về phía 1
    // để tránh phát sinh lỗi C ngẫu nhiên khi người dùng chỉ dùng dữ liệu hệ thống.
    if (n > 2) {
      let cr = computeAhpCr(m);
      let guard = 0;
      while (cr >= 0.1 && guard < 8) {
        for (const p of pairs) {
          const ratio = Number(m[p.i][p.j] || 1);
          const isDirect = ratio >= 1;
          const mag = Math.max(1, Math.round(isDirect ? ratio : 1 / ratio));
          const nextMag = Math.max(1, mag - 1);
          const nextRatio = isDirect ? nextMag : 1 / nextMag;
          m[p.i][p.j] = Number(nextRatio.toFixed(10));
          m[p.j][p.i] = Number((1 / nextRatio).toFixed(10));
        }
        cr = computeAhpCr(m);
        guard += 1;
      }
    }
    return m;
  };
  return {
    C1: buildFor("C1"),
    C2: buildFor("C2"),
    C3: buildFor("C3"),
    C4: buildFor("C4"),
  } as Record<AhpCriterionKey, number[][]>;
}

function levelStyle(level: string) {
  const lv = (level || "").toLowerCase();
  if (lv.includes("xanh")) return { color: "#065f46", bg: "#d1fae5" };
  if (lv.includes("vàng") || lv.includes("vang")) return { color: "#92400e", bg: "#fef3c7" };
  if (lv.includes("cam")) return { color: "#9a3412", bg: "#ffedd5" };
  if (lv.includes("đỏ") || lv.includes("do")) return { color: "#991b1b", bg: "#fee2e2" };
  return { color: "#374151", bg: "#f3f4f6" };
}

type BBox = { minLat: number; minLon: number; maxLat: number; maxLon: number };

function bboxAround(lat: number, lon: number, km: number): BBox {
  const dLat = km / 111;
  const dLon = km / (111 * Math.cos((lat * Math.PI) / 180));
  return {
    minLat: lat - dLat,
    minLon: lon - dLon,
    maxLat: lat + dLat,
    maxLon: lon + dLon,
  };
}

function todayYmdLocal() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function monthStartYmdLocal() {
  const d = new Date();
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  return `${yyyy}-${mm}-01`;
}


function cloneMatrix4(m: number[][]) {
  return m.map((r) => r.slice());
}

function formatSaatyFraction(value: number) {
  if (!Number.isFinite(value)) return "1";
  if (value >= 1) return `${Math.round(value)}`;
  const inv = Math.max(1, Math.round(1 / value));
  return `1/${inv}`;
}

function clampSaatyMagnitude(v: number) {
  return Math.min(SAATY_MAX, Math.max(SAATY_MIN, Math.round(v)));
}

function clampSaatyAltValue(v: number) {
  if (!Number.isFinite(v)) return 1;
  const clipped = Math.min(SAATY_MAX, Math.max(SAATY_ALT_MIN, v));
  return Number(clipped.toFixed(6));
}

function nearestSaatyRatioValue(v: number) {
  if (!Number.isFinite(v)) return 1;
  let best = SAATY_RATIO_OPTIONS[0].value;
  let bestDiff = Math.abs(v - best);
  for (const opt of SAATY_RATIO_OPTIONS) {
    const diff = Math.abs(v - opt.value);
    if (diff < bestDiff) {
      best = opt.value;
      bestDiff = diff;
    }
  }
  return best;
}

function computeAhpCr(matrix: number[][]) {
  const n = matrix.length;
  if (n < 2) return 0;
  const colSums = Array.from({ length: n }, (_, j) =>
    matrix.reduce((sum, row) => sum + Number(row[j] || 0), 0)
  );
  const norm = matrix.map((row) =>
    row.map((v, j) => {
      const s = colSums[j];
      return s ? Number(v || 0) / s : 0;
    })
  );
  const w = norm.map((row) => row.reduce((a, b) => a + b, 0) / n);
  const aw = matrix.map((row) => row.reduce((sum, v, j) => sum + Number(v || 0) * Number(w[j] || 0), 0));
  const lambdaI = aw.map((v, i) => {
    const wi = Number(w[i] || 0);
    return wi === 0 ? 0 : v / wi;
  });
  const lambdaMax = lambdaI.reduce((a, b) => a + b, 0) / n;
  const ci = n > 1 ? (lambdaMax - n) / (n - 1) : 0;
  const ri = AHP_RI_TABLE[n] ?? 1.49;
  return ri ? ci / ri : 0;
}

function buildSaatyIdentity(n: number) {
  const m = Array.from({ length: n }, () => Array.from({ length: n }, () => 1));
  for (let i = 0; i < n; i += 1) m[i][i] = 1;
  return m;
}

function toFixedOrDash(v: number | null | undefined, digits = 6) {
  return Number.isFinite(Number(v)) ? Number(v).toFixed(digits) : "-";
}

function formatRankShiftForHuman(deltaRank: number) {
  const d = Number(deltaRank || 0);
  if (d < 0) return `tăng ưu tiên ${Math.abs(d)} hạng`;
  if (d > 0) return `giảm ưu tiên ${d} hạng`;
  return "không đổi hạng";
}

function scenarioPriorityLabelByRank(rank: number, topN: number) {
  const safeRank = Number(rank || 0);
  const safeTopN = Math.max(1, Number(topN || 5));
  if (!safeRank || safeRank <= 0) return "Thấp";
  if (safeRank === 1) return "Rất cao";
  if (safeRank <= safeTopN) return "Cao";
  if (safeRank <= safeTopN + 3) return "Trung bình";
  return "Thấp";
}

function normalizeDistrictKey(value: string) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function compactScenarioRecommendation(
  level: string,
  recommendation: string,
  earlyWarning: boolean,
  scenarioRank: number,
  topN: number,
  rankDelta: number
) {
  const lv = (level || "").toLowerCase();
  const inTop = Number(scenarioRank || 0) > 0 && Number(scenarioRank || 0) <= Math.max(1, Number(topN || 5));
  const movedUpStrong = Number(rankDelta || 0) <= -3;
  const highAlert = lv.includes("đỏ") || lv.includes("do") || lv.includes("cam");

  if (highAlert && inTop) return "Ưu tiên kiểm tra hiện trường";
  if ((lv.includes("xanh") || lv.includes("xanh lá")) && inTop) return "Đưa vào danh sách theo dõi ưu tiên";
  if (movedUpStrong) return "Ưu tiên xem trước trong kịch bản này";
  if (earlyWarning && inTop) return "Đưa vào danh sách theo dõi ưu tiên";
  if (earlyWarning) return "Cảnh báo sớm";
  if (lv.includes("cam")) return "Theo dõi tăng cường";
  if (lv.includes("vàng") || lv.includes("vang")) return "Theo dõi tăng cường";
  if (recommendation.toLowerCase().includes("giám sát")) return "Giám sát định kỳ";
  if (scenarioPriorityLabelByRank(scenarioRank, topN) === "Thấp") return "Giám sát định kỳ";
  return "Giám sát định kỳ";
}

function buildScenarioReasonHint(
  presetName: string,
  criteriaValues: { C1?: number; C2?: number; C3?: number; C4?: number } | null | undefined,
  rankDelta: number,
  scenarioRank: number,
  topN: number
) {
  const c1 = Number(criteriaValues?.C1 || 0);
  const c2 = Number(criteriaValues?.C2 || 0);
  const c3 = Number(criteriaValues?.C3 || 0);
  const c4 = Number(criteriaValues?.C4 || 0);
  const movedUp = Number(rankDelta || 0) < 0;
  const movedDown = Number(rankDelta || 0) > 0;
  const inTop = Number(scenarioRank || 0) > 0 && Number(scenarioRank || 0) <= Math.max(1, Number(topN || 5));

  if (presetName === "persistent" || presetName === "prolonged_pollution") {
    if (c2 >= 60 || c3 >= 60) return "Ưu tiên kéo dài/lặp lại nên quận có C2-C3 cao tăng hạng";
  }
  if (presetName === "early_warning") {
    if (c4 >= 60) return "Ưu tiên khí tượng/dự báo nên quận có C4 cao được đẩy lên";
  }
  if (presetName === "severe_now") {
    if (c1 >= 60) return "Ưu tiên hiện trạng nghiêm trọng nên quận có C1 cao tăng hạng";
  }
  if (movedDown && !inTop) return "Điểm giảm tương đối nên rời nhóm ưu tiên Top-N";
  if (movedUp && inTop) return "Điểm tăng tương đối nên vào nhóm ưu tiên Top-N";
  return "";
}

function recommendationByAhpPriority(priorityLabel: string) {
  if (priorityLabel === "Rất cao") return "Ưu tiên kiểm tra hiện trường";
  if (priorityLabel === "Cao") return "Đưa vào danh sách theo dõi ưu tiên";
  if (priorityLabel === "Trung bình") return "Theo dõi tăng cường";
  return "Giám sát định kỳ";
}

export default function Dashboard() {
  const [lat, setLat] = useState(10.7769);
  const [lon, setLon] = useState(106.7009);

  const [hours] = useState(24);

  // Trọng số AHP (mặc định)
  const [weights] = useState<Weights>({
    "PM2.5": 0.496316,
    PM10: 0.245375,
    NO2: 0.123473,
    O3: 0.084801,
    CO: 0.050034,
  });

  // Kết quả
  const [risk, setRisk] = useState<any>(null);
  const [hourly, setHourly] = useState<any>(null);
  const [alerts, setAlerts] = useState<any>(null);
  const [grid, setGrid] = useState<any>(null);
  const [warning, setWarning] = useState<EarlyWarningRes | null>(null);
  const [gridLoading, setGridLoading] = useState(false);
  const [gridErr, setGridErr] = useState<string | null>(null);
  const gridAbortRef = useRef<AbortController | null>(null);

  // ====== AI (M3 skeleton) ======
  const [aiHorizon, setAiHorizon] = useState(24);
  const [aiForecastRes, setAiForecastRes] = useState<AiForecastRes | null>(null);
  const [aiForecastLoading, setAiForecastLoading] = useState(false);
  const [aiForecastErr, setAiForecastErr] = useState<string | null>(null);

  // Chat UI moved to AiChatPanel (Ollama-backed)

  // Grid config
  const [gridKm] = useState(8);
  const [gridStepKm] = useState(4);
  const [maxPoints] = useState(200);

  // UX
  const [tuDongChay, setTuDongChay] = useState(true);
  const [quetKhuVuc, setQuetKhuVuc] = useState(false);

  // ===== UI layout prefs (reduce clutter) =====
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [focusMap, setFocusMap] = useState(false);
  const [showLeftPanel, setShowLeftPanel] = useState(true);
  const [showRightPanel, setShowRightPanel] = useState(true);
  const [showWeightsPanel, setShowWeightsPanel] = useState(true);
  const [showGridPanel, setShowGridPanel] = useState(true);
  const [showAiPanel, setShowAiPanel] = useState(true);
  const [showEarlyWarningPanel, setShowEarlyWarningPanel] = useState(true);
  const [showRightTabsPanel, setShowRightTabsPanel] = useState(true);
  const [mapLegendOpen, setMapLegendOpen] = useState(true);
  const [aiDrawerOpen, setAiDrawerOpen] = useState(false);
  const [, setTopNavOpen] = useState<string | null>(null);
  const [guideOpen, setGuideOpen] = useState(false);
  const [introOpen, setIntroOpen] = useState(false);
  const [topPageMode, setTopPageMode] = useState<TopPageMode>("home");
  const [mapPageTransition, setMapPageTransition] = useState<"idle" | "leaving" | "entering">("idle");
  const mapTransitionTimersRef = useRef<ReturnType<typeof setTimeout>[]>([]);
  const topNavRef = useRef<HTMLDivElement | null>(null);
  const [, setViewMode] = useState<ViewMode>("dashboard");
  const [criteriaTourOpen, setCriteriaTourOpen] = useState(false);
  const [criteriaTourStep, setCriteriaTourStep] = useState(0);
  const [criteriaTourTransitioning, setCriteriaTourTransitioning] = useState(false);
  const [criteriaTourRect, setCriteriaTourRect] = useState<{ x: number; y: number; w: number; h: number } | null>(null);
  const criteriaStepRef = useRef<HTMLDivElement | null>(null);
  const criteriaControlRef = useRef<HTMLDivElement | null>(null);
  const criteriaSaatyRef = useRef<HTMLDetailsElement | null>(null);
  const criteriaMatrixRef = useRef<HTMLDivElement | null>(null);
  const criteriaResultRef = useRef<HTMLDivElement | null>(null);
  const criteriaStep2Ref = useRef<HTMLDivElement | null>(null);
  const criteriaOverlayRef = useRef<HTMLDivElement | null>(null);
  const criteriaTourTooltipRef = useRef<HTMLDivElement | null>(null);
  const criteriaStepScrollerRef = useRef<HTMLDivElement | null>(null);
  const criteriaStepDropdownRef = useRef<HTMLDetailsElement | null>(null);
  const altMatrixDetailsRef = useRef<HTMLDetailsElement | null>(null);
  const [criteriaTourTooltipHeight, setCriteriaTourTooltipHeight] = useState(250);
  const [activeCriteriaStep, setActiveCriteriaStep] = useState<CriteriaStepIndex>(1);

  // Loading & lỗi
  const [dangChay, setDangChay] = useState(false);
  const [err, setErr] = useState<any>(null);

  const [tab, setTab] = useState<"hourly" | "alerts" | "districts">("hourly");

  // District daily history (13 districts) - required for "truy vet qua khu"
  const [histDate, setHistDate] = useState(() => todayYmdLocal());
  const [districtDaily, setDistrictDaily] = useState<{ date: string; count: number; items: DistrictDailyRow[] } | null>(null);
  const [districtDailyLoading, setDistrictDailyLoading] = useState(false);
  const [districtDailyErr, setDistrictDailyErr] = useState<string | null>(null);
  const [districtCriteria, setDistrictCriteria] = useState<DistrictCriteriaResponse | null>(null);
  const [districtCriteriaLoading, setDistrictCriteriaLoading] = useState(false);
  const [districtCriteriaErr, setDistrictCriteriaErr] = useState<string | null>(null);
  const [districtCriteriaInfo, setDistrictCriteriaInfo] = useState<string | null>(null);
  const criteriaReqSeqRef = useRef(0);
  const [criteriaInputRows, setCriteriaInputRows] = useState<CriteriaInputRow[]>([]);
  const [ahpMatrix, setAhpMatrix] = useState<number[][]>(() => cloneMatrix4(AHP_MATRIX_DEFAULT));
  const [criteriaPairResult, setCriteriaPairResult] = useState<AhpWeightsResult | null>(null);
  const [criteriaPairLoading, setCriteriaPairLoading] = useState(false);
  const [criteriaPairErr, setCriteriaPairErr] = useState<string | null>(null);
  const [criteriaForceContinue, setCriteriaForceContinue] = useState(false);
  const [criteriaCrModalOpen, setCriteriaCrModalOpen] = useState(false);
  const [criteriaCrModalResult, setCriteriaCrModalResult] = useState<AhpWeightsResult | null>(null);
  const [manualStep, setManualStep] = useState<1 | 2 | 3 | 4>(1);
  const [activeAltCriterion, setActiveAltCriterion] = useState<AhpCriterionKey>("C1");
  const [altMatrices, setAltMatrices] = useState<Record<AhpCriterionKey, number[][]>>({
    C1: buildSaatyIdentity(0),
    C2: buildSaatyIdentity(0),
    C3: buildSaatyIdentity(0),
    C4: buildSaatyIdentity(0),
  });
  const [altResults, setAltResults] = useState<Partial<Record<AhpCriterionKey, AhpWeightsResult>>>({});
  const [altCalcLoading, setAltCalcLoading] = useState(false);
  const [, setAltCalcErr] = useState<string | null>(null);
  const [altStepModalOpen, setAltStepModalOpen] = useState(false);
  const [altStepModalMessage, setAltStepModalMessage] = useState<string | null>(null);
  const [manualFinalRows, setManualFinalRows] = useState<
    Array<{
      DistrictId: number;
      DistrictName: string;
      Score: number;
      Rank: number;
      Details: Record<AhpCriterionKey, number>;
    }>
  >([]);
  const [ahpResult, setAhpResult] = useState<{
    date: string;
    count: number;
    ahp: {
      labels: string[];
      weights: Array<{ label: string; weight: number }>;
      CI: number;
      CR: number;
      lambda_max: number;
      is_consistent: boolean;
    };
    items: DistrictAHPScoredRow[];
  } | null>(null);
  const [ahpLoading, setAhpLoading] = useState(false);
  const [ahpErr, setAhpErr] = useState<string | null>(null);
  const [pairInputNotice, setPairInputNotice] = useState<string | null>(null);
  const lastPairWarningRef = useRef<string>("");
  const [activeCriteria, setActiveCriteria] = useState<Record<(typeof AHP_LABELS)[number], boolean>>({
    C1: true,
    C2: true,
    C3: true,
    C4: true,
  });
  const [selectedDistrictIds, setSelectedDistrictIds] = useState<number[]>([]);
  const [covFromDate, setCovFromDate] = useState(() => monthStartYmdLocal());
  const [covToDate, setCovToDate] = useState(() => todayYmdLocal());
  const [coverageLoading, setCoverageLoading] = useState(false);
  const [coverageErr, setCoverageErr] = useState<string | null>(null);
  const [dailyCoverage, setDailyCoverage] = useState<DistrictCoverageItem[]>([]);
  const [criteriaCoverage, setCriteriaCoverage] = useState<DistrictCoverageItem[]>([]);
  const [backfillLoading, setBackfillLoading] = useState(false);
  const [backfillMsg, setBackfillMsg] = useState<string | null>(null);
  const [scenarioPresetName, setScenarioPresetName] = useState<ScenarioPresetName>("balanced");
  const [scenarioUseCustomWeights, setScenarioUseCustomWeights] = useState(false);
  const [scenarioCustomWeights, setScenarioCustomWeights] = useState<ScenarioWeights>({
    C1: 0.25,
    C2: 0.25,
    C3: 0.25,
    C4: 0.25,
  });
  const [scenarioThresholds, setScenarioThresholds] = useState<ScenarioThresholds>({
    ...POLICY_SCENARIO_DEFAULT_THRESHOLDS,
  });
  const [scenarioEarlyWarningEnabled, setScenarioEarlyWarningEnabled] = useState(true);
  const [scenarioTopN, setScenarioTopN] = useState(5);
  const [scenarioLoading, setScenarioLoading] = useState(false);
  const [scenarioErr, setScenarioErr] = useState<string | null>(null);
  const [scenarioResult, setScenarioResult] = useState<DistrictPolicyScenarioResponse | null>(null);
  const [showEarlyWarningDetails, setShowEarlyWarningDetails] = useState(false);
  const [showScenarioAdvanced, setShowScenarioAdvanced] = useState(false);
  const [showScenarioGuide, setShowScenarioGuide] = useState(false);

  async function loadDistrictDaily(dateStr: string) {
    setDistrictDailyErr(null);
    setDistrictDailyLoading(true);
    try {
      const res = await getDistrictDaily(dateStr);
      setDistrictDaily(res);
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setDistrictDaily(null);
      setDistrictDailyErr(msg);
    } finally {
      setDistrictDailyLoading(false);
    }
  }

  async function refreshDistrictDailyNow(dateStr: string) {
    setDistrictDailyErr(null);
    setDistrictDailyLoading(true);
    try {
      // Auto mode: OpenAQ daily (station-based) -> AlertHistory -> Open-Meteo.
      await refreshDistrictDaily(dateStr, "mean", "auto");
      const res = await getDistrictDaily(dateStr);
      setDistrictDaily(res);
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setDistrictDaily(null);
      setDistrictDailyErr(msg);
    } finally {
      setDistrictDailyLoading(false);
    }
  }

  async function loadDistrictCriteriaNow(dateStr: string, forceRefresh = false): Promise<CriteriaInputRow[] | null> {
    const reqId = ++criteriaReqSeqRef.current;
    setDistrictCriteriaErr(null);
    setDistrictCriteriaInfo(null);
    setDistrictCriteriaLoading(true);
    try {
      const fallbackDays = 30;
      let refreshWarnMsg: string | null = null;
      if (forceRefresh) {
        try {
          await refreshDistrictCriteria(dateStr, { t: 15, t_high: 35, air_source: "openmeteo" });
        } catch (e: any) {
          const msg = e?.response?.data?.detail || e?.message || String(e);
          refreshWarnMsg = `Làm mới dữ liệu nguồn bị chậm/lỗi: ${msg}. Hệ thống dùng dữ liệu đã lưu + cơ chế bù.`;
        }
      }
      const res = await getDistrictCriteria(dateStr, {
        autofill: true,
        fallback_days: fallbackDays,
        t: 15,
        t_high: 35,
        timeout: 120000,
      });
      if (reqId !== criteriaReqSeqRef.current) return null;
      setDistrictCriteria(res);
      const rows = (res.items || [])
        .map((it) => ({
          DistrictId: Number(it.DistrictId),
          DistrictName: String(it.DistrictName || ""),
          C1: Number(it.C1 || 0),
          C2: Number(it.C2 || 0),
          C3: Number(it.C3 || 0),
          C4: Number(it.C4 || 0),
        }))
        .sort((a, b) => a.DistrictId - b.DistrictId);
      setCriteriaInputRows(rows);
      // Keep alternative matrices in sync with selected date data.
      setAltMatrices(buildAltMatricesFromRows(rows));
      setManualStep(1);
      setCriteriaPairResult(null);
      setCriteriaForceContinue(false);
      setCriteriaCrModalOpen(false);
      setCriteriaCrModalResult(null);
      setAltResults({});
      setManualFinalRows([]);
      setAltCalcErr(null);
      const expected = Number(res.expected_count ?? 13);
      const got = Number(res.count ?? rows.length);
      const imputed = Number(res.imputed_count ?? 0);
      const infoParts: string[] = [];
      if (imputed > 0) {
        infoParts.push(`Đã tự bù ${imputed}/${expected} quận từ lịch sử ${fallbackDays} ngày gần nhất.`);
      }
      if (refreshWarnMsg) {
        infoParts.push(refreshWarnMsg);
      }
      if (res.refresh_warning) {
        infoParts.push(`Nguồn API phản hồi chậm/lỗi: ${res.refresh_warning}.`);
      }
      if (!rows.length) {
        infoParts.push("Ngày này chưa có dữ liệu C1-C4 để tạo ma trận.");
      } else if (got >= expected) {
        infoParts.push(`Đủ dữ liệu ${got}/${expected} quận cho ngày ${dateStr}.`);
      } else {
        infoParts.push(`Hiện có ${got}/${expected} quận cho ngày ${dateStr}.`);
      }
      setDistrictCriteriaInfo(infoParts.join(" "));
      if (res.items?.length) {
        setSelectedDistrictIds((prev) => {
          if (!prev.length) return res.items.map((x) => x.DistrictId);
          const allow = new Set(res.items.map((x) => x.DistrictId));
          const kept = prev.filter((id) => allow.has(id));
          return kept.length ? kept : res.items.map((x) => x.DistrictId);
        });
      }
      return rows;
    } catch (e: any) {
      if (reqId !== criteriaReqSeqRef.current) return null;
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setDistrictCriteria(null);
      setCriteriaInputRows([]);
      setAltMatrices(buildAltMatricesFromRows([]));
      setDistrictCriteriaErr(msg);
      setDistrictCriteriaInfo(null);
      return null;
    } finally {
      if (reqId === criteriaReqSeqRef.current) {
        setDistrictCriteriaLoading(false);
      }
    }
  }

  function onHistDateChange(dateStr: string) {
    setHistDate(dateStr);
    setScenarioResult(null);
    setScenarioErr(null);
    loadDistrictCriteriaNow(dateStr, false).catch(() => {});
  }

  useEffect(() => {
    if (topPageMode !== "criteria") return;
    loadDistrictCriteriaNow(histDate, false).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [topPageMode]);

  function updateCriteriaInputCell(districtId: number, key: "C1" | "C2" | "C3" | "C4", value: string) {
    const parsed = Number(value);
    const safeValue = Number.isFinite(parsed) ? parsed : 0;
    setCriteriaInputRows((prev) =>
      prev.map((row) => (row.DistrictId === districtId ? { ...row, [key]: safeValue } : row))
    );
  }

  function updateAhpMatrixCell(i: number, j: number, rawValue: string) {
    const parsed = Number(rawValue);
    if (!Number.isFinite(parsed)) return;
    if (parsed < SAATY_MIN || parsed > SAATY_MAX) {
      const pairLabel = `${AHP_LABELS[i]}-${AHP_LABELS[j]}`;
      if (lastPairWarningRef.current !== pairLabel) {
        lastPairWarningRef.current = pairLabel;
        window.alert(`Giá trị ${pairLabel} chỉ được nhập từ 1 đến 9.`);
      }
      return;
    }
    lastPairWarningRef.current = "";
    const magnitude = clampSaatyMagnitude(parsed);
    setPairInputNotice(
      `${AHP_LABELS[i]}-${AHP_LABELS[j]} = ${magnitude}; ${AHP_LABELS[j]}-${AHP_LABELS[i]} tự động = ${formatSaatyFraction(
        1 / magnitude
      )}.`
    );
    setCriteriaPairResult(null);
    setCriteriaForceContinue(false);
    setAltResults({});
    setManualFinalRows([]);
    setAltCalcErr(null);
    setManualStep(1);
    setAhpMatrix((prev) => {
      const next = cloneMatrix4(prev);
      next[i][j] = magnitude;
      next[j][i] = Number((1 / magnitude).toFixed(10));
      next[i][i] = 1;
      next[j][j] = 1;
      return next;
    });
  }

  function updateAlternativeMatrixCell(criteriaKey: AhpCriterionKey, i: number, j: number, rawValue: string) {
    if (i === j) return;
    const parsed = Number(rawValue);
    if (!Number.isFinite(parsed)) return;
    if (parsed < SAATY_ALT_MIN || parsed > SAATY_MAX) {
      window.alert(`Giá trị chỉ được nhập trong khoảng 1/9 đến 9.`);
      return;
    }
    const ratio = nearestSaatyRatioValue(clampSaatyAltValue(parsed));
    setAltResults({});
    setManualFinalRows([]);
    setAltCalcErr(null);
    if (manualStep < 3) setManualStep(3);
    setAltMatrices((prev) => {
      const base = prev[criteriaKey] || buildSaatyIdentity(selectedCriteriaRows.length);
      const next = cloneMatrix4(base);
      next[i][j] = ratio;
      next[j][i] = Number((1 / ratio).toFixed(10));
      next[i][i] = 1;
      next[j][j] = 1;
      return { ...prev, [criteriaKey]: next };
    });
  }

  async function calculateCriteriaStep() {
    setCriteriaPairErr(null);
    setCriteriaForceContinue(false);
    setCriteriaCrModalOpen(false);
    setCriteriaCrModalResult(null);
    setCriteriaPairLoading(true);
    try {
      const res = await ahpWeights(ahpMatrix, [...AHP_LABELS], { timeout: 60000 });
      setCriteriaPairResult(res);
      if (res.is_consistent) {
        setManualStep(2);
      } else {
        setManualStep(1);
        setCriteriaCrModalResult(res);
        setCriteriaCrModalOpen(true);
      }
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setCriteriaPairResult(null);
      setCriteriaPairErr(msg);
      setManualStep(1);
    } finally {
      setCriteriaPairLoading(false);
    }
  }

  async function continueToStep3FromStep2() {
    setCriteriaPairErr(null);
    let rowsForStep3 = criteriaInputRows;
    if (!criteriaInputRows.length) {
      const rows = await loadDistrictCriteriaNow(histDate, false);
      if (!rows?.length) {
        setCriteriaPairErr("Chưa có dữ liệu C1-C4 của 13 quận cho ngày đang chọn. Hãy bấm “Nạp dữ liệu ngày” rồi thử lại.");
        return;
      }
      rowsForStep3 = rows;
    }
    if (rowsForStep3.length) {
      setAltMatrices(buildAltMatricesFromRows(rowsForStep3));
      setAltResults({});
      setManualFinalRows([]);
      setAltCalcErr(null);
    }
    setManualStep(3);
  }

  async function calculateAlternativesStep() {
    setAltCalcErr(null);
    setAltStepModalOpen(false);
    setAltStepModalMessage(null);
    setAltCalcLoading(true);
    try {
      if (!criteriaPairResult) {
        throw new Error("Chưa có kết quả ma trận tiêu chí ở bước 2.");
      }
      if (!criteriaPairResult?.is_consistent && !criteriaForceContinue) {
        throw new Error("Ma trận tiêu chí chưa đạt CR < 10%.");
      }
      if (selectedCriteriaRows.length < 2) {
        throw new Error("Cần ít nhất 2 phương án để so sánh.");
      }
      const labels = selectedCriteriaRows.map((r) => r.DistrictName);
      const entries = await Promise.all(
        AHP_LABELS.map(async (c) => {
          const matrix = altMatrices[c] || buildSaatyIdentity(labels.length);
          const r = await ahpWeights(matrix, labels, { timeout: 60000 });
          return [c, r] as const;
        })
      );
      const next = Object.fromEntries(entries) as Partial<Record<AhpCriterionKey, AhpWeightsResult>>;
      setAltResults(next);

      const invalid = AHP_LABELS.filter((c) => !next[c]?.is_consistent);
      if (invalid.length) {
        setManualStep(3);
        throw new Error(`Ma trận phương án chưa nhất quán ở: ${invalid.join(", ")} (CR phải < 10%).`);
      }

      const criteriaW = Object.fromEntries(
        (criteriaPairResult.weights || []).map((w) => [String(w.label), Number(w.weight)])
      ) as Record<string, number>;

      const scoreRows = selectedCriteriaRows.map((row) => {
        const details = {} as Record<AhpCriterionKey, number>;
        let score = 0;
        for (const c of AHP_LABELS) {
          const cW = Number(criteriaW[c] || 0);
          const altWMap = Object.fromEntries(
            ((next[c]?.weights || []) as Array<{ label: string; weight: number }>).map((w) => [
              String(w.label),
              Number(w.weight),
            ])
          ) as Record<string, number>;
          const part = cW * Number(altWMap[row.DistrictName] || 0);
          details[c] = Number(part.toFixed(6));
          score += part;
        }
        return {
          DistrictId: row.DistrictId,
          DistrictName: row.DistrictName,
          Score: Number(score.toFixed(6)),
          Rank: 0,
          Details: details,
        };
      });

      scoreRows.sort((a, b) => b.Score - a.Score);
      const ranked = scoreRows.map((r, idx) => ({ ...r, Rank: idx + 1 }));
      setManualFinalRows(ranked);
      setManualStep(4);
      setAltStepModalMessage(null);
      setAltStepModalOpen(true);
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setAltCalcErr(msg);
      setManualFinalRows([]);
      setAltStepModalMessage(msg);
      setAltStepModalOpen(true);
    } finally {
      setAltCalcLoading(false);
    }
  }

  function jumpToAltCriterion(c: AhpCriterionKey) {
    setActiveAltCriterion(c);
    setManualStep(3);
    setAltStepModalOpen(false);
    window.setTimeout(() => {
      criteriaMatrixRef.current?.scrollIntoView({ behavior: "smooth", block: "center" });
      if (altMatrixDetailsRef.current) {
        altMatrixDetailsRef.current.open = true;
      }
    }, 120);
  }

  function initAlternativeMatricesFromData() {
    const rows = selectedCriteriaRows;
    if (!rows.length) return;
    setAltMatrices(buildAltMatricesFromRows(rows));
    setAltResults({});
    setManualFinalRows([]);
    setAltCalcErr(null);
    if (manualStep >= 4) setManualStep(3);
  }

  async function runDistrictAhpNow(dateStr: string, rowsOverride?: CriteriaInputRow[] | null) {
    setAhpErr(null);
    setAhpLoading(true);
    try {
      const matrixRows = (rowsOverride && rowsOverride.length ? rowsOverride : criteriaInputRows).map((r) => ({
        DistrictId: r.DistrictId,
        DistrictName: r.DistrictName,
        C1: Number(r.C1 || 0),
        C2: Number(r.C2 || 0),
        C3: Number(r.C3 || 0),
        C4: Number(r.C4 || 0),
      }));
      const res = await getDistrictAHPScore(dateStr, {
        matrix: ahpMatrix,
        labels: [...AHP_LABELS],
        normalize_alternatives: true,
        rank_mode: "cost",
        alternatives_override: matrixRows.length ? matrixRows : undefined,
      });
      setAhpResult(res);
      if (!selectedDistrictIds.length && res.items?.length) {
        setSelectedDistrictIds(res.items.map((x) => x.DistrictId));
      }
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setAhpResult(null);
      setAhpErr(msg);
    } finally {
      setAhpLoading(false);
    }
  }

  function applyScenarioPreset(presetId: ScenarioPresetName) {
    setScenarioPresetName(presetId);
    const preset = POLICY_SCENARIO_PRESETS.find((it) => it.id === presetId);
    if (!preset) return;
    setScenarioCustomWeights({
      C1: Number(preset.weights.C1 || 0),
      C2: Number(preset.weights.C2 || 0),
      C3: Number(preset.weights.C3 || 0),
      C4: Number(preset.weights.C4 || 0),
    });
  }

  function updateScenarioWeightCell(key: keyof ScenarioWeights, raw: string) {
    const num = Number(raw);
    setScenarioCustomWeights((prev) => ({
      ...prev,
      [key]: Number.isFinite(num) ? num : 0,
    }));
  }

  function updateScenarioThresholdCell(key: keyof ScenarioThresholds, raw: string) {
    const num = Number(raw);
    setScenarioThresholds((prev) => ({
      ...prev,
      [key]: Number.isFinite(num) ? num : 0,
    }));
  }

  async function runPolicyScenarioNow(dateStr: string) {
    setScenarioErr(null);
    setScenarioLoading(true);
    try {
      const safeTopN = Math.max(1, Math.min(13, Number(scenarioTopN) || 5));
      if (safeTopN !== scenarioTopN) setScenarioTopN(safeTopN);

      const res = await runDistrictPolicyScenario({
        date: dateStr,
        presetName: scenarioPresetName,
        useCustomWeights: scenarioUseCustomWeights,
        customWeights: scenarioUseCustomWeights ? scenarioCustomWeights : undefined,
        normalizeCustomWeights: true,
        thresholds: scenarioThresholds,
        earlyWarningEnabled: scenarioEarlyWarningEnabled,
        compareWithBaseline: true,
        topN: safeTopN,
        autofill: true,
        fallback_days: 30,
        force_refresh: false,
      });
      setScenarioResult(res);
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setScenarioResult(null);
      setScenarioErr(msg);
    } finally {
      setScenarioLoading(false);
    }
  }

  async function loadCoverageRange(fromDate: string, toDate: string) {
    setCoverageErr(null);
    setCoverageLoading(true);
    try {
      const [d1, d2] = await Promise.all([
        getDistrictDailyCoverage(fromDate, toDate),
        getDistrictCriteriaCoverage(fromDate, toDate),
      ]);
      setDailyCoverage(d1.items || []);
      setCriteriaCoverage(d2.items || []);
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setDailyCoverage([]);
      setCriteriaCoverage([]);
      setCoverageErr(msg);
    } finally {
      setCoverageLoading(false);
    }
  }

  async function backfillRange(fromDate: string, toDate: string) {
    setBackfillMsg(null);
    setCoverageErr(null);
    setBackfillLoading(true);
    try {
      const [r1, r2] = await Promise.all([
        backfillDistrictDaily(fromDate, toDate, { agg: "mean", source: "openmeteo" }),
        backfillDistrictCriteria(fromDate, toDate, { air_source: "openmeteo", t: 15, t_high: 35 }),
      ]);
      setBackfillMsg(
        `Daily: ${r1.success_days}/${r1.total_days} ngày, Criteria: ${r2.success_days}/${r2.total_days} ngày`
      );
      await loadCoverageRange(fromDate, toDate);
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setBackfillMsg(`Backfill lỗi: ${msg}`);
    } finally {
      setBackfillLoading(false);
    }
  }

  useEffect(() => {
    loadCoverageRange(covFromDate, covToDate).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    setIntroOpen(false);
  }, []);

  // debounce
  const debounceRef = useRef<number | null>(null);

  function cancelGridRequest() {
    if (!gridAbortRef.current) return;
    try {
      gridAbortRef.current.abort();
    } catch {}
    gridAbortRef.current = null;
  }

  async function runGridForPoint(centerLat: number, centerLon: number) {
    if (!quetKhuVuc) return;

    cancelGridRequest();
    const ac = new AbortController();
    gridAbortRef.current = ac;

    setGridErr(null);
    setGridLoading(true);
    try {
      const bbox = bboxAround(centerLat, centerLon, gridKm);
      const gj = await gridScore(bbox, gridStepKm, hours, weights, maxPoints, {
        signal: ac.signal,
        inner_only: true,
      });
      setGrid(gj ?? null);
    } catch (e: any) {
      // ignore aborts (user moved fast)
      if (e?.name === "CanceledError" || e?.name === "AbortError") return;
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setGrid(null);
      setGridErr(msg);
    } finally {
      if (gridAbortRef.current === ac) gridAbortRef.current = null;
      setGridLoading(false);
    }
  }

  // persist UI prefs (local only)
  useEffect(() => {
    try {
      const raw = localStorage.getItem("dss_ui_prefs_v1");
      if (!raw) return;
      const p = JSON.parse(raw || "{}");
      if (typeof p.focusMap === "boolean") setFocusMap(p.focusMap);
      if (typeof p.showLeftPanel === "boolean") setShowLeftPanel(p.showLeftPanel);
      if (typeof p.showRightPanel === "boolean") setShowRightPanel(p.showRightPanel);
      if (typeof p.showWeightsPanel === "boolean") setShowWeightsPanel(p.showWeightsPanel);
      if (typeof p.showGridPanel === "boolean") setShowGridPanel(p.showGridPanel);
      if (typeof p.showAiPanel === "boolean") setShowAiPanel(p.showAiPanel);
      if (typeof p.showEarlyWarningPanel === "boolean") setShowEarlyWarningPanel(p.showEarlyWarningPanel);
      if (typeof p.showRightTabsPanel === "boolean") setShowRightTabsPanel(p.showRightTabsPanel);
    } catch {}
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem(
        "dss_ui_prefs_v1",
        JSON.stringify({
          focusMap,
          showLeftPanel,
          showRightPanel,
          showWeightsPanel,
          showGridPanel,
          showAiPanel,
          showEarlyWarningPanel,
          showRightTabsPanel,
        })
      );
    } catch {}
  }, [
    focusMap,
    showLeftPanel,
    showRightPanel,
    showWeightsPanel,
    showGridPanel,
    showAiPanel,
    showEarlyWarningPanel,
    showRightTabsPanel,
  ]);

  // Close side drawer on ESC for better UX (esp. when zoom=100%).
  useEffect(() => {
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        setAiDrawerOpen(false);
        setGuideOpen(false);
        if (topPageMode === "criteria") setTopPageMode("map");
        setTopNavOpen(null);
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [topPageMode]);

  useEffect(() => {
    function onPointerDown(e: MouseEvent) {
      if (!topNavRef.current) return;
      if (topNavRef.current.contains(e.target as Node)) return;
      setTopNavOpen(null);
    }
    window.addEventListener("mousedown", onPointerDown);
    return () => window.removeEventListener("mousedown", onPointerDown);
  }, []);

  const layoutClass = useMemo(() => {
    if (focusMap) return "cols1";
    if (topPageMode === "map" && showRightPanel) return "colsMap4060";
    const l = !!showLeftPanel;
    const r = !!showRightPanel;
    if (l && r) return "cols3";
    if (l && !r) return "cols2l";
    if (!l && r) return "cols2r";
    return "cols1";
  }, [focusMap, showLeftPanel, showRightPanel, topPageMode]);

  function applyViewMode(mode: ViewMode) {
    setViewMode(mode);
    setTopNavOpen(null);
    if (mode === "dashboard") {
      setFocusMap(false);
      setShowLeftPanel(true);
      setShowRightPanel(true);
      setShowWeightsPanel(true);
      setShowGridPanel(true);
      setShowRightTabsPanel(true);
      setShowEarlyWarningPanel(true);
      setShowAiPanel(true);
      return;
    }
    if (mode === "map") {
      setFocusMap(true);
      setShowLeftPanel(false);
      setShowRightPanel(false);
      setAiDrawerOpen(false);
      return;
    }
    if (mode === "history") {
      setFocusMap(false);
      setShowLeftPanel(false);
      setShowRightPanel(true);
      setShowRightTabsPanel(true);
      setShowEarlyWarningPanel(false);
      setTab("districts");
      setAiDrawerOpen(false);
      return;
    }
    if (mode === "ahp") {
      setFocusMap(false);
      setShowLeftPanel(true);
      setShowRightPanel(true);
      setShowWeightsPanel(true);
      setShowGridPanel(false);
      setShowRightTabsPanel(false);
      setShowEarlyWarningPanel(false);
      setAiDrawerOpen(false);
      return;
    }
    // ai
    setFocusMap(false);
    setShowLeftPanel(false);
    setShowRightPanel(true);
    setShowWeightsPanel(false);
    setShowGridPanel(false);
    setShowRightTabsPanel(false);
    setShowEarlyWarningPanel(false);
    setShowAiPanel(true);
    setAiDrawerOpen(true);
  }

  function markIntroSeen() {
    try {
      localStorage.setItem(INTRO_SEEN_KEY, "1");
    } catch {}
  }

  function openGuideModal() {
    setGuideOpen(true);
    setTopNavOpen(null);
  }

  function closeGuideModal() {
    setGuideOpen(false);
  }

  function switchTopPage(mode: TopPageMode) {
    setTopPageMode(mode);
    setTopNavOpen(null);
    if (mode !== "criteria") {
      setCriteriaTourOpen(false);
      setCriteriaTourStep(0);
      setCriteriaTourTransitioning(false);
    }
    if (mode === "map") {
      setFocusMap(false);
      setShowLeftPanel(true);
      setShowRightPanel(true);
      return;
    }
    setAiDrawerOpen(false);
  }

  function clearMapTransitionTimers() {
    if (!mapTransitionTimersRef.current.length) return;
    mapTransitionTimersRef.current.forEach((id) => window.clearTimeout(id));
    mapTransitionTimersRef.current = [];
  }

  function openMapWithTransition() {
    if (topPageMode === "map") return;
    clearMapTransitionTimers();
    setMapPageTransition("leaving");
    const leaveTimer = window.setTimeout(() => {
      switchTopPage("map");
      setMapPageTransition("entering");
      const enterTimer = window.setTimeout(() => {
        setMapPageTransition("idle");
        mapTransitionTimersRef.current = mapTransitionTimersRef.current.filter((id) => id !== enterTimer);
      }, 420);
      mapTransitionTimersRef.current.push(enterTimer);
      mapTransitionTimersRef.current = mapTransitionTimersRef.current.filter((id) => id !== leaveTimer);
    }, 180);
    mapTransitionTimersRef.current.push(leaveTimer);
  }

  function closeCriteriaPage() {
    openMapWithTransition();
  }

  function openCriteriaTourAtCurrentStep() {
    const stepMap: Record<CriteriaStepIndex, number> = {
      1: 1,
      2: 2,
      3: 3,
      4: 4,
    };
    setCriteriaTourStep(stepMap[manualStep] ?? 0);
    setCriteriaTourOpen(true);
  }

  function openStep4ResultOnMap() {
    if (!manualFinalRows.length) return;
    const rows: DistrictMapResultItem[] = manualFinalRows.map((r) => ({
      districtName: String(r.DistrictName || ""),
      rank: Number(r.Rank || 0),
      score: Number(r.Score || 0),
    }));
    const weightOrderText = [...(criteriaPairResult?.weights || [])]
      .sort((a, b) => Number(b.weight || 0) - Number(a.weight || 0))
      .map((w) => String(w.label || ""))
      .join(" > ");
    setDistrictResultPaint({
      source: "criteria-step4",
      label: `Kết quả AHP bước 4 (${histDate})`,
      rows,
      date: histDate,
      topDistricts: manualFinalRows.slice(0, 3).map((r) => String(r.DistrictName || "")),
      weightOrderText,
    });
    setSelectedMapDistrict("");
    openMapWithTransition();
    setFocusMap(false);
    setShowLeftPanel(true);
    setShowRightPanel(true);
  }

  function openScenarioResultOnMap() {
    const srcRows = scenarioResult?.scenarioResult || [];
    if (!srcRows.length) return;
    const rows: DistrictMapResultItem[] = srcRows.map((r) => ({
      districtName: String(r.districtName || ""),
      rank: Number(r.rank || 0),
      score: Number(r.score || 0),
    }));
    setDistrictResultPaint({
      source: "policy-scenario",
      label: `Scenario ${scenarioPresetName} (${scenarioResult?.date || histDate})`,
      rows,
      date: scenarioResult?.date || histDate,
      topDistricts: srcRows
        .slice()
        .sort((a, b) => Number(a.rank || 0) - Number(b.rank || 0))
        .slice(0, 3)
        .map((r) => String(r.districtName || "")),
    });
    setSelectedMapDistrict("");
    openMapWithTransition();
    setFocusMap(false);
    setShowLeftPanel(true);
    setShowRightPanel(true);
  }

  function dismissIntro(opts?: { openGuide?: boolean; goMap?: boolean }) {
    setIntroOpen(false);
    markIntroSeen();
    if (opts?.goMap) {
      openMapWithTransition();
      applyViewMode("map");
      return;
    }
    if (opts?.openGuide) {
      openGuideModal();
    }
  }

  useEffect(() => {
    return () => {
      mapTransitionTimersRef.current.forEach((id) => window.clearTimeout(id));
      mapTransitionTimersRef.current = [];
    };
  }, []);

  // ====== AQICN Station mode ======
  const [source] = useState<"model" | "station">("model");
  const [stations, setStations] = useState<any[]>([]);
  const [stationUid, setStationUid] = useState<number | null>(null);
  const [, setLoadingStations] = useState(false);

  const [addrLabel, setAddrLabel] = useState<string | null>(null);
  const [addrText, setAddrText] = useState("");
  const [addrLoading, setAddrLoading] = useState(false);
  const [selectedMapDistrict, setSelectedMapDistrict] = useState("");
  const addrAbortRef = useRef<AbortController | null>(null);
  const [selectedOpenAQStation, setSelectedOpenAQStation] = useState<OpenAQStation | null>(null);
  const [openaqMapCount, setOpenaqMapCount] = useState(0);
  const [aqicnMapCount, setAqicnMapCount] = useState(0);
  const [iqairMapCount, setIqairMapCount] = useState(0);
  const [purpleAirMapCount, setPurpleAirMapCount] = useState(0);
  const [districtResultPaint, setDistrictResultPaint] = useState<{
    source: "criteria-step4" | "policy-scenario";
    label: string;
    rows: DistrictMapResultItem[];
    date?: string;
    topDistricts?: string[];
    weightOrderText?: string;
  } | null>(null);
  const [mapSourceStatuses, setMapSourceStatuses] = useState<SourceStatusMap>({});
  const sourceReliability = useMemo(() => {
    const rows = ["openaq", "aqicn", "iqair", "purpleair"]
      .map((k) => mapSourceStatuses[k as keyof SourceStatusMap])
      .filter(Boolean) as MapSourceStatus[];
    const errorCount = rows.filter((r) => !!r.error).length;
    let score =
      (openaqMapCount >= 10 ? 52 : openaqMapCount > 0 ? 34 : 0) +
      (aqicnMapCount > 0 ? 10 : 0) +
      (purpleAirMapCount > 0 ? 8 : 0) +
      (iqairMapCount > 0 ? 6 : 0) -
      errorCount * 12;
    score = Math.max(0, Math.min(100, score));
    const label = score >= 70 ? "Cao" : score >= 40 ? "Trung bình" : "Thấp";
    const tone = score >= 70 ? "green" : score >= 40 ? "yellow" : "orange";
    const errorSources = rows.filter((r) => !!r.error).map((r) => r.source.toUpperCase());
    return { score, label, tone, errorSources };
  }, [mapSourceStatuses, openaqMapCount, aqicnMapCount, iqairMapCount, purpleAirMapCount]);
  const [selectedStationQuick, setSelectedStationQuick] = useState<any | null>(null);
  const [selectedStationQuickLoading, setSelectedStationQuickLoading] = useState(false);
  const [selectedStationQuickErr, setSelectedStationQuickErr] = useState<string | null>(null);
  const [stationForecastPageOpen, setStationForecastPageOpen] = useState(false);
  const stationQuickReqRef = useRef(0);

  async function reverseLookupAddress(nLat: number, nLon: number) {
    try {
      setAddrLoading(true);
      addrAbortRef.current?.abort();
      const ac = new AbortController();
      addrAbortRef.current = ac;

      const res = await geocodeReverse({ lat: nLat, lon: nLon }, { signal: ac.signal });
      if (ac.signal.aborted) return;

      const label = String(res?.display_name || "").trim();
      if (label) {
        setAddrLabel(label);
        setAddrText(label);
      } else {
        // keep previous text if reverse failed to return a label
        setAddrLabel(null);
      }
    } catch (e: any) {
      if (e?.name === "CanceledError" || e?.name === "AbortError" || e?.code === "ERR_CANCELED") return;
      // Do not hard-fail UI if Nominatim is rate-limited; keep last known label.
    } finally {
      setAddrLoading(false);
    }
  }

  async function inspectOpenAQStation(st: OpenAQStation) {
    setSelectedOpenAQStation(st);
    setSelectedStationQuickErr(null);
    setSelectedStationQuickLoading(true);
    const reqId = ++stationQuickReqRef.current;
    try {
      const [riskAtStation, airAtStation] = await Promise.all([
        scoreRisk(st.lat, st.lon, weights),
        getAirQuality(st.lat, st.lon, 24),
      ]);
      if (stationQuickReqRef.current !== reqId) return;
      setSelectedStationQuick({
        risk: riskAtStation,
        air: airAtStation,
        fetchedAt: new Date().toISOString(),
      });
    } catch (e: any) {
      if (stationQuickReqRef.current !== reqId) return;
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setSelectedStationQuick(null);
      setSelectedStationQuickErr(msg);
    } finally {
      if (stationQuickReqRef.current === reqId) {
        setSelectedStationQuickLoading(false);
      }
    }
  }

  async function loadStationsAroundPoint(centerLat: number, centerLon: number) {
    try {
      setLoadingStations(true);

      // quét rộng 80km quanh điểm đang chọn (đủ bắt trạm)
      const km = 200;
      const bbox = bboxAround(centerLat, centerLon, km);

      const res = await getStationsBounds(bbox);
      const items = res.items || [];
      setStations(items);

      // auto chọn trạm đầu tiên nếu chưa chọn hoặc trạm cũ không còn trong list
      if (items.length > 0) {
        const stillExists = stationUid ? items.some((x: any) => Number(x.uid) === Number(stationUid)) : false;
        if (!stationUid || !stillExists) {
          setStationUid(Number(items[0].uid));
        }
      } else {
        setStationUid(null);
      }
    } finally {
      setLoadingStations(false);
    }
  }

  async function refreshAlerts() {
    const a = await getAlerts(ALERTS_HISTORY_LIMIT);
    setAlerts(a);
  }

  useEffect(() => {
    refreshAlerts().catch(() => {});
    loadStationsAroundPoint(lat, lon).catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  async function runAiForecast() {
    if (aiForecastLoading) return;

    setAiForecastErr(null);
    setAiForecastLoading(true);
    try {
      const res = await aiForecast({
        lat,
        lon,
        horizon_hours: aiHorizon,
        weights,
        threshold: 60,
        model: "openmeteo_baseline",
      });
      setAiForecastRes(res);
    } catch (e: any) {
      const msg = e?.response?.data?.detail || e?.message || String(e);
      setAiForecastErr(msg);
      setAiForecastRes(null);
    } finally {
      setAiForecastLoading(false);
    }
  }

  // runAiChat removed (AiChatPanel owns conversation state)

  // ✅ CHẠY DSS (1 action)
  async function chayDSS(_opts?: { isManual?: boolean }) {
    if (dangChay) return;

    setErr(null);
    setDangChay(true);

    try {
      // nếu đang ở chế độ trạm thì phải có stationUid
      if (source === "station") {
        if (!stationUid) throw new Error("Chưa chọn trạm AQICN.");

        // tìm station đã load để lấy lat/lon
        const st = stations.find((x) => Number(x.uid) === Number(stationUid));
        const useLat = st?.lat ?? lat;
        const useLon = st?.lon ?? lon;

        const [resRiskStation, resDss, resHourly, resAlerts] = await Promise.all([
          scoreRiskStation(stationUid, weights),
          runDss({
            lat: useLat,
            lon: useLon,
            hours,
            weights,
            threshold: 60,
            delta_threshold: 15,
            delta_window: 3,
          }),
          getAirQuality(useLat, useLon, hours),
          getAlerts(ALERTS_HISTORY_LIMIT),
        ]);

        setRisk(resRiskStation); // risk hiển thị = trạm
        setWarning(resDss.early_warning); // warning = DSS
        setGrid(null);
        setHourly(resHourly);
        setAlerts(resAlerts);

        setTab("hourly");
        if (quetKhuVuc) runGridForPoint(useLat, useLon).catch(() => {});
        return;
      }

      // ====== MODEL mode ======
      const [resDss, resHourly, resAlerts] = await Promise.all([
        runDss({
          lat,
          lon,
          hours,
          weights,
          threshold: 60,
          delta_threshold: 15,
          delta_window: 3,
        }),
        getAirQuality(lat, lon, hours),
        getAlerts(ALERTS_HISTORY_LIMIT),
      ]);

      setRisk(resDss.score);
      setWarning(resDss.early_warning);
      setGrid(null);
      setHourly(resHourly);
      setAlerts(resAlerts);

      setTab("hourly");
      if (quetKhuVuc) runGridForPoint(lat, lon).catch(() => {});
    } catch (e: any) {
      setErr(e?.response?.data?.detail ?? e?.response?.data ?? e?.message ?? String(e));
    } finally {
      setDangChay(false);
    }
  }

  // ✅ Auto-run + debounce khi đổi config
  useEffect(() => {
    if (!tuDongChay) return;
    if (dangChay) return;

    // nếu đang ở station mode mà chưa chọn trạm thì không auto-run
    if (source === "station" && !stationUid) return;

    if (debounceRef.current) window.clearTimeout(debounceRef.current);

    debounceRef.current = window.setTimeout(() => {
      chayDSS({ isManual: false });
    }, 600);

    return () => {
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    tuDongChay,
    quetKhuVuc,
    source,
    stationUid,
    lat,
    lon,
    hours,
    gridKm,
    gridStepKm,
    maxPoints,
    JSON.stringify(weights),
  ]);

  const topFactor = risk?.explain?.top_factors?.[0] ?? null;
  const coverageRows = useMemo(() => {
    const dm = new Map<string, number>();
    const cm = new Map<string, number>();
    for (const it of dailyCoverage || []) dm.set(String(it.date), Number(it.count || 0));
    for (const it of criteriaCoverage || []) cm.set(String(it.date), Number(it.count || 0));
    const keys = Array.from(new Set([...dm.keys(), ...cm.keys()])).sort();
    return keys.map((k) => {
      const daily = dm.get(k) ?? 0;
      const criteria = cm.get(k) ?? 0;
      return {
        date: k,
        daily,
        criteria,
        ok: daily === 13 && criteria === 13,
      };
    });
  }, [dailyCoverage, criteriaCoverage]);

  const districtOptions = useMemo(() => {
    const byId = new Map<number, string>();
    const push = (id: any, name: any) => {
      const nId = Number(id);
      if (!Number.isFinite(nId)) return;
      if (!byId.has(nId)) byId.set(nId, String(name || `Quận ${nId}`));
    };
    (ahpResult?.items || []).forEach((x) => push(x.DistrictId, x.DistrictName));
    (districtCriteria?.items || []).forEach((x) => push(x.DistrictId, x.DistrictName));
    (districtDaily?.items || []).forEach((x) => push(x.DistrictId, x.DistrictName));
    return Array.from(byId.entries())
      .map(([id, name]) => ({ id, name }))
      .sort((a, b) => a.id - b.id);
  }, [ahpResult, districtCriteria, districtDaily]);

  const selectedCriteriaRows = useMemo(() => {
    const selectedSet = new Set(selectedDistrictIds);
    const rows = criteriaInputRows.filter((r) => !selectedSet.size || selectedSet.has(r.DistrictId));
    return rows.sort((a, b) => a.DistrictId - b.DistrictId);
  }, [criteriaInputRows, selectedDistrictIds]);

  const criterionFlatState = useMemo(() => {
    const out = {} as Record<AhpCriterionKey, boolean>;
    for (const c of AHP_LABELS) {
      const vals = selectedCriteriaRows.map((r) => Number(r[c] || 0).toFixed(6));
      out[c] = vals.length > 0 && new Set(vals).size <= 1;
    }
    return out;
  }, [selectedCriteriaRows]);

  const inconsistentAltCriteria = useMemo(
    () => AHP_LABELS.filter((c) => !!altResults[c] && !altResults[c]!.is_consistent),
    [altResults]
  );

  useEffect(() => {
    if (!districtOptions.length) return;
    setSelectedDistrictIds((prev) => {
      if (!prev.length) return districtOptions.map((x) => x.id);
      const allow = new Set(districtOptions.map((x) => x.id));
      const kept = prev.filter((id) => allow.has(id));
      return kept.length ? kept : districtOptions.map((x) => x.id);
    });
  }, [districtOptions]);

  useEffect(() => {
    const n = selectedCriteriaRows.length;
    setAltMatrices((prev) => {
      const next = { ...prev };
      const seeded = buildAltMatricesFromRows(selectedCriteriaRows);
      for (const c of AHP_LABELS) {
        const m = next[c];
        if (!m || m.length !== n || (n > 0 && (m[0]?.length ?? 0) !== n)) {
          next[c] = seeded[c];
        }
      }
      return next;
    });
    setAltResults({});
    setManualFinalRows([]);
    setAltCalcErr(null);
    setManualStep((prev) => (prev >= 4 ? 3 : prev));
  }, [selectedCriteriaRows]);

  useEffect(() => {
    if (topPageMode !== "criteria") return;
    setActiveCriteriaStep(manualStep);
    if (manualStep <= 1) {
      criteriaStepScrollerRef.current?.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    const target =
      manualStep === 2
        ? criteriaStep2Ref.current
        : manualStep === 3
          ? criteriaMatrixRef.current
          : criteriaResultRef.current;
    if (!target) return;
    const t = window.setTimeout(() => {
      target.scrollIntoView({ behavior: "smooth", block: "center" });
    }, 120);
    return () => window.clearTimeout(t);
  }, [manualStep, topPageMode]);

  useEffect(() => {
    if (topPageMode !== "criteria") return;
    const prev = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.body.style.overflow = prev;
    };
  }, [topPageMode]);

  useEffect(() => {
    const html = document.documentElement;
    const body = document.body;
    const root = document.getElementById("root");

    const prevHtmlOverflow = html.style.overflow;
    const prevBodyOverflow = body.style.overflow;
    const prevRootHeight = root?.style.getPropertyValue("height") ?? "";
    const prevRootHeightPriority = root?.style.getPropertyPriority("height") ?? "";
    const prevRootMinHeight = root?.style.getPropertyValue("min-height") ?? "";
    const prevRootMinHeightPriority = root?.style.getPropertyPriority("min-height") ?? "";
    const prevRootOverflow = root?.style.getPropertyValue("overflow") ?? "";
    const prevRootOverflowPriority = root?.style.getPropertyPriority("overflow") ?? "";

    if (topPageMode === "home") {
      html.style.overflow = "auto";
      body.style.overflow = "auto";
      if (root) {
        root.style.setProperty("height", "auto", "important");
        root.style.setProperty("min-height", "100vh", "important");
        root.style.setProperty("overflow", "visible", "important");
      }
    } else {
      html.style.overflow = "hidden";
      body.style.overflow = "hidden";
      if (root) {
        root.style.setProperty("height", "100%", "important");
        root.style.setProperty("min-height", "100%", "important");
        root.style.removeProperty("overflow");
      }
    }

    return () => {
      html.style.overflow = prevHtmlOverflow;
      body.style.overflow = prevBodyOverflow;
      if (root) {
        if (prevRootHeight) {
          root.style.setProperty("height", prevRootHeight, prevRootHeightPriority || "");
        } else {
          root.style.removeProperty("height");
        }
        if (prevRootMinHeight) {
          root.style.setProperty("min-height", prevRootMinHeight, prevRootMinHeightPriority || "");
        } else {
          root.style.removeProperty("min-height");
        }
        if (prevRootOverflow) {
          root.style.setProperty("overflow", prevRootOverflow, prevRootOverflowPriority || "");
        } else {
          root.style.removeProperty("overflow");
        }
      }
    };
  }, [topPageMode]);

  useEffect(() => {
    if (topPageMode !== "criteria") return;
    const root = criteriaStepScrollerRef.current;
    if (!root) return;
    const stepEls = Array.from(root.querySelectorAll<HTMLElement>("[data-criteria-step]"));
    if (!stepEls.length) return;

    const observer = new IntersectionObserver(
      (entries) => {
        let bestStep: CriteriaStepIndex | null = null;
        let bestRatio = 0;
        for (const entry of entries) {
          if (!entry.isIntersecting) continue;
          const ratio = entry.intersectionRatio || 0;
          if (ratio < 0.6 || ratio < bestRatio) continue;
          const raw = Number(entry.target.getAttribute("data-criteria-step"));
          if (![1, 2, 3, 4].includes(raw)) continue;
          bestStep = raw as CriteriaStepIndex;
          bestRatio = ratio;
        }
        if (bestStep) setActiveCriteriaStep(bestStep);
      },
      {
        root,
        threshold: [0.6, 0.75, 0.9],
      }
    );

    stepEls.forEach((el) => observer.observe(el));
    return () => observer.disconnect();
  }, [topPageMode]);

  const ahpInteractiveRows = useMemo(() => {
    const items = ahpResult?.items || [];
    const selectedSet = new Set(selectedDistrictIds);
    const labels = AHP_LABELS.filter((c) => activeCriteria[c]);
    const rows = items
      .filter((it) => !selectedSet.size || selectedSet.has(Number(it.DistrictId)))
      .map((it) => {
        const contrib = (it.AHPContrib || {}) as Record<string, number>;
        const interactiveScore = labels.length
          ? labels.reduce((s, c) => s + Number(contrib[c] || 0), 0)
          : Number(it.AHPScore || 0);
        return {
          ...it,
          InteractiveScore: Number(interactiveScore.toFixed(6)),
        };
      })
      .sort((a, b) => a.InteractiveScore - b.InteractiveScore);
    return rows.map((it, idx) => ({ ...it, InteractiveRank: idx + 1 }));
  }, [ahpResult, selectedDistrictIds, activeCriteria]);
  const criteriaWeightChartData = useMemo(
    () =>
      (criteriaPairResult?.weights || []).map((w) => ({
        name: String(w.label),
        value: Number(w.weight),
      })),
    [criteriaPairResult]
  );

  const manualBarData = useMemo(
    () =>
      manualFinalRows.map((r) => ({
        name: r.DistrictName,
        score: Number(r.Score),
      })),
    [manualFinalRows]
  );
  const scenarioWeightSum = useMemo(
    () =>
      Number(
        (
          Number(scenarioCustomWeights.C1 || 0) +
          Number(scenarioCustomWeights.C2 || 0) +
          Number(scenarioCustomWeights.C3 || 0) +
          Number(scenarioCustomWeights.C4 || 0)
        ).toFixed(6)
      ),
    [scenarioCustomWeights]
  );
  const scenarioCompareItems = useMemo(() => scenarioResult?.comparison?.items || [], [scenarioResult]);
  const scenarioSummaryShifts = useMemo(() => scenarioResult?.summary?.top3StrongestShifts || [], [scenarioResult]);
  const scenarioAppliedTopN = useMemo(
    () => Math.max(1, Number(scenarioResult?.comparison?.topN || scenarioTopN || 5)),
    [scenarioResult, scenarioTopN]
  );
  const scenarioPresetDescription = useMemo(
    () => POLICY_SCENARIO_PRESET_DESCRIPTIONS[scenarioPresetName] || "Kịch bản hiện tại sẽ dùng để tính thứ hạng.",
    [scenarioPresetName]
  );
  const scenarioRowsByDistrictId = useMemo(() => {
    const m = new Map<number, any>();
    for (const row of scenarioResult?.scenarioResult || []) {
      m.set(Number(row.districtId), row);
    }
    return m;
  }, [scenarioResult]);
  const scenarioStrongestShift = useMemo(() => {
    if (scenarioSummaryShifts.length) return scenarioSummaryShifts[0];
    if (!scenarioCompareItems.length) return null;
    const sorted = [...scenarioCompareItems].sort((a, b) => Math.abs(Number(b.rankDelta || 0)) - Math.abs(Number(a.rankDelta || 0)));
    return sorted[0] || null;
  }, [scenarioSummaryShifts, scenarioCompareItems]);
  const scenarioPresetPriorityText = useMemo(
    () => POLICY_SCENARIO_PRESET_PRIORITY_HINT[scenarioPresetName] || "phân bổ theo cấu hình kịch bản hiện tại.",
    [scenarioPresetName]
  );
  const scenarioDataBiasText = useMemo(() => {
    const bias = String(scenarioResult?.summary?.scenarioBias?.criterion || "");
    const biasHint = POLICY_SCENARIO_CRITERION_HINT[bias];
    if (!biasHint) return "";
    return `Trong dữ liệu hiện tại, tiêu chí nổi bật: ${biasHint}.`;
  }, [scenarioResult]);
  const scenarioWatchList = useMemo(() => {
    if (!scenarioCompareItems.length) return [];
    return [...scenarioCompareItems]
      .sort((a, b) => Number(a.scenarioRank || 0) - Number(b.scenarioRank || 0))
      .slice(0, Math.min(3, scenarioAppliedTopN))
      .map((x) => String(x.districtName || "").trim())
      .filter((x) => x.length > 0);
  }, [scenarioCompareItems, scenarioAppliedTopN]);
  const step4WeightOrderText = useMemo(
    () =>
      [...(criteriaPairResult?.weights || [])]
        .sort((a, b) => Number(b.weight || 0) - Number(a.weight || 0))
        .map((w) => String(w.label || ""))
        .filter((x) => x.length > 0)
        .join(" > "),
    [criteriaPairResult]
  );
  const step4Top3DistrictText = useMemo(
    () => manualFinalRows.slice(0, 3).map((r) => String(r.DistrictName || "")).join(", "),
    [manualFinalRows]
  );
  const mapModeLabel = useMemo(() => {
    if (!districtResultPaint) return "";
    const dateText = districtResultPaint.date || histDate;
    if (districtResultPaint.source === "policy-scenario") {
      return `Đang hiển thị mức ưu tiên theo kịch bản hiện tại - ngày ${dateText}.`;
    }
    return `Bản đồ đang tô màu theo mức ưu tiên AHP của 13 quận - ngày ${dateText}.`;
  }, [districtResultPaint, histDate]);
  const mapStatusText = useMemo(() => {
    if (!districtResultPaint) {
      return "Bản đồ đang hiển thị dữ liệu tại điểm chọn hiện tại.";
    }
    const tops = districtResultPaint.topDistricts?.length ? districtResultPaint.topDistricts.join(", ") : "";
    return tops ? `${mapModeLabel} Top hiện tại: ${tops}.` : mapModeLabel;
  }, [districtResultPaint, mapModeLabel]);
  const currentDistrictContext = useMemo(() => {
    const byMapPick = String(selectedMapDistrict || "").trim();
    if (byMapPick) return byMapPick;
    const byStation = String(selectedOpenAQStation?.district || "").trim();
    if (byStation) return byStation;
    const label = String(addrLabel || "").trim();
    if (!label) return "";
    const rows = districtResultPaint?.rows || [];
    const key = normalizeDistrictKey(label);
    const matched = rows.find((r) => key.includes(normalizeDistrictKey(String(r.districtName || ""))));
    return matched ? String(matched.districtName || "") : "";
  }, [selectedMapDistrict, selectedOpenAQStation, addrLabel, districtResultPaint]);
  const currentDistrictDecisionContext = useMemo(() => {
    const rows = districtResultPaint?.rows || [];
    if (!rows.length || !currentDistrictContext) return null;
    const decisionTopN = districtResultPaint?.source === "policy-scenario" ? scenarioAppliedTopN : 5;
    const key = normalizeDistrictKey(currentDistrictContext);
    let matched = rows.find((r) => normalizeDistrictKey(String(r.districtName || "")) === key);
    if (!matched) {
      matched = rows.find((r) => key.includes(normalizeDistrictKey(String(r.districtName || ""))));
    }
    if (!matched) return null;
    const rank = Number(matched.rank || 0);
    return {
      districtName: String(matched.districtName || ""),
      rank,
      total: rows.length,
      score: Number(matched.score || 0),
      priorityLabel: scenarioPriorityLabelByRank(rank, decisionTopN),
    };
  }, [districtResultPaint, currentDistrictContext, scenarioAppliedTopN]);
  const quickDecisionMessage = useMemo(() => {
    if (!districtResultPaint) return "";
    if (!currentDistrictDecisionContext) {
      return "Chọn một quận trên bản đồ để xem hạng AHP, mức ưu tiên và khuyến nghị chi tiết.";
    }
    const dateText = districtResultPaint.date || histDate;
    if (districtResultPaint.source === "policy-scenario") {
      return `Theo kịch bản hiện tại, ${currentDistrictDecisionContext.districtName} thuộc nhóm ưu tiên ${currentDistrictDecisionContext.priorityLabel}.`;
    }
    if (currentDistrictDecisionContext.rank <= 3) {
      return `Theo kết quả AHP ngày ${dateText}, ${currentDistrictDecisionContext.districtName} nằm trong Top 3 ưu tiên.`;
    }
    return `Theo kết quả AHP ngày ${dateText}, khu vực đang xem thuộc nhóm ưu tiên ${currentDistrictDecisionContext.priorityLabel}.`;
  }, [districtResultPaint, currentDistrictDecisionContext, histDate]);
  const selectedDistrictScenarioContext = useMemo(() => {
    if (!currentDistrictDecisionContext || districtResultPaint?.source !== "policy-scenario") return null;
    const key = normalizeDistrictKey(currentDistrictDecisionContext.districtName);
    return (
      scenarioCompareItems.find((it) => normalizeDistrictKey(String(it.districtName || "")) === key) ||
      scenarioCompareItems.find((it) => key.includes(normalizeDistrictKey(String(it.districtName || "")))) ||
      null
    );
  }, [currentDistrictDecisionContext, districtResultPaint, scenarioCompareItems]);
  const selectedDistrictTopCriteriaText = useMemo(() => {
    if (!currentDistrictDecisionContext) return "";
    const key = normalizeDistrictKey(currentDistrictDecisionContext.districtName);
    let criteriaObj: Record<string, number> | null = null;
    if (districtResultPaint?.source === "policy-scenario") {
      const pickedScenarioRow =
        (scenarioResult?.scenarioResult || []).find((row) => normalizeDistrictKey(String(row.districtName || "")) === key) ||
        (scenarioResult?.scenarioResult || []).find((row) =>
          key.includes(normalizeDistrictKey(String(row.districtName || "")))
        );
      if (pickedScenarioRow?.criteriaValues && typeof pickedScenarioRow.criteriaValues === "object") {
        criteriaObj = pickedScenarioRow.criteriaValues as Record<string, number>;
      }
    }
    if (!criteriaObj) {
      const baselineRow = manualFinalRows.find((row) => normalizeDistrictKey(String(row.DistrictName || "")) === key);
      if (baselineRow?.Details) criteriaObj = baselineRow.Details as unknown as Record<string, number>;
    }
    if (!criteriaObj) return "";
    const ranked = (["C1", "C2", "C3", "C4"] as const)
      .map((k) => ({ key: k, value: Number(criteriaObj?.[k] || 0) }))
      .filter((it) => Number.isFinite(it.value))
      .sort((a, b) => b.value - a.value)
      .slice(0, 2)
      .map((it) => it.key);
    return ranked.length ? ranked.join(", ") : "";
  }, [currentDistrictDecisionContext, districtResultPaint, scenarioResult, manualFinalRows]);
  const selectedDistrictRecommendation = useMemo(() => {
    if (!currentDistrictDecisionContext) return "";
    if (selectedDistrictScenarioContext) {
      return compactScenarioRecommendation(
        String(selectedDistrictScenarioContext.scenarioLevel || ""),
        String(selectedDistrictScenarioContext.recommendation || ""),
        Boolean(selectedDistrictScenarioContext.earlyWarning),
        Number(selectedDistrictScenarioContext.scenarioRank || 0),
        scenarioAppliedTopN,
        Number(selectedDistrictScenarioContext.rankDelta || 0)
      );
    }
    return recommendationByAhpPriority(currentDistrictDecisionContext.priorityLabel);
  }, [currentDistrictDecisionContext, selectedDistrictScenarioContext, scenarioAppliedTopN]);
  const earlyWarningSummaryText = useMemo(() => {
    const status = warning?.warning ? "Có tín hiệu cần chú ý" : "Chưa kích hoạt";
    const maxScore = warning?.maxScore !== undefined ? Number(warning.maxScore).toFixed(1) : "—";
    const maxLevel = warning?.maxLevel || "—";
    return `Trạng thái: ${status} · maxScore: ${maxScore} · mức dự báo cao nhất: ${maxLevel}.`;
  }, [warning]);

  const stepStateClass = (step: CriteriaStepIndex) =>
    manualStep === step ? "current" : manualStep > step ? "done" : "locked";
  const stepFocusClass = (step: CriteriaStepIndex) => (activeCriteriaStep === step ? "active-step" : "inactive-step");
  const criteriaCurrentStepMeta = useMemo(
    () => CRITERIA_STEP_OPTIONS.find((item) => item.step === manualStep) || CRITERIA_STEP_OPTIONS[0],
    [manualStep]
  );
  const criteriaCurrentStepTitle = criteriaCurrentStepMeta.title;
  const criteriaStepNextHint = criteriaCurrentStepMeta.hint;
  const stepStatusText = (step: CriteriaStepIndex) => {
    const status = stepStateClass(step);
    if (status === "done") return "Hoàn thành";
    if (status === "current") return "Đang làm";
    return "Đang khóa";
  };
  const jumpToCriteriaStep = (step: CriteriaStepIndex) => {
    if (step > manualStep) return;
    setManualStep(step);
    setActiveCriteriaStep(step);
    if (criteriaStepDropdownRef.current) {
      criteriaStepDropdownRef.current.open = false;
    }
  };

  const decisionSnapshot = useMemo(() => {
    const rawScore = Number(risk?.score_0_100);
    const score = Number.isFinite(rawScore) ? rawScore : null;
    const level = String(risk?.level || "");
    const lv = level.toLowerCase();
    const hasWarning = Boolean(warning?.warning);
    const topLabel = topFactor?.label ? String(topFactor.label) : "PM2.5";
    const topValue = topFactor?.value ?? "—";

    let tone: "green" | "yellow" | "orange" | "red" | "slate" = "slate";
    let headline = "Chưa đủ dữ liệu để kết luận";
    let action = "Bấm Chạy DSS để tạo kết quả đánh giá.";

    if (score !== null) {
      if (lv.includes("đỏ") || lv.includes("do") || score >= 85) {
        tone = "red";
        headline = "Nguy cơ rất cao - cần hành động ngay";
        action = "Ưu tiên cảnh báo cộng đồng và giảm nguồn phát thải cục bộ tại khu vực nóng.";
      } else if (lv.includes("cam") || score >= 65) {
        tone = "orange";
        headline = "Nguy cơ cao - cần theo dõi chặt";
        action = "Tăng tần suất giám sát, theo dõi 6-24 giờ và chuẩn bị kịch bản cảnh báo.";
      } else if (lv.includes("vàng") || lv.includes("vang") || score >= 40) {
        tone = "yellow";
        headline = "Nguy cơ trung bình - cần giám sát định kỳ";
        action = "Duy trì theo dõi theo giờ, ưu tiên kiểm tra các điểm có mật độ giao thông cao.";
      } else {
        tone = "green";
        headline = "Nguy cơ thấp - duy trì giám sát";
        action = "Tiếp tục theo dõi và duy trì lịch kiểm tra dữ liệu hằng ngày.";
      }
    }

    const warningLine = hasWarning
      ? `Cảnh báo sớm đang kích hoạt (${warning?.maxLevel || "mức chưa xác định"}).`
      : "Chưa có cảnh báo sớm trong chu kỳ dự báo hiện tại.";
    const sourceLine = `Độ tin cậy nguồn: ${sourceReliability.label} (${sourceReliability.score}/100).`;
    const reason = `Yếu tố chi phối hiện tại: ${topLabel} = ${topValue}.`;

    return {
      tone,
      headline,
      action,
      scoreText: score === null ? "—" : score.toFixed(2),
      levelText: level || "Chưa có mức",
      warningLine,
      sourceLine,
      reason,
    };
  }, [risk, warning, topFactor, sourceReliability]);

  const criteriaTourSteps = useMemo(
    () => [
      {
        id: "overview",
        title: "Tổng quan trang Tiêu Chí",
        desc: "Trang này cho người dùng tự đánh giá theo tiêu chí: chọn ngày, nhập mức ưu tiên 1-9, chỉnh bảng dữ liệu 13 quận rồi tính AHP.",
        ref: criteriaStepRef,
      },
      {
        id: "controls",
        title: "Bước 1: Nạp dữ liệu C1-C4",
        desc: "Chọn ngày cần đánh giá, bấm “Nạp C1-C4”, nhập ma trận tiêu chí rồi bấm “Tính toán bước 2”.",
        ref: criteriaControlRef,
      },
      {
        id: "saaty",
        title: "Bước 2: Tham chiếu thang đo Saaty",
        desc: "Nhập mức 1-9 cho từng cặp tiêu chí. Hệ thống tự tạo nghịch đảo (ví dụ 5 ↔ 1/5) và cảnh báo nếu nhập ngoài khoảng cho phép.",
        ref: criteriaSaatyRef,
      },
      {
        id: "matrix",
        title: "Bước 3: Chỉnh ma trận dữ liệu 13 quận",
        desc: "Sửa trực tiếp C1-C4 cho từng quận. Đây là bảng được thay đổi số để tính lại AHP.",
        ref: criteriaMatrixRef,
      },
      {
        id: "result",
        title: "Bước 4: Đọc kết quả AHP",
        desc: "Kiểm tra CR, xếp hạng và score theo ma trận dữ liệu bạn vừa nhập.",
        ref: criteriaResultRef,
      },
    ],
    []
  );

  useEffect(() => {
    if (topPageMode !== "criteria") {
      setCriteriaTourOpen(false);
      setCriteriaTourRect(null);
    }
  }, [topPageMode]);

  useEffect(() => {
    if (topPageMode !== "criteria") return;
    try {
      const seen = localStorage.getItem(CRITERIA_TOUR_SEEN_KEY);
      if (!seen) {
        setCriteriaTourStep(0);
        setCriteriaTourOpen(true);
        localStorage.setItem(CRITERIA_TOUR_SEEN_KEY, "1");
      }
    } catch {
      setCriteriaTourStep(0);
      setCriteriaTourOpen(true);
    }
  }, [topPageMode]);

  useEffect(() => {
    if (!criteriaTourOpen || topPageMode !== "criteria") return;
    const current = criteriaTourSteps[criteriaTourStep];
    const el = current?.ref.current;
    if (!el) return;

    el.scrollIntoView({ behavior: "auto", block: "center", inline: "nearest" });
    setCriteriaTourTransitioning(true);

    const measure = () => {
      const rect = el.getBoundingClientRect();
      const pad = 10;
      const pageTop = rect.top + window.scrollY;
      const pageLeft = rect.left + window.scrollX;
      const viewportTop = pageTop - window.scrollY;
      const viewportLeft = pageLeft - window.scrollX;
      setCriteriaTourRect({
        x: Math.max(6, viewportLeft - pad),
        y: Math.max(6, viewportTop - pad),
        w: Math.min(window.innerWidth - 12, rect.width + pad * 2),
        h: Math.min(window.innerHeight - 12, rect.height + pad * 2),
      });
    };

    const raf1 = window.requestAnimationFrame(measure);
    const raf2 = window.requestAnimationFrame(() => window.requestAnimationFrame(measure));
    const t2 = window.setTimeout(() => setCriteriaTourTransitioning(false), 420);
    const onResize = () => measure();
    const onAnyScroll = () => measure();
    const ro = new ResizeObserver(() => measure());
    const overlayEl = criteriaOverlayRef.current;
    ro.observe(el);
    window.addEventListener("resize", onResize);
    window.addEventListener("scroll", onAnyScroll);
    overlayEl?.addEventListener("scroll", onAnyScroll, { passive: true });
    return () => {
      window.cancelAnimationFrame(raf1);
      window.cancelAnimationFrame(raf2);
      window.clearTimeout(t2);
      ro.disconnect();
      window.removeEventListener("resize", onResize);
      window.removeEventListener("scroll", onAnyScroll);
      overlayEl?.removeEventListener("scroll", onAnyScroll as EventListener);
    };
  }, [criteriaTourOpen, criteriaTourStep, criteriaTourSteps, topPageMode]);

  useEffect(() => {
    const html = document.documentElement;
    const prevHtmlOverflow = html.style.overflow;
    const prev = document.body.style.overflow;
    const prevBodyPaddingRight = document.body.style.paddingRight;
    const preventScroll = (e: Event) => e.preventDefault();
    if (criteriaTourOpen && topPageMode === "criteria") {
      const sbw = window.innerWidth - document.documentElement.clientWidth;
      html.style.overflow = "hidden";
      document.body.style.overflow = "hidden";
      if (sbw > 0) document.body.style.paddingRight = `${sbw}px`;
      window.addEventListener("wheel", preventScroll, { passive: false });
      window.addEventListener("touchmove", preventScroll, { passive: false });
    }
    return () => {
      html.style.overflow = prevHtmlOverflow;
      document.body.style.overflow = prev;
      document.body.style.paddingRight = prevBodyPaddingRight;
      window.removeEventListener("wheel", preventScroll as EventListener);
      window.removeEventListener("touchmove", preventScroll as EventListener);
    };
  }, [criteriaTourOpen, topPageMode]);

  useEffect(() => {
    if (!criteriaTourOpen) return;
    const tooltipEl = criteriaTourTooltipRef.current;
    if (!tooltipEl) return;
    const updateTooltipSize = () => {
      const rect = tooltipEl.getBoundingClientRect();
      if (rect.height > 0) setCriteriaTourTooltipHeight(rect.height);
    };
    updateTooltipSize();
    const ro = new ResizeObserver(updateTooltipSize);
    ro.observe(tooltipEl);
    window.addEventListener("resize", updateTooltipSize);
    return () => {
      ro.disconnect();
      window.removeEventListener("resize", updateTooltipSize);
    };
  }, [criteriaTourOpen, criteriaTourStep]);

  useEffect(() => {
    if (!criteriaTourOpen) return;
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        setCriteriaTourOpen(false);
        return;
      }
      if (e.key === "ArrowRight" || e.key === "Enter") {
        setCriteriaTourStep((prev) => Math.min(prev + 1, criteriaTourSteps.length - 1));
      }
      if (e.key === "ArrowLeft") {
        setCriteriaTourStep((prev) => Math.max(prev - 1, 0));
      }
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [criteriaTourOpen, criteriaTourSteps.length]);

  const criteriaTourCurrent = criteriaTourSteps[criteriaTourStep] || criteriaTourSteps[0];
  const criteriaTourProgress = ((criteriaTourStep + 1) / Math.max(criteriaTourSteps.length, 1)) * 100;
  const criteriaTourTooltipStyle = useMemo(() => {
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const tooltipWidth = Math.min(420, vw - 24);
    const tooltipHeight = Math.min(Math.max(criteriaTourTooltipHeight || 250, 190), vh - 24);
    if (!criteriaTourRect) {
      const leftCenter = Math.max(12, (vw - tooltipWidth) / 2);
      const topCenter = Math.max(12, (vh - tooltipHeight) / 2);
      return { top: topCenter, left: leftCenter, width: tooltipWidth };
    }

    type Dir = "top" | "bottom" | "left" | "right";
    const margin = 12;
    const gap = 14;
    const rect = criteriaTourRect;
    const spaces: Record<Dir, number> = {
      top: rect.y - margin,
      bottom: vh - (rect.y + rect.h) - margin,
      left: rect.x - margin,
      right: vw - (rect.x + rect.w) - margin,
    };
    const dirs: Dir[] = (Object.entries(spaces) as Array<[Dir, number]>)
      .sort((a, b) => b[1] - a[1])
      .map(([d]) => d);
    const clamp = (v: number, min: number, max: number) => Math.min(Math.max(v, min), max);
    const overlap = (a: { left: number; top: number; width: number; height: number }, b: { left: number; top: number; width: number; height: number }) =>
      a.left < b.left + b.width && a.left + a.width > b.left && a.top < b.top + b.height && a.top + a.height > b.top;

    const makeCandidate = (dir: Dir) => {
      if (dir === "top") {
        return {
          left: rect.x + rect.w / 2 - tooltipWidth / 2,
          top: rect.y - tooltipHeight - gap,
          width: tooltipWidth,
          height: tooltipHeight,
        };
      }
      if (dir === "bottom") {
        return {
          left: rect.x + rect.w / 2 - tooltipWidth / 2,
          top: rect.y + rect.h + gap,
          width: tooltipWidth,
          height: tooltipHeight,
        };
      }
      if (dir === "left") {
        return {
          left: rect.x - tooltipWidth - gap,
          top: rect.y + rect.h / 2 - tooltipHeight / 2,
          width: tooltipWidth,
          height: tooltipHeight,
        };
      }
      return {
        left: rect.x + rect.w + gap,
        top: rect.y + rect.h / 2 - tooltipHeight / 2,
        width: tooltipWidth,
        height: tooltipHeight,
      };
    };

    const focusRect = { left: rect.x, top: rect.y, width: rect.w, height: rect.h };
    for (const dir of dirs) {
      const cand = makeCandidate(dir);
      const fixed = {
        left: clamp(cand.left, margin, vw - tooltipWidth - margin),
        top: clamp(cand.top, margin, vh - tooltipHeight - margin),
        width: tooltipWidth,
        height: tooltipHeight,
      };
      if (!overlap(fixed, focusRect)) {
        return { left: fixed.left, top: fixed.top, width: tooltipWidth };
      }
    }

    const fallbackLeft = clamp(vw / 2 - tooltipWidth / 2, margin, vw - tooltipWidth - margin);
    const fallbackTop = rect.y + rect.h + gap + tooltipHeight <= vh - margin
      ? rect.y + rect.h + gap
      : clamp(rect.y - tooltipHeight - gap, margin, vh - tooltipHeight - margin);
    return { left: fallbackLeft, top: fallbackTop, width: tooltipWidth };
  }, [criteriaTourRect, criteriaTourTooltipHeight]);
  const criteriaTourSpotlightStyle = useMemo(() => {
    if (!criteriaTourRect) return undefined;
    const r = Math.max(12, Math.min(22, criteriaTourRect.h * 0.16));
    return {
      top: `${criteriaTourRect.y}px`,
      left: `${criteriaTourRect.x}px`,
      width: `${criteriaTourRect.w}px`,
      height: `${criteriaTourRect.h}px`,
      borderRadius: `${r}px`,
      clipPath: `inset(0 round ${r}px)`,
      WebkitClipPath: `inset(0 round ${r}px)`,
      ["--spot-r" as any]: `${r}px`,
    } as any;
  }, [criteriaTourRect]);

  useEffect(() => {
    if (topPageMode !== "criteria") return;
    requestAnimationFrame(() => {
      criteriaOverlayRef.current?.scrollTo({ top: 0, left: 0, behavior: "auto" });
      criteriaStepScrollerRef.current?.scrollTo({ top: 0, left: 0, behavior: "auto" });
      const inner = criteriaStepScrollerRef.current?.querySelector<HTMLElement>(".step-inner-scroll");
      inner?.scrollTo({ top: 0, left: 0, behavior: "auto" });
    });
  }, [topPageMode, manualStep]);
  const criteriaTourShadeStyles = useMemo(() => {
    if (!criteriaTourRect) return null;
    const vw = window.innerWidth;
    const vh = window.innerHeight;
    const x = Math.max(0, criteriaTourRect.x);
    const y = Math.max(0, criteriaTourRect.y);
    const w = Math.max(0, criteriaTourRect.w);
    const h = Math.max(0, criteriaTourRect.h);
    return {
      top: { top: 0, left: 0, width: "100%", height: y },
      bottom: { top: y + h, left: 0, width: "100%", height: Math.max(0, vh - (y + h)) },
      left: { top: y, left: 0, width: x, height: h },
      right: { top: y, left: x + w, width: Math.max(0, vw - (x + w)), height: h },
    };
  }, [criteriaTourRect]);
  const criteriaTourOverlayPortal = criteriaTourOpen ? (
    <div className={`criteriaTourOverlay ${criteriaTourTransitioning ? "moving" : ""}`}>
      {criteriaTourShadeStyles ? (
        <>
          <div className={`criteriaTourShade ${criteriaTourTransitioning ? "moving" : ""}`} style={criteriaTourShadeStyles.top as any} />
          <div className={`criteriaTourShade ${criteriaTourTransitioning ? "moving" : ""}`} style={criteriaTourShadeStyles.left as any} />
          <div className={`criteriaTourShade ${criteriaTourTransitioning ? "moving" : ""}`} style={criteriaTourShadeStyles.right as any} />
          <div className={`criteriaTourShade ${criteriaTourTransitioning ? "moving" : ""}`} style={criteriaTourShadeStyles.bottom as any} />
        </>
      ) : (
        <div className="criteriaTourShade criteriaTourShadeFull" />
      )}
      <div
        className={`criteriaTourSpotlight ${criteriaTourTransitioning ? "moving" : ""}`}
        style={criteriaTourSpotlightStyle}
      />
      <div
        ref={criteriaTourTooltipRef}
        className={`criteriaTourTooltip ${criteriaTourTransitioning ? "switching" : ""}`}
        style={criteriaTourTooltipStyle}
        onMouseDown={(e) => e.stopPropagation()}
      >
        <div className="criteriaTourBadge">Guided tour - AHP criteria</div>
        <div className="criteriaTourTitle">{criteriaTourCurrent.title}</div>
        <div className="criteriaTourDesc">{criteriaTourCurrent.desc}</div>
        <div className="criteriaTourProgressRow">
          <span>
            Step {criteriaTourStep + 1} / {criteriaTourSteps.length}
          </span>
          <span>{Math.round(criteriaTourProgress)}%</span>
        </div>
        <div className="criteriaTourProgressTrack">
          <div className="criteriaTourProgressFill" style={{ width: `${criteriaTourProgress}%` }} />
        </div>
        <div className="criteriaTourActions">
          <button
            className="btn secondary"
            onClick={() => setCriteriaTourStep((prev) => Math.max(prev - 1, 0))}
            disabled={criteriaTourStep === 0}
          >
            Back
          </button>
          {criteriaTourStep < criteriaTourSteps.length - 1 ? (
            <button className="btn" onClick={() => setCriteriaTourStep((prev) => Math.min(prev + 1, criteriaTourSteps.length - 1))}>
              Next
            </button>
          ) : (
            <button
              className="btn"
              onClick={() => {
                setCriteriaTourOpen(false);
                setCriteriaTourStep(0);
              }}
            >
              Done
            </button>
          )}
        </div>
        <div className="criteriaTourHint">Keyboard: Left/Right to change step, ESC to exit.</div>
      </div>
    </div>
  ) : null;

  return (
    <div
      className={`page ${topPageMode === "home" ? "page-home" : ""} ${
        mapPageTransition !== "idle" ? `page-map-transition-${mapPageTransition}` : ""
      }`.trim()}
    >
      {topPageMode !== "home" ? (
      <div className="topbar">
        <div className="topBrand">
          <span className="topBrandLogo" aria-hidden="true">AQ</span>
          <span className="topBrandText">AirDSS</span>
        </div>

        <div className="topMainNavWrap" ref={topNavRef}>
          <nav className="topMainNav" aria-label="Main menu">
            <button
              className={`topMainBtn ${topPageMode === "home" ? "active" : ""}`}
              onClick={() => switchTopPage("home")}
            >
              Trang Chủ
            </button>
            <button
              className={`topMainBtn ${topPageMode === "news" ? "active" : ""}`}
              onClick={() => switchTopPage("news")}
            >
              Tin Tức
            </button>
            <button
              className={`topMainBtn topMainBtnMap ${topPageMode === "map" ? "active" : ""}`}
              onClick={openMapWithTransition}
            >
              Dữ liệu bản đồ
            </button>
            <button
              className={`topMainBtn ${topPageMode === "criteria" ? "active" : ""}`}
              onClick={() => switchTopPage("criteria")}
            >
              Tiêu Chí
            </button>
            <button
              className={`topMainBtn ${topPageMode === "system" ? "active" : ""}`}
              onClick={() => switchTopPage("system")}
            >
              Hệ thống
            </button>
          </nav>
        </div>

        <div className="topbarActions">
          <button className="btn secondary topGuideBtn" onClick={openGuideModal} title="Hướng dẫn sử dụng">
            Hướng dẫn
          </button>

          {topPageMode === "map" ? (
            <>
              <button
                className={`topIconBtn ${focusMap ? "active" : ""}`}
                onClick={() => {
                  const next = !focusMap;
                  setFocusMap(next);
                  setViewMode(next ? "map" : "dashboard");
                }}
                title={focusMap ? "Hiện lại panel" : "Focus map"}
                aria-label={focusMap ? "Hiện lại panel" : "Focus map"}
              >
                ◱
              </button>

              <button
                className={`topIconBtn ${tuDongChay ? "active" : ""}`}
                onClick={() => setTuDongChay((v) => !v)}
                title={tuDongChay ? "Tắt tự chạy" : "Bật tự chạy"}
                aria-label={tuDongChay ? "Tắt tự chạy" : "Bật tự chạy"}
              >
                ⚡
              </button>

              <button
                className={`topIconBtn ${quetKhuVuc ? "active" : ""}`}
                onClick={() => {
                  const v = !quetKhuVuc;
                  setQuetKhuVuc(v);
                  if (!v) {
                    cancelGridRequest();
                    setGrid(null);
                    setGridErr(null);
                    setGridLoading(false);
                  } else {
                    runGridForPoint(lat, lon).catch(() => {});
                  }
                }}
                title={quetKhuVuc ? "Tắt quét khu vực" : "Bật quét khu vực"}
                aria-label={quetKhuVuc ? "Tắt quét khu vực" : "Bật quét khu vực"}
              >
                ▦
              </button>

              <button className="topIconBtn" onClick={() => setSettingsOpen(true)} title="Cài đặt" aria-label="Cài đặt">
                ⚙
              </button>

              {showAiPanel ? (
                <button
                  className={`btn secondary aiBtn topAiBtn ${aiDrawerOpen ? "active" : ""}`}
                  onClick={() => {
                    setViewMode("ai");
                    setAiDrawerOpen(true);
                  }}
                  title="Open AI drawer"
                >
                  <span className="aiDot" aria-hidden="true" /> AI
                </button>
              ) : null}

              <button className="btn topRunBtn" onClick={() => chayDSS({ isManual: true })} disabled={dangChay}>
                {dangChay ? "Đang chạy..." : "Chạy DSS"}
              </button>
            </>
          ) : (
            <>
              <button className="btn secondary" onClick={openMapWithTransition}>
                Mở dữ liệu bản đồ
              </button>
              <button className="btn" onClick={() => switchTopPage("criteria")}>
                Mở tiêu chí
              </button>
            </>
          )}
        </div>
      </div>
      ) : null}

      {introOpen ? (
        <div
          className="introOverlay"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) dismissIntro();
          }}
        >
          <div className="introCard">
            <div className="introBadge">Hệ hỗ trợ ra quyết định chất lượng không khí</div>
            <h2 className="introTitle">AirDSS TP.HCM nội thành</h2>
            <p className="introDesc">
              Nền tảng này phục vụ đánh giá rủi ro theo AHP, giám sát theo bản đồ trạm, và cảnh báo sớm theo thời gian.
            </p>

            <div className="introAudienceGrid">
              <div className="introAudienceItem">
                <div className="introAudienceTitle">Đối tượng sử dụng</div>
                <div className="introAudienceText">Nhóm nghiên cứu, quản lý môi trường, và đội vận hành cảnh báo.</div>
              </div>
              <div className="introAudienceItem">
                <div className="introAudienceTitle">Mục tiêu chính</div>
                <div className="introAudienceText">Chấm điểm rủi ro, truy vết lịch sử 13 quận, và hỗ trợ quyết định nhanh.</div>
              </div>
            </div>

            <div className="introStepGrid">
              <div className="introStepItem">
                <span className="introStepNum">1</span>
                <div>
                  <b>Chọn vị trí</b>
                  <p>Click bản đồ hoặc nhập địa chỉ trong TP.HCM.</p>
                </div>
              </div>
              <div className="introStepItem">
                <span className="introStepNum">2</span>
                <div>
                  <b>Chạy DSS</b>
                  <p>Hệ thống tính điểm rủi ro theo trọng số AHP hiện tại.</p>
                </div>
              </div>
              <div className="introStepItem">
                <span className="introStepNum">3</span>
                <div>
                  <b>Đọc khuyến nghị</b>
                  <p>Xem cảnh báo sớm, biểu đồ lịch sử, và đề xuất hành động.</p>
                </div>
              </div>
            </div>

            <div className="introActions">
              <button className="btn" onClick={() => dismissIntro()}>
                Bắt đầu
              </button>
              <button className="btn secondary" onClick={() => dismissIntro({ openGuide: true })}>
                Xem hướng dẫn nhanh
              </button>
              <button className="btn secondary" onClick={() => dismissIntro({ goMap: true })}>
                Chỉ xem bản đồ
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {guideOpen ? (
        <div
          className="modalOverlay"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) closeGuideModal();
          }}
        >
          <div className="modal guideModal">
            <div className="modalHeader">
              <div className="modalTitle">Hướng dẫn sử dụng nhanh</div>
              <button className="btn secondary" onClick={closeGuideModal}>
                Đóng
              </button>
            </div>
            <div className="modalBody">
              <div className="guideSteps">
                <div className="guideStepCard">
                  <div className="guideStepHead">Bước 1 - Chọn dữ liệu đầu vào</div>
                  <p>Chọn điểm trên bản đồ, hoặc nhập địa chỉ để hệ thống lấy tọa độ.</p>
                </div>
                <div className="guideStepCard">
                  <div className="guideStepHead">Bước 2 - Chạy đánh giá</div>
                  <p>Bấm <b>Chạy DSS</b> để tính điểm rủi ro, mức cảnh báo và cập nhật lịch sử.</p>
                </div>
                <div className="guideStepCard">
                  <div className="guideStepHead">Bước 3 - Đọc quyết định</div>
                  <p>Xem khuyến nghị nhanh, cảnh báo sớm, tab lịch sử và dữ liệu 13 quận theo ngày.</p>
                </div>
              </div>

              <div className="guideQuickActions">
                <button
                  className="btn secondary"
                  onClick={() => {
                    applyViewMode("dashboard");
                    closeGuideModal();
                  }}
                >
                  Mở chế độ tổng quan
                </button>
                <button
                  className="btn secondary"
                  onClick={() => {
                    applyViewMode("map");
                    closeGuideModal();
                  }}
                >
                  Mở chế độ bản đồ
                </button>
                <button
                  className="btn secondary"
                  onClick={() => {
                    setShowAiPanel(true);
                    setAiDrawerOpen(true);
                    closeGuideModal();
                  }}
                >
                  Mở AI forecast/chat
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {criteriaCrModalOpen && criteriaCrModalResult ? (
        <div
          className="modalOverlay"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) setCriteriaCrModalOpen(false);
          }}
        >
          <div className="modal criteriaModal criteriaCrModal">
            <div className="modalHeader">
              <div className="modalTitle">Kết quả kiểm tra nhất quán ma trận tiêu chí</div>
              <button className="btn secondary" onClick={() => setCriteriaCrModalOpen(false)}>
                Đóng
              </button>
            </div>
            <div className="modalBody">
              <div className="criteriaInputNotice">
                λmax: <b>{toFixedOrDash(criteriaCrModalResult.lambda_max, 4)}</b> · CI:{" "}
                <b>{toFixedOrDash(criteriaCrModalResult.CI, 4)}</b> · CR:{" "}
                <b>{toFixedOrDash(criteriaCrModalResult.CR, 4)}</b>
              </div>
              <div className="criteriaConsistencyBanner bad" style={{ marginTop: 8 }}>
                CR lớn hơn 10%. Theo quy chuẩn AHP nên nhập lại ma trận; nếu cần demo, bạn vẫn có thể tiếp tục sang bước 2.
              </div>
              <div className="btnRow" style={{ marginTop: 12 }}>
                <button
                  className="btn secondary"
                  onClick={() => {
                    setCriteriaCrModalOpen(false);
                    setCriteriaForceContinue(false);
                    setManualStep(1);
                  }}
                >
                  Nhập lại bước 1
                </button>
                <button
                  className="btn"
                  onClick={() => {
                    setCriteriaCrModalOpen(false);
                    setCriteriaForceContinue(true);
                    setManualStep(2);
                  }}
                >
                  Tiếp tục sang bước 2
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {altStepModalOpen ? (
        <div
          className="modalOverlay"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) setAltStepModalOpen(false);
          }}
        >
          <div className="modal altStepModal criteriaModal criteriaAltModal">
            <div className="modalHeader">
              <div className="modalTitle">Kết quả bước 3 - Ma trận phương án</div>
              <button className="btn secondary" onClick={() => setAltStepModalOpen(false)}>
                Đóng popup
              </button>
            </div>
            <div className="modalBody">
              {altStepModalMessage ? (
                <div className="criteriaConsistencyBanner bad">Lỗi bước 3: {altStepModalMessage}</div>
              ) : (
                <div className="criteriaConsistencyBanner ok">Bước 3 hoàn tất. Tất cả ma trận phương án hiện đạt CR &lt; 10%.</div>
              )}
              {AHP_LABELS.some((c) => altResults[c]) ? (
                <div className="altStepCriteriaGrid" style={{ marginTop: 10 }}>
                  {AHP_LABELS.map((c) => {
                    const r = altResults[c];
                    const stateClass = !r ? "na" : r.is_consistent ? "ok" : "bad";
                    return (
                      <div key={`alt-cr-modal-${c}`} className={`altStepCriterionCard ${stateClass}`}>
                        <div className="altStepCriterionHead">{c}</div>
                        <div className="altStepCriterionValue">CR {r ? toFixedOrDash(r.CR, 6) : "-"}</div>
                        <div className="altStepCriterionState">
                          {!r ? "Chưa tính" : r.is_consistent ? "Đạt" : "Chưa đạt"}
                        </div>
                      </div>
                    );
                  })}
                </div>
              ) : null}
              {inconsistentAltCriteria.length ? (
                <div className="btnRow" style={{ marginTop: 12, flexWrap: "wrap" }}>
                  {inconsistentAltCriteria.map((c) => (
                    <button key={`jump-criterion-${c}`} className="btn secondary" onClick={() => jumpToAltCriterion(c)}>
                      Đi tới {c} đang lỗi
                    </button>
                  ))}
                </div>
              ) : null}
              <div className="criteriaMeta" style={{ marginTop: 10 }}>
                {inconsistentAltCriteria.length
                  ? "Gợi ý: mở ma trận phương án đang lỗi để chỉnh lại các cặp so sánh, mục tiêu là CR < 10%."
                  : "Bạn có thể chỉnh tay từng ma trận phương án nếu muốn tinh chỉnh kết quả theo chuyên gia."}
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {topPageMode === "criteria" ? (
        <div
          ref={criteriaOverlayRef}
          className={`criteriaPageOverlay ${criteriaTourOpen ? "tour-lock" : ""}`}
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) closeCriteriaPage();
          }}
        >
          <div className={`criteriaPageShell ${criteriaTourOpen ? "tour-open" : ""} ${criteriaTourTransitioning ? "tour-moving" : ""}`}>
            <div className="criteriaPageTopbar">
              <button className="btn secondary" onClick={closeCriteriaPage}>
                ← Quay lại dashboard
              </button>
              <div className="criteriaPageHead">
                <div className="criteriaPageTitle">Trang xét tiêu chí AHP (4 tiêu chí - 13 quận)</div>
                <div className="criteriaPageSubTitle">
                  Luồng 4 bước: (1) nhập ma trận tiêu chí, (2) kiểm tra CR/CI, (3) so sánh phương án theo từng tiêu chí, (4) xem kết quả tổng hợp.
                </div>
              </div>
              <div className="criteriaTopActions">
                <button
                  className="btn secondary"
                  onClick={openCriteriaTourAtCurrentStep}
                >
                  Hướng dẫn tiêu chí
                </button>
                <button
                  className="btn"
                  onClick={async () => {
                    await loadDistrictCriteriaNow(histDate, false);
                  }}
                >
                  Nạp dữ liệu ngày
                </button>
              </div>
            </div>

            <div
              className={`criteriaNowHint criteriaTourTarget ${criteriaTourOpen && criteriaTourStep === 0 ? "criteriaTourFocus tour-focused" : ""}`}
              ref={criteriaStepRef}
            >
              <div className="criteriaNowHintMain">
                <span className="pill">Đang ở bước {manualStep}/4</span>
                <span className="criteriaNowTitle">{criteriaCurrentStepTitle}</span>
                <span className="criteriaNowDesc">{criteriaStepNextHint}</span>
              </div>
              <details className="criteriaStepJumpDropdown" ref={criteriaStepDropdownRef}>
                <summary>Chuyển bước nhanh</summary>
                <div className="criteriaStepJumpMenu">
                  {CRITERIA_STEP_OPTIONS.map((item) => {
                    const statusClass = stepStateClass(item.step);
                    const isLocked = statusClass === "locked";
                    return (
                      <button
                        key={`criteria-step-jump-${item.step}`}
                        type="button"
                        className={`criteriaStepJumpItem ${statusClass}`}
                        onClick={() => jumpToCriteriaStep(item.step)}
                        disabled={isLocked}
                      >
                        <span className="criteriaStepJumpDot" />
                        <span className="criteriaStepJumpText">{item.title}</span>
                        <span className="criteriaStepJumpState">{stepStatusText(item.step)}</span>
                      </button>
                    );
                  })}
                </div>
              </details>
            </div>

            <div className="criteriaPageGrid criteriaStepScroller" ref={criteriaStepScrollerRef}>
              {manualStep === 1 ? (
              <div
                data-criteria-step="1"
                className={`criteriaControlCard criteriaTourTarget criteriaStepPanel criteriaSnapStep criteriaFocusStep ${stepStateClass(1)} ${stepFocusClass(1)} ${criteriaTourOpen && criteriaTourStep === 1 ? "criteriaTourFocus tour-focused" : ""}`}
                ref={criteriaControlRef}
              >
                <div className="step-inner-scroll">
                <div className="field">
                  <span className="label">Ngày đánh giá</span>
                  <input className="input" type="date" value={histDate} onChange={(e) => onHistDateChange(e.target.value)} />
                </div>
                <div className="btnRow criteriaLoadRow">
                  <button className="btn secondary" onClick={() => loadDistrictCriteriaNow(histDate, false)} disabled={districtCriteriaLoading}>
                    Nạp C1-C4
                  </button>
                  {districtCriteriaInfo ? <div className="criteriaInputNotice criteriaInlineNotice">{districtCriteriaInfo}</div> : null}
                </div>

                <div className="criteriaMeta">
                  {districtCriteriaLoading ? <div>Đang tải C1-C4...</div> : null}
                  {districtCriteriaErr ? <div className="ahpErrText">Lỗi C1-C4: {districtCriteriaErr}</div> : null}
                  {criteriaPairErr ? <div className="ahpErrText">Lỗi bước 2: {criteriaPairErr}</div> : null}
                </div>

                <details
                  className={`criteriaSaatyCard criteriaTourTarget ${criteriaTourOpen && criteriaTourStep === 2 ? "criteriaTourFocus tour-focused" : ""}`}
                  ref={criteriaSaatyRef}
                >
                  <summary className="criteriaSaatySummary">Mở bảng hướng dẫn tiêu chí và thang đo Saaty</summary>
                  <div className="criteriaSaatyTitle">Bảng tiêu chí và thang đo Saaty</div>
                  <div className="criteriaMiniBlock">
                    <div className="criteriaMiniTitle">4 tiêu chí đang dùng</div>
                    <table className="criteriaGuideTable">
                      <thead>
                        <tr>
                          <th>Mã</th>
                          <th>Tiêu chí</th>
                          <th>Mô tả ngắn</th>
                        </tr>
                      </thead>
                      <tbody>
                        {CRITERIA_META.map((item) => (
                          <tr key={`meta-${item.key}`}>
                            <td>{item.key}</td>
                            <td>{item.title}</td>
                            <td>{item.desc}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  <div className="criteriaMiniBlock">
                    <div className="criteriaMiniTitle">Xác định mức độ ưu tiên cho các tiêu chí</div>
                    <div className="criteriaScaleStrip criteriaScaleStripVerbose">
                      {SAATY_PRIORITY_GUIDE.map((item) => (
                        <div key={`strip-${item.value}`} className={`criteriaScalePoint ${item.value === "1" ? "mid" : ""}`}>
                          <span>{item.value}</span>
                          <small>{item.label}</small>
                        </div>
                      ))}
                    </div>
                  </div>
                </details>

                <div className="criteriaSaatyCard criteriaMainMatrixCard criteriaStep1MatrixCard">
                  <div className="criteriaMiniTitle">Ma trận so sánh cặp tiêu chí (nhập trực tiếp theo bảng)</div>
                  <div className="criteriaPairMatrixWrap">
                    <table className="criteriaPairMatrixTable">
                      <thead>
                        <tr>
                          <th>Tiêu chí</th>
                          {AHP_LABELS.map((label) => (
                            <th key={`pair-head-main-${label}`}>{label}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {AHP_LABELS.map((rowLabel, i) => (
                          <tr key={`pair-main-row-${rowLabel}`}>
                            <th className="criteriaPairMatrixRowHead">{rowLabel}</th>
                            {AHP_LABELS.map((colLabel, j) => {
                              if (i === j) {
                                return (
                                  <td key={`pair-main-cell-${rowLabel}-${colLabel}`} className="criteriaPairMatrixDiag">
                                    1
                                  </td>
                                );
                              }
                              if (i < j) {
                                return (
                                  <td key={`pair-main-cell-${rowLabel}-${colLabel}`}>
                                    <input
                                      className="criteriaPairMatrixInput"
                                      type="number"
                                      min={SAATY_MIN}
                                      max={SAATY_MAX}
                                      step={1}
                                      value={clampSaatyMagnitude(ahpMatrix[i][j])}
                                      onChange={(e) => updateAhpMatrixCell(i, j, e.target.value)}
                                    />
                                  </td>
                                );
                              }
                              return (
                                <td key={`pair-main-cell-${rowLabel}-${colLabel}`} className="criteriaPairMatrixAuto">
                                  {formatSaatyFraction(ahpMatrix[i][j])}
                                </td>
                              );
                            })}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                  {pairInputNotice ? <div className="criteriaInputNotice">{pairInputNotice}</div> : null}
                  <div className="criteriaInputWarn">Nếu nhập ngoài khoảng 1-9, hệ thống sẽ cảnh báo để nhập lại.</div>
                  <div className="btnRow" style={{ marginTop: 8 }}>
                    <button className="btn secondary" onClick={calculateCriteriaStep} disabled={criteriaPairLoading}>
                      {criteriaPairLoading ? "Đang tính..." : "Tính toán bước 2"}
                    </button>
                  </div>
                  {criteriaPairErr ? <div className="ahpErrText">Lỗi ma trận tiêu chí: {criteriaPairErr}</div> : null}
                  {criteriaPairResult ? (
                    <div className="criteriaInputNotice criteriaCompactNote" style={{ marginTop: 8 }}>
                      Đã tính xong bước 1. Chuyển xuống <b>Bước 2</b> để xem đầy đủ λmax / CI / CR và quyết định tiếp tục.
                    </div>
                  ) : null}
                  <div className="criteriaRefVideo">
                    Tham khảo thao tác:{" "}
                    <a href="https://www.youtube.com/watch?v=6dWmc72aTGc" target="_blank" rel="noreferrer">
                      video minh họa
                    </a>
                  </div>
                </div>
                </div>
              </div>
              ) : null}

              {manualStep === 2 ? (
              <div
                  data-criteria-step="2"
                  ref={criteriaStep2Ref}
                  className={`criteriaControlCard criteriaStep2Panel criteriaWizardSection criteriaStepPanel criteriaSnapStep criteriaFocusStep ${stepStateClass(2)} ${stepFocusClass(2)} ${manualStep >= 2 ? "" : "criteriaStepLocked"}`}
                >
                  <div className="step-inner-scroll">
                  {manualStep < 2 ? (
                    <div className="criteriaLockText">Hoàn thành bước 1 và bấm “Tính toán bước 2” để mở mục này.</div>
                  ) : criteriaPairResult ? (
                    <>
                      <div className="criteriaInputNotice">
                        λmax: <b>{toFixedOrDash(criteriaPairResult.lambda_max, 4)}</b> · CI:{" "}
                        <b>{toFixedOrDash(criteriaPairResult.CI, 4)}</b> · CR:{" "}
                        <b>{toFixedOrDash(criteriaPairResult.CR, 4)}</b>
                      </div>
                      <div className={`criteriaConsistencyBanner ${criteriaPairResult.is_consistent ? "ok" : "bad"}`}>
                        {criteriaPairResult.is_consistent
                          ? "Tỷ số nhất quán CR < 10%: có thể tiếp tục so sánh phương án."
                          : criteriaForceContinue
                            ? "CR >= 10%: đang tiếp tục theo chế độ bỏ qua kiểm tra nhất quán."
                            : "Tỷ số nhất quán CR >= 10%: cần nhập lại ma trận tiêu chí ở bước 1."}
                      </div>
                      <div className="ahpWeightRow">
                        {(criteriaPairResult.weights || []).map((w) => (
                          <span key={`c-w-${w.label}`} className="ahpWeightPill">
                            {w.label}: {toFixedOrDash(w.weight, 6)}
                          </span>
                        ))}
                      </div>
                      <div className="btnRow" style={{ marginTop: 8 }}>
                        <button
                          className="btn"
                          disabled={!criteriaPairResult.is_consistent && !criteriaForceContinue}
                          onClick={continueToStep3FromStep2}
                        >
                          Tiếp tục so sánh phương án
                        </button>
                      </div>
                    </>
                  ) : (
                    <div className="criteriaEmptyText">Chưa có kết quả bước 2.</div>
                  )}
                  </div>
                </div>
              ) : null}

              {manualStep === 3 ? (
              <div
                  data-criteria-step="3"
                  className={`criteriaControlCard criteriaTourTarget criteriaWizardSection ${
                    criteriaTourOpen && criteriaTourStep === 3 ? "criteriaTourFocus tour-focused" : ""
                  } criteriaStepPanel criteriaStep3Panel criteriaSnapStep criteriaFocusStep ${stepStateClass(3)} ${stepFocusClass(3)} ${manualStep >= 3 ? "" : "criteriaStepLocked"}`}
                  ref={criteriaMatrixRef}
                >
                  <div className="step-inner-scroll">
                  {manualStep < 3 ? (
                    <div className="criteriaLockText">Hoàn thành bước 2 (CR đạt) để mở so sánh phương án.</div>
                  ) : criteriaInputRows.length ? (
                    <details className="criteriaDataEditDetails">
                      <summary className="criteriaDataEditSummaryInline">
                        <span>Mở bảng chỉnh C1-C4</span>
                        <small className="criteriaSummaryHint">
                          Tùy chọn: bạn có thể chỉnh bảng dữ liệu 13 quận × C1-C4 trước khi tính so sánh phương án.
                        </small>
                      </summary>
                      <table className="ahpMatrixTable ahpDataMatrixTable" style={{ marginTop: 8 }}>
                        <thead>
                          <tr>
                            <th>Phương án (Quận)</th>
                            <th>C1</th>
                            <th>C2</th>
                            <th>C3</th>
                            <th>C4</th>
                          </tr>
                        </thead>
                        <tbody>
                          {criteriaInputRows.map((row) => (
                            <tr key={`dm-${row.DistrictId}`}>
                              <td>{row.DistrictName}</td>
                              {(["C1", "C2", "C3", "C4"] as const).map((key) => (
                                <td key={`dm-${row.DistrictId}-${key}`}>
                                  <input
                                    className="ahpDataInput"
                                    type="number"
                                    step="0.0001"
                                    value={Number(row[key])}
                                    onChange={(e) => updateCriteriaInputCell(row.DistrictId, key, e.target.value)}
                                  />
                                </td>
                              ))}
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </details>
                  ) : (
                    <div className="criteriaEmptyBox">
                      <div className="criteriaEmptyText">Chưa có ma trận dữ liệu C1-C4 cho 13 quận.</div>
                      <div className="btnRow" style={{ marginTop: 8 }}>
                        <button
                          className="btn secondary"
                          disabled={districtCriteriaLoading}
                          onClick={async () => {
                            await loadDistrictCriteriaNow(histDate, false);
                          }}
                        >
                          {districtCriteriaLoading ? "Đang nạp..." : "Nạp C1-C4 theo ngày đang chọn"}
                        </button>
                      </div>
                    </div>
                  )}

                  <div className="criteriaAltToolbar" style={{ marginTop: 8 }}>
                    <div className="ahpCriteriaToggleRow">
                      {AHP_LABELS.map((c) => (
                        <button
                          key={`alt-tab-${c}`}
                          type="button"
                          className={`ahpCriteriaChip ${activeAltCriterion === c ? "active" : ""}`}
                          onClick={() => setActiveAltCriterion(c)}
                        >
                          {c}
                        </button>
                      ))}
                    </div>
                    <div className="criteriaAltActions">
                      <button
                        className="btn altActionBtn altActionBtnLoad"
                        onClick={initAlternativeMatricesFromData}
                        disabled={!selectedCriteriaRows.length}
                      >
                        Nạp ma trận từ C1-C4
                      </button>
                      <button
                        className="btn altActionBtn altActionBtnCalc"
                        onClick={calculateAlternativesStep}
                        disabled={(!criteriaPairResult?.is_consistent && !criteriaForceContinue) || !selectedCriteriaRows.length || altCalcLoading}
                      >
                        {altCalcLoading ? "Đang tính..." : "Tính ma trận phương án"}
                      </button>
                      <button
                        className="btn altActionBtn altActionBtnView"
                        onClick={() => setAltStepModalOpen(true)}
                        disabled={!AHP_LABELS.some((c) => altResults[c])}
                      >
                        Xem chi tiết CR bước 3
                      </button>
                    </div>
                  </div>
                  {criterionFlatState[activeAltCriterion] ? (
                    <div className="criteriaInputNotice" style={{ marginTop: 8 }}>
                      Tiêu chí {activeAltCriterion} của ngày này đang bằng nhau giữa các quận, nên ma trận tự sinh chủ yếu là giá trị 1.
                    </div>
                  ) : null}
                  {criteriaPairResult && !criteriaPairResult.is_consistent && !criteriaForceContinue ? (
                    <div className="ahpErrText" style={{ marginTop: 8 }}>
                      Ma trận tiêu chí chưa đạt CR &lt; 10%, vui lòng nhập lại bước 1 trước khi so sánh phương án.
                    </div>
                  ) : null}

                  {manualStep >= 3 && selectedCriteriaRows.length ? (
                    <details
                      className="criteriaDataEditDetails criteriaAltMatrixDetails"
                      style={{ marginTop: 8 }}
                      ref={altMatrixDetailsRef}
                      open={true}
                    >
                      <summary>Ma trận so sánh phương án ({activeAltCriterion})</summary>
                      <div className="criteriaPairMatrixWrap criteriaAltMatrixWrap" style={{ marginTop: 8 }}>
                        <table className="criteriaPairMatrixTable criteriaAltMatrixTable">
                          <thead>
                            <tr>
                              <th>{activeAltCriterion}</th>
                              {selectedCriteriaRows.map((r) => (
                                <th key={`alt-head-${activeAltCriterion}-${r.DistrictId}`} className="criteriaAltHead">
                                  {r.DistrictName}
                                </th>
                              ))}
                            </tr>
                          </thead>
                          <tbody>
                            {selectedCriteriaRows.map((row, i) => (
                              <tr key={`alt-row-${activeAltCriterion}-${row.DistrictId}`}>
                                <th className="criteriaPairMatrixRowHead criteriaAltHead">{row.DistrictName}</th>
                                {selectedCriteriaRows.map((col, j) => {
                                  if (i === j) {
                                    return (
                                      <td key={`alt-cell-${activeAltCriterion}-${row.DistrictId}-${col.DistrictId}`} className="criteriaPairMatrixDiag">
                                        1
                                      </td>
                                    );
                                  }
                                  if (i < j) {
                                    const value = Number(altMatrices[activeAltCriterion]?.[i]?.[j] ?? 1);
                                    return (
                                      <td key={`alt-cell-${activeAltCriterion}-${row.DistrictId}-${col.DistrictId}`}>
                                        <select
                                          className="criteriaPairMatrixSelect"
                                          value={nearestSaatyRatioValue(value).toFixed(10)}
                                          onChange={(e) => updateAlternativeMatrixCell(activeAltCriterion, i, j, e.target.value)}
                                        >
                                          {SAATY_RATIO_OPTIONS.map((opt) => (
                                            <option key={`ratio-opt-${opt.label}`} value={opt.value.toFixed(10)}>
                                              {opt.label}
                                            </option>
                                          ))}
                                        </select>
                                      </td>
                                    );
                                  }
                                  return (
                                    <td key={`alt-cell-${activeAltCriterion}-${row.DistrictId}-${col.DistrictId}`} className="criteriaPairMatrixAuto">
                                      {formatSaatyFraction(Number(altMatrices[activeAltCriterion]?.[i]?.[j] ?? 1))}
                                    </td>
                                  );
                                })}
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </details>
                  ) : manualStep >= 3 ? (
                    <div className="criteriaEmptyText" style={{ marginTop: 8 }}>
                      Chưa có phương án để so sánh. Hãy nạp C1-C4 trước.
                    </div>
                  ) : null}
                  </div>
                </div>
              ) : null}

              {manualStep === 4 ? (
              <div
                data-criteria-step="4"
                className={`criteriaResultCard criteriaTourTarget criteriaWizardSection ${
                  criteriaTourOpen && criteriaTourStep === 4 ? "criteriaTourFocus tour-focused" : ""
                } criteriaStepPanel criteriaSnapStep criteriaFocusStep ${stepStateClass(4)} ${stepFocusClass(4)} ${manualStep >= 4 ? "" : "criteriaStepLocked"}`}
                ref={criteriaResultRef}
              >
                <div className="step-inner-scroll">
                {manualStep < 4 ? (
                  <div className="criteriaLockText">Hoàn thành bước 3 và bấm “Tính ma trận phương án” để xem kết quả tổng hợp.</div>
                ) : manualFinalRows.length ? (
                  <>
                    <div className="criteriaInputNotice" style={{ marginTop: 8 }}>
                      <b>Kết quả AHP ngày {histDate}</b>
                      <div style={{ marginTop: 4 }}>
                        {criteriaPairResult?.is_consistent ? "CR đạt yêu cầu, ma trận hợp lệ." : "CR chưa đạt yêu cầu, cần kiểm tra lại ma trận."}
                      </div>
                      <div>
                        Trọng số nổi bật: <b>{step4WeightOrderText || "—"}</b>
                      </div>
                      <div>
                        Top 3 quận: <b>{step4Top3DistrictText || "—"}</b>
                      </div>
                      <div>Mở bản đồ để xem phân bố không gian và diễn giải kết quả.</div>
                    </div>
                    <div className="btnRow" style={{ marginTop: 8 }}>
                      <button className="btn criteriaMapWideBtn" onClick={openStep4ResultOnMap}>
                        Mở bản đồ và tô màu theo kết quả này
                      </button>
                    </div>

                    <div className="criteriaChartGrid">
                      <div className="criteriaChartCard">
                        <div className="criteriaMiniTitle">Biểu đồ tròn - Trọng số tiêu chí</div>
                        <div className="criteriaChartBox">
                          <ResponsiveContainer width="100%" height={180}>
                            <PieChart>
                              <Pie data={criteriaWeightChartData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={62} label>
                                {criteriaWeightChartData.map((_, idx) => (
                                  <Cell key={`crit-pie-${idx}`} fill={STEP_COLORS[idx % STEP_COLORS.length]} />
                                ))}
                              </Pie>
                              <RechartsTooltip formatter={(v: any) => Number(v).toFixed(6)} />
                            </PieChart>
                          </ResponsiveContainer>
                        </div>
                      </div>

                      <div className="criteriaChartCard">
                        <div className="criteriaMiniTitle">Biểu đồ cột - Điểm tổng thể phương án</div>
                        <div className="criteriaChartBox">
                          <ResponsiveContainer width="100%" height={180}>
                            <BarChart data={manualBarData}>
                              <XAxis dataKey="name" interval={0} angle={-25} textAnchor="end" height={48} tick={{ fontSize: 10 }} />
                              <YAxis tick={{ fontSize: 10 }} />
                              <RechartsTooltip formatter={(v: any) => Number(v).toFixed(6)} />
                              <Bar dataKey="score" fill="#2563eb" radius={[4, 4, 0, 0]} />
                            </BarChart>
                          </ResponsiveContainer>
                        </div>
                      </div>
                    </div>

                    <div className="ahpResultTableWrap criteriaResultTableWrap" style={{ marginTop: 10 }}>
                      <table>
                        <thead>
                          <tr>
                            <th>Rank</th>
                            <th>Phương án (Quận)</th>
                            <th>Điểm tổng</th>
                            <th>W*C1</th>
                            <th>W*C2</th>
                            <th>W*C3</th>
                            <th>W*C4</th>
                          </tr>
                        </thead>
                        <tbody>
                          {manualFinalRows.map((it) => (
                            <tr key={`manual-final-${it.DistrictId}`}>
                              <td>{it.Rank}</td>
                              <td>{it.DistrictName}</td>
                              <td>{toFixedOrDash(it.Score, 6)}</td>
                              <td>{toFixedOrDash(it.Details.C1, 6)}</td>
                              <td>{toFixedOrDash(it.Details.C2, 6)}</td>
                              <td>{toFixedOrDash(it.Details.C3, 6)}</td>
                              <td>{toFixedOrDash(it.Details.C4, 6)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>

                  </>
                ) : (
                  <div className="criteriaEmptyText">
                    Chưa có kết quả bước 4. Thực hiện lần lượt: <b>bước 1 → bước 2 → bước 3</b>, nếu CR &lt; 10% thì hệ thống sẽ cho ra kết quả tổng hợp.
                  </div>
                )}
                </div>
              </div>
              ) : null}
            </div>

          </div>
          {criteriaTourOverlayPortal}
        </div>
      ) : null}

      {aiDrawerOpen && showAiPanel ? (
        <div
          className="drawerOverlay"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) setAiDrawerOpen(false);
          }}
        >
          <div className="drawer">
            <div className="drawerHeader">
              <div className="drawerTitle">AI (Forecast + Chat)</div>
              <button className="btn secondary" onClick={() => setAiDrawerOpen(false)}>
                Close
              </button>
            </div>

            <div className="drawerBody">
              <div style={{ display: "flex", gap: 10, alignItems: "flex-end" }}>
                <div style={{ flex: 1 }}>
                  <div className="label">Dự báo (6-24h)</div>
                  <input
                    className="input"
                    type="number"
                    min={6}
                    max={24}
                    value={aiHorizon}
                    onChange={(e) => setAiHorizon(Number(e.target.value) || 24)}
                  />
                </div>
                <button className="btn secondary" onClick={runAiForecast} disabled={aiForecastLoading}>
                  {aiForecastLoading ? "Dang chay..." : "Forecast"}
                </button>
              </div>

              {aiForecastErr ? <div style={{ fontSize: 11, color: "#b91c1c", marginTop: 6 }}>{aiForecastErr}</div> : null}

              {aiForecastRes ? (
                <div style={{ marginTop: 10 }}>
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 10, fontSize: 12 }}>
                    <div>
                      Warning: <b>{aiForecastRes.warning ? "YES" : "NO"}</b>
                    </div>
                    <div style={{ color: "#6b7280" }}>
                      max={aiForecastRes.max_risk_score} @ {aiForecastRes.time_of_max}
                    </div>
                  </div>

                  <div style={{ display: "flex", justifyContent: "space-between", gap: 10, fontSize: 12, marginTop: 6 }}>
                    <div style={{ color: "#6b7280" }}>
                      Current:{" "}
                      <b>
                        {aiForecastRes.current_risk_score ?? "?"}{" "}
                        {aiForecastRes.current_level ? `(${aiForecastRes.current_level})` : ""}
                      </b>{" "}
                      {aiForecastRes.current_time ? `@ ${aiForecastRes.current_time}` : ""}
                    </div>
                    <div style={{ color: "#6b7280" }}>
                      Confidence:{" "}
                      <b>
                        {aiForecastRes.confidence_label ?? "?"}{" "}
                        {typeof aiForecastRes.confidence_0_100 === "number" ? `(${aiForecastRes.confidence_0_100})` : ""}
                      </b>
                    </div>
                  </div>

                  <div
                    style={{
                      height: "clamp(160px, 22vh, 220px)",
                      border: "1px solid #e5e7eb",
                      borderRadius: 14,
                      padding: 8,
                      marginTop: 8,
                    }}
                  >
                    <RiskScoreChart
                      forecast={aiForecastRes.series.map((p) => ({ time: p.time, risk_score_0_100: p.risk_score_0_100 }))}
                      baseline={(aiForecastRes.baseline_series || []).map((p: any) => ({
                        time: String(p.time),
                        risk_score_0_100: Number(p.risk_score_0_100),
                      }))}
                    />
                  </div>
                </div>
              ) : null}

              <div style={{ marginTop: 12 }}>
                <AiChatPanel lat={lat} lon={lon} hours={hours} weights={weights} />
              </div>
            </div>
          </div>
        </div>
      ) : null}

      {settingsOpen && (
        <div
          className="modalOverlay"
          onMouseDown={(e) => {
            if (e.target === e.currentTarget) setSettingsOpen(false);
          }}
        >
          <div className="modal">
            <div className="modalHeader">
              <div className="modalTitle">Layout settings</div>
              <button className="btn secondary" onClick={() => setSettingsOpen(false)}>
                Close
              </button>
            </div>

            <div className="modalBody">
              <div className="subtle" style={{ marginBottom: 10 }}>
                Hide/Show panels to make the map wider and reduce clutter.
              </div>

              <div className="modalGrid">
                <div className="toggle">
                  <div className="tLeft">
                    <div className="tTitle">Left panel</div>
                    <div className="tDesc">Location, weights, grid settings</div>
                  </div>
                  <input
                    type="checkbox"
                    checked={showLeftPanel}
                    onChange={(e) => setShowLeftPanel(e.target.checked)}
                  />
                </div>

                <div className="toggle">
                  <div className="tLeft">
                    <div className="tTitle">Right panel</div>
                    <div className="tDesc">Risk, warning, AI, charts</div>
                  </div>
                  <input
                    type="checkbox"
                    checked={showRightPanel}
                    onChange={(e) => setShowRightPanel(e.target.checked)}
                  />
                </div>

                <div className="toggle">
                  <div className="tLeft">
                    <div className="tTitle">AHP weights</div>
                    <div className="tDesc">Show/hide weight inputs</div>
                  </div>
                  <input
                    type="checkbox"
                    checked={showWeightsPanel}
                    onChange={(e) => setShowWeightsPanel(e.target.checked)}
                  />
                </div>

                <div className="toggle">
                  <div className="tLeft">
                    <div className="tTitle">Grid settings</div>
                    <div className="tDesc">Show/hide grid config card</div>
                  </div>
                  <input type="checkbox" checked={showGridPanel} onChange={(e) => setShowGridPanel(e.target.checked)} />
                </div>

                <div className="toggle">
                  <div className="tLeft">
                    <div className="tTitle">AI panel</div>
                    <div className="tDesc">Show/hide AI button (drawer)</div>
                  </div>
                  <input type="checkbox" checked={showAiPanel} onChange={(e) => setShowAiPanel(e.target.checked)} />
                </div>

                <div className="toggle">
                  <div className="tLeft">
                    <div className="tTitle">Early warning</div>
                    <div className="tDesc">Show/hide early warning card</div>
                  </div>
                  <input
                    type="checkbox"
                    checked={showEarlyWarningPanel}
                    onChange={(e) => setShowEarlyWarningPanel(e.target.checked)}
                  />
                </div>

                <div className="toggle">
                  <div className="tLeft">
                    <div className="tTitle">Charts/history</div>
                    <div className="tDesc">Hourly chart + alerts history</div>
                  </div>
                  <input
                    type="checkbox"
                    checked={showRightTabsPanel}
                    onChange={(e) => setShowRightTabsPanel(e.target.checked)}
                  />
                </div>
              </div>

              <div className="btnRow" style={{ marginTop: 12 }}>
                <button
                  className="btn secondary"
                  onClick={() => {
                    setFocusMap(false);
                    setShowLeftPanel(true);
                    setShowRightPanel(true);
                    setShowWeightsPanel(true);
                    setShowGridPanel(true);
                    setShowAiPanel(true);
                    setShowEarlyWarningPanel(true);
                    setShowRightTabsPanel(true);
                  }}
                >
                  Reset
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {topPageMode === "home" ? (
        <Suspense fallback={<div className="simplePage">Đang tải trang chủ 3D...</div>}>
          <VenusLanding
            onOpenMap={openMapWithTransition}
            onOpenCriteria={() => switchTopPage("criteria")}
            totalStations={openaqMapCount + aqicnMapCount + iqairMapCount + purpleAirMapCount}
            scoreText={risk ? Number(risk.score_0_100).toFixed(2) : "—"}
            levelText={risk?.level || "Chưa có"}
            reliabilityText={`${sourceReliability.label} (${sourceReliability.score})`}
          />
        </Suspense>
      ) : null}

      {topPageMode === "news" ? (
        <div className="simplePage">
          <div className="simplePageHead">
            <h2>Tin Tức & Tham khảo dữ liệu</h2>
            <p>Tập hợp nguồn tham khảo để đối chiếu xu hướng chất lượng không khí.</p>
          </div>
          <div className="newsGrid">
            <a className="newsCard" href="https://aqicn.org/map/vietnam/" target="_blank" rel="noreferrer">
              <b>Bản đồ AQICN Việt Nam</b>
              <span>So sánh nhanh chỉ số trạm theo khu vực.</span>
            </a>
            <a className="newsCard" href="https://www.openaq.org/" target="_blank" rel="noreferrer">
              <b>OpenAQ</b>
              <span>Dữ liệu quan trắc mở để kiểm chứng chéo theo thời điểm.</span>
            </a>
            <a className="newsCard" href="https://air-quality-api.open-meteo.com/" target="_blank" rel="noreferrer">
              <b>Open-Meteo Air Quality</b>
              <span>Nguồn mô hình nền phục vụ tính điểm và dự báo.</span>
            </a>
            <a className="newsCard" href="https://www.who.int/news-room/fact-sheets/detail/ambient-(outdoor)-air-quality-and-health" target="_blank" rel="noreferrer">
              <b>WHO - Air quality & health</b>
              <span>Tài liệu chuẩn tham chiếu cho truyền thông rủi ro.</span>
            </a>
          </div>
        </div>
      ) : null}

      {topPageMode === "system" ? (
        <div className="simplePage">
          <div className="simplePageHead">
            <h2>Hệ thống</h2>
            <p>Trạng thái nhanh và thao tác điều hướng.</p>
          </div>
          <div className="systemGrid">
            <div className="systemCard">
              <span>OpenAQ</span>
              <b>{openaqMapCount} trạm</b>
            </div>
            <div className="systemCard">
              <span>AQICN</span>
              <b>{aqicnMapCount} trạm</b>
            </div>
            <div className="systemCard">
              <span>IQAir</span>
              <b>{iqairMapCount} trạm</b>
            </div>
            <div className="systemCard">
              <span>PurpleAir</span>
              <b>{purpleAirMapCount} trạm</b>
            </div>
          </div>
          <div className="landingActionRow">
            <button className="btn" onClick={openMapWithTransition}>Mở Dữ liệu bản đồ</button>
            <button className="btn secondary" onClick={() => setSettingsOpen(true)}>Cài đặt layout</button>
          </div>
        </div>
      ) : null}

      {topPageMode === "map" && err ? (
        <div className="errorBox" style={{ marginTop: 10 }}>
          <b>Lỗi:</b>
          <pre style={{ margin: "6px 0 0", whiteSpace: "pre-wrap" }}>
            {typeof err === "string" ? err : JSON.stringify(err, null, 2)}
          </pre>
        </div>
      ) : null}

      {topPageMode === "map" ? (
      <div
        className={`main ${layoutClass} ${showLeftPanel ? "has-left" : "no-left"} ${showRightPanel ? "has-right" : "no-right"} map-focus-layout ${mapPageTransition === "entering" ? "map-main-enter" : ""}`}
      >
        {/* ===== TRÁI ===== */}
        {!focusMap && showLeftPanel ? (
        <div className="left">
          <div className="leftScroll">
            <div className="card leftHeroCard">
              <div className="cardTitle">Vị trí, nguồn dữ liệu & trạm</div>
              <div className="leftAirHero">
                <div className="leftAirHeroTop">
                  <div className="leftAirTopMain">
                    <div className="leftAirCaption">Không khí tại vị trí đang chọn</div>
                    <div className="leftAirScoreLine">
                      <span className="leftAirScore">{risk ? Number(risk.score_0_100).toFixed(1) : "--"}</span>
                      <span className="leftAirLevelInline">- {risk?.level || "Chua chay DSS"}</span>
                    </div>
                  </div>
                  <div className="leftAirTopRight">
                    <span className="leftAirStatusTag">Live</span>
                    <div className="leftAirAddressInline">
                      <AddressSearch
                        disabled={false}
                        compact
                        hideHint
                        value={addrText}
                        onValueChange={setAddrText}
                        placeholder="Tìm địa chỉ TP.HCM..."
                        onPick={(newLat, newLon, label) => {
                          setAddrLabel(label || null);
                          setAddrText(label || "");
                          const nLat = Number(newLat.toFixed(6));
                          const nLon = Number(newLon.toFixed(6));
                          setLat(nLat);
                          setLon(nLon);

                          if (source === "station") {
                            loadStationsAroundPoint(nLat, nLon).catch(() => {});
                          }
                        }}
                      />
                      {addrLabel ? (
                        <div className="leftAirAddressChosen" title={addrLabel}>
                          {addrLabel}
                          {addrLoading ? <span> (đang tìm...)</span> : null}
                        </div>
                      ) : null}
                    </div>
                  </div>
                </div>

                <div className="leftAirMetaGrid">
                  <div className="leftAirMetaItem">
                    <span>Toa do:</span>
                    <b>{lat.toFixed(4)}, {lon.toFixed(4)}</b>
                  </div>
                  <div className="leftAirMetaItem">
                    <span>Chi so chinh:</span>
                    <b>{topFactor ? `${topFactor.label}: ${topFactor.value}` : "Dang cho du lieu"}</b>
                  </div>
                  <div className="leftAirMetaItem">
                    <span>Khoang phan tich:</span>
                    <b>{hours} gio</b>
                  </div>
                  <div className="leftAirMetaItem">
                    <span>Tram tren ban do:</span>
                    <b>Hien thi tu dong</b>
                  </div>
                </div>
                <div className="criteriaInputNotice" style={{ marginTop: 8 }}>
                  {currentDistrictDecisionContext ? (
                    <>
                      Quận hiện tại: <b>{currentDistrictDecisionContext.districtName}</b> · Hạng AHP:{" "}
                      <b>
                        {currentDistrictDecisionContext.rank}/{currentDistrictDecisionContext.total}
                      </b>{" "}
                      · Ưu tiên AHP: <b>{currentDistrictDecisionContext.priorityLabel}</b>
                    </>
                  ) : (
                    "Chưa chọn quận trên bản đồ để xem thông tin AHP."
                  )}
                </div>
              </div>
            </div>

          </div>
        </div>

        ) : null}

        <div className="center">
          <div className="card mapCard">
            {districtResultPaint ? (
              <div className="criteriaMapJumpBar compact">
                <span>
                  <b>{mapStatusText || districtResultPaint.label}</b>
                </span>
                <button
                  className="btn secondary"
                  onClick={() => {
                    setDistrictResultPaint(null);
                    setSelectedMapDistrict("");
                  }}
                >
                  Bỏ tô màu kết quả
                </button>
              </div>
            ) : null}

            <div className="mapFrame">
              <div className="legendOverlay">
                <div className="legendOverlayHead">
                  <div style={{ fontWeight: 900, fontSize: 12 }}>Chú giải (Risk 0-100)</div>
                  <button
                    type="button"
                    className="mapOverlayToggleBtn"
                    onClick={() => setMapLegendOpen((v) => !v)}
                    title={mapLegendOpen ? "Thu gon chu giai" : "Mo rong chu giai"}
                  >
                    {mapLegendOpen ? "−" : "+"}
                  </button>
                </div>

                {mapLegendOpen ? (
                  <>
                    <div className="legendScale">
                      <div className="legendBar" />
                      <div className="legendTicks">
                        <span>0</span>
                        <span>25</span>
                        <span>50</span>
                        <span>75</span>
                        <span>100</span>
                      </div>
                      <div className="legendHint">Xanh - Vàng - Cam - Đỏ</div>
                    </div>

                    {gridLoading ? <div className="legendStatus">Đang quét lưới...</div> : null}
                    {gridErr ? <div className="legendErr">{gridErr}</div> : null}
                  </>
                ) : (
                  <div className="legendCollapsed">0-100 · Xanh-Vàng-Cam-Đỏ</div>
                )}
              </div>

              <MapPicker
                lat={lat}
                lon={lon}
                level={risk?.level}
                grid={grid}
                scanOn={quetKhuVuc}
                scanKm={gridKm}
                onOpenAQStationPick={inspectOpenAQStation}
                selectedOpenAQStationId={selectedOpenAQStation?.id ?? null}
                onOpenAQStationsCountChange={setOpenaqMapCount}
                onAQICNStationsCountChange={setAqicnMapCount}
                onIQAirStationsCountChange={setIqairMapCount}
                onPurpleAirStationsCountChange={setPurpleAirMapCount}
                onSourcesStatusChange={(s) => setMapSourceStatuses(s)}
                districtResultItems={districtResultPaint?.rows || null}
                onDistrictPick={(districtName) => setSelectedMapDistrict(String(districtName || "").trim())}
                onPick={(newLat, newLon) => {
                  if (source === "station") {
                    // station mode: click map để đổi vùng quét trạm
                    const nLat = Number(newLat.toFixed(6));
                    const nLon = Number(newLon.toFixed(6));
                    setLat(nLat);
                    setLon(nLon);
                    reverseLookupAddress(nLat, nLon).catch(() => {});
                    loadStationsAroundPoint(nLat, nLon).catch(() => {});
                    return;
                  }

                  const nLat = Number(newLat.toFixed(6));
                  const nLon = Number(newLon.toFixed(6));
                  setLat(nLat);
                  setLon(nLon);
                  reverseLookupAddress(nLat, nLon).catch(() => {});
                }}
              />
            </div>
          </div>
        </div>

        {/* ===== PHẢI ===== */}
        {!focusMap && showRightPanel ? (
        <div className="right">
          <div className="rightScroll">
          <div className={`card decisionCard tone-${decisionSnapshot.tone}`}>
            <div className="cardTitle">Kết luận nhanh cho người ra quyết định</div>
            <div className="decisionHeadline">{decisionSnapshot.headline}</div>
            <div className="decisionMetaRow">
              <span>Điểm DSS: <b>{decisionSnapshot.scoreText}</b></span>
              <span>Mức: <b>{decisionSnapshot.levelText}</b></span>
              {districtResultPaint ? (
                <span>
                  Ưu tiên AHP: <b>{currentDistrictDecisionContext?.priorityLabel || "Chưa xác định"}</b>
                </span>
              ) : null}
            </div>
            <div className="decisionReason">{decisionSnapshot.reason}</div>
            {quickDecisionMessage ? (
              <div className="criteriaInputNotice" style={{ marginBottom: 8 }}>
                {quickDecisionMessage}
                {currentDistrictDecisionContext ? (
                  <span>
                    {" "}
                    (Hạng: <b>#{currentDistrictDecisionContext.rank}</b>, Điểm:{" "}
                    <b>{toFixedOrDash(currentDistrictDecisionContext.score, 6)}</b>)
                  </span>
                ) : null}
              </div>
            ) : null}
            <ul className="decisionList">
              <li>{decisionSnapshot.action}</li>
              <li>{decisionSnapshot.warningLine}</li>
              <li>{decisionSnapshot.sourceLine}</li>
            </ul>
          </div>

          <div className="card" style={{ marginTop: 6 }}>
            <div className="cardTitle">Tóm tắt nguồn & độ tin cậy</div>
            <div className="decisionMetaRow" style={{ marginBottom: 6 }}>
              <span>
                Tọa độ: <b>{lat.toFixed(4)}, {lon.toFixed(4)}</b>
              </span>
              <span>
                Địa chỉ: <b>{addrLabel || "Chưa chọn"}</b>
              </span>
            </div>
            <div style={{ fontSize: 12, color: "#4b5563" }}>
              Dữ liệu dùng để diễn giải bản đồ hiện tại:
            </div>
            <div style={{ fontSize: 12, color: "#4b5563", marginTop: 4 }}>
              Nguồn trạm trên bản đồ: OpenAQ <b>{openaqMapCount}</b>, AQICN <b>{aqicnMapCount}</b>, IQAir <b>{iqairMapCount}</b>,
              PurpleAir <b>{purpleAirMapCount}</b> (Tổng marker: <b>{openaqMapCount + aqicnMapCount + iqairMapCount + purpleAirMapCount}</b>).
            </div>
            <div style={{ fontSize: 12, color: "#4b5563", marginTop: 4 }}>
              Độ tin cậy tổng hợp: <b>{sourceReliability.label}</b> ({sourceReliability.score}).
            </div>
          </div>

          <div className="card" style={{ marginTop: 6 }}>
            <div className="cardTitle">Quận đang chọn theo AHP</div>
            {currentDistrictDecisionContext ? (
              <div style={{ display: "grid", gap: 6, fontSize: 12, color: "#374151" }}>
                <div>
                  Quận: <b>{currentDistrictDecisionContext.districtName}</b>
                </div>
                <div>
                  Hạng AHP: <b>{currentDistrictDecisionContext.rank}/{currentDistrictDecisionContext.total}</b>
                </div>
                <div>
                  Điểm tổng: <b>{toFixedOrDash(currentDistrictDecisionContext.score, 6)}</b>
                </div>
                <div>
                  Ưu tiên AHP: <b>{currentDistrictDecisionContext.priorityLabel}</b>
                </div>
                <div>
                  Tiêu chí nổi bật: <b>{selectedDistrictTopCriteriaText || "Chưa có dữ liệu C1-C4 chi tiết"}</b>
                </div>
                <div>
                  Khuyến nghị: <b>{selectedDistrictRecommendation}</b>
                </div>
                {selectedDistrictScenarioContext ? (
                  <div>
                    Hạng kịch bản: <b>{selectedDistrictScenarioContext.scenarioRank}</b> · Đổi hạng:{" "}
                    <b>{Number(selectedDistrictScenarioContext.rankDelta || 0) > 0 ? "+" : ""}{selectedDistrictScenarioContext.rankDelta}</b>
                  </div>
                ) : null}
              </div>
            ) : (
              <div style={{ fontSize: 12, color: "#6b7280" }}>
                Bấm vào quận trên bản đồ để xem hạng AHP và khuyến nghị.
              </div>
            )}
          </div>

          {/* Cảnh báo sớm */}
          {showEarlyWarningPanel ? (
            <div className="card">
              <div className="cardTitle">Cảnh báo sớm</div>
              <div className="decisionMetaRow" style={{ marginBottom: 8 }}>
                <span>
                  Trạng thái: <b>{warning?.warning ? "Có tín hiệu cần chú ý" : "Chưa kích hoạt"}</b>
                </span>
                <span>
                  maxScore: <b>{warning?.maxScore !== undefined ? Number(warning.maxScore).toFixed(1) : "—"}</b>
                </span>
                <span>
                  Mức dự báo cao nhất: <b>{warning?.maxLevel || "—"}</b>
                </span>
              </div>
              <div style={{ fontSize: 12, color: "#4b5563", marginBottom: 8 }}>{earlyWarningSummaryText}</div>
              <button className="btn secondary" onClick={() => setShowEarlyWarningDetails((v) => !v)}>
                {showEarlyWarningDetails ? "Ẩn chi tiết" : "Xem chi tiết"}
              </button>
              {showEarlyWarningDetails ? (
                <div style={{ marginTop: 8 }}>
                  <EarlyWarningCard
                    data={warning}
                    loading={dangChay}
                    error={null}
                    onCheck={() => chayDSS({ isManual: true })}
                  />
                </div>
              ) : null}
            </div>
          ) : null}

          <div className="ahpPlaygroundCard" style={{ marginTop: 6 }}>
            <div className="ahpPlaygroundHead">
              <div className="ahpPlaygroundTitle">Chính sách quyết định / Kịch bản</div>
              <div className="ahpPlaygroundSub">
                Chọn một kịch bản để xem thứ hạng quận thay đổi như thế nào so với cấu hình mặc định của hệ thống.
              </div>
            </div>

            <div style={{ padding: "0 10px 10px", display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
              <button className="btn secondary" onClick={() => setShowScenarioAdvanced((v) => !v)}>
                {showScenarioAdvanced ? "Ẩn phân tích nâng cao" : "Phân tích nâng cao"}
              </button>
              {showScenarioAdvanced ? (
                <button className="btn secondary" onClick={() => setShowScenarioGuide((v) => !v)}>
                  {showScenarioGuide ? "Ẩn hướng dẫn" : "Xem hướng dẫn"}
                </button>
              ) : null}
              <span style={{ fontSize: 12, color: "#6b7280" }}>
                Scenario là lớp bổ sung để so sánh ưu tiên theo chính sách.
              </span>
            </div>
            {showScenarioAdvanced ? (
            <div style={{ display: "grid", gap: 8, padding: "0 10px 10px" }}>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "center" }}>
                <label style={{ fontSize: 12, color: "#374151" }}>
                  Kịch bản:&nbsp;
                  <select
                    className="input"
                    style={{ width: 170 }}
                    value={scenarioPresetName}
                    onChange={(e) => applyScenarioPreset(e.target.value as ScenarioPresetName)}
                  >
                    {POLICY_SCENARIO_PRESETS.map((it) => (
                      <option key={it.id} value={it.id}>
                        {it.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label style={{ fontSize: 12, color: "#374151", display: "flex", alignItems: "center", gap: 6 }}>
                  <input
                    type="checkbox"
                    checked={scenarioUseCustomWeights}
                    onChange={(e) => setScenarioUseCustomWeights(e.target.checked)}
                  />
                  Tự chỉnh trọng số C1-C4
                </label>
                <label style={{ fontSize: 12, color: "#374151", display: "flex", alignItems: "center", gap: 6 }}>
                  <input
                    type="checkbox"
                    checked={scenarioEarlyWarningEnabled}
                    onChange={(e) => setScenarioEarlyWarningEnabled(e.target.checked)}
                  />
                  Bật cảnh báo sớm
                </label>
                <label style={{ fontSize: 12, color: "#374151" }}>
                  Top ưu tiên:&nbsp;
                  <input
                    className="input"
                    type="number"
                    min={1}
                    max={13}
                    style={{ width: 72 }}
                    value={scenarioTopN}
                    onChange={(e) => setScenarioTopN(Number(e.target.value) || 5)}
                  />
                </label>
              </div>
              {showScenarioGuide ? (
                <>
                  <div className="criteriaInputNotice" style={{ marginTop: 2 }}>
                    <b>Bạn đang làm gì ở đây?</b>
                    <div style={{ marginTop: 4 }}>
                      Chọn một kịch bản để xem quận nào sẽ được ưu tiên hơn so với cấu hình mặc định, từ đó quyết định nên theo dõi
                      khu vực nào trước.
                    </div>
                  </div>
                  <div className="criteriaInputNotice" style={{ marginTop: 2 }}>
                    <b>Cách dùng nhanh</b>
                    <div style={{ marginTop: 4 }}>Bước 1. Chọn Kịch bản hoặc tự chỉnh trọng số C1-C4.</div>
                    <div>Bước 2. Bấm “So sánh gốc vs kịch bản” để xem quận nào tăng hoặc giảm ưu tiên.</div>
                    <div>Bước 3. Dựa vào các cột “Ưu tiên theo kịch bản”, “Mức cảnh báo”, “Khuyến nghị” để chọn quận nên xem trước.</div>
                  </div>
                </>
              ) : null}
              <div className="criteriaInputNotice" style={{ marginTop: 2 }}>
                {scenarioPresetDescription}
              </div>

              {scenarioUseCustomWeights ? (
                <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "flex-end" }}>
                  {AHP_LABELS.map((c) => (
                    <label key={`scenario-weight-${c}`} style={{ fontSize: 12, color: "#374151" }}>
                      {c}
                      <input
                        className="input"
                        type="number"
                        step="0.01"
                        min={0}
                        style={{ width: 92 }}
                        value={scenarioCustomWeights[c]}
                        onChange={(e) => updateScenarioWeightCell(c, e.target.value)}
                      />
                    </label>
                  ))}
                  <div style={{ fontSize: 12, color: "#6b7280", paddingBottom: 8 }}>
                    Tổng trọng số: <b>{scenarioWeightSum.toFixed(6)}</b>
                  </div>
                </div>
              ) : (
                <div className="criteriaInputNotice" style={{ marginTop: 2 }}>
                  Trọng số kịch bản:{" "}
                  <b>
                    C1={POLICY_SCENARIO_PRESETS.find((x) => x.id === scenarioPresetName)?.weights.C1 ?? 0}, C2=
                    {POLICY_SCENARIO_PRESETS.find((x) => x.id === scenarioPresetName)?.weights.C2 ?? 0}, C3=
                    {POLICY_SCENARIO_PRESETS.find((x) => x.id === scenarioPresetName)?.weights.C3 ?? 0}, C4=
                    {POLICY_SCENARIO_PRESETS.find((x) => x.id === scenarioPresetName)?.weights.C4 ?? 0}
                  </b>
                </div>
              )}

              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", alignItems: "flex-end" }}>
                <label style={{ fontSize: 12, color: "#374151" }}>
                  Vàng
                  <input
                    className="input"
                    type="number"
                    min={0}
                    max={1}
                    step="0.01"
                    style={{ width: 92 }}
                    value={scenarioThresholds.yellow}
                    onChange={(e) => updateScenarioThresholdCell("yellow", e.target.value)}
                  />
                </label>
                <label style={{ fontSize: 12, color: "#374151" }}>
                  Cam
                  <input
                    className="input"
                    type="number"
                    min={0}
                    max={1}
                    step="0.01"
                    style={{ width: 92 }}
                    value={scenarioThresholds.orange}
                    onChange={(e) => updateScenarioThresholdCell("orange", e.target.value)}
                  />
                </label>
                <label style={{ fontSize: 12, color: "#374151" }}>
                  Đỏ
                  <input
                    className="input"
                    type="number"
                    min={0}
                    max={1}
                    step="0.01"
                    style={{ width: 92 }}
                    value={scenarioThresholds.red}
                    onChange={(e) => updateScenarioThresholdCell("red", e.target.value)}
                  />
                </label>
                <span style={{ fontSize: 12, color: "#6b7280", paddingBottom: 8 }}>
                  Mặc định: 0.45 / 0.65 / 0.80
                </span>
              </div>

              <div className="btnRow">
                <button
                  className="btn secondary"
                  title="Xem thứ hạng quận thay đổi thế nào khi đổi cách ưu tiên."
                  onClick={() => runPolicyScenarioNow(histDate)}
                  disabled={scenarioLoading}
                >
                  {scenarioLoading ? "Đang chạy kịch bản..." : "So sánh gốc vs kịch bản"}
                </button>
                <button
                  className="btn"
                  title="Hiển thị các quận nổi bật theo kịch bản đang chọn."
                  onClick={openScenarioResultOnMap}
                  disabled={!scenarioResult?.scenarioResult?.length}
                >
                  Tô bản đồ theo kịch bản
                </button>
              </div>

              {scenarioErr ? <div className="ahpErrText">Lỗi kịch bản: {scenarioErr}</div> : null}
              {scenarioResult ? (
                <>
                  <div className="criteriaInputNotice" style={{ marginTop: 4 }}>
                    Top mặc định: <b>{scenarioResult.summary?.baselineTopDistrict?.districtName || "—"}</b> · Top theo kịch bản:{" "}
                    <b>{scenarioResult.summary?.scenarioTopDistrict?.districtName || "—"}</b>
                  </div>
                  <div className="criteriaInputNotice">
                    Thay đổi mạnh nhất:{" "}
                    <b>
                      {scenarioStrongestShift?.districtName || "—"}{" "}
                      {scenarioStrongestShift ? `(${formatRankShiftForHuman(Number(scenarioStrongestShift.rankDelta || 0))})` : ""}
                    </b>
                    {" · "}
                    Tăng/Giảm/Giữ nguyên:{" "}
                    <b>
                      {scenarioResult.summary?.upPriorityCount ?? 0} / {scenarioResult.summary?.downPriorityCount ?? 0} /{" "}
                      {scenarioResult.summary?.stablePriorityCount ?? 0}
                    </b>
                  </div>
                  <div className="criteriaInputNotice">
                    Kịch bản hiện tại ưu tiên: <b>{scenarioPresetPriorityText}</b>
                    {" · "}
                    Nên xem trước: <b>{scenarioWatchList.length ? scenarioWatchList.join(", ") : "—"}</b>
                    {scenarioDataBiasText ? ` · ${scenarioDataBiasText}` : ""}
                  </div>
                  <div style={{ fontSize: 11, color: "#6b7280", marginTop: 2 }}>
                    Mức cảnh báo phản ánh hiện trạng ô nhiễm; ưu tiên theo kịch bản phản ánh mục tiêu ra quyết định đang chọn.
                  </div>
                  <div className="ahpResultTableWrap" style={{ marginTop: 8 }}>
                    <table>
                      <thead>
                        <tr>
                          <th>Quận</th>
                          <th title="Thứ hạng theo cấu hình mặc định hiện tại">Hạng gốc</th>
                          <th title="Thứ hạng theo cấu hình kịch bản đang chọn">Hạng kịch bản</th>
                          <th title="Mức ưu tiên theo thứ hạng kịch bản (Top-N)">Ưu tiên theo kịch bản</th>
                          <th title="Âm = ưu tiên tăng, dương = ưu tiên giảm">Đổi hạng</th>
                          <th title="Điểm theo cấu hình mặc định">Điểm gốc</th>
                          <th title="Điểm theo kịch bản">Điểm kịch bản</th>
                          <th title="Chênh lệch điểm giữa kịch bản và gốc">Chênh lệch điểm</th>
                          <th title="Mức cảnh báo môi trường trước và sau khi áp kịch bản">Mức cảnh báo</th>
                          <th>Loại rủi ro</th>
                          <th>Khuyến nghị</th>
                          <th title="Giải thích nhanh vì sao thứ hạng thay đổi">Lý do thay đổi</th>
                        </tr>
                      </thead>
                      <tbody>
                        {scenarioCompareItems.map((it) => {
                          const deltaRank = Number(it.rankDelta || 0);
                          const deltaScore = Number(it.scoreDelta || 0);
                          const rankTone = deltaRank < 0 ? "#065f46" : deltaRank > 0 ? "#991b1b" : "#374151";
                          const scoreTone = deltaScore > 0 ? "#991b1b" : deltaScore < 0 ? "#065f46" : "#374151";
                          const scenarioPriorityLabel = scenarioPriorityLabelByRank(Number(it.scenarioRank || 0), scenarioAppliedTopN);
                          const priorityTone =
                            scenarioPriorityLabel === "Rất cao"
                              ? "#991b1b"
                              : scenarioPriorityLabel === "Cao"
                                ? "#b45309"
                                : scenarioPriorityLabel === "Trung bình"
                                  ? "#1d4ed8"
                                  : "#374151";
                          return (
                            <tr key={`scenario-cmp-${it.districtId}`}>
                              <td>{it.districtName}</td>
                              <td>{it.baselineRank}</td>
                              <td>{it.scenarioRank}</td>
                              <td title={`Ưu tiên theo thứ hạng trong Top ${scenarioAppliedTopN}`} style={{ color: priorityTone, fontWeight: 700 }}>
                                {scenarioPriorityLabel}
                              </td>
                              <td style={{ color: rankTone, fontWeight: 700 }}>
                                {deltaRank > 0 ? `+${deltaRank}` : deltaRank}
                              </td>
                              <td>{toFixedOrDash(it.baselineScore, 6)}</td>
                              <td>{toFixedOrDash(it.scenarioScore, 6)}</td>
                              <td style={{ color: scoreTone, fontWeight: 700 }}>
                                {deltaScore > 0 ? "+" : ""}
                                {toFixedOrDash(deltaScore, 6)}
                              </td>
                              <td>
                                <span className="badge" style={levelStyle(it.baselineLevel)}>
                                  {it.baselineLevel}
                                </span>{" "}
                                →{" "}
                                <span className="badge" style={levelStyle(it.scenarioLevel)}>
                                  {it.scenarioLevel}
                                </span>
                              </td>
                              <td>{it.riskType}</td>
                              <td title={it.explanation || it.recommendation}>
                                {compactScenarioRecommendation(
                                  String(it.scenarioLevel || ""),
                                  String(it.recommendation || ""),
                                  Boolean(it.earlyWarning),
                                  Number(it.scenarioRank || 0),
                                  scenarioAppliedTopN,
                                  deltaRank
                                )}
                              </td>
                              <td
                                title={
                                  it.rankChangeReason
                                    ? `${it.rankChangeReason}. ${it.explanation || ""}`.trim()
                                    : it.explanation || ""
                                }
                              >
                                {buildScenarioReasonHint(
                                  scenarioPresetName,
                                  scenarioRowsByDistrictId.get(Number(it.districtId))?.criteriaValues,
                                  deltaRank,
                                  Number(it.scenarioRank || 0),
                                  scenarioAppliedTopN
                                ) || it.rankChangeReason || "Điều chỉnh theo trọng số/ngưỡng kịch bản"}
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                </>
              ) : (
                <div style={{ fontSize: 12, color: "#6b7280" }}>
                  Chưa chạy scenario. Baseline cũ vẫn hoạt động độc lập như trước.
                </div>
              )}
            </div>
            ) : (
              <div style={{ fontSize: 12, color: "#6b7280", padding: "0 10px 10px" }}>
                Scenario đang thu gọn để ưu tiên luồng quyết định chính. Bấm “Phân tích nâng cao” để mở.
              </div>
            )}
          </div>

          {showRightTabsPanel ? (
          <details className="card collapsible rightTabs rightTabsPrimary" open>
            <summary className="cardTitle">Charts / History</summary>
            <div className="collapsibleBody">
            <div className="tabs">
              <button className={`tab ${tab === "hourly" ? "active" : ""}`} onClick={() => setTab("hourly")}>
                Biểu đồ theo giờ
              </button>
              <button className={`tab ${tab === "alerts" ? "active" : ""}`} onClick={() => setTab("alerts")}>
                Lịch sử cảnh báo
              </button>
              <button className={`tab ${tab === "districts" ? "active" : ""}`} onClick={() => setTab("districts")}>
                13 quận (theo ngày)
              </button>
            </div>

            <div className="tabBody">
              {tab === "hourly" ? (
                <div className="chartBox">
                  {dangChay && !hourly ? (
                    <div style={{ color: "#6b7280", fontSize: 12 }}>Đang tải dữ liệu biểu đồ...</div>
                  ) : (
                    <div style={{ height: 220, width: "100%" }}>
                      <HourlyChart hourly={hourly} />
                    </div>
                  )}
                </div>
              ) : tab === "alerts" ? (
                <div className="tableWrap alertsTableWrap">
                  {!alerts ? (
                    <div style={{ padding: 10, fontSize: 12 }}>Đang tải...</div>
                  ) : alerts.items?.length === 0 ? (
                    <div style={{ padding: 10, fontSize: 12, color: "#6b7280" }}>Chưa có bản ghi.</div>
                  ) : (
                    <table>
                      <thead>
                        <tr>
                          <th>Id</th>
                          <th>Thời gian</th>
                          <th>Lat</th>
                          <th>Lon</th>
                          <th>Điểm</th>
                          <th>Mức</th>
                        </tr>
                      </thead>
                      <tbody>
                        {alerts.items.map((it: any) => {
                          const st = levelStyle(it.Level);
                          return (
                            <tr key={it.Id}>
                              <td>{it.Id}</td>
                              <td>{it.CreatedAt}</td>
                              <td>{it.Lat}</td>
                              <td>{it.Lon}</td>
                              <td>{it.Score}</td>
                              <td>
                                <span className="badge" style={{ color: st.color, background: st.bg }}>
                                  {it.Level}
                                </span>
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  )}
                </div>
              ) : (
                <div className="tableWrap">
                  <div style={{ display: "flex", gap: 8, alignItems: "center", padding: 10, flexWrap: "wrap" }}>
                    <label style={{ fontSize: 12, color: "#374151" }}>
                      Ngày:&nbsp;
                      <input
                        type="date"
                        value={histDate}
                        onChange={(e) => onHistDateChange(e.target.value)}
                        style={{ padding: "6px 8px", borderRadius: 10, border: "1px solid #e5e7eb" }}
                      />
                    </label>
                    <button className="btn" onClick={() => loadDistrictDaily(histDate)} disabled={districtDailyLoading}>
                      Xem
                    </button>
                    <button className="btn" onClick={() => refreshDistrictDailyNow(histDate)} disabled={districtDailyLoading}>
                      Lấy từ Open-Meteo & lưu
                    </button>
                    <button className="btn secondary" onClick={() => loadDistrictCriteriaNow(histDate, false)} disabled={districtCriteriaLoading}>
                      Tải C1-C4
                    </button>
                    <button className="btn secondary" onClick={() => runDistrictAhpNow(histDate)} disabled={ahpLoading}>
                      {ahpLoading ? "Đang tính AHP..." : "Tính AHP (4 tiêu chí)"}
                    </button>
                    {districtDailyLoading ? <span style={{ fontSize: 12, color: "#6b7280" }}>Đang xử lý...</span> : null}
                    {districtCriteriaLoading ? <span style={{ fontSize: 12, color: "#6b7280" }}>Đang tải C1-C4...</span> : null}
                  </div>

                  {districtDailyErr ? (
                    <div style={{ padding: 10, fontSize: 12, color: "#b91c1c" }}>{districtDailyErr}</div>
                  ) : !districtDaily ? (
                    <div style={{ padding: 10, fontSize: 12, color: "#6b7280" }}>
                      Chưa có dữ liệu. Bấm "Xem" để tải từ CSDL hoặc "Lấy từ Open-Meteo & lưu" để tạo dữ liệu cho ngày này.
                    </div>
                  ) : districtDaily.items?.length === 0 ? (
                    <div style={{ padding: 10, fontSize: 12, color: "#6b7280" }}>Không có bản ghi cho ngày này.</div>
                  ) : (
                    <table>
                      <thead>
                        <tr>
                          <th>Quận</th>
                          <th>PM2.5</th>
                          <th>PM10</th>
                          <th>NO2</th>
                          <th>O3</th>
                          <th>CO</th>
                          <th>Hours</th>
                        </tr>
                      </thead>
                      <tbody>
                        {districtDaily.items.map((it) => (
                          <tr key={it.DistrictId}>
                            <td>{it.DistrictName}</td>
                            <td>{it.PM25 ?? "—"}</td>
                            <td>{it.PM10 ?? "—"}</td>
                            <td>{it.NO2 ?? "—"}</td>
                            <td>{it.O3 ?? "—"}</td>
                            <td>{it.CO ?? "—"}</td>
                            <td>{it.HoursCount ?? "—"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}

                  <div className="ahpPlaygroundCard">
                    <div className="ahpPlaygroundHead">
                      <div className="ahpPlaygroundTitle">Đánh giá AHP theo 13 quận</div>
                      <div className="ahpPlaygroundSub">Giao diện rút gọn: xem nhanh kết quả trước, tùy chỉnh nâng cao nằm ở phần mở rộng.</div>
                    </div>

                    {districtCriteriaErr ? <div className="ahpErrText">Lỗi C1-C4: {districtCriteriaErr}</div> : null}
                    {districtCriteriaInfo ? <div className="criteriaInputNotice">{districtCriteriaInfo}</div> : null}
                    {ahpErr ? <div className="ahpErrText">Lỗi AHP: {ahpErr}</div> : null}

                    {ahpResult ? (
                      <>
                        <div className="ahpMetaRow">
                          <span>CR: <b>{Number(ahpResult.ahp.CR).toFixed(4)}</b></span>
                          <span>CI: <b>{Number(ahpResult.ahp.CI).toFixed(4)}</b></span>
                          <span>Nhất quán: <b>{ahpResult.ahp.is_consistent ? "Đạt" : "Chưa đạt"}</b></span>
                          <span>Số phương án đang xem: <b>{ahpInteractiveRows.length}</b></span>
                        </div>
                        <div className="ahpResultTableWrap">
                          <table>
                            <thead>
                              <tr>
                                <th>Rank</th>
                                <th>Quận</th>
                                <th>Score</th>
                                <th>C1</th>
                                <th>C2</th>
                                <th>C3</th>
                                <th>C4</th>
                              </tr>
                            </thead>
                            <tbody>
                              {ahpInteractiveRows.map((it: any) => (
                                <tr key={`${it.Date}-${it.DistrictId}`}>
                                  <td>{it.InteractiveRank}</td>
                                  <td>{it.DistrictName}</td>
                                  <td>{Number(it.InteractiveScore).toFixed(6)}</td>
                                  <td>{it.C1}</td>
                                  <td>{it.C2}</td>
                                  <td>{it.C3}</td>
                                  <td>{it.C4}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>

                        <details className="ahpAdvancedDetails">
                          <summary>Tùy chỉnh nâng cao (tiêu chí, quận, ma trận)</summary>
                          <div className="ahpAdvancedBody">
                            <div className="ahpCriteriaToggleRow">
                              {AHP_LABELS.map((c) => (
                                <button
                                  key={c}
                                  type="button"
                                  className={`ahpCriteriaChip ${activeCriteria[c] ? "active" : ""}`}
                                  onClick={() => setActiveCriteria((prev) => ({ ...prev, [c]: !prev[c] }))}
                                  title={`Bật/tắt ${c}`}
                                >
                                  {c}
                                </button>
                              ))}
                              <button
                                type="button"
                                className="btn secondary"
                                onClick={() =>
                                  setActiveCriteria({
                                    C1: true,
                                    C2: true,
                                    C3: true,
                                    C4: true,
                                  })
                                }
                              >
                                Bật cả 4 tiêu chí
                              </button>
                            </div>

                            <div className="ahpDistrictRow">
                              <div className="ahpDistrictTools">
                                <button type="button" className="btn secondary" onClick={() => setSelectedDistrictIds(districtOptions.map((d) => d.id))}>
                                  Chọn đủ 13 quận
                                </button>
                                <button type="button" className="btn secondary" onClick={() => setSelectedDistrictIds([])}>
                                  Bỏ chọn tất cả
                                </button>
                              </div>
                              <div className="ahpDistrictChips">
                                {districtOptions.map((d) => {
                                  const on = selectedDistrictIds.includes(d.id);
                                  return (
                                    <button
                                      key={d.id}
                                      type="button"
                                      className={`ahpDistrictChip ${on ? "active" : ""}`}
                                      onClick={() =>
                                        setSelectedDistrictIds((prev) =>
                                          prev.includes(d.id) ? prev.filter((x) => x !== d.id) : [...prev, d.id]
                                        )
                                      }
                                    >
                                      {d.name}
                                    </button>
                                  );
                                })}
                              </div>
                            </div>

                            <div className="ahpMatrixWrap">
                              <div className="ahpMatrixTitle">Ma trận so sánh cặp C1-C4 (nhập bảng, auto nghịch đảo)</div>
                              <div className="criteriaPairMatrixWrap">
                                <table className="criteriaPairMatrixTable">
                                  <thead>
                                    <tr>
                                      <th>Tiêu chí</th>
                                      {AHP_LABELS.map((label) => (
                                        <th key={`pair-head-adv-${label}`}>{label}</th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {AHP_LABELS.map((rowLabel, i) => (
                                      <tr key={`pair-adv-row-${rowLabel}`}>
                                        <th className="criteriaPairMatrixRowHead">{rowLabel}</th>
                                        {AHP_LABELS.map((colLabel, j) => {
                                          if (i === j) {
                                            return (
                                              <td key={`pair-adv-cell-${rowLabel}-${colLabel}`} className="criteriaPairMatrixDiag">
                                                1
                                              </td>
                                            );
                                          }
                                          if (i < j) {
                                            return (
                                              <td key={`pair-adv-cell-${rowLabel}-${colLabel}`}>
                                                <input
                                                  className="criteriaPairMatrixInput"
                                                  type="number"
                                                  min={SAATY_MIN}
                                                  max={SAATY_MAX}
                                                  step={1}
                                                  value={clampSaatyMagnitude(ahpMatrix[i][j])}
                                                  onChange={(e) => updateAhpMatrixCell(i, j, e.target.value)}
                                                />
                                              </td>
                                            );
                                          }
                                          return (
                                            <td key={`pair-adv-cell-${rowLabel}-${colLabel}`} className="criteriaPairMatrixAuto">
                                              {formatSaatyFraction(ahpMatrix[i][j])}
                                            </td>
                                          );
                                        })}
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                              {pairInputNotice ? <div className="criteriaInputNotice">{pairInputNotice}</div> : null}
                            </div>
                          </div>
                        </details>
                      </>
                    ) : (
                      <div style={{ fontSize: 12, color: "#6b7280", marginTop: 8 }}>
                        Chưa có bảng xếp hạng AHP. Chọn ngày rồi bấm <b>Tính AHP (4 tiêu chí)</b>.
                      </div>
                    )}
                  </div>

                  <div style={{ marginTop: 14, borderTop: "1px solid #e5e7eb", paddingTop: 12 }}>
                    <div style={{ fontWeight: 700, fontSize: 13, padding: "0 10px 8px" }}>
                      Tra cứu độ phủ CSDL theo khoảng ngày
                    </div>
                    <div style={{ display: "flex", gap: 8, alignItems: "center", padding: "0 10px 10px", flexWrap: "wrap" }}>
                      <label style={{ fontSize: 12, color: "#374151" }}>
                        Từ:&nbsp;
                        <input
                          type="date"
                          value={covFromDate}
                          onChange={(e) => setCovFromDate(e.target.value)}
                          style={{ padding: "6px 8px", borderRadius: 10, border: "1px solid #e5e7eb" }}
                        />
                      </label>
                      <label style={{ fontSize: 12, color: "#374151" }}>
                        Đến:&nbsp;
                        <input
                          type="date"
                          value={covToDate}
                          onChange={(e) => setCovToDate(e.target.value)}
                          style={{ padding: "6px 8px", borderRadius: 10, border: "1px solid #e5e7eb" }}
                        />
                      </label>
                      <button className="btn" onClick={() => loadCoverageRange(covFromDate, covToDate)} disabled={coverageLoading || backfillLoading}>
                        Tra CSDL
                      </button>
                      <button className="btn" onClick={() => backfillRange(covFromDate, covToDate)} disabled={coverageLoading || backfillLoading}>
                        Backfill range
                      </button>
                      {coverageLoading || backfillLoading ? (
                        <span style={{ fontSize: 12, color: "#6b7280" }}>Đang xử lý...</span>
                      ) : null}
                    </div>
                    {backfillMsg ? (
                      <div style={{ padding: "0 10px 8px", fontSize: 12, color: backfillMsg.startsWith("Backfill lỗi") ? "#b91c1c" : "#065f46" }}>
                        {backfillMsg}
                      </div>
                    ) : null}
                    {coverageErr ? (
                      <div style={{ padding: "0 10px 8px", fontSize: 12, color: "#b91c1c" }}>{coverageErr}</div>
                    ) : coverageRows.length === 0 ? (
                      <div style={{ padding: "0 10px 8px", fontSize: 12, color: "#6b7280" }}>
                        Không có dữ liệu trong khoảng ngày đã chọn.
                      </div>
                    ) : (
                      <table>
                        <thead>
                          <tr>
                            <th>Ngày</th>
                            <th>DailyMetrics</th>
                            <th>DailyCriteria</th>
                            <th>Trạng thái</th>
                          </tr>
                        </thead>
                        <tbody>
                          {coverageRows.map((r) => (
                            <tr key={r.date}>
                              <td>{r.date}</td>
                              <td>{r.daily}/13</td>
                              <td>{r.criteria}/13</td>
                              <td>
                                <span
                                  className="badge"
                                  style={{
                                    color: r.ok ? "#065f46" : "#92400e",
                                    background: r.ok ? "#d1fae5" : "#fef3c7",
                                  }}
                                >
                                  {r.ok ? "Đủ dữ liệu" : "Thiếu dữ liệu"}
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </div>
                </div>
              )}
            </div>
            </div>
          </details>
          ) : null}

          </div>
        </div>
        ) : null}
      </div>
      ) : null}

      <StationForecastPage
        open={stationForecastPageOpen}
        station={selectedOpenAQStation}
        quick={selectedStationQuick}
        loading={selectedStationQuickLoading}
        error={selectedStationQuickErr}
        onClose={() => setStationForecastPageOpen(false)}
        onRefresh={() => {
          if (!selectedOpenAQStation) return;
          inspectOpenAQStation(selectedOpenAQStation).catch(() => {});
        }}
        onUseCoords={
          selectedOpenAQStation
            ? () => {
                const nLat = Number(selectedOpenAQStation.lat.toFixed(6));
                const nLon = Number(selectedOpenAQStation.lon.toFixed(6));
                setLat(nLat);
                setLon(nLon);
                reverseLookupAddress(nLat, nLon).catch(() => {});
              }
            : undefined
        }
      />
    </div>
  );
}
