import axios from "axios";

export const api = axios.create({
  // Nếu bạn đã cấu hình Vite proxy "/api" -> "http://127.0.0.1:8000"
  // thì để baseURL = "" là OK.
  // Nếu CHƯA có proxy thì đổi thành: baseURL: "http://127.0.0.1:8000",
  baseURL: "",
  timeout: 20000,
});

export type BBox = {
  minLat: number;
  minLon: number;
  maxLat: number;
  maxLon: number;
};

export async function pingDb() {
  const res = await api.get("/api/db/ping");
  return res.data;
}

export async function getAlerts(limit = 5) {
  const res = await api.get("/api/alerts", { params: { limit } });
  return res.data;
}

export async function scoreRisk(lat: number, lon: number, weights: Record<string, number>) {
  const res = await api.post("/api/risk/score", { lat, lon, weights });
  return res.data;
}

export async function scoreAndSave(lat: number, lon: number, weights: Record<string, number>) {
  const res = await api.post("/api/risk/score-and-save", { lat, lon, weights });
  return res.data;
}

export async function getAirQuality(lat: number, lon: number, hours = 24) {
  const res = await api.get("/api/air-quality", { params: { lat, lon, hours } });
  return res.data;
}

/**
 * Gọi API grid-score đúng schema backend:
 * POST /api/risk/grid-score
 * body = { bbox: {minLat,minLon,maxLat,maxLon}, step_km, max_points, hours, weights }
 */
export async function gridScore(
  bbox: { minLat: number; minLon: number; maxLat: number; maxLon: number },
  step_km: number,
  hours: number,
  weights: Record<string, number>,
  max_points: number
) {
  const res = await api.post("/api/risk/grid-score", {
    bbox,
    step_km,
    hours,
    weights,
    max_points,
  });
  return res.data;
}

export async function getGeeTiles(params: {
  layer: string;
  start: string;
  end: string;
  mask?: "hcm" | "hcm_inner";
  bbox?: string; // optional: "minLon,minLat,maxLon,maxLat"
}) {
  const res = await api.get("/api/gee/tiles", { params });
  return res.data;
}

export type EarlyWarningReq = {
  lat: number;
  lon: number;
  hours?: number;
  weights: Record<string, number>;
  threshold?: number;
  delta_threshold?: number;
  delta_window?: number;
};

export type EarlyWarningPoint = {
  time: string;
  score_0_100: number;
  level: string;
};

export type EarlyWarningRes = {
  warning: boolean;
  reason: string;
  threshold: number;
  maxScore: number;
  maxLevel: string;
  timeOfMax: string;
  series: EarlyWarningPoint[];
};

export async function getEarlyWarning(payload: EarlyWarningReq) {
  const res = await api.post("/api/alerts/early-warning", payload);
  return res.data as EarlyWarningRes;
}

/* =========================
   ✅ NEW: Run DSS (1 call)
   ========================= */

export type RunDssRequest = {
  lat: number;
  lon: number;
  hours: number;
  weights: Record<string, number>;

  // early-warning params
  threshold?: number;
  delta_threshold?: number;
  delta_window?: number;

  // grid optional
  include_grid?: boolean;
  bbox?: BBox;
  step_km?: number;
  max_points?: number;
};

export type RunDssResponse = {
  timezone: string;
  score: any; // bạn có thể thay any -> type RiskRes nếu muốn chặt hơn
  saved: any;
  early_warning: any;
  grid: any | null;
};

export async function runDss(payload: RunDssRequest) {
  const res = await api.post("/api/dss/run", payload);
  return res.data as RunDssResponse;
}

export async function getStationsBounds(bbox: { minLat: number; minLon: number; maxLat: number; maxLon: number }) {
  const res = await api.get("/api/stations/bounds", { params: bbox });
  return res.data as { items: any[]; count: number };
}

export async function scoreRiskStation(uid: number, weights: Record<string, number>) {
  const res = await api.post("/api/risk/score-station", { uid, weights });
  return res.data;
}

export async function getStationDetail(uid: number) {
  const res = await api.get(`/api/stations/${uid}`);
  return res.data;
}


export type OpenAQStation = {
  id: number;
  name: string;
  lat: number;
  lon: number;
  provider?: string;
  district?: string;
};

export async function fetchOpenAQStations(params?: {
  minLat?: number;
  minLon?: number;
  maxLat?: number;
  maxLon?: number;
  limit?: number;
}) {
  const res = await api.get("/api/openaq/stations", {
    params: {
      minLat: params?.minLat ?? 10.6,
      minLon: params?.minLon ?? 106.55,
      maxLat: params?.maxLat ?? 10.95,
      maxLon: params?.maxLon ?? 106.9,
      limit: params?.limit ?? 200,
    },
  });
  return res.data as { count: number; stations: OpenAQStation[] };
}

export type AiForecastReq = {
  lat: number;
  lon: number;
  horizon_hours: number;
  weights: Record<string, number>;
  threshold?: number;
  model?: string;
};

export type AiForecastRes = {
  warning: boolean;
  max_risk_score: number;
  time_of_max: string;
  current_risk_score?: number;
  current_level?: string;
  current_time?: string;
  confidence_label?: string;
  confidence_0_100?: number;
  series: Array<{ time: string; risk_score_0_100: number }>;
  baseline_series?: Array<{ time: string; risk_score_0_100: number }>;
};

export async function aiForecast(payload: AiForecastReq) {
  const res = await api.post("/api/ai/forecast", payload);
  return res.data as AiForecastRes;
}

export type AiChatMessage = {
  role: "system" | "user" | "assistant";
  content: string;
};

export type AiChatReq = {
  messages: AiChatMessage[];
  lat?: number;
  lon?: number;
  hours?: number;
  weights?: Record<string, number>;
  decision_date?: string;
  ranking_source?: string;
  district_rows?: Array<{
    districtName: string;
    rank: number;
    score: number;
    C1?: number;
    C2?: number;
    C3?: number;
    C4?: number;
  }>;
  forecast_series?: Array<{
    time: string;
    risk_score_0_100: number;
  }>;
  provider?: string;
  model?: string;
  temperature?: number;
};

export type AiChatRes = {
  provider: string;
  model: string;
  reply: string;
};

export async function aiChat(payload: AiChatReq) {
  const res = await api.post("/api/ai/chat", payload);
  return res.data as AiChatRes;
}

export async function geocodeReverse(
  payload: { lat: number; lon: number },
  config?: { signal?: AbortSignal }
) {
  try {
    const res = await api.get("/api/geocode/reverse", {
      params: payload,
      signal: config?.signal,
    });
    return res.data as { display_name?: string; district?: string };
  } catch {
    const nRes = await axios.get("https://nominatim.openstreetmap.org/reverse", {
      params: {
        format: "jsonv2",
        lat: payload.lat,
        lon: payload.lon,
      },
      signal: config?.signal,
      headers: {
        Accept: "application/json",
      },
      timeout: 15000,
    });
    return nRes.data as { display_name?: string; district?: string };
  }
}

export type DistrictDailyRow = {
  DistrictId: number;
  DistrictName: string;
  PM25?: number;
  PM10?: number;
  NO2?: number;
  O3?: number;
  CO?: number;
  [k: string]: any;
};

export async function getDistrictDaily(date: string) {
  const res = await api.get("/api/district/daily", { params: { date } });
  return res.data as { date: string; count: number; items: DistrictDailyRow[] };
}

export async function refreshDistrictDaily(date: string, agg = "mean", source = "auto") {
  const res = await api.post("/api/district/daily/refresh", { date, agg, source });
  return res.data;
}

export type DistrictCoverageItem = {
  date: string;
  count: number;
  [k: string]: any;
};

export async function getDistrictDailyCoverage(from_date: string, to_date: string) {
  const res = await api.get("/api/district/daily/coverage", { params: { from_date, to_date } });
  return res.data as { items: DistrictCoverageItem[] };
}

export async function backfillDistrictDaily(
  from_date: string,
  to_date: string,
  opts?: { agg?: string; source?: string }
) {
  const res = await api.post("/api/district/daily/backfill", {
    from_date,
    to_date,
    agg: opts?.agg ?? "mean",
    source: opts?.source ?? "auto",
  });
  return res.data;
}

export type DistrictCriteriaRow = {
  DistrictId: number;
  DistrictName: string;
  C1: number;
  C2: number;
  C3: number;
  C4: number;
  [k: string]: any;
};

export type DistrictCriteriaResponse = {
  date?: string;
  count?: number;
  expected_count?: number;
  imputed_count?: number;
  refresh_warning?: string;
  items: DistrictCriteriaRow[];
};

export async function getDistrictCriteria(
  date: string,
  opts?: {
    autofill?: boolean;
    fallback_days?: number;
    t?: number;
    t_high?: number;
    timeout?: number;
  }
) {
  const res = await api.get("/api/district/criteria", {
    params: { date, ...(opts || {}) },
    timeout: opts?.timeout ?? 30000,
  });
  return res.data as DistrictCriteriaResponse;
}

export async function refreshDistrictCriteria(
  date: string,
  opts?: { t?: number; t_high?: number; air_source?: string }
) {
  const res = await api.post("/api/district/criteria/refresh", {
    date,
    ...(opts || {}),
  });
  return res.data;
}

export async function getDistrictCriteriaCoverage(from_date: string, to_date: string) {
  const res = await api.get("/api/district/criteria/coverage", { params: { from_date, to_date } });
  return res.data as { items: DistrictCoverageItem[] };
}

export async function backfillDistrictCriteria(
  from_date: string,
  to_date: string,
  opts?: { air_source?: string; t?: number; t_high?: number }
) {
  const res = await api.post("/api/district/criteria/backfill", {
    from_date,
    to_date,
    ...(opts || {}),
  });
  return res.data;
}

export type AhpWeightsResult = {
  lambda_max: number;
  CI: number;
  CR: number;
  is_consistent: boolean;
  weights: Array<{ label: string; weight: number }>;
};

export async function ahpWeights(matrix: number[][], labels: string[], config?: { timeout?: number }) {
  const res = await api.post(
    "/api/ahp/weights",
    { matrix, labels },
    { timeout: config?.timeout ?? 30000 }
  );
  return res.data as AhpWeightsResult;
}

export type DistrictAHPScoredRow = {
  DistrictId: number;
  DistrictName: string;
  Score: number;
  Rank: number;
  [k: string]: any;
};

export async function getDistrictAHPScore(
  date: string,
  payload: {
    matrix: number[][];
    labels: string[];
    normalize_alternatives?: boolean;
    rank_mode?: string;
    alternatives_override?: Array<Record<string, any>>;
  }
) {
  const res = await api.post("/api/district/ahp-score", {
    date,
    ...payload,
  });
  return res.data as {
    ahp: AhpWeightsResult;
    items: DistrictAHPScoredRow[];
  };
}

export type ScenarioPresetName =
  | "balanced"
  | "severe_now"
  | "persistent"
  | "early_warning"
  | "prolonged_pollution";

export type ScenarioWeights = {
  C1: number;
  C2: number;
  C3: number;
  C4: number;
};

export type ScenarioThresholds = {
  yellow: number;
  orange: number;
  red: number;
};

export type DistrictPolicyScenarioResponse = {
  summary?: any;
  rows?: Array<Record<string, any>>;
  [k: string]: any;
};

export async function runDistrictPolicyScenario(payload: {
  date: string;
  presetName: ScenarioPresetName;
  useCustomWeights?: boolean;
  customWeights?: ScenarioWeights;
  normalizeCustomWeights?: boolean;
  thresholds?: ScenarioThresholds;
  earlyWarningEnabled?: boolean;
  compareWithBaseline?: boolean;
  topN?: number;
  autofill?: boolean;
  fallback_days?: number;
  force_refresh?: boolean;
}) {
  const res = await api.post("/api/district/policy-scenario", payload);
  return res.data as DistrictPolicyScenarioResponse;
}


