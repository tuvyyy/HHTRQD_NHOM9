import { useEffect, useMemo, useState } from "react";
import { Circle, GeoJSON, MapContainer, Marker, Popup, TileLayer, useMap, useMapEvents } from "react-leaflet";
import type { LatLngBoundsExpression, LatLngExpression } from "leaflet";
import type { Feature, FeatureCollection, MultiPolygon, Point, Polygon as GeoPolygon } from "geojson";

import RiskGridLayer from "./RiskGridLayer";
import { fetchOpenAQStations, getStationsBounds, type OpenAQStation } from "./api";

export type DistrictMapResultItem = {
  districtName: string;
  rank: number;
  score: number;
};

export type DistrictMapForecastSummary = {
  label: string;
  rank?: number | null;
  score?: number | null;
  topDistrict?: string;
  timeRef?: string;
};

export type DistrictMapDetail = {
  districtName: string;
  currentRank?: number | null;
  currentTotal?: number | null;
  currentScore?: number | null;
  currentPriority?: string;
  forecast1d?: DistrictMapForecastSummary | null;
  forecast3d?: DistrictMapForecastSummary | null;
  forecastLoading?: boolean;
  forecastError?: string | null;
  note?: string;
  healthAdvice?: string[];
};

export type MapSourceStatus = {
  source: "openaq" | "aqicn" | "iqair" | "purpleair";
  count: number;
  updatedAt?: string;
  error?: string;
};

type Props = {
  lat: number;
  lon: number;
  onPick: (lat: number, lon: number) => void;
  level?: string;
  grid?: FeatureCollection<Point> | null;
  scanOn?: boolean;
  scanKm?: number;
  districtResultItems?: DistrictMapResultItem[] | null;
  selectedOpenAQStationId?: number | null;
  onOpenAQStationPick?: (st: OpenAQStation) => void;
  onOpenAQStationsCountChange?: (n: number) => void;
  onAQICNStationsCountChange?: (n: number) => void;
  onIQAirStationsCountChange?: (n: number) => void;
  onPurpleAirStationsCountChange?: (n: number) => void;
  onSourcesStatusChange?: (statuses: Partial<Record<MapSourceStatus["source"], MapSourceStatus>>) => void;
  onDistrictPick?: (districtName: string) => void;
  selectedDistrictName?: string;
  districtDetailPanel?: DistrictMapDetail | null;
  onClearDistrictPick?: () => void;
  showSourcePanel?: boolean;
};

type DistrictBoundaryProps = {
  districtId?: number;
  districtName?: string;
  aliases?: string[];
  [k: string]: unknown;
};

type DistrictBoundaryFeature = Feature<GeoPolygon | MultiPolygon, DistrictBoundaryProps>;
type DistrictBoundaryCollection = FeatureCollection<GeoPolygon | MultiPolygon, DistrictBoundaryProps>;
type MaskFeatureCollection = FeatureCollection<GeoPolygon, Record<string, never>>;

const DISTRICT_BORDER_COLORS = [
  "#1d4eff",
  "#ff2d2d",
  "#10b95c",
  "#ff9800",
  "#9d4dff",
  "#00aee8",
  "#ff1f6b",
  "#00bfa5",
  "#ff6b00",
  "#7a67ff",
  "#00a0ff",
  "#79c900",
  "#ff3fa4",
];

type BoundsTuple = [[number, number], [number, number]];
type PanelPos = { left: number; top: number };

const HCM_BOUNDS: BoundsTuple = [
  [10.33, 106.35],
  [11.17, 107.05],
];

function hcmOuterRing() {
  const [sw, ne] = HCM_BOUNDS;
  return [
    [sw[1], sw[0]],
    [ne[1], sw[0]],
    [ne[1], ne[0]],
    [sw[1], ne[0]],
    [sw[1], sw[0]],
  ] as number[][];
}

function normalizeKey(v: string) {
  return String(v || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function extractDistrictNumber(text: string): number | null {
  const raw = String(text || "").trim();
  if (!raw) return null;
  const normalized = raw
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase();
  const m = normalized.match(/\b(?:q|quan|district)\s*\.?\s*(\d{1,2})\b/);
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isFinite(n) ? n : null;
}

function toLevelStyle(level?: string) {
  const lv = normalizeKey(level || "");
  if (lv.includes("xanh")) return { radius: 400, color: "#16a34a" };
  if (lv.includes("vang")) return { radius: 700, color: "#f59e0b" };
  if (lv.includes("cam")) return { radius: 1000, color: "#f97316" };
  if (lv.includes("do")) return { radius: 1400, color: "#ef4444" };
  return { radius: 500, color: "#2563eb" };
}

function scoreFillByRank(rank: number, total: number) {
  if (!rank || !total) return "rgba(30, 64, 175, 0.08)";
  const p = rank / Math.max(total, 1);
  if (p <= 0.2) return "rgba(239, 68, 68, 0.30)";
  if (p <= 0.45) return "rgba(245, 158, 11, 0.28)";
  if (p <= 0.7) return "rgba(250, 204, 21, 0.22)";
  return "rgba(34, 197, 94, 0.20)";
}

function mapSourceFromProvider(providerName?: string): MapSourceStatus["source"] {
  const p = String(providerName || "").toLowerCase();
  if (p.includes("aqicn") || p.includes("waqi")) return "aqicn";
  if (p.includes("iqair") || p.includes("airvisual") || p.includes("airgradient")) return "iqair";
  if (p.includes("purpleair") || p.includes("habitatmap")) return "purpleair";
  return "openaq";
}

function hexToRgba(hex: string, alpha: number) {
  const m = String(hex || "").replace("#", "");
  if (m.length !== 6) return `rgba(30, 64, 175, ${alpha})`;
  const r = Number.parseInt(m.slice(0, 2), 16);
  const g = Number.parseInt(m.slice(2, 4), 16);
  const b = Number.parseInt(m.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function reliabilityFromTotal(total: number) {
  if (total >= 15) return { label: "Cao", score: 78 };
  if (total >= 8) return { label: "Trung bình", score: 64 };
  return { label: "Thấp", score: 45 };
}

function extractOuterRings(geometry: GeoPolygon | MultiPolygon): number[][][] {
  if (geometry.type === "Polygon") {
    return Array.isArray(geometry.coordinates?.[0]) ? [geometry.coordinates[0] as number[][]] : [];
  }
  return (geometry.coordinates || [])
    .map((poly) => (Array.isArray(poly?.[0]) ? (poly[0] as number[][]) : null))
    .filter((ring): ring is number[][] => Array.isArray(ring) && ring.length >= 4);
}

function closeRing(ring: number[][]): number[][] {
  if (!ring.length) return ring;
  const first = ring[0];
  const last = ring[ring.length - 1];
  if (!first || !last) return ring;
  if (first[0] === last[0] && first[1] === last[1]) return ring;
  return [...ring, [first[0], first[1]]];
}

function extractItemDistrictNumber(item: {
  districtId?: number;
  districtName: string;
  aliases?: string[];
}): number | null {
  const byId = Number(item.districtId);
  if (Number.isFinite(byId) && byId > 0) return byId;
  const fromName = extractDistrictNumber(item.districtName);
  if (fromName) return fromName;
  for (const alias of item.aliases || []) {
    const n = extractDistrictNumber(alias);
    if (n) return n;
  }
  return null;
}

function bboxAround(lat: number, lon: number, km: number) {
  const dLat = km / 111;
  const dLon = km / (111 * Math.max(0.2, Math.cos((lat * Math.PI) / 180)));
  return {
    minLat: lat - dLat,
    minLon: lon - dLon,
    maxLat: lat + dLat,
    maxLon: lon + dLon,
  };
}

function clampToCustomBounds(lat: number, lon: number, bounds: BoundsTuple) {
  const [sw, ne] = bounds;
  return {
    lat: Math.min(Math.max(lat, sw[0]), ne[0]),
    lon: Math.min(Math.max(lon, sw[1]), ne[1]),
  };
}

function isInsideBounds(lat: number, lon: number) {
  const [sw, ne] = HCM_BOUNDS;
  return lat >= sw[0] && lat <= ne[0] && lon >= sw[1] && lon <= ne[1];
}

function isInsideCustomBounds(lat: number, lon: number, bounds: BoundsTuple) {
  const [sw, ne] = bounds;
  return lat >= sw[0] && lat <= ne[0] && lon >= sw[1] && lon <= ne[1];
}

function ClickHandler({ onPick, bounds }: { onPick: (lat: number, lon: number) => void; bounds: BoundsTuple }) {
  useMapEvents({
    click: (e) => {
      const { lat, lng } = e.latlng;
      if (!isInsideBounds(lat, lng)) return;
      if (!isInsideCustomBounds(lat, lng, bounds)) return;
      onPick(lat, lng);
    },
  });
  return null;
}

function MapBoundsGuard({ focusBounds }: { focusBounds: BoundsTuple }) {
  useMapEvents({
    moveend: (e) => {
      const map = e.target;
      const c = map.getCenter();
      if (isInsideCustomBounds(c.lat, c.lng, focusBounds)) return;
      const clamped = clampToCustomBounds(c.lat, c.lng, focusBounds);
      map.flyTo([clamped.lat, clamped.lon], map.getZoom(), { animate: true, duration: 0.35 });
    },
  });
  return null;
}

function computeFeatureBounds(feature: DistrictBoundaryFeature): BoundsTuple | null {
  let minLat = Infinity;
  let minLon = Infinity;
  let maxLat = -Infinity;
  let maxLon = -Infinity;

  for (const ring of extractOuterRings(feature.geometry)) {
    for (const p of ring) {
      const lon = Number(p[0]);
      const lat = Number(p[1]);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
      minLat = Math.min(minLat, lat);
      minLon = Math.min(minLon, lon);
      maxLat = Math.max(maxLat, lat);
      maxLon = Math.max(maxLon, lon);
    }
  }

  if (!Number.isFinite(minLat) || !Number.isFinite(minLon) || !Number.isFinite(maxLat) || !Number.isFinite(maxLon)) {
    return null;
  }
  return [
    [minLat, minLon],
    [maxLat, maxLon],
  ];
}

function DistrictFocusController({
  focusBounds,
  focusTrigger,
  rightPanelPx,
}: {
  focusBounds: BoundsTuple | null;
  focusTrigger: number;
  rightPanelPx: number;
}) {
  const map = useMap();
  useEffect(() => {
    if (!focusBounds) return;
    map.flyToBounds(focusBounds as unknown as LatLngBoundsExpression, {
      animate: true,
      duration: 0.55,
      maxZoom: 13,
      paddingTopLeft: [26, 24],
      paddingBottomRight: [Math.max(90, rightPanelPx), 24],
    });
  }, [map, focusBounds, focusTrigger, rightPanelPx]);
  return null;
}

function DistrictPanelAnchorController({
  anchor,
  panelWidth,
  onPosition,
}: {
  anchor: [number, number] | null;
  panelWidth: number;
  onPosition: (next: PanelPos | null) => void;
}) {
  const map = useMap();
  useEffect(() => {
    if (!anchor) {
      onPosition(null);
      return;
    }
    const update = () => {
      const pt = map.latLngToContainerPoint([anchor[0], anchor[1]] as any);
      const size = map.getSize();
      const panelH = 360;
      let left = pt.x + 44;
      let top = pt.y - 150;
      if (left + panelWidth > size.x - 10) left = pt.x - panelWidth - 44;
      left = Math.max(10, Math.min(left, size.x - panelWidth - 10));
      top = Math.max(10, Math.min(top, size.y - panelH - 10));
      onPosition({ left, top });
    };
    update();
    map.on("move", update);
    map.on("zoom", update);
    map.on("resize", update);
    return () => {
      map.off("move", update);
      map.off("zoom", update);
      map.off("resize", update);
    };
  }, [map, anchor, panelWidth, onPosition]);
  return null;
}

export default function MapPicker({
  lat,
  lon,
  onPick,
  level,
  grid,
  districtResultItems,
  selectedOpenAQStationId,
  onOpenAQStationPick,
  onOpenAQStationsCountChange,
  onAQICNStationsCountChange,
  onIQAirStationsCountChange,
  onPurpleAirStationsCountChange,
  onSourcesStatusChange,
  onDistrictPick,
  selectedDistrictName,
  districtDetailPanel,
  onClearDistrictPick,
  showSourcePanel = false,
}: Props) {
  const circle = toLevelStyle(level);

  const [showDistrictLayer, setShowDistrictLayer] = useState(true);
  const [pickedDistrict, setPickedDistrict] = useState("");
  const [stations, setStations] = useState<OpenAQStation[]>([]);
  const [sourceCounts, setSourceCounts] = useState({
    openaq: 0,
    aqicn: 0,
    iqair: 0,
    purpleair: 0,
  });
  const [sourceUpdatedAt, setSourceUpdatedAt] = useState("");
  const [sourcePanelCollapsed, setSourcePanelCollapsed] = useState(false);
  const [districtBoundaries, setDistrictBoundaries] = useState<DistrictBoundaryFeature[]>([]);
  const [districtBoundaryErr, setDistrictBoundaryErr] = useState<string | null>(null);
  const [focusDistrictBounds, setFocusDistrictBounds] = useState<BoundsTuple | null>(null);
  const [focusTrigger, setFocusTrigger] = useState(0);
  const [panelAnchor, setPanelAnchor] = useState<[number, number] | null>(null);
  const [panelPos, setPanelPos] = useState<PanelPos | null>(null);

  const innerCityBounds = useMemo<BoundsTuple>(() => {
    if (!districtBoundaries.length) return HCM_BOUNDS;
    let minLat = Infinity;
    let minLon = Infinity;
    let maxLat = -Infinity;
    let maxLon = -Infinity;

    for (const feature of districtBoundaries) {
      for (const ring of extractOuterRings(feature.geometry)) {
        for (const p of ring) {
          const lon = Number(p[0]);
          const lat = Number(p[1]);
          if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
          if (lat < minLat) minLat = lat;
          if (lat > maxLat) maxLat = lat;
          if (lon < minLon) minLon = lon;
          if (lon > maxLon) maxLon = lon;
        }
      }
    }

    if (!Number.isFinite(minLat) || !Number.isFinite(minLon) || !Number.isFinite(maxLat) || !Number.isFinite(maxLon)) {
      return HCM_BOUNDS;
    }

    const latPad = 0.008;
    const lonPad = 0.01;
    return [
      [minLat - latPad, minLon - lonPad],
      [maxLat + latPad, maxLon + lonPad],
    ];
  }, [districtBoundaries]);

  const clamped = useMemo(() => clampToCustomBounds(lat, lon, innerCityBounds), [lat, lon, innerCityBounds]);
  const center: LatLngExpression = [clamped.lat, clamped.lon];
  const maxBounds = HCM_BOUNDS as unknown as LatLngBoundsExpression;

  const sourceTotal = sourceCounts.openaq + sourceCounts.aqicn + sourceCounts.iqair + sourceCounts.purpleair;
  const sourceReliability = useMemo(() => reliabilityFromTotal(sourceTotal), [sourceTotal]);

  useEffect(() => {
    let cancelled = false;
    const geoJsonUrl = `${import.meta.env.BASE_URL}data/hcm_districts_13.geojson`;

    async function loadDistrictBoundaries() {
      try {
        setDistrictBoundaryErr(null);
        const res = await fetch(geoJsonUrl, { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const fc = (await res.json()) as DistrictBoundaryCollection;
        const features = Array.isArray(fc?.features) ? fc.features : [];
        if (!features.length) throw new Error("GeoJSON không có dữ liệu feature.");
        if (!cancelled) setDistrictBoundaries(features);
      } catch (e: any) {
        if (cancelled) return;
        setDistrictBoundaries([]);
        setDistrictBoundaryErr(e?.message || "Không tải được ranh giới quận.");
      }
    }

    loadDistrictBoundaries();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function loadSources() {
      const statuses: Partial<Record<MapSourceStatus["source"], MapSourceStatus>> = {
        openaq: { source: "openaq", count: 0, updatedAt: new Date().toISOString() },
        aqicn: { source: "aqicn", count: 0, updatedAt: new Date().toISOString() },
        iqair: { source: "iqair", count: 0, updatedAt: new Date().toISOString() },
        purpleair: { source: "purpleair", count: 0, updatedAt: new Date().toISOString() },
      };

      try {
        const openaqRes = await fetchOpenAQStations({
          minLat: 10.6,
          minLon: 106.55,
          maxLat: 10.95,
          maxLon: 106.9,
          limit: 1000,
        });
        if (cancelled) return;

        const list = Array.isArray(openaqRes?.stations) ? openaqRes.stations : [];
        setStations(list);

        const grouped = { openaq: 0, aqicn: 0, iqair: 0, purpleair: 0 };
        for (const st of list) grouped[mapSourceFromProvider(st.provider)] += 1;

        try {
          const aqicnRes = await getStationsBounds(bboxAround(clamped.lat, clamped.lon, 60));
          grouped.aqicn = Math.max(grouped.aqicn, Number(aqicnRes?.count || 0));
        } catch {
          // noop
        }

        if (cancelled) return;

        setSourceCounts(grouped);
        setSourceUpdatedAt(new Date().toLocaleTimeString("vi-VN", { hour: "2-digit", minute: "2-digit" }));

        statuses.openaq = { source: "openaq", count: grouped.openaq, updatedAt: new Date().toISOString() };
        statuses.aqicn = { source: "aqicn", count: grouped.aqicn, updatedAt: new Date().toISOString() };
        statuses.iqair = { source: "iqair", count: grouped.iqair, updatedAt: new Date().toISOString() };
        statuses.purpleair = { source: "purpleair", count: grouped.purpleair, updatedAt: new Date().toISOString() };

        onOpenAQStationsCountChange?.(grouped.openaq);
        onAQICNStationsCountChange?.(grouped.aqicn);
        onIQAirStationsCountChange?.(grouped.iqair);
        onPurpleAirStationsCountChange?.(grouped.purpleair);
      } catch (e: any) {
        if (cancelled) return;
        const msg = e?.message || "Lỗi tải nguồn trạm";
        statuses.openaq = { source: "openaq", count: 0, updatedAt: new Date().toISOString(), error: msg };
      } finally {
        if (!cancelled) onSourcesStatusChange?.(statuses);
      }
    }

    loadSources();
    return () => {
      cancelled = true;
    };
  }, [
    clamped.lat,
    clamped.lon,
    onAQICNStationsCountChange,
    onIQAirStationsCountChange,
    onOpenAQStationsCountChange,
    onPurpleAirStationsCountChange,
    onSourcesStatusChange,
  ]);

  const districtRowsByKey = useMemo(() => {
    const map = new Map<string, DistrictMapResultItem>();
    for (const row of districtResultItems || []) {
      const key = normalizeKey(String(row.districtName || ""));
      if (key) map.set(key, row);
    }
    return map;
  }, [districtResultItems]);

  const districtBoundaryPaintItems = useMemo(() => {
    const total = districtResultItems?.length || districtBoundaries.length || 13;

    return districtBoundaries.map((feature, idx) => {
      const props = feature.properties || {};
      const districtName = String(props.districtName || `Quận ${idx + 1}`);
      const aliases = Array.isArray(props.aliases) ? props.aliases.map((x) => String(x)) : [];
      const candidateNames = [districtName, ...aliases];

      let matched: DistrictMapResultItem | undefined;
      for (const candidate of candidateNames) {
        const key = normalizeKey(candidate);
        if (!key) continue;
        matched = districtRowsByKey.get(key);
        if (matched) break;
      }

      if (!matched) {
        for (const [rowKey, row] of districtRowsByKey.entries()) {
          if (candidateNames.some((name) => {
            const nk = normalizeKey(name);
            return nk && (nk.includes(rowKey) || rowKey.includes(nk));
          })) {
            matched = row;
            break;
          }
        }
      }

      const id = Number(props.districtId || idx + 1);
      const borderColor = DISTRICT_BORDER_COLORS[(Math.max(1, id) - 1) % DISTRICT_BORDER_COLORS.length];
      const fillColor = matched ? scoreFillByRank(Number(matched.rank || 0), total) : hexToRgba(borderColor, 0.2);

      return {
        feature,
        districtName,
        key: normalizeKey(districtName),
        districtId: Number(props.districtId || idx + 1),
        aliases,
        borderColor,
        fillColor,
      };
    });
  }, [districtBoundaries, districtResultItems, districtRowsByKey]);

  const selectedBoundaryItem = useMemo(() => {
    const incoming = String(pickedDistrict || "").trim();
    if (!incoming) return null;
    const key = normalizeKey(incoming);
    let matched = districtBoundaryPaintItems.find((it) => it.key === key) || null;
    if (matched) return matched;

    const wantedNo = extractDistrictNumber(incoming);
    if (wantedNo) {
      matched =
        districtBoundaryPaintItems.find((it) => extractItemDistrictNumber(it) === wantedNo) ||
        null;
      if (matched) return matched;
    }

    matched =
      districtBoundaryPaintItems.find((it) =>
        [it.districtName, ...(it.aliases || [])].some((name) => normalizeKey(name) === key)
      ) || null;
    if (matched) return matched;

    return null;
  }, [pickedDistrict, districtBoundaryPaintItems]);

  // Always dim outside the inner-city district polygons.
  const outsideInnerCityMask = useMemo<MaskFeatureCollection | null>(() => {
    if (!showDistrictLayer || !districtBoundaryPaintItems.length) return null;
    const holes = districtBoundaryPaintItems
      .flatMap((it) => extractOuterRings(it.feature.geometry))
      .map(closeRing)
      .filter((r) => r.length >= 4);
    if (!holes.length) return null;
    return {
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          properties: {},
          geometry: {
            type: "Polygon",
            coordinates: [hcmOuterRing(), ...holes],
          },
        },
      ],
    };
  }, [showDistrictLayer, districtBoundaryPaintItems]);

  // Blur mask that keeps ONLY selected district bright:
  // full HCM bounds as outer ring + selected district rings as holes.
  const selectedDistrictBlurMask = useMemo<MaskFeatureCollection | null>(() => {
    if (!showDistrictLayer || !selectedBoundaryItem) return null;
    const holes = extractOuterRings(selectedBoundaryItem.feature.geometry).map(closeRing).filter((r) => r.length >= 4);
    if (!holes.length) return null;
    return {
      type: "FeatureCollection",
      features: [
        {
          type: "Feature",
          properties: {},
          geometry: {
            type: "Polygon",
            coordinates: [hcmOuterRing(), ...holes],
          },
        },
      ],
    };
  }, [showDistrictLayer, selectedBoundaryItem]);

  useEffect(() => {
    const incoming = String(selectedDistrictName || "").trim();
    if (!incoming) {
      setPickedDistrict("");
      setFocusDistrictBounds(null);
      setPanelAnchor(null);
      return;
    }
    const key = normalizeKey(incoming);
    const wantedNo = extractDistrictNumber(incoming);
    const matched =
      districtBoundaryPaintItems.find((it) => it.key === key) ||
      districtBoundaryPaintItems.find((it) =>
        [it.districtName, ...(it.aliases || [])].some((name) => normalizeKey(name) === key)
      ) ||
      (wantedNo
        ? districtBoundaryPaintItems.find((it) => extractItemDistrictNumber(it) === wantedNo)
        : null) ||
      null;
    if (!matched) {
      setPickedDistrict(incoming);
      setFocusDistrictBounds(null);
      setPanelAnchor(null);
      return;
    }
    setPickedDistrict(matched.districtName);
    const fb = computeFeatureBounds(matched.feature);
    setFocusDistrictBounds(fb);
    if (fb) {
      const centerLat = (fb[0][0] + fb[1][0]) / 2;
      const centerLon = (fb[0][1] + fb[1][1]) / 2;
      setPanelAnchor([centerLat, centerLon]);
    }
    setFocusTrigger((v) => v + 1);
  }, [selectedDistrictName, districtBoundaryPaintItems]);

  const topDistricts = useMemo(() => {
    return [...(districtResultItems || [])]
      .sort((a, b) => Number(a.rank || 0) - Number(b.rank || 0))
      .slice(0, 3)
      .map((x) => x.districtName);
  }, [districtResultItems]);

  // Keep the detail panel away from the selected district region.
  // Anchoring the panel near the polygon center makes it "đè" lên quận và khó quan sát.
  const useAnchoredDistrictPanel = false;

  return (
    <div style={{ width: "100%", height: "100%", position: "relative" }}>
      {showSourcePanel ? (
      <div className="mapSourcePanel">
        <div className="mapSourceHead">
          <b>Nguồn / Layer</b>
          <button
            type="button"
            className="mapSourceCollapseBtn"
            title={sourcePanelCollapsed ? "Mở rộng" : "Thu gọn"}
            onClick={() => setSourcePanelCollapsed((v) => !v)}
          >
            {sourcePanelCollapsed ? "+" : "−"}
          </button>
        </div>

        {!sourcePanelCollapsed ? (
          <>
            <label className="mapSourceToggle">
              <input type="checkbox" checked={showDistrictLayer} onChange={(e) => setShowDistrictLayer(e.target.checked)} />
              <span>Ranh giới nội thành theo quận</span>
            </label>

            {districtBoundaryErr ? <div className="mapSourceErr">{districtBoundaryErr}</div> : null}

            <div className="mapSourceSection">
              <div className="mapSourceTopRow">
                <b>Nguồn trạm</b>
                <span className="mapReliabilityBadge">Tin cậy: {sourceReliability.label} ({sourceReliability.score})</span>
              </div>
              <div className="mapSourceHint">
                Tổng marker: {sourceTotal} {sourceUpdatedAt ? `| Cập nhật ${sourceUpdatedAt}` : ""}
              </div>
              <div className="mapSourceRow">
                <span className="mapSourceName"><i className="mapSourceDot src-openaq" />OpenAQ</span>
                <b>{sourceCounts.openaq}</b>
              </div>
              <div className="mapSourceRow">
                <span className="mapSourceName"><i className="mapSourceDot src-aqicn" />AQICN</span>
                <b>{sourceCounts.aqicn}</b>
              </div>
              <div className="mapSourceRow">
                <span className="mapSourceName"><i className="mapSourceDot src-iqair" />IQAir</span>
                <b>{sourceCounts.iqair}</b>
              </div>
              <div className="mapSourceRow">
                <span className="mapSourceName"><i className="mapSourceDot src-purpleair" />PurpleAir</span>
                <b>{sourceCounts.purpleair}</b>
              </div>
            </div>

            {districtResultItems?.length ? (
              <div className="mapSourceSection">
                <b>Tô màu theo kết quả AHP</b>
                <div className="mapSourceHint">Áp dụng cho {districtResultItems.length} quận theo thứ hạng.</div>
                {topDistricts.length ? <div className="mapSourceHint">Top 3: {topDistricts.join(" · ")}</div> : null}
              </div>
            ) : null}
          </>
        ) : null}
      </div>
      ) : null}

      {districtDetailPanel && String(districtDetailPanel.districtName || "").trim() ? (
        <div
          className="mapDistrictDetailPanel"
          style={
            useAnchoredDistrictPanel && panelPos
              ? {
                  left: `${panelPos.left}px`,
                  top: `${panelPos.top}px`,
                  right: "auto",
                  bottom: "auto",
                  transform: "none",
                }
              : undefined
          }
        >
          <div className="mapDistrictDetailHead">
            <div>
              <div className="mapDistrictDetailTitle">{districtDetailPanel.districtName}</div>
              <div className="mapDistrictDetailSub">Chi tiết trực quan theo quận</div>
            </div>
            <button type="button" className="mapDistrictDetailClose" onClick={onClearDistrictPick}>
              ×
            </button>
          </div>

          <div className="mapDistrictDetailGrid">
            <div>
              Hạng hiện tại:{" "}
              <b>
                {districtDetailPanel.currentRank && districtDetailPanel.currentTotal
                  ? `#${districtDetailPanel.currentRank}/${districtDetailPanel.currentTotal}`
                  : "—"}
              </b>
            </div>
            <div>
              Điểm hiện tại: <b>{Number.isFinite(Number(districtDetailPanel.currentScore)) ? Number(districtDetailPanel.currentScore).toFixed(6) : "—"}</b>
            </div>
            <div>
              Mức ưu tiên: <b>{districtDetailPanel.currentPriority || "—"}</b>
            </div>
          </div>

          <div className="mapDistrictForecastWrap">
            <div className="mapDistrictForecastCard">
              <div className="mapDistrictForecastLabel">{districtDetailPanel.forecast1d?.label || "Dự báo 1 ngày tới"}</div>
              <div>Hạng: <b>{districtDetailPanel.forecast1d?.rank ? `#${districtDetailPanel.forecast1d.rank}` : "—"}</b></div>
              <div>Điểm: <b>{Number.isFinite(Number(districtDetailPanel.forecast1d?.score)) ? Number(districtDetailPanel.forecast1d?.score).toFixed(6) : "—"}</b></div>
            </div>
            <div className="mapDistrictForecastCard">
              <div className="mapDistrictForecastLabel">{districtDetailPanel.forecast3d?.label || "Dự báo 3 ngày tới"}</div>
              <div>Hạng: <b>{districtDetailPanel.forecast3d?.rank ? `#${districtDetailPanel.forecast3d.rank}` : "—"}</b></div>
              <div>Điểm: <b>{Number.isFinite(Number(districtDetailPanel.forecast3d?.score)) ? Number(districtDetailPanel.forecast3d?.score).toFixed(6) : "—"}</b></div>
            </div>
          </div>

          {districtDetailPanel.forecastLoading ? <div className="mapDistrictDetailHint">Đang cập nhật dự báo theo quận...</div> : null}
          {districtDetailPanel.forecastError ? <div className="mapDistrictDetailWarn">{districtDetailPanel.forecastError}</div> : null}
          {districtDetailPanel.note ? <div className="mapDistrictDetailNote">{districtDetailPanel.note}</div> : null}
          {districtDetailPanel.healthAdvice?.length ? (
            <div className="mapDistrictDetailHealth">
              <div className="mapDistrictDetailHealthTitle">Lưu ý sức khỏe khi ra đường</div>
              <ul>
                {districtDetailPanel.healthAdvice.slice(0, 4).map((line, idx) => (
                  <li key={`health-note-${idx}`}>{line}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </div>
      ) : null}

      <MapContainer
        center={center}
        zoom={12}
        style={{ width: "100%", height: "100%" }}
        maxBounds={maxBounds}
        maxBoundsViscosity={1}
        minZoom={11}
        maxZoom={18}
        worldCopyJump={false}
      >
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" noWrap />

        <ClickHandler onPick={onPick} bounds={innerCityBounds} />
        <MapBoundsGuard focusBounds={innerCityBounds} />
        <DistrictFocusController
          focusBounds={focusDistrictBounds}
          focusTrigger={focusTrigger}
          rightPanelPx={districtDetailPanel ? 360 : 100}
        />
        {useAnchoredDistrictPanel ? (
          <DistrictPanelAnchorController
            anchor={panelAnchor}
            panelWidth={340}
            onPosition={(next) =>
              setPanelPos((prev) => {
                if (!next) return null;
                if (!prev) return next;
                if (Math.abs(prev.left - next.left) < 1 && Math.abs(prev.top - next.top) < 1) return prev;
                return next;
              })
            }
          />
        ) : null}
        <RiskGridLayer geojson={grid ?? null} />

        {(selectedDistrictBlurMask || outsideInnerCityMask) ? (
          <GeoJSON
            data={(selectedDistrictBlurMask || outsideInnerCityMask) as any}
            style={{
              stroke: false,
              color: "transparent",
              opacity: 0,
              weight: 0,
              fillRule: "evenodd",
              fillColor: "#6b7280",
              fillOpacity: selectedDistrictBlurMask ? 0.50 : 0.42,
              interactive: false,
            }}
          />
        ) : null}

        {showDistrictLayer && selectedBoundaryItem ? (
          <GeoJSON
            key={`district-selected-halo-${selectedBoundaryItem.key}`}
            data={selectedBoundaryItem.feature as DistrictBoundaryFeature}
            style={{
              className: "district-selected-halo",
              color: "#f59e0b",
              weight: 9,
              opacity: 0.46,
              fillColor: "rgba(245, 158, 11, 0.10)",
              fillOpacity: 0.14,
              interactive: false,
            }}
          />
        ) : null}

        {showDistrictLayer
          ? districtBoundaryPaintItems.map((it) => {
              const hasSelected = Boolean(String(pickedDistrict || "").trim());
              const selected = !!selectedBoundaryItem && selectedBoundaryItem.key === it.key;
              return (
                <GeoJSON
                  key={`district-geo-${it.key}`}
                  data={it.feature as DistrictBoundaryFeature}
                  style={{
                    className: selected ? "district-selected-stroke" : "district-stroke-strong",
                    color: selected ? "#ea580c" : it.borderColor,
                    weight: selected ? 6.4 : hasSelected ? 2.9 : 3.0,
                    // Keep selected district bright; dimming is handled by the global mask.
                    fillColor: hasSelected ? (selected ? "rgba(251, 146, 60, 0.18)" : "rgba(255, 255, 255, 0)") : it.fillColor,
                    fillOpacity: hasSelected ? (selected ? 0.26 : 0) : 0.56,
                    opacity: selected ? 1 : hasSelected ? 1 : 0.94,
                  }}
                  onEachFeature={(_, layer) => {
                    layer.bindTooltip(it.districtName, { sticky: true, direction: "top" });
                    if (selected) layer.bringToFront();
                  }}
                  eventHandlers={{
                    click: (e: any) => {
                      e?.originalEvent?.stopPropagation?.();
                      setPickedDistrict(it.districtName);
                      const b = e?.target?.getBounds?.();
                      if (b && typeof b.getSouth === "function") {
                        setFocusDistrictBounds([
                          [Number(b.getSouth()), Number(b.getWest())],
                          [Number(b.getNorth()), Number(b.getEast())],
                        ]);
                        const c = b.getCenter?.();
                        if (c) {
                          setPanelAnchor([Number(c.lat), Number(c.lng)]);
                        }
                      } else {
                        const fb = computeFeatureBounds(it.feature);
                        setFocusDistrictBounds(fb);
                        if (fb) {
                          const centerLat = (fb[0][0] + fb[1][0]) / 2;
                          const centerLon = (fb[0][1] + fb[1][1]) / 2;
                          setPanelAnchor([centerLat, centerLon]);
                        }
                      }
                      setFocusTrigger((v) => v + 1);
                      onDistrictPick?.(it.districtName);
                    },
                  }}
                />
              );
            })
          : null}

        {stations.map((s) => {
          const isSelected = Number(selectedOpenAQStationId || 0) === Number(s.id);
          return (
            <Marker
              key={`station-${s.id}`}
              position={[s.lat, s.lon]}
              eventHandlers={{
                click: () => {
                  onOpenAQStationPick?.(s);
                  if (s.district) onDistrictPick?.(s.district);
                },
              }}
              opacity={isSelected ? 1 : 0.85}
            >
              <Popup>
                <div style={{ minWidth: 220 }}>
                  <div><b>{s.name}</b></div>
                  <div>Provider: {s.provider || "-"}</div>
                  <div>District: {s.district || "-"}</div>
                  <div>Lat/Lon: {s.lat}, {s.lon}</div>
                  <div>ID: {s.id}</div>
                </div>
              </Popup>
            </Marker>
          );
        })}

        <Marker position={center} />
        <Circle center={center} radius={circle.radius} pathOptions={{ color: circle.color }} />
      </MapContainer>
    </div>
  );
}
