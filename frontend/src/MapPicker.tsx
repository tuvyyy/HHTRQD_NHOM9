import { useEffect, useMemo, useState } from "react";
import { Circle, GeoJSON, MapContainer, Marker, Popup, TileLayer, useMapEvents } from "react-leaflet";
import type { LatLngBoundsExpression, LatLngExpression } from "leaflet";
import type { Feature, FeatureCollection, MultiPolygon, Point, Polygon as GeoPolygon } from "geojson";

import RiskGridLayer from "./RiskGridLayer";
import { fetchOpenAQStations, getStationsBounds, type OpenAQStation } from "./api";

export type DistrictMapResultItem = {
  districtName: string;
  rank: number;
  score: number;
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
};

type DistrictBoundaryProps = {
  districtId?: number;
  districtName?: string;
  aliases?: string[];
  [k: string]: unknown;
};

type DistrictBoundaryFeature = Feature<GeoPolygon | MultiPolygon, DistrictBoundaryProps>;
type DistrictBoundaryCollection = FeatureCollection<GeoPolygon | MultiPolygon, DistrictBoundaryProps>;

const DISTRICT_BORDER_COLORS = [
  "#2563eb",
  "#ef4444",
  "#22c55e",
  "#f59e0b",
  "#a855f7",
  "#06b6d4",
  "#e11d48",
  "#10b981",
  "#f97316",
  "#8b5cf6",
  "#0ea5e9",
  "#84cc16",
  "#ec4899",
];

type BoundsTuple = [[number, number], [number, number]];

const HCM_BOUNDS: BoundsTuple = [
  [10.33, 106.35],
  [11.17, 107.05],
];

function normalizeKey(v: string) {
  return String(v || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

function escapeHtml(v: string) {
  return String(v || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
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

function pointKey(p: number[], precision = 8) {
  return `${Number(p[0]).toFixed(precision)},${Number(p[1]).toFixed(precision)}`;
}

function ringAbsArea(ring: number[][]) {
  if (!ring.length) return 0;
  let sum = 0;
  for (let i = 0; i < ring.length - 1; i += 1) {
    const p1 = ring[i];
    const p2 = ring[i + 1];
    sum += p1[0] * p2[1] - p2[0] * p1[1];
  }
  return Math.abs(sum / 2);
}

function buildInnerCityOuterRing(features: DistrictBoundaryFeature[]): number[][] | null {
  const edgeCount = new Map<string, number>();
  const points = new Map<string, number[]>();

  const edgeKey = (a: string, b: string) => (a < b ? `${a}|${b}` : `${b}|${a}`);

  for (const feature of features) {
    const rings = extractOuterRings(feature.geometry).map((r) => closeRing(r)).filter((r) => r.length >= 4);
    for (const ring of rings) {
      for (let i = 0; i < ring.length - 1; i += 1) {
        const p1 = ring[i];
        const p2 = ring[i + 1];
        const k1 = pointKey(p1);
        const k2 = pointKey(p2);
        if (!points.has(k1)) points.set(k1, [Number(k1.split(",")[0]), Number(k1.split(",")[1])]);
        if (!points.has(k2)) points.set(k2, [Number(k2.split(",")[0]), Number(k2.split(",")[1])]);
        const ek = edgeKey(k1, k2);
        edgeCount.set(ek, (edgeCount.get(ek) || 0) + 1);
      }
    }
  }

  const adjacency = new Map<string, Set<string>>();
  for (const [k, c] of edgeCount.entries()) {
    if (c !== 1) continue;
    const [a, b] = k.split("|");
    if (!adjacency.has(a)) adjacency.set(a, new Set());
    if (!adjacency.has(b)) adjacency.set(b, new Set());
    adjacency.get(a)!.add(b);
    adjacency.get(b)!.add(a);
  }
  if (!adjacency.size) return null;

  const visited = new Set<string>();
  const loops: number[][][] = [];

  for (const [start, neighSet] of adjacency.entries()) {
    for (const first of neighSet) {
      const startEdge = edgeKey(start, first);
      if (visited.has(startEdge)) continue;
      visited.add(startEdge);

      const keyLoop: string[] = [start];
      let prev = start;
      let curr = first;
      let safe = 0;

      while (safe < 20000) {
        safe += 1;
        keyLoop.push(curr);
        if (curr === start) break;

        const nextCandidates = [...(adjacency.get(curr) || [])].filter((k) => k !== prev);
        if (!nextCandidates.length) break;
        const next = nextCandidates.find((k) => !visited.has(edgeKey(curr, k))) ?? nextCandidates[0];
        const ek = edgeKey(curr, next);
        if (visited.has(ek) && next !== start) break;
        visited.add(ek);
        prev = curr;
        curr = next;
      }

      if (keyLoop.length > 3 && keyLoop[0] === keyLoop[keyLoop.length - 1]) {
        const ring = keyLoop.map((k) => points.get(k)).filter((p): p is number[] => Array.isArray(p));
        if (ring.length >= 4) loops.push(closeRing(ring));
      }
    }
  }

  if (!loops.length) return null;
  loops.sort((a, b) => ringAbsArea(b) - ringAbsArea(a));
  return loops[0];
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
        borderColor,
        fillColor,
        rankText: matched ? `#${matched.rank}` : "-",
        scoreText: matched ? Number(matched.score || 0).toFixed(5) : "-",
      };
    });
  }, [districtBoundaries, districtResultItems, districtRowsByKey]);

  const districtOutsideMaskFeature = useMemo(() => {
    if (!districtBoundaryPaintItems.length) return null;

    const [sw, ne] = HCM_BOUNDS;
    const outerRing: number[][] = [
      [sw[1], sw[0]],
      [ne[1], sw[0]],
      [ne[1], ne[0]],
      [sw[1], ne[0]],
      [sw[1], sw[0]],
    ];

    const innerOuterRing = buildInnerCityOuterRing(districtBoundaryPaintItems.map((it) => it.feature));
    if (!innerOuterRing || innerOuterRing.length < 4) return null;

    return {
      type: "Feature" as const,
      properties: {},
      geometry: {
        type: "Polygon" as const,
        coordinates: [outerRing, innerOuterRing],
      },
    };
  }, [districtBoundaryPaintItems]);

  const topDistricts = useMemo(() => {
    return [...(districtResultItems || [])]
      .sort((a, b) => Number(a.rank || 0) - Number(b.rank || 0))
      .slice(0, 3)
      .map((x) => x.districtName);
  }, [districtResultItems]);

  return (
    <div style={{ width: "100%", height: "100%", position: "relative" }}>
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
        <RiskGridLayer geojson={grid ?? null} />

        {showDistrictLayer && districtOutsideMaskFeature ? (
          <GeoJSON
            data={districtOutsideMaskFeature}
            style={{
              color: "transparent",
              weight: 0,
              fillColor: "#6b7280",
              fillOpacity: 0.66,
              interactive: false,
            }}
          />
        ) : null}

        {showDistrictLayer
          ? districtBoundaryPaintItems.map((it) => {
              const selected = normalizeKey(pickedDistrict) === it.key;
              const popupHtml = `<div style="min-width:170px"><div><b>${escapeHtml(it.districtName)}</b></div><div>Hạng: ${escapeHtml(it.rankText)}</div><div>Điểm: ${escapeHtml(it.scoreText)}</div></div>`;
              return (
                <GeoJSON
                  key={`district-geo-${it.key}`}
                  data={it.feature as DistrictBoundaryFeature}
                  style={{
                    color: it.borderColor,
                    weight: selected ? 3.4 : 2.2,
                    fillColor: it.fillColor,
                    fillOpacity: 0.52,
                  }}
                  onEachFeature={(_, layer) => {
                    layer.bindTooltip(it.districtName, { sticky: true, direction: "top" });
                    layer.bindPopup(popupHtml);
                  }}
                  eventHandlers={{
                    click: () => {
                      setPickedDistrict(it.districtName);
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
