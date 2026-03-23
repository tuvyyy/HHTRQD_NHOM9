import { useMemo } from "react";

type PointRow = {
  t: string; // time label
  pm2_5: number | null;
  pm10: number | null;
};

function toNum(v: any): number | null {
  if (v === null || v === undefined) return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeHourly(hourly: any): PointRow[] {
  if (!hourly) return [];

  // Case A: already array of rows
  if (Array.isArray(hourly)) {
    return hourly
      .map((x: any) => ({
        t: String(x.time ?? x.t ?? x.datetime ?? ""),
        pm2_5: toNum(x.pm2_5 ?? x.pm25 ?? x["PM2.5"]),
        pm10: toNum(x.pm10 ?? x["PM10"]),
      }))
      .filter((x: PointRow) => x.t);
  }

  // Case B: { items: [...] }
  if (Array.isArray(hourly.items)) {
    return hourly.items
      .map((x: any) => ({
        t: String(x.time ?? x.t ?? x.datetime ?? ""),
        pm2_5: toNum(x.pm2_5 ?? x.pm25 ?? x["PM2.5"]),
        pm10: toNum(x.pm10 ?? x["PM10"]),
      }))
      .filter((x: PointRow) => x.t);
  }

  // Case C: Open-Meteo style
  const root = hourly;
const h = root.hourly ?? root; // đôi khi backend trả thẳng hourly luôn
const times: any[] = h.time ?? h.times ?? root.time ?? root.times ?? [];
const a25: any[] = h.pm2_5 ?? h.pm25 ?? h["PM2.5"] ?? [];
const a10: any[] = h.pm10 ?? h["PM10"] ?? [];

  if (Array.isArray(times) && times.length) {
    const n = times.length;
    const rows: PointRow[] = [];
    for (let i = 0; i < n; i++) {
      rows.push({
        t: String(times[i]),
        pm2_5: toNum(a25?.[i]),
        pm10: toNum(a10?.[i]),
      });
    }
    return rows.filter((r) => r.pm2_5 !== null || r.pm10 !== null);
  }

  return [];
}

function formatTimeLabel(t: string) {
  const m = t.match(/T(\d{2}):(\d{2})/);
  if (m) return `${m[1]}:${m[2]}`;
  return t;
}

export default function HourlyChart({ hourly }: { hourly: any }) {
  const data = useMemo(() => normalizeHourly(hourly), [hourly]);

  const { minV, maxV } = useMemo(() => {
    let min = Infinity;
    let max = -Infinity;
    for (const r of data) {
      if (r.pm2_5 !== null) {
        min = Math.min(min, r.pm2_5);
        max = Math.max(max, r.pm2_5);
      }
      if (r.pm10 !== null) {
        min = Math.min(min, r.pm10);
        max = Math.max(max, r.pm10);
      }
    }
    if (!Number.isFinite(min) || !Number.isFinite(max)) return { minV: 0, maxV: 1 };
    if (min === max) return { minV: min - 1, maxV: max + 1 };
    return { minV: min, maxV: max };
  }, [data]);

  if (!data.length) {
    return <div style={{ color: "#6b7280", padding: 10 }}>Chưa có hourly data.</div>;
  }

  const W = 900;
  const H = 220;
  const padL = 44;
  const padR = 12;
  const padT = 14;
  const padB = 28;

  const innerW = W - padL - padR;
  const innerH = H - padT - padB;

  const xAt = (i: number) => padL + (i * innerW) / Math.max(1, data.length - 1);
  const yAt = (v: number) => padT + ((maxV - v) * innerH) / (maxV - minV);

  const poly = (key: "pm2_5" | "pm10") => {
    const pts: string[] = [];
    data.forEach((r, i) => {
      const v = r[key];
      if (v === null) return;
      pts.push(`${xAt(i)},${yAt(v)}`);
    });
    return pts.join(" ");
  };

  const yTicks = 4;
  const tickVals = Array.from({ length: yTicks + 1 }, (_, i) => minV + ((maxV - minV) * i) / yTicks);
  const xLabelsEvery = Math.max(1, Math.floor(data.length / 6));

  return (
    // ✅ CHỈ đổi chỗ này: cho chart tự ăn theo container ngoài
    <div style={{ width: "100%", height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div style={{ display: "flex", gap: 12, alignItems: "center", padding: "6px 10px" }}>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12 }}>
          <span style={{ width: 10, height: 10, borderRadius: 999, background: "#2563eb" }} />
          PM2.5
        </span>
        <span style={{ display: "inline-flex", alignItems: "center", gap: 6, fontSize: 12 }}>
          <span style={{ width: 10, height: 10, borderRadius: 999, background: "#f97316" }} />
          PM10
        </span>
      </div>

      <div style={{ flex: 1, minHeight: 0 }}>
        <svg
          viewBox={`0 0 ${W} ${H}`}
          width="100%"
          height="100%"
          preserveAspectRatio="none"
          style={{ display: "block" }}
        >
          {tickVals.map((v, idx) => {
            const y = yAt(v);
            return (
              <g key={idx}>
                <line x1={padL} y1={y} x2={W - padR} y2={y} stroke="#e5e7eb" strokeWidth="1" />
                <text x={8} y={y + 4} fontSize="11" fill="#6b7280">
                  {v.toFixed(0)}
                </text>
              </g>
            );
          })}

          <line x1={padL} y1={H - padB} x2={W - padR} y2={H - padB} stroke="#e5e7eb" strokeWidth="1" />

          {data.map((r, i) => {
            if (i % xLabelsEvery !== 0 && i !== data.length - 1) return null;
            const x = xAt(i);
            return (
              <text key={i} x={x} y={H - 8} fontSize="11" fill="#6b7280" textAnchor="middle">
                {formatTimeLabel(r.t)}
              </text>
            );
          })}

          <polyline fill="none" stroke="#2563eb" strokeWidth="2.2" points={poly("pm2_5")} />
          <polyline fill="none" stroke="#f97316" strokeWidth="2.2" points={poly("pm10")} />

          {data.map((r, i) => {
            const x = xAt(i);
            return (
              <g key={i}>
                {r.pm2_5 !== null && <circle cx={x} cy={yAt(r.pm2_5)} r="2.4" fill="#2563eb" />}
                {r.pm10 !== null && <circle cx={x} cy={yAt(r.pm10)} r="2.4" fill="#f97316" />}
              </g>
            );
          })}
        </svg>
      </div>
    </div>
  );
}