import { CircleMarker, Popup } from "react-leaflet";
import type { FeatureCollection } from "geojson";

type Props = {
  geojson: FeatureCollection | null;
};

export default function RiskGridLayer({ geojson }: Props) {
  const features = geojson?.features ?? [];
  if (!features.length) return null;

  return (
    <>
      {features.map((f: any, idx: number) => {
        const lat = Number(f?.properties?.lat);
        const lon = Number(f?.properties?.lon);

        if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;

        const level = String(f?.properties?.level ?? "Xanh");
        const score = f?.properties?.score_0_100;
        const latest = f?.properties?.latest_values ?? {};

        const { color, radius } = styleByLevel(level);

        return (
          <CircleMarker
            key={idx}
            center={[lat, lon]}
            radius={radius}
            pathOptions={{ color, fillColor: color, fillOpacity: 0.65, weight: 1 }}
          >
            <Popup>
              <div style={{ minWidth: 220 }}>
                <div><b>Score:</b> {score ?? "-"}</div>
                <div><b>Level:</b> {level}</div>
                <hr />
                {Object.entries(latest).map(([k, v]: any) => (
                  <div key={k}>
                    {k}: {v ?? "null"}
                  </div>
                ))}
              </div>
            </Popup>
          </CircleMarker>
        );
      })}
    </>
  );
}

function styleByLevel(level: string) {
  const lv = level.toLowerCase();
  if (lv.includes("đỏ") || lv.includes("do")) return { color: "#d32f2f", radius: 9 };
  if (lv.includes("cam")) return { color: "#f57c00", radius: 8 };
  if (lv.includes("vàng") || lv.includes("vang")) return { color: "#fbc02d", radius: 7 };
  return { color: "#2e7d32", radius: 6 };
}
