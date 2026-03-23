import { useEffect, useMemo, useRef, useState } from "react";
import { Marker, Popup, useMap } from "react-leaflet";
import L from "leaflet";
import { fetchOpenAQStations } from "./api";
import type { OpenAQStation } from "./api";


export default function OpenAQStationsLayer() {
  const map = useMap();
  const [stations, setStations] = useState<OpenAQStation[]>([]);
  const [err, setErr] = useState<string>("");

  // ✅ chỉ auto-fit 1 lần sau khi load
  const didFitRef = useRef(false);

  useEffect(() => {
    let mounted = true;

    fetchOpenAQStations({ limit: 200 })
      .then((data) => {
        if (!mounted) return;

        const list = Array.isArray(data?.stations) ? data.stations : [];

        // ✅ filter điểm lỗi lat/lon
        const cleaned = list.filter(
          (s) =>
            typeof s?.lat === "number" &&
            typeof s?.lon === "number" &&
            Number.isFinite(s.lat) &&
            Number.isFinite(s.lon)
        );

        setStations(cleaned);
        setErr("");
        didFitRef.current = false; // reset để fit lại khi reload data
      })
      .catch((e) => {
        if (!mounted) return;
        setErr(String(e?.message || e));
      });

    return () => {
      mounted = false;
    };
  }, []);

  const bounds = useMemo(() => {
    if (!stations.length) return null;
    const latlngs = stations.map((s) => L.latLng(s.lat, s.lon));
    return L.latLngBounds(latlngs);
  }, [stations]);

  useEffect(() => {
    if (!bounds) return;
    if (didFitRef.current) return;

    didFitRef.current = true;
    map.fitBounds(bounds, { padding: [30, 30] });
  }, [bounds, map]);

  if (err) {
    console.error("[OpenAQStationsLayer] Error:", err);
  }

  return (
    <>
      {stations.map((s) => (
        <Marker key={s.id} position={[s.lat, s.lon]}>
          <Popup>
            <div style={{ minWidth: 220 }}>
              <div>
                <b>{s.name}</b>
              </div>
              <div>Provider: {s.provider || "-"}</div>
              <div>District: {s.district || "-"}</div>
              <div>
                Lat/Lon: {s.lat}, {s.lon}
              </div>
              <div>ID: {s.id}</div>
            </div>
          </Popup>
        </Marker>
      ))}
    </>
  );
}
