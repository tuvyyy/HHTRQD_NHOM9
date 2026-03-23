import { useEffect, useMemo, useRef, useState } from "react";

type Props = {
  placeholder?: string;
  onPick: (lat: number, lon: number, label?: string) => void;
  disabled?: boolean;
  value?: string;
  onValueChange?: (value: string) => void;
  compact?: boolean;
  hideHint?: boolean;
};

type NominatimItem = {
  place_id: number;
  display_name: string;
  lat: string;
  lon: string;
};

const HCM_BOUNDS = {
  south: 10.33,
  west: 106.35,
  north: 11.17,
  east: 107.05,
};

export default function AddressSearch({
  placeholder,
  onPick,
  disabled,
  value,
  onValueChange,
  compact = false,
  hideHint = false,
}: Props) {
  const [q, setQ] = useState("");
  const [items, setItems] = useState<NominatimItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [open, setOpen] = useState(false);

  const debounceRef = useRef<number | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const viewbox = useMemo(() => {
    return `${HCM_BOUNDS.west},${HCM_BOUNDS.north},${HCM_BOUNDS.east},${HCM_BOUNDS.south}`;
  }, []);

  async function doSearch(query: string) {
    const term = query.trim();
    if (!term) {
      setItems([]);
      setErr(null);
      return;
    }

    try {
      setLoading(true);
      setErr(null);

      abortRef.current?.abort();
      const ac = new AbortController();
      abortRef.current = ac;

      const url = new URL("https://nominatim.openstreetmap.org/search");
      url.searchParams.set("format", "jsonv2");
      url.searchParams.set("q", term);
      url.searchParams.set("limit", "6");
      url.searchParams.set("countrycodes", "vn");
      url.searchParams.set("addressdetails", "1");
      url.searchParams.set("viewbox", viewbox);
      url.searchParams.set("bounded", "1");

      const res = await fetch(url.toString(), {
        signal: ac.signal,
        headers: {
          "Accept-Language": "vi,en;q=0.8",
        },
      });

      if (!res.ok) throw new Error(`Geocode failed (${res.status})`);
      const data = (await res.json()) as NominatimItem[];
      setItems(Array.isArray(data) ? data : []);
      setOpen(true);
    } catch (e: any) {
      if (e?.name === "AbortError") return;
      setErr(e?.message || "Geocode error");
      setItems([]);
      setOpen(true);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    if (typeof value !== "string") return;
    setQ(value);
  }, [value]);

  useEffect(() => {
    if (disabled) return;
    if (debounceRef.current) window.clearTimeout(debounceRef.current);
    debounceRef.current = window.setTimeout(() => doSearch(q), 350);
    return () => {
      if (debounceRef.current) window.clearTimeout(debounceRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [q, disabled]);

  function pick(it: NominatimItem) {
    const lat = Number(it.lat);
    const lon = Number(it.lon);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;
    onPick(lat, lon, it.display_name);
    setOpen(false);
    setItems([]);
  }

  return (
    <div style={{ position: "relative" }}>
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <input
          className="input"
          value={q}
          disabled={disabled}
          placeholder={placeholder || "Tim dia chi trong TP.HCM"}
          onChange={(e) => {
            const v = e.target.value;
            setQ(v);
            onValueChange?.(v);
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter" && items.length) pick(items[0]);
            if (e.key === "Escape") setOpen(false);
          }}
          style={compact ? { padding: "7px 10px", borderRadius: 10, fontSize: 13 } : undefined}
        />
      </div>

      {!hideHint ? (
        <div style={{ fontSize: 11, color: "#6b7280", marginTop: 6 }}>
          * Geocode bang Nominatim (OSM), gioi han trong TP.HCM.
        </div>
      ) : null}

      {loading ? <div style={{ fontSize: 11, color: "#6b7280", marginTop: hideHint ? 4 : 6 }}>Dang tim...</div> : null}
      {err ? <div style={{ fontSize: 11, color: "#b91c1c", marginTop: hideHint ? 4 : 6 }}>{err}</div> : null}

      {open && items.length > 0 ? (
        <div
          style={{
            position: "absolute",
            top: compact ? 38 : 44,
            left: 0,
            right: 0,
            zIndex: 50,
            background: "white",
            border: "1px solid #e5e7eb",
            borderRadius: 10,
            boxShadow: "0 6px 18px rgba(0,0,0,0.08)",
            overflow: "hidden",
          }}
        >
          {items.map((it) => (
            <div
              key={it.place_id}
              onClick={() => pick(it)}
              style={{
                padding: "10px 12px",
                cursor: "pointer",
                borderBottom: "1px solid #f3f4f6",
                fontSize: 12,
                color: "#111827",
              }}
            >
              {it.display_name}
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}
