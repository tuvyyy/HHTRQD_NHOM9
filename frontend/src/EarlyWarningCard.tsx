import { useMemo } from "react";
import type { EarlyWarningRes } from "./api";

function badgeStyle(warning: boolean) {
  return {
    display: "inline-block",
    padding: "4px 10px",
    borderRadius: 999,
    fontWeight: 700,
    fontSize: 12,
    color: warning ? "#fff" : "#111827",
    background: warning ? "#ef4444" : "#22c55e",
  } as const;
}

export default function EarlyWarningCard({
  data,
  loading,
  error,
  onCheck,
}: {
  data: EarlyWarningRes | null;
  loading: boolean;
  error: string | null;
  onCheck: () => void;
}) {
  const top5 = useMemo(() => {
    if (!data?.series) return [];
    return [...data.series]
      .sort((a, b) => b.score_0_100 - a.score_0_100)
      .slice(0, 5);
  }, [data]);

  return (
    <div className="card">
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
        <div style={{ fontWeight: 800, fontSize: 14 }}>Early Warning</div>
        <button className="btn" onClick={onCheck} disabled={loading}>
          {loading ? "Checking..." : "Check warning"}
        </button>
      </div>

      {error && (
        <div style={{ marginTop: 10, color: "#ef4444", fontSize: 13 }}>
          {error}
        </div>
      )}

      {!data && !error && (
        <div style={{ marginTop: 10, color: "#6b7280", fontSize: 13 }}>
          Chưa có dữ liệu cảnh báo. Bấm <b>Check warning</b>.
        </div>
      )}

      {data && (
        <div style={{ marginTop: 10 }}>
          <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
            <span style={badgeStyle(data.warning)}>
              {data.warning ? "WARNING" : "SAFE"}
            </span>
            <span style={{ fontSize: 13, color: "#374151" }}>
              maxScore: <b>{data.maxScore}</b> ({data.maxLevel})
            </span>
          </div>

          <div style={{ marginTop: 6, fontSize: 13, color: "#374151" }}>
            timeOfMax: <b>{data.timeOfMax}</b>
          </div>
          <div style={{ marginTop: 6, fontSize: 12, color: "#6b7280" }}>
            reason: {data.reason}
          </div>

          <div style={{ marginTop: 10, fontWeight: 700, fontSize: 13 }}>Top 5 hours</div>
          <div style={{ marginTop: 6, display: "grid", gap: 6 }}>
            {top5.map((x) => (
              <div
                key={x.time}
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  gap: 10,
                  fontSize: 13,
                  padding: "6px 8px",
                  border: "1px solid #e5e7eb",
                  borderRadius: 10,
                  background: "#fff",
                }}
              >
                <span style={{ color: "#374151" }}>{x.time}</span>
                <span style={{ fontWeight: 800 }}>{x.score_0_100} ({x.level})</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
