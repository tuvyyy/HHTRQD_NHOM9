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

  const staleNote = useMemo(() => {
    if (!data?.timeOfMax) return "";
    const parsed = new Date(String(data.timeOfMax));
    if (!Number.isFinite(parsed.getTime())) return "";
    if (parsed.getTime() < Date.now() - 60 * 60 * 1000) {
      return "Dữ liệu forecast chưa mới; kết quả AI chỉ nên xem như hỗ trợ vận hành.";
    }
    return "";
  }, [data]);

  return (
    <div className="card">
      <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
        <div style={{ fontWeight: 800, fontSize: 14 }}>Nguy cơ ngắn hạn (AI hỗ trợ)</div>
        <button className="btn" onClick={onCheck} disabled={loading}>
          {loading ? "Đang kiểm tra..." : "Cập nhật cảnh báo"}
        </button>
      </div>

      {error ? (
        <div style={{ marginTop: 10, color: "#ef4444", fontSize: 13 }}>
          {error}
        </div>
      ) : null}

      {!data && !error ? (
        <div style={{ marginTop: 10, color: "#6b7280", fontSize: 13 }}>
          Chưa có dữ liệu cảnh báo. Bấm <b>Cập nhật cảnh báo</b>.
        </div>
      ) : null}

      {data ? (
        <div style={{ marginTop: 10 }}>
          {staleNote ? (
            <div
              style={{
                marginBottom: 8,
                border: "1px solid #fca5a5",
                background: "#fef2f2",
                color: "#991b1b",
                borderRadius: 10,
                padding: "7px 9px",
                fontSize: 12,
                lineHeight: 1.4,
              }}
            >
              {staleNote}
            </div>
          ) : null}

          <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap" }}>
            <span style={badgeStyle(data.warning)}>
              {data.warning ? "CẦN CHÚ Ý" : "ỔN ĐỊNH"}
            </span>
            <span style={{ fontSize: 13, color: "#374151" }}>
              Mức nguy cơ ngắn hạn: <b>{data.maxLevel}</b> (điểm cao nhất: {data.maxScore})
            </span>
          </div>

          <div style={{ marginTop: 6, fontSize: 13, color: "#374151" }}>
            Khung thời gian tham chiếu: <b>{data.timeOfMax}</b>
          </div>
          <div style={{ marginTop: 6, fontSize: 12, color: "#6b7280" }}>
            Lý do chính: {data.reason}
          </div>

          <div style={{ marginTop: 10, fontWeight: 700, fontSize: 13 }}>Top 5 khung giờ cần theo dõi</div>
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
      ) : null}
    </div>
  );
}
