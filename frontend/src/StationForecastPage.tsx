import type { OpenAQStation } from "./api";

type Props = {
  open: boolean;
  station: OpenAQStation | null;
  quick: any | null;
  loading: boolean;
  error: string | null;
  onClose: () => void;
  onRefresh: () => void;
  onUseCoords?: () => void;
};

export default function StationForecastPage({
  open,
  station,
  quick,
  loading,
  error,
  onClose,
  onRefresh,
  onUseCoords,
}: Props) {
  if (!open) return null;

  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.4)",
        zIndex: 9999,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: 16,
      }}
      onClick={onClose}
    >
      <div
        style={{
          width: "min(760px, 100%)",
          maxHeight: "85vh",
          overflow: "auto",
          background: "#fff",
          borderRadius: 14,
          padding: 16,
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: 8 }}>
          <h3 style={{ margin: 0 }}>Chi tiết trạm (tạm thời)</h3>
          <button onClick={onClose}>Đóng</button>
        </div>

        <div style={{ marginTop: 10, fontSize: 14 }}>
          <div>
            <b>Trạm:</b> {station?.name ?? "Chưa chọn"}
          </div>
          <div>
            <b>Tọa độ:</b> {station ? `${station.lat}, ${station.lon}` : "-"}
          </div>
          <div>
            <b>Quận:</b> {station?.district ?? "-"}
          </div>
        </div>

        <div style={{ marginTop: 12, display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button onClick={onRefresh} disabled={!station || loading}>
            {loading ? "Đang tải..." : "Làm mới"}
          </button>
          <button onClick={onUseCoords} disabled={!onUseCoords}>
            Dùng tọa độ trạm
          </button>
        </div>

        {error ? (
          <div style={{ marginTop: 10, color: "#b91c1c" }}>
            <b>Lỗi:</b> {error}
          </div>
        ) : null}

        {quick ? (
          <pre
            style={{
              marginTop: 12,
              background: "#f8fafc",
              border: "1px solid #e2e8f0",
              borderRadius: 10,
              padding: 12,
              fontSize: 12,
              overflow: "auto",
            }}
          >
            {JSON.stringify(quick, null, 2)}
          </pre>
        ) : null}
      </div>
    </div>
  );
}
