type Props = {
  lat: number;
  lon: number;
  hours: number;
  weights: Record<string, number>;
};

export default function AiChatPanel({ lat, lon, hours, weights }: Props) {
  return (
    <div
      style={{
        border: "1px solid #dbe3ef",
        borderRadius: 12,
        padding: 12,
        background: "#f8fbff",
      }}
    >
      <div style={{ fontWeight: 700, marginBottom: 6 }}>AI Chat (tạm thời)</div>
      <div style={{ fontSize: 13, color: "#4b5563", lineHeight: 1.5 }}>
        File chat gốc đang thiếu trong bản backup. Panel này là bản tạm để không vỡ giao diện.
      </div>
      <div style={{ fontSize: 12, color: "#6b7280", marginTop: 8 }}>
        Vị trí: {lat.toFixed(4)}, {lon.toFixed(4)} · {hours} giờ · Trọng số:{" "}
        {Object.entries(weights)
          .map(([k, v]) => `${k}=${Number(v).toFixed(2)}`)
          .join(", ")}
      </div>
    </div>
  );
}
