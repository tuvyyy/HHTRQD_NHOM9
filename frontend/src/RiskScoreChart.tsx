type Point = {
  time: string;
  risk_score_0_100: number;
};

type Props = {
  forecast?: Point[];
  baseline?: Point[];
};

function summarize(points: Point[] | undefined) {
  if (!points || points.length === 0) return "Không có dữ liệu";
  const vals = points.map((p) => Number(p.risk_score_0_100) || 0);
  const max = Math.max(...vals).toFixed(1);
  const min = Math.min(...vals).toFixed(1);
  const last = vals[vals.length - 1].toFixed(1);
  return `min ${min} · max ${max} · cuối ${last}`;
}

export default function RiskScoreChart({ forecast, baseline }: Props) {
  return (
    <div
      style={{
        width: "100%",
        height: "100%",
        borderRadius: 10,
        background: "#fff",
        padding: 10,
        fontSize: 12,
        color: "#334155",
        overflow: "auto",
      }}
    >
      <div style={{ fontWeight: 700, marginBottom: 6 }}>Biểu đồ rủi ro (tạm thời)</div>
      <div>Dự báo: {summarize(forecast)}</div>
      <div style={{ marginTop: 4 }}>Baseline: {summarize(baseline)}</div>
    </div>
  );
}
