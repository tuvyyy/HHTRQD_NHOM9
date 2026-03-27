import { useMemo, useRef, useState, type KeyboardEvent } from "react";
import { aiChat, type AiChatMessage } from "./api";

type DecisionRow = {
  districtName: string;
  rank: number;
  score: number;
  C1?: number;
  C2?: number;
  C3?: number;
  C4?: number;
};

type Props = {
  lat: number;
  lon: number;
  hours: number;
  weights: Record<string, number>;
  decisionDate?: string;
  rankingSource?: string;
  districtRows?: DecisionRow[];
  forecastSeries?: Array<{ time: string; risk_score_0_100: number }>;
};

type ChatMessage = {
  role: "user" | "assistant";
  content: string;
};

function parseAssistantMessage(content: string): { text: string; staleWarning: boolean } {
  const raw = String(content || "");
  const staleWarning = /\[warning_data_stale\]/i.test(raw);
  const text = raw.replace(/\[warning_data_stale\]/gi, "").trim();
  return { text, staleWarning };
}

const STARTER_MESSAGE =
  "Mình là trợ lý AI của DSS. AHP dùng để đánh giá hiện trạng theo quận; AI hỗ trợ diễn giải và nhận diện nguy cơ ngắn hạn theo dữ liệu gần. Nếu dữ liệu thiếu hoặc forecast cũ, mình sẽ nói rõ độ tin cậy giảm.";

export default function AiChatPanel({
  lat,
  lon,
  hours,
  weights,
  decisionDate,
  rankingSource,
  districtRows,
  forecastSeries,
}: Props) {
  const [messages, setMessages] = useState<ChatMessage[]>([{ role: "assistant", content: STARTER_MESSAGE }]);
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const listRef = useRef<HTMLDivElement | null>(null);

  const normalizedRows = useMemo(() => {
    return (districtRows || [])
      .map((r, idx) => ({
        districtName: String(r.districtName || "").trim(),
        rank: Number(r.rank || idx + 1),
        score: Number(r.score || 0),
        C1: Number(r.C1 || 0),
        C2: Number(r.C2 || 0),
        C3: Number(r.C3 || 0),
        C4: Number(r.C4 || 0),
      }))
      .filter((r) => r.districtName)
      .sort((a, b) => Number(a.rank || 9999) - Number(b.rank || 9999));
  }, [districtRows]);

  const hasForecastData = useMemo(() => (forecastSeries || []).length >= 2, [forecastSeries]);

  const contextText = useMemo(() => {
    const weightText = Object.entries(weights)
      .map(([k, v]) => `${k}=${Number(v).toFixed(2)}`)
      .join(", ");
    const top1 = normalizedRows[0];
    const top1Text = top1 ? `${top1.districtName} (#${top1.rank}, ${top1.score.toFixed(6)})` : "chưa có";
    const sourceText = rankingSource || "AHP";
    const dateText = decisionDate ? ` · Ngày: ${decisionDate}` : "";
    return `Vị trí: ${lat.toFixed(4)}, ${lon.toFixed(4)} · Cửa sổ: ${hours} giờ · Trọng số: ${weightText}${dateText} · Nguồn xếp hạng: ${sourceText} · Top 1 hiện tại: ${top1Text}`;
  }, [lat, lon, hours, weights, normalizedRows, rankingSource, decisionDate]);

  const suggestedQuestions = useMemo(
    () => [
      "Quận nào đang cần ưu tiên theo dõi nhất hiện tại?",
      "Top 3 quận ưu tiên hiện tại là những quận nào?",
      "Vì sao Quận 4 đang cao hơn Quận 1 ở thời điểm hiện tại?",
      "Trong 6 giờ tới, khu vực nào đáng chú ý hơn hiện tại?",
      "Có quận nào hạng chưa cao nhưng cần theo dõi thêm trong ngắn hạn không?",
      "Quận nào hạng cao theo AHP nhưng ngắn hạn chưa thấy tín hiệu tăng thêm?",
      "Forecast hiện tại có mới không, độ tin cậy ra sao?",
      "Có nên đi chạy bộ ngoài trời lúc 17:00 ở Quận 7 không?",
    ],
    []
  );

  function scrollToBottom() {
    requestAnimationFrame(() => {
      const el = listRef.current;
      if (!el) return;
      el.scrollTop = el.scrollHeight;
    });
  }

  async function handleSend() {
    const input = text.trim();
    if (!input || loading) return;
    setError(null);
    setText("");

    const nextMessages: ChatMessage[] = [...messages, { role: "user", content: input }];
    setMessages(nextMessages);
    scrollToBottom();
    setLoading(true);

    try {
      const reqMessages: AiChatMessage[] = nextMessages.slice(-12).map((m) => ({
        role: m.role,
        content: m.content,
      }));

      const res = await aiChat({
        messages: reqMessages,
        lat,
        lon,
        hours,
        weights,
        decision_date: decisionDate,
        ranking_source: rankingSource,
        district_rows: normalizedRows,
        forecast_series: (forecastSeries || []).slice(0, 72),
      });

      setMessages((prev) => [...prev, { role: "assistant", content: res.reply }]);
      scrollToBottom();
    } catch (e: any) {
      const raw = e?.response?.data?.detail || e?.response?.data?.message || e?.message || "Không gọi được AI chat.";
      const safeMsg =
        typeof raw === "string" && /request failed|status code|500|network error/i.test(raw)
          ? "AI đang tạm thời gián đoạn kết nối. Bạn thử lại sau vài giây."
          : String(raw);
      setError(safeMsg);
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: "Mình chưa kết nối được mô hình AI. Bạn kiểm tra backend và dịch vụ AI rồi thử lại giúp mình.",
        },
      ]);
      scrollToBottom();
    } finally {
      setLoading(false);
    }
  }

  function handleKeyDown(e: KeyboardEvent<HTMLTextAreaElement>) {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      void handleSend();
    }
  }

  return (
    <div
      style={{
        border: "1px solid #dbe3ef",
        borderRadius: 12,
        padding: 14,
        background: "#f8fbff",
        height: "100%",
        minHeight: 0,
        display: "flex",
        flexDirection: "column",
      }}
    >
      <div style={{ fontWeight: 800, fontSize: 20, marginBottom: 4 }}>AI hỗ trợ cảnh báo ngắn hạn</div>
      <div style={{ fontSize: 14, color: "#475569", marginBottom: 10, lineHeight: 1.5 }}>
        AHP cho biết hiện trạng theo quận. AI hỗ trợ đọc xu hướng gần và gợi ý hành động ngắn hạn.
      </div>

      <div
        style={{
          border: "1px solid #dbeafe",
          background: "#eff6ff",
          borderRadius: 10,
          padding: "8px 10px",
          marginBottom: 10,
          fontSize: 13,
          color: "#1e3a8a",
          lineHeight: 1.45,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 4 }}>AI đang dùng dữ liệu AHP hiện tại và xu hướng gần</div>
        <div>{contextText}</div>
      </div>

      {!normalizedRows.length || !hasForecastData ? (
        <div
          style={{
            border: "1px solid #fde68a",
            background: "#fffbeb",
            borderRadius: 10,
            padding: "8px 10px",
            marginBottom: 10,
            fontSize: 13,
            color: "#92400e",
            lineHeight: 1.45,
          }}
        >
          Dữ liệu ngữ cảnh chưa đầy đủ hoặc chưa mới. AI sẽ trả lời theo hướng thận trọng và có thể nêu rõ
          “chưa đủ cơ sở để kết luận mạnh”.
        </div>
      ) : null}

      <div
        style={{
          border: "1px solid #e2e8f0",
          background: "#f8fafc",
          borderRadius: 10,
          padding: "8px 10px",
          marginBottom: 10,
          fontSize: 13,
          color: "#334155",
          lineHeight: 1.45,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 4 }}>Gợi ý câu hỏi</div>
        {suggestedQuestions.map((q) => (
          <div key={q}>• {q}</div>
        ))}
      </div>

      <div
        ref={listRef}
        style={{
          flex: 1,
          minHeight: 0,
          overflowY: "auto",
          border: "1px solid #e5e7eb",
          borderRadius: 10,
          padding: 12,
          background: "#fff",
        }}
      >
        {messages.map((m, idx) => (
          <div
            key={`${m.role}-${idx}`}
            style={{
              marginBottom: 8,
              display: "flex",
              justifyContent: m.role === "user" ? "flex-end" : "flex-start",
            }}
          >
            {(() => {
              const parsed = m.role === "assistant" ? parseAssistantMessage(m.content) : { text: m.content, staleWarning: false };
              return (
                <div
                  style={{
                    maxWidth: "88%",
                    borderRadius: 10,
                    padding: "10px 12px",
                    whiteSpace: "pre-wrap",
                    lineHeight: 1.45,
                    background: m.role === "user" ? "#dbeafe" : "#eef2ff",
                    color: "#0f172a",
                    border: "1px solid #dbeafe",
                  }}
                >
                  {parsed.staleWarning ? (
                    <div
                      style={{
                        marginBottom: 6,
                        display: "inline-block",
                        padding: "2px 8px",
                        borderRadius: 999,
                        fontSize: 12,
                        fontWeight: 700,
                        color: "#991b1b",
                        background: "#fee2e2",
                        border: "1px solid #fca5a5",
                      }}
                    >
                      Dữ liệu forecast chưa mới
                    </div>
                  ) : null}
                  <div>{parsed.text}</div>
                </div>
              );
            })()}
          </div>
        ))}
        {loading ? <div style={{ fontSize: 13, color: "#6b7280" }}>Đang trả lời...</div> : null}
      </div>

      <div style={{ marginTop: 10, display: "flex", gap: 8 }}>
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Hỏi về hiện trạng AHP, nguy cơ ngắn hạn, dữ liệu đang dùng, hoặc nên/không nên làm gì..."
          rows={2}
          style={{
            flex: 1,
            resize: "none",
            borderRadius: 10,
            border: "1px solid #cbd5e1",
            padding: "8px 10px",
            fontSize: 14,
            outline: "none",
          }}
        />
        <button className="btn" onClick={() => void handleSend()} disabled={loading || !text.trim()}>
          Gửi
        </button>
      </div>

      {error ? <div style={{ marginTop: 8, color: "#b91c1c", fontSize: 13 }}>{error}</div> : null}
    </div>
  );
}
