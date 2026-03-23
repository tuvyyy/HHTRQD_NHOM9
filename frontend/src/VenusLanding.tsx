import { useEffect, useRef } from "react";
import gsap from "gsap";
import { ScrollTrigger } from "gsap/ScrollTrigger";
import "./venus-landing.css";

type VenusLandingProps = {
  onOpenMap: () => void;
  onOpenCriteria: () => void;
  totalStations: number;
  scoreText: string;
  levelText: string;
  reliabilityText: string;
};

type WorkflowStage = {
  id: string;
  code: string;
  title: string;
  copy: string;
  meta: string;
};

const FIELD_CARDS = [
  { title: "Nguồn Trạm", copy: "Hợp nhất OpenAQ, AQICN, IQAir, PurpleAir để tạo lớp dữ liệu nền theo thời gian thực." },
  { title: "Chuẩn Hóa Chỉ Số", copy: "Đồng bộ dữ liệu về cùng thang điểm DSS và chuẩn hóa theo quận nội thành TP.HCM." },
  { title: "Lõi AHP C1-C4", copy: "Sử dụng ma trận tiêu chí để tính trọng số và xếp hạng mức độ ưu tiên theo quận." },
  { title: "AI Cảnh Báo Sớm", copy: "Dự báo xu hướng ngắn hạn và phát hiện vùng có nguy cơ ô nhiễm tăng nhanh." },
] as const;

const WORKFLOW_STAGES: WorkflowStage[] = [
  {
    id: "collect",
    code: "001",
    title: "Thu Thập & Làm Sạch",
    copy: "Nạp dữ liệu quan trắc theo vị trí, đồng bộ thời gian và chuẩn hóa để đưa vào pipeline phân tích.",
    meta: "Nạp dữ liệu • Kiểm định vị trí • Kiểm tra chất lượng",
  },
  {
    id: "score",
    code: "002",
    title: "Tính Điểm AHP + AI",
    copy: "Kết hợp trọng số C1-C4 với mô hình AI để đánh giá mức độ ô nhiễm và mức ưu tiên theo quận.",
    meta: "Ma trận AHP • Hợp nhất điểm • Cảnh báo sớm",
  },
  {
    id: "act",
    code: "003",
    title: "Diễn Giải & Quyết Định",
    copy: "Xuất xếp hạng, bản đồ trực quan và khuyến nghị hành động để ưu tiên kiểm tra hiện trường.",
    meta: "Xếp hạng quận • Bản đồ DSS • Hỗ trợ quyết định",
  },
];

export default function VenusLanding(props: VenusLandingProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const heroRef = useRef<HTMLElement | null>(null);
  const heroHeadlineRef = useRef<HTMLDivElement | null>(null);
  const heroMediaRef = useRef<HTMLDivElement | null>(null);
  const workflowRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    const root = rootRef.current;
    if (!root) return;

    gsap.registerPlugin(ScrollTrigger);
    ScrollTrigger.config({ ignoreMobileResize: true });

    const prefersReducedMotion =
      typeof window !== "undefined" &&
      "matchMedia" in window &&
      window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    const nav = navigator as Navigator & { connection?: { saveData?: boolean }; deviceMemory?: number };
    const saveData = nav.connection?.saveData === true;
    const lowHardware =
      (typeof navigator.hardwareConcurrency === "number" && navigator.hardwareConcurrency <= 4) ||
      (typeof nav.deviceMemory === "number" && nav.deviceMemory <= 4);
    const liteMotion = prefersReducedMotion || saveData || lowHardware;
    const cinematicMotion = !liteMotion;

    root.classList.toggle("ecoPerfLite", liteMotion);

    const ctx = gsap.context(() => {
      const revealItems = gsap.utils.toArray<HTMLElement>(".ecoReveal");
      revealItems.forEach((item, idx) => {
        gsap.from(item, {
          autoAlpha: 0,
          y: liteMotion ? 18 : 46,
          scale: liteMotion ? 1 : 0.97,
          duration: liteMotion ? 0.7 : 0.95,
          ease: "power2.out",
          delay: idx * 0.02,
          immediateRender: false,
          scrollTrigger: {
            trigger: item,
            start: "top 86%",
            toggleActions: "play none none reverse",
          },
        });
      });

      gsap.from(".fieldCard", {
        autoAlpha: 0,
        y: liteMotion ? 20 : 36,
        stagger: liteMotion ? 0.06 : 0.1,
        duration: liteMotion ? 0.55 : 0.8,
        ease: "power2.out",
        scrollTrigger: {
          trigger: "#field",
          start: "top 80%",
        },
      });

      gsap.from(".mapShell", {
        autoAlpha: 0,
        y: liteMotion ? 22 : 46,
        duration: liteMotion ? 0.65 : 0.95,
        ease: "power2.out",
        scrollTrigger: {
          trigger: "#map-intel",
          start: "top 84%",
        },
      });

      if (cinematicMotion && window.innerWidth >= 1200 && heroRef.current && heroHeadlineRef.current && heroMediaRef.current) {
        gsap
          .timeline({
            scrollTrigger: {
              trigger: heroRef.current,
              start: "top top",
              end: "bottom top",
              scrub: 0.35,
            },
          })
          .to(
            heroMediaRef.current,
            {
              scale: 1.14,
              xPercent: -4,
              yPercent: 2,
            },
            0
          )
          .to(heroHeadlineRef.current, { autoAlpha: 0.2, yPercent: -14, xPercent: -4 }, 0)
          .to(".heroGrid", { yPercent: -6 }, 0)
          .to(".heroActions", { autoAlpha: 0.42, yPercent: 10 }, 0)
          .to(".heroLowerLabel", { autoAlpha: 0.25, yPercent: 12 }, 0)
          .to(".ecoTopNav", { autoAlpha: 0.35, yPercent: -6 }, 0)
          .to(".heroContour", { yPercent: 10, ease: "none" }, 0)
          .to(".heroParallaxBack", { yPercent: 8, scale: 1.02, ease: "none" }, 0);
      }

      if (window.innerWidth >= 1180 && workflowRef.current) {
        const workflowEl = workflowRef.current;
        workflowEl.classList.add("workflowPinnedMode");
        const stages = gsap.utils.toArray<HTMLElement>(".workflowStage", workflowEl);

        if (stages.length) {
          const setActive = (activeIdx: number) => {
            stages.forEach((stage, idx) => {
              gsap.to(stage, {
                autoAlpha: idx === activeIdx ? 1 : 0.34,
                scale: idx === activeIdx ? 1 : 0.975,
                y: idx === activeIdx ? 0 : 12,
                duration: 0.28,
                overwrite: true,
                ease: "power2.out",
              });
            });
          };

          gsap.set(stages, { autoAlpha: 0.34, y: 12, scale: 0.975 });
          setActive(0);
          let lastActive = 0;

          ScrollTrigger.create({
            trigger: workflowEl,
            start: "top top+=72",
            end: () => `+=${Math.max(1200, (stages.length - 1) * 540)}`,
            pin: true,
            scrub: 0.25,
            anticipatePin: 1,
            invalidateOnRefresh: true,
            fastScrollEnd: true,
            onUpdate: (self) => {
              const idx = Math.min(stages.length - 1, Math.round(self.progress * (stages.length - 1)));
              if (idx !== lastActive) {
                lastActive = idx;
                setActive(idx);
              }
            },
          });
        }
      }

      if (cinematicMotion) {
        const mapPulseLoop = gsap.timeline({ paused: true, repeat: -1, repeatDelay: 0.2 });
        mapPulseLoop.to(".mapPulse", {
          scale: 1.2,
          opacity: 0.18,
          duration: 1.6,
          yoyo: true,
          repeat: 1,
          ease: "sine.inOut",
          stagger: 0.2,
        });

        ScrollTrigger.create({
          trigger: "#map-intel",
          start: "top 82%",
          end: "bottom 10%",
          onEnter: () => mapPulseLoop.play(),
          onEnterBack: () => mapPulseLoop.play(),
          onLeave: () => mapPulseLoop.pause(0),
          onLeaveBack: () => mapPulseLoop.pause(0),
        });
      }

      if (cinematicMotion) {
        gsap.to(".ecoContourBg", {
          yPercent: -4,
          scale: 1.1,
          ease: "none",
          scrollTrigger: {
            trigger: root,
            start: "top top",
            end: "bottom bottom",
            scrub: 0.35,
          },
        });
      }
    }, root);

    ScrollTrigger.refresh();

    return () => {
      if (workflowRef.current) {
        workflowRef.current.classList.remove("workflowPinnedMode");
      }
      ctx.revert();
      root.classList.remove("ecoPerfLite");
    };
  }, []);

  return (
    <div className="ecoLanding" ref={rootRef}>
      <div className="ecoNoise" aria-hidden="true" />
      <div className="ecoContourBg" aria-hidden="true" />

      <section className="ecoHero" ref={heroRef}>
        <div className="heroParallaxBack" aria-hidden="true" />
        <div className="heroContour" aria-hidden="true" />

        <header className="ecoHeader">
          <a href="#" className="ecoLogo" aria-label="Air Quality DSS">
            <span>AQ</span>
            <b>AHP + AI Air DSS</b>
          </a>
          <nav className="ecoTopNav" aria-label="Điều hướng trang chủ">
            <a href="#field">Dữ liệu</a>
            <a href="#workflow">Quy trình</a>
            <a href="#map-intel">Bản đồ</a>
          </nav>
        </header>

        <div className="heroGrid">
          <div className="heroCopy" ref={heroHeadlineRef}>
            <p className="heroEyebrow">HỆ HỖ TRỢ RA QUYẾT ĐỊNH Ô NHIỄM KHÔNG KHÍ</p>
            <h1>AHP + AI cho cảnh báo sớm ô nhiễm không khí nội thành TP.HCM.</h1>
            <p className="heroSub">
              Hệ thống kết hợp AHP và AI để xếp hạng ưu tiên 13 quận nội thành, hỗ trợ theo dõi
              và ra quyết định kiểm tra hiện trường theo dữ liệu thời gian thực.
            </p>
            <div className="heroActions">
              <button className="ecoBtn ecoBtnLaunch" onClick={props.onOpenMap}>
                Mở bản đồ DSS <span className="btnArrow" aria-hidden="true">→</span>
              </button>
              <button className="ecoBtn ecoBtnGhost" onClick={props.onOpenCriteria}>
                Mở trang tiêu chí AHP
              </button>
            </div>
          </div>

          <div className="heroMediaFrame" ref={heroMediaRef}>
            <div className="heroMediaOverlay" />
            <img
              loading="eager"
              decoding="async"
              src="https://images.unsplash.com/photo-1477959858617-67f85cf4f1df?auto=format&fit=crop&w=1800&q=80"
              alt="Không gian đô thị phục vụ giám sát chất lượng không khí"
            />
            <div className="heroMediaCaption">
              <span>LỚP DỮ LIỆU THỜI GIAN THỰC</span>
              <b>Nội thành TP.HCM · AHP C1-C4 · AI Early Warning</b>
            </div>
          </div>
        </div>

        <div className="heroLowerLabel">
          {props.totalStations} nguồn trực tuyến · điểm DSS {props.scoreText} · mức {props.levelText}
        </div>
      </section>

      <section className="ecoSection ecoReveal" id="field">
        <div className="sectionHead">
          <p className="sectionTag">Tầng dữ liệu hiện trường</p>
          <h2>Kết nối trạm đo, dữ liệu khí tượng và lớp phân tích theo quận.</h2>
          <p>
            Mọi nguồn dữ liệu được gom về cùng một mặt phẳng ra quyết định để phục vụ đánh giá mức độ
            ô nhiễm và phát hiện sớm khu vực cần ưu tiên xử lý.
          </p>
        </div>
        <div className="fieldGrid">
          {FIELD_CARDS.map((item) => (
            <article className="fieldCard ambientNode" key={item.title}>
              <span className="fieldDot" />
              <b>{item.title}</b>
              <p>{item.copy}</p>
            </article>
          ))}
        </div>
      </section>
      <section className="ecoSection ecoWorkflow" id="workflow" ref={workflowRef}>
        <div className="workflowSplit">
          <div className="workflowLeft">
            <p className="sectionTag">Luồng hệ thống</p>
            <h2>Quy trình từ dữ liệu đầu vào đến khuyến nghị hành động.</h2>
            <p>Cuộn chuột để xem từng bước 001 → 003 theo thứ tự trong cùng khu vực này.</p>
          </div>

          <div className="workflowRight">
            <div className="workflowStageViewport">
              {WORKFLOW_STAGES.map((stage) => (
                <article className="workflowStage" key={stage.id}>
                  <div className="workflowCode">{stage.code}</div>
                  <div>
                    <h3>{stage.title}</h3>
                    <p>{stage.copy}</p>
                    <small>{stage.meta}</small>
                  </div>
                </article>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="ecoStatement ecoReveal">
        <h2>Ưu tiên đúng quận, cảnh báo sớm đúng thời điểm.</h2>
      </section>

      <section className="ecoSection ecoReveal" id="map-intel">
        <div className="sectionHead">
          <p className="sectionTag">Bản đồ trực quan theo quận</p>
          <h2>Đọc kết quả AHP theo không gian để ra quyết định tại hiện trường.</h2>
          <p>Bản đồ thể hiện khu vực nội thành cần theo dõi trước, đi kèm mức cảnh báo và khuyến nghị.</p>
        </div>

        <div className="mapShell">
          <div className="mapLayerBase" />
          <div className="mapLayerGrid" />

          <div className="mapNode" style={{ left: "18%", top: "38%" }}>
            <span className="mapPulse" />
            <i />
          </div>
          <div className="mapNode" style={{ left: "44%", top: "24%" }}>
            <span className="mapPulse" />
            <i />
          </div>
          <div className="mapNode" style={{ left: "64%", top: "56%" }}>
            <span className="mapPulse" />
            <i />
          </div>
          <div className="mapNode" style={{ left: "82%", top: "34%" }}>
            <span className="mapPulse" />
            <i />
          </div>

          <svg className="mapLinks" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">
            <path d="M18,38 C30,24 37,23 44,24" />
            <path d="M44,24 C53,37 58,49 64,56" />
            <path d="M64,56 C73,48 78,40 82,34" />
          </svg>

          <div className="mapContent">
            <p>Lớp bản đồ quyết định theo thời gian thực</p>
            <h3>Bản đồ ưu tiên quận nội thành TP.HCM</h3>
            <button className="ecoBtn ecoBtnLaunch" onClick={props.onOpenMap}>
              Mở bản đồ DSS <span className="btnArrow" aria-hidden="true">→</span>
            </button>
          </div>
        </div>
      </section>

      <footer className="ecoFooter ecoReveal">
        <div className="footerSubscribe">
          <div className="footerSubscribeText">
            <strong>Nhận bản tin cảnh báo sớm</strong>
            <p>Cập nhật nhanh khu vực rủi ro cao và khuyến nghị ưu tiên theo quận nội thành.</p>
          </div>
          <div className="footerSubscribeForm">
            <input type="email" placeholder="Nhập email để nhận bản tin..." aria-label="Email nhận bản tin" />
            <button type="button">Đăng ký</button>
          </div>
        </div>

        <div className="footerMain">
          <div className="footerBrandCard">
            <p className="footerLabel">Air Quality Decision Support System</p>
            <h3>AHP + AI cho đánh giá ô nhiễm không khí nội thành TP.HCM</h3>
            <p>
              Nền tảng hỗ trợ ra quyết định kết hợp dữ liệu quan trắc, AHP C1-C4, bản đồ trực quan và cảnh báo sớm.
            </p>
            <a href="mailto:ops@airdss.local">ops@airdss.local</a>
          </div>

          <div className="footerCols">
            <div>
              <span>Điều hướng</span>
              <a href="#field">Dữ liệu hiện trường</a>
              <a href="#workflow">Luồng hệ thống</a>
              <a href="#map-intel">Bản đồ DSS</a>
            </div>
            <div>
              <span>Tính năng</span>
              <a href="#workflow">Xếp hạng AHP C1-C4</a>
              <a href="#map-intel">Bản đồ ưu tiên theo quận</a>
              <a href="#map-intel">Cảnh báo sớm AI</a>
            </div>
            <div>
              <span>Hỗ trợ</span>
              <a href="mailto:ops@airdss.local">Liên hệ vận hành</a>
              <a href="#workflow">Tài liệu quy trình</a>
              <a href="#map-intel">Hướng dẫn đọc bản đồ</a>
            </div>
          </div>
        </div>

        <div className="footerBottom">
          <span>© 2026 AHP + AI AIR DSS · Nhóm đồ án hỗ trợ quyết định môi trường.</span>
          <div className="footerSocials">
            <a href="#" aria-label="Facebook">Fb</a>
            <a href="#" aria-label="Zalo">Za</a>
            <a href="#" aria-label="Github">Gh</a>
          </div>
        </div>

        <h2 className="footerWordmark">AHP + AI AIR DSS</h2>
      </footer>
    </div>
  );
}












