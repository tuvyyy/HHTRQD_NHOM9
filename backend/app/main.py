"""FastAPI entrypoint.

Notes
-----
- Load .env robustly (đúng file backend/.env dù bạn chạy uvicorn ở thư mục nào).
- Log trạng thái token AQICN theo dạng ***last4 để dễ debug (không lộ token).
"""

from dotenv import load_dotenv
from pathlib import Path
import os

# ✅ Ưu tiên load backend/.env (ổn định dù bạn chạy uvicorn ở đâu)
_BACKEND_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(_BACKEND_ROOT / ".env", override=False)

# ✅ fallback: nếu bạn có .env ở current working dir
load_dotenv(override=False)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.routers.air_quality import router as air_router
from app.routers.ahp import router as ahp_router
from app.routers.risk import router as risk_router
from app.routers.alerts import router as alerts_router
from app.routers.risk_save import router as risk_save_router
from app.routers.risk_grid import router as risk_grid_router
from app.routers.gee import router as gee_router
from app.routers.early_warning import router as early_warning_router
from app.routers.stations import router as stations_router
from app.routers.risk_station import router as risk_station_router
from app.routers.openaq_test import router as openaq_test_router
# ✅ NEW
from app.routers.dss_run import router as dss_router
from app.routers.district import router as district_router

app = FastAPI(title="Environmental DSS API")


def _mask_token(t: str) -> str:
    t = (t or "").strip()
    if not t:
        return "(missing)"
    if len(t) <= 4:
        return "***" + t
    return "***" + t[-4:]


@app.on_event("startup")
def _startup_log():
    # AQICN token check (không lộ token)
    print(f"[startup] AQICN_TOKEN={_mask_token(os.getenv('AQICN_TOKEN',''))}")

# ✅ CORS (cho FE Vite gọi được)
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"message": "API lấy dữ liệu môi trường đang chạy"}

app.include_router(air_router, prefix="/api")
app.include_router(ahp_router, prefix="/api")
app.include_router(risk_router, prefix="/api")
app.include_router(alerts_router, prefix="/api")
app.include_router(risk_save_router, prefix="/api")
app.include_router(risk_grid_router, prefix="/api")
app.include_router(gee_router, prefix="/api")
app.include_router(early_warning_router, prefix="/api")
app.include_router(stations_router, prefix="/api")
app.include_router(risk_station_router, prefix="/api")
# ✅ NEW
app.include_router(dss_router, prefix="/api")
app.include_router(district_router, prefix="/api")
app.include_router(openaq_test_router)
