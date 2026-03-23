import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ✅ Load .env ổn định (đúng backend/.env)
_BACKEND_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(_BACKEND_ROOT / ".env", override=False)
load_dotenv(override=False)

DB_SERVER = os.getenv("DB_SERVER", r"localhost\SQLEXPRESS")
DB_NAME = os.getenv("DB_NAME", "DSS_AirQuality")
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
TRUST_CERT = os.getenv("DB_TRUST_CERT", "yes").lower() in ("1", "true", "yes")

connection_url = (
    f"mssql+pyodbc://@{DB_SERVER}/{DB_NAME}"
    f"?driver={ODBC_DRIVER}"
    f"&Trusted_Connection=yes"
)

if TRUST_CERT:
    connection_url += "&TrustServerCertificate=yes"

_ENGINE = None
_ENGINE_ERROR = None


def get_engine():
    """Lazy init DB engine.

    Lý do: tránh crash toàn bộ API khi máy chưa cài ODBC/pyodbc hoặc chưa có SQL Server.
    Các endpoint DB sẽ trả lỗi rõ ràng, nhưng các endpoint khác (Open-Meteo/AQICN) vẫn chạy được.
    """
    global _ENGINE, _ENGINE_ERROR
    if _ENGINE is not None:
        return _ENGINE
    if _ENGINE_ERROR is not None:
        raise RuntimeError(_ENGINE_ERROR)
    try:
        _ENGINE = create_engine(connection_url, pool_pre_ping=True)
        return _ENGINE
    except Exception as e:
        _ENGINE_ERROR = f"DB engine init failed: {type(e).__name__}: {e}"
        raise RuntimeError(_ENGINE_ERROR)


# Backward-compat: giữ tên biến `engine` như cũ
try:
    engine = get_engine()
except Exception:
    engine = None

def ping_db() -> str:
    eng = get_engine()
    with eng.connect() as conn:
        return conn.execute(text("SELECT DB_NAME()")).scalar_one()
