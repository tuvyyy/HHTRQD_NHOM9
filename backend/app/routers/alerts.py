from fastapi import APIRouter, HTTPException
from sqlalchemy import text
from app.core.db import ping_db, engine

router = APIRouter(tags=["Alerts"])

@router.get("/db/ping")
def db_ping():
    try:
        return {"ok": True, "db": ping_db()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/alerts")
def get_alerts(limit: int = 20):
    limit = max(1, min(limit, 200))
    sql = text(f"""
        SELECT TOP ({limit})
            Id, CreatedAt, Lat, Lon, Score, Level, PM25, PM10, NO2, O3, CO
        FROM AlertHistory
        ORDER BY Id DESC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
        return {"count": len(rows), "items": [dict(r) for r in rows]}
