from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from app.services.ahp import compute_ahp

router = APIRouter(tags=["AHP"])

class AHPRequest(BaseModel):
    matrix: List[List[float]]
    labels: Optional[List[str]] = None

@router.post("/ahp/weights")
def ahp_weights(req: AHPRequest):
    try:
        return compute_ahp(req.matrix, req.labels)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
