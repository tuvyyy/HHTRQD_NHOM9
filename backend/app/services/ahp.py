from __future__ import annotations
from typing import List, Dict
import math

# Random Index (RI) theo Saaty cho n=1..10 (đủ dùng đồ án)
RI_TABLE = {
    1: 0.00,
    2: 0.00,
    3: 0.58,
    4: 0.90,
    5: 1.12,
    6: 1.24,
    7: 1.32,
    8: 1.41,
    9: 1.45,
    10: 1.49
}

def _is_square(matrix: List[List[float]]) -> bool:
    n = len(matrix)
    return n > 0 and all(isinstance(row, list) and len(row) == n for row in matrix)

def _normalize_columns(A: List[List[float]]) -> List[List[float]]:
    n = len(A)
    col_sums = [0.0] * n
    for j in range(n):
        s = 0.0
        for i in range(n):
            s += A[i][j]
        col_sums[j] = s

    norm = [[0.0] * n for _ in range(n)]
    for i in range(n):
        for j in range(n):
            if col_sums[j] == 0:
                norm[i][j] = 0.0
            else:
                norm[i][j] = A[i][j] / col_sums[j]
    return norm

def _row_average(M: List[List[float]]) -> List[float]:
    n = len(M)
    w = [0.0] * n
    for i in range(n):
        w[i] = sum(M[i]) / n
    return w

def _mat_vec(A: List[List[float]], w: List[float]) -> List[float]:
    n = len(A)
    out = [0.0] * n
    for i in range(n):
        s = 0.0
        for j in range(n):
            s += A[i][j] * w[j]
        out[i] = s
    return out

def _safe_div(a: float, b: float) -> float:
    return a / b if b != 0 else 0.0

def compute_ahp(matrix: List[List[float]], labels: List[str] | None = None) -> Dict:
    """
    Tính trọng số AHP bằng phương pháp chuẩn hoá cột + trung bình hàng.
    Trả về weights, lambda_max, CI, CR.
    """
    if not _is_square(matrix):
        raise ValueError("matrix must be a non-empty square matrix")

    n = len(matrix)
    if n < 2:
        raise ValueError("matrix size must be >= 2")

    if labels is None:
        labels = [f"C{i+1}" for i in range(n)]
    if len(labels) != n:
        raise ValueError("labels length must match matrix size")

    # Normalize + weights
    norm = _normalize_columns(matrix)
    w = _row_average(norm)

    # lambda_max via (Aw / w) average
    Aw = _mat_vec(matrix, w)
    ratios = [_safe_div(Aw[i], w[i]) for i in range(n)]
    lambda_max = sum(ratios) / n

 # chỗ này xét theo trạm nên nếu n>10 thì tạm lấy 1.49 
    CI = (lambda_max - n) / (n - 1) if n > 1 else 0.0
    RI = RI_TABLE.get(n, 1.49) 
    CR = (CI / RI) if RI != 0 else 0.0

    weights = [{"label": labels[i], "weight": round(w[i], 6)} for i in range(n)]

    return {
        "n": n,
        "weights": weights,
        "lambda_max": round(lambda_max, 6),
        "CI": round(CI, 6),
        "RI": RI,
        "CR": round(CR, 6),
        "is_consistent": CR < 0.1
    }
