# app/services/grid.py
from __future__ import annotations
import math
from typing import List, Tuple, Dict

def km_to_deg_lat(km: float) -> float:
    return km / 110.574  # xấp xỉ

def km_to_deg_lon(km: float, lat: float) -> float:
    return km / (111.320 * math.cos(math.radians(lat)) + 1e-9)

def generate_grid_points(
    bbox: Dict[str, float],
    step_km: float,
    max_points: int
) -> List[Tuple[float, float]]:
    """
    bbox: {"minLat":..,"minLon":..,"maxLat":..,"maxLon":..}
    return list[(lat, lon)]
    """
    min_lat = min(bbox["minLat"], bbox["maxLat"])
    max_lat = max(bbox["minLat"], bbox["maxLat"])
    min_lon = min(bbox["minLon"], bbox["maxLon"])
    max_lon = max(bbox["minLon"], bbox["maxLon"])

    points: List[Tuple[float, float]] = []
    step_lat = km_to_deg_lat(step_km)

    lat = min_lat
    while lat <= max_lat + 1e-9:
        step_lon = km_to_deg_lon(step_km, lat)
        lon = min_lon
        while lon <= max_lon + 1e-9:
            points.append((round(lat, 6), round(lon, 6)))
            if len(points) >= max_points:
                return points
            lon += step_lon
        lat += step_lat

    return points
