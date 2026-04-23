"""
pickorder_parser.py — Parses the AMZL PickOrder CSV and applies lane spreading.

The AMZL PickOrder CSV (downloaded from Dispatch Planning portal) assigns routes
to consecutive lanes starting from 1:
  e.g. 19 routes → STG-A.1, STG-A.2, ..., STG-A.19

The spreading algorithm redistributes those N routes evenly across all 30 lanes
so there are visible gaps between DSP groups on the pad floor:
  e.g. 19 routes → STG-A1, STG-A3, STG-A4, ..., STG-A28, STG-A30

Rules:
  - Pad A: spread across lanes 1-30
  - Pad B: spread across lanes 1-30
  - Pad C (cargo bikes): NO spreading — keep consecutive as-is
  - Wave order preserved (dispatch time → pad assignment → route order)
  - Lane numbering in output uses STG-A1 format (no dot) to match wave plan display
"""

import csv
import io
import math
from collections import defaultdict
from datetime import datetime


MAX_LANES = 30
CARGO_BIKE_PREFIX = "BK_"


# ─────────────────────────────────────────────
#  SPREAD ALGORITHM
# ─────────────────────────────────────────────

def spread_lanes(n: int, max_lanes: int = MAX_LANES) -> list[int]:
    """
    Spread n routes evenly across max_lanes.

    Algorithm: evenly spaced so first route = lane 1, last route = lane max_lanes.
    For n routes:
        lane[i] = round(i * (max_lanes - 1) / (n - 1)) + 1

    Edge cases:
      n == 1      → [1]
      n >= max    → [1, 2, 3, ..., n]  (no space to spread)
      n == 0      → []

    Examples:
      n=7  → [1, 6, 11, 15, 20, 25, 30]
      n=19 → [1, 3, 4, 6, 7, 9, 11, 12, 14, 15, 17, 19, 20, 22, 24, 25, 27, 28, 30]
      n=30 → [1, 2, 3, ..., 30]
    """
    if n <= 0:
        return []
    if n == 1:
        return [1]
    if n >= max_lanes:
        return list(range(1, n + 1))

    step = (max_lanes - 1) / (n - 1)
    return [round(i * step) + 1 for i in range(n)]


# ─────────────────────────────────────────────
#  PARSER
# ─────────────────────────────────────────────

def parse_pickorder_csv(file_path_or_content) -> dict:
    """
    Parse AMZL PickOrder CSV.

    CSV format:
      dispatchTime, routeCode, routeID, dispatchArea
      10:20:00, CA_A151, 7429433-151, STG-A.1
      ...

    Returns structured dict:
      {
        "waves": {
          "10:20": {
            "A": [{"route": "CA_A151", "route_id": "...", "original_lane": 1, "spread_lane": 1, "lane_label": "STG-A1"}],
            "B": [...],
            "C": [...]   # never spread
          },
          ...
        },
        "route_to_lane": {"CA_A151": {"wave_time": "10:20", "pad": "A", "lane": 1, "lane_label": "STG-A1"}},
        "total_routes": 228,
        "warnings": []
      }
    """
    rows = _load_rows(file_path_or_content)
    if not rows:
        return {"waves": {}, "route_to_lane": {}, "total_routes": 0, "warnings": ["Empty CSV"]}

    # Group by dispatch time then pad
    by_time_pad = defaultdict(lambda: defaultdict(list))
    warnings = []

    for row in rows:
        time_str = _normalise_time(row.get("dispatchTime", ""))
        route = (row.get("routeCode") or "").strip()
        route_id = (row.get("routeID") or "").strip()
        area = (row.get("dispatchArea") or "").strip()

        if not route or not time_str or not area:
            continue

        pad, lane_num = _parse_area(area)
        if pad is None:
            warnings.append(f"Unrecognised area: {area} for route {route}")
            continue

        by_time_pad[time_str][pad].append({
            "route": route,
            "route_id": route_id,
            "original_lane": lane_num,
            "is_cargo_bike": route.startswith(CARGO_BIKE_PREFIX),
        })

    # Apply spreading and build output
    waves = {}
    route_to_lane = {}

    for time_str in sorted(by_time_pad.keys()):
        pads = by_time_pad[time_str]
        waves[time_str] = {}

        for pad in ["A", "B", "C"]:
            routes = pads.get(pad, [])
            if not routes:
                continue

            # Sort by original lane number to preserve AMZL order
            routes.sort(key=lambda r: r["original_lane"])

            # Apply spread — Pad C (cargo bikes) never spread
            if pad in ("A", "B"):
                spread = spread_lanes(len(routes), MAX_LANES)
            else:
                # Pad C: keep consecutive 1..N
                spread = list(range(1, len(routes) + 1))

            enriched = []
            for i, route_data in enumerate(routes):
                lane = spread[i]
                label = f"STG-{pad}{lane}"
                entry = {
                    **route_data,
                    "spread_lane": lane,
                    "lane_label": label,
                    "pad": pad,
                    "wave_time": time_str,
                }
                enriched.append(entry)
                route_to_lane[route_data["route"]] = {
                    "wave_time": time_str,
                    "pad": pad,
                    "lane": lane,
                    "lane_label": label,
                    "original_lane": route_data["original_lane"],
                }

            waves[time_str][pad] = enriched

    return {
        "waves": waves,
        "route_to_lane": route_to_lane,
        "total_routes": len(route_to_lane),
        "warnings": warnings,
        "max_lanes": MAX_LANES,
    }


# ─────────────────────────────────────────────
#  APPLY TO WAVE PLAN
# ─────────────────────────────────────────────

def apply_pickorder_to_plan(wave_plan: dict, pickorder_data: dict) -> dict:
    """
    Override lane assignments in the wave plan with spread pickorder lanes.
    
    For each route in each wave/pad, look up its spread lane from pickorder_data
    and update the lane_label. Routes not found in pickorder keep their existing lane.
    
    Pad C (cargo bikes) routes are never modified.
    
    Returns the modified wave_plan (in-place update + return).
    """
    route_to_lane = pickorder_data.get("route_to_lane", {})
    applied = 0
    not_found = []

    for wave in wave_plan.get("waves", []):
        for pad_key in ("pad_a", "pad_b"):
            for route in wave.get(pad_key, {}).get("routes", []):
                route_code = route.get("route", "")
                if route_code in route_to_lane:
                    entry = route_to_lane[route_code]
                    route["lane"] = entry["lane_label"]
                    route["spread_lane"] = entry["lane"]
                    route["original_lane"] = entry["original_lane"]
                    applied += 1
                else:
                    not_found.append(route_code)

    # Sort routes within each pad by spread_lane number
    for wave in wave_plan.get("waves", []):
        for pad_key in ("pad_a", "pad_b"):
            routes = wave.get(pad_key, {}).get("routes", [])
            routes.sort(key=lambda r: r.get("spread_lane", r.get("original_lane", 999)))

    wave_plan["pickorder_applied"] = True
    wave_plan["pickorder_stats"] = {
        "routes_mapped": applied,
        "routes_not_found": len(not_found),
        "not_found_list": not_found[:10],  # first 10 only
    }

    return wave_plan


def get_pickorder_summary(pickorder_data: dict) -> dict:
    """Summary stats for dashboard display."""
    waves = pickorder_data.get("waves", {})
    total = pickorder_data.get("total_routes", 0)
    wave_count = sum(1 for t, pads in waves.items() if any(p in pads for p in ("A", "B")))

    return {
        "total_routes": total,
        "wave_slots": wave_count,
        "max_lanes": pickorder_data.get("max_lanes", MAX_LANES),
        "warnings": pickorder_data.get("warnings", []),
        "has_warnings": len(pickorder_data.get("warnings", [])) > 0,
        "message": f"✅ PickOrder loaded — {total} routes across {wave_count} wave slots, spread across {MAX_LANES} lanes",
    }


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _load_rows(file_path_or_content) -> list:
    if isinstance(file_path_or_content, bytes) or (
        isinstance(file_path_or_content, str) and "\n" in file_path_or_content
    ):
        content = file_path_or_content if isinstance(file_path_or_content, str) else file_path_or_content.decode("utf-8-sig")
        return list(csv.DictReader(io.StringIO(content)))
    else:
        with open(file_path_or_content, "r", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))


def _normalise_time(raw: str) -> str:
    """Normalise '10:20:00' or '10:20' to 'HH:MM'."""
    raw = raw.strip()
    parts = raw.split(":")
    if len(parts) >= 2:
        try:
            return f"{int(parts[0]):02d}:{int(parts[1]):02d}"
        except ValueError:
            pass
    return raw


def _parse_area(area: str) -> tuple:
    """
    Parse 'STG-A.1' or 'STG-B.12' or 'STG-C..1' into (pad, lane_number).
    Returns (None, None) if unrecognised.
    """
    area = area.strip()
    # Handle STG-C.. (double dot) for cargo bike second pad
    area = area.replace("STG-C..", "STG-C.")

    if not area.startswith("STG-"):
        return None, None

    try:
        # STG-A.1 → pad=A, lane=1
        parts = area[4:]  # "A.1"
        dot_pos = parts.find(".")
        if dot_pos == -1:
            return None, None
        pad = parts[:dot_pos].upper()
        lane = int(parts[dot_pos + 1:])
        return pad, lane
    except (ValueError, IndexError):
        return None, None
