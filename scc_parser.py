"""
scc_parser.py — Parses SCC Pick Export CSV for cart counts.

CSV Structure:
  - Overview row: Route Code present, Picklist Code empty — summary row for the route
  - Picklist rows: Route Code present, Picklist Code present — each row = 1 cart

Cart counting logic:
  - Each picklist row = 1 cart
  - Count the number of picklist rows per route to get total carts
  - Overview row contains Bags/OVs/SPR totals but we use picklist count for carts
"""

import csv
import io
import re
from datetime import datetime
from collections import defaultdict


def parse_scc_csv(file_path_or_content):
    """
    Parse SCC Pick Export CSV.
    
    Each picklist row = 1 cart. We count picklist rows per route.
    
    Returns: {route_code: {total_carts, bags, ovs, spr, status, ...}}
    """
    if isinstance(file_path_or_content, bytes) or (
        isinstance(file_path_or_content, str) and "\n" in file_path_or_content
    ):
        content = file_path_or_content
        if isinstance(content, bytes):
            content = content.decode("utf-8-sig")
        rows = list(csv.DictReader(io.StringIO(content)))
    else:
        with open(file_path_or_content, "r", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))

    # First pass: collect overview data and count picklists per route
    routes = {}
    picklist_counts = defaultdict(int)
    
    for row in rows:
        route_code = (row.get("Route Code") or "").strip()
        picklist_code = (row.get("Picklist Code") or "").strip()
        
        if not route_code:
            continue
        
        if picklist_code:
            # This is a picklist row — each picklist = 1 cart
            picklist_counts[route_code] += 1
        else:
            # This is an overview row — capture summary data
            bags = _safe_int(row.get("Bags"))
            ovs = _safe_int(row.get("OVs"))
            spr = _safe_int(row.get("SPR"))
            stage_by = _parse_excel_time(row.get("Stage by time", ""))
            
            routes[route_code] = {
                "bags": bags,
                "ovs": ovs,
                "spr": spr,
                "status": (row.get("Status") or "").strip(),
                "stage_by": stage_by,
                "associate": (row.get("Associate") or "").strip(),
                "route_type": _classify_route(route_code, row.get("Type", "")),
                "total_carts": 0,  # Will be updated from picklist count
            }
    
    # Second pass: set total_carts from picklist count
    for route_code, count in picklist_counts.items():
        if route_code in routes:
            routes[route_code]["total_carts"] = count
        else:
            # Route has picklists but no overview row — create entry
            routes[route_code] = {
                "bags": 0,
                "ovs": 0,
                "spr": 0,
                "status": "Unknown",
                "stage_by": "",
                "associate": "",
                "route_type": _classify_route(route_code, ""),
                "total_carts": count,
            }
    
    # Handle routes with overview but no picklists (shouldn't happen, but fallback)
    for route_code, data in routes.items():
        if data["total_carts"] == 0 and (data["bags"] > 0 or data["ovs"] > 0):
            # Fallback: use bags + ovs if no picklist rows found
            data["total_carts"] = data["bags"] + data["ovs"]
    
    return routes


def _safe_int(val):
    """Safely convert value to int."""
    try:
        return int(float(str(val).strip()))
    except (ValueError, TypeError):
        return 0


def _parse_excel_time(val):
    """Parse Excel time fraction to HH:MM string."""
    try:
        f = float(val)
        total_minutes = round(f * 24 * 60)
        hours = total_minutes // 60
        minutes = total_minutes % 60
        return f"{hours:02d}:{minutes:02d}"
    except (ValueError, TypeError):
        return str(val).strip()


def _classify_route(route_code, type_str):
    """Classify route type based on code and type string."""
    if route_code.startswith("BK_"):
        return "cargo_bike"
    type_lower = (type_str or "").lower()
    if "flex" in type_lower or "amflex" in type_lower:
        return "flex"
    return "dsp"


def enrich_routes_with_carts(wave_routes, scc_data):
    """
    Enrich wave route data with cart counts from SCC data.
    
    Args:
        wave_routes: List of route dicts from wave plan
        scc_data: Dict from parse_scc_csv()
    
    Returns:
        wave_routes with cart data added
    """
    for route in wave_routes:
        code = route.get("route", "")
        if code in scc_data:
            route["bags"] = scc_data[code]["bags"]
            route["ovs"] = scc_data[code]["ovs"]
            route["total_carts"] = scc_data[code]["total_carts"]
            route["spr"] = scc_data[code]["spr"]
            route["pick_status"] = scc_data[code]["status"]
            route["stage_by"] = scc_data[code]["stage_by"]
        else:
            route.setdefault("bags", 0)
            route.setdefault("ovs", 0)
            route.setdefault("total_carts", 0)
            route.setdefault("spr", 0)
            route.setdefault("pick_status", "Unknown")
            route.setdefault("stage_by", "")
    return wave_routes


def get_scc_summary(scc_data):
    """Get summary statistics from parsed SCC data."""
    total_routes = len(scc_data)
    picked = sum(1 for r in scc_data.values() if r["status"] == "Picked")
    total_carts = sum(r["total_carts"] for r in scc_data.values())
    total_bags = sum(r["bags"] for r in scc_data.values())
    total_ovs = sum(r["ovs"] for r in scc_data.values())
    cargo_bikes = sum(1 for k in scc_data if k.startswith("BK_"))
    return {
        "total_routes": total_routes,
        "picked": picked,
        "pending": total_routes - picked,
        "total_carts": total_carts,
        "total_bags": total_bags,
        "total_ovs": total_ovs,
        "cargo_bike_routes": cargo_bikes,
        "van_routes": total_routes - cargo_bikes,
    }
