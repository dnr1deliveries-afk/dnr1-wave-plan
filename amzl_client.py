"""
amzl_client.py — Fetches live data from Dispatch Planning and Assignment Planning
Uses Midway cookie auth (same pattern as atlas-web)
"""

import requests
import re
from datetime import date
from bs4 import BeautifulSoup
from midway_auth import get_midway_session


STATION  = "DNR1"
CYCLE_ID = "761553f5-9fc1-4cef-8815-b974bc63f0a9"

DISPATCH_URL   = ("https://eu.dispatch.planning.last-mile.a2z.com"
                  "/dispatch-planning/{station}/{cycle}/{date}?laborType=DSP")
ASSIGNMENT_URL = ("https://eu.assignment.planning.last-mile.a2z.com"
                  "/assignment-planning/{station}/{cycle}/{date}")


def _get_session():
    return get_midway_session()


def get_plan_date():
    return date.today().strftime("%Y-%m-%d")


# ─────────────────────────────────────────────────────────────────────────────
#  COMPLETION CHECKS
# ─────────────────────────────────────────────────────────────────────────────

def check_readiness(plan_date=None):
    plan_date = plan_date or get_plan_date()
    session   = _get_session()
    dispatch_status   = _check_dispatch_planning(session, plan_date)
    assignment_status = _check_assignment_planning(session, plan_date)
    return {
        "plan_date":            plan_date,
        "sequencing_complete":  dispatch_status["finalized"],
        "unplanned_count":      dispatch_status["unplanned_count"],
        "unplanned_routes":     dispatch_status["unplanned_routes"],
        "auto_assign_complete": assignment_status["auto_assign_complete"],
        "dsp_unassigned_count": assignment_status["dsp_unassigned_count"],
        "flex_unassigned_count":assignment_status["flex_unassigned_count"],
        "total_routes":         assignment_status["total_routes"],
        "ready": (
            dispatch_status["finalized"]
            and assignment_status["auto_assign_complete"]
            and assignment_status["dsp_unassigned_count"] == 0
        ),
        "ready_message": _build_ready_message(dispatch_status, assignment_status),
    }


def _check_dispatch_planning(session, plan_date):
    url = DISPATCH_URL.format(station=STATION, cycle=CYCLE_ID, date=plan_date)
    try:
        r    = session.get(url, timeout=15)
        text = r.text
        finalized      = "Plan has been finalized" in text or "plan has been finalized" in text.lower()
        unplanned_m    = re.search(r"Unplanned Routes\s*\n\s*(\d+)", text)
        unplanned_count= int(unplanned_m.group(1)) if unplanned_m else 0
        unplanned_routes = re.findall(r"(CA_A\d+|BK_A\d+)\s*:\s*MATCHING_WAVE_NOT_FOUND", text)
        return {"finalized": finalized, "unplanned_count": unplanned_count,
                "unplanned_routes": unplanned_routes}
    except Exception as e:
        return {"finalized": False, "unplanned_count": -1, "unplanned_routes": [], "error": str(e)}


def _check_assignment_planning(session, plan_date):
    url = ASSIGNMENT_URL.format(station=STATION, cycle=CYCLE_ID, date=plan_date)
    try:
        r    = session.get(url, timeout=15)
        text = r.text
        auto_assign_complete = "Auto Assign completed" in text
        total_m    = re.search(r"Total Routes\s*\n\s*(\d+)", text)
        dsp_una_m  = re.search(r"DSP Routes Not Assigned\s*\n\s*(\d+)", text)
        flex_una_m = re.search(r"Flex Routes Not Assigned\s*\n\s*(\d+)", text)
        return {
            "auto_assign_complete": auto_assign_complete,
            "total_routes":         int(total_m.group(1))    if total_m    else 0,
            "dsp_unassigned_count": int(dsp_una_m.group(1))  if dsp_una_m  else -1,
            "flex_unassigned_count":int(flex_una_m.group(1)) if flex_una_m else -1,
        }
    except Exception as e:
        return {"auto_assign_complete": False, "total_routes": 0,
                "dsp_unassigned_count": -1, "flex_unassigned_count": -1, "error": str(e)}


def _build_ready_message(dispatch_status, assignment_status):
    msgs = []
    if not dispatch_status["finalized"]:
        msgs.append("⏳ Sequencing not yet finalized")
    elif dispatch_status["unplanned_count"] > 0:
        msgs.append(f"⚠️ {dispatch_status['unplanned_count']} unplanned: "
                    f"{', '.join(dispatch_status['unplanned_routes'])}")
    else:
        msgs.append("✅ Sequencing complete")
    if not assignment_status["auto_assign_complete"]:
        msgs.append("⏳ Auto-assign not yet complete")
    elif assignment_status["dsp_unassigned_count"] > 0:
        msgs.append(f"⚠️ {assignment_status['dsp_unassigned_count']} DSP routes unassigned")
    else:
        msgs.append("✅ Auto-assign complete")
    return " | ".join(msgs)


# ─────────────────────────────────────────────────────────────────────────────
#  DISPATCH PLAN PARSER
# ─────────────────────────────────────────────────────────────────────────────

def fetch_dispatch_plan(plan_date=None):
    plan_date = plan_date or get_plan_date()
    session   = _get_session()
    url = DISPATCH_URL.format(station=STATION, cycle=CYCLE_ID, date=plan_date)
    r   = session.get(url, timeout=15)
    return _parse_dispatch_plan(r.text)


def _parse_dispatch_plan(html):
    """
    Parse dispatch planning page (HTML or plain text from Playwright inner_text).
    Returns: {wave_time_str: {"A": [...], "B": [...], "C": [...], "C2": [...]}}
    Each entry: {lane, dsp, route}
    """
    # Try BeautifulSoup first (HTML), fall back to plain text
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n")
    except Exception:
        text = html

    lines = [l.strip() for l in text.split("\n") if l.strip()]

    waves          = {}
    current_time   = None
    time_pattern   = re.compile(r"^(\d{1,2}:\d{2}\s*[AP]M)$")
    lane_pattern   = re.compile(r"^STG-([ABC]+\.?\.?\d+)$")
    assign_pattern = re.compile(r"([A-Z]{4})\s+\(((?:CA|BK)_A\d+)\)")

    i = 0
    while i < len(lines):
        line = lines[i]

        # Wave time header
        tm = time_pattern.match(line)
        if tm:
            current_time = tm.group(1).replace(" ", "")
            if current_time not in waves:
                waves[current_time] = {"A": [], "B": [], "C": [], "C2": []}
            i += 1
            continue

        # Lane + route assignment
        lm = lane_pattern.match(line)
        if lm and current_time:
            pad = _lane_to_pad(line)
            assignment = None
            if i + 1 < len(lines):
                am = assign_pattern.search(lines[i + 1])
                if am:
                    assignment = {"dsp": am.group(1), "route": am.group(2)}
                    i += 1
            if pad:
                waves.setdefault(current_time, {"A": [], "B": [], "C": [], "C2": []})
                waves[current_time][pad].append({
                    "lane":  line,
                    "dsp":   assignment["dsp"]   if assignment else None,
                    "route": assignment["route"] if assignment else None,
                })
            i += 1
            continue

        # Inline format: "DSP (ROUTE)" — no explicit lane line
        am = assign_pattern.search(line)
        if am and current_time:
            pad = "A"   # default — will be corrected by pickorder
            waves.setdefault(current_time, {"A": [], "B": [], "C": [], "C2": []})
            waves[current_time][pad].append({
                "lane":  "",
                "dsp":   am.group(1),
                "route": am.group(2),
            })

        i += 1

    return waves


def _lane_to_pad(lane_str):
    if "STG-C.." in lane_str: return "C2"
    if "STG-C"   in lane_str: return "C"
    if "STG-B"   in lane_str: return "B"
    if "STG-A"   in lane_str: return "A"
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  ASSIGNMENT PLAN PARSER
# ─────────────────────────────────────────────────────────────────────────────

# Known non-DSP tokens that appear in the DSP column
_NON_DSP = {"More Info", "Assigned", "Assignable", "Unassigned", "UNASSIGNED", ""}

# Valid Amazon DA ID pattern
_DA_PATTERN = re.compile(r"^A[A-Z0-9]{13,}$")

# 4-letter uppercase DSP codes
_DSP_PATTERN = re.compile(r"^[A-Z]{4}$")

# Valid statuses
_STATUS_VALUES = {"Assigned", "Assignable", "Unassigned"}


def fetch_assignment_data(plan_date=None):
    plan_date = plan_date or get_plan_date()
    session   = _get_session()
    url = ASSIGNMENT_URL.format(station=STATION, cycle=CYCLE_ID, date=plan_date)
    r   = session.get(url, timeout=15)
    return _parse_assignment_data(r.text)


def _parse_assignment_data(html):
    """
    Robust assignment data parser.

    Handles two formats produced by Playwright inner_text():

    FORMAT A — one field per line (ideal):
        CA_A151
        Assigned
        Standard Parcel Medium Van
        240
        MOLI
        A1234567890123

    FORMAT B — tab-separated on one line (React table collapse):
        CA_A151
        Assigned
        Standard Parcel Medium Van\t240\tMOLI\tA1234567890123

    Also handles:
        - dsp = "More Info" (UI button, not real DSP) → scan forward for 4-letter code
        - da_id = Amazon ID or "UNASSIGNED"
        - Cargo bike routes: BK_A*
        - Tab-embedded service_type fields
    """
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text("\n")
    except Exception:
        text = html

    # Normalise: expand tabs to newlines for uniform processing
    text  = text.replace("\t", "\n")
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    routes        = {}
    route_pattern = re.compile(r"^((?:CA|BK)_A\d+)$")

    i = 0
    while i < len(lines):
        rm = route_pattern.match(lines[i])
        if not rm:
            i += 1
            continue

        route_code = rm.group(1)
        i += 1

        # Gather the next up-to-10 non-route lines as a candidate window
        window = []
        j = i
        while j < len(lines) and len(window) < 10:
            if route_pattern.match(lines[j]):
                break   # hit next route
            window.append(lines[j])
            j += 1

        # Extract fields from window
        status       = _extract_status(window)
        service_type = _extract_service_type(window)
        duration     = _extract_duration(window)
        dsp          = _extract_dsp(window)
        da_id        = _extract_da_id(window)

        if not status:
            # Could not find a valid status — skip
            continue

        routes[route_code] = {
            "status":           status,
            "service_type":     service_type,
            "duration":         duration,
            "dsp":              dsp,
            "da_id":            da_id,
            "is_cargo_bike":    route_code.startswith("BK_"),
            "is_flex":          dsp == "FLEX",
            "is_low_emission":  "Low Emission" in service_type or "Ironhide" in service_type,
            "is_nursery":       "Nursery" in service_type,
            "is_remote_debrief": "Remote Debrief" in service_type,
        }
        i = j   # advance past consumed window

    return routes


# ── Field extractors ──────────────────────────────────────────────────────────

def _extract_status(window: list) -> str:
    for token in window:
        if token in _STATUS_VALUES:
            return token
    return ""


# Service type keywords — used to identify the service type token
_SVC_KEYWORDS = (
    "Standard Parcel", "Nursery Route", "Remote Debrief",
    "Low Emission", "Large Van", "Medium Van", "AmFlex",
    "Cargo Electric", "Ironhide", "Ride Along",
)


def _extract_service_type(window: list) -> str:
    for token in window:
        for kw in _SVC_KEYWORDS:
            if kw in token:
                return token.strip()
    return ""


def _extract_duration(window: list) -> int:
    for token in window:
        token = token.strip()
        if token.isdigit() and 30 <= int(token) <= 720:
            return int(token)
    return 0


def _extract_dsp(window: list) -> str:
    for token in window:
        token = token.strip()
        if _DSP_PATTERN.match(token) and token not in _NON_DSP:
            return token
    return "UNASSIGNED"


def _extract_da_id(window: list) -> str:
    for token in window:
        token = token.strip()
        if _DA_PATTERN.match(token):
            return token
    return "UNASSIGNED"
