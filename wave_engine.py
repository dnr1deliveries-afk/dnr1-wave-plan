"""
wave_engine.py — Core wave plan builder for DNR1.

v1.5 UPDATE: PickOrder CSV is now the SOURCE OF TRUTH for wave structure.
  - Wave times come from PickOrder dispatchTime
  - Route-to-pad assignments come from PickOrder dispatchArea (STG-A vs STG-B)
  - Route counts per wave match PickOrder exactly
  - Dispatch/Assignment data used only for enrichment (DSP, DA, service type)
  - SCC data used for cart counts

Wave structure:
  - Waves 1-N, each with Pad A and Pad B (staggered by pad_b_offset_min)
  - Wave C: cargo bikes (BK_ routes) — separate, independent timing, no lane spread
  - Up to 30 staging lanes per pad
  - Dynamic: built from live data, not fixed templates
  - Lane spread: evenly across all 30 lanes via pickorder CSV (Pad C excluded)
"""

from datetime import datetime, timedelta
import re
from lane_optimizer import (
    optimize_wave_pair,
    optimize_single_pad,
    get_capacity_warnings,
    get_optimization_summary,
)

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_CONFIG = {
    "first_wave_time":          "10:20",
    "dispatch_frequency_min":   25,
    "arrival_offset_min":       15,
    "on_pad_offset_min":        5,
    "pad_b_offset_min":         10,
    "last_exit_min":            25,
    "cargo_bike_wave_time":     "10:00",
    "cargo_bike_frequency_min": 25,
    "lanes_per_pad":            30,
    "lane_capacity":            6,
}

SERVICE_TYPE_PRIORITY = {
    "Standard Parcel - Low Emission Vehicle": 1,
    "Nursery Route":                          2,
    "Standard Parcel Medium Van":             3,
    "Standard Parcel - Large Van":            4,
    "Standard Parcel":                        5,
    "Remote Debrief":                         6,
    "AmFlex":                                 99,
}


# ─────────────────────────────────────────────────────────────────────────────
#  CLASS WRAPPER
# ─────────────────────────────────────────────────────────────────────────────

class WaveEngine:
    """Thin wrapper so legacy code can call we.build_plan(routes, **kwargs)."""

    def build_plan(self, routes, first_wave_time="10:20", wave_frequency=25,
                   pad_a_offset=15, pad_b_offset=10, pickorder_data=None):
        dispatch_data   = {}
        van_routes  = [r for r in routes if not r.get("is_cargo_bike")]
        bike_routes = [r for r in routes if r.get("is_cargo_bike")]

        cfg = {**DEFAULT_CONFIG,
               "first_wave_time":        first_wave_time,
               "dispatch_frequency_min": wave_frequency,
               "arrival_offset_min":     pad_a_offset,
               "pad_b_offset_min":       pad_b_offset}

        wave_size = cfg["lanes_per_pad"]
        first     = _parse_time_str(first_wave_time)
        wave_num  = 0
        for i in range(0, len(van_routes), wave_size):
            batch = van_routes[i:i + wave_size]
            mid   = len(batch) // 2
            tk    = _fmt_time(first + timedelta(minutes=wave_num * wave_frequency))
            dispatch_data[tk] = {
                "A": [{"route": r["route"], "dsp": r["dsp"], "lane": ""} for r in batch[:mid]],
                "B": [{"route": r["route"], "dsp": r["dsp"], "lane": ""} for r in batch[mid:]],
            }
            wave_num += 1

        if bike_routes:
            tk = cfg["cargo_bike_wave_time"]
            dispatch_data.setdefault(tk, {})
            dispatch_data[tk]["C"] = [
                {"route": r["route"], "dsp": r["dsp"], "lane": ""} for r in bike_routes
            ]

        assignment_data = {
            r["route"]: {k: r.get(k, v) for k, v in {
                "service_type": "", "dsp": "", "da_id": "UNASSIGNED",
                "is_low_emission": False, "is_nursery": False,
                "is_remote_debrief": False,
            }.items()}
            for r in routes
        }
        scc_data = {
            r["route"]: {"total_carts": r.get("total_carts", 0),
                         "bags": r.get("bags", 0), "ovs": r.get("ovs", 0)}
            for r in routes
        }
        return build_wave_plan(dispatch_data, assignment_data, scc_data,
                               config=cfg, pickorder_data=pickorder_data)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def build_wave_plan(dispatch_data, assignment_data, scc_data,
                    config=None, pickorder_data=None):
    """
    Build the complete wave plan.

    PRIORITY (v1.5): If pickorder_data has 'waves', use it as SOURCE OF TRUTH
    for wave structure (times, pads, routes per wave). Dispatch data is fallback.

    dispatch_data  : {wave_time_str: {"A": [...], "B": [...], "C": [...]}}
    assignment_data: {route_code: {service_type, dsp, da_id, ...}}
    scc_data       : {route_code: {bags, ovs, total_carts, ...}}
    pickorder_data : parsed pickorder with 'waves' dict (from pickorder_parser.py)
    config         : optional overrides
    """
    cfg = {**DEFAULT_CONFIG, **(config or {})}
    
    # v1.5: Use PickOrder as source of truth if it has waves structure
    if pickorder_data and pickorder_data.get("waves"):
        waves = _build_waves_from_pickorder(pickorder_data, assignment_data, scc_data, cfg)
    else:
        waves = _build_main_waves(dispatch_data, assignment_data, scc_data, cfg, pickorder_data)
    
    wave_c = _build_cargo_bike_waves(dispatch_data, assignment_data, scc_data, cfg)

    # Annotate wave pair labels  (Wave 1 ↔ Wave 2, Wave 3 ↔ Wave 4 …)
    for i in range(0, len(waves), 2):
        w1 = waves[i]
        w2 = waves[i + 1] if i + 1 < len(waves) else None
        if w2:
            w1["pair_label"] = w2["wave_label"]
            w2["pair_label"] = w1["wave_label"]
        else:
            w1["pair_label"] = None

    all_warnings = []
    for wave in waves:
        all_warnings.extend(
            get_capacity_warnings(wave["pad_a"]["routes"], wave["pad_b"]["routes"])
        )

    return {
        "waves":             waves,
        "wave_c":            wave_c,
        "summary":           _build_summary(waves, wave_c, scc_data, assignment_data),
        "optimisation":      _build_optimisation_summary(waves),
        "warnings":          all_warnings,
        "pickorder_applied": bool(pickorder_data and pickorder_data.get("waves")),
        "config":            cfg,
        "plan_date":         _today_str(),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  PICKORDER-BASED WAVE BUILDER (v1.5 - Source of Truth)
# ─────────────────────────────────────────────────────────────────────────────

def _build_waves_from_pickorder(pickorder_data, assignment_data, scc_data, cfg):
    """
    Build waves directly from PickOrder data (source of truth for wave structure).
    
    PickOrder structure from pickorder_parser.py:
      pickorder_data["waves"] = {
        "10:20": {
          "A": [{"route": "CA_A151", "spread_lane": 1, "lane_label": "STG-A1", ...}, ...],
          "B": [{"route": "CA_A200", "spread_lane": 1, "lane_label": "STG-B1", ...}, ...]
        },
        "10:30": {...},
        ...
      }
    
    Each unique time slot with Pad A routes = one wave.
    Pad B routes at +10 minutes are paired with the previous A wave.
    """
    waves = []
    wave_num = 1
    
    po_waves = pickorder_data.get("waves", {})
    
    # Get all unique times that have Pad A routes (these define our waves)
    a_times = sorted([t for t, pads in po_waves.items() if pads.get("A")], key=_parse_time_str)
    
    # Track which B times we've used
    used_b_times = set()
    
    for a_time in a_times:
        # Get Pad A routes
        pad_a_raw = po_waves.get(a_time, {}).get("A", [])
        
        # Find corresponding Pad B time (typically 10 min after A)
        a_dt = _parse_time_str(a_time)
        expected_b_time = _fmt_time(a_dt + timedelta(minutes=cfg["pad_b_offset_min"]))
        
        # Look for B routes at expected time
        pad_b_raw = []
        b_time_used = expected_b_time
        
        if expected_b_time in po_waves and po_waves[expected_b_time].get("B"):
            pad_b_raw = po_waves[expected_b_time].get("B", [])
            used_b_times.add(expected_b_time)
        
        # Enrich routes with assignment and SCC data
        pad_a = _enrich_pickorder_routes(pad_a_raw, assignment_data, scc_data)
        pad_b = _enrich_pickorder_routes(pad_b_raw, assignment_data, scc_data)
        
        # Sort by spread_lane
        pad_a.sort(key=lambda r: r.get("lane_num") or r.get("spread_lane") or 99)
        pad_b.sort(key=lambda r: r.get("lane_num") or r.get("spread_lane") or 99)
        
        pad_a_time = _parse_time_str(a_time)
        pad_b_time = _parse_time_str(b_time_used)
        
        waves.append({
            "wave_number": wave_num,
            "wave_label":  f"Wave {wave_num}",
            "pair_label":  None,
            "pad_a":       _build_pad_block("A", pad_a, pad_a_time, cfg),
            "pad_b":       _build_pad_block("B", pad_b, pad_b_time, cfg),
            "status":      "not_started",
            "cleared_at":  None,
            "swiped_at":   None,
        })
        wave_num += 1
    
    # Handle any remaining B-only times (shouldn't normally happen)
    for time_str, pads in sorted(po_waves.items(), key=lambda x: _parse_time_str(x[0])):
        if time_str in used_b_times:
            continue
        if not pads.get("B"):
            continue
        # This is an orphan B time - create a wave with empty A
        pad_b_raw = pads.get("B", [])
        pad_b = _enrich_pickorder_routes(pad_b_raw, assignment_data, scc_data)
        pad_b.sort(key=lambda r: r.get("lane_num") or r.get("spread_lane") or 99)
        
        b_dt = _parse_time_str(time_str)
        a_dt = b_dt - timedelta(minutes=cfg["pad_b_offset_min"])
        
        waves.append({
            "wave_number": wave_num,
            "wave_label":  f"Wave {wave_num}",
            "pair_label":  None,
            "pad_a":       _build_pad_block("A", [], a_dt, cfg),
            "pad_b":       _build_pad_block("B", pad_b, b_dt, cfg),
            "status":      "not_started",
            "cleared_at":  None,
            "swiped_at":   None,
        })
        wave_num += 1
    
    return waves


def _enrich_pickorder_routes(po_routes, assignment_data, scc_data):
    """Enrich pickorder routes with assignment and SCC data."""
    enriched = []
    
    for po in po_routes:
        route_code = po.get("route", "")
        if not route_code or route_code.startswith("BK_"):
            continue
        
        assign = assignment_data.get(route_code, {})
        scc = scc_data.get(route_code, {})
        svc = assign.get("service_type", "")
        
        enriched.append({
            "lane":             po.get("lane_label", ""),
            "lane_num":         po.get("spread_lane", po.get("original_lane", 0)),
            "spread_lane":      po.get("spread_lane", 0),
            "original_lane":    po.get("original_lane", 0),
            "route":            route_code,
            "dsp":              assign.get("dsp", ""),
            "da_id":            assign.get("da_id", "UNASSIGNED"),
            "service_type":     svc,
            "service_short":    _shorten_service_type(svc),
            "duration_min":     assign.get("duration", 0),
            "total_carts":      scc.get("total_carts", 0),
            "bags":             scc.get("bags", 0),
            "ovs":              scc.get("ovs", 0),
            "pick_status":      scc.get("status", ""),
            "stage_by":         scc.get("stage_by", ""),
            "is_low_emission":  assign.get("is_low_emission", False) or "Low Emission" in svc,
            "is_nursery":       assign.get("is_nursery", False) or "Nursery" in svc,
            "is_remote_debrief": assign.get("is_remote_debrief", False) or "Remote Debrief" in svc,
            "is_cargo_bike":    route_code.startswith("BK_"),
            "notes":            _auto_notes(svc),
            "shares_with":      None,
            "partner_carts":    0,
            "combined_carts":   scc.get("total_carts", 0),
            "lane_utilization": 0,
            "dispatched":       False,
            "staged":           False,
        })
    
    return enriched


# ─────────────────────────────────────────────────────────────────────────────
#  WAVE BUILDERS (Fallback when no PickOrder)
# ─────────────────────────────────────────────────────────────────────────────

    wave_num = 1

    for time_str in sorted(dispatch_data.keys(), key=_parse_time_str):
        pads = dispatch_data[time_str]

        # Guard: skip None entries, skip BK_ routes
        pad_a_raw = [r for r in pads.get("A", [])
                     if r and r.get("route") and not r["route"].startswith("BK_")]
        pad_b_raw = [r for r in pads.get("B", [])
                     if r and r.get("route") and not r["route"].startswith("BK_")]

        if not pad_a_raw and not pad_b_raw:
            continue

        wave_time = _parse_time_str(time_str)
        pad_a     = _sort_pad(_enrich_routes(pad_a_raw, assignment_data, scc_data))
        pad_b     = _sort_pad(_enrich_routes(pad_b_raw, assignment_data, scc_data))

        if pickorder_data:
            pad_a = _apply_pickorder(pad_a, pickorder_data, "A")
            pad_b = _apply_pickorder(pad_b, pickorder_data, "B")

        if pad_a and pad_b:
            pad_a, pad_b = optimize_wave_pair(pad_a, pad_b)
        elif pad_a:
            pad_a = optimize_single_pad(pad_a, "A")
        elif pad_b:
            pad_b = optimize_single_pad(pad_b, "B")

        pad_a.sort(key=lambda r: r.get("lane_num") or 99)
        pad_b.sort(key=lambda r: r.get("lane_num") or 99)

        pad_a_time = wave_time
        pad_b_time = wave_time + timedelta(minutes=cfg["pad_b_offset_min"])

        waves.append({
            "wave_number": wave_num,
            "wave_label":  f"Wave {wave_num}",
            "pair_label":  None,
            "pad_a":       _build_pad_block("A", pad_a, pad_a_time, cfg),
            "pad_b":       _build_pad_block("B", pad_b, pad_b_time, cfg),
            "status":      "not_started",
            "cleared_at":  None,
            "swiped_at":   None,
        })
        wave_num += 1

    return waves


def _build_cargo_bike_waves(dispatch_data, assignment_data, scc_data, cfg):
    """Wave C — BK_ routes only, consecutive lanes, no optimisation."""
    wave_c_blocks = []
    wave_num      = 1

    for time_str in sorted(dispatch_data.keys(), key=_parse_time_str):
        pads = dispatch_data[time_str]
        c_routes = [
            r for pad_key in ("C", "C2")
            for r in pads.get(pad_key, [])
            if r and r.get("route") and r["route"].startswith("BK_")
        ]
        if not c_routes:
            continue

        wave_time = _parse_time_str(time_str)
        routes    = _enrich_routes(c_routes, assignment_data, scc_data)

        for i, route in enumerate(routes, 1):
            route["lane"]     = f"STG-C{i}"
            route["lane_num"] = i

        wave_c_blocks.append({
            "wave_number":    wave_num,
            "wave_label":     f"Wave C{wave_num}",
            "wave_time":      _fmt_time(wave_time),
            "first_entrance": _fmt_time(wave_time - timedelta(minutes=cfg["arrival_offset_min"])),
            "last_entrance":  _fmt_time(wave_time - timedelta(minutes=cfg["on_pad_offset_min"])),
            "last_exit":      _fmt_time(wave_time + timedelta(minutes=cfg["last_exit_min"])),
            "routes":         routes,
            "total_routes":   len(routes),
            "status":         "not_started",
            "cleared_at":     None,
        })
        wave_num += 1

    return wave_c_blocks


def _build_pad_block(pad_label, routes, wave_time, cfg):
    return {
        "pad_label":       pad_label,
        "wave_time":       _fmt_time(wave_time),
        "first_entrance":  _fmt_time(wave_time - timedelta(minutes=cfg["arrival_offset_min"])),
        "last_entrance":   _fmt_time(wave_time - timedelta(minutes=cfg["on_pad_offset_min"])),
        "last_exit":       _fmt_time(wave_time + timedelta(minutes=cfg["last_exit_min"])),
        "routes":          routes,
        "total_routes":    len(routes),
        "total_carts":     sum(r.get("total_carts", 0) for r in routes),
        "dsps":            list(dict.fromkeys(r["dsp"] for r in routes if r.get("dsp"))),
        "paired_count":    sum(1 for r in routes if r.get("shares_with")),
        "solo_count":      sum(1 for r in routes if not r.get("shares_with")),
        "avg_utilization": (
            round(sum(r.get("lane_utilization", 0) for r in routes) / len(routes))
            if routes else 0
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  PICKORDER LANE APPLICATION
# ─────────────────────────────────────────────────────────────────────────────

def _apply_pickorder(routes, pickorder_data, pad_letter):
    field = f"lane_num_{pad_letter.lower()}"
    for route in routes:
        code = route.get("route", "")
        if code in pickorder_data:
            ln = pickorder_data[code].get(field) or pickorder_data[code].get("lane_num")
            if ln is not None:
                route["lane_num"] = int(ln)
                route["lane"]     = f"STG-{pad_letter}{int(ln)}"
    return routes


# ─────────────────────────────────────────────────────────────────────────────
#  ENRICHMENT + SORTING
# ─────────────────────────────────────────────────────────────────────────────

def _enrich_routes(pad_routes, assignment_data, scc_data):
    enriched = []
    for r in pad_routes:
        code       = r.get("route", "")
        assignment = assignment_data.get(code, {})
        scc        = scc_data.get(code, {})
        svc        = assignment.get("service_type", r.get("service_type", ""))

        enriched.append({
            "lane":             r.get("lane", ""),
            "lane_num":         None,
            "route":            code,
            "dsp":              r.get("dsp") or assignment.get("dsp", ""),
            "da_id":            assignment.get("da_id", "UNASSIGNED"),
            "service_type":     svc,
            "service_short":    _shorten_service_type(svc),
            "duration_min":     assignment.get("duration", 0),
            "total_carts":      scc.get("total_carts",  r.get("total_carts",  0)),
            "bags":             scc.get("bags",          r.get("bags",          0)),
            "ovs":              scc.get("ovs",           r.get("ovs",           0)),
            "pick_status":      scc.get("status", ""),
            "stage_by":         scc.get("stage_by", ""),
            "is_low_emission":  (
                assignment.get("is_low_emission", False)
                or "Low Emission" in svc or "Ironhide" in svc
            ),
            "is_nursery":       (
                assignment.get("is_nursery", False) or "Nursery" in svc
            ),
            "is_remote_debrief":(
                assignment.get("is_remote_debrief", False) or "Remote Debrief" in svc
            ),
            "is_cargo_bike":    code.startswith("BK_"),
            "notes":            _auto_notes(svc),
            "shares_with":      None,
            "partner_carts":    0,
            "combined_carts":   scc.get("total_carts", r.get("total_carts", 0)),
            "lane_utilization": 0,
            "dispatched":       False,
            "staged":           False,
        })
    return enriched


def _sort_pad(routes):
    return sorted(routes, key=lambda r: (
        0 if r.get("is_low_emission") else 1,
        _service_priority(r.get("service_type", "")),
        r.get("dsp", "ZZZ"),
    ))


# ─────────────────────────────────────────────────────────────────────────────
#  OPTIMISATION SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def _build_optimisation_summary(waves):
    all_routes = [
        r for w in waves
        for pad in (w["pad_a"], w["pad_b"])
        for r in pad["routes"]
    ]
    if not all_routes:
        return {
            "paired_routes": 0, "solo_routes": 0, "overcap_routes": 0,
            "overcap_route_list": [], "avg_utilisation": 0,
            "lanes_used": 0, "lanes_total": 30,
        }

    paired_routes      = sum(1 for r in all_routes if r.get("shares_with"))
    solo_routes        = sum(1 for r in all_routes if not r.get("shares_with") and r.get("total_carts", 0) <= 6)
    overcap_routes     = sum(1 for r in all_routes if r.get("total_carts", 0) > 6)
    overcap_route_list = [{"route": r["route"], "carts": r["total_carts"]}
                          for r in all_routes if r.get("total_carts", 0) > 6]
    utils              = [r.get("lane_utilization", 0) for r in all_routes if r.get("lane_utilization", 0) > 0]
    avg_utilisation    = round(sum(utils) / len(utils)) if utils else 0
    lane_nums          = [r.get("lane_num") for r in all_routes if r.get("lane_num")]

    return {
        "paired_routes":      paired_routes,
        "solo_routes":        solo_routes,
        "overcap_routes":     overcap_routes,
        "overcap_route_list": overcap_route_list,
        "avg_utilisation":    avg_utilisation,
        "lanes_used":         len(set(lane_nums)),
        "lanes_total":        30,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  PLAN SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary(waves, wave_c, scc_data, assignment_data):
    van_routes        = sum(w["pad_a"]["total_routes"] + w["pad_b"]["total_routes"] for w in waves)
    cargo_bike_routes = sum(wc["total_routes"] for wc in wave_c)
    total_carts       = sum(w["pad_a"]["total_carts"] + w["pad_b"]["total_carts"] for w in waves)

    dsp_counts = {}
    for w in waves:
        for pad in (w["pad_a"], w["pad_b"]):
            for r in pad["routes"]:
                dsp = r.get("dsp", "?")
                dsp_counts[dsp] = dsp_counts.get(dsp, 0) + 1

    unassigned = sum(
        1 for w in waves for pad in (w["pad_a"], w["pad_b"])
        for r in pad["routes"] if r.get("da_id") == "UNASSIGNED"
    )

    return {
        "total_waves":        len(waves),
        "total_wave_c":       len(wave_c),
        "total_routes":       van_routes + cargo_bike_routes,
        "van_routes":         van_routes,
        "cargo_bike_routes":  cargo_bike_routes,
        "total_carts":        total_carts,
        "unassigned_das":     unassigned,
        "dsp_breakdown":      dict(sorted(dsp_counts.items())),
        "dsp_count":          len(dsp_counts),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_time_str(time_str):
    for fmt in ("%H:%M", "%I:%M %p", "%I:%M%p"):
        try:
            return datetime.strptime(str(time_str).strip(), fmt)
        except ValueError:
            continue
    return datetime.min


def _fmt_time(dt):
    try:    return dt.strftime("%H:%M")
    except: return ""


def _today_str():
    return datetime.now().strftime("%Y-%m-%d")


def _auto_notes(service_type):
    notes = []
    if "Low Emission" in service_type or "Ironhide" in service_type:
        notes.append("Low Emission")
    if "Nursery" in service_type:
        m = re.search(r"Level (\d)", service_type)
        notes.append(f"Nursery L{m.group(1)}" if m else "Nursery")
    if "Remote Debrief" in service_type:
        notes.append("Remote Debrief")
    if "Ride Along" in service_type:
        notes.append("Ride Along")
    return ", ".join(notes)


def _shorten_service_type(st):
    st = st.strip()
    if "Low Emission Vehicle (350CF" in st: return "LEV"
    if "Low Emission Vehicle" in st:        return "LEV"
    if "Large Van" in st:                   return "LV"
    if "Medium Van" in st:                  return "MV"
    if "Nursery Route Level" in st:
        m   = re.search(r"Level (\d)", st)
        lvl = m.group(1) if m else "?"
        suf = " LEV" if "Low Emission" in st else ""
        return f"NR{lvl}{suf}"
    if "Standard Parcel" in st and "Van" not in st: return "SP"
    if "Remote Debrief" in st:              return "RD"
    if "AmFlex" in st:                      return "Flex"
    if "Cargo Electric Bicycle" in st:      return "eBike"
    return st[:12]


def _service_priority(service_type):
    for key, priority in SERVICE_TYPE_PRIORITY.items():
        if key in service_type:
            return priority
    return 50
