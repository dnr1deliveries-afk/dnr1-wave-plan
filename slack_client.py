"""
slack_client.py — Slack webhook integration for DNR1 Wave Plan Tool.

Sends wave alerts to DSP-specific OPS channels.
Each DSP has their own Slack webhook (from DSP_Webhooks_DNR1_v1.6.xlsx).

Output format matches the Wave Plan table:
  Arrival Window | Route Info | Loading Area
  First Entry | Last Entry | Route Number | Service Type | Driver ID | Carts | Staging Pad | Wave Time | Last Exit
"""

import requests
import json
from typing import Optional
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
#  DSP WEBHOOK CONFIGURATION
#  Source: DSP_Webhooks_DNR1_v1.6.xlsx (OPS channels only)
# ─────────────────────────────────────────────────────────────────────────────

DSP_OPS_WEBHOOKS = {
    "AKTD": "https://hooks.slack.com/triggers/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",
    "ATAG": "https://hooks.slack.com/triggers/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",
    "DELL": "https://hooks.slack.com/triggers/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",
    "DNZN": "https://hooks.slack.com/triggers/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",
    "DTTD": "https://hooks.slack.com/triggers/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",
    "DYYL": "https://hooks.slack.com/triggers/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",
    "HPLM": "https://hooks.slack.com/triggers/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",
    "KMIL": "https://hooks.slack.com/triggers/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",
    "MOLI": "https://hooks.slack.com/triggers/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",
    "SLTD": "https://hooks.slack.com/triggers/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",
    "ULSL": "https://hooks.slack.com/triggers/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",
    "VILO": "https://hooks.slack.com/triggers/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",
    "WACC": "https://hooks.slack.com/triggers/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",
}

# Metrics webhooks (for future use — KPI alerts, etc.)
DSP_METRICS_WEBHOOKS = {
    "AKTD": "https://hooks.slack.com/triggers/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",
    "ATAG": "https://hooks.slack.com/triggers/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",
    "DELL": "https://hooks.slack.com/triggers/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",
    "DNZN": "https://hooks.slack.com/triggers/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",
    "DTTD": "https://hooks.slack.com/triggers/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",
    "DYYL": "https://hooks.slack.com/triggers/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",
    "HPLM": "https://hooks.slack.com/triggers/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",
    "KMIL": "https://hooks.slack.com/triggers/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",
    "MOLI": "https://hooks.slack.com/triggers/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",
    "SLTD": "https://hooks.slack.com/triggers/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",
    "ULSL": "https://hooks.slack.com/triggers/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",
    "VILO": "https://hooks.slack.com/triggers/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",
    "WACC": "https://hooks.slack.com/triggers/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",
}

# ─────────────────────────────────────────────────────────────────────────────
#  TIMING CONFIG (matches wave_engine.py DEFAULT_CONFIG)
# ─────────────────────────────────────────────────────────────────────────────

ARRIVAL_OFFSET_MIN = 15   # First entry = wave_time - 15min
ON_PAD_OFFSET_MIN = 5     # Last entry = wave_time - 5min  
LAST_EXIT_MIN = 20        # Last exit = wave_time + 20min

# Service type short names for table display
SERVICE_TYPE_SHORT = {
    "Standard Parcel Medium Van": "Medium Van",
    "Standard Parcel - Large Van": "Large Van",
    "Standard Parcel - Low Emission Vehicle": "Low Emission",
    "Standard Parcel": "Standard",
    "Nursery Route": "Nursery",
    "Remote Debrief": "Remote Debrief",
    "AmFlex": "AmFlex",
}


# ─────────────────────────────────────────────────────────────────────────────
#  CORE SEND FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def send_to_webhook(webhook_url: str, message: str, blocks: list = None) -> dict:
    """
    Send a message to a specific Slack webhook.
    Returns {"success": True/False, "error": str or None}
    """
    if not webhook_url:
        return {"success": False, "error": "No webhook URL provided"}
    
    try:
        payload = {"text": message}
        if blocks:
            payload["blocks"] = blocks
        
        r = requests.post(webhook_url, json=payload, timeout=10)
        
        if r.status_code == 200:
            return {"success": True, "error": None}
        else:
            return {"success": False, "error": f"HTTP {r.status_code}: {r.text[:100]}"}
    
    except requests.Timeout:
        return {"success": False, "error": "Request timed out"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_to_dsp_ops(dsp: str, message: str, blocks: list = None) -> dict:
    """Send a message to a specific DSP's OPS channel."""
    webhook_url = DSP_OPS_WEBHOOKS.get(dsp.upper())
    if not webhook_url:
        return {"success": False, "error": f"No OPS webhook configured for DSP: {dsp}"}
    
    result = send_to_webhook(webhook_url, message, blocks)
    result["dsp"] = dsp
    return result


def send_to_dsp_metrics(dsp: str, message: str, blocks: list = None) -> dict:
    """Send a message to a specific DSP's Metrics channel."""
    webhook_url = DSP_METRICS_WEBHOOKS.get(dsp.upper())
    if not webhook_url:
        return {"success": False, "error": f"No Metrics webhook configured for DSP: {dsp}"}
    
    result = send_to_webhook(webhook_url, message, blocks)
    result["dsp"] = dsp
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  TIME HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _parse_time(time_str: str) -> datetime:
    """Parse HH:MM to datetime object (today's date)."""
    try:
        return datetime.strptime(time_str, "%H:%M")
    except:
        return datetime.now()


def _format_time(dt: datetime) -> str:
    """Format datetime to HH:MM."""
    return dt.strftime("%H:%M")


def _calc_arrival_window(wave_time_str: str) -> tuple:
    """Calculate First Entry and Last Entry from wave time."""
    wave_time = _parse_time(wave_time_str)
    first_entry = wave_time - timedelta(minutes=ARRIVAL_OFFSET_MIN)
    last_entry = wave_time - timedelta(minutes=ON_PAD_OFFSET_MIN)
    return _format_time(first_entry), _format_time(last_entry)


def _calc_last_exit(wave_time_str: str) -> str:
    """Calculate Last Exit from wave time."""
    wave_time = _parse_time(wave_time_str)
    last_exit = wave_time + timedelta(minutes=LAST_EXIT_MIN)
    return _format_time(last_exit)


def _get_service_short(service_type: str) -> str:
    """Get shortened service type for table display."""
    if not service_type:
        return "Standard"
    return SERVICE_TYPE_SHORT.get(service_type, service_type[:12])


def _format_driver_id(da_id: str) -> str:
    """Format driver ID for display (truncate if too long)."""
    if not da_id or da_id == "UNASSIGNED":
        return "UNASSIGNED"
    # Amazon DA IDs are like A1B2C3D4E5F6G7 — show last 8 chars
    if len(da_id) > 10:
        return f"...{da_id[-8:]}"
    return da_id


# ─────────────────────────────────────────────────────────────────────────────
#  WAVE ALERT FUNCTIONS — TABLE FORMAT
# ─────────────────────────────────────────────────────────────────────────────

def send_wave_alert_to_dsp(dsp: str, wave_label: str, wave_time: str,
                           routes: list, pad: str = None) -> dict:
    """
    Send a wave alert to a specific DSP's OPS channel in table format.
    
    Format:
      Arrival Window | Route Info | Loading Area
      First Entry | Last Entry | Route Number | Service Type | Driver ID | Carts | Staging Pad | Wave Time | Last Exit
    
    Args:
        dsp: DSP code (e.g., "MOLI")
        wave_label: e.g., "Wave 1"
        wave_time: e.g., "10:20"
        routes: List of route dicts for this DSP in this wave
        pad: Optional pad label ("A" or "B") if routes are pad-specific
    
    Returns:
        {"success": bool, "dsp": str, "routes_count": int, "error": str or None}
    """
    if not routes:
        return {"success": True, "dsp": dsp, "routes_count": 0, "error": "No routes for this DSP"}
    
    # Calculate timing
    first_entry, last_entry = _calc_arrival_window(wave_time)
    last_exit = _calc_last_exit(wave_time)
    
    # Calculate totals
    total_carts = sum(r.get("total_carts", 0) for r in routes)
    
    # Build the table message
    lines = []
    lines.append(f"🚛 *{wave_label} — {dsp}*")
    lines.append(f"📅 {datetime.now().strftime('%d/%m/%Y')} | {len(routes)} routes | {total_carts} carts")
    lines.append("")
    lines.append("```")
    lines.append("┌───────────────────┬──────────────────────────────────────────────────────────────┬───────────────────┐")
    lines.append("│  Arrival Window   │                         Route Info                          │   Loading Area    │")
    lines.append("├─────────┬─────────┼───────────┬─────────────┬───────────┬───────┬───────────────┼─────────┬─────────┤")
    lines.append("│First Ent│Last Ent │  Route    │Service Type │ Driver ID │ Carts │  Staging Pad  │Wave Time│Last Exit│")
    lines.append("├─────────┼─────────┼───────────┼─────────────┼───────────┼───────┼───────────────┼─────────┼─────────┤")
    
    for r in routes:
        route_code = r.get("route", "?")
        service_type = _get_service_short(r.get("service_type", ""))
        driver_id = _format_driver_id(r.get("da_id", ""))
        carts = r.get("total_carts", 0)
        lane = r.get("lane", r.get("lane_label", "?"))
        
        # Clean up lane format
        if lane and not lane.startswith("STG-"):
            lane = f"STG-{pad or 'A'}{lane}" if str(lane).isdigit() else lane
        
        # Truncate/pad fields to fit columns
        route_short = route_code[-8:] if len(route_code) > 8 else route_code  # e.g., "CA_A155"
        service_short = service_type[:11] if len(service_type) > 11 else service_type
        driver_short = driver_id[:9] if len(driver_id) > 9 else driver_id
        lane_short = lane[:13] if len(lane) > 13 else lane
        carts_str = str(carts)
        
        lines.append(
            f"│ {first_entry:^7} │ {last_entry:^7} │ {route_short:^9} │ {service_short:^11} │ {driver_short:^9} │ {carts_str:^5} │ {lane_short:^13} │ {wave_time:^7} │ {last_exit:^7} │"
        )
    
    lines.append("└─────────┴─────────┴───────────┴─────────────┴───────────┴───────┴───────────────┴─────────┴─────────┘")
    lines.append("```")
    
    message = "\n".join(lines)
    
    result = send_to_dsp_ops(dsp, message)
    result["routes_count"] = len(routes)
    result["total_carts"] = total_carts
    return result


def send_wave_alerts_to_all_dsps(wave: dict) -> dict:
    """
    Send wave alerts to ALL DSPs that have routes in this wave.
    
    Args:
        wave: Wave dict from wave_engine with pad_a and pad_b route lists
    
    Returns:
        {"sent": int, "failed": int, "results": [per-DSP results]}
    """
    wave_label = wave.get("wave_label", "Wave ?")
    
    # Collect routes by DSP across both pads
    dsp_routes = {}
    
    for pad_key, pad_label in [("pad_a", "A"), ("pad_b", "B")]:
        pad_data = wave.get(pad_key, {})
        pad_time = pad_data.get("wave_time", "??:??")
        routes = pad_data.get("routes", [])
        
        for route in routes:
            dsp = route.get("dsp", "UNKNOWN")
            if dsp not in dsp_routes:
                dsp_routes[dsp] = {"A": [], "B": [], "time_a": None, "time_b": None}
            
            # Add pad info to route for lane formatting
            route_with_pad = {**route, "pad": pad_label}
            dsp_routes[dsp][pad_label].append(route_with_pad)
            dsp_routes[dsp][f"time_{pad_label.lower()}"] = pad_time
    
    # Send to each DSP
    results = []
    sent = 0
    failed = 0
    
    for dsp, data in dsp_routes.items():
        # Send separate alerts for each pad (different timing)
        for pad_label in ["A", "B"]:
            routes = data[pad_label]
            if not routes:
                continue
            
            wave_time = data[f"time_{pad_label.lower()}"]
            
            result = send_wave_alert_to_dsp(
                dsp=dsp,
                wave_label=f"{wave_label} Pad {pad_label}",
                wave_time=wave_time,
                routes=routes,
                pad=pad_label
            )
            results.append(result)
            
            if result["success"]:
                sent += 1
            else:
                failed += 1
    
    return {
        "sent": sent,
        "failed": failed,
        "total_dsps": len(dsp_routes),
        "results": results
    }


def send_all_wave_alerts(wave_plan: dict) -> dict:
    """
    Send wave alerts for ALL waves in the plan to all DSPs.
    
    Args:
        wave_plan: Full wave plan dict from wave_engine
    
    Returns:
        {"waves_processed": int, "total_sent": int, "total_failed": int, "details": [...]}
    """
    waves = wave_plan.get("waves", [])
    
    total_sent = 0
    total_failed = 0
    details = []
    
    for wave in waves:
        result = send_wave_alerts_to_all_dsps(wave)
        total_sent += result["sent"]
        total_failed += result["failed"]
        details.append({
            "wave": wave.get("wave_label"),
            **result
        })
    
    return {
        "waves_processed": len(waves),
        "total_sent": total_sent,
        "total_failed": total_failed,
        "details": details
    }


# ─────────────────────────────────────────────────────────────────────────────
#  UTILITY FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def get_configured_dsps() -> list:
    """Return list of DSPs with configured OPS webhooks."""
    return list(DSP_OPS_WEBHOOKS.keys())


def test_dsp_webhook(dsp: str) -> dict:
    """Send a test message to a DSP's OPS channel."""
    message = f"🧪 Test message from DNR1 Wave Plan Tool\nDSP: {dsp}\nIf you see this, the webhook is working!"
    return send_to_dsp_ops(dsp, message)


def test_all_webhooks() -> dict:
    """Test all configured DSP webhooks. Returns summary."""
    results = []
    for dsp in DSP_OPS_WEBHOOKS.keys():
        result = test_dsp_webhook(dsp)
        results.append(result)
    
    success_count = sum(1 for r in results if r["success"])
    return {
        "total": len(results),
        "success": success_count,
        "failed": len(results) - success_count,
        "results": results
    }
