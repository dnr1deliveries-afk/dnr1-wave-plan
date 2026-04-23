"""
app.py — DNR1 Wave Plan Tool v1.5
Flask web app with full missing data prompting + DSP Slack integration + Excel Export.

If any required data source is missing, the app:
  1. Shows exactly what is missing and why
  2. Offers the correct fix action (auto-fetch, upload, paste)
  3. Blocks plan generation until all sources are present
  4. Never silently produces a partial or empty plan

Slack Integration:
  - Sends wave alerts to DSP-specific OPS channels
  - Each DSP receives only their routes
  - Configurable via DSP_Webhooks_DNR1_v1.6.xlsx

Excel Export:
  - Download print-optimized WAVE PLAN Excel file
  - Space-efficient: only includes populated lane rows
  - Professional formatting matching yard marshal format

File Caching (v1.5):
  - Uploaded CSV files are cached to disk
  - Survives server restarts
  - Clear cache button in UI
"""

import os
import io
import json
from datetime import datetime, date
from flask import (
    Flask, render_template, request, jsonify,
    session, redirect, url_for, flash, send_file
)
from werkzeug.utils import secure_filename

from data_manager import DataManager
from wave_engine import build_wave_plan
from slack_client import (
    send_to_dsp_ops,
    send_wave_alert_to_dsp,
    send_wave_alerts_to_all_dsps,
    send_all_wave_alerts,
    get_configured_dsps,
    test_dsp_webhook,
    test_all_webhooks,
    DSP_OPS_WEBHOOKS,
)
from file_cache import (
    cache_file, cache_text, load_cached_file, load_cached_text,
    get_cache_status, clear_cache, clear_all_cache, has_cached_file,
)

# ─────────────────────────────────────────────────────────────────────────────
#  APP INIT
# ─────────────────────────────────────────────────────────────────────────────

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dnr1-wave-dev-key")
app.config["MAX_CONTENT_LENGTH"] = 5 * 1024 * 1024  # 5MB

MIDWAY_BYPASS = os.environ.get("MIDWAY_BYPASS", "false").lower() == "true"

# Single DataManager instance for the shift session
_dm = DataManager()

# Wave status tracking (cleared_at, swiped_at per wave)
_wave_statuses = {}

# Built plan cache
_plan_cache = {"wave_plan": None, "built_at": None}


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    status = _dm.get_status()
    can_gen, missing = _get_plan_readiness()
    wave_plan = _plan_cache.get("wave_plan")

    if wave_plan:
        _merge_statuses(wave_plan)

    from playwright_scraper import get_scraper_status
    scraper = get_scraper_status()

    return render_template(
        "am_view.html",
        status=status,
        missing=missing,
        can_generate=can_gen,
        wave_plan=wave_plan,
        scraper=scraper,
        built_at=_plan_cache.get("built_at"),
        plan_date=date.today().strftime("%A %d %B %Y"),
        today=date.today().strftime("%Y-%m-%d"),
        configured_dsps=get_configured_dsps(),
    )


# ─────────────────────────────────────────────────────────────────────────────
#  DATA ACQUISITION ROUTES
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/fetch-all", methods=["POST"])
def fetch_all():
    """Trigger Playwright auto-fetch of dispatch + assignment data."""
    results = _dm.auto_fetch_all()

    if results.get("fallback_required"):
        flash(f"⚠️ Auto-fetch unavailable: {results.get('error')} — use manual input below.", "warning")
        return redirect(url_for("index"))

    messages = []
    if results.get("dispatch", {}).get("success"):
        r = results["dispatch"]["records"]
        messages.append(f"✅ Dispatch plan loaded — {r} wave slots")
    else:
        err = results.get("dispatch", {}).get("error", "Unknown error")
        messages.append(f"❌ Dispatch failed: {err}")

    if results.get("assignment", {}).get("success"):
        r = results["assignment"]["records"]
        messages.append(f"✅ Assignment data loaded — {r} routes")
    else:
        err = results.get("assignment", {}).get("error", "Unknown error")
        messages.append(f"❌ Assignment failed: {err}")

    for msg in messages:
        flash(msg, "success" if msg.startswith("✅") else "error")

    # Auto-build plan if all data present
    can_gen, _ = _get_plan_readiness()
    if can_gen:
        _build_plan()
        flash("🚛 Wave plan generated!", "success")

    return redirect(url_for("index"))


@app.route("/paste-dispatch", methods=["POST"])
def paste_dispatch():
    """Accept pasted dispatch plan text."""
    text = request.form.get("dispatch_text", "").strip()
    if not text:
        flash("❌ No text pasted — copy the full content from the Dispatch Planning page", "error")
        return redirect(url_for("index"))

    result = _dm.load_dispatch_from_paste(text)
    if result["success"]:
        flash(result["message"], "success")
        _try_auto_build()
    else:
        flash(f"❌ {result['error']}", "error")
    return redirect(url_for("index"))


@app.route("/paste-assignment", methods=["POST"])
def paste_assignment():
    """Accept pasted assignment planning text."""
    text = request.form.get("assignment_text", "").strip()
    if not text:
        flash("❌ No text pasted — copy the full content from the Assignment Planning page", "error")
        return redirect(url_for("index"))

    result = _dm.load_assignment_from_paste(text)
    if result["success"]:
        flash(f"✅ Assignment data loaded — {result['records']} routes", "success")
        _try_auto_build()
    else:
        flash(f"❌ {result['error']}", "error")
    return redirect(url_for("index"))


@app.route("/upload-scc", methods=["POST"])
def upload_scc():
    """Accept SCC pick export CSV upload."""
    if "scc_file" not in request.files or not request.files["scc_file"].filename:
        flash("❌ No file selected — please choose your SCC pick export CSV", "error")
        return redirect(url_for("index"))
    f = request.files["scc_file"]
    if not f.filename.lower().endswith(".csv"):
        flash("❌ Wrong file type — upload a CSV file (PICK_YYYY-MM-DD.csv)", "error")
        return redirect(url_for("index"))
    result = _dm.load_scc_from_csv(f.read())
    if result["success"]:
        flash(result["message"], "success")
        _try_auto_build()
    else:
        flash(f"❌ {result['error']}", "error")
    return redirect(url_for("index"))


@app.route("/upload-pickorder", methods=["POST"])
def upload_pickorder():
    """Accept PickOrder CSV upload (manual fallback)."""
    if "pickorder_file" not in request.files or not request.files["pickorder_file"].filename:
        flash("❌ No file selected — download the PickOrder CSV from Dispatch Planning (bottom of page → Day of Ops)", "error")
        return redirect(url_for("index"))
    f = request.files["pickorder_file"]
    if not f.filename.lower().endswith(".csv"):
        flash("❌ Wrong file type — upload the PickOrder CSV file", "error")
        return redirect(url_for("index"))
    result = _dm.load_pickorder_from_csv(f.read())
    if result["success"]:
        flash(result["message"], "success")
        # If plan already exists, re-apply spread immediately
        if _plan_cache.get("wave_plan") and _dm.pickorder_data:
            from pickorder_parser import apply_pickorder_to_plan
            apply_pickorder_to_plan(_plan_cache["wave_plan"], _dm.pickorder_data)
            flash("🔀 Lane spread applied to existing plan", "success")
        _try_auto_build()
    else:
        flash(f"❌ {result['error']}", "error")
    return redirect(url_for("index"))


# ─────────────────────────────────────────────────────────────────────────────
#  PLAN GENERATION
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/generate-plan", methods=["POST"])
def generate_plan():
    """Manually trigger plan generation — only if all data present."""
    can_gen, missing = _get_plan_readiness()
    if not can_gen:
        missing_names = [m["name"] for m in missing]
        flash(f"❌ Cannot generate plan — missing: {', '.join(missing_names)}", "error")
        return redirect(url_for("index"))

    wave_plan = _build_plan()
    if wave_plan:
        s = wave_plan["summary"]
        flash(
            f"✅ Wave plan generated — {s['total_waves']} waves, "
            f"{s['total_routes']} routes, {s['total_carts']} carts",
            "success",
        )
    else:
        flash("❌ Plan generation failed — check error logs", "error")
    return redirect(url_for("index"))


@app.route("/clear-plan", methods=["POST"])
@app.route("/clear-plan", methods=["POST"])
def clear_plan():
    """Clear the current plan and all loaded data — fresh start."""
    global _dm, _wave_statuses, _plan_cache
    _dm = DataManager()
    _wave_statuses = {}
    _plan_cache = {"wave_plan": None, "built_at": None}
    flash("🔄 Plan cleared — ready for fresh data", "success")
    return redirect(url_for("index"))


@app.route("/clear-cache", methods=["POST"])
def clear_cache_route():
    """Clear all cached files from disk."""
    result = clear_all_cache()
    if result["success"]:
        flash(f"🗑️ Cache cleared — {result['files_deleted']} files deleted", "success")
    else:
        flash(f"⚠️ Cache clear had errors: {result.get('errors', 'Unknown')}", "warning")
    return redirect(url_for("index"))


@app.route("/api/cache-status", methods=["GET"])
def api_cache_status():
    """Get current cache status."""
    return jsonify(get_cache_status())


def update_wave_status(wave_label):
    data = request.get_json() or {}
    action = data.get("action")
    now = datetime.now().strftime("%H:%M")

    if wave_label not in _wave_statuses:
        _wave_statuses[wave_label] = {}

    st = _wave_statuses[wave_label]
    if action == "staged":
        st["status"] = "staging"
    elif action == "cleared":
        st["status"] = "cleared"
        st["cleared_at"] = now
        _send_cleared_alert(wave_label, now)
    elif action == "swiped":
        st["status"] = "dispatched"
        st["swiped_at"] = now

    return jsonify({"success": True, "wave": wave_label, "action": action, "time": now})


# ─────────────────────────────────────────────────────────────────────────────
#  VIEWS
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/dsp/<dsp_code>")
def dsp_view(dsp_code):
    dsp_code = dsp_code.upper()
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return render_template(
            "dsp_view.html",
            dsp_code=dsp_code, waves=[], wave_c=[],
            plan_date=date.today().strftime("%A %d %B %Y"),
            last_refreshed=None,
            no_plan=True,
        )
    _merge_statuses(wave_plan)
    dsp_waves = _filter_dsp_waves(wave_plan, dsp_code)
    return render_template(
        "dsp_view.html",
        dsp_code=dsp_code,
        waves=dsp_waves,
        wave_c=[],
        plan_date=date.today().strftime("%A %d %B %Y"),
        last_refreshed=_plan_cache.get("built_at"),
    )


@app.route("/wave-print")
def wave_print():
    wave_plan = _plan_cache.get("wave_plan")
    if wave_plan:
        _merge_statuses(wave_plan)
    return render_template(
        "wave_print.html",
        wave_plan=wave_plan,
        plan_date=date.today().strftime("%A %d %B %Y"),
        station="DNR1",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  EXCEL EXPORT & PRINT
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/export/wave-plan")
def export_wave_plan_excel():
    """Download print-optimized WAVE PLAN Excel file."""
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        flash("❌ No wave plan to export — generate a plan first", "error")
        return redirect(url_for("index"))
    
    try:
        from wave_plan_excel_generator import generate_wave_plan_excel
        
        # Generate to temp file
        date_str = date.today().strftime("%Y-%m-%d")
        filename = f"WavePlan_DNR1_{date_str}.xlsx"
        temp_path = os.path.join(app.root_path, "temp", filename)
        
        # Ensure temp directory exists
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        
        # Generate the Excel file
        generate_wave_plan_excel(wave_plan, temp_path, station="DNR1")
        
        # Send file for download
        return send_file(
            temp_path,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        app.logger.error(f"Excel export failed: {e}")
        flash(f"❌ Excel export failed: {str(e)}", "error")
        return redirect(url_for("index"))


@app.route("/export/wave-plan-print")
def export_wave_plan_print():
    """Generate Excel file and open for printing (returns file inline)."""
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return jsonify({"error": "No wave plan to export"}), 404
    
    try:
        from wave_plan_excel_generator import generate_wave_plan_excel
        
        # Generate to BytesIO for inline display
        date_str = date.today().strftime("%Y-%m-%d")
        filename = f"WavePlan_DNR1_{date_str}.xlsx"
        temp_path = os.path.join(app.root_path, "temp", filename)
        
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        generate_wave_plan_excel(wave_plan, temp_path, station="DNR1")
        
        # Return file inline (browser will open it)
        return send_file(
            temp_path,
            as_attachment=False,  # Inline display
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    except Exception as e:
        app.logger.error(f"Excel print export failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/export/wave-plan")
def api_export_wave_plan():
    """API endpoint to generate and return Excel file path."""
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return jsonify({"error": "No wave plan loaded"}), 404
    
    try:
        from wave_plan_excel_generator import generate_wave_plan_excel
        
        date_str = date.today().strftime("%Y-%m-%d")
        filename = f"WavePlan_DNR1_{date_str}.xlsx"
        temp_path = os.path.join(app.root_path, "temp", filename)
        
        os.makedirs(os.path.dirname(temp_path), exist_ok=True)
        output_path = generate_wave_plan_excel(wave_plan, temp_path, station="DNR1")
        
        return jsonify({
            "success": True,
            "filename": filename,
            "path": output_path,
            "download_url": url_for("export_wave_plan_excel", _external=True),
            "summary": wave_plan.get("summary", {})
        })
    except Exception as e:
        app.logger.error(f"API Excel export failed: {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
#  API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────
# ───────────────────────────────────────────────────────────────────────────────
#  REMOTE DATA PUSH API (for Render deployment)
# ───────────────────────────────────────────────────────────────────────────────

@app.route("/api/push-data", methods=["POST"])
def api_push_data():
    """
    Receive data pushed from a local machine with Midway access.
    This allows the Render deployment to receive auto-fetched data.
    
    Expected JSON payload:
    {
        "plan_date": "2026-04-23",
        "dispatch_text": "...",
        "assignment_text": "..."
    }
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data received"}), 400
    
    results = {
        "plan_date": data.get("plan_date"),
        "dispatch": None,
        "assignment": None,
        "plan_generated": False
    }
    
    # Load dispatch data
    dispatch_text = data.get("dispatch_text")
    if dispatch_text:
        result = _dm.load_dispatch_from_paste(dispatch_text)
        results["dispatch"] = {
            "success": result.get("success", False),
            "records": result.get("records", 0),
            "error": result.get("error")
        }
    
    # Load assignment data
    assignment_text = data.get("assignment_text")
    if assignment_text:
        result = _dm.load_assignment_from_paste(assignment_text)
        results["assignment"] = {
            "success": result.get("success", False),
            "records": result.get("records", 0),
            "error": result.get("error")
        }
    
    # Auto-build plan if all data present
    can_gen, _ = _get_plan_readiness()
    if can_gen:
        wave_plan = _build_plan()
        if wave_plan:
            results["plan_generated"] = True
            results["summary"] = wave_plan.get("summary", {})
    
    return jsonify(results)


# ───────────────────────────────────────────────────────────────────────────────
#  API ENDPOINTS
# ───────────────────────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    """JSON status — used for polling from the dashboard."""
    status = _dm.get_status()
    can_gen, missing = _get_plan_readiness()
    return jsonify({
        "status": status,
        "can_generate": can_gen,
        "missing": missing,
        "plan_built": _plan_cache.get("wave_plan") is not None,
        "built_at": _plan_cache.get("built_at"),
    })


@app.route("/api/plan")
def api_plan():
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return jsonify({"error": "No plan loaded", "missing": check_missing_data(_dm.sources)}), 404
    return jsonify(wave_plan)

    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return jsonify({"error": "No plan loaded", "missing": check_missing_data(_dm.sources)}), 404
    return jsonify(wave_plan)


# ─────────────────────────────────────────────────────────────────────────────
#  SLACK API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/slack/dsps")
def api_slack_dsps():
    """List all configured DSP webhooks."""
    return jsonify({
        "dsps": get_configured_dsps(),
        "count": len(DSP_OPS_WEBHOOKS)
    })


@app.route("/api/slack/test/<dsp>", methods=["POST"])
def api_slack_test_dsp(dsp):
    """Send a test message to a specific DSP's OPS channel."""
    dsp = dsp.upper()
    result = test_dsp_webhook(dsp)
    return jsonify(result)


@app.route("/api/slack/test-all", methods=["POST"])
def api_slack_test_all():
    """Test all DSP webhooks — sends a test message to each OPS channel."""
    result = test_all_webhooks()
    return jsonify(result)


@app.route("/api/slack/send-wave/<int:wave_num>", methods=["POST"])
def api_slack_send_wave(wave_num):
    """Send wave alerts to all DSPs for a specific wave."""
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return jsonify({"error": "No plan loaded"}), 404
    
    waves = wave_plan.get("waves", [])
    wave = next((w for w in waves if w["wave_number"] == wave_num), None)
    if not wave:
        return jsonify({"error": f"Wave {wave_num} not found"}), 404
    
    result = send_wave_alerts_to_all_dsps(wave)
    return jsonify(result)


@app.route("/api/slack/send-all", methods=["POST"])
def api_slack_send_all():
    """Send ALL wave alerts to ALL DSPs — use with caution!"""
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return jsonify({"error": "No plan loaded"}), 404
    
    result = send_all_wave_alerts(wave_plan)
    return jsonify(result)


@app.route("/api/slack/send-dsp/<dsp>/<int:wave_num>", methods=["POST"])
def api_slack_send_dsp_wave(dsp, wave_num):
    """Send a specific wave alert to a specific DSP."""
    dsp = dsp.upper()
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return jsonify({"error": "No plan loaded"}), 404
    
    waves = wave_plan.get("waves", [])
    wave = next((w for w in waves if w["wave_number"] == wave_num), None)
    if not wave:
        return jsonify({"error": f"Wave {wave_num} not found"}), 404
    
    # Get routes for this DSP in this wave
    dsp_routes_a = [r for r in wave["pad_a"]["routes"] if r.get("dsp") == dsp]
    dsp_routes_b = [r for r in wave["pad_b"]["routes"] if r.get("dsp") == dsp]
    all_routes = dsp_routes_a + dsp_routes_b
    
    if not all_routes:
        return jsonify({"error": f"No routes for {dsp} in Wave {wave_num}"}), 404
    
    # Determine pad info
    if dsp_routes_a and dsp_routes_b:
        pad = "A+B"
        wave_time = wave["pad_a"]["wave_time"]
    elif dsp_routes_a:
        pad = "A"
        wave_time = wave["pad_a"]["wave_time"]
    else:
        pad = "B"
        wave_time = wave["pad_b"]["wave_time"]
    
    result = send_wave_alert_to_dsp(
        dsp=dsp,
        wave_label=wave["wave_label"],
        wave_time=wave_time,
        routes=all_routes,
        pad=pad
    )
    return jsonify(result)


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_plan_readiness():
    """Check both data sources AND planning portal readiness."""
    from data_manager import can_generate_plan, check_missing_data
    return can_generate_plan(_dm.sources)


def check_missing_data(sources):
    from data_manager import check_missing_data as _cmd
    return _cmd(sources)


def _build_plan():
    """Build the wave plan — only called when all data is confirmed present."""
    try:
        wave_plan = build_wave_plan(
            _dm.dispatch_data or {},
            _dm.assignment_data or {},
            _dm.scc_data or {},
        )

        # Apply pickorder lane spreading if available
        if _dm.pickorder_data:
            from pickorder_parser import apply_pickorder_to_plan
            apply_pickorder_to_plan(wave_plan, _dm.pickorder_data)
            app.logger.info(
                f"Pickorder spread applied: {wave_plan.get('pickorder_stats', {}).get('routes_mapped', 0)} routes"
            )
        else:
            wave_plan["pickorder_applied"] = False

        _plan_cache["wave_plan"] = wave_plan
        _plan_cache["built_at"] = datetime.now().strftime("%H:%M:%S")
        
        # Don't auto-send alerts on plan build — let user trigger manually
        app.logger.info(f"Wave plan built: {wave_plan['summary']['total_waves']} waves")
        
        return wave_plan
    except Exception as e:
        app.logger.error(f"Plan build failed: {e}")
        return None


def _try_auto_build():
    """Build plan automatically if all data just became available."""
    can_gen, _ = _get_plan_readiness()
    if can_gen and not _plan_cache.get("wave_plan"):
        wave_plan = _build_plan()
        if wave_plan:
            flash("🚛 All data loaded — wave plan generated automatically!", "success")


def _merge_statuses(wave_plan):
    for wave in wave_plan.get("waves", []):
        label = wave["wave_label"]
        if label in _wave_statuses:
            wave.update(_wave_statuses[label])
    for wave in wave_plan.get("wave_c", []):
        label = wave["wave_label"]
        if label in _wave_statuses:
            wave.update(_wave_statuses[label])


def _filter_dsp_waves(wave_plan, dsp_code):
    result = []
    for wave in wave_plan.get("waves", []):
        a = [r for r in wave["pad_a"]["routes"] if r.get("dsp") == dsp_code]
        b = [r for r in wave["pad_b"]["routes"] if r.get("dsp") == dsp_code]
        if a or b:
            result.append({
                **wave,
                "pad_a": {**wave["pad_a"], "routes": a},
                "pad_b": {**wave["pad_b"], "routes": b},
            })
    return result


def _send_cleared_alert(wave_label, time_str):
    """Send wave cleared alerts to all DSPs with routes in this wave."""
    wave_plan = _plan_cache.get("wave_plan")
    if not wave_plan:
        return
    
    for wave in wave_plan.get("waves", []):
        if wave["wave_label"] == wave_label:
            # Group routes by DSP
            dsp_routes = {}
            for r in wave["pad_a"]["routes"] + wave["pad_b"]["routes"]:
                dsp = r.get("dsp", "UNKNOWN")
                if dsp not in dsp_routes:
                    dsp_routes[dsp] = []
                dsp_routes[dsp].append(r)
            
            # Send cleared notification to each DSP
            for dsp, routes in dsp_routes.items():
                message = (
                    f"✅ *{wave_label} CLEARED* — {time_str}\n"
                    f"{len(routes)} routes dispatched"
                )
                send_to_dsp_ops(dsp, message)
            
            app.logger.info(f"Sent cleared alerts for {wave_label} to {len(dsp_routes)} DSPs")
            return


# ─────────────────────────────────────────────────────────────────────────────
#  RUN
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    debug = os.environ.get("FLASK_DEBUG", "true").lower() == "true"
    print(f"\n🚛 DNR1 Wave Plan Tool v1.4 starting on http://localhost:{port}")
    print(f"📱 Slack webhooks configured for {len(DSP_OPS_WEBHOOKS)} DSPs: {', '.join(DSP_OPS_WEBHOOKS.keys())}")
    print(f"📄 Excel export available at /export/wave-plan\n")
    app.run(host="0.0.0.0", port=port, debug=debug)
