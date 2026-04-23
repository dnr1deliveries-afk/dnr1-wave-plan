"""
data_manager.py — Single source of truth for all data acquisition.

Handles three data sources with clear fallback chain:
  1. Playwright (auto-scrape, needs Midway + Chromium)
  2. Manual paste / upload (user provides data via UI)
  3. Prompt (block plan generation, tell user exactly what's missing)

MISSING DATA POLICY:
  - Never silently use empty data
  - Never generate a partial plan
  - Always tell the user what's missing and how to fix it
  - Plan generation is blocked until all required data is present
"""

from dataclasses import dataclass, field
from typing import Optional
from datetime import date
import re


# ─────────────────────────────────────────────
#  DATA STATUS
# ─────────────────────────────────────────────

@dataclass
class DataSource:
    name: str                        # Human-readable name
    key: str                         # Internal key
    required: bool = True            # If True, plan cannot generate without this
    available: bool = False          # Is data currently loaded?
    source_method: str = "none"      # "playwright", "upload", "paste", "none"
    record_count: int = 0            # How many records loaded
    loaded_at: Optional[str] = None  # HH:MM when loaded
    error: Optional[str] = None      # Last error message
    prompt_message: str = ""         # What to show user if missing
    prompt_action: str = ""          # What action fixes it (for UI buttons)


def build_data_sources() -> dict:
    """Return the master registry of all required data sources."""
    return {
        "dispatch": DataSource(
            name="Dispatch Plan", key="dispatch", required=True,
            prompt_message="The dispatch plan hasn't been loaded. This tells us which routes are in each wave and staging lanes.",
            prompt_action="auto_fetch",
        ),
        "assignment": DataSource(
            name="Assignment Planning", key="assignment", required=True,
            prompt_message="Assignment data hasn't been loaded. This tells us DSP, DA, and service type per route.",
            prompt_action="auto_fetch",
        ),
        "scc": DataSource(
            name="SCC Pick Export", key="scc", required=True,
            prompt_message="No SCC pick export uploaded. This is needed for accurate cart counts (bags + OVs) per route.",
            prompt_action="upload_csv",
        ),
        "pickorder": DataSource(
            name="Pick Order (Lane Spread)", key="pickorder", required=True,
            prompt_message=(
                "The Pick Order CSV hasn't been loaded. "
                "This spreads routes evenly across all 30 staging lanes. "
                "Download from Dispatch Planning: bottom of page, 'Day of Ops' CSV. "
                "Without it, routes use consecutive lanes from 1."
            ),
            prompt_action="auto_fetch",
        ),
        "sequencing": DataSource(
            name="Sequencing Complete", key="sequencing", required=True,
            prompt_message="Sequencing not yet finalised. Wave times may change. Wait before generating.",
            prompt_action="check_status",
        ),
        "auto_assign": DataSource(
            name="Auto-Assign Complete", key="auto_assign", required=True,
            prompt_message="Auto-assign not yet complete. Some routes may have no DA. Wait or manually assign.",
            prompt_action="check_status",
        ),
    }


def check_missing_data(sources: dict) -> list[dict]:
    """
    Returns a list of missing data items that must be resolved before
    the plan can be generated. Empty list = all clear.

    Each item: {key, name, message, action, severity}
    """
    missing = []
    severity_order = ["sequencing", "auto_assign", "dispatch", "assignment", "scc", "pickorder"]

    for key in severity_order:
        src = sources.get(key)
        if src and src.required and not src.available:
            missing.append({
                "key": key,
                "name": src.name,
                "message": src.prompt_message,
                "action": src.prompt_action,
                "severity": "blocking" if key in ("sequencing", "auto_assign") else "required",
                "error": src.error,
            })
    return missing


def can_generate_plan(sources: dict) -> tuple[bool, list]:
    """
    Returns (can_generate, missing_items).
    can_generate is True only when ALL required sources are available.
    """
    missing = check_missing_data(sources)
    return len(missing) == 0, missing


def get_data_summary(sources: dict) -> dict:
    """Summary for dashboard header."""
    total = sum(1 for s in sources.values() if s.required)
    loaded = sum(1 for s in sources.values() if s.required and s.available)
    return {
        "total_required": total,
        "loaded": loaded,
        "missing": total - loaded,
        "ready": loaded == total,
        "status_text": f"{loaded}/{total} data sources ready",
        "status_class": "green" if loaded == total else "yellow" if loaded > 0 else "red",
    }


# ─────────────────────────────────────────────
#  DATA LOADER
# ─────────────────────────────────────────────

class DataManager:
    """
    Manages all data acquisition with Playwright + fallback + prompting.
    Holds state for a single shift session.
    """

    def __init__(self):
        from datetime import datetime
        self.plan_date = date.today().strftime("%Y-%m-%d")
        self.sources = build_data_sources()
        self.dispatch_data = None
        self.assignment_data = None
        self.scc_data = None
        self.pickorder_data = None
        self._raw_dispatch_text = None
        self._raw_assignment_text = None
        self._raw_assignment_text = None

    def get_status(self) -> dict:
        return {
            "sources": {k: self._source_to_dict(v) for k, v in self.sources.items()},
            "summary": get_data_summary(self.sources),
            "missing": check_missing_data(self.sources),
            "can_generate": can_generate_plan(self.sources)[0],
            "plan_date": self.plan_date,
        }

    def _source_to_dict(self, src: DataSource) -> dict:
        return {
            "name": src.name,
            "available": src.available,
            "source_method": src.source_method,
            "record_count": src.record_count,
            "loaded_at": src.loaded_at,
            "error": src.error,
            "prompt_message": src.prompt_message if not src.available else None,
            "prompt_action": src.prompt_action if not src.available else None,
        }

    # ── AUTO-FETCH (Playwright) ──────────────────

    def auto_fetch_all(self) -> dict:
        """
        Try to fetch all data automatically via Playwright.
        Returns result dict with what succeeded and what failed.
        """
        from playwright_scraper import scrape_dispatch_plan, scrape_assignment_plan, get_scraper_status
        status = get_scraper_status()

        results = {"playwright": status, "dispatch": None, "assignment": None, "pickorder": None}

        if not status["can_auto_scrape"]:
            return {
                **results,
                "error": status["recommendation"],
                "fallback_required": True,
            }

        # Fetch dispatch
        dispatch_text, dispatch_err = scrape_dispatch_plan(self.plan_date)
        if dispatch_err:
            self.sources["dispatch"].error = dispatch_err
            results["dispatch"] = {"success": False, "error": dispatch_err}
        else:
            self._process_dispatch_text(dispatch_text)
            results["dispatch"] = {"success": True, "records": self.sources["dispatch"].record_count}

        # Fetch assignment
        assign_text, assign_err = scrape_assignment_plan(self.plan_date)
        if assign_err:
            self.sources["assignment"].error = assign_err
            results["assignment"] = {"success": False, "error": assign_err}
        else:
            self._process_assignment_text(assign_text)
            results["assignment"] = {"success": True, "records": self.sources["assignment"].record_count}

        # Fetch PickOrder CSV (auto-download via Playwright)
        from pickorder_scraper import download_pickorder_csv
        po_content, po_err = download_pickorder_csv(self.plan_date)
        if po_err:
            self.sources["pickorder"].error = (
                po_err + " — upload manually: Dispatch Planning → bottom → Day of Ops CSV"
            )
            results["pickorder"] = {"success": False, "error": po_err}
        else:
            po_result = self.load_pickorder_from_csv(po_content.encode())
            results["pickorder"] = po_result

        return results


        return results

    # ── MANUAL PASTE ────────────────────────────

    def load_dispatch_from_paste(self, pasted_text: str) -> dict:
        """
        Accept pasted text from the Dispatch Planning page.
        User copies all text from the page and pastes into the UI.
        """
        if not pasted_text or len(pasted_text.strip()) < 50:
            return {"success": False, "error": "Pasted text is too short — copy the full page content"}

        self._process_dispatch_text(pasted_text)

        if self.sources["dispatch"].available:
            return {
                "success": True,
                "records": self.sources["dispatch"].record_count,
                "message": f"Dispatch plan loaded — {self.sources['dispatch'].record_count} wave slots",
            }
        return {
            "success": False,
            "error": "Could not extract wave data from pasted text. Make sure you copied from the Dispatch Planning page.",
        }

    def load_assignment_from_paste(self, pasted_text: str) -> dict:
        """Accept pasted text from Assignment Planning page."""
        if not pasted_text or len(pasted_text.strip()) < 50:
            return {"success": False, "error": "Pasted text is too short"}

        self._process_assignment_text(pasted_text)

        if self.sources["assignment"].available:
            return {
                "success": True,
                "records": self.sources["assignment"].record_count,
            }
        return {
            "success": False,
            "error": "Could not extract assignment data. Copy from the Assignment Planning page.",
        }

    def load_scc_from_csv(self, file_content: bytes) -> dict:
        """Accept uploaded SCC CSV bytes."""
        from scc_parser import parse_scc_csv, get_scc_summary
        try:
            data = parse_scc_csv(file_content)
            if not data:
                return {"success": False, "error": "No routes found in CSV — check file format"}
            self.scc_data = data
            summary = get_scc_summary(data)
            self._mark_available("scc", "upload", summary["total_routes"])
            return {
                "success": True,
                "records": summary["total_routes"],
                "total_carts": summary["total_carts"],
                "picked": summary["picked"],
                "message": f"✅ SCC loaded — {summary['total_routes']} routes, {summary['total_carts']} carts ({summary['picked']} picked)",
            }
        except Exception as e:
            return {"success": False, "error": f"CSV parse error: {str(e)}"}

    def load_pickorder_from_csv(self, file_content: bytes) -> dict:
        """Accept uploaded PickOrder CSV bytes (or str from auto-download)."""
        from pickorder_parser import parse_pickorder_csv, get_pickorder_summary
        try:
            if isinstance(file_content, str):
                file_content = file_content.encode("utf-8")
            data = parse_pickorder_csv(file_content)
            if not data or not data.get("route_to_lane"):
                return {"success": False, "error": "No routes found in PickOrder CSV — check file format"}
            self.pickorder_data = data
            summary = get_pickorder_summary(data)
            self._mark_available("pickorder", "upload", data["total_routes"])
            if summary["has_warnings"]:
                self.sources["pickorder"].error = "; ".join(summary["warnings"][:3])
            return {
                "success": True,
                "records": data["total_routes"],
                "message": summary["message"],
            }
        except Exception as e:
            return {"success": False, "error": f"PickOrder parse error: {str(e)}"}

    # ── READINESS FLAGS ──────────────────────────

    def set_sequencing_complete(self, complete: bool, unplanned: list = None):
        src = self.sources["sequencing"]
        if complete:
            self._mark_available("sequencing", "api", 1)
            if unplanned:
                src.error = f"⚠️ {len(unplanned)} unplanned: {', '.join(unplanned)}"
        else:
            src.available = False
            src.source_method = "none"

    def set_auto_assign_complete(self, complete: bool, unassigned_count: int = 0):
        if complete and unassigned_count == 0:
            self._mark_available("auto_assign", "api", 1)
        elif complete and unassigned_count > 0:
            self._mark_available("auto_assign", "api", 1)
            self.sources["auto_assign"].error = f"⚠️ {unassigned_count} DSP routes still unassigned"
        else:
            self.sources["auto_assign"].available = False

    # ── INTERNAL PROCESSORS ─────────────────────

    def _process_dispatch_text(self, text: str):
        """Parse scraped/pasted dispatch page text into structured data."""
        from amzl_client import _parse_dispatch_plan
        try:
            data = _parse_dispatch_plan(text)
            wave_count = len([k for k, v in data.items()
                               if any(r.get("route") for pad in v.values() for r in pad)])

            # Also extract readiness signals
            finalized = "Plan has been finalized" in text or "plan has been finalized" in text.lower()
            unplanned = re.findall(r"(CA_A\d+|BK_A\d+)\s*:\s*MATCHING_WAVE_NOT_FOUND", text)
            unplanned_count_match = re.search(r"Unplanned Routes\s*\n\s*(\d+)", text)
            unplanned_count = int(unplanned_count_match.group(1)) if unplanned_count_match else len(unplanned)

            self.dispatch_data = data
            self._mark_available("dispatch", "playwright" if not self._is_paste() else "paste", wave_count)
            self.set_sequencing_complete(finalized, unplanned)

            if unplanned_count > 0:
                self.sources["dispatch"].error = f"⚠️ {unplanned_count} unplanned routes: {', '.join(unplanned)}"

        except Exception as e:
            self.sources["dispatch"].error = f"Parse error: {str(e)}"

    def _process_assignment_text(self, text: str):
        """Parse scraped/pasted assignment page text into structured data."""
        from amzl_client import _parse_assignment_data
        try:
            data = _parse_assignment_data(text)
            route_count = len(data)

            auto_assign = "Auto Assign completed" in text
            dsp_unassigned_match = re.search(r"DSP Routes Not Assigned\s*\n\s*(\d+)", text)
            dsp_unassigned = int(dsp_unassigned_match.group(1)) if dsp_unassigned_match else -1

            self.assignment_data = data
            self._mark_available("assignment", "playwright" if not self._is_paste() else "paste", route_count)
            self.set_auto_assign_complete(auto_assign, max(dsp_unassigned, 0))

        except Exception as e:
            self.sources["assignment"].error = f"Parse error: {str(e)}"

    def _mark_available(self, key: str, method: str, count: int):
        from datetime import datetime
        src = self.sources[key]
        src.available = True
        src.source_method = method
        src.record_count = count
        src.loaded_at = datetime.now().strftime("%H:%M")
        src.error = None

    def _is_paste(self) -> bool:
        """Heuristic — if called outside auto_fetch, it's a paste."""
        return True  # Simplified — could track context if needed
