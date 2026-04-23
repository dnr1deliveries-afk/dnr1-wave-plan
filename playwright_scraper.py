"""
playwright_scraper.py — Headless browser scraper for AMZL Planning Portal.
Uses Playwright with Midway cookies injected — renders the React SPA fully
before extracting data.

Falls back gracefully if Playwright is not installed or Midway is expired.
"""

import os
import re
import json
import time

MIDWAY_COOKIE_PATH = os.path.expanduser("~/.midway/cookie")

DISPATCH_URL = (
    "https://eu.dispatch.planning.last-mile.a2z.com"
    "/dispatch-planning/DNR1/761553f5-9fc1-4cef-8815-b974bc63f0a9/{date}?laborType=DSP"
)
ASSIGNMENT_URL = (
    "https://eu.assignment.planning.last-mile.a2z.com"
    "/assignment-planning/DNR1/761553f5-9fc1-4cef-8815-b974bc63f0a9/{date}"
)

# How long to wait for React to render (seconds)
RENDER_WAIT = 8


# ─────────────────────────────────────────────
#  AVAILABILITY CHECK
# ─────────────────────────────────────────────

def playwright_available() -> bool:
    """Check if Playwright + Chromium are installed."""
    try:
        from playwright.sync_api import sync_playwright
        return True
    except ImportError:
        return False


def midway_valid() -> bool:
    """Check if Midway cookie exists and is < 20h old."""
    try:
        age = (time.time() - os.stat(MIDWAY_COOKIE_PATH).st_mtime) / 3600
        return age < 20
    except FileNotFoundError:
        return False


# ─────────────────────────────────────────────
#  COOKIE LOADER
# ─────────────────────────────────────────────

def _load_playwright_cookies() -> list:
    """
    Parse Netscape cookie file into Playwright cookie format.
    Returns list of {name, value, domain, path, secure, httpOnly} dicts.
    """
    cookies = []
    try:
        with open(MIDWAY_COOKIE_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                # Strip #HttpOnly_ prefix (sets httpOnly=True)
                http_only = line.startswith("#HttpOnly_")
                if http_only:
                    line = line[len("#HttpOnly_"):]
                if line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) < 7:
                    continue
                domain_raw = parts[0]
                path = parts[2]
                secure = parts[3].upper() == "TRUE"
                try:
                    expires = int(parts[4])
                except ValueError:
                    expires = -1
                name = parts[5]
                value = parts[6]

                cookies.append({
                    "name": name,
                    "value": value,
                    "domain": domain_raw.lstrip("."),
                    "path": path,
                    "secure": secure,
                    "httpOnly": http_only,
                    "sameSite": "None" if secure else "Lax",
                })
    except Exception as e:
        print(f"[Playwright] Cookie load error: {e}")
    return cookies


# ─────────────────────────────────────────────
#  MAIN SCRAPERS
# ─────────────────────────────────────────────

def scrape_dispatch_plan(plan_date: str) -> tuple[str, str]:
    """
    Scrape the Dispatch Planning page using Playwright.
    Returns: (page_text, error_message)
    error_message is None on success, string on failure.
    """
    if not playwright_available():
        return "", "Playwright not installed — run: pip install playwright && playwright install chromium"
    if not midway_valid():
        return "", "Midway session expired — run mwinit"

    url = DISPATCH_URL.format(date=plan_date)
    return _scrape_page(url, wait_for_selector="text=STG-A", timeout_ms=20000)


def scrape_assignment_plan(plan_date: str) -> tuple[str, str]:
    """
    Scrape the Assignment Planning page using Playwright.
    Returns: (page_text, error_message)
    """
    if not playwright_available():
        return "", "Playwright not installed"
    if not midway_valid():
        return "", "Midway session expired — run mwinit"

    url = ASSIGNMENT_URL.format(date=plan_date)
    return _scrape_page(url, wait_for_selector="text=Total Routes", timeout_ms=20000)


def _scrape_page(url: str, wait_for_selector: str = None, timeout_ms: int = 15000) -> tuple[str, str]:
    """
    Launch headless Chromium, inject Midway cookies, navigate, wait for render.
    Returns (page_text, error_or_None).
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    except ImportError:
        return "", "Playwright not installed"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            )

            # Inject Midway cookies before navigation
            cookies = _load_playwright_cookies()
            if cookies:
                context.add_cookies(cookies)

            page = context.new_page()

            # Navigate
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

            # Wait for key content to render
            if wait_for_selector:
                try:
                    page.wait_for_selector(wait_for_selector, timeout=timeout_ms)
                except PlaywrightTimeout:
                    # Page loaded but selector not found — might still have useful data
                    page.wait_for_timeout(RENDER_WAIT * 1000)
            else:
                page.wait_for_timeout(RENDER_WAIT * 1000)

            text = page.inner_text("body")
            browser.close()
            return text, None

    except Exception as e:
        return "", f"Playwright error: {str(e)}"


# ─────────────────────────────────────────────
#  DATA STATUS CHECK
# ─────────────────────────────────────────────

def get_scraper_status() -> dict:
    """Return current availability status of all data sources."""
    pw = playwright_available()
    mw = midway_valid()

    return {
        "playwright_installed": pw,
        "midway_valid": mw,
        "can_auto_scrape": pw and mw,
        "playwright_message": "✅ Installed" if pw else "❌ Not installed — run: pip install playwright && playwright install chromium",
        "midway_message": "✅ Valid" if mw else "❌ Expired — run mwinit",
        "recommendation": (
            "🟢 Full automation available" if (pw and mw)
            else "🟡 Manual data entry required" if not pw
            else "🔴 Run mwinit to enable automation"
        ),
    }
