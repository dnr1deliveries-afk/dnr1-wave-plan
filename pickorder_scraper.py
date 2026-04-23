"""
pickorder_scraper.py — Download PickOrder CSV from AMZL Dispatch Planning portal.

The PickOrder CSV download link is available on the Dispatch Planning page,
labelled 'Day of Ops' / 'Pick Order' at the bottom of the page.
We use Playwright to click the download link and capture the file.
"""

import os
import re
from datetime import date

DISPATCH_URL = (
    "https://eu.dispatch.planning.last-mile.a2z.com"
    "/dispatch-planning/DNR1/761553f5-9fc1-4cef-8815-b974bc63f0a9/{date}?laborType=DSP"
)

MIDWAY_COOKIE_PATH = os.path.expanduser("~/.midway/cookie")


def download_pickorder_csv(plan_date: str = None) -> tuple:
    """
    Download the PickOrder CSV via Playwright.
    Looks for a download link containing 'PickOrder' on the Dispatch Planning page.
    
    Returns: (csv_content_str, error_or_None)
    """
    plan_date = plan_date or date.today().strftime("%Y-%m-%d")

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None, "Playwright not installed"

    try:
        from playwright_scraper import _load_playwright_cookies
    except ImportError:
        return None, "playwright_scraper not found"

    url = DISPATCH_URL.format(date=plan_date)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                accept_downloads=True,
            )

            cookies = _load_playwright_cookies()
            if cookies:
                context.add_cookies(cookies)

            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=20000)

            # Wait for page to render
            try:
                page.wait_for_selector("text=STG-A", timeout=15000)
            except Exception:
                page.wait_for_timeout(8000)

            # Look for PickOrder download link
            # Try multiple selectors — the button text varies
            download_selectors = [
                "text=Pick Order",
                "text=PickOrder",
                "text=Day of Ops",
                "[data-testid*='pickorder']",
                "[data-testid*='pick-order']",
                "a[href*='PickOrder']",
                "button:has-text('CSV')",
            ]

            csv_content = None
            for selector in download_selectors:
                try:
                    element = page.query_selector(selector)
                    if element:
                        with page.expect_download(timeout=10000) as download_info:
                            element.click()
                        download = download_info.value
                        csv_content = download.path()
                        with open(csv_content, "r", encoding="utf-8-sig") as f:
                            content = f.read()
                        browser.close()
                        return content, None
                except Exception:
                    continue

            # Fallback: look for any CSV download link
            links = page.query_selector_all("a[href*='.csv'], a[href*='download']")
            for link in links:
                href = link.get_attribute("href") or ""
                text = link.inner_text()
                if "pick" in text.lower() or "order" in text.lower() or "PickOrder" in href:
                    try:
                        with page.expect_download(timeout=10000) as dl:
                            link.click()
                        download = dl.value
                        with open(download.path(), "r", encoding="utf-8-sig") as f:
                            content = f.read()
                        browser.close()
                        return content, None
                    except Exception:
                        continue

            browser.close()
            return None, (
                "Could not find PickOrder download button on the page. "
                "Please upload the PickOrder CSV manually: "
                "Dispatch Planning → bottom of page → 'Day of Ops' download"
            )

    except Exception as e:
        return None, f"Playwright error: {str(e)}"
