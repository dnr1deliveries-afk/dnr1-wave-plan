"""
midway_auth.py — Midway cookie auth for AMZL internal APIs.
Parses Netscape-format cookie file (~/.midway/cookie).
"""

import os
import time
import requests

MIDWAY_COOKIE_PATH = os.path.expanduser("~/.midway/cookie")
MIDWAY_BYPASS = os.environ.get("MIDWAY_BYPASS", "false").lower() == "true"
MIDWAY_DEV_USER = os.environ.get("MIDWAY_DEV_USER", "keenlys")


def get_midway_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DNR1-WavePlan/1.0",
        "Accept": "text/html,application/xhtml+xml,application/json,*/*",
    })

    if MIDWAY_BYPASS:
        session.headers["x-amzn-midway-user"] = MIDWAY_DEV_USER
        return session

    for name, value, domain in _parse_netscape_cookies():
        session.cookies.set(name, value, domain=domain)

    return session


def _parse_netscape_cookies():
    """
    Parse Netscape cookie file format:
    domain  flag  path  secure  expiry  name  value
    Lines starting with # or empty are skipped.
    """
    cookies = []
    try:
        with open(MIDWAY_COOKIE_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) >= 7:
                    domain = parts[0].lstrip(".")
                    name = parts[5]
                    value = parts[6]
                    cookies.append((name, value, domain))
    except FileNotFoundError:
        print(f"[Midway] Cookie file not found at {MIDWAY_COOKIE_PATH} — run mwinit")
    except Exception as e:
        print(f"[Midway] Error reading cookie file: {e}")
    return cookies


def check_midway_status() -> dict:
    try:
        stat = os.stat(MIDWAY_COOKIE_PATH)
        age_hours = (time.time() - stat.st_mtime) / 3600
        cookies = _parse_netscape_cookies()
        return {
            "found": True,
            "age_hours": round(age_hours, 1),
            "likely_valid": age_hours < 20,
            "cookie_count": len(cookies),
            "message": (
                f"Cookie is {round(age_hours, 1)}h old, {len(cookies)} entries"
                + (" ✓" if age_hours < 20 else " — may be expired, run mwinit")
            ),
        }
    except FileNotFoundError:
        return {
            "found": False,
            "age_hours": None,
            "likely_valid": False,
            "cookie_count": 0,
            "message": "No Midway cookie found — run mwinit",
        }
