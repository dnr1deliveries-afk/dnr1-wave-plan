"""
push_to_render.py — Fetch data locally (with Midway) and push to Render.

Run this script on your Amazon laptop to:
1. Auto-fetch Dispatch + Assignment data using Playwright + Midway
2. Push the data to your Render deployment
3. The Render site will then have all the data ready

Usage:
    python push_to_render.py
    
Or double-click: PUSH_TO_RENDER.bat
"""

import requests
import json
from datetime import date
from playwright_scraper import (
    scrape_dispatch_plan, 
    scrape_assignment_plan, 
    get_scraper_status
)

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────────────────

RENDER_URL = "https://dnr1-wave-plan.onrender.com"
PUSH_ENDPOINT = f"{RENDER_URL}/api/push-data"

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  DNR1 Wave Plan — Push to Render")
    print("=" * 60)
    
    # Check Midway status
    status = get_scraper_status()
    print(f"\n📡 Playwright: {status['playwright_message']}")
    print(f"🔐 Midway: {status['midway_message']}")
    
    if not status["can_auto_scrape"]:
        print(f"\n❌ Cannot auto-fetch: {status['recommendation']}")
        print("   Run 'mwinit' first, then try again.")
        input("\nPress Enter to exit...")
        return
    
    plan_date = date.today().strftime("%Y-%m-%d")
    print(f"\n📅 Plan Date: {plan_date}")
    
    # Fetch Dispatch
    print("\n🚛 Fetching Dispatch Plan...")
    dispatch_text, dispatch_err = scrape_dispatch_plan(plan_date)
    if dispatch_err:
        print(f"   ❌ Error: {dispatch_err}")
        dispatch_text = None
    else:
        print(f"   ✅ Got {len(dispatch_text):,} characters")
    
    # Fetch Assignment
    print("\n👥 Fetching Assignment Planning...")
    assign_text, assign_err = scrape_assignment_plan(plan_date)
    if assign_err:
        print(f"   ❌ Error: {assign_err}")
        assign_text = None
    else:
        print(f"   ✅ Got {len(assign_text):,} characters")
    
    if not dispatch_text and not assign_text:
        print("\n❌ No data fetched. Cannot push to Render.")
        input("\nPress Enter to exit...")
        return
    
    # Push to Render
    print(f"\n📤 Pushing to {RENDER_URL}...")
    
    payload = {
        "plan_date": plan_date,
        "dispatch_text": dispatch_text,
        "assignment_text": assign_text,
    }
    
    try:
        resp = requests.post(PUSH_ENDPOINT, json=payload, timeout=30)
        if resp.status_code == 200:
            result = resp.json()
            print(f"   ✅ Success!")
            print(f"   📊 Dispatch: {result.get('dispatch', {}).get('records', 0)} waves")
            print(f"   📊 Assignment: {result.get('assignment', {}).get('records', 0)} routes")
            print(f"\n🌐 Open: {RENDER_URL}")
        else:
            print(f"   ❌ Server error: {resp.status_code}")
            print(f"   {resp.text[:500]}")
    except requests.exceptions.ConnectionError:
        print(f"   ❌ Cannot connect to {RENDER_URL}")
        print("   Make sure Render deployment is running.")
    except Exception as e:
        print(f"   ❌ Error: {e}")
    
    input("\nPress Enter to exit...")


if __name__ == "__main__":
    main()
