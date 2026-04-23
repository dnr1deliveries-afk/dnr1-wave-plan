"""
lane_optimizer.py — Cart-aware lane placement optimizer for DNR1.

RULES:
  1. Lane capacity: max 6 carts per lane
  2. Optimize in PAIRS of waves (Wave 1A+1B together, 2A+2B, etc.)
  3. No two routes from the SAME pad can share a lane
     → A route from Pad A + a route from Pad B CAN share a lane (same slot)
  4. Both Pad A and Pad B independently spread across all 30 lanes
     (first=1, last=30 for each pad)
  5. Pad C: never touched — consecutive lanes only

ALGORITHM:
  Step 1 — Greedy cart-aware pairing:
    Sort A DESC by carts, sort B DESC by carts.
    For each A route, find the largest B route where A+B <= 6.
    Unmatched routes go in solo slots.

  Step 2 — Independent spreading:
    Pad A routes (paired + solo A): spread n_a items across 1..30
    Pad B routes (paired + solo B): spread n_b items across 1..30
    Paired routes get adjacent positions in each spread sequence.

  Step 3 — Write lane labels back to routes:
    STG-A{lane_num_a} and STG-B{lane_num_b}
    shares_with = partner route code or None
"""

LANE_CAPACITY = 6
LANES_PER_PAD = 30


# ─────────────────────────────────────────────────────────────────
#  PUBLIC ENTRY POINTS
# ─────────────────────────────────────────────────────────────────

def optimize_wave_pair(pad_a_routes, pad_b_routes):
    """
    Takes enriched routes for Pad A and Pad B of a wave.
    Returns (optimized_pad_a, optimized_pad_b) with lane assignments.

    Each route gets:
      lane_num         : int 1-30
      lane             : str "STG-A{n}" or "STG-B{n}"
      shares_with      : route code of paired route or None
      combined_carts   : carts in the lane slot (self + partner if paired)
      lane_utilization : % of 6-cart capacity used
    """
    slots = _pair_routes(pad_a_routes, pad_b_routes)
    slots = _spread_independently(slots, LANES_PER_PAD)
    pad_a_out = list(pad_a_routes)
    pad_b_out = list(pad_b_routes)
    _apply_lanes(pad_a_out, pad_b_out, slots)
    return pad_a_out, pad_b_out


def optimize_single_pad(pad_routes, pad_label):
    """Single pad (no partner pad) — spread solo routes across 30 lanes."""
    if not pad_routes:
        return pad_routes
    spread = _spread_positions(len(pad_routes), LANES_PER_PAD)
    out = list(pad_routes)
    for i, route in enumerate(out):
        ln = spread[i]
        route["lane_num"] = ln
        route["lane"] = f"STG-{pad_label}{ln}"
        route["shares_with"] = None
        route["combined_carts"] = route.get("total_carts", 0)
        route["lane_utilization"] = round(route.get("total_carts", 0) / LANE_CAPACITY * 100)
    return out


# ─────────────────────────────────────────────────────────────────
#  STEP 1 — PAIRING
# ─────────────────────────────────────────────────────────────────

def _pair_routes(pad_a_routes, pad_b_routes):
    """
    Greedy largest-fits-first pairing of A and B routes.

    Returns list of slot dicts:
      { "a_route": ..., "b_route": ..., "total_carts": int, "pad": "AB"|"A"|"B" }
    """
    a_sorted = sorted(pad_a_routes, key=lambda r: r.get("total_carts", 0), reverse=True)
    b_pool   = sorted(pad_b_routes, key=lambda r: r.get("total_carts", 0), reverse=True)

    slots  = []
    b_used = set()

    for a_route in a_sorted:
        a_carts  = a_route.get("total_carts", 0)
        best_b   = None
        best_idx = -1

        # Find largest B that fits (b_pool sorted DESC → first fit = largest fit)
        for j, b_route in enumerate(b_pool):
            if j in b_used:
                continue
            if a_carts + b_route.get("total_carts", 0) <= LANE_CAPACITY:
                best_b   = b_route
                best_idx = j
                break

        if best_b is not None:
            b_used.add(best_idx)
            slots.append({
                "a_route":     a_route,
                "b_route":     best_b,
                "total_carts": a_carts + best_b.get("total_carts", 0),
                "pad":         "AB",
            })
        else:
            slots.append({
                "a_route":     a_route,
                "b_route":     None,
                "total_carts": a_carts,
                "pad":         "A",
            })

    # Remaining unmatched B routes
    for j, b_route in enumerate(b_pool):
        if j not in b_used:
            slots.append({
                "a_route":     None,
                "b_route":     b_route,
                "total_carts": b_route.get("total_carts", 0),
                "pad":         "B",
            })

    return slots


# ─────────────────────────────────────────────────────────────────
#  STEP 2 — INDEPENDENT SPREADING
# ─────────────────────────────────────────────────────────────────

def _spread_independently(slots, total_lanes):
    """
    Assign lane numbers independently to Pad A and Pad B.

    Pad A routes (AB paired + A solo) → spread n_a items across 1..30
    Pad B routes (AB paired + B solo) → spread n_b items across 1..30

    Paired slots consume one position from EACH pad's sequence.
    """
    ab_slots = [s for s in slots if s["pad"] == "AB"]
    a_only   = [s for s in slots if s["pad"] == "A"]
    b_only   = [s for s in slots if s["pad"] == "B"]

    n_a = len(ab_slots) + len(a_only)
    n_b = len(ab_slots) + len(b_only)

    a_pos = iter(_spread_positions(n_a, total_lanes)) if n_a else iter([])
    b_pos = iter(_spread_positions(n_b, total_lanes)) if n_b else iter([])

    # Paired slots: consume one from each sequence
    for slot in ab_slots:
        slot["lane_num_a"] = next(a_pos)
        slot["lane_num_b"] = next(b_pos)

    for slot in a_only:
        slot["lane_num_a"] = next(a_pos)
        slot["lane_num_b"] = None

    for slot in b_only:
        slot["lane_num_a"] = None
        slot["lane_num_b"] = next(b_pos)

    return slots


def _spread_positions(n, total_lanes):
    """
    Evenly spread n positions across total_lanes.
    First = lane 1, Last = lane total_lanes.

    n=1  → [1]
    n=2  → [1, 30]
    n=7  → [1, 6, 11, 15, 20, 25, 30]
    n=16 → [1, 3, 5, 7, 9, 11, 13, 15, 16, 18, 20, 22, 24, 26, 28, 30]
    """
    if n <= 0:
        return []
    if n == 1:
        return [1]
    if n >= total_lanes:
        return list(range(1, n + 1))
    return [round(i * (total_lanes - 1) / (n - 1)) + 1 for i in range(n)]


# ─────────────────────────────────────────────────────────────────
#  STEP 3 — APPLY BACK TO ROUTES
# ─────────────────────────────────────────────────────────────────

def _apply_lanes(pad_a_routes, pad_b_routes, slots):
    """Write lane assignments from slots back to route dicts."""
    a_lookup = {r["route"]: r for r in pad_a_routes if r.get("route")}
    b_lookup = {r["route"]: r for r in pad_b_routes if r.get("route")}

    for slot in slots:
        ar = slot.get("a_route")
        br = slot.get("b_route")
        ln_a = slot.get("lane_num_a")
        ln_b = slot.get("lane_num_b")
        total = slot["total_carts"]

        if ar and ln_a is not None:
            r = a_lookup.get(ar["route"])
            if r:
                r["lane_num"]       = ln_a
                r["lane"]           = f"STG-A{ln_a}"
                r["shares_with"]    = br["route"] if br else None
                r["combined_carts"] = total
                r["lane_utilization"] = round(total / LANE_CAPACITY * 100)

        if br and ln_b is not None:
            r = b_lookup.get(br["route"])
            if r:
                r["lane_num"]       = ln_b
                r["lane"]           = f"STG-B{ln_b}"
                r["shares_with"]    = ar["route"] if ar else None
                r["combined_carts"] = total
                r["lane_utilization"] = round(total / LANE_CAPACITY * 100)


# ─────────────────────────────────────────────────────────────────
#  WARNINGS + SUMMARY
# ─────────────────────────────────────────────────────────────────

def get_capacity_warnings(pad_a_routes, pad_b_routes):
    """Routes that individually exceed lane capacity (>6 carts)."""
    warnings = []
    for route in pad_a_routes + pad_b_routes:
        carts = route.get("total_carts", 0)
        if carts > LANE_CAPACITY:
            warnings.append({
                "route":   route.get("route"),
                "dsp":     route.get("dsp"),
                "carts":   carts,
                "over_by": carts - LANE_CAPACITY,
                "message": (
                    f"{route.get('route')} ({route.get('dsp')}) has {carts} carts "
                    f"— exceeds lane capacity of {LANE_CAPACITY} by {carts - LANE_CAPACITY}"
                ),
            })
    return warnings


def get_optimization_summary(waves):
    """Pairing efficiency stats for the AM dashboard header."""
    total_paired  = 0
    total_solo    = 0
    over_capacity = 0

    for wave in waves:
        for pad_routes in [wave["pad_a"]["routes"], wave["pad_b"]["routes"]]:
            for r in pad_routes:
                if r.get("shares_with"):
                    total_paired += 1
                else:
                    total_solo += 1
                if r.get("total_carts", 0) > LANE_CAPACITY:
                    over_capacity += 1

    total = total_paired + total_solo
    return {
        "total_paired":        total_paired // 2,
        "total_solo":          total_solo,
        "over_capacity":       over_capacity,
        "pairing_efficiency":  round(total_paired / total * 100) if total else 0,
    }
