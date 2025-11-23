from datetime import datetime, timezone
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

LOG_PREFIX = "[ranking]"

def _log(msg: str) -> None:
    print(f"{LOG_PREFIX} {time.strftime('%Y-%m-%d %H:%M:%S')} - {msg}", flush=True)

def _write_json_atomic(path: Path, obj: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

def _age_in_months(ts: float, now: float) -> float:
    return max(0.0, (now - ts) / 2629800.0)  # 30.44 days

def _age_in_years(ts: float, now: float) -> float:
    return max(0.0, (now - ts) / 31557600.0)  # 365.25 days

def _extract_demand(entry: Dict[str, Any]) -> float:
    pi = entry.get("popular_info") or {}
    demand = pi.get("demand")
    if demand is None:
        demand = entry.get("demand_value")
    try:
        return float(demand) if demand is not None else 0.0
    except Exception:
        return 0.0

def _extract_shop_created_ts(entry: Dict[str, Any]) -> float:
    pi = entry.get("popular_info") or {}
    shop = pi.get("shop") or entry.get("shop") or {}
    details = shop.get("details") or {}
    ts = details.get("created_timestamp") or details.get("create_date")
    try:
        return float(ts) if ts is not None else 0.0
    except Exception:
        return 0.0

def _extract_listing_created_ts(entry: Dict[str, Any]) -> float:
    pi = entry.get("popular_info") or {}
    ts = (
        pi.get("original_creation_timestamp")
        or pi.get("creation_timestamp")
        or pi.get("created_timestamp")
    )
    try:
        return float(ts) if ts is not None else 0.0
    except Exception:
        return 0.0

def _overall_listing_review_percent(entry: Dict[str, Any]) -> float:
    reviews = entry.get("reviews") or []
    if not isinstance(reviews, list) or not reviews:
        return 0.0

    raw_id = entry.get("listing_id")
    if raw_id is None:
        return 0.0
    try:
        listing_id = int(raw_id)
    except Exception:
        return 0.0

    total = 0
    listing_total = 0
    for rev in reviews:
        if not isinstance(rev, dict):
            continue
        total += 1
        try:
            rid = int(rev.get("listing_id"))
        except Exception:
            rid = None
        if rid is not None and rid == listing_id:
            listing_total += 1

    if total <= 0:
        return 0.0

    pct = (listing_total / total) * 100.0
    return float(f"{pct:.6f}")

def _extract_everbee_averages(entry: Dict[str, Any]) -> Tuple[float, float]:
    eb = entry.get("everbee") or {}
    results = eb.get("results") or []
    vols: List[float] = []
    comps: List[float] = []
    for res in results:
        metrics = res.get("metrics") or {}
        vol = metrics.get("vol")
        comp = metrics.get("competition")
        if vol is None or comp is None:
            continue
        try:
            vol = float(vol)
            comp = float(comp)
        except Exception:
            continue
        # Ignore useless (0,0) pairs
        if vol == 0.0 and comp == 0.0:
            continue
        vols.append(vol)
        comps.append(comp)
    if not vols:
        return 0.0, 0.0
    avg_vol = sum(vols) / len(vols)
    avg_comp = sum(comps) / len(comps)
    return avg_vol, avg_comp

def _collect_maxima(entries: List[Dict[str, Any]]) -> Dict[str, float]:
    now = time.time()
    max_demand = 0.0
    max_vol = 0.0
    max_comp = 0.0
    max_ratio = 0.0
    for entry in entries:
        d = _extract_demand(entry)
        max_demand = max(max_demand, d)
        v, c = _extract_everbee_averages(entry)
        max_vol = max(max_vol, v)
        max_comp = max(max_comp, c)
        r = (v / (c + 1.0)) if (v > 0.0) else 0.0
        max_ratio = max(max_ratio, r)
    maxima = {
        "now": now,
        "max_demand": max_demand or 1.0,
        "max_vol": max_vol or 1.0,
        "max_comp": max_comp or 1.0,
        "max_ratio": max_ratio or 1e-9,
    }
    _log(
        f"Maxima computed: demand={maxima['max_demand']:.4f}, "
        f"vol={maxima['max_vol']:.4f}, comp={maxima['max_comp']:.4f}, "
        f"ratio={maxima['max_ratio']:.6f}"
    )
    return maxima

def _parse_timestamp(value: Any) -> Optional[float]:
    """
    Robustly parse a timestamp from various formats:
    - int/float epoch (seconds or milliseconds)
    - digit-only strings (seconds or milliseconds)
    - ISO 8601 strings like '2024-11-14T19:39:41Z' or '2024-11-14'
    Returns epoch seconds or None if parsing fails.
    """
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            ts = float(value)
            # Heuristic: >10^12 → milliseconds
            if ts > 10**12:
                ts /= 1000.0
            return ts
        if isinstance(value, str):
            s = value.strip()
            if not s:
                return None
            if s.isdigit():
                ts = float(s)
                if ts > 10**12:
                    ts /= 1000.0
                return ts
            # Try ISO formats
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except Exception:
                pass
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                    return dt.timestamp()
                except Exception:
                    continue
    except Exception:
        return None
    return None

def _recent_listing_review_percent(entry: Dict[str, Any], now: float) -> float:
    """
    Compute percentage (0..100) of last-year reviews that are for this listing.
    - Numerator: count of reviews in the last year where review['listing_id'] == entry['listing_id'].
    - Denominator: count of all reviews in the last year in entry['reviews'].
    If listing_id missing or no last-year reviews, returns 0.0.
    """
    try:
        reviews = entry.get("reviews") or []
        if not isinstance(reviews, list) or not reviews:
            return 0.0

        raw_id = entry.get("listing_id")
        if raw_id is None:
            return 0.0
        try:
            listing_id = int(raw_id)
        except Exception:
            return 0.0

        total = 0
        listing_total = 0
        for rev in reviews:
            if not isinstance(rev, dict):
                continue
            total += 1
            try:
                rid = int(rev.get("listing_id"))
            except Exception:
                rid = None
            if rid is not None and rid == listing_id:
                listing_total += 1

        if total <= 0:
            return 0.0

        pct = (listing_total / total) * 100.0
        return float(f"{pct:.6f}")
    except Exception:
        return 0.0

def _recency_score(listing_ts: float, now: float) -> float:
    """
    Heavily favor newer products using a penalty scheme:
    - If age < 30 days: penalty = 0.1 per day
    - If age <= 5 months: penalty = 1 per month
    - If age > 5 months: penalty = 5 + 2 per extra month
    Convert penalty points to [0..1] score: score = max(0, 1 - min(penalty, 100)/100)
    """
    try:
        if listing_ts is None or listing_ts <= 0:
            return 0.0
        age_sec = max(0.0, now - float(listing_ts))
        days = int(age_sec // 86400.0)
        months = int(age_sec // 2629800.0)  # 30.44 day months

        if months == 0:
            penalty = min(days, 30) * 0.1
        elif months <= 5:
            penalty = float(months) * 1.0
        else:
            penalty = 5.0 + 2.0 * float(months - 5)

        score = max(0.0, 1.0 - min(penalty, 100.0) / 100.0)
        return float(f"{score:.6f}")
    except Exception:
        return 0.0

def _shop_freshness_score(ts: float, now: float) -> float:
    # ≤1 year: 1.0; 1–5 years: gentle taper; >5 years: floor at 0.3
    age_years = _age_in_years(ts, now)
    if age_years <= 1.0:
        return 1.0
    penalty_years = age_years - 1.0
    return max(0.3, 1.0 - 0.05 * penalty_years)

def _normalize(x: float, max_x: float) -> float:
    if max_x <= 0.0:
        return 0.0
    return max(0.0, min(1.0, x / max_x))

def _score_entry(entry: Dict[str, Any], maxima: Dict[str, float]) -> float:
    """
    Composite score combining demand, ratio, recency, and reviews bonus.
    Reviews bonus: entries whose last-year listing-review percentage is above the average
    across all entries get extra points proportional to the difference.
    Also includes an all-reviews share bonus with tiered thresholds.
    """
    now = maxima["now"]
    demand = _extract_demand(entry)
    avg_vol, avg_comp = _extract_everbee_averages(entry)
    ratio = avg_vol / (avg_comp + 1.0) if avg_vol > 0.0 else 0.0
    listing_ts = _extract_listing_created_ts(entry)
    shop_ts = _extract_shop_created_ts(entry)

    demand_norm = _normalize(demand, maxima["max_demand"])
    vol_norm = _normalize(avg_vol, maxima["max_vol"])
    comp_norm = _normalize(avg_comp, maxima["max_comp"])
    ratio_norm = _normalize(ratio, maxima["max_ratio"])
    recency = _recency_score(listing_ts, now)
    shop_fresh = _shop_freshness_score(shop_ts, now)

    recency_demand = recency * demand_norm

    # Reviews bonus based on last-year listing review percentage
    avg_reviews_pct = maxima.get("avg_recent_review_percent", 0.0)
    this_reviews_pct = _recent_listing_review_percent(entry, now)
    reviews_diff = (this_reviews_pct - avg_reviews_pct) / 100.0
    age_months = _age_in_months(listing_ts, now)

    # Only allow negative review impact if age is between 1 and 2 months.
    apply_negative = (age_months >= 1.0 and age_months <= 2.0)
    if reviews_diff >= 0.0:
        reviews_adj = reviews_diff
    else:
        reviews_adj = reviews_diff if apply_negative else 0.0
    reviews_adj = float(f"{reviews_adj:.6f}")

    # New: bonus based on share of all reviews tied to this listing
    overall_reviews_pct = _overall_listing_review_percent(entry)
    if overall_reviews_pct >= 100.0:
        reviews_share_bonus = 1.0
    elif overall_reviews_pct >= 80.0:
        reviews_share_bonus = 0.8
    elif overall_reviews_pct >= 70.0:
        reviews_share_bonus = 0.6
    elif overall_reviews_pct >= 50.0:
        reviews_share_bonus = 0.4
    elif overall_reviews_pct >= 40.0:
        reviews_share_bonus = 0.2
    else:
        reviews_share_bonus = 0.0
    reviews_share_bonus = float(f"{reviews_share_bonus:.6f}")

    weights = {
        "demand": 0.32,
        "ratio": 0.20,
        "recency": 0.24,
        "recency_demand": 0.08,
        "shop": 0.03,
        "volume": 0.02,
        "competition": 0.01,
        "reviews": 0.10,         # recent-review diff vs average (age-gated negatives)
        "reviews_share": 0.12,   # new: all-reviews share tiered bonus
    }

    composite = (
        weights["demand"] * demand_norm
        + weights["ratio"] * ratio_norm
        + weights["recency"] * recency
        + weights["recency_demand"] * recency_demand
        + weights["shop"] * shop_fresh
        + weights["volume"] * vol_norm
        + weights["competition"] * (1.0 - comp_norm)
        + weights["reviews"] * reviews_adj
        + weights["reviews_share"] * reviews_share_bonus
    )
    return float(f"{composite:.6f}")

def _assign_unique_scores(composites: List[Tuple[int, float]]) -> Dict[int, float]:
    """
    composites: list of (index_in_entries, composite_score [0..1]).
    Returns mapping index -> unique score in (0..100], strictly decreasing by rank.
    """
    n = len(composites)
    if n == 0:
        return {}
    sorted_items = sorted(composites, key=lambda t: t[1], reverse=True)
    step = 100.0 / (n + 1)
    unique_scores: Dict[int, float] = {}
    for rank, (idx, _comp) in enumerate(sorted_items):
        score = 100.0 - step * rank
        unique_scores[idx] = float(f"{score:.5f}")
    return unique_scores

def _ranked_out_path(in_path: Path) -> Path:
    if in_path.suffix.lower() == ".json":
        return in_path.with_name(in_path.stem + "_ranked.json")
    return in_path.with_name(in_path.name + "_ranked")

def rank_megafile(in_path: str, out_path: Optional[str] = None) -> str:
    """
    Rank the megafile JSON and write a sibling file with `_ranked.json` suffix.
    Returns the output path.

    Raises:
      FileNotFoundError, ValueError on structural issues, or IOError on write errors.
    """
    start = time.time()
    p = Path(in_path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Input JSON not found: {in_path}")
    _log(f"Reading megafile JSON: {p}")

    with p.open("r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception as e:
            raise ValueError(f"Failed to parse JSON at {p}: {e}")

    entries = data.get("entries")
    if not isinstance(entries, list):
        raise ValueError("Input JSON must contain an 'entries' array.")

    _log(f"Loaded megafile with {len(entries)} entries.")
    if len(entries) == 0:
        out_p = Path(out_path) if out_path else _ranked_out_path(p)
        _write_json_atomic(out_p, {"entries": [], "meta": {"ranked_empty": True}})
        _log(f"No entries. Wrote empty ranked megafile: {out_p}")
        return str(out_p)

    maxima = _collect_maxima(entries)

    # Precompute average last-year listing-review percent across entries
    review_percents: List[float] = []
    for i, entry in enumerate(entries):
        rp = _recent_listing_review_percent(entry, maxima["now"])
        review_percents.append(rp)
        if (i + 1) % 50 == 0 or (i + 1) == len(entries):
            _log(f"Computed review % for {i + 1}/{len(entries)}; last={rp:.3f}%")
    avg_recent_pct = (sum(review_percents) / len(review_percents)) if review_percents else 0.0
    maxima["avg_recent_review_percent"] = avg_recent_pct
    _log(f"Average last-year listing-review percentage across entries: {avg_recent_pct:.3f}%")

    composites: List[Tuple[int, float]] = []
    bad_entry_count = 0
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            bad_entry_count += 1
            composites.append((i, 0.0))
            continue
        comp = _score_entry(entry, maxima)
        composites.append((i, comp))
        if (i + 1) % 50 == 0 or (i + 1) == len(entries):
            _log(f"Scored {i + 1}/{len(entries)} entries; last composite={comp:.5f}")

    if bad_entry_count:
        _log(f"Warning: {bad_entry_count} entries were non-dict and scored as 0.0")

    unique_scores = _assign_unique_scores(composites)

    missing_id = 0
    for i, entry in enumerate(entries):
        entry["Ranking"] = unique_scores.get(i, 0.0)
        if "listing_id" not in entry:
            missing_id += 1

    if missing_id:
        _log(f"Note: {missing_id} entries missing 'listing_id'")

    entries.sort(key=lambda e: e.get("Ranking", 0.0), reverse=True)
    _log("Sorted entries by Ranking (desc).")

    out_p = Path(out_path) if out_path else _ranked_out_path(p)
    try:
        _write_json_atomic(out_p, data)
    except Exception as e:
        raise IOError(f"Failed to write ranked JSON to {out_p}: {e}")

    elapsed = time.time() - start
    _log(f"Wrote ranked megafile: {out_p} (elapsed {elapsed:.2f}s)")
    return str(out_p)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Rank a megafile JSON and write a _ranked.json sibling.")
    parser.add_argument("in_path", help="Path to the megafile JSON")
    parser.add_argument("--out", help="Output ranked JSON path (optional)", default=None)
    args = parser.parse_args()

    try:
        out_path = rank_megafile(args.in_path, args.out)
        _log(f"Success. Ranked file written to: {out_path}")
    except Exception as e:
        _log(f"Error: {e}")
        raise