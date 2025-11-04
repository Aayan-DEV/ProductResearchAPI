#!/usr/bin/env python3
"""
Everbee/Etsy Search keyword metrics compiler (run-aware, demand summary first).

- Selects the newest run under `outputs/` that has both:
  • `demand extracted/run_summary_*.json`
  • `demand extracted/run_summary_*_keywords.json`
- Builds products from the demand summary entries (so listing_id, group, url, popular_info, demand are filled).
- Attaches keywords from the keywords JSON by title match (normalized, with fuzzy fallback).
- Executes Etsy Search (Marketplace Insights) request for each keyword and extracts searchVolume (vol) and avgTotalListings (competition).
- Writes outputs to `<run>/outputs/everbee/compiled_products_keywords_metrics.json` and per-keyword JSONs to `<run>/outputs/everbee/keyword_outputs/`.
"""

import json
import re
import sys
import html
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from difflib import SequenceMatcher

import requests

# ------------------------------
# Discovery helpers
# ------------------------------

def project_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except Exception:
        return Path.cwd()


def discover_runs_root(root: Path) -> Path:
    return root / "outputs"


def find_runs(outputs_root: Path) -> List[Path]:
    try:
        return sorted([p for p in outputs_root.iterdir() if p.is_dir()], key=lambda x: x.stat().st_mtime, reverse=True)
    except Exception:
        return []


def find_demand_summary(run_dir: Path) -> Optional[Path]:
    demand_dir = run_dir / "demand extracted"
    if not demand_dir.exists():
        return None
    # Only pick true run_summary files, exclude any *_keywords.json
    candidates = [
        p for p in demand_dir.glob("run_summary_*.json")
        if p.is_file() and not p.name.endswith("_keywords.json")
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def find_keywords_json_for_summary(summary_path: Path) -> Optional[Path]:
    demand_dir = summary_path.parent
    stem = summary_path.stem  # e.g., run_summary_YYYYMMDD_HHMMSS
    candidate = demand_dir / f"{stem}_keywords.json"
    if candidate.exists():
        return candidate
    # Fallback: any *_keywords.json next to summary
    alts = [p for p in demand_dir.glob("*_keywords.json") if p.is_file()]
    if not alts:
        return None
    alts.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return alts[0]


def ensure_run_everbee_dir(run_dir: Path) -> Path:
    out_dir = run_dir / "outputs" / "everbee"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def discover_etsy_search_req_file(root: Path) -> Optional[Path]:
    """Prefer the explicit Etsy Search curl file Etreq.txt; fallback to latest .txt in Etsy_Search."""
    explicit = root / "EtoRequests" / "Etsy_Search" / "Etreq.txt"
    if explicit.exists():
        return explicit
    base = root / "EtoRequests" / "Etsy_Search"
    if not base.exists():
        return None
    candidates: List[Path] = []
    for p in base.glob("*.txt"):
        if p.is_file():
            try:
                txt = p.read_text(encoding="utf-8", errors="ignore")
                if "curl" in txt:
                    candidates.append(p)
            except Exception:
                continue
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

# ------------------------------
# IO helpers
# ------------------------------

def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

# ------------------------------
# Curl parsing & request utils
# ------------------------------

def parse_curl_file(curl_file_path: Path) -> Tuple[str, Dict[str, str]]:
    """
    Parse a curl file with lines like:
      curl --request GET \
        --url 'https://.../results-data?query=...' \
        --header 'Header-Name: value' \
        --cookie 'name=value; other=value' \
    Returns (base_url, headers_dict). Cookies are merged into headers['Cookie'] if present.
    """
    text = curl_file_path.read_text(encoding="utf-8", errors="ignore")
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        raise ValueError("curl file is empty")

    base_url = None
    headers: Dict[str, str] = {}
    cookie_value: Optional[str] = None

    for line in lines:
        if base_url is None:
            m_url = re.search(r"--url\s+'([^']+)'", line)
            if m_url:
                base_url = m_url.group(1)
                continue
            m_alt = re.search(r"curl\s+'([^']+)'", line)
            if m_alt:
                base_url = m_alt.group(1)
                continue

        m_header = re.match(r"(?:-H|--header)\s+'([^:]+):\s*(.+)'\s*", line)
        if m_header:
            key = m_header.group(1).strip()
            val = m_header.group(2).strip()
            headers[key] = val
            continue

        m_cookie = re.match(r"--cookie\s+'(.+)'\s*", line)
        if m_cookie:
            cookie_value = m_cookie.group(1).strip()
            continue

    if not base_url:
        raise ValueError("Could not extract URL from curl file")

    if cookie_value:
        # Merge cookie into headers; prefer explicit Cookie header if already present.
        if "Cookie" in headers and headers["Cookie"]:
            headers["Cookie"] = f"{headers['Cookie']}; {cookie_value}"
        else:
            headers["Cookie"] = cookie_value

    return base_url, headers


def replace_query_in_url(base_url: str, keyword: str) -> str:
    """
    Replace the 'query' parameter in the Etsy Search URL with the given keyword.
    """
    parsed = urllib.parse.urlparse(base_url)
    q = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    q["query"] = [keyword]
    # Keep existing params including blank ones
    # parse_qs returns lists; urlencode with doseq=True preserves multiple values.
    flat_q = {}
    for k, v in q.items():
        # take first value if single
        flat_q[k] = v if isinstance(v, list) else [v]
    new_query = urllib.parse.urlencode(flat_q, doseq=True)
    new_url = urllib.parse.urlunparse((
        parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment
    ))
    return new_url


def extract_metrics_from_response(resp_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract Etsy Marketplace Insights metrics:
      - vol: stats.searchVolume
      - competition: stats.avgTotalListings
    """
    metrics = {"vol": None, "competition": None}
    stats = resp_json.get("stats", {})
    if isinstance(stats, dict):
        metrics["vol"] = stats.get("searchVolume")
        metrics["competition"] = stats.get("avgTotalListings")
    return metrics

# ------------------------------
# Matching helpers
# ------------------------------

def normalize_title(t: Optional[str]) -> str:
    if not t:
        return ""
    s = html.unescape(str(t)).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return s


def jaccard(a: str, b: str) -> float:
    A = set(re.findall(r"\w+", a))
    B = set(re.findall(r"\w+", b))
    if not A or not B:
        return 0.0
    inter = len(A & B)
    union = len(A | B)
    return inter / union if union else 0.0


def best_title_match(title: str, run_title_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    nt = normalize_title(title)
    if nt in run_title_map:
        return run_title_map[nt]
    best_entry = None
    best_score = 0.0
    for rt_norm, entry in run_title_map.items():
        s1 = SequenceMatcher(None, nt, rt_norm).ratio()
        s2 = jaccard(nt, rt_norm)
        score = 0.6 * s1 + 0.4 * s2
        if score > best_score:
            best_score = score
            best_entry = entry
    return best_entry if best_score >= 0.55 else None

# ------------------------------
# Loaders
# ------------------------------

def load_run_summary_entries(summary_path: Path) -> List[Dict[str, Any]]:
    data = read_json(summary_path)
    entries = data.get("entries", [])
    return entries if isinstance(entries, list) else []


def load_keyword_entries(keywords_path: Path) -> List[Dict[str, Any]]:
    data = read_json(keywords_path)
    results = data.get("results", [])
    out: List[Dict[str, Any]] = []
    for item in results:
        title = item.get("title")
        kws_raw = item.get("keywords", [])
        kws = [str(k) for k in kws_raw if isinstance(k, (str, int, float))]
        out.append({"title": title, "keywords": kws})
    return out

# ------------------------------
# Demand fallback helpers
# ------------------------------

def parse_demand_from_line(line: Optional[str]) -> Optional[int]:
    if not isinstance(line, str):
        return None
    m = re.search(r"(\d{1,3}(?:,\d{3})*)\s*bought in the past 24 hours", line, flags=re.IGNORECASE)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except Exception:
            return None
    m2 = re.search(r"(\d{1,3}(?:,\d{3})*)\s*sold in the past 24 hours", line, flags=re.IGNORECASE)
    if m2:
        try:
            return int(m2.group(1).replace(",", ""))
        except Exception:
            return None
    return None

def select_run_interactive(outputs_root: Path) -> Tuple[Path, Path, Path]:
    runs = find_runs(outputs_root)
    candidates: List[Tuple[Path, Path, Path]] = []
    for rd in runs:
        sp = find_demand_summary(rd)
        if not sp:
            continue
        kp = find_keywords_json_for_summary(sp)
        if kp:
            candidates.append((rd, sp, kp))

    if not candidates:
        print("Demand run_summary and *_keywords.json not found in outputs.")
        sys.exit(1)

    if len(candidates) == 1:
        return candidates[0]

    print("Available runs:")
    for i, (rd, _sp, _kp) in enumerate(candidates, start=1):
        ts = datetime.fromtimestamp(rd.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        print(f"  [{i}] folder: {rd} | time: {ts}")

    choice = None
    while choice is None:
        raw = input(f"Select a run [1-{len(candidates)}]: ").strip()
        try:
            n = int(raw)
            if 1 <= n <= len(candidates):
                choice = n
            else:
                print("Please enter a valid option number.")
        except Exception:
            print("Please enter a number.")

    return candidates[choice - 1]

# ------------------------------
# Main
# ------------------------------

def main() -> None:
    root = project_root()
    outputs_root = discover_runs_root(root)

    # Interactive run selection among runs that have summary + keywords JSON
    run_dir, summary_path, keywords_path = select_run_interactive(outputs_root)

    everbee_dir = ensure_run_everbee_dir(run_dir)

    # Resolve Etsy Search curl request file
    req_file = discover_etsy_search_req_file(root)
    if not req_file:
        print("Etsy Search curl request file not found in EtoRequests/Etsy_Search.")
        sys.exit(1)

    try:
        base_url, headers = parse_curl_file(req_file)
    except Exception as e:
        print(f"Error parsing curl file: {e}")
        sys.exit(1)

    # Load summary entries and keywords
    run_entries = load_run_summary_entries(summary_path)
    kw_entries = load_keyword_entries(keywords_path)

    if not run_entries:
        print(f"No entries found in summary: {summary_path}")
        sys.exit(1)

    # Build keywords map by normalized title
    keywords_map: Dict[str, List[str]] = {}
    for ke in kw_entries:
        nt = normalize_title(ke.get("title"))
        if nt and nt not in keywords_map:
            keywords_map[nt] = ke.get("keywords", [])

    max_products = len(run_entries)
    print(f"Using run: {run_dir}")
    print(f"Demand summary: {summary_path}")
    print(f"Keywords JSON: {keywords_path}")
    print(f"Available products (from summary): {max_products}")

    # Ask user how many products to process (from summary order)
    while True:
        try:
            raw = input(f"Enter number of products to process (1-{max_products}): ").strip()
            num_products = int(raw)
            if 1 <= num_products <= max_products:
                break
            print(f"Please enter a number between 1 and {max_products}.")
        except Exception:
            print("Invalid input, please enter an integer.")

    per_kw_dir = everbee_dir / "keyword_outputs"
    per_kw_dir.mkdir(parents=True, exist_ok=True)

    compiled: Dict[str, Any] = {
        "meta": {
            "run_dir": str(run_dir),
            "summary_path": str(summary_path),
            "keywords_path": str(keywords_path),
            "req_file": str(req_file),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "requested_products_count": num_products,
        },
        "products": []
    }

    for idx in range(num_products):
        entry = run_entries[idx]
        popular_info = entry.get("popular_info", {})
        final_title = popular_info.get("title")
        listing_id = entry.get("listing_id", popular_info.get("listing_id"))
        url = popular_info.get("url") or popular_info.get("listing_url")
        group = entry.get("group", entry.get("group_type"))
        demand = entry.get("demand")
        if demand is None:
            demand = parse_demand_from_line(entry.get("cart_scarcity_line"))

        # Attach keywords by title match (normalized, fuzzy fallback)
        product_keywords: List[str] = []
        nt = normalize_title(final_title)
        if nt in keywords_map:
            product_keywords = keywords_map[nt]
        else:
            # Fuzzy best match across keywords_map keys
            best_key = None
            best_score = 0.0
            for k in keywords_map.keys():
                s1 = SequenceMatcher(None, nt, k).ratio()
                s2 = jaccard(nt, k)
                score = 0.6 * s1 + 0.4 * s2
                if score > best_score:
                    best_score = score
                    best_key = k
            if best_key and best_score >= 0.55:
                product_keywords = keywords_map.get(best_key, [])

        print(f"\n[{idx+1}/{num_products}] listing_id={listing_id} | group={group} | title={final_title}")

        product_result = {
            "listing_id": listing_id,
            "group": group,
            "title": final_title,
            "url": url,
            "popular_info": popular_info,
            "demand": demand,
            "keywords": []
        }

        for kw in product_keywords:
            req_url = replace_query_in_url(base_url, kw)
            try:
                resp = requests.get(req_url, headers=headers, timeout=30)
                status = resp.status_code
                try:
                    resp_json = resp.json()
                except Exception:
                    resp_json = {"_non_json_response": resp.text}

                # Save per-keyword enriched payload
                safe_kw = re.sub(r"[^a-zA-Z0-9_\-]+", "_", kw).strip("_")
                per_kw_path = per_kw_dir / f"keyword_{safe_kw}.json"
                enriched = {
                    "source_title": final_title,
                    "listing_id": listing_id,
                    "group": group,
                    "url": url,
                    "keyword": kw,
                    "request_url": req_url,
                    "status_code": status,
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "response": resp_json,
                }
                write_json(per_kw_path, enriched)

                metrics = extract_metrics_from_response(resp_json)
                product_result["keywords"].append({
                    "keyword": kw,
                    "request_url": req_url,
                    "status_code": status,
                    "metrics": metrics,
                })
                print(f"  kw='{kw}' -> {status} | metrics={metrics}")
            except Exception as e:
                safe_kw = re.sub(r"[^a-zA-Z0-9_\-]+", "_", kw).strip("_")
                per_kw_path = per_kw_dir / f"keyword_{safe_kw}.json"
                error_payload = {
                    "source_title": final_title,
                    "listing_id": listing_id,
                    "group": group,
                    "url": url,
                    "keyword": kw,
                    "request_url": req_url,
                    "status_code": None,
                    "timestamp": datetime.now().isoformat(timespec="seconds"),
                    "error": str(e),
                    "response": None,
                }
                write_json(per_kw_path, error_payload)
                product_result["keywords"].append({
                    "keyword": kw,
                    "request_url": req_url,
                    "status_code": None,
                    "metrics": {"vol": None, "competition": None},
                    "error": str(e),
                })
                print(f"  kw='{kw}' -> request failed: {e}")

        compiled["products"].append(product_result)

    out_path = everbee_dir / "compiled_products_keywords_metrics.json"
    write_json(out_path, compiled)
    print(f"\nCompiled output written to: {out_path}")
    print(f"Per-keyword outputs: {per_kw_dir}")
    print("Done.")

# Cached Etsy Search config for quick API calls
_ETSY_SEARCH_CFG = None

def ensure_etsy_search_config(root: Optional[Path] = None) -> Tuple[str, Dict[str, str]]:
    """
    Resolve base_url and headers for Etsy Search (Marketplace Insights).
    """
    global _ETSY_SEARCH_CFG
    if _ETSY_SEARCH_CFG is not None:
        return _ETSY_SEARCH_CFG
    base = project_root() if root is None else root
    req_file = discover_etsy_search_req_file(base)
    if not req_file:
        raise RuntimeError("Etsy Search curl request file not found in EtoRequests/Etsy_Search.")
    base_url, headers = parse_curl_file(req_file)
    _ETSY_SEARCH_CFG = (base_url, headers)
    return _ETSY_SEARCH_CFG

def fetch_metrics_for_keyword(keyword: str, base_url: Optional[str] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """
    Execute Etsy Search request for a single keyword and return metrics + response info.
    """
    if not base_url or not headers:
        base_url, headers = ensure_etsy_search_config()
    url = replace_query_in_url(base_url, keyword)

    # Clone headers and update Referer to match the keyword (mirrors manual curl)
    headers_to_use = dict(headers or {})
    try:
        ref_key = next((k for k in headers_to_use.keys() if k.lower() == "referer"), None)
        if ref_key:
            headers_to_use[ref_key] = replace_query_in_url(headers_to_use[ref_key], keyword)
    except Exception:
        pass

    try:
        resp = requests.get(url, headers=headers_to_use, timeout=30)
        try:
            resp_json = resp.json()
        except Exception:
            resp_json = {"_non_json_response": resp.text}
        metrics = extract_metrics_from_response(resp_json)
        return {
            "keyword": keyword,
            "request_url": url,
            "status_code": resp.status_code,
            "metrics": metrics,
            "response": resp_json,
        }
    except Exception as e:
        return {
            "keyword": keyword,
            "request_url": url,
            "status_code": None,
            "metrics": {"vol": None, "competition": None},
            "error": str(e),
        }

def fetch_metrics_for_keywords(keywords: List[str], base_url: Optional[str] = None, headers: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """
    Batch convenience wrapper; sequential for simplicity and rate-limit friendliness.
    """
    results = []
    for kw in keywords:
        results.append(fetch_metrics_for_keyword(kw, base_url, headers))
    return results

if __name__ == "__main__":
    main()