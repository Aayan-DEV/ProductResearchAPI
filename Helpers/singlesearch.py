#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None
from first_etsy_api_use import (
    fetch_listing_details_by_ids,
    fetch_shop_info,
    generate_listing_card_html,             # NEW: used for fallback HTML
    fetch_listing_primary_image,            # NEW: Open API primary image fetch
    write_single_listing_helpers_via_curl,  # NEW: run curl_original with single listing_id
)
# Make Helpers importable if the script is run outside the folder
CURRENT_FILE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_FILE_DIR.parent
USERS_DIR = os.getenv("AI_USERS_DIR") or str(PROJECT_ROOT / "users")
if str(CURRENT_FILE_DIR) not in sys.path:
    sys.path.append(str(CURRENT_FILE_DIR))

# Imports from existing helpers
from first_etsy_api_use import fetch_listing_details_by_ids, fetch_shop_info  # type: ignore
from second_demand_extractor import process_listing_demand, discover_popular_json  # type: ignore
from third_ai_keywords import generate_keywords_for_title_api  # type: ignore
from fourth_everbee_keyword import fetch_metrics_for_keywords  # type: ignore

# Simple lock registry (per-path) to protect concurrent megafile updates
_FILE_LOCKS: Dict[str, Any] = {}
_FILE_LOCKS_REGISTRY_LOCK = None

def _get_lock_for(path: Path):
    global _FILE_LOCKS_REGISTRY_LOCK
    if _FILE_LOCKS_REGISTRY_LOCK is None:
        import threading
        _FILE_LOCKS_REGISTRY_LOCK = threading.Lock()

    key = str(Path(path).resolve())
    with _FILE_LOCKS_REGISTRY_LOCK:
        lock = _FILE_LOCKS.get(key)
        if lock is None:
            import threading
            lock = threading.Lock()
            _FILE_LOCKS[key] = lock
        return lock

def run_single_search(listing_id: int, session_id: Optional[str] = None, forced_personalize: Optional[bool] = None) -> Dict[str, Any]:
    """
    Normal single search:
    - Compiles the listing.
    - Writes to outputs/single_search_{listing_id}_{ts}/outputs/popular_listings_full_pdf.json.
    - Returns the compiled payload (with meta.compiled_path).
    Note: user_id is intentionally not needed/used here per request.
    """
    compiled = compile_single_listing(int(listing_id), forced_personalize)
    if isinstance(session_id, str) and session_id.strip():
        compiled.setdefault("meta", {})["session_id"] = slugify_safe(session_id)
    return compiled

def run_replace_listing(listing_id: int, user_id: str, session_id: str, forced_personalize: Optional[bool] = None) -> Dict[str, Any]:
    """
    Replace listing:
    - Compiles the listing.
    - Locates session megafile and replaces/creates the entry, preserving previous source_paths.
    - Returns the final megafile JSON.
    """
    lid = int(listing_id)
    user_id_slug = slugify_safe(user_id)
    sess_id_slug = slugify_safe(session_id)

    compiled = compile_single_listing(lid, forced_personalize, skip_ai_keywords=True)
    compiled.setdefault("meta", {})["session_id"] = sess_id_slug
    compiled.setdefault("meta", {})["user_id"] = user_id_slug

    # Keep example.json for traceability
    example_path = CURRENT_FILE_DIR / "example.json"
    write_json_atomic(example_path, compiled)

    # Locate megafile via session meta first; fallback to outputs/<session>/outputs/
    megafile_path = _find_megafile_from_user_session(user_id_slug, sess_id_slug) or _find_megafile_in_session(sess_id_slug)
    if not megafile_path or not megafile_path.exists():
        raise FileNotFoundError(
            f"Could not find megafile via user/session.\n"
            f"Checked: {USERS_DIR}/{user_id_slug}/sessions/{sess_id_slug}.json and outputs/{sess_id_slug}/outputs/."
        )

    try:
        mega = json.loads(megafile_path.read_text(encoding="utf-8"))
    except Exception:
        mega = {"entries": []}

    entries = mega.get("entries") or []
    new_entry = _to_megafile_entry(compiled)

    # Concurrency-safe replace
    lock = _get_lock_for(megafile_path)
    lock.acquire()
    try:
        idx = next((i for i, e in enumerate(entries) if int(e.get("listing_id") or 0) == lid), None)
        if idx is None:
            # No prior entry: add fresh entry as-is
            entries.append(new_entry)
        else:
            prev_entry = entries[idx]
            # Merge values while preserving primary_image, source_paths, and listing_id
            merged = _merge_entry_preserving_primary_image(prev_entry, new_entry)
            entries[idx] = merged

        mega["entries"] = entries
        write_json_atomic(megafile_path, mega)
    finally:
        lock.release()

    # Return raw megafile JSON (no wrappers)
    return mega

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_json_atomic(path: Path, obj: Any) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(str(tmp), str(path))

def _safe_json(obj: Any) -> Any:
    try:
        json.dumps(obj)
        return obj
    except Exception:
        return str(obj)

def slugify_safe(text: str) -> str:
    try:
        import re
        text = str(text or "")
        out = "".join(ch.lower() if ch.isalnum() else "-" for ch in text)
        out = re.sub(r"-{2,}", "-", out).strip("-")
        return out or "session"
    except Exception:
        return "session"

def _normalize_listing_object(listing_raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize shape returned by Etsy Open API v3 for listings/{id}.
    Ensures: listing_id, shop_id, title, url.
    """
    out: Dict[str, Any] = {}
    if not isinstance(listing_raw, dict):
        return out

    # Some endpoints return {"results": [obj]}
    if "results" in listing_raw and isinstance(listing_raw["results"], list) and listing_raw["results"]:
        cand = listing_raw["results"][0]
        if isinstance(cand, dict):
            listing_raw = cand

    lid = listing_raw.get("listing_id") or listing_raw.get("listingId") or listing_raw.get("id")
    try:
        out["listing_id"] = int(str(lid)) if lid is not None else None
    except Exception:
        out["listing_id"] = None

    out["shop_id"] = listing_raw.get("shop_id") or listing_raw.get("shopId")
    out["title"] = listing_raw.get("title")
    out["url"] = listing_raw.get("url") or listing_raw.get("listing_url") or None

    # Pass through additional fields as reference
    out["raw"] = listing_raw
    return out


def _prompt_listing_id() -> Optional[int]:
    try:
        raw = input("Enter Etsy listing ID: ").strip()
        if raw.isdigit():
            return int(raw)
        digits = "".join(ch for ch in raw if ch.isdigit())
        return int(digits) if digits else None
    except Exception:
        return None


def _prompt_user_id() -> Optional[str]:
    try:
        user = input("Enter user id: ").strip()
        return user if user else None
    except Exception:
        return None

def _prompt_session_id() -> Optional[str]:
    try:
        sess = input("Enter session id (users/<user>/sessions/<id>.json): ").strip()
        return sess if sess else None
    except Exception:
        return None


def _prompt_force_personalization() -> Optional[bool]:
    try:
        ans = (input("Force personalise or non personalise? [y/N]: ").strip().lower() or "n")
        if ans not in ("y", "yes"):
            return None
        print("\npersonalise - 1\nnon personalise - 2")
        choice = input("choose : ").strip()
        if choice == "1":
            return True
        if choice == "2":
            return False
        return None
    except Exception:
        return None

def _resolve_user_session_meta_path(user_id: str, session_id: str) -> Optional[Path]:
    try:
        user_slug = slugify_safe(user_id)
        sess_slug = slugify_safe(session_id)
        p = Path(USERS_DIR) / user_slug / "sessions" / f"{sess_slug}.json"
        return p if p.exists() else None
    except Exception:
        return None

def _read_run_root_dir_from_session_meta(meta_path: Path) -> Optional[Path]:
    try:
        doc = json.loads(meta_path.read_text(encoding="utf-8"))
        rr = doc.get("run_root_dir") or (doc.get("meta") or {}).get("run_root_dir")
        if isinstance(rr, str) and rr.strip():
            p = Path(rr)
            return p if p.exists() else None
        return None
    except Exception:
        return None

def _find_megafile_from_user_session(user_id: str, session_id: str) -> Optional[Path]:
    meta = _resolve_user_session_meta_path(user_id, session_id)
    if not meta:
        return None
    run_root = _read_run_root_dir_from_session_meta(meta)
    if not run_root:
        return None
    outputs_dir = run_root / "outputs"
    if not outputs_dir.exists():
        return None
    cands = sorted(outputs_dir.glob("megafile_listings_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return cands[0] if cands else None

def _find_megafile_in_session(session_id: str) -> Optional[Path]:
    """
    Locate megafile in outputs/<session_id>/outputs/:
    returns latest megafile_listings_*.json if multiple exist.
    """
    base = PROJECT_ROOT / "outputs" / session_id / "outputs"
    if not base.exists():
        return None
    candidates = sorted(base.glob("megafile_listings_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def _to_megafile_entry(compiled: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform compiled single listing payload into a megafile entry shape.
    """
    listing_id = compiled.get("listing_id")
    title = compiled.get("title")
    entry = {
        "listing_id": compiled.get("listing_id"),
        "title": compiled.get("title"),
        "popular_info": compiled.get("popular_info"),
        "signals": compiled.get("signals"),
        "demand": compiled.get("demand"),
        "demand_extras": compiled.get("demand_extras"),
        "keywords": compiled.get("keywords") or [],
        "everbee": {"results": (compiled.get("everbee") or {}).get("results") or []},
        "primary_image": compiled.get("primary_image"),
        "variations_cleaned": compiled.get("variations_cleaned"),
        "sale_info": compiled.get("sale_info"),
        "shop": compiled.get("shop"),
        "source_paths": {
            "summary_source": None,
            "ai_keywords_source": None,
            "everbee_source": None,
            "combined": None,
            "primary_image": None,
            "variations_cleaned": None,
            "listing_sale_info": None,
            "extras_from_listing": None,
        },
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    return entry

# Merge helper: updates values while preserving structure and primary image
def _merge_entry_preserving_primary_image(prev_entry: Dict[str, Any], new_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge new_entry values into prev_entry while preserving structure and
    keeping primary_image, source_paths, and listing_id exactly as-is.
    Only updates keys that already exist in prev_entry.
    """
    merged = dict(prev_entry)
    # Preserve additional fields exactly: sale_info and keywords stay untouched
    skip_keys = {"primary_image", "source_paths", "listing_id", "sale_info", "keywords"}

    for key in list(merged.keys()):
        if key in skip_keys:
            continue

        if key == "everbee":
            prev_ev = merged.get("everbee")
            new_ev = new_entry.get("everbee")
            if isinstance(prev_ev, dict):
                if isinstance(new_ev, dict) and "results" in new_ev:
                    new_results = new_ev.get("results") or []
                    # Only update if non-empty to avoid wiping previous results
                    if isinstance(new_results, list) and len(new_results) > 0:
                        prev_ev["results"] = new_results
                merged["everbee"] = prev_ev
            elif new_ev is not None:
                merged["everbee"] = new_ev
            continue

        if key in new_entry:
            val = new_entry.get(key)
            if val is not None:
                merged[key] = val

    # Canonicalize demand: always use 'demand'
    new_demand = new_entry.get("demand", new_entry.get("demand_value"))
    if "demand" in merged:
        if new_demand is not None:
            merged["demand"] = new_demand
    elif "demand_value" in merged:
        merged["demand"] = new_demand if new_demand is not None else merged.get("demand_value")
        try:
            del merged["demand_value"]
        except Exception:
            pass

    if "timestamp" in new_entry:
        merged["timestamp"] = new_entry["timestamp"]

    return merged

def compile_single_listing(listing_id: int, forced_personalize: Optional[bool] = None, skip_ai_keywords: bool = False) -> Dict[str, Any]:
    """
    Orchestrates the single-listing pipeline and returns the compiled payload,
    also saving artifacts under outputs/single_search_{listing_id}_{ts}/.
    """
    if load_dotenv:
        try:
            load_dotenv()
        except Exception:
            pass

    ts = time.strftime("%Y%m%d_%H%M%S")
    run_dir = PROJECT_ROOT / "outputs" / f"single_search_{listing_id}_{ts}"
    outputs_dir = run_dir / "outputs"
    ensure_dir(outputs_dir)

    compiled: Dict[str, Any] = {
        "listing_id": int(listing_id),
        "title": None,
        "url": None,
        "popular_info": None,
        "shop": None,
        "signals": None,
        "demand": None,
        "demand_extras": None,
        "primary_image": None,
        "variations_cleaned": None,
        "sale_info": None,
        "keywords": [],
        "everbee": {"results": []},
        "meta": {
            "run_dir": str(run_dir),
            "outputs_dir": str(outputs_dir),
            "timestamp": ts,
        }
    }

    # 1) Fetch listing details (Open API v3)
    listing_payloads: List[Dict[str, Any]] = []
    try:
        listing_payloads = fetch_listing_details_by_ids([int(listing_id)])
    except Exception as e:
        compiled.setdefault("errors", {})["open_api"] = str(e)
    listing_obj = listing_payloads[0] if listing_payloads else {}
    normalized = _normalize_listing_object(listing_obj)
    compiled["popular_info"] = normalized.get("raw")
    compiled["title"] = normalized.get("title")
    compiled["url"] = normalized.get("url")

    # NEW: Pre-demand helpers via real listingCards cURL; fallback to synthetic if it fails
    primary_img: Optional[Dict[str, Any]] = None
    try:
        primary_img = fetch_listing_primary_image(int(listing_id)) or None
    except Exception as e:
        compiled.setdefault("errors", {})["primary_image_fetch"] = str(e)

    try:
        # Try to generate helpers by running curl_original with single listing_id
        helpers_html_path = write_single_listing_helpers_via_curl(Path(run_dir), int(listing_id))
        if helpers_html_path:
            compiled.setdefault("meta", {})["helpers_dir"] = str(Path(run_dir) / "helpers")
            compiled.setdefault("meta", {})["listingcards_cleaned_path"] = str(helpers_html_path)
        else:
            # Fallback: synthesize minimal listing card HTML using Open API image
            helpers_dir = Path(run_dir) / "helpers"
            ensure_dir(helpers_dir)
            html_path = helpers_dir / "listingcards_cleaned_single_chunk_1.html"
            html_str = generate_listing_card_html(
                int(listing_id),
                compiled.get("title"),
                compiled.get("url"),
                primary_img,
            )
            html_path.write_text(html_str, encoding="utf-8")
            compiled.setdefault("meta", {})["helpers_dir"] = str(helpers_dir)
            compiled.setdefault("meta", {})["listingcards_cleaned_path"] = str(html_path)
    except Exception as e:
        compiled.setdefault("errors", {})["helpers_write"] = str(e)
        # Fallback on error as well
        try:
            helpers_dir = Path(run_dir) / "helpers"
            ensure_dir(helpers_dir)
            html_path = helpers_dir / "listingcards_cleaned_single_chunk_1.html"
            html_str = generate_listing_card_html(
                int(listing_id),
                compiled.get("title"),
                compiled.get("url"),
                primary_img,
            )
            html_path.write_text(html_str, encoding="utf-8")
            compiled.setdefault("meta", {})["helpers_dir"] = str(helpers_dir)
            compiled.setdefault("meta", {})["listingcards_cleaned_path"] = str(html_path)
        except Exception as e2:
            compiled.setdefault("errors", {})["helpers_fallback_write"] = str(e2)

    # Apply user-forced personalization override if provided
    if forced_personalize is not None:
        compiled.setdefault("meta", {}).setdefault("overrides", {})["forced_personalization"] = (
            "personalise" if forced_personalize else "non_personalise"
        )
        if isinstance(compiled.get("popular_info"), dict):
            compiled["popular_info"]["is_personalizable"] = bool(forced_personalize)
        try:
            override_path = outputs_dir / "popular_listings_full_pdf.json"
            override_payload = {
                "listings": [
                    {
                        "listing_id": int(listing_id),
                        "is_personalizable": bool(forced_personalize),
                    }
                ]
            }
            override_path.write_text(json.dumps(override_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            compiled["meta"]["popular_override_source"] = str(override_path)
        except Exception as e:
            compiled.setdefault("errors", {})["personalization_override"] = str(e)

    # 2) Fetch shop info + up to 100 reviews (if shop_id present)
    shop_id = normalized.get("shop_id")
    if isinstance(shop_id, (int, str)) and str(shop_id).isdigit():
        try:
            shop_info = fetch_shop_info(int(shop_id), reviews_limit=100)
            compiled["shop"] = _safe_json(shop_info)
        except Exception as e:
            compiled.setdefault("errors", {})["shop_info"] = str(e)

    # 3) Demand extractor (consumes helpers and writes primary image JSON)
    try:
        demand_summary = process_listing_demand(int(listing_id), Path(run_dir), Path(outputs_dir))
        compiled["signals"] = {
            "scarcity_title": demand_summary.get("scarcity_title"),
        }
        # Map extractor's demand_value to canonical 'demand'
        compiled["demand"] = demand_summary.get("demand_value")

        base_out_dir = Path(demand_summary.get("out_dir") or outputs_dir)

        img_path = base_out_dir / "listingcard_primary_image.json"
        if img_path.exists():
            try:
                compiled["primary_image"] = json.loads(img_path.read_text(encoding="utf-8"))
            except Exception:
                compiled["primary_image"] = None

        var_path = base_out_dir / "variations_cleaned.json"
        if var_path.exists():
            try:
                compiled["variations_cleaned"] = json.loads(var_path.read_text(encoding="utf-8"))
            except Exception:
                compiled["variations_cleaned"] = None

        sale_path = base_out_dir / "listing_sale_info.json"
        if sale_path.exists():
            try:
                compiled["sale_info"] = json.loads(sale_path.read_text(encoding="utf-8"))
            except Exception:
                compiled["sale_info"] = None

        extras_path = base_out_dir / "extras_from_listing.json"
        if extras_path.exists():
            try:
                compiled["demand_extras"] = json.loads(extras_path.read_text(encoding="utf-8"))
            except Exception:
                compiled["demand_extras"] = None

    except Exception as e:
        compiled.setdefault("errors", {})["demand_extractor"] = str(e)

    # Fallback: if demand didn't produce a primary image, use the one fetched pre-demand
    if compiled.get("primary_image") in (None, {}) and primary_img:
        compiled["primary_image"] = primary_img

    # 4) AI Keywords for the title
    title = compiled.get("title")
    if not skip_ai_keywords and isinstance(title, str) and title.strip():
        try:
            kws = generate_keywords_for_title_api(title)
            compiled["keywords"] = kws
        except Exception as e:
            compiled.setdefault("errors", {})["ai_keywords"] = str(e)

    # 5) Everbee/Etsy Search metrics per keyword
    if compiled.get("keywords"):
        try:
            eb_results = fetch_metrics_for_keywords(compiled["keywords"])
            compiled["everbee"]["results"] = [_safe_json(r) for r in eb_results]
        except Exception as e:
            compiled.setdefault("errors", {})["everbee_metrics"] = str(e)

    # Optional: embed discovery of a popular listings source file (if exists)
    try:
        popular_path = discover_popular_json(PROJECT_ROOT / "outputs")
        compiled.setdefault("meta", {})["popular_source"] = str(popular_path)
    except Exception:
        pass

    # Save compiled JSON under run_dir outputs for traceability
    out_path = outputs_dir / "popular_listings_full_pdf.json"
    write_json_atomic(out_path, compiled)

    compiled.setdefault("meta", {})["compiled_path"] = str(out_path)
    return compiled
    
def update_listing_api_flow() -> None:
    """
    Flow:
    1) Ask for listing id
    2) Ask for user id
    3) Ask for session id (users/<user>/sessions/<id>.json)
    4) Ask for forced personalization (optional)
    5) Compile single listing as usual
       - Update Helpers/example.json with the compiled payload
    6) Locate session meta -> run_root_dir -> outputs -> megafile_listings_*.json
    7) Replace the listing entry while preserving previous source_paths
    8) Print the full updated megafile JSON to stdout
    """
    lid = _prompt_listing_id()
    if not isinstance(lid, int) or lid <= 0:
        print("Invalid listing ID. Please enter a numeric listing ID.")
        sys.exit(1)

    user_id = _prompt_user_id()
    if not isinstance(user_id, str) or not user_id.strip():
        print("Invalid user id. Please enter a valid user id.")
        sys.exit(1)
    user_id = slugify_safe(user_id)

    sess_id = _prompt_session_id()
    if not isinstance(sess_id, str) or not sess_id.strip():
        print("Invalid session id. Please enter a valid session id (users/<user>/sessions/<id>.json).")
        sys.exit(1)
    sess_id = slugify_safe(sess_id)

    forced_personalize = _prompt_force_personalization()

    # Compile a fresh single listing payload
    compiled = compile_single_listing(lid, forced_personalize)
    compiled.setdefault("meta", {})["session_id"] = sess_id
    compiled.setdefault("meta", {})["user_id"] = user_id

    # Update Helpers/example.json with the compiled payload
    example_path = CURRENT_FILE_DIR / "example.json"
    write_json_atomic(example_path, compiled)

    # Locate megafile via users/<user>/sessions/<id>.json -> run_root_dir -> outputs/megafile_listings_*.json
    megafile_path = _find_megafile_from_user_session(user_id, sess_id)
    if not megafile_path or not megafile_path.exists():
        print(
            f"Could not find megafile via user session.\n"
            f"Checked: {USERS_DIR}/{user_id}/sessions/{sess_id}.json -> run_root_dir -> outputs/megafile_listings_*.json"
        )
        sys.exit(1)

    # Load megafile, replace listing entry while preserving existing source_paths
    try:
        mega = json.loads(megafile_path.read_text(encoding="utf-8"))
    except Exception:
        mega = {"entries": []}

    entries = mega.get("entries") or []
    new_entry = _to_megafile_entry(compiled)

    idx = next((i for i, e in enumerate(entries) if int(e.get("listing_id") or 0) == int(lid)), None)
    if idx is None:
        entries.append(new_entry)
    else:
        prev_entry = entries[idx]
        prev_source_paths = prev_entry.get("source_paths")
        if prev_source_paths is not None:
            # Preserve the previous source_paths exactly as they were
            new_entry["source_paths"] = prev_source_paths
        entries[idx] = new_entry

    mega["entries"] = entries
    write_json_atomic(megafile_path, mega)

    # Print full updated megafile JSON
    try:
        print(json.dumps(mega, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"Updated megafile written, but printing failed: {e}\nPath: {megafile_path}")

def single_search_api_flow() -> None:
    """
    Flow:
    1) Ask for listing id
    2) Ask for session id (for record/meta only)
    3) Ask for forced personalization (optional)
    4) Compile single search as usual
       - Save it in a dedicated single_search folder with same structure
       - Print the compiled JSON path
    """
    lid = _prompt_listing_id()
    if not isinstance(lid, int) or lid <= 0:
        print("Invalid listing ID. Please enter a numeric listing ID.")
        sys.exit(1)

    sess_id = _prompt_session_id()
    if isinstance(sess_id, str) and sess_id.strip():
        sess_id = slugify_safe(sess_id)
    else:
        sess_id = None

    forced_personalize = _prompt_force_personalization()

    compiled = compile_single_listing(lid, forced_personalize)
    if sess_id:
        compiled.setdefault("meta", {})["session_id"] = sess_id

    out_path = compiled.get("meta", {}).get("compiled_path")
    print(f"\nCompiled JSON written to:\n{out_path}")


def main() -> None:
    """
    Start menu:
    1) updating a listing api
    2) single search api
    """
    try:
        print("\nChoose an option:\n1) updating a listing api\n2) single search api")
        ans = (input("Enter choice [1/2]: ").strip() or "1")
        if ans == "1":
            update_listing_api_flow()
        elif ans == "2":
            single_search_api_flow()
        else:
            print("Invalid choice. Please enter 1 or 2.")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\nExiting.")


if __name__ == "__main__":
    main()