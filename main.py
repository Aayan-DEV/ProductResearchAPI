# module: main.py (add startup model check and /ready endpoint)
import os
import sys
import time
import json
from pathlib import Path
from typing import Optional, Dict, List
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = os.getenv("ETO_OUTPUT_DIR") or str(PROJECT_ROOT / "outputs")
USERS_DIR = os.getenv("AI_USERS_DIR") or str(PROJECT_ROOT / "users")
OUTPUT_DIR = os.getenv("ETO_OUTPUT_DIR") or str(PROJECT_ROOT / "outputs")
USERS_DIR = os.getenv("AI_USERS_DIR") or str(PROJECT_ROOT / "users")
TEST_TXT_PATH = os.getenv("ETO_SEARCH_CURL_PATH") or str(
    PROJECT_ROOT / "EtoRequests" / "Search_request" / "txt_files" / "1" / "EtoRequest1.txt"
)

# Ensure we can import helpers from subfolder
HELPERS_DIR = PROJECT_ROOT / "Helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))

# Import the existing implementation as a library and align its config
import first_etsy_api_use as etsy
import third_ai_keywords as ai
import fourth_everbee_keyword as everbee

# Align helper module config to project root for API-only usage
etsy.PROJECT_ROOT = PROJECT_ROOT
etsy.OUTPUT_DIR = OUTPUT_DIR
etsy.TEST_TXT_PATH = TEST_TXT_PATH
etsy.API_KEYSTRING = os.getenv("ETSY_X_API_KEY") or os.getenv("ETSY_API_KEYSTRING") or ""
etsy.SHARED_SECRET = os.getenv("SHARED_SECRET", "")

# API server
from fastapi import FastAPI
from pydantic import BaseModel, Field

# ---------- Models ----------

class RunRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    keyword: str = Field(..., min_length=1)
    desired_total: Optional[int] = None

class RunScriptRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    script: str = Field(..., min_length=1)
    keyword: Optional[str] = None
    desired_total: Optional[int] = None

# ---------- App ----------

app = FastAPI(title="AI Keywords Etsy API", version="1.2.0")

# ---------- Utils ----------

def ensure_model_available() -> Dict[str, any]:
    """
    Ensure the GGUF model exists at MODEL_PATH.
    If missing and MODEL_URL is set, download it once into the volume.
    Returns a small status dict.
    """
    model_dir = Path(os.getenv("MODEL_DIR") or "/data/models")
    default_name = "gemma-3n-E4B-it-Q4_K_S.gguf"
    model_path = Path(os.getenv("MODEL_PATH") or (model_dir / default_name))
    model_url = os.getenv("MODEL_URL", "")
    model_sha256 = os.getenv("MODEL_SHA256", "")

    status = {
        "model_dir": str(model_dir),
        "model_path": str(model_path),
        "model_present": False,
        "model_size_bytes": None,
        "volume_mounted": False,
        "downloaded": False,
        "error": None,
    }

    try:
        model_dir.mkdir(parents=True, exist_ok=True)
        # Writable volume check
        test_file = model_dir / ".rw_test"
        try:
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)
            status["volume_mounted"] = True
        except Exception:
            status["volume_mounted"] = False

        if model_path.exists() and model_path.is_file():
            status["model_present"] = True
            status["model_size_bytes"] = model_path.stat().st_size
            return status

        if not model_url:
            status["error"] = f"Model missing at {model_path} and MODEL_URL not set"
            return status

        # Download to temp, stream safely
        tmp_path = model_path.with_suffix(model_path.suffix + ".tmp")
        with requests.get(model_url, stream=True, timeout=600) as r:
            r.raise_for_status()
            hasher = None
            if model_sha256:
                import hashlib
                hasher = hashlib.sha256()
            with open(tmp_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        if hasher:
                            hasher.update(chunk)

        # Verify checksum if provided
        if model_sha256:
            digest = hasher.hexdigest() if hasher else ""
            if digest.lower() != model_sha256.strip().lower():
                tmp_path.unlink(missing_ok=True)
                raise RuntimeError(f"SHA256 mismatch: expected {model_sha256}, got {digest}")

        tmp_path.replace(model_path)
        status["downloaded"] = True
        status["model_present"] = True
        status["model_size_bytes"] = model_path.stat().st_size
        return status
    except Exception as e:
        status["error"] = str(e)
        return status

@app.on_event("startup")
def _startup_model_check():
    st = ensure_model_available()
    # Print a concise log line; useful in Railway logs
    print(f"[Startup] model_dir={st['model_dir']} model_present={st['model_present']} size={st['model_size_bytes']} downloaded={st['downloaded']} error={st['error']}", flush=True)

@app.get("/ready")
def ready() -> Dict:
    """
    Lightweight readiness probe:
      - volume check
      - model presence and size
    """
    st = ensure_model_available()
    return {
        "status": "ok" if st["model_present"] and st["volume_mounted"] and not st["error"] else "degraded",
        "volume_mounted": st["volume_mounted"],
        "model_present": st["model_present"],
        "model_path": st["model_path"],
        "model_size_bytes": st["model_size_bytes"],
        "download_attempted": st["downloaded"],
        "error": st["error"],
    }

def slugify_safe(text: str) -> str:
    # Reuse helper slugify to keep naming consistent
    try:
        return etsy.slugify_keyword(text)
    except Exception:
        return "".join(ch.lower() if ch.isalnum() else "-" for ch in text).strip("-")

def ensure_dir(path_str: str) -> None:
    os.makedirs(path_str, exist_ok=True)

def normalize_script_name(name: str) -> str:
    try:
        return Path(name).stem
    except Exception:
        return name

def build_session_folder_name(keyword_slug: str, desired_total: Optional[int], fetched_total: Optional[int]) -> str:
    # Folder name: keyword_YYYYMMDD_HHMMSS_amount
    date_str = time.strftime("%Y%m%d")
    time_str = time.strftime("%H%M%S")
    amount = desired_total if desired_total is not None else (fetched_total if fetched_total is not None else "NA")
    return f"{keyword_slug}_{date_str}_{time_str}_{amount}"

def create_user_session_dirs(user_id: str, keyword_slug: str, desired_total: Optional[int], fetched_total: Optional[int]) -> Dict[str, str]:
    """
    Create:
      users/<user_id>/sessions/<keyword>_<YYYYMMDD>_<HHMMSS>_<amount>/
    Returns dict of constructed paths for logging.
    """
    users_root = USERS_DIR  # env-overridable
    ensure_dir(users_root)

    user_slug = slugify_safe(user_id)
    user_dir = str(Path(users_root) / user_slug)
    ensure_dir(user_dir)

    sessions_dir = str(Path(user_dir) / "sessions")
    ensure_dir(sessions_dir)

    session_folder_name = build_session_folder_name(keyword_slug, desired_total, fetched_total)
    session_root_dir = str(Path(sessions_dir) / session_folder_name)
    ensure_dir(session_root_dir)

    run_log_path = str(Path(session_root_dir) / "run.json")

    return {
        "users_root": users_root,
        "user_dir": user_dir,
        "sessions_dir": sessions_dir,
        "session_root_dir": session_root_dir,
        "run_log_path": run_log_path,
        "session_folder_name": session_folder_name,
    }

def start_queue_consumer_thread(queue_path: str, run_root_dir: str, outputs_dir: str, popular_listings_path: Optional[str] = None) -> "threading.Thread":
    """
    Start a background thread to consume the real-time popular queue and
    process listings one-by-one. Returns the thread handle.
    """
    import threading
    from pathlib import Path
    import second_demand_extractor as dem

    t = threading.Thread(
        target=dem.consume_popular_queue,
        args=(Path(queue_path), Path(run_root_dir), Path(outputs_dir), 0.5, Path(popular_listings_path) if popular_listings_path else None),
        daemon=True,
        name=f"popular-queue-consumer-{Path(run_root_dir).name}",
    )
    t.start()
    return t

def start_artifact_processor_thread(queue_path: str, run_root_dir: str, outputs_dir: str, listing_ids: List[int], slug: str) -> "threading.Thread":
    """
    Stream-processor to merge demand artifacts into a megafile, now reading AI/Everbee
    outputs produced in parallel. Runs as a background thread and updates
    outputs/megafile_listings_{slug}.json in near realtime.
    """
    import threading
    t = threading.Thread(
        target=process_listing_artifacts_stream,
        args=(queue_path, run_root_dir, outputs_dir, listing_ids, slug),
        daemon=True,
        name=f"artifact-processor-{Path(run_root_dir).name}",
    )
    t.start()
    return t

def process_listing_artifacts_stream(queue_path: str, run_root_dir: str, outputs_dir: str, listing_ids: List[int], slug: str) -> None:
    """
    Merge new demand artifacts with concurrently produced AI keywords and Everbee metrics.
    For each new combined_demand_and_product.json:
    - Attach AI keywords from outputs/ai_keywords_results_{slug}.json
    - Attach Everbee metrics from outputs/everbee_realtime_results_{slug}.json
    - Write/append to outputs/megafile_listings_{slug}.json atomically
    """
    from pathlib import Path
    import time as _time
    import json as _json

    cart_runs_root = Path(run_root_dir) / "cart_runs"
    megafile_path = Path(outputs_dir) / f"megafile_listings_{slug}.json"
    processed: set[int] = set()

    # Initialize megafile if missing
    if not megafile_path.exists():
        with open(megafile_path, "w", encoding="utf-8") as f:
            _json.dump({"entries": []}, f, ensure_ascii=False, indent=2)

    # Helper: load AI keywords and Everbee maps (listing_id -> ...)
    def _load_ai_map() -> dict[int, list[str]]:
        path = Path(outputs_dir) / f"ai_keywords_results_{slug}.json"
        try:
            obj = _json.loads(path.read_text(encoding="utf-8"))
            items = obj.get("listings") or obj.get("results") or []
            out: dict[int, list[str]] = {}
            for it in items:
                lid = it.get("listing_id")
                kws = it.get("keywords") or []
                try:
                    li = int(str(lid))
                except Exception:
                    continue
                out[li] = [str(x) for x in kws if isinstance(x, str)]
            return out
        except Exception:
            return {}

    def _load_everbee_map() -> dict[int, list[dict]]:
        path = Path(outputs_dir) / f"everbee_realtime_results_{slug}.json"
        try:
            obj = _json.loads(path.read_text(encoding="utf-8"))
            items = obj.get("listings") or []
            out: dict[int, list[dict]] = {}
            for it in items:
                lid = it.get("listing_id")
                res = it.get("results") or []
                try:
                    li = int(str(lid))
                except Exception:
                    continue
                # normalize entries, now with full response and no null error
                clean = []
                for r in res:
                    if isinstance(r, dict):
                        entry = {
                            "keyword": r.get("keyword"),
                            "status_code": r.get("status_code"),
                            "request_url": r.get("request_url"),
                            "metrics": r.get("metrics"),
                            "timestamp": r.get("timestamp"),
                        }
                        if r.get("response") is not None:
                            entry["response"] = r.get("response")
                        if r.get("error") not in (None, "", False):
                            entry["error"] = r.get("error")
                        clean.append(entry)
                out[li] = clean
            return out
        except Exception:
            return {}

    # Loop until queue is completed and all listing_ids processed
    while True:
        found_new = False
        ai_map = _load_ai_map()
        ev_map = _load_everbee_map()

        for root, _dirs, files in os.walk(str(cart_runs_root)):
            if "combined_demand_and_product.json" not in files:
                continue
            p = Path(root) / "combined_demand_and_product.json"
            try:
                obj = _json.load(open(p, "r", encoding="utf-8"))
            except Exception as e:
                print(f"[ArtifactProcessor] WARN failed to read {p}: {e}")
                continue

            lid = obj.get("meta", {}).get("listing_id") or (obj.get("popular_info") or {}).get("listing_id")
            try:
                li = int(lid)
            except Exception:
                continue
            if listing_ids and li not in set(listing_ids):
                continue
            if li in processed:
                continue

            # Collect title and unescape
            title = None
            pop = obj.get("popular_info") or {}
            t = pop.get("title")
            try:
                import html as html_lib
                title = html_lib.unescape(t) if isinstance(t, str) else None
            except Exception:
                title = t if isinstance(t, str) else None

            # Attach AI keywords (non-blocking, produced by separate thread)
            keywords = ai_map.get(li, [])

            # Attach Everbee metrics (non-blocking, produced by separate thread)
            everbee_results = ev_map.get(li, [])

            # Attach other artifacts if present
            primary_image = None
            vclean = None
            img_path = Path(root) / "listingcard_primary_image.json"
            if img_path.exists():
                try:
                    with open(img_path, "r", encoding="utf-8") as f:
                        primary_image = _json.load(f)
                except Exception:
                    primary_image = None
            vc_path = Path(root) / "variations_cleaned.json"
            if vc_path.exists():
                try:
                    with open(vc_path, "r", encoding="utf-8") as f:
                        vclean = _json.load(f)
                except Exception:
                    vclean = None

            # NEW: attach sale info if present
            sale_info = None
            sale_path = Path(root) / "listing_sale_info.json"
            if sale_path.exists():
                try:
                    with open(sale_path, "r", encoding="utf-8") as f:
                        sale_info = _json.load(f)
                except Exception:
                    sale_info = None

            # Build entry
            entry = {
                "listing_id": li,
                "title": title,
                "popular_info": pop,
                "signals": obj.get("signals"),
                "demand_value": obj.get("signals", {}).get("demand_value"),
                "keywords": keywords,
                "everbee": {
                    "results": everbee_results
                },
                "primary_image": primary_image,
                "variations_cleaned": vclean,
                "sale_info": sale_info,
                "source_paths": {
                    "combined": str(p),
                    "primary_image": str(img_path) if primary_image else None,
                    "variations_cleaned": str(vc_path) if vclean else None,
                    "listing_sale_info": str(sale_path) if sale_info else None,
                },
                "timestamp": _time.strftime("%Y-%m-%dT%H:%M:%S"),
            }

            # Append to megafile (atomic update)
            try:
                with open(megafile_path, "r", encoding="utf-8") as f:
                    mega = _json.load(f)
            except Exception:
                mega = {"entries": []}

            idx = next((i for i, e in enumerate(mega.get("entries", [])) if e.get("listing_id") == li), None)
            if idx is None:
                mega.setdefault("entries", []).append(entry)
            else:
                mega["entries"][idx] = entry
            write_json_file(str(megafile_path), mega)

            processed.add(li)
            found_new = True
            print(f"[ArtifactProcessor] listing_id={li} merged into megafile (keywords={len(keywords)}, everbee={len(everbee_results)})")

        # Exit conditions
        all_done = listing_ids and len(processed) >= len(listing_ids)
        if all_done:
            print(f"[ArtifactProcessor] Completed. processed={len(processed)} of {len(listing_ids)}.")
            break

        # Secondary exit: queue finished and nothing new surfaced
        queue_completed = False
        try:
            with open(queue_path, "r", encoding="utf-8") as f:
                qobj = _json.load(f)
            queue_completed = str(qobj.get("status", "")).lower() == "completed"
        except Exception:
            queue_completed = not os.path.exists(queue_path)

        if queue_completed and not found_new:
            print(f"[ArtifactProcessor] Completed (queue finalized). processed={len(processed)} of {len(listing_ids)}.")
            break

        time.sleep(1)

def write_json_file(path: str, obj: Dict) -> None:
    """
    Atomically write JSON to path by writing to a temp file then replacing.
    This prevents truncated/partial JSON files under concurrent or interrupted writes.
    """
    ensure_dir(str(Path(path).parent))
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)

def collect_latest_demand_per_listing(cart_runs_root: str) -> Dict[int, Optional[int]]:
    listing_to_latest_ts: Dict[int, float] = {}
    listing_to_demand: Dict[int, Optional[int]] = {}
    base = Path(cart_runs_root)
    if not base.exists():
        return listing_to_demand

    for root, dirs, files in os.walk(str(base)):
        # Prefer combined JSON if available
        if "combined_demand_and_product.json" in files:
            p = Path(root) / "combined_demand_and_product.json"
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
                lid = obj.get("meta", {}).get("listing_id") or (obj.get("popular_info") or {}).get("listing_id")
                demand_value = obj.get("signals", {}).get("demand_value")
            except Exception:
                continue
            try:
                li = int(lid)
            except Exception:
                continue
            ts = p.stat().st_mtime
            prev = listing_to_latest_ts.get(li, -1)
            if ts >= prev:
                if isinstance(demand_value, int):
                    listing_to_demand[li] = demand_value
                elif isinstance(demand_value, str) and demand_value.isdigit():
                    listing_to_demand[li] = int(demand_value)
                else:
                    listing_to_demand[li] = None
                listing_to_latest_ts[li] = ts
            continue

        # Fallback: demand_value.txt
        if "demand_value.txt" in files:
            p = Path(root) / "demand_value.txt"
            try:
                s = p.read_text(encoding="utf-8").strip()
                demand_value = int(s) if s.isdigit() else None
            except Exception:
                demand_value = None

            # Derive listing_id from path segments: .../cart_runs/group_x_y/<listing_id>/<timestamp>/
            li = None
            for seg in Path(root).parts[::-1]:
                if seg.isdigit():
                    li = int(seg)
                    break
            if li is None:
                continue

            ts = p.stat().st_mtime
            prev = listing_to_latest_ts.get(li, -1)
            if ts >= prev:
                listing_to_demand[li] = demand_value
                listing_to_latest_ts[li] = ts

    return listing_to_demand

def collect_primary_image_per_listing(cart_runs_root: str) -> Dict[int, Optional[Dict]]:
    # Collect latest listingcard_primary_image.json per listing_id
    listing_to_latest_ts: Dict[int, float] = {}
    listing_to_image: Dict[int, Optional[Dict]] = {}
    base = Path(cart_runs_root)
    if not base.exists():
        return listing_to_image

    for root, dirs, files in os.walk(str(base)):
        if "listingcard_primary_image.json" in files:
            p = Path(root) / "listingcard_primary_image.json"
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
            except Exception:
                continue
            lid = obj.get("listing_id")
            try:
                li = int(lid)
            except Exception:
                # Fallback: try to infer from path segments
                li = None
                for seg in Path(root).parts[::-1]:
                    if seg.isdigit():
                        li = int(seg)
                        break
            if li is None:
                continue
            ts = p.stat().st_mtime
            prev = listing_to_latest_ts.get(li, -1)
            if ts >= prev:
                listing_to_image[li] = obj
                listing_to_latest_ts[li] = ts
    return listing_to_image

def collect_variations_cleaned_per_listing(cart_runs_root: str) -> Dict[int, Optional[Dict]]:
    # Collect latest variations_cleaned.json per listing_id
    listing_to_latest_ts: Dict[int, float] = {}
    listing_to_variations: Dict[int, Optional[Dict]] = {}
    base = Path(cart_runs_root)
    if not base.exists():
        return listing_to_variations

    for root, dirs, files in os.walk(str(base)):
        if "variations_cleaned.json" in files:
            p = Path(root) / "variations_cleaned.json"
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
            except Exception:
                continue

            # variations_cleaned.json typically lacks listing_id; derive from path
            li = None
            for seg in Path(root).parts[::-1]:
                if seg.isdigit():
                    li = int(seg)
                    break
            # Fallback: attempt from object if present
            if li is None:
                lid = obj.get("listing_id")
                try:
                    li = int(lid) if lid is not None else None
                except Exception:
                    li = None
            if li is None:
                continue

            ts = p.stat().st_mtime
            prev = listing_to_latest_ts.get(li, -1)
            if ts >= prev:
                listing_to_variations[li] = obj
                listing_to_latest_ts[li] = ts

    return listing_to_variations

def collect_listing_sale_info_per_listing(cart_runs_root: str) -> Dict[int, Optional[Dict]]:
    # Collect latest listing_sale_info.json per listing_id
    listing_to_latest_ts: Dict[int, float] = {}
    listing_to_sale: Dict[int, Optional[Dict]] = {}
    base = Path(cart_runs_root)
    if not base.exists():
        return listing_to_sale

    for root, dirs, files in os.walk(str(base)):
        if "listing_sale_info.json" in files:
            p = Path(root) / "listing_sale_info.json"
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
            except Exception:
                continue

            # Derive listing_id from path segments; JSON may not contain it reliably
            li = None
            for seg in Path(root).parts[::-1]:
                if seg.isdigit():
                    li = int(seg)
                    break
            if li is None:
                # Fallback try from JSON (least reliable)
                cand = (obj.get("active_promotion") or {}).get("listing_id")
                try:
                    li = int(cand) if cand is not None else None
                except Exception:
                    li = None
            if li is None:
                continue

            ts = p.stat().st_mtime
            prev = listing_to_latest_ts.get(li, -1)
            if ts >= prev:
                listing_to_sale[li] = obj
                listing_to_latest_ts[li] = ts
    return listing_to_sale

def build_second_step_demand_summary(run_root_dir: str, popular_listings_path: str, slug: str) -> str:
    second_dir = os.path.join(run_root_dir, "second_step_done_demand_extraction")
    ensure_dir(second_dir)

    try:
        with open(popular_listings_path, "r", encoding="utf-8") as f:
            popular_obj = json.load(f)
    except Exception:
        popular_obj = {"count": 0, "popular_now_ids": [], "listings": []}

    cart_runs_root = os.path.join(run_root_dir, "cart_runs")
    demand_map = collect_latest_demand_per_listing(cart_runs_root)

    # NEW: collect per-listing primary image and variations artifacts
    image_map = collect_primary_image_per_listing(cart_runs_root)
    variations_map = collect_variations_cleaned_per_listing(cart_runs_root)
    # NEW: collect per-listing sale info
    sale_info_map = collect_listing_sale_info_per_listing(cart_runs_root)

    enriched_listings: List[Dict] = []
    for it in (popular_obj.get("listings") or []):
        lid = it.get("listing_id") or it.get("listingId")
        try:
            li = int(lid) if lid is not None else None
        except Exception:
            li = None
        enriched = dict(it)
        enriched["demand"] = demand_map.get(li) if li is not None else None
        # Attach primary image details if available
        if li is not None and li in image_map:
            enriched["primary_image"] = image_map.get(li)
        # Attach variations (only if personalized product produced a file)
        if li is not None and li in variations_map:
            enriched["variations_cleaned"] = variations_map.get(li)
        # NEW: Attach sale info if available
        if li is not None and li in sale_info_map:
            enriched["sale_info"] = sale_info_map.get(li)
        enriched_listings.append(enriched)

    summary_obj = {
        "count": popular_obj.get("count"),
        "popular_now_ids": popular_obj.get("popular_now_ids"),
        "listings": enriched_listings,
    }

    summary_path = os.path.join(second_dir, f"popular_now_listings_{slug}_with_demand.json")
    write_json_file(summary_path, summary_obj)
    return summary_path

def build_megafile_from_outputs(outputs_dir: str, second_step_summary_path: str, slug: str) -> str:
    import json as _json
    from pathlib import Path
    import time as _time
    import html as html_lib

    def _safe_load(path: Path) -> dict:
        try:
            return _json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    # Inputs
    summary_obj = _safe_load(Path(second_step_summary_path))
    listings = summary_obj.get("listings") or []

    ai_obj = _safe_load(Path(outputs_dir) / f"ai_keywords_results_{slug}.json")
    ai_items = ai_obj.get("listings") or ai_obj.get("results") or []
    ai_map: dict[int, list[str]] = {}
    for it in ai_items:
        lid = it.get("listing_id")
        try:
            li = int(str(lid))
        except Exception:
            continue
        ai_map[li] = [str(x) for x in (it.get("keywords") or []) if isinstance(x, str)]

    ev_obj = _safe_load(Path(outputs_dir) / f"everbee_realtime_results_{slug}.json")
    ev_items = ev_obj.get("listings") or []
    ev_map: dict[int, list[dict]] = {}
    for it in ev_items:
        lid = it.get("listing_id")
        try:
            li = int(str(lid))
        except Exception:
            continue
        res = it.get("results") or []
        clean = []
        for r in res:
            if isinstance(r, dict):
                entry = {
                    "keyword": r.get("keyword"),
                    "status_code": r.get("status_code"),
                    "request_url": r.get("request_url"),
                    "metrics": r.get("metrics"),
                    "timestamp": r.get("timestamp"),
                }
                if r.get("response") is not None:
                    entry["response"] = r.get("response")
                if r.get("error") not in (None, "", False):
                    entry["error"] = r.get("error")
                clean.append(entry)
        ev_map[li] = clean

    entries = []
    for it in listings:
        lid = it.get("listing_id") or it.get("listingId")
        try:
            li = int(str(lid))
        except Exception:
            continue
        title = it.get("title")
        try:
            title = html_lib.unescape(title) if isinstance(title, str) else title
        except Exception:
            pass

        entry = {
            "listing_id": li,
            "title": title,
            "popular_info": it,  # full per-listing object from the summary
            "signals": None,
            "demand_value": it.get("demand"),
            "keywords": ai_map.get(li, []),
            "everbee": {"results": ev_map.get(li, [])},
            "primary_image": it.get("primary_image"),
            "variations_cleaned": it.get("variations_cleaned"),
            "sale_info": it.get("sale_info"),
            "source_paths": {
                "summary_source": str(second_step_summary_path),
                "ai_keywords_source": str(Path(outputs_dir) / f"ai_keywords_results_{slug}.json"),
                "everbee_source": str(Path(outputs_dir) / f"everbee_realtime_results_{slug}.json"),
                "combined": None,
                "primary_image": None,
                "variations_cleaned": None,
                "listing_sale_info": None,
            },
            "timestamp": _time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        entries.append(entry)

    megafile_path = Path(outputs_dir) / f"megafile_listings_{slug}.json"
    write_json_file(str(megafile_path), {"entries": entries})
    return str(megafile_path)

# ---------- Core Orchestration ----------

def orchestrate_run(user_id: str, keyword: str, desired_total: Optional[int] = None) -> Dict:
    """
    Orchestrate the full pipeline with precise scoping:
    - Etsy v3 aggregated search
    - Start AI Keywords + Everbee thread immediately (reads realtime popular file)
    - Execute listingCards cURL in chunks to detect 'Popular now' and update popular JSON ONLY
    - Enqueue ONLY final popular IDs into the queue (no extras)
    - Run demand consumer thread (filtered) and artifact processor for ONLY those IDs
    - After completion, write a demand-enriched summary and finalize logs
    """
    start_ts = time.time()

    # Base outputs
    ensure_dir(OUTPUT_DIR)
    slug = slugify_safe(keyword)

    # Aggregated search
    print(f"[Run] Starting aggregated v3 search for keyword='{keyword}' user='{user_id}' desired_total={desired_total}")
    search_json = etsy.fetch_listings_aggregated(keyword, desired_total)
    fetched_total = len(search_json.get("results") or [])
    print(f"[Run] Aggregated search fetched {fetched_total} listings.")

    # Run directories (global outputs)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    top_folder_name = f"{slug}_{timestamp}_{fetched_total}"
    run_root_dir = os.path.join(OUTPUT_DIR, top_folder_name)
    helpers_dir = os.path.join(run_root_dir, "helpers")
    outputs_dir = os.path.join(run_root_dir, "outputs")
    ensure_dir(helpers_dir)
    ensure_dir(outputs_dir)

    # Persist search results
    search_results_path = os.path.join(outputs_dir, "search_results.json")
    write_json_file(search_results_path, search_json)

    # Extract listing IDs (search scope, not necessarily popular)
    listing_ids = etsy.extract_listing_ids(search_json)
    if not listing_ids:
        raise RuntimeError("No listing IDs found in search results.")
    if desired_total is not None and desired_total < len(listing_ids):
        listing_ids = listing_ids[:desired_total]

    # Save IDs
    ids_txt_path = os.path.join(helpers_dir, f"listing_ids_{slug}.txt")
    ids_json_path = os.path.join(outputs_dir, f"listing_ids_{slug}.json")
    etsy.write_text(ids_txt_path, "\n".join(str(x) for x in listing_ids))
    write_json_file(ids_json_path, {"listing_ids": listing_ids})

    # Real-time popular listings file and queue file
    popular_listings_path = os.path.join(outputs_dir, f"popular_now_listings_{slug}.json")
    popular_queue_path = os.path.join(outputs_dir, f"popular_queue_{slug}.json")

    # Start keywords + Everbee right away; they watch the popular file in realtime
    keywords_t = start_keywords_and_everbee_thread(popular_listings_path, outputs_dir, slug, queue_path=popular_queue_path)

    # Execute listingCards cURL in multi-part requests
    # IMPORTANT: Do NOT enqueue here; only update realtime popular JSON.
    print("[Run] Executing listingCards cURL in chunks (no queue writes) and updating realtime popular ...")
    combined_obj = etsy.run_listingcards_curl_for_ids(
        slug,
        listing_ids,
        helpers_dir,
        outputs_dir,
        queue_path=None,  # disable queue writes here to avoid non-popular enqueues
        queue_user_id=None,
        popular_listings_path=popular_listings_path,
        search_json=search_json,
    )

    # Cross-reference Popular Now IDs (final consolidation)
    popular_ids = combined_obj.get("popular_now_ids") or []
    # dedupe before final writes
    popular_ids_dedup = list(dict.fromkeys(int(x) for x in popular_ids))
    popular_listings = etsy.cross_reference_popular(search_json, popular_ids_dedup)

    # Write final popular listings file
    write_json_file(popular_listings_path, {
        "count": len(popular_ids_dedup),
        "popular_now_ids": popular_ids_dedup,
        "listings": popular_listings,
    })
    popular_ids_path = os.path.join(outputs_dir, f"popular_now_ids_{slug}.json")
    write_json_file(popular_ids_path, {"popular_now_ids": popular_ids_dedup})

    # Initialize the queue cleanly and append ONLY final popular IDs
    queue_initialized = False
    try:
        # Initialize the queue cleanly and append ONLY final popular IDs
        etsy.init_popular_queue(popular_queue_path, user_id, slug, desired_total)
        if popular_ids_dedup:
            etsy.append_to_popular_queue(popular_queue_path, popular_ids_dedup, 0, user_id)
        else:
            print("[Run] WARNING: No popular IDs found; demand consumer will have nothing to process.")

        # FINALIZE THE QUEUE NOW so consumers can exit once work is done
        try:
            etsy.finalize_popular_queue(popular_queue_path, user_id, destroy=False)
        except Exception as e:
            print(f"[Run] WARN: finalize queue failed: {e}")

        # Start demand consumer (filtered to only popular file), and artifact processor restricted to those IDs
        consumer_t = start_queue_consumer_thread(popular_queue_path, run_root_dir, outputs_dir, popular_listings_path=popular_listings_path)
        processor_t = start_artifact_processor_thread(popular_queue_path, run_root_dir, outputs_dir, popular_ids_dedup, slug)

        # Wait for threads (block until completion)
        try:
            consumer_t.join()
        except Exception:
            print("[Run] WARN: demand consumer thread join failed.")

        try:
            processor_t.join()
        except Exception:
            print("[Run] WARN: artifact processor thread join failed.")

        # Ensure keywords/Everbee worker has finished so its results are in the megafile
        try:
            keywords_t.join()
        except Exception:
            print("[Run] WARN: keywords/everbee thread join failed.")

        try:
            processor_t.join()
        except Exception:
            print("[Run] WARN: artifact processor thread join failed.")

        # Ensure keywords/Everbee worker has finished so its results are in the megafile
        try:
            keywords_t.join()
        except Exception:
            print("[Run] WARN: keywords/everbee thread join failed.")

    finally:
        # Guarantee queue gets finalized even on exceptions
        if queue_initialized:
            try:
                etsy.finalize_popular_queue(popular_queue_path, user_id, destroy=False)
            except Exception as e:
                print(f"[Run] WARN: finalize queue in finally failed: {e}")

    # NEW: Abort run if demand extractor signaled fatal cart residual error
    fatal_path = os.path.join(outputs_dir, "fatal_cart_residual_error.json")
    if os.path.exists(fatal_path):
        try:
            with open(fatal_path, "r", encoding="utf-8") as f:
                err_obj = json.load(f)
        except Exception:
            err_obj = {"error": "even after removing product still in cart"}
        return {
            "success": False,
            "error": err_obj.get("error") or "even after removing product still in cart",
            "meta": {
                "fatal_path": fatal_path,
                "run_root_dir": run_root_dir,
                "keyword_slug": slug,
            },
        }

    print("[Run] Background threads completed.")

    # Optional cleanup: remove queue file if still present
    try:
        if os.path.exists(popular_queue_path):
            os.remove(popular_queue_path)
    except Exception as e:
        print(f"[Run] WARN: failed to remove queue file: {e}")

    # Build second-step demand summary folder and enriched listings file
    second_step_summary_path = build_second_step_demand_summary(run_root_dir, popular_listings_path, slug)

    # Megafile consolidated from summary + AI + Everbee
    megafile_path = build_megafile_from_outputs(outputs_dir, second_step_summary_path, slug)

    # Create per-user session folder and run log
    session_paths = create_user_session_dirs(user_id, slug, desired_total, fetched_total)
    run_log = {
        "success": True,
        "user_id": user_id,
        "keyword": keyword,
        "keyword_slug": slug,
        "desired_total": desired_total,
        "fetched_total": fetched_total,
        "popular_now_ids_count": len(popular_ids_dedup),
        "popular_now_ids": popular_ids_dedup,
        "outputs": {
            "run_root_dir": run_root_dir,
            "search_results_path": search_results_path,
            "ids_txt_path": ids_txt_path,
            "ids_json_path": ids_json_path,
            "popular_listings_path": popular_listings_path,
            "popular_ids_path": popular_ids_path,
            "second_step_summary_dir": os.path.join(run_root_dir, "second_step_done_demand_extraction"),
            "second_step_summary_path": second_step_summary_path,
            "megafile_path": megafile_path,
        },
        "curl_template_used": combined_obj.get("curl_original_path"),
        "session": {
            "users_root": session_paths["users_root"],
            "user_dir": session_paths["user_dir"],
            "sessions_dir": session_paths["sessions_dir"],
            "session_root_dir": session_paths["session_root_dir"],
            "session_folder_name": session_paths["session_folder_name"],
        },
        "timing": {
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(start_ts)),
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
            "duration_seconds": round(time.time() - start_ts, 3),
        },
    }
    write_json_file(session_paths["run_log_path"], run_log)

    print("[Run] Completed. Logs and outputs written.")
    return {
        "success": True,
        "message": (
            f"Fetched {fetched_total}; executed listingCards in "
            f"{len(etsy.chunk_list(listing_ids, etsy.CURL_CHUNK_SIZE))} parts."
        ),
        "meta": {
            "megafile_path": megafile_path,
            "run_root_dir": run_root_dir,
            "keyword_slug": slug,
        },
    }

def start_keywords_and_everbee_thread(popular_listings_path: str, outputs_dir: str, slug: str, queue_path: Optional[str] = None) -> "threading.Thread":
    """
    Spawn background thread that:
    - Watches realtime popular listings JSON for new titles
    - Generates AI keywords as soon as titles appear (no waiting)
    - Fires asynchronous Everbee requests for each keyword
    - Saves ai_keywords_results_{slug}.json and everbee_realtime_results_{slug}.json in realtime
    """
    import threading
    t = threading.Thread(
        target=keywords_and_everbee_stream_worker,
        args=(popular_listings_path, outputs_dir, slug, queue_path),
        daemon=True,
        name=f"keywords-everbee-{slug}",
    )
    t.start()
    return t

def keywords_and_everbee_stream_worker(popular_listings_path: str, outputs_dir: str, slug: str, queue_path: Optional[str]) -> None:
    from pathlib import Path
    import json
    import time
    from concurrent.futures import ThreadPoolExecutor
    import threading

    ai_out = Path(outputs_dir) / f"ai_keywords_results_{slug}.json"
    ev_out = Path(outputs_dir) / f"everbee_realtime_results_{slug}.json"
    ai_out.parent.mkdir(parents=True, exist_ok=True)

    # Initialize files if missing
    for p in (ai_out, ev_out):
        if not p.exists():
            try:
                p.write_text(json.dumps({"listings": []}, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as e:
                print(f"[KeywordsEverbee] WARN: failed to init {p}: {e}")

    # Preload LLM and Etsy Search config
    try:
        llm = ai.ensure_llm_loaded(Path(__file__).resolve().parent)
    except Exception as e:
        print(f"[KeywordsEverbee] ERROR: LLM load failed: {e}")
        llm = None
    try:
        base_url, headers = everbee.ensure_etsy_search_config(Path(__file__).resolve().parent)
    except Exception as e:
        print(f"[KeywordsEverbee] ERROR: Etsy Search config failed: {e}")
        base_url, headers = None, None

    processed_ids: set[int] = set()
    futures = []
    executor = ThreadPoolExecutor(max_workers=4)
    out_lock = threading.Lock()
    search_map: dict[int, str] = {}
    try:
        sr = json.load(open(Path(outputs_dir) / "search_results.json", "r", encoding="utf-8"))
        for it in sr.get("results", []):
            lid = it.get("listing_id")
            title = it.get("title")
            try:
                li = int(str(lid))
                if isinstance(title, str):
                    search_map[li] = title
            except Exception:
                continue
    except Exception:
        search_map = {}

    def _read_queue_ids() -> list[int]:
        try:
            if not queue_path or not os.path.exists(queue_path):
                return []
            obj = json.load(open(queue_path, "r", encoding="utf-8"))
            items = obj.get("items") or []
            out = []
            for it in items:
                lid = it.get("listing_id")
                try:
                    li = int(str(lid))
                except Exception:
                    continue
                if li not in processed_ids:
                    out.append(li)
            return out
        except Exception:
            return []

    def _read_popular_listings() -> list[dict]:
        try:
            if not popular_listings_path or not os.path.exists(popular_listings_path):
                return []
            obj = json.load(open(popular_listings_path, "r", encoding="utf-8"))
            items = obj.get("listings") or obj.get("popular_results") or []
            return [it for it in items if isinstance(it, dict)]
        except Exception:
            return []

    def _read_popular_ids() -> set[int]:
        try:
            if not popular_listings_path or not os.path.exists(popular_listings_path):
                return set()
            obj = json.load(open(popular_listings_path, "r", encoding="utf-8"))
            ids = obj.get("popular_now_ids") or []
            # Fallback: derive from listings if IDs aren’t present
            if not ids:
                ids = [it.get("listing_id") or it.get("listingId") for it in (obj.get("listings") or [])]
            out = set()
            for lid in ids:
                try:
                    out.add(int(str(lid)))
                except Exception:
                    continue
            return out
        except Exception:
            return set()

    def _append_ai(listing_id: int, title: str, keywords: list[str]) -> None:
        with out_lock:
            try:
                doc = json.load(open(ai_out, "r", encoding="utf-8"))
            except Exception:
                doc = {"listings": []}
            idx = next((i for i, e in enumerate(doc.get("listings", [])) if e.get("listing_id") == listing_id), None)
            entry = {"listing_id": listing_id, "title": title, "keywords": keywords, "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")}
            if idx is None:
                doc.setdefault("listings", []).append(entry)
            else:
                doc["listings"][idx] = entry
            write_json_file(str(ai_out), doc)

    def _append_ev(listing_id: int, title: str, result: dict) -> None:
        with out_lock:
            try:
                doc = json.load(open(ev_out, "r", encoding="utf-8"))
            except Exception:
                doc = {"listings": []}
            idx = next((i for i, e in enumerate(doc.get("listings", [])) if e.get("listing_id") == listing_id), None)
            clean = {
                "keyword": result.get("keyword"),
                "status_code": result.get("status_code"),
                "request_url": result.get("request_url"),
                "metrics": result.get("metrics"),
                "response": result.get("response"),
                "timestamp": result.get("timestamp"),
            }
            if result.get("error") not in (None, "", False):
                clean["error"] = result.get("error")
            if idx is None:
                doc.setdefault("listings", []).append({"listing_id": listing_id, "title": title, "results": [clean]})
            else:
                lst = doc["listings"][idx].setdefault("results", [])
                lst.append(clean)
                doc["listings"][idx]["results"] = lst
            write_json_file(str(ev_out), doc)

    def _everbee_submit(listing_id: int, title: str, keyword: str):
        if not base_url or not headers:
            return
        def run_one():
            r = everbee.fetch_metrics_for_keyword(keyword, base_url, headers)
            r["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
            r["keyword"] = keyword
            _append_ev(listing_id, title, r)
            print(f"[Everbee] listing_id={listing_id} kw='{keyword}' -> {r.get('status_code')} metrics={r.get('metrics')}")
        futures.append(executor.submit(run_one))

    print(f"[KeywordsEverbee] START popular='{popular_listings_path}' -> ai='{ai_out.name}' ev='{ev_out.name}'")
    while True:
        # Keep a fresh popular-title map for fallback titles
        pop_items = _read_popular_listings()
        pop_title_map = {}
        for it in pop_items:
            lid = it.get("listing_id") or it.get("listingId")
            title = it.get("title") or ""
            try:
                li = int(str(lid))
                if isinstance(title, str):
                    pop_title_map[li] = title
            except Exception:
                continue

        # Process queue IDs first, but only mark processed if we have a title
        for listing_id in _read_queue_ids():
            title = search_map.get(listing_id, "") or pop_title_map.get(listing_id, "")
            if listing_id in processed_ids:
                continue
            kws = []
            has_title = isinstance(title, str) and title.strip() != ""
            if llm and has_title:
                try:
                    kws, _raw = ai.generate_keywords_for_title(llm, title.strip())
                    kws = ai.dedup_and_cap_keywords(kws, cap=ai.MAX_KEYWORDS)
                except Exception as e:
                    print(f"[KeywordsEverbee] WARN: AI generation failed for listing_id={listing_id}: {e}")
                    kws = []
            _append_ai(listing_id, title, kws)
            for kw in kws:
                _everbee_submit(listing_id, title, kw)
            if has_title:
                processed_ids.add(listing_id)

        # Also process all popular listings to ensure titles-based coverage
        for it in pop_items:
            lid = it.get("listing_id") or it.get("listingId")
            title = it.get("title") or ""
            try:
                listing_id = int(str(lid))
            except Exception:
                continue
            if listing_id in processed_ids:
                continue
            kws = []
            has_title = isinstance(title, str) and title.strip() != ""
            if llm and has_title:
                try:
                    kws, _raw = ai.generate_keywords_for_title(llm, title.strip())
                    kws = ai.dedup_and_cap_keywords(kws, cap=ai.MAX_KEYWORDS)
                except Exception as e:
                    print(f"[KeywordsEverbee] WARN: AI generation failed for listing_id={listing_id}: {e}")
                    kws = []
            _append_ai(listing_id, title, kws)
            for kw in kws:
                _everbee_submit(listing_id, title, kw)
            processed_ids.add(listing_id)

        queue_completed = False
        try:
            if queue_path and os.path.exists(queue_path):
                obj = json.load(open(queue_path, "r", encoding="utf-8"))
                queue_completed = str(obj.get("status", "")).lower() == "completed"
        except Exception:
            queue_completed = False

        expected_ids = _read_popular_ids()
        all_futures_done = all(f.done() for f in futures) if futures else True
        all_processed_expected = expected_ids.issubset(processed_ids) if expected_ids else False

        if queue_completed and all_futures_done and all_processed_expected:
            break
        time.sleep(1)
    executor.shutdown(wait=True)    

# ---------- Endpoints ----------

@app.get("/health")
def health() -> Dict:
    return {"success": True, "status": "ok"}

@app.post("/run")
def run(payload: RunRequest) -> Dict:
    try:
        if not payload.user_id.strip():
            return {"success": False, "error": "user_id is required"}
        if not payload.keyword.strip():
            return {"success": False, "error": "keyword is required"}

        result = orchestrate_run(payload.user_id.strip(), payload.keyword.strip(), payload.desired_total)

        # On 100% success, return the full megafile JSON content
        if isinstance(result, dict) and result.get("success") is True:
            try:
                import json
                megafile_path = (result.get("meta") or {}).get("megafile_path")
                if not megafile_path:
                    return {"success": False, "error": "megafile_path missing from result"}
                with open(megafile_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                return {"success": False, "error": f"Failed to load megafile: {e}"}

        # If not a full success, return the orchestrator response (contains error details)
        return result

    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        text = e.response.text if e.response is not None else ""
        return {"success": False, "error": f"HTTP error from Etsy API: {status} {text}"}
    except Exception as e:
        # Attempt to log failure to a per-user run file if possible
        try:
            user_id = getattr(payload, "user_id", "")
            keyword = getattr(payload, "keyword", "")
            slug = slugify_safe(keyword) if keyword else "unknown"
            session_paths = create_user_session_dirs(user_id or "unknown", slug, payload.desired_total, None)
            fail_log = {
                "success": False,
                "error": str(e),
                "user_id": user_id,
                "keyword": keyword,
                "desired_total": payload.desired_total,
                "timing": {
                    "failed_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
                },
            }
            write_json_file(session_paths["run_log_path"], fail_log)
        except Exception:
            pass
        return {"success": False, "error": str(e)}

@app.post("/run-script")
def run_script(payload: RunScriptRequest) -> Dict:
    try:
        if not payload.user_id.strip():
            return {"success": False, "error": "user_id is required"}
        script_id = normalize_script_name(payload.script or "")
        if script_id not in {"first_etsy_api_use"}:
            return {"success": False, "error": f"unsupported script '{payload.script}'"}
        if not payload.keyword or not payload.keyword.strip():
            return {"success": False, "error": "keyword is required for script run"}

        result = orchestrate_run(payload.user_id.strip(), payload.keyword.strip(), payload.desired_total)
        result["meta"]["script"] = script_id
        return result
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        text = e.response.text if e.response is not None else ""
        return {"success": False, "error": f"HTTP error from Etsy API: {status} {text}"}
    except Exception as e:
        # Attempt to log failure to a per-user run file if possible
        try:
            user_id = getattr(payload, "user_id", "")
            keyword = getattr(payload, "keyword", "")
            slug = slugify_safe(keyword) if keyword else "unknown"
            session_paths = create_user_session_dirs(user_id or "unknown", slug, payload.desired_total, None)
            fail_log = {
                "success": False,
                "error": str(e),
                "user_id": user_id,
                "keyword": keyword,
                "desired_total": payload.desired_total,
                "script": getattr(payload, "script", ""),
                "timing": {
                    "failed_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
                },
            }
            write_json_file(session_paths["run_log_path"], fail_log)
        except Exception:
            pass
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))