# module: main.py (add startup model check and /ready endpoint)
import os
import sys
import time
import json
from pathlib import Path
from typing import Optional, Dict, List
import requests
from dotenv import load_dotenv
from contextlib import asynccontextmanager
from fastapi import FastAPI
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask
import threading
from concurrent.futures import ThreadPoolExecutor
import asyncio
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi import FastAPI, BackgroundTasks

# Ensure Helpers are importable and import singlesearch helper
PROJECT_ROOT = Path(__file__).resolve().parent
HELPERS_DIR = PROJECT_ROOT / "Helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))

import first_etsy_api_use as etsy
import third_ai_keywords as ai
import fourth_everbee_keyword as everbee
import singlesearch as ss

# Shared thread pool for high concurrency (tune via env API_MAX_WORKERS)
EXECUTOR = ThreadPoolExecutor(max_workers=int(os.getenv("API_MAX_WORKERS", "128")))
# Simple in-memory progress tracker (per-process)
download_progress: Dict[str, Optional[float | int | str]] = {
    "status": "idle",               # idle | downloading | complete | error
    "source": None,                 # URL
    "destination": None,            # file path
    "total_bytes": None,            # int or None if unknown
    "downloaded_bytes": 0,          # int
    "percent": None,                # float 0..100 or None
    "speed_bps": None,              # float bytes/sec
    "eta_seconds": None,            # float seconds or None
    "started_at": None,             # epoch seconds
    "updated_at": None,             # epoch seconds
    "error": None,                  # str
}
download_lock = threading.Lock()

load_dotenv()
OUTPUT_DIR = os.getenv("ETO_OUTPUT_DIR") or str(PROJECT_ROOT / "outputs")
USERS_DIR = os.getenv("AI_USERS_DIR") or str(PROJECT_ROOT / "users")
TEST_TXT_PATH = os.getenv("ETO_SEARCH_CURL_PATH") or str(
    PROJECT_ROOT / "EtoRequests" / "Search_request" / "txt_files" / "1" / "EtoRequest1.txt"
)
# Align helper module config to project root for API-only usage
etsy.PROJECT_ROOT = PROJECT_ROOT
etsy.OUTPUT_DIR = OUTPUT_DIR
etsy.TEST_TXT_PATH = TEST_TXT_PATH
etsy.API_KEYSTRING = os.getenv("ETSY_X_API_KEY") or os.getenv("ETSY_API_KEYSTRING") or ""
etsy.SHARED_SECRET = os.getenv("SHARED_SECRET", "")

# ---------- Models ----------

class RunRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    keyword: str = Field(..., min_length=1)
    desired_total: Optional[int] = None
    session_id: str = Field(..., min_length=1)

class RunScriptRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    script: str = Field(..., min_length=1)
    keyword: Optional[str] = None
    desired_total: Optional[int] = None

class ReconnectRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    keyword: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)

class SingleSearchRequest(BaseModel):
    listing_id: int = Field(..., gt=0)
    user_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    forced_personalize: Optional[bool] = None

class ReplaceListingRequest(BaseModel):
    listing_id: int = Field(..., gt=0)
    user_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    forced_personalize: Optional[bool] = None

# ---------- App ----------

app = FastAPI(title="AI Keywords Etsy API", version="1.2.0")

# ---------- Utils ----------

@app.post("/single-search")
async def api_single_search(payload: SingleSearchRequest):
    """
    Normal single search:
    - Creates outputs/single_search_{listing_id}_{ts}/outputs/popular_listings_full_pdf.json
    - On success: return the compiled JSON directly (no wrapper)
    - Also organizes session meta using user_id/session_id on the server
    """
    loop = asyncio.get_running_loop()
    try:
        compiled = await loop.run_in_executor(
            EXECUTOR,
            ss.run_single_search,
            int(payload.listing_id),
            payload.session_id,
            payload.forced_personalize,
        )

        # Organize session meta for this single-search run (non-fatal)
        try:
            meta = compiled.get("meta") or {}
            update_session_meta_file(
                payload.user_id,
                payload.session_id,
                run_root_dir=meta.get("run_dir"),
                outputs_dir=meta.get("outputs_dir"),
                last_action="single_search",
                compiled_path=meta.get("compiled_path"),
                listing_id=compiled.get("listing_id"),
                timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
        except Exception as meta_err:
            print(f"[single-search] Failed to update session meta: {meta_err}", flush=True)

        # Return the raw compiled JSON, exactly as generated
        return JSONResponse(content=compiled)
    except Exception as e:
        # Keep a simple error wrapper for failures
        return JSONResponse(content={"success": False, "error": str(e)}, status_code=500)


def _resolve_session_meta_path(user_id: str, session_id: str) -> Optional[Path]:
    """
    Find the session meta file robustly:
    - Try slugified (main's slugify), raw, and a "strict" slug without dashes.
    - Fallback: scan all files in sessions dir and match JSON's session_id.
    """
    try:
        users_root = Path(USERS_DIR)
        user_slug = slugify_safe(user_id)
        sessions_dir = users_root / user_slug / "sessions"
        if not sessions_dir.exists():
            return None

        # Candidates by naming strategies
        candidates = []
        # a) main's slugify (may remove/keep dashes based on etsy.slugify_keyword)
        candidates.append(sessions_dir / f"{slugify_safe(session_id)}.json")
        # b) raw session id as filename
        candidates.append(sessions_dir / f"{session_id}.json")
        # c) strict slug: only alnum, no dashes
        strict = "".join(ch.lower() for ch in session_id if ch.isalnum())
        if strict:
            candidates.append(sessions_dir / f"{strict}.json")

        for c in candidates:
            if c.exists():
                return c

        # d) Fallback: scan and match JSON content
        for p in sessions_dir.glob("*.json"):
            try:
                with p.open("r", encoding="utf-8") as f:
                    doc = json.load(f)
                if doc.get("session_id") == session_id:
                    return p
            except Exception:
                continue
        return None
    except Exception:
        return None

def _find_megafile_for_user_session(user_id: str, session_id: str) -> Optional[str]:
    """
    Resolve megafile path using the session meta file:
    - Prefer direct paths stored in meta: last_ranked_megafile_path, last_megafile_path, megafile_path.
    - If not present or missing on disk, fallback to run_root_dir/outputs and prefer ranked files.
    """
    try:
        meta_path = _resolve_session_meta_path(user_id, session_id)
        if not meta_path or not meta_path.exists():
            return None
        with meta_path.open("r", encoding="utf-8") as f:
            doc = json.load(f)

        # Prefer direct paths in session meta (ranked first)
        meta_block = doc.get("meta") or {}
        candidates_direct: List[str] = []
        for key in ("last_ranked_megafile_path", "last_megafile_path", "megafile_path"):
            v = doc.get(key) or meta_block.get(key)
            if isinstance(v, str) and v.strip():
                candidates_direct.append(v.strip())

        for pstr in candidates_direct:
            try:
                p = Path(pstr)
                if p.exists() and p.is_file():
                    return str(p)
            except Exception:
                continue

        # Fallback to run_root_dir/outputs, prefer ranked, else latest normal
        rr = doc.get("run_root_dir") or meta_block.get("run_root_dir")
        if rr:
            base = Path(rr) / "outputs"
            if base.exists():
                # Prefer ranked megafile
                ranked_candidates = sorted(
                    base.glob("megafile_listings_*_ranked.json"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                if ranked_candidates:
                    return str(ranked_candidates[0])

                candidates = sorted(
                    base.glob("megafile_listings_*.json"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                if candidates:
                    return str(candidates[0])

        return None
    except Exception:
        return None

_FILE_LOCKS: Dict[str, any] = {}

def _get_lock_for(path: Path):
    key = str(Path(path).resolve())
    import threading
    lock = _FILE_LOCKS.get(key)
    if lock is None:
        lock = threading.Lock()
        _FILE_LOCKS[key] = lock
    return lock

def _write_json_atomic(path: Path, obj: Dict[str, any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    tmp.replace(path)

@app.post("/replace-listing")
async def api_replace_listing(payload: ReplaceListingRequest):
    """
    Replace listing:
    - Compiles listing and updates the session megafile.
    - Returns final megafile JSON to the requester.
    - Organizes session meta with megafile_path for the user/session.
    """
    loop = asyncio.get_running_loop()
    try:
        # Compile the single listing (heavy work off main thread)
        compiled = await loop.run_in_executor(
            EXECUTOR,
            ss.compile_single_listing,
            int(payload.listing_id),
            payload.forced_personalize,
            True,  # skip_ai_keywords for replace
        )
        # attach meta context
        compiled.setdefault("meta", {})["session_id"] = payload.session_id
        compiled.setdefault("meta", {})["user_id"] = payload.user_id

        # Keep example.json for traceability
        try:
            example_path = (PROJECT_ROOT / "Helpers" / "example.json")
            _write_json_atomic(example_path, compiled)
        except Exception:
            pass

        # Resolve megafile path using robust session meta lookup
        mf_path_str = _find_megafile_for_user_session(payload.user_id, payload.session_id)
        if not mf_path_str:
            return JSONResponse(
                content={
                    "success": False,
                    "error": (
                        "Could not find megafile via user/session. "
                        f"Checked: {USERS_DIR}/{slugify_safe(payload.user_id)}/sessions/{slugify_safe(payload.session_id)}.json "
                        f"and outputs/{slugify_safe(payload.session_id)}/outputs/. "
                        "Also scanned sessions dir and matched by session_id."
                    )
                },
                status_code=404,
            )
        mf_path = Path(mf_path_str)

        # Load and update entries, preserving source_paths; concurrency-safe write
        try:
            with mf_path.open("r", encoding="utf-8") as f:
                mega = json.load(f)
        except Exception:
            mega = {"entries": []}

        entries = mega.get("entries") or []
        new_entry = ss._to_megafile_entry(compiled)
        lock = _get_lock_for(mf_path)
        lock.acquire()
        try:
            idx = next((i for i, e in enumerate(entries) if int(e.get("listing_id") or 0) == int(payload.listing_id)), None)
            if idx is None:
                entries.append(new_entry)
            else:
                prev_entry = entries[idx]
                # Merge values while preserving primary_image, source_paths, and listing_id
                merged = ss._merge_entry_preserving_primary_image(prev_entry, new_entry)
                entries[idx] = merged

            mega["entries"] = entries
            _write_json_atomic(mf_path, mega)
        finally:
            lock.release()

        # Organize session meta for replace-listing run (non-fatal)
        try:
            update_session_meta_file(
                payload.user_id,
                payload.session_id,
                last_action="replace_listing",
                last_megafile_path=str(mf_path),
                last_updated_listing_id=int(payload.listing_id),
                timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
        except Exception as meta_err:
            print(f"[replace-listing] Failed to update session meta: {meta_err}", flush=True)

        # Rank the megafile and return the ranked JSON
        try:
            import ranking as rnk
            print("[ranking] Ranking megafile before sending...", flush=True)
            ranked_path = rnk.rank_megafile(str(mf_path))
            print(f"[ranking] Ranked megafile created: {ranked_path}", flush=True)
            try:
                update_session_meta_file(
                    payload.user_id,
                    payload.session_id,
                    last_action="replace_listing_ranked",
                    last_megafile_path=str(mf_path),
                    last_ranked_megafile_path=str(ranked_path),
                    last_updated_listing_id=int(payload.listing_id),
                    timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
                )
            except Exception as meta_err:
                print(f"[replace-listing] Failed to update session meta (ranked): {meta_err}", flush=True)

            with open(ranked_path, "r", encoding="utf-8") as f:
                ranked_json = json.load(f)

            print("[ranking] Sending ranked megafile JSON to client.", flush=True)
            def _notify_sent_replace():
                print("[ranking] Sent ranked megafile JSON to client (replace-listing).", flush=True)
            return JSONResponse(content=ranked_json, background=BackgroundTask(_notify_sent_replace))
        except Exception as e:
            print(f"[ranking] Ranking failed; sending unranked megafile. Error: {e}", flush=True)
            def _notify_sent_unranked():
                print("[ranking] Sent unranked megafile JSON to client (replace-listing).", flush=True)
            return JSONResponse(content=mega, background=BackgroundTask(_notify_sent_unranked))
    except Exception as e:
        return JSONResponse(content={"success": False, "error": str(e)}, status_code=500)

@app.post("/reconnect/stream")
def reconnect_stream(payload: ReconnectRequest):
    import json, time, queue
    user_id = payload.user_id.strip()
    keyword = payload.keyword.strip()
    session_id = payload.session_id.strip()
    if not user_id or not keyword or not session_id:
        return {"success": False, "error": "user_id, keyword, and session_id are required"}

    key = _session_key(user_id, keyword, session_id)
    sess = _SESSIONS.get(key)

    # If no active session, return raw megafile JSON if present; else emit a 'complete' SSE without megafile
    if not sess or sess.get("status") in (None, "completed", "error"):
        # First, resolve via user/session meta file (preferred)
        mf_path = _find_megafile_for_user_session(user_id, session_id)
        if not mf_path:
            # Fallback to any megafile tracked in memory
            mf_path = (sess or {}).get("megafile_path")

        if not mf_path:
            # Final fallback: by keyword slug, if helper exists
            slug = slugify_safe(keyword)
            try:
                mf_path = _find_latest_megafile_for_slug(slug)
            except Exception:
                mf_path = None

        if mf_path:
            try:
                p = Path(mf_path)
                # If already ranked, just read and send (no rebuild)
                if p.name.endswith("_ranked.json"):
                    with p.open("r", encoding="utf-8") as f:
                        mf_json = json.load(f)
                    print("[ranking] Reconnect: sending ranked megafile JSON (no rebuild).", flush=True)
                    def _notify_sent_reconnect_ranked():
                        print("[ranking] Reconnect: ranked megafile JSON sent.", flush=True)
                    return JSONResponse(content=mf_json, background=BackgroundTask(_notify_sent_reconnect_ranked))

                # Try sibling ranked without recomputing
                ranked_candidate = (
                    p.with_name(p.stem + "_ranked.json")
                    if p.suffix.lower() == ".json"
                    else p.with_name(p.name + "_ranked")
                )
                if ranked_candidate.exists():
                    with ranked_candidate.open("r", encoding="utf-8") as f:
                        mf_json = json.load(f)
                    print(f"[ranking] Reconnect: using existing ranked file: {ranked_candidate}", flush=True)
                    def _notify_sent_reconnect_sibling():
                        print("[ranking] Reconnect: ranked megafile JSON sent (existing sibling).", flush=True)
                    return JSONResponse(content=mf_json, background=BackgroundTask(_notify_sent_reconnect_sibling))

                # Fallback: build ranked once, then return and persist path in session meta
                import ranking as rnk
                print("[ranking] Reconnect: no ranked file found; ranking once.", flush=True)
                ranked_path = rnk.rank_megafile(str(p))
                try:
                    update_session_meta_file(
                        user_id,
                        session_id,
                        last_ranked_megafile_path=str(ranked_path),
                        last_megafile_path=str(p),
                        last_action="reconnect_ranked",
                        timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
                    )
                except Exception:
                    pass
                with open(ranked_path, "r", encoding="utf-8") as f:
                    mf_json = json.load(f)
                print("[ranking] Reconnect: sending ranked megafile JSON.", flush=True)
                def _notify_sent_reconnect_new():
                    print("[ranking] Reconnect: ranked megafile JSON sent (newly built).", flush=True)
                return JSONResponse(content=mf_json, background=BackgroundTask(_notify_sent_reconnect_new))
            except Exception as e:
                print(f"[ranking] Reconnect: ranking failed; sending unranked. Error: {e}", flush=True)
                try:
                    with open(mf_path, "r", encoding="utf-8") as f:
                        mf_json = json.load(f)
                    def _notify_sent_reconnect_unranked():
                        print("[ranking] Reconnect: unranked megafile JSON sent.", flush=True)
                    return JSONResponse(content=mf_json, background=BackgroundTask(_notify_sent_reconnect_unranked))
                except Exception as e2:
                    return JSONResponse(
                        content={"error": f"Failed to load megafile: {str(e2)}", "path": mf_path},
                        status_code=500,
                    )

        def sse_done():
            ev = {
                "type": "complete",
                "user_id": user_id,
                "keyword": keyword,
                "session_id": session_id,
                "message": "No active session and megafile not found.",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "megafile": None,
                "success": True,
            }
            yield f"data: {json.dumps(ev)}\n\n"

        def _notify_sse_closed_no_megafile():
            print("[reconnect] SSE stream closed (no megafile).", flush=True)

        return StreamingResponse(
            sse_done(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
            background=BackgroundTask(_notify_sse_closed_no_megafile),
        )

    # Active session: subscribe and stream future events
    def sse_iter():
        sub_q = _session_subscribe(key)
        try:
            resume_ev = {
                "type": "resume",
                "user_id": user_id,
                "keyword": keyword,
                "session_id": session_id,
                "message": "Reconnected to active session; streaming updates.",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            yield f"data: {json.dumps(resume_ev)}\n\n"
            last_keepalive = time.time()
            while True:
                try:
                    ev = sub_q.get(timeout=0.5)
                    yield f"data: {json.dumps(ev)}\n\n"
                    if ev.get("type") in ("complete", "error"):
                        break
                except queue.Empty:
                    now = time.time()
                    if now - last_keepalive >= 5.0:
                        last_keepalive = now
                        yield f": keepalive {int(now)}\n\n"
        finally:
            _session_unsubscribe(key, sub_q)

    def _notify_sse_closed_active():
        print("[reconnect] SSE stream closed (active session).", flush=True)

    return StreamingResponse(
        sse_iter(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        background=BackgroundTask(_notify_sse_closed_active),
    )

def ensure_model_available() -> Dict[str, any]:
    """
    Ensure the GGUF model exists at MODEL_PATH.
    If missing and MODEL_URL is set, download it once.
    Fallback to PROJECT_ROOT/data/models when /data is not writable (local dev).
    """
    project_root = Path(__file__).resolve().parent
    preferred_dir = Path(os.getenv("MODEL_DIR") or "/data/models")
    fallback_dir = project_root / "data" / "models"

    default_name = "gemma3-999.89M-Q4_K_M.gguf"
    model_path_env = os.getenv("MODEL_PATH")
    model_url = os.getenv("MODEL_URL", "")
    model_sha256 = os.getenv("MODEL_SHA256", "")

    # Decide model_dir based on writability
    model_dir = preferred_dir
    status = {
        "model_dir": str(model_dir),
        "model_path": None,
        "model_present": False,
        "model_size_bytes": None,
        "volume_mounted": False,
        "downloaded": False,
        "error": None,
    }

    try:
        # Try preferred dir (/data/models)
        try:
            model_dir.mkdir(parents=True, exist_ok=True)
            test_file = model_dir / ".rw_test"
            test_file.write_text("ok", encoding="utf-8")
            test_file.unlink(missing_ok=True)
            status["volume_mounted"] = True
        except Exception:
            # Fallback to local project dir
            model_dir = fallback_dir
            status["volume_mounted"] = False
            status["model_dir"] = str(model_dir)
            model_dir.mkdir(parents=True, exist_ok=True)

        # Resolve model_path
        if model_path_env:
            mp = Path(model_path_env)
            if not mp.is_absolute():
                mp = project_root / mp
            model_path = mp
        else:
            model_path = model_dir / default_name
        status["model_path"] = str(model_path)

        # Short-circuit if present
        if model_path.exists() and model_path.is_file():
            status["model_present"] = True
            status["model_size_bytes"] = model_path.stat().st_size
            # Reset progress (already present)
            try:
                download_progress.update({
                    "status": "idle",
                    "source": None,
                    "destination": str(model_path),
                    "total_bytes": status["model_size_bytes"],
                    "downloaded_bytes": status["model_size_bytes"],
                    "percent": 100.0,
                    "speed_bps": None,
                    "eta_seconds": 0.0,
                    "started_at": None,
                    "updated_at": time.time(),
                    "error": None,
                })
            except Exception:
                pass
            return status

        if not model_url:
            status["error"] = f"Model missing at {model_path} and MODEL_URL not set"
            print(
                f"[Model Error] AI model not available.\n"
                f"  model_dir={status['model_dir']}\n"
                f"  model_path={status['model_path']}\n"
                f"  Hint: set MODEL_PATH to a .gguf file or set MODEL_URL to a direct download URL; optionally set MODEL_DIR to a writable location (volume_mounted={status['volume_mounted']}).",
                flush=True,
            )
            return status

        # Robust download with simple retry/backoff
        tmp_path = model_path.with_suffix(model_path.suffix + ".tmp")
        # Pre-clean any stale temp file
        try:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
        except Exception:
            pass

        tries = 3
        last_err = None
        for i in range(1, tries + 1):
            try:
                # Ensure only one concurrent download attempt
                with download_lock:
                    # Initialize progress
                    try:
                        download_progress.update({
                            "status": "downloading",
                            "source": model_url,
                            "destination": str(model_path),
                            "total_bytes": None,
                            "downloaded_bytes": 0,
                            "percent": None,
                            "speed_bps": None,
                            "eta_seconds": None,
                            "started_at": time.time(),
                            "updated_at": time.time(),
                            "error": None,
                        })
                    except Exception:
                        pass
                    last_logged_percent = -5.0
                    last_logged_time = time.time()

                    with requests.get(model_url, stream=True, timeout=600) as r:
                        r.raise_for_status()
                        # Total size if known
                        total_hdr = r.headers.get("Content-Length")
                        total = int(total_hdr) if total_hdr and total_hdr.isdigit() else None
                        try:
                            download_progress["total_bytes"] = total
                        except Exception:
                            pass

                        hasher = None
                        if model_sha256:
                            import hashlib
                            hasher = hashlib.sha256()

                        downloaded = 0
                        start_t = time.time()
                        with open(tmp_path, "wb") as f:
                            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                                if not chunk:
                                    continue
                                f.write(chunk)
                                downloaded += len(chunk)
                                if hasher:
                                    hasher.update(chunk)

                                # Update progress metrics
                                now = time.time()
                                elapsed = max(now - start_t, 1e-6)
                                speed = downloaded / elapsed
                                percent = (downloaded / total * 100.0) if total else None
                                eta = ((total - downloaded) / speed) if (total and speed > 0) else None

                                try:
                                    download_progress.update({
                                        "downloaded_bytes": downloaded,
                                        "speed_bps": speed,
                                        "percent": percent,
                                        "eta_seconds": eta,
                                        "updated_at": now,
                                    })
                                except Exception:
                                    pass

                                # Periodic log every 5 seconds or +5% progress
                                should_log = (now - last_logged_time >= 5.0) or (
                                    percent is not None and percent - last_logged_percent >= 5.0
                                )
                                if should_log:
                                    last_logged_time = now
                                    last_logged_percent = percent or last_logged_percent
                                    if percent is not None and total is not None:
                                        print(f"[Download] {percent:.1f}% ({downloaded}/{total} bytes) "
                                              f"speed={speed/1e6:.2f} MB/s eta={int(eta or 0)}s", flush=True)
                                    else:
                                        print(f"[Download] {downloaded} bytes streamed "
                                              f"speed={speed/1e6:.2f} MB/s", flush=True)

                    # Checksum if provided
                    if model_sha256:
                        digest = hasher.hexdigest() if hasher else ""
                        if digest.lower() != model_sha256.strip().lower():
                            try:
                                tmp_path.unlink(missing_ok=True)
                            except Exception:
                                pass
                            raise RuntimeError(f"SHA256 mismatch: expected {model_sha256}, got {digest}")

                    # Finalize
                    tmp_path.replace(model_path)
                    status["downloaded"] = True
                    status["model_present"] = True
                    status["model_size_bytes"] = model_path.stat().st_size

                    try:
                        download_progress.update({
                            "status": "complete",
                            "destination": str(model_path),
                            "total_bytes": status["model_size_bytes"],
                            "downloaded_bytes": status["model_size_bytes"],
                            "percent": 100.0,
                            "eta_seconds": 0.0,
                            "updated_at": time.time(),
                        })
                    except Exception:
                        pass
                    return status
            except Exception as e:
                last_err = e
                # Clean tmp on failure
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    pass
                backoff = 2 ** (i - 1)
                try:
                    download_progress.update({
                        "status": "error",
                        "error": str(e),
                        "updated_at": time.time(),
                    })
                except Exception:
                    pass
                print(f"[Startup] Download attempt {i}/{tries} failed: {e}. Retrying in {backoff}s...", flush=True)
                time.sleep(backoff)

        status["error"] = f"Download failed after {tries} attempts: {last_err}"
        print(
            f"[Model Error] Download failed after {tries} attempts: {last_err}\n"
            f"  source={model_url}\n"
            f"  destination={status['model_path']}\n"
            f"  Hint: verify MODEL_URL is reachable and correct; if using MODEL_SHA256 ensure checksum matches; alternatively mount a model file and set MODEL_PATH.",
            flush=True,
        )
        return status
    except Exception as e:
        status["error"] = str(e)
        print(
            f"[Model Error] Unexpected error ensuring model: {e}\n"
            f"  model_dir={status['model_dir']}\n"
            f"  destination={status['model_path']}",
            flush=True,
        )
        return status

@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=ensure_model_available, daemon=True, name="model-ensure").start()
    yield

# Do NOT recreate `app` here; attach a startup hook instead so routes remain.
# Attach startup hook; do not recreate `app` again.
@app.on_event("startup")
async def _startup():
    threading.Thread(target=ensure_model_available, daemon=True, name="model-ensure").start()

@app.get("/ready")
def ready() -> Dict:
    st = ensure_model_available()
    return {
        "status": "ok" if st["model_present"] and not st["error"] else "degraded",
        "model_dir": st["model_dir"],
        "model_path": st["model_path"],
        "model_present": st["model_present"],
        "model_size_bytes": st["model_size_bytes"],
        "download_attempted": st["downloaded"],
        "volume_mounted": st["volume_mounted"],
        "error": st["error"],
        "download_progress": {
            "status": download_progress.get("status"),
            "total_bytes": download_progress.get("total_bytes"),
            "downloaded_bytes": download_progress.get("downloaded_bytes"),
            "percent": download_progress.get("percent"),
            "speed_bps": download_progress.get("speed_bps"),
            "eta_seconds": download_progress.get("eta_seconds"),
            "started_at": download_progress.get("started_at"),
            "updated_at": download_progress.get("updated_at"),
            "source": download_progress.get("source"),
            "destination": download_progress.get("destination"),
            "error": download_progress.get("error"),
        },
    }


@app.get("/download/status")
def download_status() -> Dict:
    # Lightweight, separate status endpoint
    return {
        "success": True,
        "progress": download_progress,
    }

def slugify_safe(text: str) -> str:
    # Reuse helper slugify to keep naming consistent
    try:
        return etsy.slugify_keyword(text)
    except Exception:
        return "".join(ch.lower() if ch.isalnum() else "-" for ch in text).strip("-")


# --- Session Registry (for reconnectable streams) ---
_SESSIONS: Dict[str, Dict] = {}
_SESSIONS_LOCK = threading.Lock()

def _session_key(user_id: str, keyword: str, session_id: Optional[str] = None) -> str:
    # Prefer explicit session_id as the unique key
    if session_id and session_id.strip():
        return slugify_safe(session_id.strip())
    return f"{slugify_safe(keyword)}::{slugify_safe(user_id)}"

def _session_get_or_create(user_id: str, keyword: str, session_id: Optional[str] = None) -> Dict:
    key = _session_key(user_id, keyword, session_id)
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(key)
        if not sess:
            sess = {
                "key": key,
                "session_id": session_id,
                "user_id": user_id,
                "keyword": keyword,
                "slug": slugify_safe(keyword),
                "status": "running",
                "started_at": time.time(),
                "ended_at": None,
                "megafile_path": None,
                "run_root_dir": None,
                "outputs_dir": None,
                "events": [],
                "subscribers": [],
                "max_events": 1000,
            }
            _SESSIONS[key] = sess
        return sess

def write_session_meta_file(user_id: str, session_id: str, keyword: str) -> str:
    users_root = USERS_DIR
    ensure_dir(users_root)
    user_dir = str(Path(users_root) / slugify_safe(user_id))
    ensure_dir(user_dir)
    sessions_dir = str(Path(user_dir) / "sessions")
    ensure_dir(sessions_dir)
    meta_path = str(Path(sessions_dir) / f"{slugify_safe(session_id)}.json")
    write_json_file(meta_path, {
        "user_id": user_id,
        "session_id": session_id,
        "keyword": keyword,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    })
    return meta_path

def update_session_meta_file(user_id: str, session_id: str, **fields) -> str:
    users_root = USERS_DIR
    ensure_dir(users_root)
    user_dir = str(Path(users_root) / slugify_safe(user_id))
    ensure_dir(user_dir)
    sessions_dir = str(Path(user_dir) / "sessions")
    ensure_dir(sessions_dir)
    meta_path = str(Path(sessions_dir) / f"{slugify_safe(session_id)}.json")
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            obj = json.load(f)
    except Exception:
        obj = {}
    obj.update(fields or {})
    write_json_file(meta_path, obj)
    return meta_path

def _session_update_meta(key: str, **kwargs) -> None:
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(key)
        if not sess:
            return
        for k, v in kwargs.items():
            sess[k] = v

def _session_emit(key: str, ev: Dict) -> None:
    import queue
    ev = dict(ev or {})
    ev["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(key)
        if not sess:
            return
        # buffer
        events = sess.get("events", [])
        max_events = int(sess.get("max_events", 1000))
        events.append(ev)
        if len(events) > max_events:
            # drop oldest
            del events[: len(events) - max_events]
        sess["events"] = events
        # broadcast
        for q in list(sess.get("subscribers", [])):
            try:
                q.put(ev, block=False)
            except Exception:
                # drop unhealthy subscriber
                try:
                    sess["subscribers"].remove(q)
                except Exception:
                    pass

def _session_subscribe(key: str) -> "queue.Queue[dict]":
    import queue
    q: "queue.Queue[dict]" = queue.Queue()
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(key)
        if not sess:
            return q
        sess.setdefault("subscribers", []).append(q)
    return q

def _session_unsubscribe(key: str, q) -> None:
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(key)
        if not sess:
            return
        try:
            subs = sess.get("subscribers", [])
            if q in subs:
                subs.remove(q)
            sess["subscribers"] = subs
        except Exception:
            pass

def _find_latest_megafile_for_slug(slug: str) -> Optional[str]:
    try:
        # Find latest run folder that starts with slug_
        runs = []
        for p in Path(OUTPUT_DIR).iterdir():
            if p.is_dir() and p.name.startswith(f"{slug}_"):
                runs.append(p)
        if not runs:
            return None
        runs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        latest = runs[0]
        mf = latest / "outputs" / f"megafile_listings_{slug}.json"
        return str(mf) if mf.exists() else None
    except Exception:
        return None

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

def start_queue_consumer_thread(queue_path: str, run_root_dir: str, outputs_dir: str, popular_listings_path: Optional[str] = None, progress_cb: Optional[callable] = None) -> "threading.Thread":
    """
    Start a background thread to consume the real-time popular queue and
    process listings one-by-one. Returns the thread handle.
    """
    import threading
    from pathlib import Path
    import second_demand_extractor as dem

    t = threading.Thread(
        target=dem.consume_popular_queue,
        args=(Path(queue_path), Path(run_root_dir), Path(outputs_dir), 0.5, Path(popular_listings_path) if popular_listings_path else None, progress_cb),
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

            # NEW: attach extras_from_listing.json if present
            extras = None
            extras_path = Path(root) / "extras_from_listing.json"
            if extras_path.exists():
                try:
                    with open(extras_path, "r", encoding="utf-8") as f:
                        extras = _json.load(f)
                except Exception:
                    extras = None

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
                "demand_extras": extras,
                "source_paths": {
                    "combined": str(p),
                    "primary_image": str(img_path) if primary_image else None,
                    "variations_cleaned": str(vc_path) if vclean else None,
                    "listing_sale_info": str(sale_path) if sale_info else None,
                    "extras_from_listing": str(extras_path) if extras else None,
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

def collect_demand_extras_per_listing(cart_runs_root: str) -> Dict[int, Optional[Dict]]:
    # Collect latest extras_from_listing.json per listing_id
    listing_to_latest_ts: Dict[int, float] = {}
    listing_to_extras: Dict[int, Optional[Dict]] = {}
    base = Path(cart_runs_root)
    if not base.exists():
        return listing_to_extras

    for root, dirs, files in os.walk(str(base)):
        if "extras_from_listing.json" in files:
            p = Path(root) / "extras_from_listing.json"
            try:
                with open(p, "r", encoding="utf-8") as f:
                    obj = json.load(f)
            except Exception:
                continue

            # Derive listing_id from path segments; extras files don’t include listing_id
            li = None
            for seg in Path(root).parts[::-1]:
                if seg.isdigit():
                    li = int(seg)
                    break
            if li is None:
                # Fallback: attempt from JSON if present
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
                listing_to_extras[li] = obj
                listing_to_latest_ts[li] = ts
    return listing_to_extras

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
    # NEW: collect per-listing extras_from_listing.json
    demand_extras_map = collect_demand_extras_per_listing(cart_runs_root)

    # NEW: collect per-listing shop info (details, sections, up to 100 reviews)
    try:
        shop_map = etsy.collect_shop_info_map_for_listings(popular_obj.get("listings") or [], reviews_limit=100)
    except Exception:
        shop_map = {}

    enriched_listings: List[Dict] = []
    for it in (popular_obj.get("listings") or []):
        lid = it.get("listing_id") or it.get("listingId")
        try:
            li = int(lid) if lid is not None else None
        except Exception:
            li = None
        enriched = dict(it)
        enriched["demand"] = demand_map.get(li) if li is not None else None
        # NEW: attach extras under demand_extras
        if li is not None and li in demand_extras_map:
            enriched["demand_extras"] = demand_extras_map.get(li)
        # Attach primary image details if available
        if li is not None and li in image_map:
            enriched["primary_image"] = image_map.get(li)
        # Attach variations (only if personalized product produced a file)
        if li is not None and li in variations_map:
            enriched["variations_cleaned"] = variations_map.get(li)
        # NEW: Attach sale info if available
        if li is not None and li in sale_info_map:
            enriched["sale_info"] = sale_info_map.get(li)
        # NEW: Attach shop info per listing
        if li is not None and li in shop_map:
            enriched["shop"] = shop_map.get(li)
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

    # NEW: explicit counts (includes zero)
    processed_total = sum(
        1 for it in listings
        if isinstance(it, dict) and it.get("demand") not in (None, "", False)
    )
    popular_count = summary_obj.get("count")
    if not isinstance(popular_count, int):
        popular_count = len(listings)

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
            "popular_info": it,
            "signals": None,
            "demand_value": it.get("demand"),
            "demand_extras": it.get("demand_extras"),
            "keywords": ai_map.get(li, []),
            "everbee": {"results": ev_map.get(li, [])},
            "primary_image": it.get("primary_image"),
            "variations_cleaned": it.get("variations_cleaned"),
            "sale_info": it.get("sale_info"),
            # NEW: Copy shop info to top-level for convenience
            "shop": it.get("shop"),
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
    # Write entries plus meta counts — includes processed_total=0 explicitly
    write_json_file(str(megafile_path), {
        "entries": entries,
        "meta": {
            "processed_total": processed_total,
            "popular_count": popular_count,
            "keyword_slug": slug,
        },
    })
    return str(megafile_path)

# ---------- Core Orchestration ----------

def orchestrate_run(user_id: str, keyword: str, desired_total: Optional[int] = None, progress_cb: Optional[callable] = None) -> Dict:
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
    try:
        if progress_cb:
            progress_cb({
                "stage": "search",
                "user_id": user_id,
                "remaining": 0,
                "total": fetched_total,
                "message": f"Aggregated search fetched {fetched_total}",
            })
    except Exception:
        pass

    # Run directories (global outputs)
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    top_folder_name = f"{slug}_{timestamp}_{fetched_total}"
    run_root_dir = os.path.join(OUTPUT_DIR, top_folder_name)
    helpers_dir = os.path.join(run_root_dir, "helpers")
    outputs_dir = os.path.join(run_root_dir, "outputs")
    ensure_dir(helpers_dir)
    ensure_dir(outputs_dir)

    # Persist search results
    try:
        search_path = os.path.join(outputs_dir, "search_results.json")
        write_json_file(search_path, search_json)
    except Exception:
        pass

    # Listing IDs + queue init
    listing_ids = etsy.extract_listing_ids(search_json)
    popular_listings_path = os.path.join(outputs_dir, f"popular_now_listings_{slug}.json")
    popular_queue_path = os.path.join(outputs_dir, f"popular_queue_{slug}.json")
    queue_initialized = False
    try:
        etsy.init_popular_queue(popular_queue_path, user_id, slug, desired_total)
        queue_initialized = True
    except Exception as e:
        print(f"[Run] WARN: init queue failed: {e}")

    # Execute listingCards cURL in chunks; emit splitting progress
    result = etsy.run_listingcards_curl_for_ids(
        slug,
        listing_ids,
        helpers_dir,
        outputs_dir,
        queue_path=popular_queue_path if queue_initialized else None,
        queue_user_id=user_id,
        popular_listings_path=popular_listings_path,
        search_json=search_json,
        progress_path=None,
        progress_cb=progress_cb,
    )
    popular_ids_dedup = list(dict.fromkeys(result.get("popular_now_ids") or []))
    total_popular = len(popular_ids_dedup)

    # NEW: ensure downstream stages run; fallback when no popular IDs
    strict_pop = str(os.getenv("STRICT_POPULAR_ONLY", "0")).lower() in ("1", "true", "yes")
    if total_popular == 0:
        if strict_pop:
            print("[Run] Strict popular-only mode: no Popular-now IDs; skipping fallback.", flush=True)
        else:
            fallback_ids = result.get("html_listing_ids_all") or listing_ids
            try:
                fallback_ids = [int(str(x)) for x in (fallback_ids or [])]
            except Exception:
                fallback_ids = listing_ids
            fallback_ids = list(dict.fromkeys(fallback_ids))
            if desired_total is not None:
                try:
                    cap = max(1, int(desired_total))
                except Exception:
                    cap = 10
                fallback_ids = fallback_ids[:cap]
            else:
                fallback_ids = fallback_ids[:min(10, len(fallback_ids))]
            try:
                etsy.append_to_popular_queue(popular_queue_path, fallback_ids, 0, user_id)
            except Exception as e:
                print(f"[Run] WARN: fallback queue append failed: {e}")
            total_popular = len(fallback_ids)
            try:
                if progress_cb:
                    progress_cb({
                        "stage": "fallback",
                        "user_id": user_id,
                        "remaining": total_popular,
                        "total": total_popular,
                        "message": f"Fallback: queued {total_popular} listings from aggregated search",
                    })
            except Exception:
                pass

    # Start AI/Everbee and demand/artifacts threads
    keywords_t = start_keywords_and_everbee_thread(
        popular_listings_path, outputs_dir, slug,
        queue_path=popular_queue_path,
        progress_cb=progress_cb,
        total_target=total_popular,
        user_id=user_id,
    )
    consumer_t = start_queue_consumer_thread(
        popular_queue_path, run_root_dir, outputs_dir,
        popular_listings_path=popular_listings_path,
        progress_cb=progress_cb,
    )
    artifact_t = start_artifact_processor_thread(
        popular_queue_path, run_root_dir, outputs_dir,
        listing_ids, slug,
    )

    # Finalize queue so consumer exits when done
    try:
        etsy.finalize_popular_queue(popular_queue_path, user_id, destroy=False)
    except Exception as e:
        print(f"[Run] WARN: finalize queue failed: {e}")

    # Wait for consumer to finish (bounded)
    t0 = time.time()
    demand_timeout_s = float(os.getenv("DEMAND_JOIN_TIMEOUT", "180"))
    while consumer_t.is_alive() and (time.time() - t0) < demand_timeout_s:
        consumer_t.join(timeout=1.0)

    # Ensure AI/Everbee and artifact processor fully drain before completing the run
    kw_timeout_s = float(os.getenv("KEYWORDS_JOIN_TIMEOUT", os.getenv("EV_JOIN_TIMEOUT", str(demand_timeout_s))))
    art_timeout_s = float(os.getenv("ARTIFACT_JOIN_TIMEOUT", str(demand_timeout_s)))

    t_kw = time.time()
    while keywords_t.is_alive() and (time.time() - t_kw) < kw_timeout_s:
        try:
            keywords_t.join(timeout=1.0)
        except Exception:
            break

    t_art = time.time()
    while artifact_t.is_alive() and (time.time() - t_art) < art_timeout_s:
        try:
            artifact_t.join(timeout=1.0)
        except Exception:
            break

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

    # Emit completion event with megafile path
    try:
        if progress_cb:
            progress_cb({
                "stage": "complete",
                "user_id": user_id,
                "remaining": 0,
                "total": total_popular,
                "message": "Run complete",
                "megafile_path": megafile_path,
            })
    except Exception:
        pass

    # Write per-user session log
    session_paths = create_user_session_dirs(user_id, slug, desired_total, fetched_total)
    run_log = {
        "success": True,
        "user_id": user_id,
        "keyword": keyword,
        "desired_total": desired_total,
        "timing": {
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(start_ts)),
            "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
            "duration_seconds": round(time.time() - start_ts, 3),
        },
        "meta": {
            "megafile_path": megafile_path,
            "run_root_dir": run_root_dir,
            "keyword_slug": slug,
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

def start_keywords_and_everbee_thread(popular_listings_path: str, outputs_dir: str, slug: str, queue_path: Optional[str] = None, progress_cb: Optional[callable] = None, total_target: int = 0, user_id: Optional[str] = None) -> "threading.Thread":
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
        args=(popular_listings_path, outputs_dir, slug, queue_path, progress_cb, total_target, user_id),
        daemon=True,
        name=f"keywords-everbee-{slug}",
    )
    t.start()
    return t

def keywords_and_everbee_stream_worker(popular_listings_path: str, outputs_dir: str, slug: str, queue_path: Optional[str], progress_cb: Optional[callable] = None, total_target: int = 0, user_id: Optional[str] = None) -> None:
    from pathlib import Path
    import json
    import time
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
    ai_completed_ids: set[int] = set()
    ev_completed_ids: set[int] = set()
    futures = []
    executor = ThreadPoolExecutor(max_workers=10)
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

    def _emit(stage: str, done_ids: set[int], total: int, msg: str):
        try:
            if progress_cb:
                remaining = max(0, total - len(done_ids))
                progress_cb({
                    "stage": stage,
                    "user_id": user_id or "n/a",
                    "remaining": remaining,
                    "total": total,
                    "message": msg,
                })
        except Exception:
            pass

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
        # mark completed and emit progress
        ai_completed_ids.add(listing_id)
        _emit("ai_keywords", ai_completed_ids, total_target, f"AI keywords saved for listing_id={listing_id}")

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
        # mark completed (first metric) and emit progress
        if listing_id not in ev_completed_ids:
            ev_completed_ids.add(listing_id)
            _emit("keywords_research", ev_completed_ids, total_target, f"Everbee metrics saved for listing_id={listing_id}")

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
    # emit initial totals
    _emit("ai_keywords", ai_completed_ids, total_target, "AI keywords started")
    _emit("keywords_research", ev_completed_ids, total_target, "Keywords research started")

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
                except Exception:
                    kws = []
            _append_ai(listing_id, title.strip(), [str(x) for x in kws if isinstance(x, str)])
            # Submit Everbee for each keyword (can be empty; will still emit a result if response comes back)
            for kw in ([title.strip()] + list(kws)):
                if isinstance(kw, str) and kw.strip():
                    _everbee_submit(listing_id, title.strip(), kw.strip())
            processed_ids.add(listing_id)

        # If we have a fixed total target, emit a heartbeat with remaining counts
        _emit("ai_keywords", ai_completed_ids, total_target, "AI keywords heartbeat")
        _emit("keywords_research", ev_completed_ids, total_target, "Keywords research heartbeat")

        # Exit when queue is completed and all futures done and expected processed
        try:
            obj = json.load(open(queue_path, "r", encoding="utf-8")) if queue_path and os.path.exists(queue_path) else None
            status = obj.get("status") if isinstance(obj, dict) else None
            expected_ids = _read_popular_ids()
            all_futures_done = all(f.done() for f in futures) if futures else True
            all_processed_expected = True if not expected_ids else all(li in processed_ids for li in expected_ids)
            if status == "completed" and all_futures_done and all_processed_expected:
                _emit("ai_keywords", ai_completed_ids, total_target, "AI keywords completed")
                _emit("keywords_research", ev_completed_ids, total_target, "Keywords research completed")
                break
        except Exception:
            pass

        time.sleep(0.5)

# ---------- Endpoints ----------

from fastapi.responses import StreamingResponse

@app.post("/run/stream")
def run_stream(payload: RunRequest):
    import threading, json, time, queue

    if not payload.user_id.strip():
        return {"success": False, "error": "user_id is required"}
    if not payload.keyword.strip():
        return {"success": False, "error": "keyword is required"}
    if not payload.session_id.strip():
        return {"success": False, "error": "session_id is required"}

    # Serialize full workflow execution inside a single process
    sem = globals().get("_RUN_SEMAPHORE")
    if sem is None:
        import threading as _t, os
        sem = _t.Semaphore(int(os.getenv("RUN_CONCURRENCY", "10")))
        globals()["_RUN_SEMAPHORE"] = sem

    user_id = payload.user_id.strip()
    keyword = payload.keyword.strip()
    session_id = payload.session_id.strip()

    # Persist minimal session meta before any search starts
    try:
        write_session_meta_file(user_id, session_id, keyword)
    except Exception:
        pass

    sess = _session_get_or_create(user_id, keyword, session_id)
    key = sess["key"]

    def emit(ev: dict):
        try:
            ev.setdefault("user_id", user_id)
            ev.setdefault("keyword", keyword)
            ev.setdefault("session_id", session_id)
            _session_emit(key, ev)
        except Exception:
            pass

    def worker():
        try:
            with sem:
                res = orchestrate_run(user_id, keyword, payload.desired_total, progress_cb=emit)
            # Attach full megafile JSON to the completion event
            megafile_json = None
            megafile_path = None
            used_ranked = False
            try:
                mp = (res.get("meta") or {}).get("megafile_path")
                if mp:
                    megafile_path = mp
                    import ranking as rnk
                    print("[ranking] Run: ranking megafile before completion event...", flush=True)
                    ranked_path = rnk.rank_megafile(str(mp))
                    print(f"[ranking] Run: ranked megafile created: {ranked_path}", flush=True)
                    with open(ranked_path, "r", encoding="utf-8") as f:
                        megafile_json = json.load(f)
                    used_ranked = True
                    try:
                        update_session_meta_file(user_id, session_id, last_ranked_megafile_path=str(ranked_path))
                    except Exception as meta_err:
                        print(f"[run_stream] Failed to update session meta (ranked): {meta_err}", flush=True)
                else:
                    megafile_json = None
            except Exception as e:
                print(f"[ranking] Run: ranking failed; sending unranked. Error: {e}", flush=True)
                try:
                    if megafile_path:
                        with open(megafile_path, "r", encoding="utf-8") as f:
                            megafile_json = json.load(f)
                    else:
                        megafile_json = {"error": f"Failed to locate megafile: {str(e)}"}
                except Exception as e2:
                    megafile_json = {"error": f"Failed to load megafile: {str(e2)}"}

            _session_update_meta(
                key,
                status="completed",
                ended_at=time.time(),
                megafile_path=megafile_path,
                run_root_dir=(res.get("meta") or {}).get("run_root_dir"),
            )
            if used_ranked:
                print("[ranking] Run: sending ranked megafile JSON in completion event.", flush=True)
            else:
                print("[ranking] Run: sending unranked megafile JSON in completion event.", flush=True)
            emit({"type": "complete", "success": res.get("success"), "result": res, "megafile": megafile_json})
        except Exception as e:
            _session_update_meta(key, status="error", ended_at=time.time())
            emit({"type": "error", "error": str(e)})

    threading.Thread(target=worker, daemon=True, name=f"run-stream-{slugify_safe(session_id)}").start()

    def sse_iter():
        # subscribe to session and stream events
        sub_q = _session_subscribe(key)
        try:
            start_ev = {
                "type": "start",
                "user_id": user_id,
                "keyword": keyword,
                "session_id": session_id,
                "stage": "start",
                "message": f"Run started for keyword='{keyword}'",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            yield f"data: {json.dumps(start_ev)}\n\n"
            last_keepalive = time.time()
            while True:
                try:
                    ev = sub_q.get(timeout=0.5)
                    yield f"data: {json.dumps(ev)}\n\n"
                    if ev.get("type") in ("complete", "error"):
                        break
                except queue.Empty:
                    now = time.time()
                    if now - last_keepalive >= 5.0:
                        last_keepalive = now
                        yield f": keepalive {int(now)}\n\n"
        finally:
            _session_unsubscribe(key, sub_q)

    return StreamingResponse(
        sse_iter(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )
def run_stream_get(user_id: str, keyword: str, session_id: str, desired_total: Optional[int] = None):
    payload = RunRequest(user_id=user_id, keyword=keyword, desired_total=desired_total, session_id=session_id)
    return run_stream(payload)

@app.get("/health")
def health() -> Dict:
    return {"success": True, "status": "ok"}

# New: non-blocking job submission
@app.post("/enqueue")
def enqueue(payload: RunRequest) -> Dict:
    if not payload.user_id.strip():
        return {"success": False, "error": "user_id is required"}
    if not payload.keyword.strip():
        return {"success": False, "error": "keyword is required"}

    import uuid, time, os
    from typing import Dict as _Dict
    from concurrent.futures import ThreadPoolExecutor

    jobs: _Dict[str, dict] = globals().setdefault("_JOBS", {})
    executor: ThreadPoolExecutor = globals().get("_JOB_EXECUTOR")
    if executor is None:
        executor = ThreadPoolExecutor(max_workers=int(os.getenv("JOB_WORKERS", "10")))
        globals()["_JOB_EXECUTOR"] = executor

    sem = globals().get("_RUN_SEMAPHORE")
    if sem is None:
        import threading
        sem = threading.Semaphore(int(os.getenv("RUN_CONCURRENCY", "10")))
        globals()["_RUN_SEMAPHORE"] = sem

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "queued",
        "user_id": payload.user_id.strip(),
        "keyword": payload.keyword.strip(),
        "desired_total": payload.desired_total,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "started_at": None,
        "finished_at": None,
        "result": None,
        "error": None,
    }

    def _run_job():
        jobs[job_id]["status"] = "running"
        jobs[job_id]["started_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        try:
            with sem:
                res = orchestrate_run(
                    jobs[job_id]["user_id"],
                    jobs[job_id]["keyword"],
                    jobs[job_id]["desired_total"],
                )
            jobs[job_id]["result"] = res
            jobs[job_id]["status"] = "completed" if (isinstance(res, dict) and res.get("success")) else "failed"
        except Exception as e:
            jobs[job_id]["error"] = str(e)
            jobs[job_id]["status"] = "failed"
        finally:
            jobs[job_id]["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

    executor.submit(_run_job)
    return {"success": True, "job_id": job_id}

# New: job status
@app.get("/jobs/{job_id}")
def job_status(job_id: str) -> Dict:
    jobs = globals().get("_JOBS") or {}
    j = jobs.get(job_id)
    if not j:
        return {"success": False, "error": "job not found"}
    return {"success": True, "job": j}

@app.post("/run")
def run(payload: RunRequest) -> Dict:
    try:
        if not payload.user_id.strip():
            return {"success": False, "error": "user_id is required"}
        if not payload.keyword.strip():
            return {"success": False, "error": "keyword is required"}

        # Serialize full workflow execution inside a single process
        sem = globals().get("_RUN_SEMAPHORE")
        if sem is None:
            import threading, os
            sem = threading.Semaphore(int(os.getenv("RUN_CONCURRENCY", "10")))
            globals()["_RUN_SEMAPHORE"] = sem

        with sem:
            result = orchestrate_run(payload.user_id.strip(), payload.keyword.strip(), payload.desired_total)

        # On 100% success, return the full megafile JSON content
        if isinstance(result, dict) and result.get("success") is True:
            try:
                import json
                import ranking as rnk
                megafile_path = (result.get("meta") or {}).get("megafile_path")
                if not megafile_path:
                    return {"success": False, "error": "megafile_path missing from result"}
                print("[ranking] Run: ranking megafile before returning response...", flush=True)
                ranked_path = rnk.rank_megafile(str(megafile_path))
                print(f"[ranking] Run: ranked megafile created: {ranked_path}", flush=True)
                with open(ranked_path, "r", encoding="utf-8") as f:
                    doc = json.load(f)
                entries = doc.get("entries") or []
                meta = doc.get("meta") or {}
                if "processed_total" not in meta:
                    meta["processed_total"] = len([e for e in entries if e.get("demand_value") not in (None, "", False)])
                if "popular_count" not in meta:
                    meta["popular_count"] = len(entries)
                doc["meta"] = meta
                try:
                    # session_id is part of RunRequest
                    update_session_meta_file(payload.user_id.strip(), payload.session_id.strip(), last_ranked_megafile_path=str(ranked_path))
                except Exception as meta_err:
                    print(f"[run] Failed to update session meta (ranked): {meta_err}", flush=True)
                print("[ranking] Run: sending ranked megafile JSON.", flush=True)
                return doc
            except Exception as e:
                print(f"[ranking] Run: ranking failed; sending unranked. Error: {e}", flush=True)
                try:
                    with open(megafile_path, "r", encoding="utf-8") as f:
                        doc = json.load(f)
                    return doc
                except Exception as e2:
                    return {"success": False, "error": f"Failed to load megafile: {e2}"}

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
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8001")))