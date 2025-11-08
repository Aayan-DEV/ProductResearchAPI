#!/usr/bin/env python3
import json
import re
import urllib.parse
import shutil
import subprocess
import sys
import shlex
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Dict
import time
import requests
import html as html_lib
from html.parser import HTMLParser
import threading
import os

try:
    from Helpers.supabase_helper import (
        upload_cart_runs_and_cleanup,
        upload_bytes,
        upload_file,
        public_url,
        get_bucket_name,
        upload_tmp_cart_runs_and_cleanup,
        upload_directory,
    )
except Exception:
    from supabase_helper import (
        upload_cart_runs_and_cleanup,
        upload_bytes,
        upload_file,
        public_url,
        get_bucket_name,
        upload_tmp_cart_runs_and_cleanup,
        upload_directory,
    )

# ===== Paths and constants =====

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RUNS_ROOT = PROJECT_ROOT / "outputs"
TXT_FILES_ROOT = PROJECT_ROOT / "EtoRequests" / "Cart_sequence" / "txt_files"
SPECIAL_ROOT = TXT_FILES_ROOT / "special"
SPECIAL_SECOND_REMOVE_PATH = SPECIAL_ROOT / "1" / "removesecond.txt"  # fixed path per user request
DEFAULT_COOKIE_JAR = PROJECT_ROOT / "outputs" / ".etsy_cookie_jar.txt"

try:
    from Helpers.supabase_helper import upload_cart_runs_and_cleanup
except Exception:
    from supabase_helper import upload_cart_runs_and_cleanup

# ===== Basic utils =====

def read_file_text(p: Path) -> str:
    # Local-first reads for performance; Supabase is handled by background uploader
    try:
        if not p.exists():
            raise FileNotFoundError(f"Missing file: {p}")
        try:
            return p.read_text(encoding="utf-8")
        except Exception:
            with open(p, "r", encoding="utf-8", errors="replace") as f:
                return f.read()
    except Exception:
        with open(p, "r", encoding="utf-8", errors="replace") as f:
            return f.read()

def write_file_text(p: Path, content: str) -> None:
    # Local-first writes for performance
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")

def safe_copy(src: Path, dst: Path) -> None:
    # Local-first copies for performance
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)

def rm_file_if_exists(p: Path) -> None:
    try:
        if p.exists():
            p.unlink()
    except Exception:
        pass

def ensure_dir(p: Path) -> None:
    # Local directories are created; background upload handles Supabase
    p.mkdir(parents=True, exist_ok=True)

def now_ts_compact() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

# Background upload scheduler for per-listing cart_runs
BACKGROUND_UPLOAD_THREADS: List[threading.Thread] = []

def schedule_background_upload_and_cleanup(out_dir: Path, run_dir: Path, progress_cb: Optional[callable] = None, user_id: Optional[str] = None) -> None:
    try:
        if not out_dir.exists() or "cart_runs" not in out_dir.parts:
            return

        # Derive relative path under cart_runs to reconstruct in staging
        try:
            idx_cr = out_dir.parts.index("cart_runs")
        except ValueError:
            return
        rel_parts = out_dir.parts[idx_cr + 1:]
        listing_id_str = rel_parts[1] if len(rel_parts) >= 2 else "listing"

        # Emit pre-stage progress
        try:
            if progress_cb:
                progress_cb({
                    "stage": "upload_cart_runs",
                    "user_id": user_id or Path.home().name,
                    "listing_id": int(listing_id_str) if str(listing_id_str).isdigit() else None,
                    "message": f"Staging cart_runs for upload to outputs/{run_dir.name}/cart_runs",
                })
        except Exception:
            pass

        # Use a unique tmp root per listing to avoid staging races
        tmp_root_name = f"{run_dir.name}__{listing_id_str}__{now_ts_compact()}"
        staging_root = Path("/tmp/ai_keywords_cart_runs") / tmp_root_name
        # Copy to staging without adding an extra 'cart_runs' layer
        stage_target = staging_root / Path(*rel_parts)

        ensure_dir(stage_target.parent)
        # Copy entire listing out_dir to staging
        shutil.copytree(out_dir, stage_target, dirs_exist_ok=True)

        dest_prefix = f"outputs/{run_dir.name}/cart_runs"

        # Start background upload to Supabase under outputs/<run_dir.name>/cart_runs
        def _worker():
            try:
                res = upload_tmp_cart_runs_and_cleanup(
                    tmp_root_name,
                    dest_prefix=dest_prefix,
                )
                # Emit success progress
                try:
                    if progress_cb:
                        uploaded_count = (res or {}).get("uploaded_count")
                        errors_count = (res or {}).get("errors_count")
                        progress_cb({
                            "stage": "upload_cart_runs",
                            "user_id": user_id or Path.home().name,
                            "listing_id": int(listing_id_str) if str(listing_id_str).isdigit() else None,
                            "message": f"Uploaded cart_runs to {dest_prefix} (uploaded={uploaded_count or 'n/a'} errors={errors_count or 0})",
                        })
                except Exception:
                    pass
            except Exception as e:
                print(f"Background upload failed for {out_dir}: {e}")
                # Emit failure progress
                try:
                    if progress_cb:
                        progress_cb({
                            "stage": "upload_cart_runs_error",
                            "user_id": user_id or Path.home().name,
                            "listing_id": int(listing_id_str) if str(listing_id_str).isdigit() else None,
                            "message": f"Upload failed: {e}",
                        })
                except Exception:
                    pass

        t = threading.Thread(target=_worker, daemon=True)
        t.start()
        BACKGROUND_UPLOAD_THREADS.append(t)

        # Remove local out_dir immediately after staging
        try:
            shutil.rmtree(out_dir, ignore_errors=True)
        except Exception:
            pass

    except Exception as e:
        print(f"Failed to schedule upload for {out_dir}: {e}")

def _supabase_cart_runs_obj_path(path: Path) -> Optional[str]:
    parts = path.parts
    if "cart_runs" not in parts:
        return None
    try:
        idx_cr = parts.index("cart_runs")
    except ValueError:
        return None

    run_name = None
    if "outputs" in parts:
        try:
            idx_out = parts.index("outputs")
            if idx_out + 1 < idx_cr:
                run_name = parts[idx_out + 1]
        except ValueError:
            pass
    if run_name is None and idx_cr - 1 >= 0:
        run_name = parts[idx_cr - 1]

    rel_parts = parts[idx_cr + 1:]
    prefix = ["outputs"]
    if run_name:
        prefix.append(run_name)
    prefix.append("cart_runs")
    return "/".join(prefix + list(rel_parts)).strip("/")

# ===== UI helpers =====

def print_banner():
    print("\n" + "=" * 80)
    print("Etsy Demand Extractor — Add → Listing → Parse → Remove")
    print("=" * 80)
    print("- Debug listing curl executes verbatim from listing.txt; no extra flags.")
    print("- Cache removal: cookie jar cleared before each listing flow.")
    print("- Outputs organized per run, per listing, timestamped.")
    print("=" * 80 + "\n")

def _parse_run_folder_name(name: str) -> dict:
    parts = name.split("_")
    info = {"keyword": name, "date": None, "time": None, "count": None, "ts": None}
    if len(parts) >= 4 and parts[-1].isdigit() and parts[-2].isdigit() and parts[-3].isdigit():
        keyword = "_".join(parts[:-3])
        date = parts[-3]
        time = parts[-2]
        count = int(parts[-1])
        ts = f"{date} {time[:2]}:{time[2:4]}:{time[4:6]}"
        info = {"keyword": keyword, "date": date, "time": time, "count": count, "ts": ts}
    return info

def discover_popular_json(outputs_dir: Path) -> Path:
    try:
        candidates: List[Path] = []
        search_bases = [outputs_dir]
        run_outputs = outputs_dir / "outputs"
        if run_outputs.exists():
            search_bases.append(run_outputs)
        for base in search_bases:
            if not base.exists():
                continue
            # Prefer first_etsy_api_info popular-now listings (full listing objects)
            candidates.extend(sorted(base.glob("popular_now_listings*.json")))
            # Then legacy/populated full listing files
            exact = base / "popular_listings_full.json"
            if exact.exists():
                candidates.append(exact)
            candidates.extend(sorted(base.glob("popular_listings_full_*.json")))
        if candidates:
            candidates = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
            return candidates[0]
    except Exception:
        pass
    return outputs_dir / "popular_listings_full_pdf.json"

def _dedupe_preserve_order_ints(seq: List) -> List[int]:
    seen = set()
    out: List[int] = []
    for x in seq or []:
        try:
            xi = int(x)
        except Exception:
            continue
        if xi not in seen:
            seen.add(xi)
            out.append(xi)
    return out

def _dedupe_listings_by_id(listings: List[dict], keep_ids: Optional[List[int]] = None) -> List[dict]:
    if not isinstance(listings, list):
        return []
    keep_set = set(keep_ids or [])
    seen = set()
    out: List[dict] = []
    for it in listings:
        if not isinstance(it, dict):
            continue
        lid = it.get("listing_id") or it.get("listingId")
        try:
            li = int(lid)
        except Exception:
            continue
        if li in seen:
            continue
        if keep_set and li not in keep_set:
            continue
        seen.add(li)
        out.append(it)
    return out

def normalize_popular_listings_file(json_path: Path) -> Path:
    """
    Dedupe 'popular_now_ids' and 'listings' (or 'popular_results') in-place and fix 'count'.
    Preserves first-seen order and writes back to the same file.
    """
    try:
        if not json_path.exists():
            return json_path
        raw = read_file_text(json_path)
        obj = json.loads(raw)
    except Exception:
        return json_path

    changed = False

    def _collect_listing_ids(listings: List[dict]) -> List[int]:
        ids: List[int] = []
        seen = set()
        if isinstance(listings, list):
            for it in listings:
                if not isinstance(it, dict):
                    continue
                lid = it.get("listing_id") or it.get("listingId")
                try:
                    li = int(lid)
                except Exception:
                    continue
                if li not in seen:
                    seen.add(li)
                    ids.append(li)
        return ids

    if isinstance(obj, dict) and ("popular_now_ids" in obj or "listings" in obj):
        # Prefer IDs proven by listings; otherwise fall back to provided popular_now_ids.
        ids_from_listings = _collect_listing_ids(obj.get("listings") or [])
        ids_from_popular = _dedupe_preserve_order_ints(obj.get("popular_now_ids") or [])
        ids_final = ids_from_listings if ids_from_listings else ids_from_popular
        ids_final = _dedupe_preserve_order_ints(ids_final)

        # Update popular_now_ids if not aligned
        if ids_from_popular != ids_final:
            obj["popular_now_ids"] = ids_final
            changed = True

        # Dedupe listings and align with ids_final
        listings_raw = obj.get("listings")
        if isinstance(listings_raw, list):
            listings_dedup = _dedupe_listings_by_id(listings_raw, keep_ids=ids_final or None)
            if listings_dedup != listings_raw:
                obj["listings"] = listings_dedup
                changed = True

        desired_count = len(ids_final)
        if obj.get("count") != desired_count:
            obj["count"] = desired_count
            changed = True

    elif isinstance(obj, dict) and "popular_results" in obj:
        results_raw = obj.get("popular_results") or []
        results_dedup = _dedupe_listings_by_id(results_raw)
        if results_dedup != results_raw:
            obj["popular_results"] = results_dedup
            changed = True
        desired_count = len(results_dedup)
        if obj.get("count") != desired_count:
            obj["count"] = desired_count
            changed = True

    if changed:
        try:
            write_file_text(json_path, json.dumps(obj, ensure_ascii=False, indent=2))
        except Exception:
            pass

    return json_path

POPULAR_JSON_PATH_DEFAULT = discover_popular_json(PROJECT_ROOT / "outputs")

def select_run_and_get_paths(outputs_root: Path) -> dict:
    if not outputs_root.exists():
        raise FileNotFoundError(f"Outputs root not found: {outputs_root}")
    run_dirs: List[Path] = [p for p in outputs_root.iterdir() if p.is_dir()]
    run_dirs = [p for p in run_dirs if "listing_prep" not in p.name]

    # Fallback: No cart runs found → use first_etsy_api_info outputs directly
    if not run_dirs:
        print("No cart runs found; using first_etsy_api_info outputs.")
        run_dir = outputs_root
        helpers_dir = run_dir / "helpers"
        outputs_dir = run_dir
        helpers_dir.mkdir(parents=True, exist_ok=True)
        outputs_dir.mkdir(parents=True, exist_ok=True)
        demand_extracted_dir = run_dir / "demand extracted"
        demand_extracted_dir.mkdir(parents=True, exist_ok=True)

        def find_one(candidates: List[Path]) -> Optional[Path]:
            existing = [p for p in candidates if p.exists()]
            if not existing:
                return None
            return sorted(existing, key=lambda p: p.stat().st_mtime, reverse=True)[0]

        search_results_path = find_one([
            outputs_dir / "search_results.json",
            run_dir / "search_results.json",
        ])

        listing_ids_path = find_one([
            # first_etsy_api_info writes listing_ids_{slug}.json
            find_one(list(outputs_dir.glob("listing_ids_*.json"))) or outputs_dir / "listing_ids.json",
        ])

        curl_combined_path = None
        try:
            combineds = list(outputs_dir.glob("curl_output_*_combined.json"))
            if combineds:
                curl_combined_path = sorted(combineds, key=lambda p: p.stat().st_mtime, reverse=True)[0]
        except Exception:
            pass

        popular_json_path = discover_popular_json(outputs_dir)
        popular_json_path = normalize_popular_listings_file(popular_json_path)

        # Resolve listing IDs from available artifacts
        resolved_listing_ids: List[int] = []
        try:
            # Prefer full listings JSON produced by first_etsy_api_info
            if popular_json_path and popular_json_path.exists():
                obj = json.loads(popular_json_path.read_text(encoding="utf-8"))
                items = obj.get("listings") or obj.get("popular_results") or []
                if isinstance(items, list):
                    for it in items:
                        lid = (isinstance(it, dict) and it.get("listing_id")) or None
                        if isinstance(lid, int):
                            resolved_listing_ids.append(lid)
            # Fallback: listing_ids JSON
            if not resolved_listing_ids and listing_ids_path and listing_ids_path.exists():
                ids_obj = json.loads(listing_ids_path.read_text(encoding="utf-8"))
                ids = ids_obj.get("listing_ids") or []
                for lid in ids:
                    try:
                        resolved_listing_ids.append(int(lid))
                    except Exception:
                        pass
            # Fallback: combined curl summary (popular_ids or html_listing_ids)
            if not resolved_listing_ids and curl_combined_path and curl_combined_path.exists():
                comb = json.loads(curl_combined_path.read_text(encoding="utf-8"))
                ids = comb.get("popular_ids") or comb.get("html_listing_ids") or []
                for lid in ids:
                    try:
                        resolved_listing_ids.append(int(lid))
                    except Exception:
                        pass
        except Exception:
            resolved_listing_ids = []

        print("\nSelected run (compatibility mode):")
        print(f"- path: {run_dir}")
        print("- saving demand outputs to:", demand_extracted_dir)
        print("- resolved files:")
        print("  • search_results:", search_results_path or "not found")
        print("  • listing_ids:", listing_ids_path or "not found")
        print("  • curl_combined:", curl_combined_path or "not found")
        print("  • popular_listings:", popular_json_path or "not found")
        print(f"  • resolved_listing_ids: {len(resolved_listing_ids)} found")

        return {
            "run_dir": run_dir,
            "helpers_dir": helpers_dir,
            "outputs_dir": outputs_dir,
            "demand_extracted_dir": demand_extracted_dir,
            "search_results_path": search_results_path,
            "listing_ids_path": listing_ids_path,
            "curl_combined_path": curl_combined_path,
            "popular_json_path": popular_json_path,
            "resolved_listing_ids": resolved_listing_ids,
        }

    print("Available runs:")
    for i, rd in enumerate(run_dirs, start=1):
        info = _parse_run_folder_name(rd.name)
        keyword_display = info["keyword"].replace("_", " ")
        ts_display = info["ts"] or "unknown time"
        count_display = info["count"] if info["count"] is not None else "unknown"
        print(f"  [{i}] keyword: '{keyword_display}', time: {ts_display}, count: {count_display}, folder: {rd}")

    choice = None
    while choice is None:
        raw = input(f"Select a run [1-{len(run_dirs)}]: ").strip()
        try:
            n = int(raw)
            if 1 <= n <= len(run_dirs):
                choice = n
            else:
                print("Please enter a valid option number.")
        except ValueError:
            print("Please enter a number.")

    run_dir = run_dirs[choice - 1]
    helpers_dir = run_dir / "helpers"
    outputs_dir = run_dir / "outputs"
    helpers_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    demand_extracted_dir = run_dir / "demand extracted"
    demand_extracted_dir.mkdir(parents=True, exist_ok=True)

    def find_one(candidates: List[Path]) -> Optional[Path]:
        existing = [p for p in candidates if p.exists()]
        if not existing:
            return None
        return sorted(existing, key=lambda p: p.stat().st_mtime, reverse=True)[0]

    search_results_path = find_one([
        outputs_dir / "search_results.json",
        run_dir / "search_results.json",
    ])

    listing_ids_path = find_one([
        outputs_dir / "listing_ids.json",
        run_dir / "listing_ids.json",
    ])

    curl_combined_path = None
    try:
        combineds = list(outputs_dir.glob("curl_output_*_combined.json"))
        if combineds:
            curl_combined_path = sorted(combineds, key=lambda p: p.stat().st_mtime, reverse=True)[0]
    except Exception:
        pass

    popular_json_path = discover_popular_json(outputs_dir)
    popular_json_path = normalize_popular_listings_file(popular_json_path)

    info = _parse_run_folder_name(run_dir.name)
    keyword_display = info["keyword"].replace("_", " ")
    ts_display = info["ts"] or "unknown time"
    count_display = info["count"] if info["count"] is not None else "unknown"
    print("\nSelected run:")
    print(f"- keyword: '{keyword_display}'")
    print(f"- time: {ts_display}")
    print(f"- count: {count_display}")
    print(f"- path: {run_dir}")
    print("- saving demand outputs to:", demand_extracted_dir)
    print("- resolved files:")
    print("  • search_results:", search_results_path or "not found")
    print("  • listing_ids:", listing_ids_path or "not found")
    print("  • curl_combined:", curl_combined_path or "not found")
    print("  • popular_listings:", popular_json_path or "not found")

    return {
        "run_dir": run_dir,
        "helpers_dir": helpers_dir,
        "outputs_dir": outputs_dir,
        "demand_extracted_dir": demand_extracted_dir,
        "search_results_path": search_results_path,
        "listing_ids_path": listing_ids_path,
        "curl_combined_path": curl_combined_path,
        "popular_json_path": popular_json_path,
    }
    
# ===== Curl parsing/execution =====

def parse_curl_file(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
    except Exception:
        return {}, {}, [], None

    normalized = re.sub(r"\\\s*\n", " ", content)

    url = None
    m_url = re.search(r"""--url\s+(?:(['"])(.*?)\1|(\S+))""", normalized, flags=re.DOTALL)
    if m_url:
        url = m_url.group(2) or m_url.group(3)
    else:
        url_match = re.search(r"""curl\s+(['"])(.*?)\1""", normalized, flags=re.DOTALL)
        if url_match:
            url = url_match.group(2)

    headers = {}
    for m in re.finditer(r"""(?:-H|--header)\s+(['"])(.*?)\1""", normalized, flags=re.DOTALL):
        hv = m.group(2)
        if ":" in hv:
            k, v = hv.split(":", 1)
            headers[k.strip().lower()] = v.strip()

    cookies = {}
    for m in re.finditer(r"""(?:-b|--cookie)\s+(?:(['"])(.*?)\1|(\S+))""", normalized, flags=re.DOTALL):
        cookie_str = m.group(2) or m.group(3) or ""
        for part in re.split(r";\s*", cookie_str.strip()):
            if not part:
                continue
            if "=" in part:
                ck, cv = part.split("=", 1)
                cookies[ck.strip()] = cv
            else:
                cookies[part.strip()] = ""

    cookie_header_val = headers.get("cookie")
    if cookie_header_val:
        for part in re.split(r";\s*", cookie_header_val.strip()):
            if not part:
                continue
            if "=" in part:
                ck, cv = part.split("=", 1)
                cookies[ck.strip()] = cv
            else:
                cookies[part.strip()] = ""

    data = []
    for m in re.finditer(r"""--data(?:-raw)?\s+(?:(['"])(.*?)\1|([^\s\\]+))""", normalized, flags=re.DOTALL):
        data_str = m.group(2) or m.group(3) or ""
        pairs = urllib.parse.parse_qsl(data_str, keep_blank_values=True)
        for k, v in pairs:
            data.append((k.strip(), v))

    return cookies, headers, data, url

def add_location_flag_to_curl(cmd: str) -> str:
    if re.search(r"(^|\s)--location(\s|$)|(^|\s)-L(\s|$)", cmd):
        return cmd
    return re.sub(r"\bcurl\b", "curl --location", cmd, count=1)

def inject_cookie_jar(cmd: str, jar_path: Path, read: bool, write: bool) -> str:
    cmd = add_location_flag_to_curl(cmd)
    flags = []
    if read:
        flags.append(f"-b '{jar_path}'")
    if write:
        flags.append(f"-c '{jar_path}'")

    # If reading from a missing jar and ETSY_COOKIE_HEADER is provided, inject header too
    try:
        jar_missing = read and (not jar_path.exists())
    except Exception:
        jar_missing = False
    cookie_hdr = os.getenv("ETSY_COOKIE_HEADER", "")
    if jar_missing and cookie_hdr:
        flags.append(f"-H 'Cookie: {cookie_hdr}'")

    if not flags:
        return cmd
    pattern = re.compile(r"\bcurl\b(?:\s+--location)?")
    def add_flags(m: re.Match) -> str:
        return f"{m.group(0)} {' '.join(flags)}"
    return pattern.sub(add_flags, cmd, count=1)

def normalize_curl_multiline(content: str) -> str:
    return re.sub(r"\\\s*\n", " ", content).strip()

def run_shell_command(cmd: str, timeout: int = 120) -> Tuple[int, str, str]:
    cmd_line = normalize_curl_multiline(cmd)
    # Ensure curl has sensible flags everywhere (including listing debug path)
    cmd_line = add_location_flag_to_curl(cmd_line)
    # Add --fail-with-body to surface HTTP >=400 as non-zero exit
    if re.search(r"(^|\s)--fail-with-body(\s|$)", cmd_line) is None:
        cmd_line = re.sub(r"\bcurl\b", "curl --fail-with-body", cmd_line, count=1)

    args = shlex.split(cmd_line)
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        out, err = proc.communicate(timeout=timeout)
        return proc.returncode, out, err
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
        except Exception:
            pass
        return 124, "", "TimeoutExpired"
    except FileNotFoundError as e:
        missing_prog = args[0] if args else "unknown"
        if missing_prog == "curl":
            return 127, "", "curl not found. Install curl or ensure PATH. On Railway, ensure Dockerfile installs curl."
        return 127, "", f"{missing_prog} not found: {e}"
    except Exception as e:
        return 1, "", f"{e}"

# ===== Group discovery =====

def fetch_inventory_id_from_cart_list(cart_id: int, listing_id: int, jar_path: Path, out_dir: Path) -> Optional[int]:
    url = f"https://www.etsy.com/api/v3/ajax/member/cart-list/{int(cart_id)}"
    cmd = f'curl --request GET "{url}"'
    cmd_jar = inject_cookie_jar(cmd, jar_path, read=True, write=True)
    rc, stdout, stderr = run_shell_command(cmd_jar, timeout=60)
    write_file_text(out_dir / "cart_list_stdout.json", stdout)
    write_file_text(out_dir / "cart_list_stderr.txt", stderr if stderr else "")
    write_file_text(out_dir / "cart_list_exit.txt", str(rc))
    if rc != 0:
        return None
    try:
        obj = json.loads(stdout)
    except Exception:
        try:
            obj = json.loads(html_lib.unescape(stdout))
        except Exception:
            return None
    def lookup(container: dict) -> Optional[int]:
        # Common containers and keys
        for key in ("line_items", "items", "cart_listings", "listings", "lineItems"):
            arr = container.get(key)
            if isinstance(arr, list):
                for it in arr:
                    if not isinstance(it, dict):
                        continue
                    # match listing id
                    lid = it.get("listing_id") or it.get("listingId") or it.get("listing")
                    try:
                        lid_i = int(str(lid)) if lid is not None else None
                    except Exception:
                        lid_i = None
                    inv = it.get("inventory_id") or it.get("inventoryId")
                    if lid_i == int(listing_id) and isinstance(inv, int) and inv > 0:
                        return inv
        return None
    if isinstance(obj, dict):
        inv = lookup(obj)
        if isinstance(inv, int) and inv > 0:
            return inv
        for ck in ("data", "payload", "response"):
            cont = obj.get(ck)
            if isinstance(cont, dict):
                inv = lookup(cont)
                if isinstance(inv, int) and inv > 0:
                    return inv
    # Regex fallback
    m = re.search(r'"inventoryId"\s*:\s*(\d+)', stdout) or re.search(r'"inventory_id"\s*:\s*(\d+)', stdout)
    if m:
        val = int(m.group(1))
        if val > 0:
            return val
    return None

def modify_listing_command(original_cmd: str, listing_id: int) -> str:
    """
    Modify the listing command to target the specific listing_id.
    Replaces the first 'listing/<number>' prefix with the provided ID.
    """
    try:
        lid = int(listing_id)
    except Exception:
        lid = int(str(listing_id))
    return _replace_prefix_number(original_cmd, "listing/", lid, max_count=1)

def extract_inventory_id_from_listing_stdout(stdout: str, listing_id: int, out_dir: Path) -> Optional[int]:
    """
    Parse inventory id for the given listing_id from the sidebar (listing) curl stdout.
    Looks for 'listing_inventory_id' (preferred) and falls back to 'inventory_id'/'inventoryId'
    within common containers: 'listings', 'cart_listings', 'line_items', 'items'.
    Writes 'inventory_id_from_listing_stdout.txt' when found.
    """
    def coerce_int(v) -> Optional[int]:
        try:
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                s = v.strip()
                return int(s) if s.isdigit() else None
        except Exception:
            pass
        return None

    def find_in_container(container: dict) -> Optional[int]:
        if not isinstance(container, dict):
            return None
        for key in ("listings", "cart_listings", "line_items", "items"):
            arr = container.get(key)
            if isinstance(arr, list):
                for it in arr:
                    if not isinstance(it, dict):
                        continue
                    lid = it.get("listing_id") or it.get("listingId") or it.get("listing")
                    try:
                        lid_i = int(str(lid)) if lid is not None else None
                    except Exception:
                        lid_i = None
                    if lid_i == int(listing_id):
                        # Prefer listing_inventory_id (as in your sidebar JSON)
                        iid = coerce_int(
                            it.get("listing_inventory_id")
                            or it.get("inventory_id")
                            or it.get("inventoryId")
                        )
                        if isinstance(iid, int) and iid > 0:
                            return iid
        return None

    obj = None
    try:
        obj = json.loads(stdout)
    except json.JSONDecodeError:
        # Try extracting a JSON object substring
        m = re.search(r"\{.*\}", stdout, flags=re.DOTALL)
        if m:
            try:
                obj = json.loads(m.group(0))
            except Exception:
                obj = None

    if not isinstance(obj, dict):
        return None

    # Search in root
    iid = find_in_container(obj)
    if isinstance(iid, int) and iid > 0:
        write_file_text(out_dir / "inventory_id_from_listing_stdout.txt", str(iid))
        return iid

    # Search common nested containers
    for ckey in ("data", "payload", "event_payload", "response"):
        cont = obj.get(ckey)
        iid = find_in_container(cont)
        if isinstance(iid, int) and iid > 0:
            write_file_text(out_dir / "inventory_id_from_listing_stdout.txt", str(iid))
            return iid

    return None

def modify_variations_command(original_cmd: str, cart_id: int, listing_id: int) -> str:
    """
    Modify the variations curl to use the provided cart_id and listing_id in the URL path segments.
    Examples:
      cart-list/<id> -> cart-list/{cart_id}
      cart/<id>      -> cart/{cart_id}
      listing/<id>   -> listing/{listing_id}
    """
    cmd = original_cmd
    cmd = _replace_prefix_number(cmd, "cart-list/", int(cart_id), max_count=2)
    cmd = _replace_prefix_number(cmd, "cart/", int(cart_id), max_count=2)
    cmd = _replace_prefix_number(cmd, "listing/", int(listing_id), max_count=1)
    return cmd

def extract_variations_html(stdout: str) -> Optional[str]:
    """
    Extract 'variations_html' string from the variations offerings response JSON.
    Searches top-level and common nested containers.
    Returns the HTML string if found.
    """
    obj = None
    try:
        obj = json.loads(stdout)
    except Exception:
        try:
            obj = json.loads(html_lib.unescape(stdout))
        except Exception:
            obj = None

    if not isinstance(obj, dict):
        return None

    # Direct key
    v = obj.get("variations_html")
    if isinstance(v, str) and v.strip():
        return html_lib.unescape(v)

    # Common nested containers
    for ckey in ("data", "payload", "response", "event_payload"):
        cont = obj.get(ckey)
        if isinstance(cont, dict):
            v2 = cont.get("variations_html")
            if isinstance(v2, str) and v2.strip():
                return html_lib.unescape(v2)

    return None

def run_variations_request(variations_txt_path: Path, cart_id: int, listing_id: int, jar_path: Path, out_dir: Path) -> Optional[str]:
    """
    Execute the variations offerings curl (GET) with cart_id and listing_id injected.
    Saves:
      - variations_original.txt
      - variations_modified.txt
      - variations_sent_with_cookiejar.txt
      - variations_stdout.json
      - variations_stderr.txt
      - variations_exitcode.txt
      - variations_html.html (formatted HTML if 'variations_html' present)
    Returns the HTML string if extracted, else None.
    """
    try:
        original = read_file_text(variations_txt_path)
    except Exception as e:
        write_file_text(out_dir / "variations_read_error.txt", str(e))
        return None

    write_file_text(out_dir / "variations_original.txt", original)

    try:
        cmd = modify_variations_command(original, cart_id=cart_id, listing_id=listing_id)
    except Exception as e:
        write_file_text(out_dir / "variations_modify_error.txt", str(e))
        return None

    write_file_text(out_dir / "variations_modified.txt", cmd)

    # Inject cookie jar flags to persist and reuse session
    cmd_jar = inject_cookie_jar(cmd, jar_path, read=True, write=True)
    write_file_text(out_dir / "variations_sent_with_cookiejar.txt", cmd_jar)

    rc, stdout, stderr = run_shell_command(cmd_jar, timeout=90)
    write_file_text(out_dir / "variations_stdout.json", stdout)
    write_file_text(out_dir / "variations_stderr.txt", stderr if stderr else "")
    write_file_text(out_dir / "variations_exitcode.txt", str(rc))

    if rc != 0 or not stdout.strip():
        return None

    html_str = extract_variations_html(stdout)
    if isinstance(html_str, str) and html_str.strip():
        # Save formatted HTML (wrap in minimal document)
        formatted = (
            "<!DOCTYPE html>\n"
            "<html>\n"
            "<head>\n"
            "  <meta charset=\"utf-8\" />\n"
            "  <title>Variations</title>\n"
            "  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\" />\n"
            "  <style>body{font-family:system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; padding:16px;}</style>\n"
            "</head>\n"
            "<body>\n"
            "  <div id=\"variations-container\">\n"
            f"{html_str}\n"
            "  </div>\n"
            "</body>\n"
            "</html>\n"
        )
        write_file_text(out_dir / "variations_html.html", formatted)
        return html_str

    return None

class VariationsHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.label_for_map = {}
        self.current_label_for = None
        self.collect_label_text = False
        self.label_text_buf = []

        self.current_select_id = None
        self.select_to_options = {}
        self.current_opt_value = None
        self.option_text_buf = []
        self.select_to_optgroup_label = {}

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if tag.lower() == "label":
            self.collect_label_text = True
            self.current_label_for = attrs_dict.get("for")
            self.label_text_buf = []
        elif tag.lower() == "select":
            sel_id = attrs_dict.get("id")
            self.current_select_id = sel_id
            if sel_id and sel_id not in self.select_to_options:
                self.select_to_options[sel_id] = []
        elif tag.lower() == "optgroup":
            label = attrs_dict.get("label")
            if self.current_select_id and label and self.current_select_id not in self.select_to_optgroup_label:
                self.select_to_optgroup_label[self.current_select_id] = label
        elif tag.lower() == "option":
            self.current_opt_value = attrs_dict.get("value")
            self.option_text_buf = []

    def handle_endtag(self, tag):
        if tag.lower() == "label":
            txt = ("".join(self.label_text_buf)).strip()
            if self.current_label_for and txt:
                self.label_for_map[self.current_label_for] = txt
            self.collect_label_text = False
            self.current_label_for = None
            self.label_text_buf = []
        elif tag.lower() == "option":
            label_txt = (" ".join(self.option_text_buf)).strip()
            if self.current_select_id and self.current_opt_value is not None:
                # skip placeholder empty value
                if str(self.current_opt_value).strip() != "":
                    self.select_to_options[self.current_select_id].append({
                        "value": str(self.current_opt_value).strip(),
                        "label": label_txt,
                    })
            self.current_opt_value = None
            self.option_text_buf = []
        elif tag.lower() == "select":
            self.current_select_id = None

    def handle_data(self, data):
        if self.collect_label_text:
            self.label_text_buf.append(data)
        if self.current_opt_value is not None:
            self.option_text_buf.append(data)

def extract_variations_clean_json(out_dir: Path) -> Optional[Path]:
    """
    Reads 'variations_html.html' in out_dir and writes 'variations_cleaned.json' with:
      { "variations": [ { "id": <select_id>, "title": <label>, "options": [ { "value", "label" }, ... ] }, ... ] }
    Only includes actual option values (skips placeholder empty value).
    """
    html_path = out_dir / "variations_html.html"
    if not html_path.exists():
        return None
    try:
        raw = html_path.read_text(encoding="utf-8")
    except Exception:
        return None

    parser = VariationsHTMLParser()
    try:
        parser.feed(raw)
    except Exception:
        # tolerate minor HTML issues; attempt a best-effort parse with simple cleanup
        cleaned = re.sub(r"(?is)<script.*?</script>", "", raw)
        parser = VariationsHTMLParser()
        parser.feed(cleaned)

    variations = []
    for sel_id, options in parser.select_to_options.items():
        title = (parser.label_for_map.get(sel_id)
                 or parser.select_to_optgroup_label.get(sel_id)
                 or sel_id) or ""
        title = str(title).strip()
        # Normalize option labels a bit: collapse whitespace
        normalized_options = []
        for opt in options:
            lbl = re.sub(r"\s+", " ", opt.get("label") or "").strip()
            val = str(opt.get("value") or "").strip()
            if val != "":
                normalized_options.append({"value": val, "label": lbl})
        variations.append({
            "id": sel_id,
            "title": title,
            "options": normalized_options,
        })

    out_json = out_dir / "variations_cleaned.json"
    try:
        write_file_text(out_json, json.dumps({"variations": variations}, ensure_ascii=False, indent=2))
    except Exception as e:
        write_file_text(out_dir / "variations_clean_error.txt", str(e))
        return None
    return out_json

def discover_listingcards_html_files(outputs_dir: Path) -> List[Path]:
    try:
        files = sorted((outputs_dir or Path()).glob("listingcards_html_*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
        return files
    except Exception:
        return []

def build_listing_image_map_from_html(html_str: str) -> Dict[int, Dict[str, Optional[str]]]:
    """
    For each listing card block, extract:
    - listing_id: from data-listing-id
    - image_url: prefer data-preload-lp-src, fallback to <img src>
    - srcset: raw srcset string if present
    - shop_id: from data-shop-id if present
    """
    mapping: Dict[int, Dict[str, Optional[str]]] = {}
    if not html_str:
        return mapping

    try:
        # Split by top-level listing blocks
        li_blocks = re.findall(r"<li[^>]*>.*?</li>", html_str, flags=re.DOTALL | re.IGNORECASE)
        for block in li_blocks:
            m_id = re.search(r'data-listing-id="(\d+)"', block, flags=re.IGNORECASE)
            if not m_id:
                continue
            try:
                lid = int(m_id.group(1))
            except Exception:
                continue

            # shop id from the listing card container
            m_shop = re.search(r'data-shop-id="(\d+)"', block, flags=re.IGNORECASE)
            shop_id = m_shop.group(1) if m_shop else None

            # Prefer preload (larger image); fallback to src
            m_preload = re.search(r'data-preload-lp-src="([^"]+)"', block, flags=re.IGNORECASE)
            image_url = m_preload.group(1) if m_preload else None
            if not image_url:
                m_src = re.search(r'<img[^>]+src="([^"]+)"', block, flags=re.IGNORECASE)
                image_url = m_src.group(1) if m_src else None

            m_srcset = re.search(r'srcset="([^"]+)"', block, flags=re.IGNORECASE)
            srcset = m_srcset.group(1) if m_srcset else None

            if image_url:
                mapping[lid] = {
                    "image_url": image_url,
                    "srcset": srcset,
                    "shop_id": shop_id,
                }
    except Exception:
        pass

    return mapping

def resolve_primary_image_for_listing(listing_id: int, outputs_dir: Path) -> Optional[Dict[str, Optional[str]]]:
    """
    Scan all listingcards_html files in outputs_dir and return the image info for listing_id.
    """
    try:
        for f in discover_listingcards_html_files(outputs_dir):
            raw = read_file_text(f)
            m = build_listing_image_map_from_html(raw)
            info = m.get(int(listing_id))
            if info and info.get("image_url"):
                info = dict(info)
                info["source_file"] = str(f)
                return info
    except Exception:
        return None
    return None

def process_listing_demand(listing_id: int, run_dir: Path, outputs_dir: Path) -> Dict:
    # Route using is_personalizable from popular_now_listings_pdf.json
    is_personalizable = resolve_is_personalizable_for_listing(int(listing_id), Path(outputs_dir))
    group_type = "personalise" if is_personalizable else "non_personalise"
    group_num = 1  # fixed per user request

    # Resolve cURL template paths
    try:
        seq = resolve_cart_sequence_paths(is_personalizable)
        gettingcart_path = seq["gettingcart"]
        listing_path = seq["listing"]
        removingcart_path = seq["removingcart"]
        variations_path = seq["variations"]  # optional
    except Exception as e:
        raise RuntimeError(f"Cart sequence files not found: {e}")

    # Prepare output base for this listing and reuse everywhere
    out_base = ensure_run_output_for_group(run_dir, int(listing_id), group_num, group_type)
    ensure_dir(out_base)

    # Save originals for visibility
    try:
        write_file_text(out_base / "gettingcart_cmd.txt", read_file_text(gettingcart_path))
        write_file_text(out_base / "listing_cmd.txt", read_file_text(listing_path))
        write_file_text(out_base / "removingcart_cmd_template.txt", read_file_text(removingcart_path))
        if variations_path:
            write_file_text(out_base / "variations_cmd.txt", read_file_text(variations_path))
    except Exception as e:
        write_file_text(out_base / "original_commands_read_error.txt", str(e))

    # Reset cookie jar to avoid cache bleed
    clear_cookie_jar(DEFAULT_COOKIE_JAR)

    summary: Dict = {
        "listing_id": int(listing_id),
        "group_type": group_type,
        "group_num": group_num,
        "cart_id": None,
        "inventory_id": None,
        "customization_id": None,
        "scarcity_title": None,
        "demand_value": None,
        "steps": {},
        "out_dir": str(out_base),
    }

    # Step 1: gettingcart — inject listing id, send with cookie jar, parse values
    try:
        add_result = run_add_to_cart(gettingcart_path, int(listing_id), DEFAULT_COOKIE_JAR, out_base)
        exit_code = None
        try:
            s = read_file_text(out_base / "gettingcart_exitcode.txt").strip()
            exit_code = int(s) if s.isdigit() else None
        except Exception:
            exit_code = None
        summary["steps"]["gettingcart"] = {"exit_code": exit_code}

        if add_result is None:
            write_file_text(out_base / "gettingcart_failed.txt", "Add-to-cart failed or values not parsed.")
        else:
            cart_id, inventory_id = add_result
            summary["cart_id"] = cart_id
            summary["inventory_id"] = inventory_id
            try:
                raw = read_file_text(out_base / "gettingcart_stdout.json")
                cust_id = extract_new_customization_id_from_add_to_cart(raw)
            except Exception:
                cust_id = None
            summary["customization_id"] = cust_id
            if isinstance(cust_id, int) and cust_id > 0:
                write_file_text(out_base / "customization_id.txt", str(cust_id))
    except Exception as e:
        write_file_text(out_base / "gettingcart_unexpected_error.txt", str(e))
        summary["steps"]["gettingcart"] = {"error": str(e)}

    # Fallback: inventory id via cart-list/<cart_id> for non_personalise when missing
    try:
        inv_candidate = summary.get("inventory_id")
        if not is_personalizable and (not isinstance(inv_candidate, int) or inv_candidate <= 0):
            cid = summary.get("cart_id")
            if isinstance(cid, int) and cid > 0:
                inv2 = fetch_inventory_id_from_cart_list(
                    cart_id=cid,
                    listing_id=int(listing_id),
                    jar_path=DEFAULT_COOKIE_JAR,
                    out_dir=out_base,
                )
                if isinstance(inv2, int) and inv2 > 0:
                    summary["inventory_id"] = inv2
                    write_file_text(out_base / "inventory_id_from_cart_list.txt", str(inv2))
    except Exception as e:
        write_file_text(out_base / "inventory_id_fallback_error.txt", str(e))

    # Step 2: listing — modify listing/<id>, run, and extract demand/signal
    try:
        mod_listing = modify_listing_command(read_file_text(listing_path), int(listing_id))
        # Inject cookie jar and ensure --location so redirects are followed
        mod_listing = inject_cookie_jar(mod_listing, DEFAULT_COOKIE_JAR, read=True, write=True)
        write_file_text(out_base / "listing_modified_curl.txt", mod_listing)
        write_file_text(out_base / "listing_sent_curl.txt", mod_listing)
        code, out, err = run_shell_command(mod_listing)
        write_file_text(out_base / "listing_stdout.txt", out)
        try:
            write_file_text(out_base / "listing_stdout.json", json.dumps(json.loads(out), ensure_ascii=False, indent=2))
        except Exception:
            pass
        write_file_text(out_base / "listing_stderr.txt", err if err else "")
        write_file_text(out_base / "listing_exit.txt", str(code))
        summary["steps"]["listing"] = {"exit_code": code}

        # If sale/promotion info exists for this product, write a focused JSON
        try:
            obj = json.loads(out)
            arr = None
            if isinstance(obj, dict):
                arr = obj.get("listings")
                if not arr:
                    for ck in ("data", "payload", "event_payload", "response"):
                        cont = obj.get(ck)
                        if isinstance(cont, dict):
                            arr = cont.get("listings") or cont.get("cart_listings") or cont.get("line_items") or cont.get("items")
                            if arr:
                                break
            cand = None
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, dict):
                        lid = it.get("listing_id") or it.get("listingId") or it.get("listing")
                        try:
                            if int(lid) == int(listing_id):
                                cand = it
                                break
                        except Exception:
                            pass
            if not cand and isinstance(obj, dict):
                cand = obj
            sale_keys = [
                "subtotal_after_discount",
                "active_promotion",
                "original_price",
                "sale_price_presentation_discount_enabled",
                "sale_price_presentation_is_gs",
                "gamed_sales_v3_enabled",
                "is_eligible_for_disable_countdown_signal",
                "estimated_delivery_date",
                "free_shipping",
            ]
            if isinstance(cand, dict):
                sale_obj = {k: cand.get(k) for k in sale_keys if k in cand}
                if sale_obj and (sale_obj.get("active_promotion") or "subtotal_after_discount" in sale_obj or "original_price" in sale_obj):
                    write_file_text(out_base / "listing_sale_info.json", json.dumps(sale_obj, ensure_ascii=False, indent=2))
        except Exception:
            pass

        scarcity_title, demand_value = parse_listing_json_and_extract(out, out_base)
        summary["scarcity_title"] = scarcity_title
        summary["demand_value"] = demand_value

        # Prefer inventory id from the sidebar JSON when non-personalise
        if not is_personalizable:
            try:
                inv3 = extract_inventory_id_from_listing_stdout(out, int(listing_id), out_base)
                if isinstance(inv3, int) and inv3 > 0:
                    summary["inventory_id"] = inv3
                    write_file_text(out_base / "inventory_id.txt", str(inv3))
            except Exception as e:
                write_file_text(out_base / "inventory_id_from_listing_stdout_error.txt", str(e))
        # NEW: attach primary image URL from listingcards HTML
        try:
            img_info = resolve_primary_image_for_listing(int(listing_id), Path(outputs_dir))
            if img_info and img_info.get("image_url"):
                payload = {
                    "listing_id": int(listing_id),
                    "image_url": img_info.get("image_url"),
                    "srcset": img_info.get("srcset"),
                    "shop_id": img_info.get("shop_id"),
                    "source_html": img_info.get("source_file"),
                }
                write_file_text(out_base / "listingcard_primary_image.json", json.dumps(payload, ensure_ascii=False, indent=2))
                summary.setdefault("steps", {}).setdefault("listing_card_image", {})["present"] = True
                summary["steps"]["listing_card_image"]["file"] = "listingcard_primary_image.json"
            else:
                summary.setdefault("steps", {}).setdefault("listing_card_image", {})["present"] = False
        except Exception as e:
            write_file_text(out_base / "listing_card_image_error.txt", str(e))
            summary.setdefault("steps", {}).setdefault("listing_card_image", {})["error"] = str(e)
    except Exception as e:
        write_file_text(out_base / "listing_unexpected_error.txt", str(e))
        summary["steps"]["listing"] = {"error": str(e)}

    # Step 3 (Personalise only): variations
    if is_personalizable and variations_path:
        try:
            html = run_variations_request(variations_path, int(summary.get("cart_id") or 0), int(listing_id), DEFAULT_COOKIE_JAR, out_base)
            summary["steps"]["variations"] = {"present": bool(html)}
            # NEW: parse variations_html.html → variations_cleaned.json
            try:
                cleaned_path = extract_variations_clean_json(out_base)
                if cleaned_path:
                    summary["steps"]["variations"]["cleaned_json"] = str(cleaned_path.name)
            except Exception as e:
                write_file_text(out_base / "variations_clean_error.txt", str(e))
        except Exception as e:
            write_file_text(out_base / "variations_unexpected_error.txt", str(e))
            summary["steps"]["variations"] = {"error": str(e)}

    # Step 4: removingcart — personalise vs non_personalise behavior
    try:
        run_remove_from_cart(
            removingcart_path=removingcart_path,
            listing_id=int(listing_id),
            cart_id=int(summary.get("cart_id") or 0),
            inventory_id=summary.get("inventory_id"),
            jar_path=DEFAULT_COOKIE_JAR,
            out_dir=out_base,
            customization_id=summary.get("customization_id"),
            is_personalizable=is_personalizable,
        )
    except Exception as e:
        write_file_text(out_base / "removingcart_unexpected_error.txt", str(e))
        summary["steps"]["removingcart"] = {"error": str(e)}
    else:
        # Capture exit code
        try:
            s = read_file_text(out_base / "removingcart_exitcode.txt").strip()
            ex = int(s) if s.isdigit() else None
        except Exception:
            ex = None
        summary["steps"]["removingcart"] = {"exit_code": ex}

        # NEW: Verify removal by re-running listing.txt — must return "null"/no inventory presence
        try:
            ok = verify_cart_empty_post_removal(listing_path, int(listing_id), out_base)
            summary.setdefault("steps", {}).setdefault("verify_post_remove", {})["ok"] = bool(ok)
            if not ok:
                stop_obj = {
                    "error": "even after removing product still in cart",
                    "listing_id": int(listing_id),
                    "group_type": group_type,
                    "group_num": group_num,
                    "out_dir": str(out_base),
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }
                write_file_text(Path(outputs_dir) / "fatal_cart_residual_error.json", json.dumps(stop_obj, ensure_ascii=False, indent=2))
                # Raise to signal the consumer to stop
                raise RuntimeError("even after removing product still in cart")
        except Exception as e:
            write_file_text(out_base / "verify_post_remove_error.txt", str(e))

    # Final combined JSON with product + demand + all commands
    save_combined_run_json(out_base, summary, Path(outputs_dir))
    return summary

def init_second_queue(second_queue_path: Path, source_queue_path: Path, user_label: str) -> None:
    payload = {
        "queue_name": "Second Etsy Demand Extractor Queue",
        "status": "running",
        "user": user_label,
        "source_queue_path": str(source_queue_path),
        "started_at": datetime.utcnow().isoformat() + "Z",
        "processed_count": 0,
        "items": [],
    }
    write_file_text(second_queue_path, json.dumps(payload, ensure_ascii=False, indent=2))

def append_to_second_queue(second_queue_path: Path, item: Dict) -> int:
    try:
        obj = json.loads(read_file_text(second_queue_path))
    except Exception:
        obj = {
            "queue_name": "Second Etsy Demand Extractor Queue",
            "status": "running",
            "user": Path.home().name,
            "source_queue_path": "",
            "started_at": datetime.utcnow().isoformat() + "Z",
            "processed_count": 0,
            "items": [],
        }
    item = dict(item)
    item["when"] = datetime.utcnow().isoformat() + "Z"
    items = obj.get("items") or []
    items.append(item)
    obj["items"] = items
    obj["processed_count"] = len(items)
    write_file_text(second_queue_path, json.dumps(obj, ensure_ascii=False, indent=2))
    return obj["processed_count"]

def finalize_second_queue(second_queue_path: Path, total_count: int, destroy: bool = True) -> None:
    try:
        obj = json.loads(read_file_text(second_queue_path))
    except Exception:
        obj = {"queue_name": "Second Etsy Demand Extractor Queue"}
    obj["status"] = "completed"
    obj["completed_at"] = datetime.utcnow().isoformat() + "Z"
    obj["total_count"] = int(total_count)
    write_file_text(second_queue_path, json.dumps(obj, ensure_ascii=False, indent=2))
    if destroy and Path(second_queue_path).exists():
        try:
            Path(second_queue_path).unlink()
        except Exception:
            pass

def consume_popular_queue(queue_path: Path, run_dir: Path, outputs_dir: Path, poll_interval: float = 0.5, popular_listings_path: Optional[Path] = None, progress_cb: Optional[callable] = None) -> None:
    """
    Consume the real-time Popular Products Queue and process listings one-by-one
    as soon as they appear. Continues until the queue is marked completed and
    all items are processed. Writes a local processed log to avoid duplicates.
    If popular_listings_path is provided, only process listing IDs present in that file.
    """
    ensure_dir(outputs_dir)
    processed_log = Path(outputs_dir) / "popular_queue_processed_ids.json"
    # Second Etsy Demand Extractor Queue lifecycle file
    second_queue_path = Path(outputs_dir) / "second_demand_extractor_queue.json"
    user_label = Path.home().name  # which user's queue (without adding imports)
    init_second_queue(second_queue_path, Path(queue_path), user_label)

    # Build allowlist from popular listings file (if provided)
    allowed_ids: Optional[set[int]] = None
    try:
        if popular_listings_path and Path(popular_listings_path).exists():
            obj = json.loads(read_file_text(Path(popular_listings_path)))
            ids = obj.get("popular_now_ids") or []
            if not ids:
                # fallback: derive from listings if IDs aren’t present
                for it in (obj.get("listings") or []):
                    lid = it.get("listing_id") or it.get("listingId")
                    try:
                        ids.append(int(str(lid)))
                    except Exception:
                        continue
            allowed = set()
            for lid in ids:
                try:
                    allowed.add(int(str(lid)))
                except Exception:
                    continue
            allowed_ids = allowed if allowed else None
    except Exception:
        allowed_ids = None

    print(
        f"Second Etsy Demand Extractor Queue STARTED\n"
        f"  user: {user_label}\n"
        f"  source_queue: {Path(queue_path)}\n"
        f"  outputs_dir: {outputs_dir}\n"
        f"  allowed_ids_from_popular: {len(allowed_ids) if allowed_ids is not None else 'ALL'}"
    )

    # Load already processed IDs to avoid duplicate work across restarts
    processed: set = set()
    try:
        if processed_log.exists():
            obj = json.loads(read_file_text(processed_log))
            arr = obj.get("processed_ids") or []
            for lid in arr:
                try:
                    processed.add(int(lid))
                except Exception:
                    continue
    except Exception:
        processed = set()

    seen_completed = False
    qpath = Path(queue_path)

    while True:
        try:
            if qpath.exists():
                obj = json.loads(read_file_text(qpath))
                items = obj.get("items") or []
                status = obj.get("status") or "running"

                total_in_source = len(items)
                remaining_before = sum(
                    1 for it in items
                    if isinstance(it, dict)
                    and str(it.get("listing_id", "")).isdigit()
                    and int(str(it.get("listing_id"))) not in processed
                )

                # Fast-complete when source queue is completed and nothing remains
                if status == "completed" and remaining_before == 0:
                    finalize_second_queue(second_queue_path, total_count=len(processed), destroy=True)
                    # Emit final demand extraction progress (0 remaining)
                    try:
                        if progress_cb:
                            progress_cb({
                                "stage": "demand_extraction",
                                "user_id": user_label,
                                "remaining": 0,
                                "total": total_in_source,
                                "message": "Demand extraction completed",
                            })
                    except Exception:
                        pass

                print(
                    f"Second Etsy Demand Extractor Queue OBSERVE\n"
                    f"  user: {user_label}\n"
                    f"  source_total: {total_in_source}\n"
                    f"  remaining_before: {remaining_before}\n"
                    f"  processed_total: {len(processed)}"
                )

                # Emit observe demand extraction snapshot
                try:
                    if progress_cb:
                        progress_cb({
                            "stage": "demand_extraction",
                            "user_id": user_label,
                            "remaining": remaining_before,
                            "total": total_in_source,
                            "message": "Demand extraction observing queue",
                        })
                except Exception:
                    pass

                for it in items:
                    lid = it.get("listing_id")
                    try:
                        lid_i = int(str(lid))
                    except Exception:
                        continue
                    if lid_i in processed:
                        continue

                    # Skip non-popular IDs if allowlist is present
                    if allowed_ids is not None and lid_i not in allowed_ids:
                        print(
                            f"Second Etsy Demand Extractor Queue SKIP (not in popular)\n"
                            f"  user: {user_label}\n"
                            f"  listing_id: {lid_i}"
                        )
                        processed.add(lid_i)
                        write_file_text(processed_log, json.dumps({"processed_ids": sorted(list(processed))}, ensure_ascii=False, indent=2))
                        # Emit progress after skip
                        try:
                            if progress_cb:
                                rem = sum(
                                    1 for jt in items
                                    if isinstance(jt, dict)
                                    and str(jt.get("listing_id", "")).isdigit()
                                    and int(str(jt.get("listing_id"))) not in processed
                                )
                                progress_cb({
                                    "stage": "demand_extraction",
                                    "user_id": user_label,
                                    "remaining": rem,
                                    "total": total_in_source,
                                    "message": f"Skipped non-popular listing_id={lid_i}",
                                })
                        except Exception:
                            pass
                        continue

                    print(
                        f"Second Etsy Demand Extractor Queue new_added=1\n"
                        f"  user: {user_label}\n"
                        f"  listing_id: {lid_i}\n"
                        f"  source_total: {total_in_source}\n"
                        f"  remaining_before: {remaining_before}"
                    )

                    # Process listing and enrich logs
                    try:
                        summary = process_listing_demand(lid_i, Path(run_dir), Path(outputs_dir))
                        processed.add(lid_i)
                        write_file_text(processed_log, json.dumps({"processed_ids": sorted(list(processed))}, ensure_ascii=False, indent=2))

                        scarcity_title = summary.get("scarcity_title")
                        demand_value = summary.get("demand_value")
                        cart_id = summary.get("cart_id")
                        inventory_id = summary.get("inventory_id")
                        group_type = summary.get("group_type")
                        group_num = summary.get("group_num")
                        # IMPORTANT: reuse the same out_dir created by process_listing_demand
                        out_dir_str = summary.get("out_dir")
                        out_dir = Path(out_dir_str) if out_dir_str else ensure_run_output_for_group(Path(run_dir), lid_i, int(group_num or 1), str(group_type or "non_personalise"))

                        append_to_second_queue(
                            second_queue_path,
                            {
                                "listing_id": lid_i,
                                "out_dir": str(out_dir),
                                "scarcity_title": scarcity_title,
                                "demand_value": demand_value,
                                "cart_id": cart_id,
                                "inventory_id": inventory_id,
                                "group_type": group_type,
                                "group_num": group_num,
                            },
                        )

                        remaining_after = sum(
                            1 for jt in items
                            if isinstance(jt, dict)
                            and str(jt.get("listing_id", "")).isdigit()
                            and int(str(jt.get("listing_id"))) not in processed
                        )

                        # Emit demand extraction progress after each processed listing
                        try:
                            if progress_cb:
                                progress_cb({
                                    "stage": "demand_extraction",
                                    "user_id": user_label,
                                    "remaining": remaining_after,
                                    "total": total_in_source,
                                    "message": f"Processed listing_id={lid_i} demand_value={demand_value}",
                                })
                        except Exception:
                            pass

                        print(
                            f"Second Etsy Demand Extractor Queue DONE\n"
                            f"  user: {user_label}\n"
                            f"  listing_id: {lid_i}\n"
                            f"  demand_value: {demand_value}\n"
                            f"  scarcity_title: {scarcity_title}\n"
                            f"  cart_id: {cart_id}\n"
                            f"  inventory_id: {inventory_id}\n"
                            f"  group: {group_type}/{group_num}\n"
                            f"  out_dir: {out_dir}\n"
                            f"  remaining_after: {remaining_after}\n"
                            f"  processed_total: {len(processed)}"
                        )

                        # Schedule background upload of per-listing cart_runs directory and cleanup
                        try:
                            schedule_background_upload_and_cleanup(out_dir, Path(run_dir), progress_cb=progress_cb, user_id=user_label)
                        except Exception:
                            pass
                    except Exception as e:
                        err_path = Path(outputs_dir) / f"queue_error_listing_{lid_i}.txt"
                        write_file_text(err_path, str(e))
                        print(
                            f"Second Etsy Demand Extractor Queue ERROR\n"
                            f"  user: {user_label}\n"
                            f"  listing_id: {lid_i}\n"
                            f"  error: {e}"
                        )
                        # Emit a user-facing error progress snapshot
                        try:
                            if progress_cb:
                                rem = sum(
                                    1 for jt in items
                                    if isinstance(jt, dict)
                                    and str(jt.get("listing_id", "")).isdigit()
                                    and int(str(jt.get("listing_id"))) not in processed
                                )
                                progress_cb({
                                    "stage": "demand_extraction_error",
                                    "user_id": user_label,
                                    "remaining": rem,
                                    "total": total_in_source,
                                    "listing_id": lid_i,
                                    "message": f"Error processing listing_id={lid_i}: {e}",
                                })
                        except Exception:
                            pass
                        # NEW: Fatal stop on residual cart error
                        if "still in cart" in str(e):
                            mark_second_queue_error(second_queue_path, str(e), lid_i)
                            print("Stopping queue due to residual cart verification failure.")
                            # Also emit a fatal error progress event
                            try:
                                if progress_cb:
                                    progress_cb({
                                        "stage": "demand_extraction_error",
                                        "user_id": user_label,
                                        "remaining": rem if 'rem' in locals() else None,
                                        "total": total_in_source,
                                        "listing_id": lid_i,
                                        "message": "Fatal: residual cart after removal, stopping.",
                                    })
                            except Exception:
                                pass
                            break

                # Exit when queue is completed and all items are processed
                if status == "completed":
                    seen_completed = True
                    all_ids = []
                    for it in items:
                        try:
                            all_ids.append(int(str(it.get("listing_id"))))
                        except Exception:
                            continue
                    if all_ids and all(lid in processed for lid in all_ids):
                        finalize_second_queue(second_queue_path, total_count=len(processed), destroy=True)
                        # Emit final completion
                        try:
                            if progress_cb:
                                progress_cb({
                                    "stage": "demand_extraction",
                                    "user_id": user_label,
                                    "remaining": 0,
                                    "total": total_in_source,
                                    "message": "Demand extraction completed",
                                })
                        except Exception:
                            pass
                        print(
                            f"Second Etsy Demand Extractor Queue COMPLETED\n"
                            f"  user: {user_label}\n"
                            f"  processed_total: {len(processed)}\n"
                            f"  destroyed_queue_path: {second_queue_path}"
                        )
                        break
            time.sleep(poll_interval)
        except Exception:
            time.sleep(poll_interval)

def find_group_numbers(txt_root: Path) -> List[int]:
    required = {"gettingcart.txt", "listing.txt", "removingcart.txt"}
    groups = []
    if not txt_root.exists():
        return groups
    for child in txt_root.iterdir():
        if child.is_dir() and child.name.isdigit():
            files = {p.name for p in child.glob("*") if p.is_file()}
            if required.issubset(files):
                groups.append(int(child.name))
    groups.sort()
    return groups

def group_paths(group_num: int) -> Tuple[Path, Path, Path]:
    base = TXT_FILES_ROOT / str(group_num)
    return base / "gettingcart.txt", base / "listing.txt", base / "removingcart.txt"

def find_special_group_numbers(txt_root: Path) -> List[int]:
    required = {"gettingcart.txt", "listing.txt", "pers.txt", "removingcart.txt"}
    groups = []
    if not txt_root.exists():
        return groups
    for child in txt_root.iterdir():
        if child.is_dir() and child.name.isdigit():
            files = {p.name for p in child.glob("*") if p.is_file()}
            if required.issubset(files):
                groups.append(int(child.name))
    groups.sort()
    return groups

def special_group_paths(group_num: int) -> Tuple[Path, Path, Path, Path]:
    base = SPECIAL_ROOT / str(group_num)
    return base / "gettingcart.txt", base / "listing.txt", base / "pers.txt", base / "removingcart.txt"

# ===== Command modifiers =====

def modify_gettingcart_command(original_cmd: str, listing_id: int) -> str:
    cmd = original_cmd
    json_pattern = r"--data(?:-raw|-binary)?\s+(?P<quote>['\"])(?P<json>\{.*?\})(?P=quote)"
    matches = list(re.finditer(json_pattern, cmd, flags=re.DOTALL))
    if not matches:
        raise ValueError("Could not find JSON payload in gettingcart.txt curl command")
    m = matches[-1]
    json_str = m.group('json')
    try:
        payload = json.loads(json_str)
    except json.JSONDecodeError:
        payload = json.loads(html_lib.unescape(json_str))
    payload["listing_id"] = int(listing_id)
    new_json_str = json.dumps(payload, separators=(",", ":"))
    return cmd[:m.start('json')] + new_json_str + cmd[m.end('json'):]

def _replace_prefix_number(text: str, prefix: str, number: int, max_count: int = 2) -> str:
    pattern = re.compile(rf"({re.escape(prefix)})\d+")
    return pattern.sub(lambda m: m.group(1) + str(number), text, count=max_count)

def modify_pers_command(original_cmd: str, cart_id: int, listing_id: int) -> str:
    cmd = original_cmd
    cmd = _replace_prefix_number(cmd, "cart-list/", cart_id, max_count=2)
    cmd = _replace_prefix_number(cmd, "cart/", cart_id, max_count=2)
    cmd = _replace_prefix_number(cmd, "listing/", listing_id, max_count=1)
    return cmd

# ===== Cookie jar =====

def clear_cookie_jar(jar_path: Path):
    rm_file_if_exists(jar_path)

def read_cookie_jar(jar_path: Path) -> dict:
    jar_cookies = {}
    try:
        with open(jar_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split("\t")
                if len(parts) >= 7:
                    name = parts[5]
                    value = parts[6]
                    jar_cookies[name] = value
    except Exception:
        pass
    return jar_cookies

# ===== JSON parsing for demand =====

# extract_cart_id_from_add_to_cart()
def extract_cart_id_from_add_to_cart(stdout: str) -> Optional[int]:
    """
    Parse cart_id from add-to-cart stdout.
    Handles top-level and nested keys with both snake_case ('cart_id') and camelCase ('cartId').
    Falls back to a regex scan if JSON loading fails.
    """
    def coerce_int(v) -> Optional[int]:
        try:
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                s = v.strip()
                return int(s) if s.isdigit() else None
        except Exception:
            pass
        return None

    def lookup_in_container(container: dict) -> Optional[int]:
        if not isinstance(container, dict):
            return None
        # Direct keys
        for key in ("cartId", "cart_id", "id"):
            cid = coerce_int(container.get(key))
            if isinstance(cid, int) and cid > 0:
                return cid
        # Nested 'cart' object (common in some shapes)
        cart = container.get("cart")
        if isinstance(cart, dict):
            for key in ("cartId", "cart_id", "id"):
                cid = coerce_int(cart.get(key))
                if isinstance(cid, int) and cid > 0:
                    return cid
        return None

    obj = None
    try:
        obj = json.loads(stdout)
    except json.JSONDecodeError:
        try:
            obj = json.loads(html_lib.unescape(stdout))
        except Exception:
            obj = None

    if isinstance(obj, dict):
        # Top-level lookup
        cid = lookup_in_container(obj)
        if isinstance(cid, int) and cid > 0:
            return cid
        # Common nested containers
        for container_key in ("data", "payload", "event_payload", "response"):
            cont = obj.get(container_key)
            cid = lookup_in_container(cont)
            if isinstance(cid, int) and cid > 0:
                return cid

    # Regex fallback on raw text
    for pat in (r'"cartId"\s*:\s*(\d+)', r'"cart_id"\s*:\s*(\d+)'):
        m = re.search(pat, stdout)
        if m:
            try:
                val = int(m.group(1))
                return val if val > 0 else None
            except Exception:
                continue

    return None

def extract_scarcity_signal_title_from_json(obj: dict) -> Optional[str]:
    """
    Extract a human-readable scarcity signal title from various JSON shapes.
    Supports:
      - Top-level 'scarcity_signal_title'
      - Nested containers: 'data', 'cart', 'payload', 'event_payload', 'response'
      - Array paths: 'listings', 'cart_listings', 'line_items', 'items'
      - 'signals' arrays containing dicts with 'title'
    Returns the first non-empty title found, or None.
    """
    def maybe_title(val) -> Optional[str]:
        if isinstance(val, str):
            t = val.strip()
            if t:
                return t
        return None

    if not isinstance(obj, dict):
        return None

    # Direct at root
    t = maybe_title(obj.get("scarcity_signal_title"))
    if t:
        return t

    # Direct signals at root
    sigs = obj.get("signals")
    if isinstance(sigs, list):
        for s in sigs:
            if isinstance(s, dict):
                t = maybe_title(s.get("title"))
                if t:
                    return t

    # Known nested containers to inspect
    for container_key in ("data", "cart", "payload", "event_payload", "response", "meta"):
        container = obj.get(container_key)
        if not isinstance(container, dict):
            continue
        # Direct key
        t = maybe_title(container.get("scarcity_signal_title"))
        if t:
            return t

        # Signals within container
        inner_sigs = container.get("signals")
        if isinstance(inner_sigs, list):
            for s in inner_sigs:
                if isinstance(s, dict):
                    t = maybe_title(s.get("title"))
                    if t:
                        return t

        # Listings-style arrays within this container
        for list_key in ("listings", "cart_listings", "line_items", "items"):
            arr = container.get(list_key)
            if isinstance(arr, list):
                for it in arr:
                    if not isinstance(it, dict):
                        continue
                    t = maybe_title(it.get("scarcity_signal_title"))
                    if t:
                        return t
                    # Some shapes place signals under each item
                    it_sigs = it.get("signals")
                    if isinstance(it_sigs, list):
                        for s in it_sigs:
                            if isinstance(s, dict):
                                t = maybe_title(s.get("title"))
                                if t:
                                    return t

    # Fallback: listings-style arrays at the root
    for list_key in ("listings", "cart_listings", "line_items", "items"):
        arr = obj.get(list_key)
        if isinstance(arr, list):
            for it in arr:
                if not isinstance(it, dict):
                    continue
                t = maybe_title(it.get("scarcity_signal_title"))
                if t:
                    return t
                it_sigs = it.get("signals")
                if isinstance(it_sigs, list):
                    for s in it_sigs:
                        if isinstance(s, dict):
                            t = maybe_title(s.get("title"))
                            if t:
                                return t

    return None

def parse_demand_from_scarcity_title(title: str) -> Optional[int]:
    """
    Parse an integer 'demand' from scarcity titles like:
      - 'In 39 carts, 3 bought in the past 24 hours'
      - '3 sold in the past 24 hours'
    Returns the first matched integer or None.
    """
    if not isinstance(title, str):
        return None
    t = title.strip()
    if not t:
        return None

    patterns = [
        r"In\s+\d+\s+carts?,\s*(\d+)\s+(?:bought|sold|orders?|purchases?|purchased|buyers?)\s+in\s+(?:the\s+)?(?:past|last)\s+24\s+hours",
        r"(\d+)\s+(?:bought|sold|orders?|purchases?|purchased|buyers?)\s+in\s+(?:the\s+)?(?:past|last)\s+24\s+hours",
        r"(\d+)\s+(?:sold|orders?)\s+today",
    ]
    for pat in patterns:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    # Fallback: if the phrase mentions sales/purchases, prefer the last number (e.g., 'In 39 carts, 3 bought...')
    try:
        if re.search(r"\b(sold|bought|orders?|purchases?|purchased|buyers?)\b", t, flags=re.IGNORECASE):
            nums = re.findall(r"\d+", t)
            if nums:
                return int(nums[-1])
    except Exception:
        pass
    return None

# ===== Output organization =====

def ensure_demand_output_dir(run_dir: Path, listing_id: int) -> Path:
    ts = now_ts_compact()
    base = run_dir / "demand extracted" / f"{listing_id}" / ts
    base.mkdir(parents=True, exist_ok=True)
    return base

def ensure_listing_output_dir(run_dir: Path, listing_id: int) -> Path:
    base = run_dir / "listing_runs" / f"{listing_id}" / now_ts_compact()
    base.mkdir(parents=True, exist_ok=True)
    return base

def ensure_run_output_for_group(run_dir: Path, listing_id: int, group_num: int, group_type: str) -> Path:
    base = run_dir / "cart_runs" / f"group_{group_type}_{group_num}" / str(listing_id) / now_ts_compact()
    # Ensure local dir exists for fast local-first writes and later staging
    ensure_dir(base)
    return base

# Helper: decide if a listing is personalizable using the latest popular JSON
def resolve_is_personalizable_for_listing(listing_id: int, outputs_dir: Path) -> bool:
    try:
        popular_path = discover_popular_json(outputs_dir)
        info_map = load_source_listing_info_map(popular_path)
        obj = info_map.get(int(listing_id)) or {}
        val = obj.get("is_personalizable")
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            v = val.strip().lower()
            if v in ("true", "1", "yes", "y"):
                return True
            if v in ("false", "0", "no", "n"):
                return False
        if isinstance(val, (int, float)):
            return int(val) != 0
    except Exception:
        pass
    return False

# Helper: locate cart sequence cURL files based on personalizable flag
def resolve_cart_sequence_paths(is_personalizable: bool) -> Dict[str, Optional[Path]]:
    base = TXT_FILES_ROOT / "1" / ("Personalise" if is_personalizable else "Non_personalise")
    paths: Dict[str, Optional[Path]] = {}
    required = ["gettingcart", "listing", "removingcart"]
    for name in required:
        p = base / f"{name}.txt"
        if not p.exists():
            raise FileNotFoundError(f"Missing {name}.txt at {p}")
        paths[name] = p

    # variations only in Personalise; optional
    var_p = base / "variations.txt"
    paths["variations"] = var_p if var_p.exists() else None
    return paths

# Persist all per-run info (commands + outputs + product source) in one JSON
def save_combined_run_json(out_dir: Path, summary: Dict, outputs_dir: Path) -> Path:
    def read_if_exists(p: Path) -> Optional[str]:
        try:
            # Attempt Supabase read for cart_runs unconditionally; tolerate missing
            if "cart_runs" in p.parts:
                return read_file_text(p)
            return p.read_text(encoding="utf-8") if p.exists() else None
        except Exception:
            return None

    combined: Dict[str, Optional[object]] = {
        "meta": {
            "listing_id": summary.get("listing_id"),
            "group_type": summary.get("group_type"),
            "group_num": summary.get("group_num"),
            "out_dir": str(out_dir),
        },
        "ids": {
            "cart_id": summary.get("cart_id"),
            "inventory_id": summary.get("inventory_id"),
            "customization_id": summary.get("customization_id"),
        },
        "commands": {
            "gettingcart_modified_curl": read_if_exists(out_dir / "gettingcart_modified.txt") or read_if_exists(out_dir / "gettingcart_modified_curl.txt"),
            "gettingcart_sent_with_cookiejar": read_if_exists(out_dir / "gettingcart_sent_with_cookiejar.txt"),
            "listing_modified_curl": read_if_exists(out_dir / "listing_modified_curl.txt"),
            "listing_sent_curl": read_if_exists(out_dir / "listing_sent_curl.txt"),
            "variations_modified_curl": read_if_exists(out_dir / "variations_modified_curl.txt"),
            "variations_sent_with_cookiejar": read_if_exists(out_dir / "variations_sent_with_cookiejar.txt"),
            "removingcart_modified_curl": read_if_exists(out_dir / "removingcart_modified_curl.txt"),
            "removingcart_sent_with_cookiejar": read_if_exists(out_dir / "removingcart_sent_with_cookiejar.txt") or read_if_exists(out_dir / "removingcart_cmd.txt"),
        },
        "responses": {
            "gettingcart_stdout_json": read_if_exists(out_dir / "gettingcart_stdout.json"),
            "listing_stdout_json": read_if_exists(out_dir / "listing_stdout.txt"),
            "variations_html": read_if_exists(out_dir / "variations_html.html"),
            "removingcart_stdout_json": read_if_exists(out_dir / "removingcart_stdout.json"),
        },
        "signals": {
            "scarcity_title": summary.get("scarcity_title"),
            "demand_value": summary.get("demand_value"),
        },
        "popular_info": None,
        # NEW: structured sale info if produced by step 2
        "sale_info": None,
    }

    # Embed the full listing source object when available
    try:
        popular_path = discover_popular_json(outputs_dir)
        info_map = load_source_listing_info_map(popular_path)
        pinfo = info_map.get(int(summary.get("listing_id"))) or None
        # ... existing code ...
        combined["popular_info"] = pinfo
    except Exception:
        combined["popular_info"] = None

    # NEW: attach parsed sale info (if file exists)
    try:
        sale_path = out_dir / "listing_sale_info.json"
        if sale_path.exists():
            combined["sale_info"] = json.loads(read_file_text(sale_path))
    except Exception:
        combined["sale_info"] = None

    out_json = out_dir / "combined_demand_and_product.json"
    try:
        out_json.write_text(json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        write_file_text(out_dir / "combined_json_error.txt", str(e))
    return out_json

# ===== Listing curl request execution (debug path) =====

def execute_listing_curl_verbatim(listing_txt_path: Path, out_dir: Path) -> Tuple[int, str, str]:
    original = read_file_text(listing_txt_path)
    write_file_text(out_dir / "listing_original.txt", original)
    # Per user: listing_modified_curl.txt must be identical to listing.txt (no injection)
    write_file_text(out_dir / "listing_modified_curl.txt", original)
    write_file_text(out_dir / "listing_sent_curl.txt", original)

    rc, stdout, stderr = run_shell_command(original, timeout=120)
    write_file_text(out_dir / "listing_curl_stdout.json", stdout)
    write_file_text(out_dir / "listing_curl_stderr.txt", stderr if stderr else "")
    write_file_text(out_dir / "listing_curl_exitcode.txt", str(rc))
    return rc, stdout, stderr

# ===== Listing JSON parsing and demand extraction =====

def parse_listing_json_and_extract(stdout: str, out_dir: Path) -> Tuple[Optional[str], Optional[int]]:
    """
    Parse the listing curl JSON stdout, extract scarcity title and demand,
    and opportunistically capture 'cart_id' from common shapes.
    Writes:
      - 'scarcity_signal_title.txt'
      - 'demand_value.txt'
      - 'cart_id_from_listing_json.txt' (if found)
    """
    listing_obj = None
    try:
        listing_obj = json.loads(stdout)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", stdout, flags=re.DOTALL)
        if m:
            try:
                listing_obj = json.loads(m.group(0))
            except Exception:
                listing_obj = None

    if listing_obj is None:
        write_file_text(out_dir / "listing_json_parse_error.txt", "Could not parse JSON from listing curl stdout.")
        return None, None

    scarcity_title = extract_scarcity_signal_title_from_json(listing_obj) or ""
    write_file_text(out_dir / "scarcity_signal_title.txt", scarcity_title)

    demand_value = parse_demand_from_scarcity_title(scarcity_title)
    write_file_text(out_dir / "demand_value.txt", str(demand_value if demand_value is not None else ""))

    # Cart id extraction from multiple shapes
    cart_id: Optional[int] = None

    def coerce_int(v) -> Optional[int]:
        try:
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        except Exception:
            pass
        return None

    # Top-level 'cart' or 'meta'
    if isinstance(listing_obj, dict):
        meta = listing_obj.get("meta")
        if isinstance(meta, dict):
            cart_id = coerce_int(meta.get("cart_id") or meta.get("cartId")) or cart_id
        cart = listing_obj.get("cart")
        if isinstance(cart, dict):
            cart_id = coerce_int(cart.get("id") or cart.get("cart_id") or cart.get("cartId")) or cart_id

        # From listings array (common in your stdout sample)
        listings_arr = listing_obj.get("listings")
        if isinstance(listings_arr, list):
            for it in listings_arr:
                if isinstance(it, dict):
                    cid = coerce_int(it.get("cart_id") or it.get("cartId"))
                    if cid is not None:
                        cart_id = cid
                        break

        # Other containers that may house listings
        for container_key in ("data", "payload", "event_payload", "response"):
            cont = listing_obj.get(container_key)
            if not isinstance(cont, dict):
                continue
            arr = cont.get("listings") or cont.get("cart_listings") or cont.get("line_items") or cont.get("items")
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, dict):
                        cid = coerce_int(it.get("cart_id") or it.get("cartId"))
                        if cid is not None:
                            cart_id = cid
                            break
                if cart_id is not None:
                    break

    if cart_id is not None:
        write_file_text(out_dir / "cart_id_from_listing_json.txt", str(cart_id))

    return scarcity_title, demand_value

# NEW: Post-removal verification helpers
def run_listing_post_remove_check(listing_txt_path: Path, out_dir: Path, jar_path: Path) -> Tuple[int, str, str]:
    original = read_file_text(listing_txt_path)
    write_file_text(out_dir / "verify_post_remove_listing_original.txt", original)

    cmd_jar = inject_cookie_jar(original, jar_path, read=True, write=True)
    write_file_text(out_dir / "verify_post_remove_listing_sent_with_cookiejar.txt", cmd_jar)

    rc, stdout, stderr = run_shell_command(cmd_jar, timeout=120)
    write_file_text(out_dir / "verify_post_remove_listing_stdout.json", stdout)
    write_file_text(out_dir / "verify_post_remove_listing_stderr.txt", stderr if stderr else "")
    write_file_text(out_dir / "verify_post_remove_listing_exitcode.txt", str(rc))
    return rc, stdout, stderr

def _is_listing_in_cart_from_listing_stdout(stdout: str, listing_id: int) -> bool:
    try:
        obj = json.loads(stdout)
    except Exception:
        try:
            m = re.search(r"\{.*\}", stdout, flags=re.DOTALL)
            obj = json.loads(m.group(0)) if m else None
        except Exception:
            obj = None

    if not isinstance(obj, dict):
        return False

    def listing_in_cart(container: dict) -> bool:
        if not isinstance(container, dict):
            return False
        for key in ("cart_listings", "line_items", "items"):
            arr = container.get(key)
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, dict):
                        lid = it.get("listing_id") or it.get("listingId") or it.get("listing")
                        try:
                            lid_i = int(str(lid)) if lid is not None else None
                        except Exception:
                            lid_i = None
                        if lid_i == int(listing_id):
                            return True
        return False

    # Check top-level first
    if listing_in_cart(obj):
        return True

    # Then common nested containers
    for ckey in ("cart", "data", "payload", "event_payload", "response", "meta"):
        cont = obj.get(ckey)
        if listing_in_cart(cont):
            return True

    return False

def verify_cart_empty_post_removal(listing_txt_path: Path, listing_id: int, out_dir: Path, jar_path: Path = DEFAULT_COOKIE_JAR) -> bool:
    # Prefer authoritative cart-list check; fall back to listing JSON only if cart_id missing
    cart_id: Optional[int] = None
    for p in (out_dir / "cart_id_from_gettingcart.txt", out_dir / "cart_id_from_listing_json.txt"):
        try:
            s = read_file_text(p).strip()
            if s.isdigit():
                cart_id = int(s)
                break
        except Exception:
            continue

    if isinstance(cart_id, int) and cart_id > 0:
        inv = fetch_inventory_id_from_cart_list(cart_id, int(listing_id), jar_path, out_dir)
        # If the listing’s inventory is found in the cart, removal failed
        return not (isinstance(inv, int) and inv > 0)

    # Fallback: replay listing curl under the same session and check cart_listings
    rc, stdout, _ = run_listing_post_remove_check(listing_txt_path, out_dir, jar_path)
    if rc != 0:
        # Non-zero curl exit does not prove removal; record and treat as failure
        write_file_text(out_dir / "verify_post_remove_error.txt", f"listing curl exit {rc}")
        return False

    present_in_cart = _is_listing_in_cart_from_listing_stdout(stdout, int(listing_id))
    return not present_in_cart

def mark_second_queue_error(second_queue_path: Path, error_msg: str, listing_id: int) -> None:
    try:
        obj = json.loads(read_file_text(second_queue_path))
    except Exception:
        obj = {"queue_name": "Second Etsy Demand Extractor Queue"}
    obj["status"] = "error"
    obj["error"] = str(error_msg)
    obj["failed_listing_id"] = int(listing_id)
    obj["completed_at"] = datetime.utcnow().isoformat() + "Z"
    write_file_text(second_queue_path, json.dumps(obj, ensure_ascii=False, indent=2))

# ===== Flow steps: add → listing → remove =====

# Function: modify_removingcart_command
def modify_removingcart_command(
    original_cmd: str,
    cart_id: int,
    listing_id: int,
    inventory_id: Optional[int],
    customization_id: Optional[int] = None,
    personalizable: bool = False,
) -> str:
    """
    Personalise: only replace cart-list/<cart_id>, cart/<cart_id>, listing/<listing_id>;
    do NOT inject inventory/customization.
    Non_personalise: also replace inventory/<inventory_id> (>0 else 0), and
    set customization/0 when inventory present; otherwise customization/<cid> if >0 else 0.
    """
    cmd = original_cmd
    # Always update cart and listing
    cmd = _replace_prefix_number(cmd, "cart-list/", int(cart_id), max_count=2)
    cmd = _replace_prefix_number(cmd, "cart/", int(cart_id), max_count=2)
    cmd = _replace_prefix_number(cmd, "listing/", int(listing_id), max_count=1)

    if personalizable:
        # Leave inventory/customization segments untouched for Personalise templates
        return cmd

    # Non_personalise behavior
    inv_val = inventory_id if isinstance(inventory_id, int) and inventory_id > 0 else 0
    cmd = _replace_prefix_number(cmd, "inventory/", inv_val, max_count=1)

    if inv_val > 0:
        cust_val = 0
    else:
        cust_val = customization_id if isinstance(customization_id, int) and customization_id > 0 else 0
    cmd = _replace_prefix_number(cmd, "customization/", cust_val, max_count=1)
    return cmd

# Function: extract_inventory_id_from_add_to_cart
def extract_inventory_id_from_add_to_cart(stdout: str) -> Optional[int]:
    """
    Parse inventory id from add-to-cart stdout.
    Handles top-level keys ('inventoryId', 'inventory_id', 'listing_inventory_id') and nested variants.
    Falls back to regex if JSON loading fails.
    """
    def coerce_int(v) -> Optional[int]:
        try:
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        except Exception:
            pass
        return None

    obj = None
    try:
        obj = json.loads(stdout)
    except json.JSONDecodeError:
        pass

    # Structured parse if JSON loaded
    if isinstance(obj, dict):
        for key in ("inventoryId", "inventory_id", "listing_inventory_id"):
            iid = coerce_int(obj.get(key))
            if iid is not None:
                return iid

        # Nested under common containers
        for container_key in ("data", "payload", "event_payload", "response"):
            cont = obj.get(container_key)
            if isinstance(cont, dict):
                for key in ("inventoryId", "inventory_id", "listing_inventory_id"):
                    iid = coerce_int(cont.get(key))
                    if iid is not None:
                        return iid

    # Regex fallback
    patterns = [
        r'"inventoryId"\s*:\s*(\d+)',
        r'"inventory_id"\s*:\s*(\d+)',
        r'"listing_inventory_id"\s*:\s*(\d+)',
    ]
    for pat in patterns:
        m = re.search(pat, stdout)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass

    return None

def extract_new_customization_id_from_add_to_cart(stdout: str) -> Optional[int]:
    """
    Parse customization id from add-to-cart stdout.
    Looks for 'newCustomizationId' or 'new_customization_id' at top-level and nested containers.
    Returns None when missing or zero.
    """
    def coerce_int(v) -> Optional[int]:
        try:
            if isinstance(v, int):
                return v
            if isinstance(v, str) and v.isdigit():
                return int(v)
        except Exception:
            pass
        return None

    obj = None
    try:
        obj = json.loads(stdout)
    except json.JSONDecodeError:
        obj = None

    if isinstance(obj, dict):
        for key in ("newCustomizationId", "new_customization_id"):
            cid = coerce_int(obj.get(key))
            if isinstance(cid, int) and cid > 0:
                return cid
        for container_key in ("data", "payload", "event_payload", "response"):
            cont = obj.get(container_key)
            if isinstance(cont, dict):
                for key in ("newCustomizationId", "new_customization_id"):
                    cid = coerce_int(cont.get(key))
                    if isinstance(cid, int) and cid > 0:
                        return cid

    for pat in (r'"newCustomizationId"\s*:\s*(\d+)', r'"new_customization_id"\s*:\s*(\d+)'):
        m = re.search(pat, stdout)
        if m:
            try:
                val = int(m.group(1))
                return val if val > 0 else None
            except Exception:
                pass

    return None

def run_add_to_cart(gettingcart_path: Path, listing_id: int, jar_path: Path, out_dir: Path) -> Optional[Tuple[int, Optional[int]]]:
    """
    Execute gettingcart curl with cookie jar, parse cart_id and inventory_id.
    Returns (cart_id, inventory_id) or None on execution failure.
    """
    try:
        original = read_file_text(gettingcart_path)
    except Exception as e:
        write_file_text(out_dir / "gettingcart_read_error.txt", str(e))
        return None

    write_file_text(out_dir / "gettingcart_original.txt", original)

    try:
        cmd = modify_gettingcart_command(original, listing_id)
    except Exception as e:
        write_file_text(out_dir / "gettingcart_modify_error.txt", str(e))
        return None

    write_file_text(out_dir / "gettingcart_modified.txt", cmd)

    # Inject cookie jar flags (write jar to persist session)
    cmd_jar = inject_cookie_jar(cmd, jar_path, read=False, write=True)
    write_file_text(out_dir / "gettingcart_sent_with_cookiejar.txt", cmd_jar)

    rc, stdout, stderr = run_shell_command(cmd_jar, timeout=120)
    write_file_text(out_dir / "gettingcart_stdout.json", stdout)
    write_file_text(out_dir / "gettingcart_stderr.txt", stderr if stderr else "")
    write_file_text(out_dir / "gettingcart_exitcode.txt", str(rc))

    if rc != 0:
        return None

    cart_id = extract_cart_id_from_add_to_cart(stdout)
    inventory_id = extract_inventory_id_from_add_to_cart(stdout)
    customization_id = extract_new_customization_id_from_add_to_cart(stdout)

    if cart_id is not None:
        write_file_text(out_dir / "cart_id_from_gettingcart.txt", str(cart_id))
    else:
        write_file_text(out_dir / "cart_id_parse_error.txt", "Could not parse cart_id from gettingcart response.")

    if inventory_id is not None:
        write_file_text(out_dir / "inventory_id_from_gettingcart.txt", str(inventory_id))
    else:
        write_file_text(out_dir / "inventory_id_parse_error.txt", "Could not parse inventory_id from gettingcart response.")

    if customization_id is not None:
        write_file_text(out_dir / "customization_id_from_gettingcart.txt", str(customization_id))
    else:
        write_file_text(out_dir / "customization_id_parse_error.txt", "Could not parse customization_id from gettingcart response.")

    return (cart_id, inventory_id) if cart_id is not None else None

# Function: run_remove_from_cart
def run_remove_from_cart(
    removingcart_path: Path,
    listing_id: int,
    cart_id: int,
    inventory_id: Optional[int],
    jar_path: Path,
    out_dir: Path,
    customization_id: Optional[int] = None,
    is_personalizable: bool = False,
):
    """
    Execute removingcart curl with robust behavior:
    - Personalise: do not inject inventory/customization; only cart/listing.
    - Non_personalise: inject inventory (>0 else 0), and customization (0 when inventory present; otherwise >0 else 0).
    Logs original, modified, and sent commands plus stdout/stderr/exit code.
    """
    try:
        original = read_file_text(removingcart_path)
    except Exception as e:
        write_file_text(out_dir / "removingcart_read_error.txt", str(e))
        return

    write_file_text(out_dir / "removingcart_original.txt", original)

    inv_val: Optional[int]
    cust_val: Optional[int]

    if is_personalizable:
        inv_val = None
        cust_val = None
    else:
        # Resolve inventory
        inv_val = inventory_id if isinstance(inventory_id, int) and inventory_id > 0 else None
        if inv_val is None:
            try:
                raw = read_file_text(out_dir / "gettingcart_stdout.json")
                parsed_inv = extract_inventory_id_from_add_to_cart(raw)
                inv_val = parsed_inv if isinstance(parsed_inv, int) and parsed_inv > 0 else 0
            except Exception:
                inv_val = 0

        # Resolve customization id; default to 0 when inventory present
        if not (isinstance(customization_id, int) and customization_id > 0):
            try:
                raw = read_file_text(out_dir / "gettingcart_stdout.json")
                parsed_cid = extract_new_customization_id_from_add_to_cart(raw)
                customization_id = parsed_cid if isinstance(parsed_cid, int) and parsed_cid > 0 else 0
            except Exception:
                customization_id = 0
        cust_val = customization_id

    try:
        cmd = modify_removingcart_command(
            original_cmd=original,
            cart_id=cart_id,
            listing_id=listing_id,
            inventory_id=inv_val,
            customization_id=cust_val,
            personalizable=is_personalizable,
        )
    except Exception as e:
        write_file_text(out_dir / "removingcart_modify_error.txt", str(e))
        return

    # Save the exact modified curl before cookie jar
    write_file_text(out_dir / "removingcart_modified_curl.txt", cmd)

    # Inject cookie jar and SAVE the exact command to these files
    cmd_jar = inject_cookie_jar(cmd, jar_path, read=True, write=True)
    write_file_text(out_dir / "removingcart_cmd.txt", cmd_jar)
    write_file_text(out_dir / "removingcart_sent_with_cookiejar.txt", cmd_jar)

    rc, stdout, stderr = run_shell_command(cmd_jar, timeout=120)
    write_file_text(out_dir / "removingcart_stdout.json", stdout)
    write_file_text(out_dir / "removingcart_stderr.txt", stderr if stderr else "")
    write_file_text(out_dir / "removingcart_exitcode.txt", str(rc))

def run_special_pers(pers_path: Path, listing_id: int, cart_id: int, jar_path: Path, out_dir: Path) -> Optional[int]:
    # Send pers request to prime personalization UI and extract a field id if present
    try:
        original = read_file_text(pers_path)
    except Exception as e:
        write_file_text(out_dir / "pers_read_error.txt", str(e))
        return None
    write_file_text(out_dir / "pers_original.txt", original)

    cmd = modify_pers_command(original, cart_id, listing_id)
    write_file_text(out_dir / "pers_modified.txt", cmd)

    cmd_jar = inject_cookie_jar(cmd, jar_path, read=True, write=True)
    write_file_text(out_dir / "pers_sent_with_cookiejar.txt", cmd_jar)

    rc, stdout, stderr = run_shell_command(cmd_jar, timeout=120)
    write_file_text(out_dir / "pers_offerings.json", stdout)
    write_file_text(out_dir / "pers_offerings_stderr.txt", stderr if stderr else "")
    write_file_text(out_dir / "pers_offerings_exitcode.txt", str(rc))

    if rc != 0:
        return None

    # Attempt to extract a personalization field id if available
    try:
        obj = json.loads(stdout)
        # Some responses expose personalization fields via "variations_html", but we don't strictly need it now
        # If a downstream remove requires customization id, it typically comes from specialized cmd, not here
        # Return cart_id as-is (we don't need inventory id anymore)
        return cart_id
    except Exception:
        return cart_id

# ===== Main orchestration =====

def load_listing_ids_from_popular(popular_path: Path, count: int) -> List[int]:
    """
    Load up to `count` listing IDs, preferring full listing objects produced by
    first_etsy_api_info or legacy popular_listings_full.json.
    Supports:
      - popular_now_listings_*.json → {'listings': [ {listing_id: ...}, ... ]}
      - popular_listings_full*.json → {'popular_results': [ {listing_id: ...}, ... ]}
      - Fallbacks: popular_now_ids*.json → {'popular_now_ids': [...]}
                  popular_listing_ids.json → {'popular_listing_ids': [...]}
                  listing_ids*.json → {'listing_ids': [...]}
    """
    # Ensure source file is normalized before reading
    try:
        popular_path = normalize_popular_listings_file(popular_path)
    except Exception:
        pass

    ids: List[int] = []

    try:
        if popular_path and popular_path.exists():
            data = json.loads(popular_path.read_text(encoding="utf-8"))
            # Prefer first_etsy_api_info shape
            items = data.get("listings")
            if not isinstance(items, list):
                # Legacy shape
                items = data.get("popular_results")
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict):
                        lid = it.get("listing_id")
                        if isinstance(lid, int):
                            ids.append(lid)
                            if len(ids) >= count:
                                break
    except Exception as e:
        print(f"Warning: failed to parse popular listings from {popular_path}: {e}")

    if len(ids) < count:
        base_dir = popular_path.parent if popular_path else None
        fallback_candidates: List[Path] = []
        if base_dir:
            # First-script IDs
            fallback_candidates.extend(sorted(base_dir.glob("popular_now_ids*.json")))
            # Legacy IDs
            fallback_candidates.append(base_dir / "popular_listing_ids.json")
            # Listing IDs produced by first script
            fallback_candidates.extend(sorted(base_dir.glob("listing_ids_*.json")))
            fallback_candidates.append(base_dir / "listing_ids.json")
        for cand in fallback_candidates:
            try:
                if cand.exists():
                    obj = json.loads(cand.read_text(encoding="utf-8"))
                    arr = obj.get("popular_now_ids") or obj.get("popular_listing_ids") or obj.get("listing_ids") or []
                    for lid in arr:
                        try:
                            lid_i = int(lid)
                        except Exception:
                            continue
                        if lid_i not in ids:
                            ids.append(lid_i)
                            if len(ids) >= count:
                                break
                    if len(ids) >= count:
                        break
            except Exception:
                continue

    # Final dedupe just in case
    ids = _dedupe_preserve_order_ints(ids)
    return ids[:count]

def ensure_output_dirs(run_dir: Path, listing_id: int, group_num: int, group_type: str) -> Path:
    ts = now_ts_compact()
    out_dir = run_dir / "cart_runs" / f"group_{group_type}_{group_num}" / f"{listing_id}" / ts
    # Supabase-backed: no local mkdir, writes intercepted by write_file_text/safe_copy
    return out_dir

# New helpers for run summary
def load_source_listing_info_map(popular_path: Path) -> Dict[int, dict]:
    """
    Build a map of listing_id -> full listing object from popular listings JSON.
    Supports both:
      - first_etsy_api_info: {'listings': [ {...}, ... ]}
      - legacy: {'popular_results': [ {...}, ... ]}
    """
    info_map: Dict[int, dict] = {}
    try:
        if popular_path and popular_path.exists():
            data = json.loads(popular_path.read_text(encoding="utf-8"))
            items = data.get("listings")
            if not isinstance(items, list):
                items = data.get("popular_results")
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict):
                        lid = it.get("listing_id")
                        if isinstance(lid, int):
                            info_map[lid] = it
    except Exception:
        pass
    return info_map

def extract_first_listing_snapshot(obj: dict) -> Optional[dict]:
    """
    Extract the first 'listing' snapshot dict from the listing curl JSON.
    Scans common containers: root, 'data', 'payload', 'event_payload', 'response'.
    """
    if not isinstance(obj, dict):
        return None

    def find_in_container(container: dict) -> Optional[dict]:
        if not isinstance(container, dict):
            return None
        for key in ("listings", "cart_listings", "line_items", "items"):
            arr = container.get(key)
            if isinstance(arr, list):
                for it in arr:
                    if isinstance(it, dict):
                        return it
        return None

    snap = find_in_container(obj)
    if snap:
        return snap

    for ckey in ("data", "payload", "event_payload", "response"):
        cont = obj.get(ckey)
        snap = find_in_container(cont)
        if snap:
            return snap

    return None

def build_run_summary_entry(
    listing_id: int,
    group_num: int,
    group_type: str,
    run_dir: Path,
    out_dir: Path,
    source_info: Optional[dict],
    popular_info: Optional[dict],
    scarcity_title: Optional[str],
    demand_value: Optional[int],
    group_files: Dict[str, Path],
) -> dict:
    """
    Assemble a single entry for the run summary using on-disk outputs.
    Reads exit codes and cart ids from per-listing files and embeds the listing curl snapshot.
    """
    def try_int_file(p: Path) -> Optional[int]:
        try:
            s = read_file_text(p).strip()
            return int(s) if s.isdigit() else None
        except Exception:
            return None

    def try_str_file(p: Path) -> Optional[str]:
        try:
            return read_file_text(p)
        except Exception:
            return None

    gettingcart_exit = try_int_file(out_dir / "gettingcart_exitcode.txt")
    listing_exit = try_int_file(out_dir / "listing_curl_exitcode.txt")
    removing_exit = try_int_file(out_dir / "removingcart_exitcode.txt")

    first_cart_id = try_int_file(out_dir / "cart_id_from_gettingcart.txt")
    cart_id_from_listing = try_int_file(out_dir / "cart_id_from_listing_json.txt")

    # Embed the first listing snapshot from the listing curl JSON
    listing_curl_json_path = out_dir / "listing_curl_stdout.json"
    listing_snapshot: Optional[dict] = None
    try:
        if listing_curl_json_path.exists():
            raw = read_file_text(listing_curl_json_path)
            obj = json.loads(raw)
            listing_snapshot = extract_first_listing_snapshot(obj) or None
    except Exception:
        listing_snapshot = None

    # NEW: Coalesce a title for downstream consumers
    title_val: Optional[str] = None
    try:
        if isinstance(popular_info, dict):
            t = popular_info.get("title")
            if isinstance(t, str) and t.strip():
                title_val = t.strip()
        if not title_val and isinstance(source_info, dict):
            t = source_info.get("title")
            if isinstance(t, str) and t.strip():
                title_val = t.strip()
        if not title_val and isinstance(listing_snapshot, dict):
            for k in ("title", "name", "listing_title"):
                t = listing_snapshot.get(k)
                if isinstance(t, str) and t.strip():
                    title_val = t.strip()
                    break
        if not title_val and isinstance(scarcity_title, str) and scarcity_title.strip():
            title_val = scarcity_title.strip()
    except Exception:
        # Final fallback
        title_val = scarcity_title if isinstance(scarcity_title, str) else None

    entry = {
        "listing_id": listing_id,
        "group": group_num,
        "group_type": group_type,
        "output_dir": str(out_dir),
        "gettingcart_exitcode": gettingcart_exit,
        "listing_curl_exitcode": listing_exit,
        "removingcart_exitcode": removing_exit,
        "first_cart_id": first_cart_id,
        "cart_id_from_listing_json": cart_id_from_listing,
        "scarcity_signal_title": scarcity_title,
        "demand": demand_value,
        "listing_curl_stdout_path": str(listing_curl_json_path),
        "group_files": {k: str(v) for k, v in group_files.items()},
        "source_info": source_info or {},
        "popular_info": popular_info or {},
        "listing_cart_snapshot": listing_snapshot or {},
        "title": title_val,  # NEW: normalized product title
    }
    return entry

def write_run_summary(
    demand_extracted_dir: Path,
    requested_count: int,
    run_dir: Path,
    source_choice: str,
    popular_json_path: Optional[Path],
    entries: List[dict],
) -> Path:
    """
    Write the run summary JSON under the selected run's 'demand extracted' directory.
    Matches the header fields layout you referenced.
    """
    ensure_dir(demand_extracted_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_path = demand_extracted_dir / f"run_summary_{ts}.json"
    summary_obj = {
        "requested_count": requested_count,
        "selected_run": str(run_dir),
        "source_choice": source_choice,
        "popular_json": str(popular_json_path) if popular_json_path else None,
        "entries": entries,
    }
    write_file_text(summary_path, json.dumps(summary_obj, ensure_ascii=False, indent=2))
    return summary_path

def main():
    print_banner()

    # Pick a run and resolve paths/artifacts
    outputs_root = PROJECT_ROOT / "outputs"
    run_info = select_run_and_get_paths(outputs_root)

    run_dir: Path = run_info["run_dir"]
    outputs_dir: Path = run_info["outputs_dir"]
    demand_extracted_dir: Path = run_info["demand_extracted_dir"]
    popular_json_path: Path = run_info.get("popular_json_path") or POPULAR_JSON_PATH_DEFAULT

    # Ask how many listings to process
    try:
        raw = input("How many listings to process from popular? [default 5]: ").strip()
        count = int(raw) if raw else 5
    except Exception:
        count = 5

    # Resolve listing_ids from the popular listings file
    listing_ids = load_listing_ids_from_popular(popular_json_path, count)
    print(f"\nProcessing {len(listing_ids)} listings...")

    # For summary embedding (title/source fields), load the source info map
    source_info_map = load_source_listing_info_map(popular_json_path)

    summary_entries: List[dict] = []

    for idx, listing_id in enumerate(listing_ids, start=1):
        print("\n" + "-" * 80)
        print(f"[{idx}/{len(listing_ids)}] Listing {listing_id}")

        summary = None
        try:
            # Run the full demand pipeline (add → listing → parse → variations → remove → verify)
            summary = process_listing_demand(int(listing_id), run_dir, outputs_dir)

            # Unify listing JSON filename for downstream consumers:
            # If listing_stdout.json exists (from process_listing_demand), mirror it to listing_curl_stdout.json
            out_dir = Path(summary.get("out_dir") or ensure_run_output_for_group(run_dir, int(listing_id), 1, summary.get("group_type") or "non_personalise"))
            lstdout_json = out_dir / "listing_stdout.json"
            lcurl_json = out_dir / "listing_curl_stdout.json"
            try:
                if lstdout_json.exists() and not lcurl_json.exists():
                    write_file_text(lcurl_json, read_file_text(lstdout_json))
            except Exception:
                pass

            # Build summary entry object aligned with the run_summary layout
            group_files = {
                "gettingcart": out_dir / "gettingcart_cmd.txt",
                "listing": out_dir / "listing_cmd.txt",
                "removingcart": out_dir / "removingcart_cmd_template.txt",
            }
            entry = build_run_summary_entry(
                listing_id=int(listing_id),
                group_num=int(summary.get("group_num") or 1),
                group_type=str(summary.get("group_type") or "non_personalise"),
                run_dir=run_dir,
                out_dir=out_dir,
                source_info=source_info_map.get(int(listing_id)),
                popular_info=source_info_map.get(int(listing_id)),
                scarcity_title=summary.get("scarcity_title"),
                demand_value=summary.get("demand_value"),
                group_files=group_files,
            )
            summary_entries.append(entry)

            # Progress info
            print(f"→ group {entry['group_type']} #{entry['group']}; demand={entry['demand']} title={entry.get('scarcity_signal_title') or 'n/a'}")

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            break
        except RuntimeError as e:
            # process_listing_demand raises when removal verification fails (fatal)
            print(f"Fatal error on listing {listing_id}: {e}")
            break
        except Exception as e:
            # Unexpected error; record and continue
            out_dir_fallback = ensure_run_output_for_group(run_dir, int(listing_id), 1, "error")
            write_file_text(out_dir_fallback / "unexpected_error.txt", str(e))
        finally:
            # Stage, upload cart_runs in background, and delete local per listing
            try:
                if summary and summary.get("out_dir"):
                    schedule_background_upload_and_cleanup(Path(summary["out_dir"]), run_dir)
            except Exception:
                pass

    # Write run summary JSON at the end
    summary_path = write_run_summary(
        demand_extracted_dir=demand_extracted_dir,
        requested_count=count,
        run_dir=run_dir,
        source_choice="popular",
        popular_json_path=popular_json_path,
        entries=summary_entries,
    )
    print(f"\nRun summary saved: {summary_path}")

    # Upload the 'demand extracted' directory to Supabase for persistence/verification
    try:
        upload_res = upload_directory(
            None,  # use default bucket
            str(demand_extracted_dir),
            dest_prefix=f"outputs/{run_dir.name}/demand_extracted",
        )
        write_file_text(demand_extracted_dir / "upload_result.json", json.dumps(upload_res, ensure_ascii=False, indent=2))
        print(f"Uploaded demand extracted: {upload_res.get('uploaded_count', 0)} files, errors: {upload_res.get('errors_count', 0)}")
        # Show one sample public URL for quick validation
        try:
            sample = next((x for x in upload_res.get("uploaded", []) if "public_url" in x), None)
            if sample:
                print(f"Sample public URL: {sample['public_url']}")
        except Exception:
            pass
    except Exception as e:
        print(f"Demand extracted upload error: {e}")

    # Wait for background uploads to finish to ensure Supabase is consistent
    print("Waiting for background uploads to finish...")
    for t in BACKGROUND_UPLOAD_THREADS:
        try:
            t.join()
        except Exception:
            pass
    print("All uploads completed.")

    # Final sweep: ensure the popular-now file reflects unique IDs and correct count
    try:
        normalize_popular_listings_file(popular_json_path)
    except Exception:
        pass
    print("\n" + "=" * 80)
    print("Completed all assigned listings.")
    print("=" * 80)

# ===== Single mode =====

def single_mode():
    print_banner()

    try:
        raw_listing_id = input("Enter a listing_id: ").strip()
        listing_id = int(raw_listing_id)
    except Exception:
        print("Invalid listing_id.")
        return

    # Choose a normal group
    normal_groups = find_group_numbers(TXT_FILES_ROOT)
    if not normal_groups:
        print("No normal groups found under txt_files.")
        return
    print("Available normal groups:", normal_groups)
    try:
        raw_group = input(f"Pick group number from above [default {normal_groups[0]}]: ").strip()
        group_num = int(raw_group) if raw_group else normal_groups[0]
    except Exception:
        group_num = normal_groups[0]

    run_dir = RUNS_ROOT / f"single_{listing_id}_{now_ts_compact()}"
    ensure_dir(run_dir)
    out_dir = ensure_listing_output_dir(run_dir, listing_id)

    clear_cookie_jar(DEFAULT_COOKIE_JAR)
    write_file_text(out_dir / "cookie_jar_cleared.txt", f"Cleared cache at start for listing_id={listing_id}")

    gettingcart_path, listing_txt_path, removingcart_path = group_paths(group_num)

    # Copy originals
    safe_copy(gettingcart_path, out_dir / "gettingcart_original.txt")
    safe_copy(listing_txt_path, out_dir / "listing_original.txt")
    safe_copy(removingcart_path, out_dir / "removingcart_original.txt")

    # Add to cart
    result = run_add_to_cart(gettingcart_path, listing_id, DEFAULT_COOKIE_JAR, out_dir)
    if result is None:
        print("Add-to-cart failed or values not parsed; aborting.")
        write_file_text(out_dir / "aborted.txt", "Aborted due to add-to-cart failure.")
        return
    cart_id, inventory_id = result

    # Listing request — debug curl verbatim
    rc, c_stdout, c_stderr = execute_listing_curl_verbatim(listing_txt_path, out_dir)
    if rc != 0 or not c_stdout.strip():
        print("Listing curl returned non-zero or empty response; attempting JSON parse anyway.")

    scarcity_title, demand_value = parse_listing_json_and_extract(c_stdout, out_dir)
    if demand_value is not None:
        print(f"Demand extracted: {demand_value}")
    else:
        print("Demand not found in scarcity signal title.")

    # Remove from cart (use both cart_id and inventory_id)
    run_remove_from_cart(removingcart_path, listing_id, cart_id, inventory_id, DEFAULT_COOKIE_JAR, out_dir)

    print("\nSingle-mode run complete.")

    # Stage, upload in background, and delete local per product
    schedule_background_upload_and_cleanup(out_dir, run_dir)
    # Ensure upload finishes before process exit in single mode
    for t in BACKGROUND_UPLOAD_THREADS:
        try:
            t.join()
        except Exception:
            pass

# ===== Entry point =====

if __name__ == "__main__":
    try:
        mode = (input("Choose mode: [1] bulk, [2] single: ").strip() or "1")
        if mode == "2":
            single_mode()
        else:
            main()
    except KeyboardInterrupt:
        print("\nExiting.")