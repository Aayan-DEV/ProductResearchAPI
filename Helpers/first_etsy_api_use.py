import json
import os
import re
import sys
import time
import html
from typing import Dict, Tuple, List, Optional
from unittest.signals import registerResult
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---
API_KEYSTRING = os.getenv("ETSY_X_API_KEY") or os.getenv("ETSY_API_KEYSTRING") or ""
SHARED_SECRET = os.getenv("SHARED_SECRET", "")
OPENAPI_BASE = "https://openapi.etsy.com"
SEARCH_ENDPOINT = f"{OPENAPI_BASE}/v3/application/listings/active"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TEST_TXT_PATH = os.getenv("ETO_SEARCH_CURL_PATH") or str(
    PROJECT_ROOT / "EtoRequests" / "Search_request" / "txt_files" / "1" / "EtoRequest1.txt"
)
OUTPUT_DIR = os.getenv("ETO_OUTPUT_DIR") or str(PROJECT_ROOT / "outputs")
V3_PAGE_SIZE = 100
CURL_CHUNK_SIZE = 20
MAX_OFFSET = 12000
HTTP_TIMEOUT = 60

# --- General helpers ---
def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def slugify_keyword(keyword: str) -> str:
    slug = keyword.strip().lower()
    slug = re.sub(r"\s+", "_", slug)
    slug = re.sub(r"[^a-z0-9_]+", "", slug)
    return slug or "keyword"


def write_json(path: str, data) -> None:
    ensure_output_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def write_text(path: str, text: str) -> None:
    ensure_output_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def backoff_sleep(attempt: int) -> None:
    # Exponential backoff with jitter
    base = 0.8
    sleep_time = base * (2 ** attempt)
    time.sleep(min(sleep_time, 10.0))


# --- Etsy v3 search ---
def fetch_listings_by_keyword(keyword: str, limit: int = 100, offset: int = 0) -> Dict:
    params = {
        "keywords": keyword,
        "limit": limit,
        "offset": offset,
        "state": "active",
    }
    headers = {
        "x-api-key": API_KEYSTRING,
        "accept": "application/json",
    }
    resp = requests.get(SEARCH_ENDPOINT, headers=headers, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    return resp.json()

def fetch_shop_info(shop_id: int, reviews_limit: int = 100) -> Dict:
    """
    Fetch broad shop information and return it under a unified object:
    - details: /v3/application/shops/{id}
    - sections: /v3/application/shops/{id}/sections (paginated)
    - reviews: /v3/application/shops/{id}/reviews (first 100 only)
    Gracefully records errors without throwing.
    """
    sid = int(str(shop_id))
    headers = {
        "x-api-key": API_KEYSTRING,
        "accept": "application/json",
    }
    # If OAuth is present, include it (may unlock more fields); otherwise it's fine
    oauth = os.getenv("ETSY_ACCESS_TOKEN") or os.getenv("ETSY_BEARER_TOKEN") or os.getenv("ETSY_OAUTH_TOKEN")
    if oauth:
        headers["authorization"] = f"Bearer {oauth}"

    result: Dict = {
        "shop_id": sid,
        "details": None,
        "sections": [],
        "reviews": [],
        "errors": {},
    }

    # Shop details
    details_url = f"{OPENAPI_BASE}/v3/application/shops/{sid}"
    for attempt in range(5):
        try:
            resp = requests.get(details_url, headers=headers, timeout=HTTP_TIMEOUT)
            status = resp.status_code
            if status in (429,) or (500 <= status < 600):
                backoff_sleep(attempt)
                continue
            try:
                data = resp.json()
            except Exception:
                data = None
            if status >= 400:
                result["errors"]["details"] = {
                    "status": status,
                    "url": resp.url,
                    "response": data,
                }
            else:
                result["details"] = data
            break
        except requests.RequestException as e:
            if attempt == 4:
                result["errors"]["details"] = {"status": None, "message": str(e), "url": details_url}
            else:
                backoff_sleep(attempt)

    # Sections (paginate until exhausted)
    sections_url = f"{OPENAPI_BASE}/v3/application/shops/{sid}/sections"
    offset = 0
    limit = 100
    while True:
        try:
            resp = requests.get(sections_url, headers=headers, params={"limit": limit, "offset": offset}, timeout=HTTP_TIMEOUT)
            status = resp.status_code
            if status in (429,) or (500 <= status < 600):
                backoff_sleep(0)
                continue
            try:
                data = resp.json()
            except Exception:
                data = None
            if status >= 400:
                result["errors"]["sections"] = {
                    "status": status,
                    "url": resp.url,
                    "response": data,
                }
                break
            page = []
            if isinstance(data, dict):
                page = data.get("results") or []
                if not page and isinstance(data.get("data"), list):
                    page = data["data"]
            if not page:
                break
            result["sections"].extend(page)
            fetched = len(page)
            if fetched < limit:
                break
            offset += fetched
        except requests.RequestException as e:
            result["errors"]["sections"] = {"status": None, "message": str(e), "url": sections_url}
            break

    # Reviews: first 100 only
    reviews_url = f"{OPENAPI_BASE}/v3/application/shops/{sid}/reviews"
    reviews_limit = max(1, min(100, int(str(reviews_limit or 100))))
    try:
        resp = requests.get(reviews_url, headers=headers, params={"limit": reviews_limit, "offset": 0}, timeout=HTTP_TIMEOUT)
        status = resp.status_code
        try:
            data = resp.json()
        except Exception:
            data = None
        if status >= 400:
            result["errors"]["reviews"] = {
                "status": status,
                "url": resp.url,
                "response": data,
            }
        else:
            page = []
            if isinstance(data, dict):
                page = data.get("results") or []
                if not page and isinstance(data.get("data"), list):
                    page = data["data"]
            if isinstance(page, list):
                result["reviews"] = page[:reviews_limit]
    except requests.RequestException as e:
        result["errors"]["reviews"] = {"status": None, "message": str(e), "url": reviews_url}

    return result

def collect_shop_info_map_for_listings(listings: List[Dict], reviews_limit: int = 100) -> Dict[int, Dict]:
    """
    Build a map of listing_id -> 'shop' dict by fetching public shop details, sections,
    and up to the first 100 reviews per unique shop_id found in `listings`.
    Gracefully handles errors; failed fetches produce minimal objects with 'errors'.
    """
    # Unique shops first to avoid redundant calls
    unique_shop_ids: List[int] = []
    for it in (listings or []):
        sid = it.get("shop_id")
        try:
            sid_int = int(str(sid))
        except Exception:
            continue
        if sid_int not in unique_shop_ids:
            unique_shop_ids.append(sid_int)

    shop_info_by_sid: Dict[int, Dict] = {}
    for sid in unique_shop_ids:
        try:
            shop_info_by_sid[sid] = fetch_shop_info(sid, reviews_limit=reviews_limit)
        except Exception as e:
            shop_info_by_sid[sid] = {
                "shop_id": sid,
                "details": None,
                "sections": [],
                "reviews": [],
                "errors": {"fetch": str(e)},
            }

    by_listing_id: Dict[int, Dict] = {}
    for it in (listings or []):
        lid = it.get("listing_id") or it.get("listingId")
        sid = it.get("shop_id")
        try:
            li = int(str(lid))
            si = int(str(sid))
        except Exception:
            continue
        by_listing_id[li] = shop_info_by_sid.get(si) or {
            "shop_id": si,
            "details": None,
            "sections": [],
            "reviews": [],
            "errors": {"missing": True},
        }
    return by_listing_id

def fetch_listings_aggregated(keyword: str, desired_total: Optional[int] = None, page_size: int = V3_PAGE_SIZE, shop_id_for_info: Optional[int] = None) -> Dict:
    """
    Paginate Etsy v3 search results until desired_total or until no more pages.
    desired_total=None means fetch all available (subject to API offset constraints).
    """
    aggregated_results: List[Dict] = []
    offset = 0
    total_fetched = 0
    page_index = 0

    while True:
        if desired_total is not None and total_fetched >= desired_total:
            break
        if offset >= MAX_OFFSET:
            # Stop at offset limit
            break

        remaining = None
        current_limit = page_size
        if desired_total is not None:
            remaining = max(0, desired_total - total_fetched)
            current_limit = min(page_size, remaining) if remaining > 0 else 0
            if current_limit <= 0:
                break

        # Retry/backoff around rate limits and transient errors
        for attempt in range(5):
            try:
                data = fetch_listings_by_keyword(keyword, limit=current_limit, offset=offset)
                break
            except requests.HTTPError as e:
                status = e.response.status_code if e.response is not None else None
                # Backoff only on 429 and 5xx
                if status in (429,) or (status and 500 <= status < 600):
                    backoff_sleep(attempt)
                    continue
                # Re-raise otherwise
                raise
            except requests.RequestException:
                backoff_sleep(attempt)
                continue
        else:
            # If we exhausted retries without break
            raise RuntimeError("Failed to fetch listings from Etsy API after retries.")

        results = data.get("results")
        if not isinstance(results, list) or len(results) == 0:
            break

        aggregated_results.extend(results)
        fetched_this_page = len(results)
        total_fetched += fetched_this_page
        page_index += 1
        offset += fetched_this_page

        # Stop if fewer than requested were returned (end reached)
        if fetched_this_page < current_limit:
            break

    aggregated_json = {
        "aggregated": True,
        "keyword": keyword,
        "meta": {
            "requested_total": desired_total if desired_total is not None else "all",
            "fetched_total": total_fetched,
            "page_size": page_size,
            "pages": page_index,
        },
        "results": aggregated_results,
    }
    # Optionally attach complete shop info under "shop"
    if shop_id_for_info is not None:
        try:
            aggregated_json["shop"] = fetch_shop_info(int(str(shop_id_for_info)), reviews_limit=20)
        except Exception as e:
            aggregated_json["shop"] = {
                "shop_id": shop_id_for_info,
                "error": f"failed to fetch shop info: {e}",
            }
    return aggregated_json


def extract_listing_ids(search_json: Dict) -> List[int]:
    """
    Extract listing IDs from aggregated Etsy v3 search response.
    """
    ids: List[int] = []
    results = search_json.get("results") or []
    for item in results:
        lid = item.get("listing_id")
        if isinstance(lid, int):
            ids.append(lid)
        else:
            # Sometimes the library returns strings; normalize
            try:
                ids.append(int(str(lid)))
            except Exception:
                pass
    # Ensure unique order preserved
    seen = set()
    deduped: List[int] = []
    for lid in ids:
        if lid not in seen:
            seen.add(lid)
            deduped.append(lid)
    return deduped

def fetch_listing_primary_image(listing_id: int) -> Optional[Dict]:
    """
    Fetch the primary image for a single listing via Etsy Open API v3.
    Returns a small dict with usable URLs and dimensions or None on failure.
    """
    sid = int(str(listing_id))
    headers = {
        "x-api-key": API_KEYSTRING,
        "accept": "application/json",
    }
    # Include OAuth if present to improve fields access
    oauth = os.getenv("ETSY_ACCESS_TOKEN") or os.getenv("ETSY_BEARER_TOKEN") or os.getenv("ETSY_OAUTH_TOKEN")
    if oauth:
        headers["authorization"] = f"Bearer {oauth}"

    url = f"{OPENAPI_BASE}/v3/application/listings/{sid}/images"
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            status = resp.status_code
            try:
                data = resp.json()
            except Exception:
                data = None

            if status in (429,) or (500 <= status < 600):
                backoff_sleep(attempt)
                continue
            if status >= 400 or not isinstance(data, dict):
                return None

            results = data.get("results") or data.get("data") or []
            if not isinstance(results, list) or not results:
                return None

            img = results[0] or {}
            def pick(*keys):
                for k in keys:
                    v = img.get(k)
                    if v:
                        return v
                return None

            return {
                "listing_id": sid,
                "image_id": img.get("image_id") or img.get("listing_image_id"),
                "url_full": pick("url_fullxfull", "full_size_image_url", "url"),
                "url_300x300": pick("url_300x300", "url_170x135", "url_200x200", "url_small", "url"),
                "width": pick("full_width", "width"),
                "height": pick("full_height", "height"),
            }
        except requests.RequestException:
            backoff_sleep(attempt)
            continue
        except Exception:
            return None
    return None

def generate_listing_card_html(listing_id: int, title: Optional[str], page_url: Optional[str], image: Optional[Dict]) -> str:
    """
    Produce a minimal 'listingcards_cleaned' HTML snippet containing the listing ID and primary <img>.
    Mirrors the structure used by search listing cards sufficiently for downstream parsing.
    """
    lid = int(str(listing_id))
    img_src = None
    if isinstance(image, dict):
        img_src = image.get("url_300x300") or image.get("url_full") or None
    alt = (title or f"Listing {lid}").strip()
    href = page_url or f"https://www.etsy.com/listing/{lid}"

    return f"""<li class="wt-list-unstyled wt-grid__item-xs-6 wt-grid__item-md-4 wt-grid__item-lg-3 ">
  <div class="js-merch-stash-check-listing v2-listing-card wt-height-full"
       data-listing-id="{lid}"
       data-page-type="search"
       data-behat-listing-card>
    <a class="v2-listing-card__img wt-position-relative listing-card-rounded-corners"
       data-listing-id="{lid}"
       href="{href}"
       target="etsy.{lid}">
      <div class="placeholder listing-card-rounded-corners">
        <div class="placeholder vertically-centered-placeholder listing-card-rounded-corners">
          <img
            class="wt-width-full wt-display-block listing-card-rounded-corners wt-image--cover wt-image"
            src="{img_src or ''}"
            alt="{html.escape(alt)}"
            data-listing-card-listing-image
          />
        </div>
      </div>
    </a>
    <div class="v2-listing-card__info wt-pt-xs-0">
      <h3 class="wt-text-caption v2-listing-card__title wt-text-truncate search-half-unit-mt wt-mt-xs-1 search-half-unit-mb"
          id="listing-title-{lid}">
        {html.escape(alt)}
      </h3>
    </div>
  </div>
</li>
"""

# new helpers for single-search via cURL (add near the bottom of file)
def find_latest_curl_original() -> Optional[Path]:
    """
    Find the latest curl_original_*.curl under outputs/*/helpers/.
    Returns the most recently modified file path or None.
    """
    base = PROJECT_ROOT / "outputs"
    try:
        candidates: List[Path] = []
        for p in base.glob("*"):
            helpers = p / "helpers"
            if helpers.exists():
                candidates.extend(helpers.glob("curl_original_*.curl"))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        return candidates[0]
    except Exception:
        return None

def extract_listingcards_html_from_response(parsed_json: Dict) -> Optional[str]:
    """
    Extract 'output.listingCards' HTML string from listingCards JSON response.
    """
    try:
        output = parsed_json.get("output")
        if isinstance(output, dict):
            html_str = output.get("listingCards")
            if isinstance(html_str, str) and html_str.strip():
                return html_str
    except Exception:
        pass
    return None

def write_single_listing_helpers_via_curl(run_dir: Path, listing_id: int, curl_hint_path: Optional[Path] = None) -> Optional[Path]:
    """
    Execute a real listingCards cURL with listing_ids=[listing_id], then save:
    - helpers/curl_original_single.curl  (rebuilt command)
    - helpers/listingcards_response_single.json  (raw response JSON)
    - helpers/listingcards_cleaned_single_chunk_1.html  (HTML from response)
    Returns path to the HTML file or None on failure.
    """
    helpers_dir = run_dir / "helpers"
    ensure_output_dir(str(helpers_dir))

    curl_path = curl_hint_path if curl_hint_path and Path(curl_hint_path).exists() else find_latest_curl_original()
    if not curl_path:
        return None

    try:
        url, method, headers, data_obj, cookies = parse_curl_file(str(curl_path))
        # Replace listing_ids to only include our single listing id
        try:
            data_obj = replace_listing_ids_in_curl_data(data_obj, [int(listing_id)])
        except Exception:
            # If structure is different, still attempt to send as-is
            pass

        # Execute request
        status, text, parsed = run_curl_request(url, method, headers, data_obj)
        if status >= 400 or (parsed is None and not text):
            return None

        # Save rebuilt cURL (for traceability)
        rebuilt = build_curl_command(url, method, headers, data_obj)
        (helpers_dir / "curl_original_single.curl").write_text(rebuilt, encoding="utf-8")

        # Save raw response JSON (prefer parsed; fallback to text)
        try:
            write_json(str(helpers_dir / "listingcards_response_single.json"), parsed if isinstance(parsed, dict) else {"raw": text})
        except Exception:
            write_text(str(helpers_dir / "listingcards_response_single.json"), text or "")

        # Extract HTML and write cleaned helpers file
        html = None
        if isinstance(parsed, dict):
            html = extract_listingcards_html_from_response(parsed)
        if not html or not html.strip():
            # If listingCards HTML not found, try to extract from text (rare fallback)
            try:
                obj = json.loads(text)
                html = extract_listingcards_html_from_response(obj)
            except Exception:
                html = None

        if not html or not html.strip():
            return None

        html_path = helpers_dir / "listingcards_cleaned_single_chunk_1.html"
        write_text(str(html_path), html)
        return html_path
    except Exception:
        return None

def write_single_listing_helpers(run_dir: Path, listing_id: int, title: Optional[str], page_url: Optional[str], image: Optional[Dict], keyword_slug: Optional[str] = None) -> Path:
    """
    Write helpers HTML under run_dir/helpers, naming like listingcards_cleaned_{keyword_or_single}_chunk_1.html.
    Returns the path to the written HTML.
    """
    helpers_dir = Path(run_dir) / "helpers"
    ensure_output_dir(str(helpers_dir))
    fname_slug = (keyword_slug or "single")
    html_path = helpers_dir / f"listingcards_cleaned_{fname_slug}_chunk_1.html"
    html_str = generate_listing_card_html(int(listing_id), title, page_url, image)
    write_text(str(html_path), html_str)
    return html_path

# --- cURL parsing and multi-request execution ---
def parse_curl_file(path: str) -> Tuple[str, str, Dict[str, str], Dict, Optional[str]]:
    """
    Parse a curl file and extract: (url, method, headers, data_json, cookies_header_combined)
    - Supports --request/ -X
    - Supports --header/ -H lines
    - Supports --cookie and 'Cookie:' header lines, merged into a single 'Cookie' header
    - Supports --data / --data-raw JSON body
    """
    text = read_text(path)

    # Reject empty curl files early
    if not text or not text.strip():
        raise ValueError("curl file is empty")

    # URL (robust extraction)
    url_match = re.search(r"--url\s+['\"]?([^'\"\s]+)['\"]?", text)
    if not url_match:
        # curl "https://..." or curl 'https://...'
        url_match = re.search(r"curl\s+['\"]([^'\"]+)['\"]", text)
    if not url_match:
        # Try to find any https URL in the first curl line
        first_line = text.strip().splitlines()[0] if text.strip().splitlines() else ""
        url_match = re.search(r"https?://[^\s'\"\\]+", first_line)
    if not url_match:
        # Fallback: any URL anywhere in the file
        url_match = re.search(r"https?://[^\s'\"\\]+", text)
    if not url_match:
        raise ValueError("Could not parse URL from cURL file.")
    url = url_match.group(1)

    # Method
    method_match = re.search(r"--request\s+(\S+)", text)
    if not method_match:
        method_match = re.search(r"-X\s+(\S+)", text)
    method = method_match.group(1).upper() if method_match else "GET"

    # Headers
    headers: Dict[str, str] = {}
    for m in re.finditer(r"--header\s+'([^:]+):\s*(.*?)'\s*\\?", text):
        k = m.group(1).strip()
        v = m.group(2).strip()
        headers[k] = v
    for m in re.finditer(r"-H\s+'([^:]+):\s*(.*?)'\s*\\?", text):
        k = m.group(1).strip()
        v = m.group(2).strip()
        headers[k] = v

    # Cookie lines from --cookie
    cookie_values: List[str] = []
    for m in re.finditer(r"--cookie\s+'([^']+)'\s*\\?", text):
        cookie_values.append(m.group(1).strip())
    for m in re.finditer(r"--cookie\s+\"([^\"]+)\"\s*\\?", text):
        cookie_values.append(m.group(1).strip())
    for m in re.finditer(r"-b\s+'([^']+)'\s*\\?", text):
        cookie_values.append(m.group(1).strip())
    for m in re.finditer(r"-b\s+\"([^\"]+)\"\s*\\?", text):
        cookie_values.append(m.group(1).strip())

    # Cookie header (explicit) merged
    cookie_header = headers.get("Cookie")
    if cookie_header and cookie_header.strip():
        cookie_values.append(cookie_header.strip())

    cookies_merged = "; ".join(cookie_values) if cookie_values else None
    if cookies_merged:
        headers["Cookie"] = cookies_merged

    # Data JSON
    data_json_obj: Dict = {}
    data_match = re.search(r"--data\s+'(.*?)'\s*\\?$", text, flags=re.DOTALL | re.MULTILINE)
    if not data_match:
        data_match = re.search(r"--data-raw\s+'(.*?)'\s*\\?$", text, flags=re.DOTALL | re.MULTILINE)
    if data_match:
        data_str = data_match.group(1)
        try:
            data_json_obj = json.loads(data_str)
        except Exception as e:
            try:
                data_json_obj = json.loads(data_str.encode("utf-8").decode("unicode_escape"))
            except Exception:
                raise ValueError(f"Failed to parse JSON body from cURL file: {e}")
    else:
        data_json_obj = {}

    return url, method, headers, data_json_obj, cookies_merged


def replace_listing_ids_in_curl_data(data_obj: Dict, listing_ids: List[int]) -> Dict:
    """
    Replace the listing_ids array inside the listingCards spec of the cURL JSON body.
    """
    if not isinstance(data_obj, dict):
        raise ValueError("cURL data object is not a dict.")
    specs = data_obj.get("specs")
    if not isinstance(specs, dict):
        raise ValueError("cURL data 'specs' missing or not a dict.")
    listing_cards = specs.get("listingCards")
    if not isinstance(listing_cards, list) or len(listing_cards) < 2:
        raise ValueError("cURL data 'specs.listingCards' malformed.")
    # listingCards payload structure: ["Search2_ApiSpecs_LazyListingCards", { ... }]
    # The second element is the payload dict with 'listing_ids'
    payload = listing_cards[1]
    if not isinstance(payload, dict):
        raise ValueError("cURL listingCards payload malformed.")
    payload["listing_ids"] = listing_ids
    return data_obj


def run_curl_request(url: str, method: str, headers: Dict[str, str], data_obj: Dict) -> Tuple[int, str, Optional[Dict]]:
    """
    Execute the cURL as a HTTP request using requests.
    Returns (status_code, text, parsed_json_or_none).
    Retries 429/5xx with backoff.
    """
    for attempt in range(5):
        try:
            if method == "POST":
                resp = requests.post(url, headers=headers, json=data_obj, timeout=HTTP_TIMEOUT)
            elif method == "GET":
                resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
            else:
                # Fallback to POST (listingCards usually POST)
                resp = requests.post(url, headers=headers, json=data_obj, timeout=HTTP_TIMEOUT)
            status = resp.status_code
            text = resp.text
            parsed = None
            try:
                parsed = resp.json()
            except Exception:
                parsed = None

            if status in (429,) or (500 <= status < 600):
                backoff_sleep(attempt)
                continue
            return status, text, parsed
        except requests.RequestException:
            backoff_sleep(attempt)
            continue
    raise RuntimeError("Failed to execute cURL request after retries.")


def chunk_list(lst: List[int], size: int) -> List[List[int]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def shell_quote_single(s: str) -> str:
    return "'" + s.replace("'", "'\"'\"'") + "'"

def build_curl_command(url: str, method: str, headers: Dict[str, str], data_obj: Dict) -> str:
    """
    Build a readable, multi-line curl command representing the request that will be sent.
    Each option is on its own line with a trailing backslash for clarity.
    """
    def line(s: str, trailing: bool = True) -> str:
        return (s + " \\") if trailing else s

    lines: List[str] = []
    lines.append(line("curl"))
    lines.append(line(f"  --request {method.upper()}"))
    lines.append(line(f"  --url {shell_quote_single(url)}"))
    for k, v in headers.items():
        # Put each header on its own line
        lines.append(line(f"  --header {shell_quote_single(f'{k}: {v}')}", trailing=True))
    if method.upper() != "GET":
        data_str = json.dumps(data_obj, separators=(",", ":"), ensure_ascii=False)
        # Last line without trailing backslash
        lines.append(line(f"  --data-raw {shell_quote_single(data_str)}", trailing=False))
    else:
        # Remove the trailing backslash from the last header line for GET
        if lines and lines[-1].endswith(" \\"):
            lines[-1] = lines[-1][:-2]
    return "\n".join(lines)

# --- HTML parsing for listingCards responses ---
def extract_listing_ids_from_html(html_str: str) -> List[int]:
    ids: List[int] = []
    for m in re.finditer(r'data-listing-id="(\d+)"', html_str):
        try:
            ids.append(int(m.group(1)))
        except Exception:
            pass
    # Deduplicate preserving order
    seen = set()
    deduped: List[int] = []
    for lid in ids:
        if lid not in seen:
            seen.add(lid)
            deduped.append(lid)
    return deduped

def find_popular_now_ids_from_json(data: Dict) -> Tuple[List[int], List[int]]:
    """
    Traverse JSON to identify 'Popular now' listings.
    Heuristics:
    - Handle listingCards JSON response: if 'output.listingCards' is present,
      parse the HTML string and delegate to HTML heuristics.
    - Otherwise, traverse dict/list looking for popularity hints and IDs.
    Returns (popular_ids, all_listing_ids_found).
    """
    # Handle listingCards JSON envelope containing HTML string
    try:
        if isinstance(data, dict):
            output = data.get("output")
            if isinstance(output, dict):
                listing_cards_html = output.get("listingCards")
                if isinstance(listing_cards_html, str):
                    # Delegate to HTML parser (which extracts data-listing-id and popular badge/param)
                    return find_popular_now_ids_from_html(listing_cards_html)
    except Exception:
        pass

    # Generic traversal fallback
    all_ids: List[int] = []
    popular_ids: List[int] = []

    def parse_int_like(x) -> Optional[int]:
        try:
            xi = int(str(x))
            return xi
        except Exception:
            return None

    def has_popular_flag(node: Dict) -> bool:
        for k, v in node.items():
            lk = str(k).lower()
            if lk in ("popular", "popular_now", "is_popular", "pop"):
                if isinstance(v, (bool, int)) and bool(v):
                    return True
                if isinstance(v, str) and "popular" in v.lower():
                    return True
            if lk in ("badge", "badges", "labels", "ranked_badges", "badges_text", "label"):
                if isinstance(v, str) and "popular" in v.lower():
                    return True
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, str) and "popular" in item.lower():
                            return True
                        if isinstance(item, dict):
                            for vv in item.values():
                                if isinstance(vv, str) and "popular" in vv.lower():
                                    return True
                if isinstance(v, dict):
                    for vv in v.values():
                        if isinstance(vv, str) and "popular" in vv.lower():
                            return True
            if lk in ("href", "url"):
                try:
                    s = str(v).lower()
                    if "pop=1" in s:
                        return True
                except Exception:
                    pass
        return False

    def traverse(node, popular_context: bool = False):
        try:
            if isinstance(node, dict):
                this_popular = popular_context or has_popular_flag(node)

                lid = None
                if "listing_id" in node:
                    lid = parse_int_like(node.get("listing_id"))
                elif "id" in node:
                    lid = parse_int_like(node.get("id"))

                if lid is not None:
                    all_ids.append(lid)
                    if this_popular:
                        popular_ids.append(lid)

                for v in node.values():
                    traverse(v, this_popular)

            elif isinstance(node, list):
                for item in node:
                    traverse(item, popular_context)
        except Exception:
            return

    traverse(data, popular_context=False)

    # Deduplicate preserving order
    seen_all = set()
    dedup_all: List[int] = []
    for lid in all_ids:
        if lid not in seen_all:
            seen_all.add(lid)
            dedup_all.append(lid)

    seen_pop = set()
    dedup_pop: List[int] = []
    for lid in popular_ids:
        if lid not in seen_pop:
            seen_pop.add(lid)
            dedup_pop.append(lid)

    return dedup_pop, dedup_all

def find_popular_now_ids_from_html(html_str: str) -> Tuple[List[int], List[int]]:
    """
    Identify listing cards marked as 'Popular now'.
    Works for:
    - HTML pages: looks for badge text, CSS hints, or 'pop=1' in href.
    - JSON responses: falls back to structured traversal if the response is JSON.
    Returns (popular_ids, all_listing_ids_found).
    """
    if not html_str:
        return [], []

    text = html_str.strip()

    # If the response is JSON, parse and delegate to JSON traversal
    if text.startswith("{") or text.startswith("["):
        try:
            obj = json.loads(text)
            return find_popular_now_ids_from_json(obj)
        except Exception:
            # Fall back to HTML heuristics if JSON parsing fails
            pass

    # HTML heuristics
    # Extract all listing IDs present in the HTML
    all_html_listing_ids = extract_listing_ids_from_html(text)

    popular_ids: List[int] = []

    # Case-insensitive popular hints
    popular_hint = re.compile(
        r"(popular\s+now|popular-now|wt-badge--popular|ranked-badges-title-bold|badge-popular|popularBadge)",
        flags=re.IGNORECASE,
    )

    # Split by <li> and inspect each block for the badge or 'pop=1'
    li_blocks = re.findall(r"<li[^>]*>.*?</li>", text, flags=re.DOTALL | re.IGNORECASE)
    for block in li_blocks:
        has_pop_text = bool(popular_hint.search(block))
        has_pop_param = ("pop=1" in block.lower())
        if has_pop_text or has_pop_param:
            m = re.search(r'data-listing-id="(\d+)"', block, flags=re.IGNORECASE)
            if m:
                try:
                    popular_ids.append(int(m.group(1)))
                except Exception:
                    pass

    # Fallback: scan around anchors with pop=1 in any href
    if not popular_ids:
        for m in re.finditer(r'href="[^"]+pop=1[^"]*"', text, flags=re.IGNORECASE):
            start = m.start()
            window_start = max(0, start - 1500)
            window = text[window_start:start + 1500]
            mm = re.search(r'data-listing-id="(\d+)"', window, flags=re.IGNORECASE)
            if mm:
                try:
                    popular_ids.append(int(mm.group(1)))
                except Exception:
                    pass

    # Deduplicate preserving order
    seen = set()
    out_pop: List[int] = []
    for lid in popular_ids:
        if lid not in seen:
            seen.add(lid)
            out_pop.append(lid)

    return out_pop, all_html_listing_ids

# --- Cross-reference ---
def cross_reference_popular(search_json: Dict, popular_ids: List[int]) -> List[Dict]:
    """
    Return full listing objects for popular_ids.
    Strategy:
    - First cross-reference against aggregated 'search_results.json' results.
    - For any IDs not found there, fetch listing details directly via Etsy Open API v3.
    """
    if not popular_ids:
        return []

    # Build a map from aggregated search results (if any)
    results = search_json.get("results") or []
    id_to_obj: Dict[int, Dict] = {}
    for r in results:
        lid = r.get("listing_id")
        try:
            lid_int = int(str(lid))
            id_to_obj[lid_int] = r
        except Exception:
            continue

    found_listings: List[Dict] = []
    missing_ids: List[int] = []

    for pid in popular_ids:
        try:
            pid_int = int(str(pid))
        except Exception:
            continue
        obj = id_to_obj.get(pid_int)
        if obj:
            found_listings.append(obj)
        else:
            missing_ids.append(pid_int)

    if missing_ids:
        # Fetch missing listing details via Open API
        fetched = fetch_listing_details_by_ids(missing_ids)
        # Normalize results into listing objects with 'listing_id' present
        for item in fetched:
            if isinstance(item, dict):
                # Some endpoints may return {"results": [obj]} or the object itself
                if "results" in item and isinstance(item["results"], list) and item["results"]:
                    obj = item["results"][0]
                    if isinstance(obj, dict):
                        found_listings.append(obj)
                else:
                    found_listings.append(item)

    return found_listings

def fetch_listing_details_by_ids(listing_ids: List[int]) -> List[Dict]:
    """
    Fetch listing details for given IDs using Etsy Open API v3.
    Falls back gracefully on errors and handles rate limits via backoff.
    """
    details: List[Dict] = []
    for lid in listing_ids:
        url = f"{OPENAPI_BASE}/v3/application/listings/{lid}"
        headers = {
            "x-api-key": API_KEYSTRING,
            "accept": "application/json",
        }
        # Retry/backoff for 429/5xx
        for attempt in range(5):
            try:
                resp = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT)
                status = resp.status_code
                if status in (429,) or (500 <= status < 600):
                    backoff_sleep(attempt)
                    continue
                try:
                    data = resp.json()
                except Exception:
                    data = None
                if isinstance(data, dict):
                    details.append(data)
                break
            except requests.RequestException:
                backoff_sleep(attempt)
                continue
            except Exception:
                # Skip irrecoverable parsing issues
                break
    return details

# --- UI prompts ---
def prompt_desired_total() -> Optional[int]:
    """
    Ask user to specify product count:
    - Blank / 'all' / 'unlimited' => None (fetch all available)
    - Integer N => up to N
    """
    raw = input("Enter number of products to fetch (1-100 for page size, or 'all'/'unlimited'/blank for everything): ").strip().lower()
    if raw in ("", "all", "unlimited"):
        return None
    try:
        n = int(raw)
        if n <= 0:
            return None
        return n
    except Exception:
        return None


# --- Orchestration: Multi-part cURL execution ---

def write_json_safe(path: str, data) -> None:
    """
    Atomically write JSON to path by writing to a temp file then replacing.
    Safe for concurrent readers and avoids partial writes.
    """
    ensure_output_dir(os.path.dirname(path))
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def write_text_safe(path: str, text: str) -> None:
    ensure_output_dir(os.path.dirname(path))
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text or "")
    os.replace(tmp, path)

def init_popular_queue(queue_path: str, user_id: str, keyword_slug: str, desired_total: Optional[int]) -> None:
    """
    Initialize the per-run 'First Etsy Api Popular Products Queue' file with metadata.
    """
    queue_obj = {
        "queue_name": "First Etsy Api Popular Products Queue",
        "status": "running",
        "user_id": user_id,
        "keyword_slug": keyword_slug,
        "desired_total": desired_total,
        "items": [],
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
    }
    write_json_safe(queue_path, queue_obj)
    print(f"[First Etsy Api Popular Products Queue] user={user_id} started; path={queue_path}")


def append_to_popular_queue(queue_path: str, listing_ids: List[int], source_chunk_index: int, user_id: str) -> None:
    """
    Append new listing IDs to the queue file (dedup by ID).
    Logs a concise message for each append.
    """
    try:
        with open(queue_path, "r", encoding="utf-8") as f:
            queue_obj = json.load(f)
    except Exception:
        queue_obj = {
            "queue_name": "First Etsy Api Popular Products Queue",
            "status": "running",
            "user_id": user_id,
            "keyword_slug": None,
            "desired_total": None,
            "items": [],
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
        }

    existing_ids = {int(item.get("listing_id")) for item in queue_obj.get("items", []) if "listing_id" in item}
    new_items = []
    for lid in listing_ids:
        try:
            lid_int = int(str(lid))
        except Exception:
            continue
        if lid_int in existing_ids:
            continue
        new_items.append({
            "listing_id": lid_int,
            "added_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
            "source_chunk": source_chunk_index,
        })
        existing_ids.add(lid_int)

    if not new_items:
        # No changes; still report current total for caller
        try:
            current_total = len(queue_obj.get("items", []))
        except Exception:
            current_total = 0
        return current_total

    queue_obj.setdefault("items", []).extend(new_items)
    write_json_safe(queue_path, queue_obj)
    total_now = len(queue_obj.get("items", []))
    print(f"[First Etsy Api Popular Products Queue] user={user_id} +{len(new_items)} items (chunk={source_chunk_index}): {[x['listing_id'] for x in new_items]}")
    return total_now

def finalize_popular_queue(queue_path: str, user_id: str, destroy: bool = True) -> None:
    """
    Finalize the queue file, log completion, and optionally delete the queue file.
    """
    try:
        with open(queue_path, "r", encoding="utf-8") as f:
            queue_obj = json.load(f)
    except Exception:
        queue_obj = {"queue_name": "First Etsy Api Popular Products Queue", "items": []}

    queue_obj["status"] = "completed"
    queue_obj["ended_at"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time()))
    queue_obj["total_items"] = len(queue_obj.get("items", []))
    write_json_safe(queue_path, queue_obj)
    print(f"[First Etsy Api Popular Products Queue] user={user_id} completed; items={queue_obj['total_items']}")

    if destroy:
        try:
            os.remove(queue_path)
            print(f"[First Etsy Api Popular Products Queue] user={user_id} queue destroyed at path={queue_path}")
        except Exception as e:
            print(f"[First Etsy Api Popular Products Queue] user={user_id} failed to destroy queue: {e}")

def run_listingcards_curl_for_ids(
    keyword_slug: str,
    listing_ids: List[int],
    helpers_dir: str,
    outputs_dir: str,
    queue_path: Optional[str] = None,
    queue_user_id: Optional[str] = None,
    popular_listings_path: Optional[str] = None,
    search_json: Optional[Dict] = None,
    progress_path: Optional[str] = None,  # NEW: write Splitting progress here
    progress_cb: Optional[callable] = None,  # NEW: streaming progress callback
) -> Dict:
    """
    Execute listingCards cURL in chunks. Save raw and HTML per chunk.
    Extract 'Popular now' IDs per chunk and:
      - Append them to the real-time queue file with user context.
      - Append listing objects in real-time to 'popular_now_listings_{slug}.json'.
    Returns a combined object with aggregated popular IDs and metadata.
    """
    used_curl_path = TEST_TXT_PATH
    try:
        url, method, headers, data_obj, _cookies = parse_curl_file(TEST_TXT_PATH)
    except Exception:
        parsed_ok = False
        requests_root = Path(os.getenv("ETO_REQUESTS_ROOT") or PROJECT_ROOT / "EtoRequests")
        search_txt_root = requests_root / "Search_request" / "txt_files"
        for i in range(1, 11):
            candidate = search_txt_root / str(i) / f"EtoRequest{i}.txt"
            if candidate.exists():
                try:
                    url, method, headers, data_obj, _cookies = parse_curl_file(str(candidate))
                    used_curl_path = str(candidate)
                    parsed_ok = True
                    break
                except Exception:
                    continue
        if not parsed_ok:
            alt_path = requests_root / "Etsy_Search" / "Etreq.txt"
            if not alt_path.exists():
                raise FileNotFoundError(
                    "No cURL template found. Set 'ETO_SEARCH_CURL_PATH' to a valid file, "
                    "or provide 'ETO_REQUESTS_ROOT' pointing to your 'EtoRequests' directory."
                )
            url, method, headers, data_obj, _cookies = parse_curl_file(str(alt_path))
            used_curl_path = str(alt_path)

    curl_orig_path = os.path.join(helpers_dir, f"curl_original_{keyword_slug}.curl")
    try:
        with open(used_curl_path, "r", encoding="utf-8") as f_in, open(curl_orig_path, "w", encoding="utf-8") as f_out:
            f_out.write(f_in.read())
    except Exception as e:
        with open(curl_orig_path, "w", encoding="utf-8") as f_out:
            f_out.write(f"# Error writing original cURL: {e}\n")

    def _init_realtime_popular_file():
        base_obj = {"count": 0, "popular_now_ids": [], "listings": []}
        if popular_listings_path and not os.path.exists(popular_listings_path):
            write_json_safe(popular_listings_path, base_obj)

    def _append_realtime_popular(new_ids: List[int]):
        if not popular_listings_path or not new_ids:
            return
        try:
            if os.path.exists(popular_listings_path):
                with open(popular_listings_path, "r", encoding="utf-8") as f:
                    obj = json.load(f)
            else:
                obj = {"count": 0, "popular_now_ids": [], "listings": []}

            existing_ids = set(int(x) for x in obj.get("popular_now_ids", []))
            unique_new = [int(x) for x in new_ids if int(x) not in existing_ids]
            if not unique_new:
                return

            obj.setdefault("popular_now_ids", []).extend(unique_new)
            obj["popular_now_ids"] = [int(x) for x in obj["popular_now_ids"]]
            obj["count"] = len(set(obj["popular_now_ids"]))

            if search_json:
                listings_objs = cross_reference_popular(search_json, unique_new)
                seen_ids = set()
                for item in obj.get("listings", []):
                    lid = item.get("listing_id")
                    try:
                        seen_ids.add(int(str(lid)))
                    except Exception:
                        continue
                for lo in listings_objs:
                    lid = lo.get("listing_id")
                    try:
                        lid_int = int(str(lid))
                    except Exception:
                        lid_int = None
                    if lid_int and lid_int not in seen_ids:
                        obj.setdefault("listings", []).append(lo)
                        seen_ids.add(lid_int)

            write_json_safe(popular_listings_path, obj)
        except Exception as e:
            print(f"[First Etsy Api Popular Products Queue] realtime write failed: {e}")

    _init_realtime_popular_file()

    chunks = chunk_list(listing_ids, CURL_CHUNK_SIZE)
    aggregate_popular_ids: List[int] = []
    seen_popular_ids: set = set()
    html_ids_all: List[int] = []
    seen_queued_ids: set = set()

    # Emit initial splitting totals
    try:
        if progress_cb:
            total_chunks = len(chunks)
            progress_cb({
                "stage": "splitting",
                "user_id": queue_user_id or "n/a",
                "remaining": total_chunks,
                "total": total_chunks,
                "message": f"Splitting initialized: {total_chunks} parts",
            })
    except Exception:
        pass

    for idx, chunk in enumerate(chunks, start=1):
        try:
            chunk_data = replace_listing_ids_in_curl_data(json.loads(json.dumps(data_obj)), chunk)
        except Exception as e:
            raise ValueError(f"Failed to prepare cURL data for chunk {idx}: {e}")

        curl_cmd_str = build_curl_command(url, method, headers, chunk_data)
        curl_cmd_path = os.path.join(helpers_dir, f"curl_chunk_{keyword_slug}_{idx}.sh")
        try:
            with open(curl_cmd_path, "w", encoding="utf-8") as f:
                f.write(curl_cmd_str + "\n")
        except Exception:
            pass

        status, text, parsed = run_curl_request(url, method, headers, chunk_data)

        raw_path = os.path.join(helpers_dir, f"listingcards_raw_{keyword_slug}_chunk_{idx}.txt")
        html_path = os.path.join(outputs_dir, f"listingcards_html_{keyword_slug}_chunk_{idx}.html")
        helpers_html_path = os.path.join(helpers_dir, f"listingcards_cleaned_{keyword_slug}_chunk_{idx}.html")

        write_text_safe(raw_path, text or "")

        html_out = ""
        try:
            if parsed and isinstance(parsed, dict):
                output = parsed.get("output")
                if isinstance(output, dict) and isinstance(output.get("listingCards"), str):
                    html_out = html.unescape(output.get("listingCards") or "")
            if not html_out:
                html_out = text or ""
        except Exception:
            html_out = text or ""

        write_text_safe(html_path, html_out)
        write_text_safe(helpers_html_path, html_out)

        # Popular detection prefers the extracted HTML string when available
        try:
            if parsed and isinstance(parsed, dict) and isinstance(parsed.get("output", {}).get("listingCards"), str):
                popular_ids_chunk, all_ids_chunk = find_popular_now_ids_from_html(html_out)
            else:
                if parsed and isinstance(parsed, (dict, list)):
                    popular_ids_chunk, all_ids_chunk = find_popular_now_ids_from_json(parsed if isinstance(parsed, dict) else {"root": parsed})
                else:
                    popular_ids_chunk, all_ids_chunk = find_popular_now_ids_from_html(html_out)
        except Exception:
            popular_ids_chunk, all_ids_chunk = [], []
        html_ids_all.extend(all_ids_chunk or [])
        # dedupe popular IDs for this chunk
        chunk_unique: List[int] = []
        _chunk_seen: set = set()
        for _pid in popular_ids_chunk or []:
            try:
                _pid_int = int(str(_pid))
            except Exception:
                continue
            if _pid_int not in _chunk_seen:
                _chunk_seen.add(_pid_int)
                chunk_unique.append(_pid_int)
        new_popular = [pid for pid in chunk_unique if pid not in seen_popular_ids]

        # QUEUE ONLY POPULAR IDS (restrict processing strictly to popular-now)
        new_to_queue = [pid for pid in new_popular if pid not in seen_queued_ids]
        queue_total_after = None
        if queue_path and queue_user_id:
            if new_to_queue:
                try:
                    queue_total_after = append_to_popular_queue(queue_path, new_to_queue, idx, queue_user_id)
                except Exception:
                    queue_total_after = None
                else:
                    for lid in new_to_queue:
                        seen_queued_ids.add(lid)
            else:
                try:
                    with open(queue_path, "r", encoding="utf-8") as f:
                        qobj = json.load(f)
                    queue_total_after = len(qobj.get("items", []))
                except Exception:
                    queue_total_after = None

        if new_popular:
            _append_realtime_popular(new_popular)
            aggregate_popular_ids.extend(new_popular)
            for pid in new_popular:
                seen_popular_ids.add(pid)
        try:
            total_str = f" total_in_queue={queue_total_after}" if queue_total_after is not None else ""
            print(f"[First Etsy Api Popular Products Queue] user={queue_user_id or 'n/a'} chunk={idx} detected_popular={len(popular_ids_chunk)} new_popular_added={len(new_popular)} new_queued={len(new_to_queue)}{total_str}")
        except Exception:
            pass

        # Emit splitting progress after each chunk
        try:
            if progress_cb:
                total_chunks = len(chunks)
                remaining_after = max(0, total_chunks - idx)
                progress_cb({
                    "stage": "splitting",
                    "user_id": queue_user_id or "n/a",
                    "remaining": remaining_after,
                    "total": total_chunks,
                    "message": f"Chunk {idx}/{total_chunks} processed",
                })
        except Exception:
            pass
    
    # After processing all chunks, return unique IDs
    aggregate_popular_ids = list(dict.fromkeys(int(x) for x in aggregate_popular_ids))
    return {
        "popular_now_ids": aggregate_popular_ids,
        "html_listing_ids_all": html_ids_all,
        "curl_original_path": used_curl_path,
        "curl_template_copy": curl_orig_path,
        "chunks": len(chunks),
    }

# --- Main flow ---
def main():
    raise SystemExit("API-only usage. Start the FastAPI server and POST to /run or /run-script")

if __name__ == "__main__":
    main()