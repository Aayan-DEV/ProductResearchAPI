import os
import re
import json
import logging
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from uuid import uuid4

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests

# Logging setup
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("keyword_api")

# Use paths relative to this file so Railway deployments resolve correctly
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_ETREQ_PATH = str(SCRIPT_DIR / "Etsy_Search" / "Etreq.txt")
DEFAULT_OUTPUT_JSON_PATH = str(SCRIPT_DIR / "Etsy_Search" / "example.json")

ETREQ_PATH = os.getenv("ETREQ_PATH", DEFAULT_ETREQ_PATH)
OUTPUT_JSON_PATH = os.getenv("OUTPUT_JSON_PATH", DEFAULT_OUTPUT_JSON_PATH)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

app = Flask(__name__)
CORS(app)


def error_response(
    code: str,
    message: str,
    http_status: int,
    request_id: str,
    hint: str | None = None,
    details: str | None = None,
    context: dict | None = None,
):
    payload = {
        "error": {
            "code": code,
            "message": message,
            "requestId": request_id,
        }
    }
    if hint:
        payload["error"]["hint"] = hint
    if details:
        payload["error"]["details"] = details
    if context:
        payload["error"]["context"] = context
    return jsonify(payload), http_status

def load_curl_spec(path: str):
    with open(path, "r", encoding="utf-8") as f:
        lines = f.read().splitlines()

    url = None
    headers = {}
    cookies_segments = []

    for raw in lines:
        line = raw.strip()
        if not line:
            continue

        if line.startswith("--url"):
            m = re.search(r"--url\s+'([^']+)'", line) or re.search(r'--url\s+"([^"]+)"', line)
            if m:
                url = m.group(1)
            continue

        if line.startswith("curl"):
            m = re.search(r"curl\s+'([^']+)'", line) or re.search(r'curl\s+"([^"]+)"', line)
            if m:
                url = m.group(1)
            continue

        if line.startswith("-H") or line.startswith("--header"):
            m = re.search(r"(?:-H|--header)\s+'(.*)'\s*\\?$", line) or re.search(r'(?:-H|--header)\s+"(.*)"\s*\\?$', line)
            if not m:
                logger.warning("Skipping malformed header line")
                continue
            header_literal = m.group(1)
            if ":" not in header_literal:
                logger.warning(f"Skipping header without colon: {header_literal[:80]}...")
                continue
            name, value = header_literal.split(":", 1)
            name = name.strip()
            value = value.strip()
            if not name or any(c in name for c in [":", "\r", "\n"]):
                logger.warning(f"Skipping invalid header name parsed from: {header_literal[:80]}...")
                continue
            headers[name] = value
            continue

        if line.startswith("-b") or line.startswith("--cookie"):
            m = re.search(r"(?:-b|--cookie)\s+'([^']+)'", line) or re.search(r'(?:-b|--cookie)\s+"([^"]+)"', line)
            if m:
                cookies_segments.append(m.group(1).strip())
            continue

    if cookies_segments:
        merged_cookie = "; ".join(cookies_segments)
        if "Cookie" in headers:
            headers["Cookie"] = headers["Cookie"] + "; " + merged_cookie
        else:
            headers["Cookie"] = merged_cookie

    return url, headers

def update_query_in_url(u: str, keyword: str) -> str:
    """
    Replace the 'query' param with the provided keyword.
    This specifically targets the URL like Etreq.txt#L2 where 'query=...' appears.
    """
    parsed = urlparse(u)
    q = parse_qs(parsed.query, keep_blank_values=True)
    q["query"] = [keyword]
    new_query = urlencode(q, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def update_header_referer(headers: dict, keyword: str):
    """
    If a Referer header exists, also update its query=... param so it aligns with the keyword.
    """
    for key in list(headers.keys()):
        if key.lower() == "referer":
            try:
                headers[key] = update_query_in_url(headers[key], keyword)
            except Exception as e:
                logger.debug(f"Failed to update referer header: {e}")
            break


def extract_output_fields(data: dict):
    stats = data.get("stats", {}) or {}
    daily_stats = data.get("dailyStats", {}) or {}
    return {
        "searchVolume": stats.get("searchVolume"),
        "avgTotalListings": stats.get("avgTotalListings"),
        "dailyStats": daily_stats.get("stats", []),
    }


@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"ok": True}), 200

def sanitize_headers_for_requests(headers: dict) -> dict:
    cleaned = {}
    for k, v in headers.items():
        vv = (v or "")
        vv = vv.replace("\r", "").replace("\n", "")
        try:
            vv.encode("latin-1")
            cleaned[k] = vv
        except UnicodeEncodeError:
            cleaned[k] = vv.encode("latin-1", "ignore").decode("latin-1", "ignore")
    return cleaned

@app.route("/api/keyword-insights", methods=["POST"])
def keyword_insights():
    request_id = str(uuid4())

    payload = request.get_json(silent=True) or {}
    keyword = payload.get("keyword") or request.form.get("keyword")
    if not keyword or not keyword.strip():
        return error_response(
            code="ERR_BAD_KEYWORD",
            message="Keyword is missing or empty.",
            http_status=400,
            request_id=request_id,
            hint="Send JSON body like {'keyword': 'your term'} or form-data 'keyword'.",
            context={"received": keyword},
        )

    # Validate presence of curl spec file before parsing
    if not Path(ETREQ_PATH).is_file():
        logger.error(f"[{request_id}] Curl spec not found at path: {ETREQ_PATH}")
        return error_response(
            code="ERR_SPEC_NOT_FOUND",
            message="Curl spec file not found.",
            http_status=500,
            request_id=request_id,
            hint="Ensure Etreq.txt exists or set ETREQ_PATH to a valid path.",
            context={"path": ETREQ_PATH, "cwd": str(Path.cwd())},
        )

    try:
        logger.info(f"[{request_id}] Using curl spec at: {ETREQ_PATH}")
        url, headers = load_curl_spec(ETREQ_PATH)
        if not url:
            return error_response(
                code="ERR_SPEC_PARSE_URL",
                message="Could not parse URL from curl spec.",
                http_status=500,
                request_id=request_id,
                hint="Check that a line like --url 'https://...' exists and is properly quoted.",
                context={"path": ETREQ_PATH},
            )

        updated_url = update_query_in_url(url, keyword.strip())
        update_header_referer(headers, keyword.strip())
        headers = sanitize_headers_for_requests(headers)

        logger.info(f"[{request_id}] Requesting Etsy insights for keyword='{keyword.strip()}'")

        try:
            resp = requests.get(updated_url, headers=headers, timeout=REQUEST_TIMEOUT)
        except requests.exceptions.Timeout as e:
            logger.error(f"[{request_id}] Timeout contacting Etsy: {e}")
            return error_response(
                code="ERR_HTTP_TIMEOUT",
                message="Timed out contacting Etsy.",
                http_status=504,
                request_id=request_id,
                hint="Etsy may be slow or blocking requests. Try again or increase REQUEST_TIMEOUT.",
                context={"timeoutSeconds": REQUEST_TIMEOUT, "url": updated_url},
            )
        except requests.exceptions.ConnectionError as e:
            logger.error(f"[{request_id}] Connection error contacting Etsy: {e}")
            return error_response(
                code="ERR_HTTP_CONNECTION",
                message="Network error contacting Etsy.",
                http_status=502,
                request_id=request_id,
                hint="Check network connectivity and VPN/proxy settings.",
                context={"url": updated_url},
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"[{request_id}] Request failed: {e}")
            return error_response(
                code="ERR_HTTP_REQUEST",
                message="Unexpected error during HTTP request.",
                http_status=502,
                request_id=request_id,
                hint="Inspect server logs for details; verify headers and cookies.",
                details=str(e),
                context={"url": updated_url},
            )
        except UnicodeEncodeError as e:
            logger.error(f"[{request_id}] Header encoding error: {e}")
            return error_response(
                code="ERR_HEADER_ENCODING",
                message="Invalid header characters (must be Latin-1).",
                http_status=400,
                request_id=request_id,
                hint="Sanitize cookies/headers; remove non-ASCII characters such as smart quotes.",
                details=str(e),
                context={"url": updated_url},
            )

        # Non-2xx responses
        try:
            resp.raise_for_status()
        except requests.HTTPError as http_err:
            status = getattr(http_err.response, "status_code", None)
            reason = getattr(http_err.response, "reason", "")
            details_text = getattr(http_err.response, "text", "")
            logger.error(
                f"[{request_id}] HTTP error from Etsy: {status} {reason}; details: {details_text[:1000]}"
            )
            return error_response(
                code="ERR_HTTP_STATUS",
                message=f"HTTP {status} from Etsy: {reason or 'Unknown'}",
                http_status=502,
                request_id=request_id,
                hint="Etsy may be rejecting the request. Refresh cookies/headers from your browser (e.g., datadome, x-page-guid).",
                details=details_text[:2000],
                context={"status": status, "url": updated_url},
            )

        # Parse JSON
        try:
            data = resp.json()
        except (ValueError, json.JSONDecodeError) as e:
            txt = resp.text
            logger.error(f"[{request_id}] Failed to decode JSON from Etsy response: {e}")
            return error_response(
                code="ERR_JSON_DECODE",
                message="Invalid JSON response from Etsy.",
                http_status=502,
                request_id=request_id,
                hint="Etsy returned non-JSON content. Verify the endpoint and headers.",
                details=txt[:2000],
                context={"contentType": resp.headers.get("Content-Type"), "url": updated_url},
            )

        # Save full raw JSON (best-effort)
        try:
            os.makedirs(os.path.dirname(OUTPUT_JSON_PATH), exist_ok=True)
            with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(
                f"[{request_id}] Failed to write output JSON to '{OUTPUT_JSON_PATH}': {e}. "
                f"Hint: ensure directory exists and is writable."
            )

        return jsonify(extract_output_fields(data)), 200

    except Exception as e:
        logger.exception(f"[{request_id}] Unhandled server error")
        return error_response(
            code="ERR_UNHANDLED",
            message="Unexpected server error.",
            http_status=500,
            request_id=request_id,
            hint="Check server logs for full traceback.",
            details=str(e),
        )


if __name__ == "__main__":
    # Local dev on port 3000 by default
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "3000")))