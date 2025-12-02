# module: main.py (add startup model check and /ready endpoint)
import os
import sys
import time
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, Callable
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
import psycopg

# Ensure Helpers are importable and import singlesearch helper
PROJECT_ROOT = Path(__file__).resolve().parent
HELPERS_DIR = PROJECT_ROOT / "Helpers"
if str(HELPERS_DIR) not in sys.path:
    sys.path.insert(0, str(HELPERS_DIR))

import first_etsy_api_use as etsy
import testing_ai_keywords as ai
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

DB_TABLE_ENTRIES = "ranked_entries"
DB_TABLE_BATCHES = "ranked_batches"
DB_TABLE_REVIEWS = "ranked_entry_reviews"
DB_TABLE_SINGLE_SEARCHES = "single_searches"


def _db_settings() -> Optional[Dict[str, str]]:
    dsn = (
        os.getenv("DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("RAILWAY_DATABASE_URL")
    )
    if not dsn:
        return None
    return {"DB_DSN": dsn}


def _db_connect():
    cfg = _db_settings()
    if not cfg:
        print("[db] ⚠️ No database configuration found. Check DB_URL, DATABASE_URL, or RAILWAY_DATABASE_URL env vars.", flush=True)
        return None
    try:
        conn = psycopg.connect(cfg["DB_DSN"])
        print(f"[db] ✅ Database connection established", flush=True)
        return conn
    except Exception as e:
        print(f"[db] ❌ Failed to connect to database: {e}", flush=True)
        import traceback
        print(f"[db] Traceback: {traceback.format_exc()}", flush=True)
        return None


def _ensure_db_tables(conn) -> None:
    print("[db] Ensuring database tables exist...", flush=True)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DB_TABLE_BATCHES} (
                batch_id TEXT PRIMARY KEY,
                user_id TEXT,
                session_id TEXT NOT NULL,
                keyword TEXT,
                keyword_slug TEXT,
                entries_count INTEGER DEFAULT 0,
                stored_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DB_TABLE_ENTRIES} (
                batch_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                user_id TEXT,
                keyword TEXT,
                keyword_slug TEXT,
                listing_id BIGINT NOT NULL,
                ranking DOUBLE PRECISION,
                demand DOUBLE PRECISION,
                title TEXT,
                url TEXT,
                etsy_user_id TEXT,
                shop_id TEXT,
                state TEXT,
                description TEXT,
                created_at_ts BIGINT,
                created_at TIMESTAMPTZ,
                last_modified_ts BIGINT,
                last_modified_at TIMESTAMPTZ,
                image_url TEXT,
                image_srcset TEXT,
                tags JSONB DEFAULT '[]'::jsonb,
                materials JSONB DEFAULT '[]'::jsonb,
                keywords JSONB DEFAULT '[]'::jsonb,
                variations JSONB DEFAULT '[]'::jsonb,
                quantity INTEGER,
                num_favorers INTEGER,
                listing_type TEXT DEFAULT '',
                file_data TEXT DEFAULT '',
                views INTEGER,
                price_value DOUBLE PRECISION,
                price_currency TEXT DEFAULT '',
                price_display TEXT DEFAULT '',
                price_amount_raw DOUBLE PRECISION,
                price_divisor_raw INTEGER,
                sale_percent DOUBLE PRECISION,
                sale_price_value DOUBLE PRECISION,
                sale_price_display TEXT DEFAULT '',
                sale_original_price DOUBLE PRECISION,
                sale_original_price_display TEXT DEFAULT '',
                sale_active_promotion_id TEXT DEFAULT '',
                sale_active_promotion_start_ts BIGINT,
                sale_active_promotion_end_ts BIGINT,
                sale_active_promotion_created_ts BIGINT,
                sale_active_promotion_updated_ts BIGINT,
                sale_active_promotion_description TEXT DEFAULT '',
                free_shipping BOOLEAN,
                buyer_promotion_name TEXT DEFAULT '',
                buyer_shop_promotion_name TEXT DEFAULT '',
                buyer_promotion_description TEXT DEFAULT '',
                buyer_applied_promotion_description TEXT DEFAULT '',
                keyword_insights JSONB DEFAULT '[]'::jsonb,
                demand_extras JSONB DEFAULT '{{}}'::jsonb,
                shop_sections JSONB DEFAULT '[]'::jsonb,
                shop_reviews JSONB DEFAULT '[]'::jsonb,
                shop_languages JSONB DEFAULT '[]'::jsonb,
                shop_created_ts BIGINT,
                shop_created_at TIMESTAMPTZ,
                shop_updated_ts BIGINT,
                shop_updated_at TIMESTAMPTZ,
                shop_details JSONB DEFAULT '{{}}'::jsonb,
                reviews JSONB DEFAULT '[]'::jsonb,
                review_count INTEGER,
                review_average DOUBLE PRECISION,
                is_personalizable BOOLEAN,
                personalization_is_required BOOLEAN,
                personalization_char_count_max INTEGER,
                personalization_instructions TEXT DEFAULT '',
                processing_min INTEGER,
                processing_max INTEGER,
                who_made TEXT DEFAULT '',
                when_made TEXT DEFAULT '',
                production_partners JSONB DEFAULT '[]'::jsonb,
                icon_url_fullxfull TEXT DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (session_id, listing_id)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE_ENTRIES}_batch ON {DB_TABLE_ENTRIES} (batch_id)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE_ENTRIES}_keyword ON {DB_TABLE_ENTRIES} (keyword_slug)"
        )
        
        # Migration: Add missing columns if they don't exist
        # Check if table exists and has old schema, then add new columns
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{DB_TABLE_ENTRIES}'
        """)
        existing_columns = {row[0] for row in cur.fetchall()}
        
        # List of columns to add if missing
        columns_to_add = [
            ("created_at_ts", "BIGINT"),
            ("created_at", "TIMESTAMPTZ"),
            ("last_modified_ts", "BIGINT"),
            ("last_modified_at", "TIMESTAMPTZ"),
            ("quantity", "INTEGER"),
            ("num_favorers", "INTEGER"),
            ("listing_type", "TEXT DEFAULT ''"),
            ("file_data", "TEXT DEFAULT ''"),
            ("views", "INTEGER"),
            ("price_currency", "TEXT DEFAULT ''"),
            ("price_display", "TEXT DEFAULT ''"),
            ("price_amount_raw", "DOUBLE PRECISION"),
            ("price_divisor_raw", "INTEGER"),
            ("sale_percent", "DOUBLE PRECISION"),
            ("sale_price_display", "TEXT DEFAULT ''"),
            ("sale_original_price", "DOUBLE PRECISION"),
            ("sale_original_price_display", "TEXT DEFAULT ''"),
            ("sale_active_promotion_id", "TEXT DEFAULT ''"),
            ("sale_active_promotion_start_ts", "BIGINT"),
            ("sale_active_promotion_end_ts", "BIGINT"),
            ("sale_active_promotion_created_ts", "BIGINT"),
            ("sale_active_promotion_updated_ts", "BIGINT"),
            ("sale_active_promotion_description", "TEXT DEFAULT ''"),
            ("free_shipping", "BOOLEAN"),
            ("buyer_promotion_name", "TEXT DEFAULT ''"),
            ("buyer_shop_promotion_name", "TEXT DEFAULT ''"),
            ("buyer_promotion_description", "TEXT DEFAULT ''"),
            ("buyer_applied_promotion_description", "TEXT DEFAULT ''"),
            ("demand_extras", "JSONB DEFAULT '{}'::jsonb"),
            ("shop_sections", "JSONB DEFAULT '[]'::jsonb"),
            ("shop_reviews", "JSONB DEFAULT '[]'::jsonb"),
            ("shop_languages", "JSONB DEFAULT '[]'::jsonb"),
            ("shop_created_ts", "BIGINT"),
            ("shop_created_at", "TIMESTAMPTZ"),
            ("shop_updated_ts", "BIGINT"),
            ("shop_updated_at", "TIMESTAMPTZ"),
            ("shop_details", "JSONB DEFAULT '{}'::jsonb"),
            ("reviews", "JSONB DEFAULT '[]'::jsonb"),
            ("is_personalizable", "BOOLEAN"),
            ("personalization_is_required", "BOOLEAN"),
            ("personalization_char_count_max", "INTEGER"),
            ("personalization_instructions", "TEXT DEFAULT ''"),
            ("processing_min", "INTEGER"),
            ("processing_max", "INTEGER"),
            ("who_made", "TEXT DEFAULT ''"),
            ("when_made", "TEXT DEFAULT ''"),
            ("production_partners", "JSONB DEFAULT '[]'::jsonb"),
            ("icon_url_fullxfull", "TEXT DEFAULT ''"),
        ]
        
        for col_name, col_type in columns_to_add:
            if col_name not in existing_columns:
                try:
                    # Use SAVEPOINT to handle errors per column without aborting entire transaction
                    savepoint_name = f"sp_add_{col_name}"
                    cur.execute(f"SAVEPOINT {savepoint_name}")
                    # Build SQL - col_type already has the correct syntax, just substitute table and column names
                    # Use % formatting to avoid issues with {} in JSONB defaults
                    sql = "ALTER TABLE %s ADD COLUMN %s %s" % (DB_TABLE_ENTRIES, col_name, col_type)
                    cur.execute(sql)
                    cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    print(f"[db] ✅ Added column {col_name} to {DB_TABLE_ENTRIES}", flush=True)
                except Exception as e:
                    try:
                        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    except:
                        pass
                    print(f"[db] ⚠️ Failed to add column {col_name}: {e}", flush=True)
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DB_TABLE_REVIEWS} (
                session_id TEXT NOT NULL,
                listing_id BIGINT NOT NULL,
                review_key TEXT NOT NULL,
                buyer_user_id TEXT,
                rating DOUBLE PRECISION,
                review TEXT,
                created_timestamp BIGINT,
                updated_timestamp BIGINT,
                language TEXT,
                image_url_fullxfull TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (session_id, listing_id, review_key)
            )
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE_REVIEWS}_listing ON {DB_TABLE_REVIEWS} (listing_id)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE_REVIEWS}_session ON {DB_TABLE_REVIEWS} (session_id)"
        )
        # Single searches table (matches Django SingleSearch model)
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {DB_TABLE_SINGLE_SEARCHES} (
                user_id TEXT NOT NULL,
                listing_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                title TEXT DEFAULT '',
                url TEXT DEFAULT '',
                demand DOUBLE PRECISION,
                ranking DOUBLE PRECISION,
                etsy_user_id TEXT,
                shop_id TEXT,
                state TEXT DEFAULT '',
                description TEXT DEFAULT '',
                created_at_ts BIGINT,
                created_at TIMESTAMPTZ,
                last_modified_ts BIGINT,
                last_modified_at TIMESTAMPTZ,
                image_url TEXT DEFAULT '',
                image_srcset TEXT DEFAULT '',
                tags JSONB DEFAULT '[]'::jsonb,
                materials JSONB DEFAULT '[]'::jsonb,
                keywords JSONB DEFAULT '[]'::jsonb,
                variations JSONB DEFAULT '[]'::jsonb,
                quantity INTEGER,
                num_favorers INTEGER,
                listing_type TEXT DEFAULT '',
                file_data TEXT DEFAULT '',
                views INTEGER,
                price_value DOUBLE PRECISION,
                price_currency TEXT DEFAULT '',
                price_display TEXT DEFAULT '',
                price_amount_raw DOUBLE PRECISION,
                price_divisor_raw INTEGER,
                sale_percent DOUBLE PRECISION,
                sale_price_value DOUBLE PRECISION,
                sale_price_display TEXT DEFAULT '',
                sale_original_price DOUBLE PRECISION,
                sale_original_price_display TEXT DEFAULT '',
                sale_active_promotion_id TEXT DEFAULT '',
                sale_active_promotion_start_ts BIGINT,
                sale_active_promotion_end_ts BIGINT,
                sale_active_promotion_created_ts BIGINT,
                sale_active_promotion_updated_ts BIGINT,
                sale_active_promotion_description TEXT DEFAULT '',
                free_shipping BOOLEAN,
                buyer_promotion_name TEXT DEFAULT '',
                buyer_shop_promotion_name TEXT DEFAULT '',
                buyer_promotion_description TEXT DEFAULT '',
                buyer_applied_promotion_description TEXT DEFAULT '',
                keyword_insights JSONB DEFAULT '[]'::jsonb,
                demand_extras JSONB DEFAULT '{{}}'::jsonb,
                shop_sections JSONB DEFAULT '[]'::jsonb,
                shop_reviews JSONB DEFAULT '[]'::jsonb,
                shop_languages JSONB DEFAULT '[]'::jsonb,
                shop_created_ts BIGINT,
                shop_created_at TIMESTAMPTZ,
                shop_updated_ts BIGINT,
                shop_updated_at TIMESTAMPTZ,
                shop_details JSONB DEFAULT '{{}}'::jsonb,
                reviews JSONB DEFAULT '[]'::jsonb,
                review_count INTEGER,
                review_average DOUBLE PRECISION,
                processing_min INTEGER,
                processing_max INTEGER,
                who_made TEXT DEFAULT '',
                when_made TEXT DEFAULT '',
                is_personalizable BOOLEAN,
                personalization_is_required BOOLEAN,
                personalization_char_count_max INTEGER,
                personalization_instructions TEXT DEFAULT '',
                production_partners JSONB DEFAULT '[]'::jsonb,
                icon_url_fullxfull TEXT DEFAULT '',
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (user_id, listing_id, session_id)
            )
            """
        )
        
        # Migration: Add missing columns for single_searches if they don't exist
        cur.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{DB_TABLE_SINGLE_SEARCHES}'
        """)
        existing_single_columns = {row[0] for row in cur.fetchall()}
        
        single_columns_to_add = [
            ("processing_min", "INTEGER"),
            ("processing_max", "INTEGER"),
            ("who_made", "TEXT DEFAULT ''"),
            ("when_made", "TEXT DEFAULT ''"),
            ("is_personalizable", "BOOLEAN"),
            ("personalization_is_required", "BOOLEAN"),
            ("personalization_char_count_max", "INTEGER"),
            ("personalization_instructions", "TEXT DEFAULT ''"),
            ("production_partners", "JSONB DEFAULT '[]'::jsonb"),
            ("icon_url_fullxfull", "TEXT DEFAULT ''"),
            ("taxonomy_id", "INTEGER"),
        ]
        
        for col_name, col_type in single_columns_to_add:
            if col_name not in existing_single_columns:
                try:
                    savepoint_name = f"sp_add_single_{col_name}"
                    cur.execute(f"SAVEPOINT {savepoint_name}")
                    sql = "ALTER TABLE %s ADD COLUMN %s %s" % (DB_TABLE_SINGLE_SEARCHES, col_name, col_type)
                    cur.execute(sql)
                    cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
                    print(f"[db] ✅ Added column {col_name} to {DB_TABLE_SINGLE_SEARCHES}", flush=True)
                except Exception as e:
                    try:
                        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                    except:
                        pass
                    print(f"[db] ⚠️ Failed to add column {col_name} to {DB_TABLE_SINGLE_SEARCHES}: {e}", flush=True)
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE_SINGLE_SEARCHES}_user_created ON {DB_TABLE_SINGLE_SEARCHES} (user_id, created_at DESC)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{DB_TABLE_SINGLE_SEARCHES}_listing ON {DB_TABLE_SINGLE_SEARCHES} (listing_id)"
        )
        conn.commit()
        print(f"[db] ✅ Database tables ensured: {DB_TABLE_BATCHES}, {DB_TABLE_ENTRIES}, {DB_TABLE_REVIEWS}, {DB_TABLE_SINGLE_SEARCHES}", flush=True)



def _ts_to_strings(ts: Optional[Any]) -> Tuple[Optional[int], Optional[str], Optional[str]]:
    if ts in (None, "", 0):
        return None, None, None
    try:
        ts_int = int(float(ts))
        dt = datetime.fromtimestamp(ts_int, tz=timezone.utc)
        return ts_int, dt.isoformat(), dt.strftime("%b %d, %Y")
    except Exception:
        try:
            return int(ts), None, str(ts)
        except Exception:
            return None, None, str(ts)


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", False):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, "", False):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _parse_sale_percent(sale_info: Optional[Dict[str, Any]]) -> Optional[float]:
    if not sale_info:
        return None
    active = sale_info.get("active_promotion") or {}
    promo_text = (
        active.get("buyer_applied_promotion_description")
        or active.get("buyer_promotion_description")
        or ""
    )
    match = re.search(r"(\\d+(?:\\.\\d+)?)\\s*%", promo_text)
    if match:
        try:
            return float(match.group(1))
        except Exception:
            pass
    seller_promo = active.get("seller_marketing_promotion") or {}
    pct = seller_promo.get("order_discount_pct") or seller_promo.get("items_in_set_discount_pct")
    return _safe_float(pct)


def _build_variations(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    variations_cleaned = entry.get("variations_cleaned") or entry.get("popular_info", {}).get("variations_cleaned") or {}
    variations = []
    raw_list = variations_cleaned.get("variations")
    if isinstance(raw_list, list):
        for v in raw_list:
            if not isinstance(v, dict):
                continue
            options_raw = v.get("options") or []
            options = []
            if isinstance(options_raw, list):
                for o in options_raw:
                    if isinstance(o, dict):
                        options.append(
                            {
                                "value": o.get("value"),
                                "label": o.get("label"),
                            }
                        )
            variations.append(
                {
                    "id": v.get("id"),
                    "title": v.get("title"),
                    "options": options,
                }
            )
    return variations


def _collect_reviews(entry: Dict[str, Any], listing_id: Optional[int]) -> Tuple[List[Dict[str, Any]], Optional[int], Optional[float]]:
    shop = entry.get("shop") or {}
    shop_reviews = shop.get("reviews") or []
    reviews = []
    if listing_id is None:
        return reviews, None, None
    lid_str = str(listing_id)
    for rv in shop_reviews:
        if not isinstance(rv, dict):
            continue
        rv_lid = rv.get("listing_id")
        if rv_lid is not None and str(rv_lid) != lid_str:
            continue
        created_iso = None
        created_display = None
        created_ts, created_iso, created_display = _ts_to_strings(rv.get("created_timestamp") or rv.get("create_timestamp"))
        updated_ts, updated_iso, updated_display = _ts_to_strings(rv.get("updated_timestamp") or rv.get("update_timestamp"))
        reviews.append(
            {
                "shop_id": rv.get("shop_id"),
                "listing_id": rv_lid,
                "transaction_id": rv.get("transaction_id"),
                "buyer_user_id": rv.get("buyer_user_id"),
                "rating": rv.get("rating"),
                "review": rv.get("review"),
                "language": rv.get("language"),
                "image_url_fullxfull": rv.get("image_url_fullxfull"),
                "created_timestamp": created_ts,
                "created_iso": created_iso,
                "created": created_display,
                "updated_timestamp": updated_ts,
                "updated_iso": updated_iso,
                "updated": updated_display,
            }
        )
    review_count = len(reviews) or None
    review_average = None
    if review_count:
        try:
            review_average = round(
                sum(_safe_float(rv.get("rating")) or 0 for rv in reviews) / max(review_count, 1),
                2,
            )
        except Exception:
            review_average = None
    return reviews, review_count, review_average


def _build_keyword_insights(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    everbee = entry.get("everbee") or {}
    results = everbee.get("results") or []
    insights: List[Dict[str, Any]] = []
    if not isinstance(results, list):
        return insights
    for res in results:
        if not isinstance(res, dict):
            continue
        stats_obj = (res.get("response") or {}).get("stats") or {}
        daily_stats_block = (res.get("response") or {}).get("dailyStats") or {}
        daily_list = daily_stats_block.get("stats") or []
        cleaned_daily = []
        if isinstance(daily_list, list):
            for d in daily_list:
                if isinstance(d, dict):
                    cleaned_daily.append({"date": d.get("date"), "searchVolume": d.get("searchVolume")})
        vol = res.get("metrics", {}).get("vol")
        if vol is None:
            vol = stats_obj.get("searchVolume")
        comp = res.get("metrics", {}).get("competition")
        if comp is None:
            comp = stats_obj.get("avgTotalListings")
        insights.append(
            {
                "keyword": res.get("keyword") or res.get("query"),
                "vol": _safe_float(vol),
                "competition": _safe_float(comp),
                "stats": stats_obj,
                "dailyStats": cleaned_daily,
            }
        )
    return insights


def _simplify_ranked_entry(entry: Dict[str, Any]) -> Tuple[Optional[int], Dict[str, Any], Dict[str, Optional[float | int]]]:
    popular = entry.get("popular_info") or {}
    listing_id = entry.get("listing_id") or popular.get("listing_id")
    listing_id = _safe_int(listing_id)
    if listing_id is None:
        return None, {}, {}

    title = entry.get("title") or popular.get("title") or ""
    url = entry.get("url") or popular.get("url") or ""

    demand_candidates = [
        entry.get("demand"),
        entry.get("demand_value"),
        popular.get("demand"),
        popular.get("demand_value"),
    ]
    demand = None
    for cand in demand_candidates:
        demand = _safe_float(cand)
        if demand is not None:
            break

    ranking_candidates = [
        entry.get("ranking"),
        popular.get("ranking"),
        entry.get("Ranking"),
        popular.get("Ranking"),
        entry.get("rank"),
        popular.get("rank"),
    ]
    ranking = None
    for cand in ranking_candidates:
        ranking = _safe_float(cand)
        if ranking is not None:
            break

    user_id = entry.get("user_id") or popular.get("user_id")
    shop_id = entry.get("shop_id") or popular.get("shop_id")
    state = entry.get("state") or popular.get("state") or ""
    description = popular.get("description") or entry.get("description") or ""

    made_ts, made_iso, made_disp = _ts_to_strings(
        popular.get("original_creation_timestamp")
        or popular.get("created_timestamp")
        or entry.get("original_creation_timestamp")
        or entry.get("created_timestamp")
    )
    last_ts, last_iso, last_disp = _ts_to_strings(
        popular.get("last_modified_timestamp") or entry.get("last_modified_timestamp")
    )

    primary_image = entry.get("primary_image") or popular.get("primary_image") or {}
    variations = _build_variations(entry)

    tags = popular.get("tags") or entry.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    materials = popular.get("materials") or entry.get("materials") or []
    if not isinstance(materials, list):
        materials = []
    keywords = entry.get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []

    # Extract price with multiple fallbacks
    price_obj = popular.get("price") or entry.get("price") or {}
    
    # Debug logging for price extraction
    if not price_obj or not isinstance(price_obj, dict):
        # Try to get price from listing_details if available
        listing_details = entry.get("listing_details")
        if isinstance(listing_details, dict):
            price_obj = listing_details.get("price") or {}
        # If still empty, check if price is directly in popular_info at root level
        if not price_obj or not isinstance(price_obj, dict):
            # Last resort: check if there's a price field we missed
            for key in ["price", "Price", "PRICE"]:
                if key in popular:
                    price_obj = popular[key] if isinstance(popular[key], dict) else {}
                    break
    
    price_amount = price_obj.get("amount") if isinstance(price_obj, dict) else None
    price_divisor = price_obj.get("divisor") if isinstance(price_obj, dict) else 1
    price_currency = price_obj.get("currency_code") if isinstance(price_obj, dict) else ""
    
    # Debug: log price extraction
    if listing_id and (not price_amount or price_amount is None):
        print(f"[_simplify_ranked_entry] ⚠️ WARNING: No price_amount for listing_id={listing_id}", flush=True)
        print(f"[_simplify_ranked_entry] 🔍 price_obj: {price_obj}", flush=True)
        print(f"[_simplify_ranked_entry] 🔍 popular.get('price'): {popular.get('price')}", flush=True)
        print(f"[_simplify_ranked_entry] 🔍 entry.get('price'): {entry.get('price')}", flush=True)
        print(f"[_simplify_ranked_entry] 🔍 popular keys: {list(popular.keys())[:20] if isinstance(popular, dict) else 'N/A'}", flush=True)
    
    price_value = None
    price_display = None
    try:
        if price_amount is not None and price_divisor:
            price_value = float(price_amount) / float(price_divisor)
            price_display = f"{price_value:.2f} {price_currency}".strip()
            if listing_id:
                print(f"[_simplify_ranked_entry] ✅ Price extracted for listing_id={listing_id}: value={price_value}, amount={price_amount}, divisor={price_divisor}, currency={price_currency}", flush=True)
    except Exception as e:
        price_value = None
        price_display = None
        if listing_id:
            print(f"[_simplify_ranked_entry] ❌ Error calculating price for listing_id={listing_id}: {e}", flush=True)

    sale_info = entry.get("sale_info") or popular.get("sale_info") or {}
    sale_percent = _parse_sale_percent(sale_info)
    sale_subtotal = sale_info.get("subtotal_after_discount")
    sale_original_price = sale_info.get("original_price")
    sale_price_value = None
    sale_price_display = None
    if isinstance(sale_subtotal, str) and sale_subtotal.strip():
        cleaned = re.sub(r"[^0-9.]", "", sale_subtotal)
        if cleaned:
            sale_price_value = _safe_float(cleaned)
            sale_price_display = sale_subtotal.strip()
    elif price_value is not None and sale_percent is not None:
        try:
            sale_price_value = price_value * (1.0 - (sale_percent / 100.0))
            sale_price_display = f"{sale_price_value:.2f} {price_currency}".strip()
        except Exception:
            sale_price_value = None
            sale_price_display = None

    quantity = entry.get("quantity") or popular.get("quantity")
    num_favorers = entry.get("num_favorers") or popular.get("num_favorers")
    listing_type = entry.get("listing_type") or popular.get("listing_type") or ""
    file_data = entry.get("file_data") or popular.get("file_data") or ""
    views = entry.get("views") or popular.get("views")
    
    # Extract personalization fields
    is_personalizable = entry.get("is_personalizable") or popular.get("is_personalizable")
    if not isinstance(is_personalizable, bool):
        is_personalizable = None
    personalization_is_required = entry.get("personalization_is_required") or popular.get("personalization_is_required")
    if not isinstance(personalization_is_required, bool):
        personalization_is_required = None
    personalization_char_count_max = _safe_int(entry.get("personalization_char_count_max") or popular.get("personalization_char_count_max"))
    personalization_instructions = (entry.get("personalization_instructions") or popular.get("personalization_instructions") or "")
    
    # Extract processing fields
    processing_min = _safe_int(entry.get("processing_min") or popular.get("processing_min"))
    processing_max = _safe_int(entry.get("processing_max") or popular.get("processing_max"))
    who_made = (entry.get("who_made") or popular.get("who_made") or "")
    when_made = (entry.get("when_made") or popular.get("when_made") or "")

    shop = entry.get("shop") or {}
    shop_details = shop.get("details") or {}
    shop_reviews = shop.get("reviews") or []
    if not isinstance(shop_reviews, list):
        shop_reviews = []
    reviews, review_count, review_average = _collect_reviews(entry, listing_id)

    shop_created_ts, shop_created_iso, shop_created_disp = _ts_to_strings(
        shop_details.get("created_timestamp") or shop_details.get("create_date") or shop.get("created_timestamp")
    )
    shop_updated_ts, shop_updated_iso, shop_updated_disp = _ts_to_strings(
        shop_details.get("updated_timestamp") or shop_details.get("update_date") or shop.get("updated_timestamp")
    )

    # Build shop_obj with fallbacks to ensure we get all values
    # Try shop_details first, then fallback to shop root level, then to popular_info.shop
    def _get_shop_field(key, default=None):
        """Get shop field with multiple fallbacks"""
        # Try shop_details first
        value = shop_details.get(key) if isinstance(shop_details, dict) else None
        if value is not None:
            return value
        
        # Try shop root level
        value = shop.get(key) if isinstance(shop, dict) else None
        if value is not None:
            return value
        
        # Try popular_info.shop.details
        popular_shop = popular.get("shop") if isinstance(popular, dict) else None
        if isinstance(popular_shop, dict):
            popular_shop_details = popular_shop.get("details")
            if isinstance(popular_shop_details, dict):
                value = popular_shop_details.get(key)
                if value is not None:
                    return value
            # Try popular_info.shop root level
            value = popular_shop.get(key)
            if value is not None:
                return value
        
        # Debug logging for critical fields
        if listing_id and key in ("shop_name", "user_id", "login_name", "currency_code", "num_favorers", "review_count", "review_average", "sections", "languages", "announcement", "sale_message", "digital_sale_message", "vacation_message"):
            print(f"[_simplify_ranked_entry] ⚠️ shop field '{key}' is None for listing_id={listing_id} (shop_details.type={type(shop_details)}, shop.type={type(shop)})", flush=True)
        
        return default
    
    # Extract sections and languages with proper fallbacks using the helper function
    shop_sections = _get_shop_field("sections") or []
    if not isinstance(shop_sections, list):
        shop_sections = []
    
    shop_languages = _get_shop_field("languages") or []
    if not isinstance(shop_languages, list):
        shop_languages = []
    
    shop_obj = {
        "shop_id": _get_shop_field("shop_id") or shop.get("shop_id"),
        "shop_name": _get_shop_field("shop_name"),
        "user_id": _get_shop_field("user_id"),
        "created_timestamp": shop_created_ts,
        "created_iso": shop_created_iso,
        "created": shop_created_disp,
        "title": _get_shop_field("title"),
        "announcement": _get_shop_field("announcement"),
        "currency_code": _get_shop_field("currency_code"),
        "is_vacation": _get_shop_field("is_vacation"),
        "vacation_message": _get_shop_field("vacation_message"),
        "sale_message": _get_shop_field("sale_message"),
        "digital_sale_message": _get_shop_field("digital_sale_message"),
        "updated_timestamp": shop_updated_ts,
        "updated_iso": shop_updated_iso,
        "updated": shop_updated_disp,
        "listing_active_count": _get_shop_field("listing_active_count"),
        "digital_listing_count": _get_shop_field("digital_listing_count"),
        "login_name": _get_shop_field("login_name"),
        "accepts_custom_requests": _get_shop_field("accepts_custom_requests"),
        "vacation_autoreply": _get_shop_field("vacation_autoreply"),
        "url": _get_shop_field("url") or shop.get("url"),
        "image_url_760x100": _get_shop_field("image_url_760x100"),
        "icon_url_fullxfull": _get_shop_field("icon_url_fullxfull"),
        "num_favorers": _get_shop_field("num_favorers"),
        "languages": shop_languages,  # Use extracted shop_languages
        "review_average": _get_shop_field("review_average") or review_average,  # Fallback to collected review_average
        "review_count": _get_shop_field("review_count") or review_count,  # Fallback to collected review_count
        "sections": shop_sections,  # Use extracted shop_sections
        "reviews": shop_reviews,
        "shipping_from_country_iso": _get_shop_field("shipping_from_country_iso"),
        "transaction_sold_count": _get_shop_field("transaction_sold_count"),
    }

    payload = {
        "listing_id": listing_id,
        "title": title,
        "url": url,
        "demand": demand,
        "ranking": ranking,
        "made_at": made_disp,
        "made_at_iso": made_iso,
        "made_at_ts": made_ts,
        "last_modified": last_disp,
        "last_modified_iso": last_iso,
        "last_modified_timestamp": last_ts,
        "primary_image": {"image_url": primary_image.get("image_url"), "srcset": primary_image.get("srcset")},
        "variations": variations,
        "has_variations": bool(variations),
        "variations_cleaned": entry.get("variations_cleaned") or popular.get("variations_cleaned") or {},
        "user_id": user_id,
        "shop_id": shop_id,
        "state": state,
        "description": description,
        "tags": tags,
        "materials": materials,
        "keywords": keywords,
        "sections": shop_sections,
        "reviews": reviews,
        "review_average": review_average,
        "review_count": review_count,
        "keyword_insights": _build_keyword_insights(entry),
        "sale_percent": sale_percent,
        "sale_price_value": sale_price_value,
        "sale_price_display": sale_price_display,
        "sale_subtotal_after_discount": sale_subtotal,
        "sale_original_price": sale_original_price,
        "price_amount": price_amount,
        "price_divisor": price_divisor,
        "price_currency": price_currency,
        "price_value": price_value,
        "price_display": price_display,
        "quantity": quantity,
        "num_favorers": num_favorers,
        "listing_type": listing_type,
        "file_data": file_data,
        "views": views,
        "is_personalizable": is_personalizable,
        "personalization_is_required": personalization_is_required,
        "personalization_char_count_max": personalization_char_count_max,
        "personalization_instructions": personalization_instructions,
        "processing_min": processing_min,
        "processing_max": processing_max,
        "who_made": who_made,
        "when_made": when_made,
        "production_partners": entry.get("production_partners") or popular.get("production_partners") or [],
        "icon_url_fullxfull": shop_obj.get("icon_url_fullxfull") or shop_details.get("icon_url_fullxfull") or shop.get("icon_url_fullxfull") or "",
        "demand_extras": entry.get("demand_extras") or popular.get("demand_extras") or {},
        "shop": shop_obj,
    }

    summary = {
        "listing_id": listing_id,
        "ranking": ranking,
        "demand": demand,
        "price_value": price_value,
        "sale_price_value": sale_price_value,
        "review_count": review_count,
        "review_average": review_average,
    }
    return listing_id, payload, summary


def _extract_single_search_fields(entry: Dict[str, Any], user_id: str, session_id: str, etsy_user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Extract all fields from entry to match Django SingleSearch model structure.
    Based on Django model's create_from_entry method.
    """
    popular = entry.get("popular_info") or entry.get("popular") or {}
    
    # Extract listing_id - ensure it's a valid integer, then convert to string for storage
    raw_listing_id = entry.get("listing_id") or popular.get("listing_id")
    if raw_listing_id is None:
        raise ValueError("listing_id is required but not found in entry")
    try:
        # Ensure it's an integer first
        listing_id_int = int(raw_listing_id)
        listing_id = str(listing_id_int)
    except (ValueError, TypeError):
        raise ValueError(f"Invalid listing_id: {raw_listing_id}")
    title = (popular.get("title") or entry.get("title") or "")[:512]
    url = (popular.get("url") or entry.get("url") or "")[:1024]
    demand = _safe_float(popular.get("demand") or entry.get("demand") or entry.get("demand_value"))
    
    ranking_raw = (entry.get("ranking") or popular.get("ranking") or entry.get("Ranking") or
                   popular.get("Ranking") or entry.get("rank") or popular.get("rank"))
    ranking = _safe_float(ranking_raw) if (ranking_raw is not None and str(ranking_raw).strip()) else None
    
    extracted_etsy_user_id = str(entry.get("user_id") or popular.get("user_id") or etsy_user_id or "")
    shop_id = str(entry.get("shop_id") or popular.get("shop_id") or "")
    state = (entry.get("state") or popular.get("state") or "")[:64]
    description = (popular.get("description") or entry.get("description") or "")
    
    # Timestamps
    ts = (popular.get("original_creation_timestamp") or popular.get("created_timestamp") or
          entry.get("original_creation_timestamp") or entry.get("created_timestamp"))
    created_at_ts = int(ts) if ts is not None else None
    created_at = None
    if created_at_ts:
        try:
            created_at = datetime.fromtimestamp(created_at_ts, tz=timezone.utc)
        except Exception:
            pass
    
    last_modified_ts = (popular.get("last_modified_timestamp") or entry.get("last_modified_timestamp"))
    last_modified_ts_int = int(last_modified_ts) if last_modified_ts is not None else None
    last_modified_at = None
    if last_modified_ts_int:
        try:
            last_modified_at = datetime.fromtimestamp(last_modified_ts_int, tz=timezone.utc)
        except Exception:
            pass
    
    # Primary image
    primary_image = entry.get("primary_image") or popular.get("primary_image") or {}
    image_url = primary_image.get("image_url") or ""
    image_srcset = primary_image.get("srcset") or ""
    
    # Variations (extract from variations_cleaned)
    variations_cleaned = entry.get("variations_cleaned") or popular.get("variations_cleaned") or {}
    var_variations = []
    vlist = variations_cleaned.get("variations") or []
    if isinstance(vlist, list):
        for v in vlist:
            if isinstance(v, dict):
                vopts = v.get("options") or []
                opts_out = []
                if isinstance(vopts, list):
                    for o in vopts:
                        if isinstance(o, dict):
                            opts_out.append({"value": o.get("value"), "label": o.get("label")})
                var_variations.append({"id": v.get("id"), "title": v.get("title"), "options": opts_out})
    
    # Tags, materials, keywords
    tags = popular.get("tags") or entry.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    materials = popular.get("materials") or entry.get("materials") or []
    if not isinstance(materials, list):
        materials = []
    keywords = entry.get("keywords") or popular.get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []
    
    # Quantity, num_favorers, etc.
    quantity = _safe_int(entry.get("quantity") or popular.get("quantity"))
    num_favorers = _safe_int(entry.get("num_favorers") or popular.get("num_favorers"))
    listing_type = (entry.get("listing_type") or popular.get("listing_type") or "")[:64]
    file_data = entry.get("file_data") or popular.get("file_data") or ""
    views = _safe_int(entry.get("views") or popular.get("views"))
    
    # Price extraction
    price = popular.get("price") or entry.get("price") or {}
    price_amount = price.get("amount") if isinstance(price, dict) else None
    price_divisor = price.get("divisor") if isinstance(price, dict) else None
    price_currency = (price.get("currency_code") or "")[:8] if isinstance(price, dict) else ""
    price_value = None
    price_display = None
    try:
        if isinstance(price_amount, (int, float)) and isinstance(price_divisor, int) and price_divisor:
            price_value = float(price_amount) / int(price_divisor)
            disp = ('{:.2f}'.format(price_value)).rstrip('0').rstrip('.')
            price_display = f'{disp} {price_currency}'.strip()[:64]
    except Exception:
        price_value = None
        price_display = None
    
    # Sale info extraction
    sale_info = popular.get("sale_info") or entry.get("sale_info") or {}
    active_promo = sale_info.get("active_promotion") or {}
    buyer_promotion_name = (active_promo.get("buyer_promotion_name") or "")[:255]
    buyer_shop_promotion_name = (active_promo.get("buyer_shop_promotion_name") or "")[:255]
    buyer_promotion_description = active_promo.get("buyer_promotion_description") or ""
    buyer_applied_promotion_description = active_promo.get("buyer_applied_promotion_description") or ""
    
    sale_percent = _parse_sale_percent(sale_info)
    
    sale_subtotal_after_discount = sale_info.get("subtotal_after_discount")
    sale_original_price_display = sale_info.get("original_price") or ""
    sale_original_price = None
    try:
        if isinstance(sale_original_price_display, str) and sale_original_price_display.strip():
            cleaned_op = re.sub(r'[^0-9.]', '', sale_original_price_display)
            if cleaned_op:
                sale_original_price = float(cleaned_op)
    except Exception:
        sale_original_price = None
    
    sale_price_value = None
    sale_price_display = None
    if isinstance(sale_subtotal_after_discount, str) and sale_subtotal_after_discount.strip():
        sale_price_display = sale_subtotal_after_discount.strip()[:64]
        try:
            cleaned = re.sub(r'[^0-9.]', '', sale_price_display)
            if cleaned:
                sale_price_value = float(cleaned)
        except Exception:
            sale_price_value = None
    elif (price_value is not None) and (sale_percent is not None):
        try:
            sale_price_value = price_value * (1.0 - (sale_percent / 100.0))
            disp = ('{:.2f}'.format(sale_price_value)).rstrip('0').rstrip('.')
            sale_price_display = f'{disp} {price_currency}'.strip()[:64]
        except Exception:
            sale_price_value = None
            sale_price_display = None
    
    sale_active_promotion_id = str(active_promo.get("id") or "")[:64]
    sale_active_promotion_start_ts = _safe_int(active_promo.get("start_timestamp"))
    sale_active_promotion_end_ts = _safe_int(active_promo.get("end_timestamp"))
    sale_active_promotion_created_ts = _safe_int(active_promo.get("created_timestamp"))
    sale_active_promotion_updated_ts = _safe_int(active_promo.get("updated_timestamp"))
    sale_active_promotion_description = (buyer_applied_promotion_description or buyer_promotion_description or "")
    free_shipping = sale_info.get("free_shipping")
    if not isinstance(free_shipping, bool):
        free_shipping = None
    
    # Shop data - use same extraction logic as _simplify_ranked_entry
    shop = popular.get("shop") or entry.get("shop") or {}
    shop_details = shop.get("details") or {}
    if not isinstance(shop_details, dict):
        shop_details = {}
    
    # Helper function to get shop field from multiple sources (same as _simplify_ranked_entry)
    def _get_shop_field_single(key, default=None):
        # Try shop.details first
        value = shop_details.get(key) if isinstance(shop_details, dict) else None
        if value is not None:
            return value
        # Try popular_info.shop.details
        popular_shop = popular.get("shop") or {}
        popular_shop_details = popular_shop.get("details") or {}
        if isinstance(popular_shop_details, dict):
            value = popular_shop_details.get(key)
            if value is not None:
                return value
        # Try popular_info.shop root level
        value = popular_shop.get(key)
        if value is not None:
            return value
        # Try entry.shop root level
        value = shop.get(key)
        if value is not None:
            return value
        return default
    
    shop_created_ts = (_safe_int(_get_shop_field_single("created_timestamp")) or
                      _safe_int(_get_shop_field_single("create_date")) or
                      _safe_int(shop.get("created_timestamp")))
    shop_updated_ts = (_safe_int(_get_shop_field_single("updated_timestamp")) or
                      _safe_int(_get_shop_field_single("update_date")) or
                      _safe_int(shop.get("updated_timestamp")))
    shop_created_at = None
    if shop_created_ts:
        try:
            shop_created_at = datetime.fromtimestamp(shop_created_ts, tz=timezone.utc)
        except Exception:
            pass
    shop_updated_at = None
    if shop_updated_ts:
        try:
            shop_updated_at = datetime.fromtimestamp(shop_updated_ts, tz=timezone.utc)
        except Exception:
            pass
    
    shop_sections = _get_shop_field_single("sections") or []
    if not isinstance(shop_sections, list):
        shop_sections = []
    shop_languages = _get_shop_field_single("languages") or []
    if not isinstance(shop_languages, list):
        shop_languages = []
    
    # Shop reviews (simplified)
    shop_reviews = shop.get("reviews") or []
    shop_reviews_simplified = []
    if isinstance(shop_reviews, list):
        try:
            for rv in shop_reviews:
                if not isinstance(rv, dict):
                    continue
                cts = rv.get("created_timestamp") or rv.get("create_timestamp")
                uts = rv.get("updated_timestamp") or rv.get("update_timestamp")
                c_iso = None
                c_disp = None
                u_iso = None
                u_disp = None
                if cts is not None:
                    try:
                        dt = datetime.fromtimestamp(int(cts), tz=timezone.utc)
                        c_iso = dt.isoformat()
                        c_disp = dt.strftime('%b %d, %Y')
                    except Exception:
                        c_disp = str(cts)
                if uts is not None:
                    try:
                        dt = datetime.fromtimestamp(int(uts), tz=timezone.utc)
                        u_iso = dt.isoformat()
                        u_disp = dt.strftime('%b %d, %Y')
                    except Exception:
                        u_disp = str(uts)
                shop_reviews_simplified.append({
                    "shop_id": rv.get("shop_id"),
                    "listing_id": rv.get("listing_id"),
                    "transaction_id": rv.get("transaction_id"),
                    "buyer_user_id": rv.get("buyer_user_id"),
                    "rating": rv.get("rating"),
                    "review": rv.get("review"),
                    "language": rv.get("language"),
                    "image_url_fullxfull": rv.get("image_url_fullxfull"),
                    "created_timestamp": cts,
                    "created_iso": c_iso,
                    "created": c_disp,
                    "updated_timestamp": uts,
                    "updated_iso": u_iso,
                    "updated": u_disp,
                })
        except Exception:
            shop_reviews_simplified = []
    
    # Listing-specific reviews
    listing_reviews = [rv for rv in shop_reviews_simplified if str(rv.get("listing_id")) == str(listing_id)]
    listing_review_count = len(listing_reviews)
    listing_review_average = None
    if listing_review_count:
        try:
            listing_review_average = round(
                sum((rv.get("rating") or 0) for rv in listing_reviews) / listing_review_count, 2
            )
        except Exception:
            listing_review_average = None
    
    # Keyword insights from everbee
    everbee = entry.get("everbee") or popular.get("everbee") or {}
    everbee_results = everbee.get("results") or []
    keyword_insights = _build_keyword_insights(entry)  # Use existing function
    
    demand_extras = entry.get("demand_extras") or popular.get("demand_extras") or {}
    
    # Shop details compact - use helper function to extract from multiple sources
    shop_details_compact = {}
    try:
        shop_details_compact = {
            "shop_id": (_get_shop_field_single("shop_id") or shop.get("shop_id")),
            "shop_name": _get_shop_field_single("shop_name"),
            "user_id": _get_shop_field_single("user_id"),
            "created_timestamp": shop_created_ts,
            "created_iso": (shop_created_at.isoformat() if shop_created_at else None),
            "created": (shop_created_at.strftime('%b %d, %Y') if shop_created_at else (str(shop_created_ts) if shop_created_ts else None)),
            "title": _get_shop_field_single("title"),
            "announcement": _get_shop_field_single("announcement"),
            "currency_code": _get_shop_field_single("currency_code"),
            "is_vacation": _get_shop_field_single("is_vacation"),
            "vacation_message": _get_shop_field_single("vacation_message"),
            "sale_message": _get_shop_field_single("sale_message"),
            "digital_sale_message": _get_shop_field_single("digital_sale_message"),
            "updated_timestamp": shop_updated_ts,
            "updated_iso": (shop_updated_at.isoformat() if shop_updated_at else None),
            "updated": (shop_updated_at.strftime('%b %d, %Y') if shop_updated_at else (str(shop_updated_ts) if shop_updated_ts else None)),
            "listing_active_count": _get_shop_field_single("listing_active_count"),
            "digital_listing_count": _get_shop_field_single("digital_listing_count"),
            "login_name": _get_shop_field_single("login_name"),
            "accepts_custom_requests": _get_shop_field_single("accepts_custom_requests"),
            "vacation_autoreply": _get_shop_field_single("vacation_autoreply"),
            "url": (_get_shop_field_single("url") or shop.get("url")),
            "image_url_760x100": _get_shop_field_single("image_url_760x100"),
            "icon_url_fullxfull": _get_shop_field_single("icon_url_fullxfull"),
            "num_favorers": _get_shop_field_single("num_favorers"),
            "languages": shop_languages,
            "review_average": _get_shop_field_single("review_average") or listing_review_average,
            "review_count": _get_shop_field_single("review_count") or listing_review_count,
            "shipping_from_country_iso": _get_shop_field_single("shipping_from_country_iso"),
            "transaction_sold_count": _get_shop_field_single("transaction_sold_count"),
        }
    except Exception as e:
        print(f"[_extract_single_search_fields] ⚠️ Error building shop_details_compact: {e}", flush=True)
        shop_details_compact = {
            "shop_id": (_get_shop_field_single("shop_id") or shop.get("shop_id")),
            "url": (_get_shop_field_single("url") or shop.get("url")),
            "languages": shop_languages,
            "review_average": _get_shop_field_single("review_average") or listing_review_average,
            "review_count": _get_shop_field_single("review_count") or listing_review_count,
        }
    
    return {
        "user_id": user_id,
        "listing_id": listing_id,
        "session_id": session_id,
        "title": title,
        "url": url,
        "demand": demand,
        "ranking": ranking,
        "etsy_user_id": extracted_etsy_user_id,
        "shop_id": shop_id,
        "state": state,
        "description": description,
        "created_at_ts": created_at_ts,
        "created_at": created_at,
        "last_modified_ts": last_modified_ts_int,
        "last_modified_at": last_modified_at,
        "image_url": image_url,
        "image_srcset": image_srcset,
        "tags": tags,
        "materials": materials,
        "keywords": keywords,
        "variations": var_variations,
        "quantity": quantity,
        "num_favorers": num_favorers,
        "listing_type": listing_type,
        "file_data": file_data,
        "views": views,
        "price_value": price_value,
        "price_currency": price_currency,
        "price_display": price_display,
        "price_amount_raw": _safe_float(price_amount),
        "price_divisor_raw": _safe_int(price_divisor),
        "sale_percent": sale_percent,
        "sale_price_value": sale_price_value,
        "sale_price_display": sale_price_display,
        "sale_original_price": _safe_float(sale_original_price),
        "sale_original_price_display": sale_original_price_display[:64],
        "sale_active_promotion_id": sale_active_promotion_id,
        "sale_active_promotion_start_ts": sale_active_promotion_start_ts,
        "sale_active_promotion_end_ts": sale_active_promotion_end_ts,
        "sale_active_promotion_created_ts": sale_active_promotion_created_ts,
        "sale_active_promotion_updated_ts": sale_active_promotion_updated_ts,
        "sale_active_promotion_description": sale_active_promotion_description,
        "free_shipping": free_shipping,
        "buyer_promotion_name": buyer_promotion_name,
        "buyer_shop_promotion_name": buyer_shop_promotion_name,
        "buyer_promotion_description": buyer_promotion_description,
        "buyer_applied_promotion_description": buyer_applied_promotion_description,
        "keyword_insights": keyword_insights,
        "demand_extras": demand_extras,
        "shop_sections": shop_sections,
        "shop_reviews": shop_reviews_simplified,
        "shop_languages": shop_languages,
        "shop_created_ts": shop_created_ts,
        "shop_created_at": shop_created_at,
        "shop_updated_ts": shop_updated_ts,
        "shop_updated_at": shop_updated_at,
        "shop_details": shop_details_compact,
        "reviews": listing_reviews,
        "review_count": listing_review_count,
        "review_average": listing_review_average,
        "processing_min": _safe_int(entry.get("processing_min") or popular.get("processing_min")),
        "processing_max": _safe_int(entry.get("processing_max") or popular.get("processing_max")),
        "who_made": (entry.get("who_made") or popular.get("who_made") or "")[:64],
        "when_made": (entry.get("when_made") or popular.get("when_made") or "")[:64],
        "is_personalizable": entry.get("is_personalizable") if isinstance(entry.get("is_personalizable"), bool) else (popular.get("is_personalizable") if isinstance(popular.get("is_personalizable"), bool) else None),
        "personalization_is_required": entry.get("personalization_is_required") if isinstance(entry.get("personalization_is_required"), bool) else (popular.get("personalization_is_required") if isinstance(popular.get("personalization_is_required"), bool) else None),
        "personalization_char_count_max": _safe_int(entry.get("personalization_char_count_max") or popular.get("personalization_char_count_max")),
        "personalization_instructions": (entry.get("personalization_instructions") or popular.get("personalization_instructions") or ""),
        "production_partners": entry.get("production_partners") or popular.get("production_partners") or [],
        "icon_url_fullxfull": (shop_details_compact.get("icon_url_fullxfull") or _get_shop_field_single("icon_url_fullxfull") or ""),
        "taxonomy_id": _safe_int(entry.get("taxonomy_id") or popular.get("taxonomy_id")),
    }

def persist_single_search(entry: Dict[str, Any], *, user_id: str, session_id: str, etsy_user_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Save single search to Railway database in Django SingleSearch model structure.
    """
    cfg = _db_settings()
    if not cfg:
        print("[db] Skipping single search persistence: database env vars not configured.", flush=True)
        return {"saved": 0, "enabled": False}
    
    try:
        fields = _extract_single_search_fields(entry, user_id, session_id, etsy_user_id)
    except Exception as e:
        print(f"[db] Failed to extract single search fields: {e}", flush=True)
        import traceback
        print(f"[db] Traceback: {traceback.format_exc()}", flush=True)
        return {"saved": 0, "enabled": True, "error": str(e)}
    
    conn = _db_connect()
    if conn is None:
        print("[db] Unable to connect to PostgreSQL for single search. Entry not saved.", flush=True)
        return {"saved": 0, "enabled": False}
    
    try:
        _ensure_db_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {DB_TABLE_SINGLE_SEARCHES} (
                    user_id, listing_id, session_id, title, url, demand, ranking,
                    etsy_user_id, shop_id, state, description,
                    created_at_ts, created_at, last_modified_ts, last_modified_at,
                    image_url, image_srcset, tags, materials, keywords, variations,
                    quantity, num_favorers, listing_type, file_data, views,
                    price_value, price_currency, price_display, price_amount_raw, price_divisor_raw,
                    sale_percent, sale_price_value, sale_price_display,
                    sale_original_price, sale_original_price_display,
                    sale_active_promotion_id, sale_active_promotion_start_ts,
                    sale_active_promotion_end_ts, sale_active_promotion_created_ts,
                    sale_active_promotion_updated_ts, sale_active_promotion_description,
                    free_shipping, buyer_promotion_name, buyer_shop_promotion_name,
                    buyer_promotion_description, buyer_applied_promotion_description,
                    keyword_insights, demand_extras,
                    shop_sections, shop_reviews, shop_languages,
                    shop_created_ts, shop_created_at, shop_updated_ts, shop_updated_at,
                    shop_details, reviews, review_count, review_average,
                    processing_min, processing_max, who_made, when_made,
                    is_personalizable, personalization_is_required, personalization_char_count_max, personalization_instructions,
                    production_partners, icon_url_fullxfull, taxonomy_id
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (user_id, listing_id, session_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    demand = EXCLUDED.demand,
                    ranking = EXCLUDED.ranking,
                    etsy_user_id = EXCLUDED.etsy_user_id,
                    shop_id = EXCLUDED.shop_id,
                    state = EXCLUDED.state,
                    description = EXCLUDED.description,
                    created_at_ts = EXCLUDED.created_at_ts,
                    created_at = EXCLUDED.created_at,
                    last_modified_ts = EXCLUDED.last_modified_ts,
                    last_modified_at = EXCLUDED.last_modified_at,
                    image_url = EXCLUDED.image_url,
                    image_srcset = EXCLUDED.image_srcset,
                    tags = EXCLUDED.tags,
                    materials = EXCLUDED.materials,
                    keywords = EXCLUDED.keywords,
                    variations = EXCLUDED.variations,
                    quantity = EXCLUDED.quantity,
                    num_favorers = EXCLUDED.num_favorers,
                    listing_type = EXCLUDED.listing_type,
                    file_data = EXCLUDED.file_data,
                    views = EXCLUDED.views,
                    price_value = EXCLUDED.price_value,
                    price_currency = EXCLUDED.price_currency,
                    price_display = EXCLUDED.price_display,
                    price_amount_raw = EXCLUDED.price_amount_raw,
                    price_divisor_raw = EXCLUDED.price_divisor_raw,
                    sale_percent = EXCLUDED.sale_percent,
                    sale_price_value = EXCLUDED.sale_price_value,
                    sale_price_display = EXCLUDED.sale_price_display,
                    sale_original_price = EXCLUDED.sale_original_price,
                    sale_original_price_display = EXCLUDED.sale_original_price_display,
                    sale_active_promotion_id = EXCLUDED.sale_active_promotion_id,
                    sale_active_promotion_start_ts = EXCLUDED.sale_active_promotion_start_ts,
                    sale_active_promotion_end_ts = EXCLUDED.sale_active_promotion_end_ts,
                    sale_active_promotion_created_ts = EXCLUDED.sale_active_promotion_created_ts,
                    sale_active_promotion_updated_ts = EXCLUDED.sale_active_promotion_updated_ts,
                    sale_active_promotion_description = EXCLUDED.sale_active_promotion_description,
                    free_shipping = EXCLUDED.free_shipping,
                    buyer_promotion_name = EXCLUDED.buyer_promotion_name,
                    buyer_shop_promotion_name = EXCLUDED.buyer_shop_promotion_name,
                    buyer_promotion_description = EXCLUDED.buyer_promotion_description,
                    buyer_applied_promotion_description = EXCLUDED.buyer_applied_promotion_description,
                    keyword_insights = EXCLUDED.keyword_insights,
                    demand_extras = EXCLUDED.demand_extras,
                    shop_sections = EXCLUDED.shop_sections,
                    shop_reviews = EXCLUDED.shop_reviews,
                    shop_languages = EXCLUDED.shop_languages,
                    shop_created_ts = EXCLUDED.shop_created_ts,
                    shop_created_at = EXCLUDED.shop_created_at,
                    shop_updated_ts = EXCLUDED.shop_updated_ts,
                    shop_updated_at = EXCLUDED.shop_updated_at,
                    shop_details = EXCLUDED.shop_details,
                    reviews = EXCLUDED.reviews,
                    review_count = EXCLUDED.review_count,
                    review_average = EXCLUDED.review_average,
                    processing_min = EXCLUDED.processing_min,
                    processing_max = EXCLUDED.processing_max,
                    who_made = EXCLUDED.who_made,
                    when_made = EXCLUDED.when_made,
                    is_personalizable = EXCLUDED.is_personalizable,
                    personalization_is_required = EXCLUDED.personalization_is_required,
                    personalization_char_count_max = EXCLUDED.personalization_char_count_max,
                    personalization_instructions = EXCLUDED.personalization_instructions,
                    production_partners = EXCLUDED.production_partners,
                    icon_url_fullxfull = EXCLUDED.icon_url_fullxfull,
                    taxonomy_id = EXCLUDED.taxonomy_id,
                    updated_at = NOW()
                """,
                (
                    fields["user_id"], fields["listing_id"], fields["session_id"],
                    fields["title"], fields["url"], fields["demand"], fields["ranking"],
                    fields["etsy_user_id"], fields["shop_id"], fields["state"], fields["description"],
                    fields["created_at_ts"], fields["created_at"], fields["last_modified_ts"], fields["last_modified_at"],
                    fields["image_url"], fields["image_srcset"],
                    json.dumps(fields["tags"], ensure_ascii=False),
                    json.dumps(fields["materials"], ensure_ascii=False),
                    json.dumps(fields["keywords"], ensure_ascii=False),
                    json.dumps(fields["variations"], ensure_ascii=False),
                    fields["quantity"], fields["num_favorers"], fields["listing_type"], fields["file_data"], fields["views"],
                    fields["price_value"], fields["price_currency"], fields["price_display"],
                    fields["price_amount_raw"], fields["price_divisor_raw"],
                    fields["sale_percent"], fields["sale_price_value"], fields["sale_price_display"],
                    fields["sale_original_price"], fields["sale_original_price_display"],
                    fields["sale_active_promotion_id"], fields["sale_active_promotion_start_ts"],
                    fields["sale_active_promotion_end_ts"], fields["sale_active_promotion_created_ts"],
                    fields["sale_active_promotion_updated_ts"], fields["sale_active_promotion_description"],
                    fields["free_shipping"], fields["buyer_promotion_name"], fields["buyer_shop_promotion_name"],
                    fields["buyer_promotion_description"], fields["buyer_applied_promotion_description"],
                    json.dumps(fields["keyword_insights"], ensure_ascii=False),
                    json.dumps(fields["demand_extras"], ensure_ascii=False),
                    json.dumps(fields["shop_sections"], ensure_ascii=False),
                    json.dumps(fields["shop_reviews"], ensure_ascii=False),
                    json.dumps(fields["shop_languages"], ensure_ascii=False),
                    fields["shop_created_ts"], fields["shop_created_at"], fields["shop_updated_ts"], fields["shop_updated_at"],
                    json.dumps(fields["shop_details"], ensure_ascii=False),
                    json.dumps(fields["reviews"], ensure_ascii=False),
                    fields["review_count"], fields["review_average"],
                    fields["processing_min"], fields["processing_max"],
                    fields["who_made"], fields["when_made"],
                    fields.get("is_personalizable"),
                    fields.get("personalization_is_required"),
                    fields.get("personalization_char_count_max"),
                    fields.get("personalization_instructions") or "",
                    json.dumps(fields.get("production_partners") or [], ensure_ascii=False),
                    fields.get("icon_url_fullxfull") or "",
                    fields.get("taxonomy_id"),
                ),
            )
        conn.commit()
        print(f"[db] ✅ Committed transaction for single search: listing_id={fields['listing_id']}, user_id={user_id}, session_id={session_id}", flush=True)
        
        # Verify the save worked
        with conn.cursor() as verify_cur:
            verify_cur.execute(
                f"SELECT listing_id, user_id, session_id FROM {DB_TABLE_SINGLE_SEARCHES} WHERE user_id = %s AND listing_id = %s AND session_id = %s",
                (user_id, fields['listing_id'], session_id)
            )
            verify_row = verify_cur.fetchone()
            if verify_row:
                print(f"[db] ✅ Verified: Entry exists in database: listing_id={verify_row[0]}, user_id={verify_row[1]}, session_id={verify_row[2]}", flush=True)
            else:
                print(f"[db] ⚠️ WARNING: Entry not found after commit! listing_id={fields['listing_id']}, user_id={user_id}, session_id={session_id}", flush=True)
        
        return {"saved": 1, "enabled": True, "listing_id": fields["listing_id"]}
    except Exception as exc:
        conn.rollback()
        print(f"[db] Failed to store single search: {exc}", flush=True)
        import traceback
        print(f"[db] Traceback: {traceback.format_exc()}", flush=True)
        return {"saved": 0, "enabled": True, "error": str(exc)}
    finally:
        conn.close()

def persist_ranked_entries(doc: Dict[str, Any], *, user_id: str, keyword: str, session_id: str, persist_reviews: bool = False) -> Dict[str, Any]:
    cfg = _db_settings()
    if not cfg:
        print("[db] Skipping persistence: database env vars not configured.", flush=True)
        return {"saved": 0, "batch_id": None, "enabled": False}

    entries = doc.get("entries") or []
    if not entries:
        print(f"[db] No entries to persist for session={session_id}; nothing written.", flush=True)
        return {"saved": 0, "batch_id": None, "enabled": True}

    slug = slugify_safe(keyword)
    batch_id = f"{slug}-{session_id}-{int(time.time())}"
    normalized_rows = []
    reviews_rows = []
    for entry in entries:
        listing_id, payload, summary = _simplify_ranked_entry(entry)
        if listing_id is None or not payload:
            continue
        
        # Extract sale promotion info from entry (not in payload)
        popular = entry.get("popular_info") or entry.get("popular") or {}
        sale_info = popular.get("sale_info") or entry.get("sale_info") or {}
        active_promo = sale_info.get("active_promotion") or {}
        buyer_promotion_name = (active_promo.get("buyer_promotion_name") or "")[:255]
        buyer_shop_promotion_name = (active_promo.get("buyer_shop_promotion_name") or "")[:255]
        buyer_promotion_description = active_promo.get("buyer_promotion_description") or ""
        buyer_applied_promotion_description = active_promo.get("buyer_applied_promotion_description") or ""
        sale_active_promotion_id = str(active_promo.get("id") or "")[:64]
        sale_active_promotion_start_ts = _safe_int(active_promo.get("start_timestamp"))
        sale_active_promotion_end_ts = _safe_int(active_promo.get("end_timestamp"))
        sale_active_promotion_created_ts = _safe_int(active_promo.get("created_timestamp"))
        sale_active_promotion_updated_ts = _safe_int(active_promo.get("updated_timestamp"))
        sale_active_promotion_description = (buyer_applied_promotion_description or buyer_promotion_description or "")
        free_shipping = sale_info.get("free_shipping")
        if not isinstance(free_shipping, bool):
            free_shipping = None
        
        # Extract sale original price
        sale_original_price_display = sale_info.get("original_price") or ""
        sale_original_price = None
        try:
            if isinstance(sale_original_price_display, str) and sale_original_price_display.strip():
                cleaned_op = re.sub(r'[^0-9.]', '', sale_original_price_display)
                if cleaned_op:
                    sale_original_price = float(cleaned_op)
        except Exception:
            sale_original_price = None
        
        # Extract shop details
        shop_obj = payload.get("shop") or {}
        shop_details_compact = {
            "shop_id": shop_obj.get("shop_id"),
            "shop_name": shop_obj.get("shop_name"),
            "user_id": shop_obj.get("user_id"),
            "created_timestamp": shop_obj.get("created_timestamp"),
            "created_iso": shop_obj.get("created_iso"),
            "created": shop_obj.get("created"),
            "updated_timestamp": shop_obj.get("updated_timestamp"),
            "updated_iso": shop_obj.get("updated_iso"),
            "updated": shop_obj.get("updated"),
            "title": shop_obj.get("title"),
            "announcement": shop_obj.get("announcement"),
            "currency_code": shop_obj.get("currency_code"),
            "is_vacation": shop_obj.get("is_vacation"),
            "vacation_message": shop_obj.get("vacation_message"),
            "sale_message": shop_obj.get("sale_message"),
            "digital_sale_message": shop_obj.get("digital_sale_message"),
            "listing_active_count": shop_obj.get("listing_active_count"),
            "digital_listing_count": shop_obj.get("digital_listing_count"),
            "login_name": shop_obj.get("login_name"),
            "accepts_custom_requests": shop_obj.get("accepts_custom_requests"),
            "vacation_autoreply": shop_obj.get("vacation_autoreply"),
            "url": shop_obj.get("url"),
            "image_url_760x100": shop_obj.get("image_url_760x100"),
            "icon_url_fullxfull": shop_obj.get("icon_url_fullxfull"),
            "num_favorers": shop_obj.get("num_favorers"),
            "review_average": shop_obj.get("review_average"),
            "review_count": shop_obj.get("review_count"),
            "shipping_from_country_iso": shop_obj.get("shipping_from_country_iso"),
            "transaction_sold_count": shop_obj.get("transaction_sold_count"),
        }
        
        # Convert timestamps to datetime objects
        shop_created_at = None
        if shop_obj.get("created_timestamp"):
            try:
                shop_created_at = datetime.fromtimestamp(int(shop_obj.get("created_timestamp")), tz=timezone.utc)
            except Exception:
                pass
        shop_updated_at = None
        if shop_obj.get("updated_timestamp"):
            try:
                shop_updated_at = datetime.fromtimestamp(int(shop_obj.get("updated_timestamp")), tz=timezone.utc)
            except Exception:
                pass
        
        created_at = None
        if payload.get("made_at_ts"):
            try:
                created_at = datetime.fromtimestamp(int(payload.get("made_at_ts")), tz=timezone.utc)
            except Exception:
                pass
        
        last_modified_at = None
        if payload.get("last_modified_timestamp"):
            try:
                last_modified_at = datetime.fromtimestamp(int(payload.get("last_modified_timestamp")), tz=timezone.utc)
            except Exception:
                pass
        
        normalized_rows.append(
            (
                batch_id,
                session_id,
                user_id,
                keyword,
                slug,
                listing_id,
                summary.get("ranking"),
                summary.get("demand"),
                payload.get("title"),
                payload.get("url"),
                payload.get("user_id"),
                payload.get("shop_id"),
                payload.get("state"),
                payload.get("description"),
                payload.get("made_at_ts"),
                created_at,
                payload.get("last_modified_timestamp"),
                last_modified_at,
                payload.get("primary_image", {}).get("image_url") if isinstance(payload.get("primary_image"), dict) else None,
                payload.get("primary_image", {}).get("srcset") if isinstance(payload.get("primary_image"), dict) else None,
                json.dumps(payload.get("tags") or [], ensure_ascii=False),
                json.dumps(payload.get("materials") or [], ensure_ascii=False),
                json.dumps(payload.get("keywords") or [], ensure_ascii=False),
                json.dumps(payload.get("variations") or [], ensure_ascii=False),
                payload.get("quantity"),
                payload.get("num_favorers"),
                payload.get("listing_type") or "",
                payload.get("file_data") or "",
                payload.get("views"),
                payload.get("price_value"),
                payload.get("price_currency") or "",
                payload.get("price_display") or "",
                payload.get("price_amount"),
                payload.get("price_divisor"),
                payload.get("sale_percent"),
                payload.get("sale_price_value"),
                payload.get("sale_price_display") or "",
                sale_original_price,
                sale_original_price_display,
                sale_active_promotion_id,
                sale_active_promotion_start_ts,
                sale_active_promotion_end_ts,
                sale_active_promotion_created_ts,
                sale_active_promotion_updated_ts,
                sale_active_promotion_description,
                free_shipping,
                buyer_promotion_name,
                buyer_shop_promotion_name,
                buyer_promotion_description,
                buyer_applied_promotion_description,
                json.dumps(payload.get("keyword_insights") or [], ensure_ascii=False),
                json.dumps(payload.get("demand_extras") or {}, ensure_ascii=False),
                json.dumps(payload.get("sections") or [], ensure_ascii=False),
                json.dumps(shop_obj.get("reviews") or [], ensure_ascii=False),
                json.dumps(shop_obj.get("languages") or [], ensure_ascii=False),
                shop_obj.get("created_timestamp"),
                shop_created_at,
                shop_obj.get("updated_timestamp"),
                shop_updated_at,
                json.dumps(shop_details_compact, ensure_ascii=False),
                json.dumps(payload.get("reviews") or [], ensure_ascii=False),
                summary.get("review_count"),
                summary.get("review_average"),
                payload.get("is_personalizable"),
                payload.get("personalization_is_required"),
                payload.get("personalization_char_count_max"),
                payload.get("personalization_instructions") or "",
                payload.get("processing_min"),
                payload.get("processing_max"),
                payload.get("who_made") or "",
                payload.get("when_made") or "",
                json.dumps(payload.get("production_partners") or [], ensure_ascii=False),
                payload.get("icon_url_fullxfull") or "",
            )
        )
        if persist_reviews:
            # Get reviews from processed payload first, fallback to raw entry
            reviews_list = payload.get("reviews") or entry.get("reviews") or []
            if not reviews_list:
                continue
            
            # Ensure listing_id is valid
            if listing_id is None:
                print(f"[db] ⚠️ Skipping reviews for entry with null listing_id", flush=True)
                continue
            
            for rv in reviews_list:
                if not isinstance(rv, dict):
                    continue
                
                review_key = (
                    rv.get("transaction_id")
                    or rv.get("review_id")
                    or rv.get("id")
                    or f"{rv.get('buyer_user_id') or 'anon'}-{rv.get('created_timestamp') or rv.get('updated_timestamp') or ''}-{listing_id}"
                )
                review_key = str(review_key)
                
                # Ensure listing_id is set (should never be None at this point, but double-check)
                review_listing_id = listing_id
                if review_listing_id is None:
                    print(f"[db] ❌ ERROR: listing_id is None when saving review! review_key={review_key}", flush=True)
                    continue
                
                reviews_rows.append(
                    (
                        session_id,
                        review_listing_id,  # Explicitly use the listing_id variable
                        review_key,
                        rv.get("buyer_user_id"),
                        rv.get("rating"),
                        rv.get("review"),
                        rv.get("created_timestamp"),
                        rv.get("updated_timestamp"),
                        rv.get("language"),
                        rv.get("image_url_fullxfull"),
                    )
                )

    if not normalized_rows:
        print(f"[db] Entries payload normalized to 0 rows for session={session_id}; nothing written.", flush=True)
        return {"saved": 0, "batch_id": batch_id, "enabled": True}

    conn = _db_connect()
    if conn is None:
        print("[db] Unable to connect to PostgreSQL. Entries not saved.", flush=True)
        return {"saved": 0, "batch_id": None, "enabled": False}

    try:
        _ensure_db_tables(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {DB_TABLE_BATCHES} (batch_id, user_id, session_id, keyword, keyword_slug, entries_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    keyword = EXCLUDED.keyword,
                    keyword_slug = EXCLUDED.keyword_slug,
                    entries_count = EXCLUDED.entries_count,
                    stored_at = NOW()
                """,
                (batch_id, user_id, session_id, keyword, slug, len(normalized_rows)),
            )
            cur.executemany(
                f"""
                INSERT INTO {DB_TABLE_ENTRIES} (
                    batch_id, session_id, user_id, keyword, keyword_slug,
                    listing_id, ranking, demand,
                    title, url, etsy_user_id, shop_id, state, description,
                    created_at_ts, created_at, last_modified_ts, last_modified_at,
                    image_url, image_srcset, tags, materials, keywords, variations,
                    quantity, num_favorers, listing_type, file_data, views,
                    price_value, price_currency, price_display, price_amount_raw, price_divisor_raw,
                    sale_percent, sale_price_value, sale_price_display, sale_original_price, sale_original_price_display,
                    sale_active_promotion_id, sale_active_promotion_start_ts, sale_active_promotion_end_ts,
                    sale_active_promotion_created_ts, sale_active_promotion_updated_ts, sale_active_promotion_description,
                    free_shipping, buyer_promotion_name, buyer_shop_promotion_name,
                    buyer_promotion_description, buyer_applied_promotion_description,
                    keyword_insights, demand_extras, shop_sections, shop_reviews, shop_languages,
                    shop_created_ts, shop_created_at, shop_updated_ts, shop_updated_at,
                    shop_details, reviews, review_count, review_average,
                    is_personalizable, personalization_is_required, personalization_char_count_max, personalization_instructions,
                    processing_min, processing_max, who_made, when_made,
                    production_partners, icon_url_fullxfull
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
                ON CONFLICT (session_id, listing_id) DO UPDATE SET
                    batch_id = EXCLUDED.batch_id,
                    ranking = EXCLUDED.ranking,
                    demand = EXCLUDED.demand,
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    etsy_user_id = EXCLUDED.etsy_user_id,
                    shop_id = EXCLUDED.shop_id,
                    state = EXCLUDED.state,
                    description = EXCLUDED.description,
                    created_at_ts = EXCLUDED.created_at_ts,
                    created_at = EXCLUDED.created_at,
                    last_modified_ts = EXCLUDED.last_modified_ts,
                    last_modified_at = EXCLUDED.last_modified_at,
                    image_url = EXCLUDED.image_url,
                    image_srcset = EXCLUDED.image_srcset,
                    tags = EXCLUDED.tags,
                    materials = EXCLUDED.materials,
                    keywords = EXCLUDED.keywords,
                    variations = EXCLUDED.variations,
                    quantity = EXCLUDED.quantity,
                    num_favorers = EXCLUDED.num_favorers,
                    listing_type = EXCLUDED.listing_type,
                    file_data = EXCLUDED.file_data,
                    views = EXCLUDED.views,
                    price_value = EXCLUDED.price_value,
                    price_currency = EXCLUDED.price_currency,
                    price_display = EXCLUDED.price_display,
                    price_amount_raw = EXCLUDED.price_amount_raw,
                    price_divisor_raw = EXCLUDED.price_divisor_raw,
                    sale_percent = EXCLUDED.sale_percent,
                    sale_price_value = EXCLUDED.sale_price_value,
                    sale_price_display = EXCLUDED.sale_price_display,
                    sale_original_price = EXCLUDED.sale_original_price,
                    sale_original_price_display = EXCLUDED.sale_original_price_display,
                    sale_active_promotion_id = EXCLUDED.sale_active_promotion_id,
                    sale_active_promotion_start_ts = EXCLUDED.sale_active_promotion_start_ts,
                    sale_active_promotion_end_ts = EXCLUDED.sale_active_promotion_end_ts,
                    sale_active_promotion_created_ts = EXCLUDED.sale_active_promotion_created_ts,
                    sale_active_promotion_updated_ts = EXCLUDED.sale_active_promotion_updated_ts,
                    sale_active_promotion_description = EXCLUDED.sale_active_promotion_description,
                    free_shipping = EXCLUDED.free_shipping,
                    buyer_promotion_name = EXCLUDED.buyer_promotion_name,
                    buyer_shop_promotion_name = EXCLUDED.buyer_shop_promotion_name,
                    buyer_promotion_description = EXCLUDED.buyer_promotion_description,
                    buyer_applied_promotion_description = EXCLUDED.buyer_applied_promotion_description,
                    keyword_insights = EXCLUDED.keyword_insights,
                    demand_extras = EXCLUDED.demand_extras,
                    shop_sections = EXCLUDED.shop_sections,
                    shop_reviews = EXCLUDED.shop_reviews,
                    shop_languages = EXCLUDED.shop_languages,
                    shop_created_ts = EXCLUDED.shop_created_ts,
                    shop_created_at = EXCLUDED.shop_created_at,
                    shop_updated_ts = EXCLUDED.shop_updated_ts,
                    shop_updated_at = EXCLUDED.shop_updated_at,
                    shop_details = EXCLUDED.shop_details,
                    reviews = EXCLUDED.reviews,
                    review_count = EXCLUDED.review_count,
                    review_average = EXCLUDED.review_average,
                    is_personalizable = EXCLUDED.is_personalizable,
                    personalization_is_required = EXCLUDED.personalization_is_required,
                    personalization_char_count_max = EXCLUDED.personalization_char_count_max,
                    personalization_instructions = EXCLUDED.personalization_instructions,
                    processing_min = EXCLUDED.processing_min,
                    processing_max = EXCLUDED.processing_max,
                    who_made = EXCLUDED.who_made,
                    when_made = EXCLUDED.when_made,
                    production_partners = EXCLUDED.production_partners,
                    icon_url_fullxfull = EXCLUDED.icon_url_fullxfull,
                    keyword = EXCLUDED.keyword,
                    keyword_slug = EXCLUDED.keyword_slug,
                    user_id = EXCLUDED.user_id,
                    updated_at = NOW()
                """,
                normalized_rows,
            )
            if persist_reviews and reviews_rows:
                # Debug: verify listing_id is set for all reviews
                null_listing_ids = [i for i, row in enumerate(reviews_rows) if row[1] is None]
                if null_listing_ids:
                    print(f"[db] ❌ ERROR: Found {len(null_listing_ids)} reviews with null listing_id at indices: {null_listing_ids[:10]}", flush=True)
                    # Filter out reviews with null listing_id
                    reviews_rows = [row for row in reviews_rows if row[1] is not None]
                    print(f"[db] ⚠️ Filtered to {len(reviews_rows)} reviews with valid listing_id", flush=True)
                
                if reviews_rows:
                    print(f"[db] Saving {len(reviews_rows)} reviews to {DB_TABLE_REVIEWS} (sample listing_id: {reviews_rows[0][1] if reviews_rows else 'N/A'})", flush=True)
                    cur.executemany(
                        f"""
                        INSERT INTO {DB_TABLE_REVIEWS} (
                            session_id,
                            listing_id,
                            review_key,
                            buyer_user_id,
                            rating,
                            review,
                            created_timestamp,
                            updated_timestamp,
                            language,
                            image_url_fullxfull
                        )
                        VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (session_id, listing_id, review_key) DO UPDATE SET
                            buyer_user_id = EXCLUDED.buyer_user_id,
                            rating = EXCLUDED.rating,
                            review = EXCLUDED.review,
                            updated_timestamp = EXCLUDED.updated_timestamp,
                            language = EXCLUDED.language,
                            image_url_fullxfull = EXCLUDED.image_url_fullxfull
                        """,
                        reviews_rows,
                    )
                else:
                    print(f"[db] ⚠️ No valid reviews to save (all had null listing_id)", flush=True)
            live_batch_id = f"{slug}-{session_id}-live"
            cur.execute(
                f"DELETE FROM {DB_TABLE_BATCHES} WHERE batch_id = %s",
                (live_batch_id,),
            )
        conn.commit()
        print(f"[db] Stored {len(normalized_rows)} ranked entries in batch {batch_id}.", flush=True)
        return {"saved": len(normalized_rows), "batch_id": batch_id, "enabled": True}
    except Exception as exc:
        conn.rollback()
        print(f"[db] Failed to store ranked entries: {exc}", flush=True)
        return {"saved": 0, "batch_id": None, "enabled": True, "error": str(exc)}
    finally:
        conn.close()

def persist_entry_live(entry: Dict[str, Any], *, user_id: str, keyword: str, session_id: str, processed_count: Optional[int] = None) -> Dict[str, Any]:
    """
    Persist a single megafile entry as soon as it is fully enriched.
    This enables realtime UI updates while ranking is still pending.
    """
    cfg = _db_settings()
    if not cfg:
        return {"saved": 0, "batch_id": None, "enabled": False}

    listing_id, payload, summary = _simplify_ranked_entry(entry)
    if listing_id is None or not payload:
        return {"saved": 0, "batch_id": None, "enabled": True}

    # Extract sale promotion info from entry (not in payload)
    popular = entry.get("popular_info") or entry.get("popular") or {}
    sale_info = popular.get("sale_info") or entry.get("sale_info") or {}
    active_promo = sale_info.get("active_promotion") or {}
    buyer_promotion_name = (active_promo.get("buyer_promotion_name") or "")[:255]
    buyer_shop_promotion_name = (active_promo.get("buyer_shop_promotion_name") or "")[:255]
    buyer_promotion_description = active_promo.get("buyer_promotion_description") or ""
    buyer_applied_promotion_description = active_promo.get("buyer_applied_promotion_description") or ""
    sale_active_promotion_id = str(active_promo.get("id") or "")[:64]
    sale_active_promotion_start_ts = _safe_int(active_promo.get("start_timestamp"))
    sale_active_promotion_end_ts = _safe_int(active_promo.get("end_timestamp"))
    sale_active_promotion_created_ts = _safe_int(active_promo.get("created_timestamp"))
    sale_active_promotion_updated_ts = _safe_int(active_promo.get("updated_timestamp"))
    sale_active_promotion_description = (buyer_applied_promotion_description or buyer_promotion_description or "")
    free_shipping = sale_info.get("free_shipping")
    if not isinstance(free_shipping, bool):
        free_shipping = None
    
    # Extract sale original price
    sale_original_price_display = sale_info.get("original_price") or ""
    sale_original_price = None
    try:
        if isinstance(sale_original_price_display, str) and sale_original_price_display.strip():
            cleaned_op = re.sub(r'[^0-9.]', '', sale_original_price_display)
            if cleaned_op:
                sale_original_price = float(cleaned_op)
    except Exception:
        sale_original_price = None
    
    # Extract shop details
    shop_obj = payload.get("shop") or {}
    shop_details_compact = {
        "shop_id": shop_obj.get("shop_id"),
        "shop_name": shop_obj.get("shop_name"),
        "user_id": shop_obj.get("user_id"),
        "created_timestamp": shop_obj.get("created_timestamp"),
        "created_iso": shop_obj.get("created_iso"),
        "created": shop_obj.get("created"),
        "updated_timestamp": shop_obj.get("updated_timestamp"),
        "updated_iso": shop_obj.get("updated_iso"),
        "updated": shop_obj.get("updated"),
        "title": shop_obj.get("title"),
        "announcement": shop_obj.get("announcement"),
        "currency_code": shop_obj.get("currency_code"),
        "is_vacation": shop_obj.get("is_vacation"),
        "vacation_message": shop_obj.get("vacation_message"),
        "sale_message": shop_obj.get("sale_message"),
        "digital_sale_message": shop_obj.get("digital_sale_message"),
        "listing_active_count": shop_obj.get("listing_active_count"),
        "digital_listing_count": shop_obj.get("digital_listing_count"),
        "login_name": shop_obj.get("login_name"),
        "accepts_custom_requests": shop_obj.get("accepts_custom_requests"),
        "vacation_autoreply": shop_obj.get("vacation_autoreply"),
        "url": shop_obj.get("url"),
        "image_url_760x100": shop_obj.get("image_url_760x100"),
        "icon_url_fullxfull": shop_obj.get("icon_url_fullxfull"),
        "num_favorers": shop_obj.get("num_favorers"),
        "review_average": shop_obj.get("review_average"),
        "review_count": shop_obj.get("review_count"),
        "shipping_from_country_iso": shop_obj.get("shipping_from_country_iso"),
        "transaction_sold_count": shop_obj.get("transaction_sold_count"),
    }
    
    # Convert timestamps to datetime objects
    shop_created_at = None
    if shop_obj.get("created_timestamp"):
        try:
            shop_created_at = datetime.fromtimestamp(int(shop_obj.get("created_timestamp")), tz=timezone.utc)
        except Exception:
            pass
    shop_updated_at = None
    if shop_obj.get("updated_timestamp"):
        try:
            shop_updated_at = datetime.fromtimestamp(int(shop_obj.get("updated_timestamp")), tz=timezone.utc)
        except Exception:
            pass
    
    created_at = None
    if payload.get("made_at_ts"):
        try:
            created_at = datetime.fromtimestamp(int(payload.get("made_at_ts")), tz=timezone.utc)
        except Exception:
            pass
    
    last_modified_at = None
    if payload.get("last_modified_timestamp"):
        try:
            last_modified_at = datetime.fromtimestamp(int(payload.get("last_modified_timestamp")), tz=timezone.utc)
        except Exception:
            pass

    slug = slugify_safe(keyword)
    batch_id = f"{slug}-{session_id}-live"
    conn = _db_connect()
    if conn is None:
        print("[db] Unable to connect to PostgreSQL for live entry persist.", flush=True)
        return {"saved": 0, "batch_id": None, "enabled": False}

    try:
        _ensure_db_tables(conn)
        entries_count = processed_count if isinstance(processed_count, int) and processed_count >= 0 else 0
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {DB_TABLE_BATCHES} (batch_id, user_id, session_id, keyword, keyword_slug, entries_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id) DO UPDATE SET
                    user_id = EXCLUDED.user_id,
                    keyword = EXCLUDED.keyword,
                    keyword_slug = EXCLUDED.keyword_slug,
                    entries_count = EXCLUDED.entries_count,
                    stored_at = NOW()
                """,
                (batch_id, user_id, session_id, keyword, slug, entries_count),
            )
            cur.execute(
                f"""
                INSERT INTO {DB_TABLE_ENTRIES} (
                    batch_id, session_id, user_id, keyword, keyword_slug,
                    listing_id, ranking, demand,
                    title, url, etsy_user_id, shop_id, state, description,
                    created_at_ts, created_at, last_modified_ts, last_modified_at,
                    image_url, image_srcset, tags, materials, keywords, variations,
                    quantity, num_favorers, listing_type, file_data, views,
                    price_value, price_currency, price_display, price_amount_raw, price_divisor_raw,
                    sale_percent, sale_price_value, sale_price_display, sale_original_price, sale_original_price_display,
                    sale_active_promotion_id, sale_active_promotion_start_ts, sale_active_promotion_end_ts,
                    sale_active_promotion_created_ts, sale_active_promotion_updated_ts, sale_active_promotion_description,
                    free_shipping, buyer_promotion_name, buyer_shop_promotion_name,
                    buyer_promotion_description, buyer_applied_promotion_description,
                    keyword_insights, demand_extras, shop_sections, shop_reviews, shop_languages,
                    shop_created_ts, shop_created_at, shop_updated_ts, shop_updated_at,
                    shop_details, reviews, review_count, review_average,
                    is_personalizable, personalization_is_required, personalization_char_count_max, personalization_instructions,
                    processing_min, processing_max, who_made, when_made
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (session_id, listing_id) DO UPDATE SET
                    batch_id = EXCLUDED.batch_id,
                    ranking = EXCLUDED.ranking,
                    demand = EXCLUDED.demand,
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    etsy_user_id = EXCLUDED.etsy_user_id,
                    shop_id = EXCLUDED.shop_id,
                    state = EXCLUDED.state,
                    description = EXCLUDED.description,
                    created_at_ts = EXCLUDED.created_at_ts,
                    created_at = EXCLUDED.created_at,
                    last_modified_ts = EXCLUDED.last_modified_ts,
                    last_modified_at = EXCLUDED.last_modified_at,
                    image_url = EXCLUDED.image_url,
                    image_srcset = EXCLUDED.image_srcset,
                    tags = EXCLUDED.tags,
                    materials = EXCLUDED.materials,
                    keywords = EXCLUDED.keywords,
                    variations = EXCLUDED.variations,
                    quantity = EXCLUDED.quantity,
                    num_favorers = EXCLUDED.num_favorers,
                    listing_type = EXCLUDED.listing_type,
                    file_data = EXCLUDED.file_data,
                    views = EXCLUDED.views,
                    price_value = EXCLUDED.price_value,
                    price_currency = EXCLUDED.price_currency,
                    price_display = EXCLUDED.price_display,
                    price_amount_raw = EXCLUDED.price_amount_raw,
                    price_divisor_raw = EXCLUDED.price_divisor_raw,
                    sale_percent = EXCLUDED.sale_percent,
                    sale_price_value = EXCLUDED.sale_price_value,
                    sale_price_display = EXCLUDED.sale_price_display,
                    sale_original_price = EXCLUDED.sale_original_price,
                    sale_original_price_display = EXCLUDED.sale_original_price_display,
                    sale_active_promotion_id = EXCLUDED.sale_active_promotion_id,
                    sale_active_promotion_start_ts = EXCLUDED.sale_active_promotion_start_ts,
                    sale_active_promotion_end_ts = EXCLUDED.sale_active_promotion_end_ts,
                    sale_active_promotion_created_ts = EXCLUDED.sale_active_promotion_created_ts,
                    sale_active_promotion_updated_ts = EXCLUDED.sale_active_promotion_updated_ts,
                    sale_active_promotion_description = EXCLUDED.sale_active_promotion_description,
                    free_shipping = EXCLUDED.free_shipping,
                    buyer_promotion_name = EXCLUDED.buyer_promotion_name,
                    buyer_shop_promotion_name = EXCLUDED.buyer_shop_promotion_name,
                    buyer_promotion_description = EXCLUDED.buyer_promotion_description,
                    buyer_applied_promotion_description = EXCLUDED.buyer_applied_promotion_description,
                    keyword_insights = EXCLUDED.keyword_insights,
                    demand_extras = EXCLUDED.demand_extras,
                    shop_sections = EXCLUDED.shop_sections,
                    shop_reviews = EXCLUDED.shop_reviews,
                    shop_languages = EXCLUDED.shop_languages,
                    shop_created_ts = EXCLUDED.shop_created_ts,
                    shop_created_at = EXCLUDED.shop_created_at,
                    shop_updated_ts = EXCLUDED.shop_updated_ts,
                    shop_updated_at = EXCLUDED.shop_updated_at,
                    shop_details = EXCLUDED.shop_details,
                    reviews = EXCLUDED.reviews,
                    review_count = EXCLUDED.review_count,
                    review_average = EXCLUDED.review_average,
                    is_personalizable = EXCLUDED.is_personalizable,
                    personalization_is_required = EXCLUDED.personalization_is_required,
                    personalization_char_count_max = EXCLUDED.personalization_char_count_max,
                    personalization_instructions = EXCLUDED.personalization_instructions,
                    processing_min = EXCLUDED.processing_min,
                    processing_max = EXCLUDED.processing_max,
                    who_made = EXCLUDED.who_made,
                    when_made = EXCLUDED.when_made,
                    keyword = EXCLUDED.keyword,
                    keyword_slug = EXCLUDED.keyword_slug,
                    user_id = EXCLUDED.user_id,
                    updated_at = NOW()
                """,
                (
                    batch_id,
                    session_id,
                    user_id,
                    keyword,
                    slug,
                    listing_id,
                    summary.get("ranking"),
                    summary.get("demand"),
                    payload.get("title"),
                    payload.get("url"),
                    payload.get("user_id"),
                    payload.get("shop_id"),
                    payload.get("state"),
                    payload.get("description"),
                    payload.get("made_at_ts"),
                    created_at,
                    payload.get("last_modified_timestamp"),
                    last_modified_at,
                    payload.get("primary_image", {}).get("image_url") if isinstance(payload.get("primary_image"), dict) else None,
                    payload.get("primary_image", {}).get("srcset") if isinstance(payload.get("primary_image"), dict) else None,
                    json.dumps(payload.get("tags") or [], ensure_ascii=False),
                    json.dumps(payload.get("materials") or [], ensure_ascii=False),
                    json.dumps(payload.get("keywords") or [], ensure_ascii=False),
                    json.dumps(payload.get("variations") or [], ensure_ascii=False),
                    payload.get("quantity"),
                    payload.get("num_favorers"),
                    payload.get("listing_type") or "",
                    payload.get("file_data") or "",
                    payload.get("views"),
                    payload.get("price_value"),
                    payload.get("price_currency") or "",
                    payload.get("price_display") or "",
                    payload.get("price_amount"),
                    payload.get("price_divisor"),
                    payload.get("sale_percent"),
                    payload.get("sale_price_value"),
                    payload.get("sale_price_display") or "",
                    sale_original_price,
                    sale_original_price_display,
                    sale_active_promotion_id,
                    sale_active_promotion_start_ts,
                    sale_active_promotion_end_ts,
                    sale_active_promotion_created_ts,
                    sale_active_promotion_updated_ts,
                    sale_active_promotion_description,
                    free_shipping,
                    buyer_promotion_name,
                    buyer_shop_promotion_name,
                    buyer_promotion_description,
                    buyer_applied_promotion_description,
                    json.dumps(payload.get("keyword_insights") or [], ensure_ascii=False),
                    json.dumps(payload.get("demand_extras") or {}, ensure_ascii=False),
                    json.dumps(payload.get("sections") or [], ensure_ascii=False),
                    json.dumps(shop_obj.get("reviews") or [], ensure_ascii=False),
                    json.dumps(shop_obj.get("languages") or [], ensure_ascii=False),
                    shop_obj.get("created_timestamp"),
                    shop_created_at,
                    shop_obj.get("updated_timestamp"),
                    shop_updated_at,
                    json.dumps(shop_details_compact, ensure_ascii=False),
                    json.dumps(payload.get("reviews") or [], ensure_ascii=False),
                    summary.get("review_count"),
                    summary.get("review_average"),
                    payload.get("is_personalizable"),
                    payload.get("personalization_is_required"),
                    payload.get("personalization_char_count_max"),
                    payload.get("personalization_instructions") or "",
                    payload.get("processing_min"),
                    payload.get("processing_max"),
                    payload.get("who_made") or "",
                    payload.get("when_made") or "",
                    json.dumps(payload.get("production_partners") or [], ensure_ascii=False),
                    payload.get("icon_url_fullxfull") or "",
                ),
            )
        conn.commit()
        return {"saved": 1, "batch_id": batch_id, "enabled": True}
    except Exception as exc:
        conn.rollback()
        print(f"[db] Live persist failed for listing_id={listing_id}: {exc}", flush=True)
        return {"saved": 0, "batch_id": None, "enabled": True, "error": str(exc)}
    finally:
        conn.close()

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
    Single search - saves to Railway database:
    - Compiles the listing with all details (demand, keywords, everbee, etc.)
    - Enriches with full listing details and reviews
    - Saves to database (same structure as bulk search)
    - Returns completion message with Railway location info for UI retrieval
    """
    listing_id = int(payload.listing_id)
    user_id = payload.user_id.strip()
    session_id = payload.session_id.strip()
    
    print(f"[single-search] Starting single search for listing_id={listing_id}, user_id={user_id}, session_id={session_id}", flush=True)
    
    loop = asyncio.get_running_loop()
    try:
        # Compile the single listing
        print(f"[single-search] Compiling listing {listing_id}...", flush=True)
        compiled = await loop.run_in_executor(
            EXECUTOR,
            ss.run_single_search,
            listing_id,
            session_id,
            payload.forced_personalize,
        )
        print(f"[single-search] ✅ Listing {listing_id} compiled successfully", flush=True)

        # Convert compiled result to megafile entry format (raw entry)
        raw_entry = ss._to_megafile_entry(compiled)
        print(f"[single-search] Converted to megafile entry format", flush=True)
        
        # Debug: Check if price is in the raw entry from compiled result
        initial_price = raw_entry.get("popular_info", {}).get("price") if isinstance(raw_entry.get("popular_info"), dict) else None
        if initial_price:
            print(f"[single-search] 🔍 Initial price from compiled result: {initial_price}", flush=True)
        else:
            print(f"[single-search] ⚠️ WARNING: No price in raw_entry.popular_info from compiled result!", flush=True)
            print(f"[single-search] 🔍 popular_info type: {type(raw_entry.get('popular_info'))}, keys: {list(raw_entry.get('popular_info', {}).keys()) if isinstance(raw_entry.get('popular_info'), dict) else 'N/A'}", flush=True)
        
        # Enrich with listing details and reviews (like bulk search does)
        if listing_id:
            print(f"[single-search] Enriching listing {listing_id} with details and reviews...", flush=True)
            try:
                snapshot = etsy.collect_listing_detail_and_reviews(listing_id, reviews_limit=100)
                detail = snapshot.get("detail")
                if detail is not None:
                    raw_entry["listing_details"] = detail
                    # Merge detail into popular_info so _simplify_ranked_entry can access it (including tags, materials, etc.)
                    # BUT preserve existing primary_image and price from compiled result (they're already properly formatted)
                    existing_primary_image = raw_entry.get("primary_image")
                    
                    # Get existing popular_info and extract price BEFORE any updates
                    existing_popular_info = raw_entry.get("popular_info") or {}
                    existing_price = existing_popular_info.get("price")
                    
                    # Debug: log what we have before merging
                    if existing_price:
                        print(f"[single-search] 🔍 Existing price BEFORE merge: {existing_price}", flush=True)
                    else:
                        print(f"[single-search] ⚠️ No existing price found in popular_info before merge", flush=True)
                    
                    # Check if detail has a price that might overwrite ours
                    detail_price = detail.get("price") if isinstance(detail, dict) else None
                    if detail_price:
                        print(f"[single-search] 🔍 Detail has price: {detail_price} (will be overwritten with existing)", flush=True)
                    
                    if not isinstance(raw_entry.get("popular_info"), dict):
                        raw_entry["popular_info"] = {}
                    
                    # Merge detail into popular_info (this might overwrite price)
                    raw_entry["popular_info"].update(detail)
                    
                    # ALWAYS restore price from compiled result (it's the source of truth)
                    # Check both existing_price and what's currently in popular_info after merge
                    price_after_merge = raw_entry.get("popular_info", {}).get("price")
                    
                    if existing_price and isinstance(existing_price, dict):
                        # Restore the original price from compiled result
                        raw_entry["popular_info"]["price"] = existing_price
                        print(f"[single-search] ✅ Price restored from compiled: amount={existing_price.get('amount')}, divisor={existing_price.get('divisor')}, currency={existing_price.get('currency_code')}", flush=True)
                    elif price_after_merge and isinstance(price_after_merge, dict):
                        # Price from detail might be okay, but normalize it to ensure it has the right structure
                        normalized_detail_price = {
                            "amount": price_after_merge.get("amount") or price_after_merge.get("price_amount"),
                            "divisor": price_after_merge.get("divisor") or price_after_merge.get("price_divisor") or 100,
                            "currency_code": price_after_merge.get("currency_code") or price_after_merge.get("currency") or price_after_merge.get("price_currency") or "",
                        }
                        # Only use if it has at least amount
                        if normalized_detail_price.get("amount") is not None:
                            raw_entry["popular_info"]["price"] = normalized_detail_price
                            print(f"[single-search] ⚠️ Using normalized price from detail: {normalized_detail_price}", flush=True)
                        else:
                            print(f"[single-search] ❌ Price from detail is invalid: {price_after_merge}", flush=True)
                    else:
                        # No price at all - this is a problem
                        print(f"[single-search] ❌ ERROR: No price found! existing_price={existing_price}, price_after_merge={price_after_merge}", flush=True)
                        print(f"[single-search] 🔍 Debug: popular_info keys: {list(raw_entry.get('popular_info', {}).keys())}", flush=True)
                        # Try to extract from listing_details as last resort
                        listing_details_price = raw_entry.get("listing_details", {}).get("price") if isinstance(raw_entry.get("listing_details"), dict) else None
                        if listing_details_price and isinstance(listing_details_price, dict):
                            normalized = {
                                "amount": listing_details_price.get("amount"),
                                "divisor": listing_details_price.get("divisor") or 100,
                                "currency_code": listing_details_price.get("currency_code") or "",
                            }
                            if normalized.get("amount") is not None:
                                raw_entry["popular_info"]["price"] = normalized
                                print(f"[single-search] ✅ Extracted price from listing_details as fallback: {normalized}", flush=True)
                    
                    # Restore primary_image if it was overwritten (preserve the properly formatted one from compiled)
                    if existing_primary_image:
                        raw_entry["primary_image"] = existing_primary_image
                        # Also ensure it's in popular_info for _simplify_ranked_entry
                        raw_entry["popular_info"]["primary_image"] = existing_primary_image
                    
                    print(f"[single-search] ✅ Listing details fetched and merged into popular_info (primary_image and price preserved)", flush=True)
                else:
                    print(f"[single-search] ⚠️ No listing details returned from API", flush=True)
                
                # First, check if we have reviews from the compiled result (shop.reviews)
                # These have all the correct fields (shop_id, listing_id, transaction_id, buyer_user_id)
                existing_shop = raw_entry.get("shop") or {}
                existing_shop_reviews = existing_shop.get("reviews") or []
                
                # Filter reviews to only include ones for this listing_id
                compiled_reviews_for_listing = []
                if existing_shop_reviews and listing_id:
                    for rv in existing_shop_reviews:
                        if isinstance(rv, dict):
                            rv_listing_id = rv.get("listing_id")
                            # Include review if it matches this listing_id or if listing_id is not specified
                            if rv_listing_id is None or int(rv_listing_id) == int(listing_id):
                                compiled_reviews_for_listing.append(rv)
                
                print(f"[single-search] 🔍 Found {len(compiled_reviews_for_listing)} reviews from compiled result for listing_id={listing_id}", flush=True)
                
                # Get API reviews as supplement
                api_reviews = snapshot.get("reviews")
                if api_reviews is not None:
                    raw_entry["listing_reviews"] = api_reviews
                    print(f"[single-search] ✅ Fetched {len(api_reviews)} reviews from API", flush=True)
                
                # Use compiled reviews if available (they have all the correct fields), otherwise use API reviews
                if compiled_reviews_for_listing:
                    # Use compiled reviews - they already have shop_id, listing_id, transaction_id, buyer_user_id
                    final_reviews = compiled_reviews_for_listing
                    print(f"[single-search] ✅ Using {len(final_reviews)} reviews from compiled result (with all fields)", flush=True)
                elif api_reviews:
                    # Fallback to API reviews, but ensure we add listing_id and shop_id
                    formatted_reviews = []
                    shop_id = raw_entry.get("shop_id") or existing_shop.get("shop_id") or existing_shop.get("details", {}).get("shop_id")
                    for rv in api_reviews:
                        if isinstance(rv, dict):
                            formatted_reviews.append({
                                "shop_id": shop_id,  # Add shop_id
                                "listing_id": listing_id,  # Add listing_id
                                "transaction_id": rv.get("transaction_id"),
                                "review_id": rv.get("review_id") or rv.get("id"),
                                "buyer_user_id": rv.get("buyer_user_id") or rv.get("buyerUserId") or rv.get("author_id"),
                                "rating": rv.get("rating"),
                                "review": rv.get("review") or rv.get("review_text") or rv.get("text"),
                                "created_timestamp": rv.get("created_timestamp") or rv.get("create_timestamp") or rv.get("createdAt"),
                                "updated_timestamp": rv.get("updated_timestamp") or rv.get("update_timestamp") or rv.get("updatedAt"),
                                "language": rv.get("language") or rv.get("review_language") or rv.get("locale"),
                                "image_url_fullxfull": rv.get("image_url_fullxfull") or rv.get("image") or rv.get("photo"),
                            })
                    final_reviews = formatted_reviews
                    print(f"[single-search] ⚠️ Using {len(final_reviews)} reviews from API (fallback, added listing_id and shop_id)", flush=True)
                else:
                    final_reviews = []
                    print(f"[single-search] ⚠️ No reviews available from compiled result or API", flush=True)
                
                # Set reviews in both places
                raw_entry["reviews"] = final_reviews
                if not isinstance(raw_entry.get("shop"), dict):
                    raw_entry["shop"] = {}
                raw_entry["shop"]["reviews"] = final_reviews
            except Exception as enrich_err:
                print(f"[single-search] ❌ Failed to enrich listing_id={listing_id}: {enrich_err}", flush=True)

        # Normalize primary_image format if needed (ensure it has image_url and srcset)
        primary_img_raw = raw_entry.get("primary_image") or raw_entry.get("popular_info", {}).get("primary_image")
        if primary_img_raw and isinstance(primary_img_raw, dict):
            # If it doesn't have image_url/srcset, try to convert from other formats
            if not primary_img_raw.get("image_url") and not primary_img_raw.get("srcset"):
                # Try to extract from url_full, url_300x300, etc.
                image_url = primary_img_raw.get("image_url") or primary_img_raw.get("url_full") or primary_img_raw.get("url_300x300") or primary_img_raw.get("url")
                srcset = primary_img_raw.get("srcset")
                if image_url and not srcset:
                    # Generate a basic srcset if we have image_url but no srcset
                    srcset = image_url
                if image_url:
                    normalized_img = {
                        "image_url": image_url,
                        "srcset": srcset or image_url,
                    }
                    # Preserve other fields
                    for key in ["listing_id", "image_id", "width", "height"]:
                        if key in primary_img_raw:
                            normalized_img[key] = primary_img_raw[key]
                    raw_entry["primary_image"] = normalized_img
                    if isinstance(raw_entry.get("popular_info"), dict):
                        raw_entry["popular_info"]["primary_image"] = normalized_img
                    print(f"[single-search] ✅ Normalized primary_image format: image_url={image_url[:50] if image_url else None}...", flush=True)

        # Debug: verify price is in popular_info before processing
        popular_info_price = raw_entry.get("popular_info", {}).get("price")
        if popular_info_price:
            print(f"[single-search] 🔍 Price in popular_info BEFORE _simplify_ranked_entry: {popular_info_price}", flush=True)
        else:
            print(f"[single-search] ⚠️ WARNING: No price in popular_info before processing! popular_info keys: {list(raw_entry.get('popular_info', {}).keys())}", flush=True)
        
        # Process entry through _simplify_ranked_entry to get fully processed payload (like bulk search)
        # This ensures all data is processed, not raw - including primary_image extraction and formatting
        print(f"[single-search] Processing entry through _simplify_ranked_entry (ensures all data is processed, not raw)...", flush=True)
        processed_listing_id, processed_payload, processed_summary = _simplify_ranked_entry(raw_entry)
        
        if processed_listing_id is None or not processed_payload:
            print(f"[single-search] ❌ Failed to process entry - listing_id or payload missing", flush=True)
            return JSONResponse(content={
                "success": False,
                "error": "Failed to process entry data",
                "listing_id": listing_id,
            }, status_code=500)
        
        # Verify primary_image was processed
        primary_img = processed_payload.get("primary_image")
        if primary_img:
            print(f"[single-search] ✅ Primary image processed: image_url={primary_img.get('image_url')}, srcset={primary_img.get('srcset')}", flush=True)
        else:
            print(f"[single-search] ⚠️ No primary_image in processed payload", flush=True)
        
        # Verify price was processed
        price_value = processed_payload.get("price_value")
        price_display = processed_payload.get("price_display")
        price_amount = processed_payload.get("price_amount")
        price_divisor = processed_payload.get("price_divisor")
        price_currency = processed_payload.get("price_currency")
        if price_value is not None:
            print(f"[single-search] ✅ Price processed: value={price_value}, display={price_display}, amount={price_amount}, divisor={price_divisor}, currency={price_currency}", flush=True)
        else:
            print(f"[single-search] ⚠️ No price_value in processed payload (amount={price_amount}, divisor={price_divisor}, currency={price_currency})", flush=True)
        
        # Create processed entry for persistence
        # IMPORTANT: persist_ranked_entries will call _simplify_ranked_entry again,
        # so we need to preserve popular_info (with price) for that second call
        processed_entry = {
            "listing_id": processed_listing_id,
            **processed_payload,  # All processed data from _simplify_ranked_entry
            # Preserve popular_info so persist_ranked_entries can extract price again
            "popular_info": raw_entry.get("popular_info", {}),
            # Also preserve other fields that _simplify_ranked_entry might need
            "sale_info": raw_entry.get("sale_info"),
            "variations_cleaned": raw_entry.get("variations_cleaned"),
        }
        
        print(f"[single-search] ✅ Entry processed - image: {bool(primary_img)}, shop: {bool(processed_payload.get('shop'))}, reviews: {len(processed_payload.get('reviews') or [])}", flush=True)
        print(f"[single-search] 🔍 Preserved popular_info.price for persist_ranked_entries: {processed_entry.get('popular_info', {}).get('price')}", flush=True)

        # Create megafile document structure for persistence (using processed entry)
        megafile_doc = {
            "entries": [processed_entry],
            "meta": {
                "processed_total": 1,
                "popular_count": 1,
                "keyword_slug": f"single-{listing_id}",
            }
        }

        # Save to database (single_searches table matching Django model)
        print(f"[single-search] Saving to Railway database (table: {DB_TABLE_SINGLE_SEARCHES})...", flush=True)
        
        # Extract etsy_user_id from the entry
        etsy_user_id = processed_entry.get("popular_info", {}).get("user_id") or processed_entry.get("user_id")
        
        persist_info = persist_single_search(
            processed_entry,
            user_id=user_id,
            session_id=session_id,
            etsy_user_id=str(etsy_user_id) if etsy_user_id else None,
        )
        
        saved_count = persist_info.get("saved", 0)
        db_enabled = persist_info.get("enabled", False)
        
        if saved_count > 0:
            print(f"[single-search] ✅ Saved to Railway database:", flush=True)
            print(f"  - Table: {DB_TABLE_SINGLE_SEARCHES}", flush=True)
            print(f"  - Session ID: {session_id}", flush=True)
            print(f"  - Listing ID: {listing_id}", flush=True)
            print(f"  - User ID: {user_id}", flush=True)
            print(f"  - Retrieval: GET /single-search/{listing_id}?user_id={user_id}&session_id={session_id}", flush=True)
        else:
            error = persist_info.get("error")
            print(f"[single-search] ⚠️ Database save returned: saved={saved_count}, enabled={db_enabled}", flush=True)
            if error:
                print(f"[single-search] ⚠️ Error: {error}", flush=True)

        # Organize session meta for this single-search run (non-fatal)
        try:
            meta = compiled.get("meta") or {}
            update_session_meta_file(
                user_id,
                session_id,
                run_root_dir=meta.get("run_dir"),
                outputs_dir=meta.get("outputs_dir"),
                last_action="single_search",
                compiled_path=meta.get("compiled_path"),
                listing_id=compiled.get("listing_id"),
                timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
            )
        except Exception as meta_err:
            print(f"[single-search] ⚠️ Failed to update session meta: {meta_err}", flush=True)

        print(f"[single-search] ✅ Single search completed for listing_id={listing_id}", flush=True)
        
        # Build exact location information for UI
        base_url = os.getenv("API_BASE_URL", "http://localhost:8000")
        retrieval_url = f"{base_url}/single-search/{listing_id}?user_id={user_id}&session_id={session_id}"
        curl_command = f'curl -X GET "{retrieval_url}"'
        
        # Return completion message with exact location details
        return JSONResponse(content={
            "success": True,
            "status": "completed",
            "message": "Single search completed and saved to Railway database",
            "listing_id": listing_id,
            "session_id": session_id,
            "user_id": user_id,
            "entries_saved": saved_count,
            "db_enabled": db_enabled,
            "source": "db",
            "railway": {
                "table": DB_TABLE_SINGLE_SEARCHES,
                "session_id": session_id,
                "listing_id": listing_id,
                "user_id": user_id,
                "retrieval": {
                    "method": "GET",
                    "url": retrieval_url,
                    "curl_command": curl_command,
                    "endpoint": f"/single-search/{listing_id}",
                    "query_params": {
                        "user_id": user_id,
                        "session_id": session_id,
                    },
                },
            },
        })
    except Exception as e:
        print(f"[single-search] ❌ Error: {e}", flush=True)
        import traceback
        print(f"[single-search] Traceback: {traceback.format_exc()}", flush=True)
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


def _resolve_keyword_for_session(user_id: str, session_id: str) -> Optional[str]:
    """
    Best-effort keyword lookup from the session meta JSON.
    Used when endpoints (like replace-listing) need a keyword but only have session_id.
    """
    meta_path = _resolve_session_meta_path(user_id, session_id)
    if not meta_path or not meta_path.exists():
        return None
    try:
        with meta_path.open("r", encoding="utf-8") as f:
            doc = json.load(f)
        return doc.get("keyword") or (doc.get("meta") or {}).get("keyword")
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

def _canonical_megafile_for_write(path: Path) -> Path:
    """
    When replace-listing is triggered after previous rankings, the session meta
    might point at a *_ranked.json file. We need to mutate the base megafile
    (non-ranked) so that future rankings do not create cascading
    *_ranked_ranked.json files. Strip repeated `_ranked` suffixes if the base
    file exists; otherwise fall back to the provided path.
    """
    try:
        suffix = path.suffix
        stem = path.stem
        original_stem = stem
        while stem.endswith("_ranked"):
            stem = stem[: -len("_ranked")]
        if stem != original_stem:
            candidate = path.with_name(f"{stem}{suffix}")
            if candidate.exists():
                return candidate
        return path
    except Exception:
        return path

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
        mf_path = _canonical_megafile_for_write(Path(mf_path_str))

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

            keyword_hint = _resolve_keyword_for_session(payload.user_id, payload.session_id) or ranked_json.get("meta", {}).get("keyword_slug") or payload.session_id
            persist_info = persist_ranked_entries(
                ranked_json,
                user_id=payload.user_id,
                keyword=keyword_hint,
                session_id=payload.session_id,
                persist_reviews=True,
            )
            print("[ranking] Stored ranked megafile JSON in database (replace-listing).", flush=True)
            return JSONResponse({
                "status": "ok",
                "session_id": payload.session_id,
                "entries_saved": persist_info.get("saved"),
                "batch_id": persist_info.get("batch_id"),
                "db_enabled": persist_info.get("enabled", False),
                "source": "db",
            })
        except Exception as e:
            print(f"[ranking] Ranking failed; sending unranked megafile. Error: {e}", flush=True)
            persist_info = persist_ranked_entries(
                mega,
                user_id=payload.user_id,
                keyword=_resolve_keyword_for_session(payload.user_id, payload.session_id) or payload.session_id,
                session_id=payload.session_id,
                persist_reviews=True,
            )
            return JSONResponse({
                "status": "ok",
                "session_id": payload.session_id,
                "entries_saved": persist_info.get("saved"),
                "batch_id": persist_info.get("batch_id"),
                "db_enabled": persist_info.get("enabled", False),
                "source": "db",
                "warning": "Ranking failed; stored unranked payload",
            })
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
                doc = None
                ranked_used = False
                ranked_candidate = (
                    p.with_name(p.stem + "_ranked.json")
                    if p.suffix.lower() == ".json"
                    else p.with_name(p.name + "_ranked")
                )
                if p.name.endswith("_ranked.json"):
                    with p.open("r", encoding="utf-8") as f:
                        doc = json.load(f)
                    ranked_used = True
                elif ranked_candidate.exists():
                    with ranked_candidate.open("r", encoding="utf-8") as f:
                        doc = json.load(f)
                    ranked_used = True
                else:
                    import ranking as rnk
                    print("[ranking] Reconnect: no ranked file found; ranking once.", flush=True)
                    ranked_path = rnk.rank_megafile(str(p))
                    ranked_used = True
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
                        doc = json.load(f)

                if not doc:
                    raise RuntimeError("Megafile payload missing")

                persist_info = persist_ranked_entries(
                    doc,
                    user_id=user_id,
                    keyword=keyword,
                    session_id=session_id,
                    persist_reviews=True,
                )
                print(f"[ranking] Reconnect: stored ranked megafile (entries={persist_info.get('saved')}).", flush=True)
                return JSONResponse({
                    "success": True,
                    "session_id": session_id,
                    "entries_saved": persist_info.get("saved"),
                    "batch_id": persist_info.get("batch_id"),
                    "db_enabled": persist_info.get("enabled", False),
                    "source": "db",
                    "used_ranked_file": ranked_used,
                })
            except Exception as e:
                print(f"[ranking] Reconnect: ranking failed; falling back to unranked. Error: {e}", flush=True)
                try:
                    with open(mf_path, "r", encoding="utf-8") as f:
                        doc = json.load(f)
                    persist_info = persist_ranked_entries(
                        doc,
                        user_id=user_id,
                        keyword=keyword,
                        session_id=session_id,
                        persist_reviews=True,
                    )
                    print(f"[db] Run /run-stream persist summary: {persist_info}", flush=True)
                    return JSONResponse({
                        "success": True,
                        "session_id": session_id,
                        "entries_saved": persist_info.get("saved"),
                        "batch_id": persist_info.get("batch_id"),
                        "db_enabled": persist_info.get("enabled", False),
                        "source": "db",
                        "used_ranked_file": False,
                        "warning": "Ranking failed; stored unranked snapshot",
                    })
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
    project_root = Path(__file__).resolve().parent
    preferred_dir = Path(os.getenv("MODEL_DIR") or "/data/models")
    fallback_dir = project_root / "data" / "models"

    default_name = "gemma3-999.89M-Q4_K_M.gguf"
    model_path_env = os.getenv("MODEL_PATH")
    model_url = os.getenv("MODEL_URL", "")
    model_sha256 = os.getenv("MODEL_SHA256", "")

    model_dir = preferred_dir
    if model_path_env:
        mp = Path(model_path_env)
        if not mp.is_absolute():
            mp = project_root / mp
        model_path = mp
    else:
        model_path = model_dir / default_name
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
        download_progress.update({
            "status": "idle",
            "source": None,
            "destination": None,
            "total_bytes": None,
            "downloaded_bytes": 0,
            "percent": None,
            "speed_bps": None,
            "eta_seconds": None,
            "started_at": None,
            "updated_at": time.time(),
            "error": None,
        })
    except Exception:
        pass
    return status

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

@app.on_event("startup")
async def _startup():
    pass

@app.get("/ready")
def ready() -> Dict:
    st = ensure_model_available()
    return {
        "status": "ok",
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

@app.get("/single-search/{listing_id}")
def get_single_search(listing_id: int, user_id: str, session_id: str) -> Dict:
    """
    Retrieve single search from Railway database (single_searches table).
    Returns all fields matching Django SingleSearch model structure.
    """
    print(f"[single-search] GET request: listing_id={listing_id}, user_id={user_id}, session_id={session_id}", flush=True)
    
    cfg = _db_settings()
    if not cfg:
        return {
            "success": False,
            "error": "Database not configured",
            "listing_id": listing_id,
            "session_id": session_id,
            "railway_table": DB_TABLE_SINGLE_SEARCHES,
        }
    
    conn = _db_connect()
    if conn is None:
        return {
            "success": False,
            "error": "Database connection failed",
            "listing_id": listing_id,
            "session_id": session_id,
            "railway_table": DB_TABLE_SINGLE_SEARCHES,
        }
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT 
                    listing_id, title, url, demand, ranking,
                    etsy_user_id, shop_id, state, description,
                    created_at_ts, created_at, last_modified_ts, last_modified_at,
                    image_url, image_srcset, tags, materials, keywords, variations,
                    quantity, num_favorers, listing_type, file_data, views,
                    price_value, price_currency, price_display, price_amount_raw, price_divisor_raw,
                    sale_percent, sale_price_value, sale_price_display,
                    sale_original_price, sale_original_price_display,
                    sale_active_promotion_id, sale_active_promotion_start_ts,
                    sale_active_promotion_end_ts, sale_active_promotion_created_ts,
                    sale_active_promotion_updated_ts, sale_active_promotion_description,
                    free_shipping, buyer_promotion_name, buyer_shop_promotion_name,
                    buyer_promotion_description, buyer_applied_promotion_description,
                    keyword_insights, demand_extras,
                    shop_sections, shop_reviews, shop_languages,
                    shop_created_ts, shop_created_at, shop_updated_ts, shop_updated_at,
                    shop_details, reviews, review_count, review_average,
                    processing_min, processing_max, who_made, when_made,
                    is_personalizable, personalization_is_required, personalization_char_count_max, personalization_instructions,
                    production_partners, icon_url_fullxfull,
                    updated_at
                FROM {DB_TABLE_SINGLE_SEARCHES}
                WHERE user_id = %s AND listing_id = %s AND session_id = %s
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (user_id, str(listing_id), session_id),
            )
            row = cur.fetchone()
            if not row:
                # Try to find what's actually in the database for debugging
                cur.execute(
                    f"""
                    SELECT listing_id, user_id, session_id, updated_at
                    FROM {DB_TABLE_SINGLE_SEARCHES}
                    WHERE listing_id = %s OR user_id = %s OR session_id = %s
                    ORDER BY updated_at DESC
                    LIMIT 5
                    """,
                    (str(listing_id), user_id, session_id),
                )
                debug_rows = cur.fetchall()
                print(f"[single-search] ❌ Entry not found: listing_id={listing_id}, user_id={user_id}, session_id={session_id}", flush=True)
                print(f"[single-search] 🔍 Debug: Found {len(debug_rows)} similar rows in database:", flush=True)
                for dr in debug_rows:
                    print(f"[single-search] 🔍   - listing_id={dr[0]}, user_id={dr[1]}, session_id={dr[2]}, updated_at={dr[3]}", flush=True)
                return {
                    "success": False,
                    "error": "Entry not found",
                    "listing_id": listing_id,
                    "session_id": session_id,
                    "user_id": user_id,
                    "railway_table": DB_TABLE_SINGLE_SEARCHES,
                    "debug": {
                        "query_listing_id": str(listing_id),
                        "query_user_id": user_id,
                        "query_session_id": session_id,
                        "similar_rows_found": len(debug_rows),
                    },
                }
            
            print(f"[single-search] ✅ Retrieved entry from Railway (single_searches table)", flush=True)
            
            # Build response matching Django model structure
            return {
                "success": True,
                "listing_id": row[0],
                "title": row[1],
                "url": row[2],
                "demand": row[3],
                "ranking": row[4],
                "etsy_user_id": row[5],
                "shop_id": row[6],
                "state": row[7],
                "description": row[8],
                "created_at_ts": row[9],
                "created_at": row[10].isoformat() if row[10] else None,
                "last_modified_ts": row[11],
                "last_modified_at": row[12].isoformat() if row[12] else None,
                "image_url": row[13],
                "image_srcset": row[14],
                "tags": row[15],
                "materials": row[16],
                "keywords": row[17],
                "variations": row[18],
                "quantity": row[19],
                "num_favorers": row[20],
                "listing_type": row[21],
                "file_data": row[22],
                "views": row[23],
                "price_value": row[24],
                "price_currency": row[25],
                "price_display": row[26],
                "price_amount_raw": row[27],
                "price_divisor_raw": row[28],
                "sale_percent": row[29],
                "sale_price_value": row[30],
                "sale_price_display": row[31],
                "sale_original_price": row[32],
                "sale_original_price_display": row[33],
                "sale_active_promotion_id": row[34],
                "sale_active_promotion_start_ts": row[35],
                "sale_active_promotion_end_ts": row[36],
                "sale_active_promotion_created_ts": row[37],
                "sale_active_promotion_updated_ts": row[38],
                "sale_active_promotion_description": row[39],
                "free_shipping": row[40],
                "buyer_promotion_name": row[41],
                "buyer_shop_promotion_name": row[42],
                "buyer_promotion_description": row[43],
                "buyer_applied_promotion_description": row[44],
                "keyword_insights": row[45],
                "demand_extras": row[46],
                "shop_sections": row[47],
                "shop_reviews": row[48],
                "shop_languages": row[49],
                "shop_created_ts": row[50],
                "shop_created_at": row[51].isoformat() if row[51] else None,
                "shop_updated_ts": row[52],
                "shop_updated_at": row[53].isoformat() if row[53] else None,
                "shop_details": row[54],
                "reviews": row[55],
                "review_count": row[56],
                "review_average": row[57],
                "processing_min": row[58],
                "processing_max": row[59],
                "who_made": row[60],
                "when_made": row[61],
                "is_personalizable": row[62],
                "personalization_is_required": row[63],
                "personalization_char_count_max": row[64],
                "personalization_instructions": row[65],
                "production_partners": row[66],
                "icon_url_fullxfull": row[67],
                "updated_at": row[68].isoformat() if row[68] else None,
                "railway": {
                    "table": DB_TABLE_SINGLE_SEARCHES,
                    "user_id": user_id,
                    "session_id": session_id,
                },
            }
    except Exception as e:
        print(f"[single-search] ❌ Error retrieving entry: {e}", flush=True)
        import traceback
        print(f"[single-search] Traceback: {traceback.format_exc()}", flush=True)
        return {
            "success": False,
            "error": str(e),
            "listing_id": listing_id,
            "session_id": session_id,
            "user_id": user_id,
            "railway_table": DB_TABLE_SINGLE_SEARCHES,
        }
    finally:
        conn.close()

@app.get("/db-diagnostics")
def db_diagnostics() -> Dict:
    """
    Diagnostic endpoint to check database connection and table status.
    """
    cfg = _db_settings()
    if not cfg:
        return {
            "success": False,
            "error": "Database not configured",
            "env_vars_checked": ["DB_URL", "DATABASE_URL", "RAILWAY_DATABASE_URL"],
        }
    
    conn = _db_connect()
    if conn is None:
        return {
            "success": False,
            "error": "Failed to connect to database",
            "config_exists": True,
        }
    
    try:
        with conn.cursor() as cur:
            # Check if tables exist
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cur.fetchall()]
            
            # Count rows in each table
            table_counts = {}
            for table in [DB_TABLE_BATCHES, DB_TABLE_ENTRIES, DB_TABLE_REVIEWS, DB_TABLE_SINGLE_SEARCHES]:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    table_counts[table] = count
                except Exception as e:
                    table_counts[table] = f"ERROR: {str(e)}"
            
            # Check recent entries
            recent_entries = {}
            try:
                cur.execute(f"""
                    SELECT listing_id, user_id, session_id, updated_at 
                    FROM {DB_TABLE_SINGLE_SEARCHES} 
                    ORDER BY updated_at DESC 
                    LIMIT 5
                """)
                recent_entries["single_searches"] = [
                    {"listing_id": row[0], "user_id": row[1], "session_id": row[2], "updated_at": str(row[3])}
                    for row in cur.fetchall()
                ]
            except Exception as e:
                recent_entries["single_searches"] = f"ERROR: {str(e)}"
            
            try:
                cur.execute(f"""
                    SELECT listing_id, user_id, session_id, updated_at 
                    FROM {DB_TABLE_ENTRIES} 
                    ORDER BY updated_at DESC 
                    LIMIT 5
                """)
                recent_entries["ranked_entries"] = [
                    {"listing_id": row[0], "user_id": row[1], "session_id": row[2], "updated_at": str(row[3])}
                    for row in cur.fetchall()
                ]
            except Exception as e:
                recent_entries["ranked_entries"] = f"ERROR: {str(e)}"
            
            return {
                "success": True,
                "database_connected": True,
                "tables_found": tables,
                "table_counts": table_counts,
                "recent_entries": recent_entries,
                "expected_tables": [DB_TABLE_BATCHES, DB_TABLE_ENTRIES, DB_TABLE_REVIEWS, DB_TABLE_SINGLE_SEARCHES],
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "database_connected": True,
        }
    finally:
        conn.close()

@app.get("/status")
def get_status(user_id: str, session_id: str) -> Dict:
    """
    Polling endpoint to check session status. 100% reliable - checks both
    in-memory sessions and persistent session metadata files.
    
    Use this as a fallback when SSE connection is lost. Poll every 2-5 seconds.
    
    Returns:
    - status: "running", "completed", "error", or "not_found"
    - megafile_path: path to megafile if completed
    - message: human-readable status message
    - timestamp: when status was last updated
    """
    import time
    
    user_id = user_id.strip()
    session_id = session_id.strip()
    
    if not user_id or not session_id:
        return {
            "success": False,
            "error": "user_id and session_id are required",
            "status": "error"
        }
    
    # First, check in-memory session (fast for active sessions)
    key = _session_key(user_id, "", session_id)  # session_id is the key
    with _SESSIONS_LOCK:
        sess = _SESSIONS.get(key)
        if sess:
            status = sess.get("status", "running")
            megafile_path = sess.get("megafile_path")
            ended_at = sess.get("ended_at")
            started_at = sess.get("started_at")
            
            # If completed, try to get ranked megafile from session meta file
            if status == "completed" and megafile_path:
                # Try to get ranked version from session meta
                try:
                    meta_path = _resolve_session_meta_path(user_id, session_id)
                    if meta_path and meta_path.exists():
                        with meta_path.open("r", encoding="utf-8") as f:
                            meta_doc = json.load(f)
                        ranked_path = meta_doc.get("last_ranked_megafile_path")
                        if ranked_path and Path(ranked_path).exists():
                            megafile_path = ranked_path
                except Exception:
                    pass  # Fall back to original megafile_path
            
            return {
                "success": True,
                "status": status,
                "megafile_path": megafile_path if status == "completed" else None,
                "message": f"Session is {status}",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ended_at if ended_at else started_at if started_at else time.time())),
                "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(started_at)) if started_at else None,
                "ended_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ended_at)) if ended_at else None,
            }
    
    # Fallback: check persistent session metadata file
    try:
        meta_path = _resolve_session_meta_path(user_id, session_id)
        if meta_path and meta_path.exists():
            with meta_path.open("r", encoding="utf-8") as f:
                meta_doc = json.load(f)
            
            # Determine status from metadata
            status = meta_doc.get("status", "completed")  # Default to completed if file exists
            if status not in ("running", "completed", "error"):
                # If no explicit status, check if megafile exists
                megafile_path = _find_megafile_for_user_session(user_id, session_id)
                status = "completed" if megafile_path else "not_found"
            else:
                megafile_path = _find_megafile_for_user_session(user_id, session_id) if status == "completed" else None
            
            # Get timestamps
            started_at = meta_doc.get("started_at")
            ended_at = meta_doc.get("ended_at")
            meta_block = meta_doc.get("meta", {})
            if not started_at:
                started_at = meta_block.get("started_at")
            if not ended_at:
                ended_at = meta_block.get("ended_at")
            
            # Convert timestamps if they're strings
            if isinstance(started_at, str):
                try:
                    started_at = time.mktime(time.strptime(started_at, "%Y-%m-%dT%H:%M:%S"))
                except Exception:
                    started_at = None
            if isinstance(ended_at, str):
                try:
                    ended_at = time.mktime(time.strptime(ended_at, "%Y-%m-%dT%H:%M:%S"))
                except Exception:
                    ended_at = None
            
            return {
                "success": True,
                "status": status,
                "megafile_path": megafile_path if status == "completed" else None,
                "message": f"Session is {status}",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ended_at if ended_at else started_at if started_at else time.time())),
                "started_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(started_at)) if started_at else None,
                "ended_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(ended_at)) if ended_at else None,
            }
    except Exception as e:
        print(f"[status] Error reading session metadata: {e}", flush=True)
    
    # Not found in memory or files
    return {
        "success": False,
        "status": "not_found",
        "message": "Session not found",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
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

def start_artifact_processor_thread(
    queue_path: str,
    run_root_dir: str,
    outputs_dir: str,
    listing_ids: List[int],
    slug: str,
    user_id: Optional[str] = None,
    keyword: Optional[str] = None,
    session_id: Optional[str] = None,
    entry_progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> "threading.Thread":
    """
    Stream-processor to merge demand artifacts into a megafile, now reading AI/Everbee
    outputs produced in parallel. Runs as a background thread and updates
    outputs/megafile_listings_{slug}.json in near realtime while optionally
    persisting finished entries to the database and emitting entry callbacks.
    """
    import threading
    t = threading.Thread(
        target=process_listing_artifacts_stream,
        args=(queue_path, run_root_dir, outputs_dir, listing_ids, slug, user_id, keyword, session_id, entry_progress_cb),
        daemon=True,
        name=f"artifact-processor-{Path(run_root_dir).name}",
    )
    t.start()
    return t

def process_listing_artifacts_stream(
    queue_path: str,
    run_root_dir: str,
    outputs_dir: str,
    listing_ids: List[int],
    slug: str,
    user_id: Optional[str] = None,
    keyword: Optional[str] = None,
    session_id: Optional[str] = None,
    entry_progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> None:
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

    pending_aux: set[int] = set()
    expected_ids = set(int(x) for x in listing_ids) if listing_ids else None
    target_total = len(expected_ids) if expected_ids else None
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
            if expected_ids is not None and li not in expected_ids:
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

            # Determine demand (skip entries with no demand)
            demand_value = obj.get("signals", {}).get("demand_value")
            if demand_value is None:
                demand_value = pop.get("demand") or pop.get("demand_value")

            keywords_ready = isinstance(keywords, list) and len(keywords) > 0
            everbee_ready = isinstance(everbee_results, list) and len(everbee_results) > 0

            if demand_value is None:
                processed.add(li)
                pending_aux.discard(li)
                print(f"[ArtifactProcessor] listing_id={li} skipped (demand missing).", flush=True)
                continue

            if not keywords_ready or not everbee_ready:
                pending_aux.add(li)
                continue

            shop_data = pop.get("shop")
            if not isinstance(shop_data, dict):
                shop_data = {}
            raw_reviews = shop_data.get("reviews")
            if not isinstance(raw_reviews, list):
                raw_reviews = []
            filtered_reviews = [rv for rv in raw_reviews if isinstance(rv, dict)]

            # Build entry
            entry = {
                "listing_id": li,
                "title": title,
                "popular_info": pop,
                "signals": obj.get("signals"),
                "demand_value": demand_value,
                "keywords": keywords,
                "everbee": {
                    "results": everbee_results
                },
                "primary_image": primary_image,
                "variations_cleaned": vclean,
                "sale_info": sale_info,
                "demand_extras": extras,
                "shop": shop_data,
                "reviews": filtered_reviews,
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
            pending_aux.discard(li)
            processed_count = len(processed)

            # Emit progress (no DB saving here - will be done in batch after ranking)
            if entry_progress_cb:
                try:
                    entry_progress_cb({
                        "listing_id": li,
                        "title": title,
                        "demand": entry.get("demand_value"),
                        "keywords_count": len(keywords),
                        "everbee_count": len(everbee_results),
                        "processed": processed_count,
                        "total": len(listing_ids) if listing_ids else None,
                        "megafile_path": str(megafile_path),
                        "db_status": "pending",
                        "db_entry_saved": False,
                        "timestamp": entry.get("timestamp"),
                    })
                except Exception as cb_err:
                    print(f"[ArtifactProcessor] WARN entry callback failed: {cb_err}", flush=True)

            found_new = True
            print(f"[ArtifactProcessor] listing_id={li} merged into megafile (keywords={len(keywords)}, everbee={len(everbee_results)})")

        waiting_for_aux = len(pending_aux) > 0

        # Exit conditions
        all_done = expected_ids is not None and processed.issuperset(expected_ids)
        if all_done:
            goal = target_total if target_total is not None else len(processed)
            print(f"[ArtifactProcessor] Completed. processed={len(processed)} of {goal}.")
            break

        # Secondary exit: queue finished and nothing new surfaced
        queue_completed = False
        try:
            with open(queue_path, "r", encoding="utf-8") as f:
                qobj = _json.load(f)
            queue_completed = str(qobj.get("status", "")).lower() == "completed"
        except Exception:
            queue_completed = not os.path.exists(queue_path)

        if queue_completed and not found_new and not waiting_for_aux:
            if expected_ids is None or processed.issuperset(expected_ids):
                goal = target_total if target_total is not None else len(processed)
                print(f"[ArtifactProcessor] Completed (queue finalized). processed={len(processed)} of {goal}.")
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

def orchestrate_run(
    user_id: str,
    keyword: str,
    desired_total: Optional[int] = None,
    progress_cb: Optional[Callable[[Dict], None]] = None,
    *,
    session_id: Optional[str] = None,
    entry_progress_cb: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict:
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
    target_listing_ids = popular_ids_dedup[:]

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
            target_listing_ids = fallback_ids[:]
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
        popular_queue_path,
        run_root_dir,
        outputs_dir,
        target_listing_ids or listing_ids,
        slug,
        user_id=user_id,
        keyword=keyword,
        session_id=session_id,
        entry_progress_cb=entry_progress_cb,
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

    # Rank the megafile before completion
    ranked_path = None
    try:
        if progress_cb:
            progress_cb({
                "stage": "ranking",
                "user_id": user_id,
                "remaining": 0,
                "total": total_popular,
                "message": "Ranking products...",
            })
        import ranking as rnk
        print("[Run] Ranking megafile before completion...", flush=True)
        ranked_path = rnk.rank_megafile(megafile_path)
        print(f"[Run] Ranked megafile created: {ranked_path}", flush=True)
    except Exception as rank_err:
        print(f"[Run] Ranking failed: {rank_err}", flush=True)
        ranked_path = megafile_path

    # Enrich all entries with listing details before saving
    try:
        if progress_cb:
            progress_cb({
                "stage": "enriching",
                "user_id": user_id,
                "remaining": 0,
                "total": total_popular,
                "message": "Fetching product details...",
            })
        with open(ranked_path, "r", encoding="utf-8") as f:
            ranked_doc = json.load(f)
        
        entries = ranked_doc.get("entries") or []
        print(f"[Run] Enriching {len(entries)} entries with listing details...", flush=True)
        
        for entry in entries:
            listing_id = entry.get("listing_id")
            if listing_id:
                try:
                    # Fetch and attach listing details
                    snapshot = etsy.collect_listing_detail_and_reviews(listing_id, reviews_limit=100)
                    detail = snapshot.get("detail")
                    if detail is not None:
                        entry["listing_details"] = detail
                    reviews = snapshot.get("reviews")
                    if reviews is not None:
                        entry["listing_reviews"] = reviews
                except Exception as enrich_err:
                    print(f"[Run] Failed to enrich listing_id={listing_id}: {enrich_err}", flush=True)
        
        # Save enriched megafile
        write_json_file(ranked_path, ranked_doc)
        print(f"[Run] Enriched megafile saved to {ranked_path}", flush=True)
    except Exception as enrich_err:
        print(f"[Run] Failed to enrich entries: {enrich_err}", flush=True)

    # Save all entries to database in batch (only after ranking and enrichment)
    persist_info = {"saved": 0, "batch_id": None, "enabled": bool(_db_settings())}
    if ranked_path:
        try:
            if progress_cb:
                progress_cb({
                    "stage": "saving",
                    "user_id": user_id,
                    "remaining": 0,
                    "total": total_popular,
                    "message": "Saving to database...",
                })
            with open(ranked_path, "r", encoding="utf-8") as f:
                final_doc = json.load(f)
            persist_info = persist_ranked_entries(
                final_doc,
                user_id=user_id,
                keyword=keyword,
                session_id=session_id,
                persist_reviews=True,
            )
            print(f"[Run] Saved {persist_info.get('saved', 0)} entries to database after ranking.", flush=True)
        except Exception as persist_err:
            print(f"[Run] Failed to persist entries: {persist_err}", flush=True)

    # Emit completion event with megafile path
    try:
        if progress_cb:
            progress_cb({
                "stage": "complete",
                "user_id": user_id,
                "remaining": 0,
                "total": total_popular,
                "message": "Run complete",
                "megafile_path": ranked_path or megafile_path,
                "entries_saved": persist_info.get("saved", 0),
                "batch_id": persist_info.get("batch_id"),
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
            "megafile_path": ranked_path or megafile_path,
            "run_root_dir": run_root_dir,
            "keyword_slug": slug,
        },
        "persist_info": persist_info,
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
        alt_file = everbee.discover_etsy_search_req_file_alt(Path(__file__).resolve().parent)
    except Exception as e:
        print(f"[KeywordsEverbee] ERROR: Etsy Search config failed: {e}")
        base_url, headers = None, None
        alt_file = None

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
            msg = f"[Everbee] listing_id={listing_id} kw='{keyword}' -> {r.get('status_code')} metrics={r.get('metrics')}"
            if r.get("fallback_used"):
                tried = alt_file.name if alt_file else "second request file"
                msg += f" | first failed, tried {tried}"
            print(msg)
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

    def entry_progress(info: Dict[str, Any]):
        try:
            payload = dict(info or {})
            payload.setdefault("type", "entry_progress")
            status = payload.get("db_status")
            if status == "db_saved":
                payload.setdefault("message", f"Listing {payload.get('listing_id')} saved to database.")
                payload.setdefault("db_entry_saved", True)
                payload.setdefault("db_saved_increment", payload.get("db_saved_increment") or 1)
            elif status == "db_error":
                payload.setdefault("message", f"Listing {payload.get('listing_id')} failed to save: {payload.get('db_error')}")
            elif status == "db_disabled":
                payload.setdefault("message", f"Listing {payload.get('listing_id')} ready (DB disabled).")
            emit(payload)
        except Exception:
            pass

    def worker():
        try:
            with sem:
                res = orchestrate_run(
                    user_id,
                    keyword,
                    payload.desired_total,
                    progress_cb=emit,
                    session_id=session_id,
                    entry_progress_cb=entry_progress,
                )
            # Get persist info from orchestrate_run result (already saved after ranking)
            megafile_path = (res.get("meta") or {}).get("megafile_path")
            persist_info = res.get("persist_info") or {"saved": 0, "batch_id": None, "enabled": bool(_db_settings())}
            
            if megafile_path:
                try:
                    update_session_meta_file(user_id, session_id, last_ranked_megafile_path=str(megafile_path))
                except Exception as meta_err:
                    print(f"[run_stream] Failed to update session meta: {meta_err}", flush=True)

            _session_update_meta(
                key,
                status="completed",
                ended_at=time.time(),
                megafile_path=megafile_path,
                run_root_dir=(res.get("meta") or {}).get("run_root_dir"),
            )
            # Persist status to file for polling endpoint reliability
            try:
                update_session_meta_file(
                    user_id,
                    session_id,
                    status="completed",
                    ended_at=time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
                    megafile_path=megafile_path,
                    run_root_dir=(res.get("meta") or {}).get("run_root_dir"),
                )
            except Exception as meta_err:
                print(f"[run_stream] Failed to persist session status to file: {meta_err}", flush=True)
            emit({
                "type": "complete",
                "success": res.get("success"),
                "result": res,
                "entries_saved": persist_info.get("saved"),
                "batch_id": persist_info.get("batch_id"),
                "source": "db",
                "db_enabled": persist_info.get("enabled", False),
                "persist_error": persist_info.get("error"),
            })
        except Exception as e:
            _session_update_meta(key, status="error", ended_at=time.time())
            # Persist error status to file
            try:
                update_session_meta_file(
                    user_id,
                    session_id,
                    status="error",
                    ended_at=time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())),
                    error=str(e),
                )
            except Exception as meta_err:
                print(f"[run_stream] Failed to persist error status to file: {meta_err}", flush=True)
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
        "session_id": payload.session_id.strip(),
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
                    session_id=jobs[job_id].get("session_id"),
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
            result = orchestrate_run(
                payload.user_id.strip(),
                payload.keyword.strip(),
                payload.desired_total,
                session_id=payload.session_id.strip(),
            )

        # On 100% success, return the full megafile JSON content
        if isinstance(result, dict) and result.get("success") is True:
            persist_info = {"saved": 0, "batch_id": None, "enabled": bool(_db_settings())}
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
                persist_info = persist_ranked_entries(
                    doc,
                    user_id=payload.user_id.strip(),
                    keyword=payload.keyword.strip(),
                    session_id=payload.session_id.strip(),
                    persist_reviews=True,
                )
                print(f"[db] Run /run persist summary: {persist_info}", flush=True)
                try:
                    # session_id is part of RunRequest
                    update_session_meta_file(payload.user_id.strip(), payload.session_id.strip(), last_ranked_megafile_path=str(ranked_path))
                except Exception as meta_err:
                    print(f"[run] Failed to update session meta (ranked): {meta_err}", flush=True)
                print("[ranking] Run: ranked megafile stored in DB.", flush=True)
                return {
                    "success": True,
                    "session_id": payload.session_id.strip(),
                    "keyword": payload.keyword.strip(),
                    "entries_saved": persist_info.get("saved"),
                    "batch_id": persist_info.get("batch_id"),
                    "db_enabled": persist_info.get("enabled", False),
                    "source": "db",
                    "message": "Ranked entries persisted to PostgreSQL",
                }
            except Exception as e:
                print(f"[ranking] Run: ranking failed; sending unranked. Error: {e}", flush=True)
                try:
                    with open(megafile_path, "r", encoding="utf-8") as f:
                        doc = json.load(f)
                    persist_info = persist_ranked_entries(
                        doc,
                        user_id=payload.user_id.strip(),
                        keyword=payload.keyword.strip(),
                        session_id=payload.session_id.strip(),
                        persist_reviews=True,
                    )
                    print(f"[db] Run /run persist summary (fallback): {persist_info}", flush=True)
                    return {
                        "success": True,
                        "session_id": payload.session_id.strip(),
                        "keyword": payload.keyword.strip(),
                        "entries_saved": persist_info.get("saved"),
                        "batch_id": persist_info.get("batch_id"),
                        "db_enabled": persist_info.get("enabled", False),
                        "source": "db",
                        "message": "Ranked entries stored using unranked fallback file",
                        "warning": "Ranking failed; data saved from unranked megafile",
                    }
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