import json
import time
from typing import Optional
import os
from pathlib import Path
import requests
from django.conf import settings
from django.http import JsonResponse, StreamingHttpResponse, HttpResponseBadRequest, Http404
from django.views.decorators.http import require_POST
from django.contrib.auth.decorators import login_required
from django.utils import timezone
from .stream_manager import bulk_stream_manager
from .models import BulkResearchSession
from django.views.decorators.csrf import csrf_exempt 
from .models import BulkResearchSession, BulkResearchEntry

# Module-level: hardcoded upstream API endpoints
UPSTREAM_STREAM_URL = "http://0.0.0.0:8001/run/stream"
UPSTREAM_RECONNECT_URL = "http://0.0.0.0:8001/reconnect/stream"

def _local_results_dir():
    base = getattr(settings, 'LOCAL_RESULTS_DIR', None)
    if not base:
        base = Path(getattr(settings, 'BASE_DIR', Path(__file__).resolve().parent.parent)) / 'local_data' / 'bulk_research' / 'sessions'
    p = Path(base)
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return p

def _result_file_path(session_id: int) -> Path:
    return _local_results_dir() / f'{session_id}.json'

def _load_result_json(session_id: int) -> dict:
    try:
        with open(_result_file_path(session_id), 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}

def _save_result_json(session_id: int, payload: dict) -> None:
    path = _result_file_path(session_id)
    try:
        tmp = path.with_suffix('.json.tmp')
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f)
        os.replace(tmp, path)
    except Exception:
        pass

def _save_entries_to_db(session, raw_entries):
    try:
        BulkResearchEntry.bulk_replace_for_session_id(session.id, raw_entries, chunk_size=1000)
    except Exception:
        pass

@login_required
def bulk_research_entries(request, session_id: int):
    try:
        session = BulkResearchSession.objects.get(id=session_id, user=request.user)
    except BulkResearchSession.DoesNotExist:
        raise Http404("Session not found")
    page = max(int(request.GET.get('page', '1')), 1)
    page_size = min(max(int(request.GET.get('page_size', '8')), 1), 100)
    sort = (request.GET.get('sort') or 'ranking').lower()
    order = (request.GET.get('order') or 'desc').lower()
    sort_map = {
        'ranking': 'ranking',
        'demand': 'demand',
        'price': 'price_value',
        'sale_price': 'sale_price_value',
        'views': 'views',
    }
    sort_field = sort_map.get(sort, 'ranking')
    ordering = sort_field if order == 'asc' else '-' + sort_field
    qs = BulkResearchEntry.objects.filter(session_id=session.id).order_by(ordering)
    total = qs.count()
    start = (page - 1) * page_size
    end = start + page_size
    rows = list(qs[start:end])

    def to_client(e: BulkResearchEntry):
        made_iso = e.created_at.isoformat() if e.created_at else (timezone.datetime.fromtimestamp(int(e.created_at_ts)).isoformat() if e.created_at_ts else None)
        made_disp = e.created_at.strftime('%b %d, %Y') if e.created_at else (timezone.datetime.fromtimestamp(int(e.created_at_ts)).strftime('%b %d, %Y') if e.created_at_ts else None)
        last_iso = e.last_modified_at.isoformat() if e.last_modified_at else (timezone.datetime.fromtimestamp(int(e.last_modified_ts)).isoformat() if e.last_modified_ts else None)
        last_disp = e.last_modified_at.strftime('%b %d, %Y') if e.last_modified_at else (timezone.datetime.fromtimestamp(int(e.last_modified_ts)).strftime('%b %d, %Y') if e.last_modified_ts else None)

        shop_created_iso = e.shop_created_at.isoformat() if e.shop_created_at else (timezone.datetime.fromtimestamp(int(e.shop_created_ts)).isoformat() if e.shop_created_ts else None)
        shop_created_disp = e.shop_created_at.strftime('%b %d, %Y') if e.shop_created_at else (timezone.datetime.fromtimestamp(int(e.shop_created_ts)).strftime('%b %d, %Y') if e.shop_created_ts else None)
        shop_updated_iso = e.shop_updated_at.isoformat() if e.shop_updated_at else (timezone.datetime.fromtimestamp(int(e.shop_updated_ts)).isoformat() if e.shop_updated_ts else None)
        shop_updated_disp = e.shop_updated_at.strftime('%b %d, %Y') if e.shop_updated_at else (timezone.datetime.fromtimestamp(int(e.shop_updated_ts)).strftime('%b %d, %Y') if e.shop_updated_ts else None)

        # Listing-specific reviews derived from shop_reviews or stored reviews
        listing_reviews = list(e.reviews or [])
        if not listing_reviews and e.listing_id:
            listing_reviews = [rv for rv in (e.shop_reviews or []) if (rv.get('listing_id') == e.listing_id)]
        # Normalize listing-specific review_count (int); fallback to computed length
        try:
            rc_raw = e.review_count
            if rc_raw is None:
                rc = None
            elif isinstance(rc_raw, (int, float)) and rc_raw >= 0:
                rc = int(rc_raw)
            elif isinstance(rc_raw, str):
                digits = ''.join(ch for ch in rc_raw if ch.isdigit())
                rc = int(digits) if digits else None
            else:
                rc = None
        except Exception:
            rc = None
        if rc is None:
            try:
                rc = len(listing_reviews) if listing_reviews else None
            except Exception:
                rc = None
        review_count = rc
        review_average = e.review_average
        if review_average is None and listing_reviews:
            try:
                review_average = round(sum((rv.get('rating') or 0) for rv in listing_reviews) / len(listing_reviews), 2)
            except Exception:
                review_average = None

        # Shop-level review metrics
        shop_review_count = None
        shop_review_average = None
        try:
            sd = e.shop_details or {}
            shop_review_count = sd.get('review_count')
            shop_review_average = sd.get('review_average')
        except Exception:
            pass
        if shop_review_count is None:
            shop_review_count = len(e.shop_reviews or [])
        if shop_review_average is None and (e.shop_reviews or []):
            try:
                shop_review_average = round(sum((rv.get('rating') or 0) for rv in e.shop_reviews) / max(len(e.shop_reviews), 1), 2)
            except Exception:
                shop_review_average = None

        sd = e.shop_details or {}
        shop_obj = {
            'shop_id': e.shop_id or sd.get('shop_id'),
            'shop_name': sd.get('shop_name'),
            'user_id': sd.get('user_id'),
            'created_timestamp': e.shop_created_ts,
            'created_iso': shop_created_iso,
            'created': shop_created_disp,
            'title': sd.get('title'),
            'announcement': sd.get('announcement'),
            'currency_code': sd.get('currency_code'),
            'is_vacation': sd.get('is_vacation'),
            'vacation_message': sd.get('vacation_message'),
            'sale_message': sd.get('sale_message'),
            'digital_sale_message': sd.get('digital_sale_message'),
            'updated_timestamp': e.shop_updated_ts,
            'updated_iso': shop_updated_iso,
            'updated': shop_updated_disp,
            'listing_active_count': sd.get('listing_active_count'),
            'digital_listing_count': sd.get('digital_listing_count'),
            'login_name': sd.get('login_name'),
            'accepts_custom_requests': sd.get('accepts_custom_requests'),
            'vacation_autoreply': sd.get('vacation_autoreply'),
            'url': sd.get('url'),
            'image_url_760x100': sd.get('image_url_760x100'),
            'icon_url_fullxfull': sd.get('icon_url_fullxfull'),
            'num_favorers': sd.get('num_favorers'),
            'languages': e.shop_languages or [],
            'review_average': shop_review_average,
            'review_count': shop_review_count,
            'sections': e.shop_sections or [],
            'reviews': e.shop_reviews or [],
            'shipping_from_country_iso': sd.get('shipping_from_country_iso'),
            'transaction_sold_count': sd.get('transaction_sold_count'),
        }

        sale_info = {
            'subtotal_after_discount': e.sale_price_display or '',
            'original_price': e.sale_original_price_display or '',
            'free_shipping': e.free_shipping,
            'active_promotion': {
                'id': e.sale_active_promotion_id or '',
                'start_timestamp': e.sale_active_promotion_start_ts,
                'end_timestamp': e.sale_active_promotion_end_ts,
                'created_timestamp': e.sale_active_promotion_created_ts,
                'updated_timestamp': e.sale_active_promotion_updated_ts,
                'buyer_promotion_name': e.buyer_promotion_name or '',
                'buyer_shop_promotion_name': e.buyer_shop_promotion_name or '',
                'buyer_promotion_description': e.buyer_promotion_description or '',
                'buyer_applied_promotion_description': e.buyer_applied_promotion_description or '',
            }
        }

        return {
            'listing_id': e.listing_id or '',
            'title': e.title or '',
            'url': e.url or '',
            'demand': e.demand,
            'ranking': e.ranking,
            'made_at': made_disp,
            'made_at_iso': made_iso,
            'made_at_ts': e.created_at_ts,
            'original_creation_timestamp': e.created_at_ts,
            'created_timestamp': e.created_at_ts,
            'last_modified_timestamp': e.last_modified_ts,
            'last_modified_iso': last_iso,
            'last_modified': last_disp,
            'primary_image': {'image_url': e.image_url or '', 'srcset': e.image_srcset or ''},
            'variations_cleaned': {'variations': e.variations or []},
            'variations': e.variations or [],
            'has_variations': bool(e.variations),
            'tags': e.tags or [],
            'materials': e.materials or [],
            'keywords': e.keywords or [],
            'sections': e.shop_sections or [],
            'reviews': listing_reviews,
            'review_average': review_average,
            'review_count': review_count,
            'keyword_insights': e.keyword_insights or [],
            'buyer_promotion_name': e.buyer_promotion_name or '',
            'buyer_shop_promotion_name': e.buyer_shop_promotion_name or '',
            'buyer_promotion_description': e.buyer_promotion_description or '',
            'buyer_applied_promotion_description': e.buyer_applied_promotion_description or '',
            'sale_percent': e.sale_percent,
            'sale_price_value': e.sale_price_value,
            'sale_price_display': e.sale_price_display or '',
            'sale_subtotal_after_discount': e.sale_price_display or '',
            'sale_original_price': e.sale_original_price,
            'price_amount': e.price_amount_raw,
            'price_divisor': e.price_divisor_raw,
            'price_currency': e.price_currency or '',
            'price': {'amount': e.price_amount_raw, 'divisor': e.price_divisor_raw, 'currency_code': e.price_currency or ''},
            'price_value': e.price_value,
            'price_display': e.price_display or '',
            'quantity': e.quantity,
            'num_favorers': e.num_favorers,
            'listing_type': e.listing_type or '',
            'file_data': e.file_data or '',
            'views': e.views,
            'demand_extras': e.demand_extras or {},
            'user_id': e.user_id or '',
            'shop_id': e.shop_id or '',
            'state': e.state or '',
            'shop': shop_obj,
            'sale_info': sale_info,
        }

    return JsonResponse({
        'session_id': session.id,
        'page': page,
        'page_size': page_size,
        'total': total,
        'done': end >= total,
        'entries': [to_client(r) for r in rows],
    })

@login_required
def bulk_research_entries_all(request):
    page = max(int(request.GET.get('page', '1')), 1)
    page_size = min(max(int(request.GET.get('page_size', '8')), 1), 100)
    sort = (request.GET.get('sort') or 'ranking').lower()
    order = (request.GET.get('order') or 'desc').lower()
    sort_map = {
        'ranking': 'ranking',
        'demand': 'demand',
        'price': 'price_value',
        'sale_price': 'sale_price_value',
        'views': 'views',
    }
    sort_field = sort_map.get(sort, 'ranking')
    ordering = sort_field if order == 'asc' else '-' + sort_field
    qs = BulkResearchEntry.objects.filter(session__user=request.user).order_by(ordering)
    total = qs.count()
    start = (page - 1) * page_size
    end = start + page_size
    rows = list(qs[start:end])

    def to_client(e: BulkResearchEntry):
        made_iso = e.created_at.isoformat() if e.created_at else (timezone.datetime.fromtimestamp(int(e.created_at_ts)).isoformat() if e.created_at_ts else None)
        made_disp = e.created_at.strftime('%b %d, %Y') if e.created_at else (timezone.datetime.fromtimestamp(int(e.created_at_ts)).strftime('%b %d, %Y') if e.created_at_ts else None)
        last_iso = e.last_modified_at.isoformat() if e.last_modified_at else (timezone.datetime.fromtimestamp(int(e.last_modified_ts)).isoformat() if e.last_modified_ts else None)
        last_disp = e.last_modified_at.strftime('%b %d, %Y') if e.last_modified_at else (timezone.datetime.fromtimestamp(int(e.last_modified_ts)).strftime('%b %d, %Y') if e.last_modified_ts else None)

        shop_created_iso = e.shop_created_at.isoformat() if e.shop_created_at else (timezone.datetime.fromtimestamp(int(e.shop_created_ts)).isoformat() if e.shop_created_ts else None)
        shop_created_disp = e.shop_created_at.strftime('%b %d, %Y') if e.shop_created_at else (timezone.datetime.fromtimestamp(int(e.shop_created_ts)).strftime('%b %d, %Y') if e.shop_created_ts else None)
        shop_updated_iso = e.shop_updated_at.isoformat() if e.shop_updated_at else (timezone.datetime.fromtimestamp(int(e.shop_updated_ts)).isoformat() if e.shop_updated_ts else None)
        shop_updated_disp = e.shop_updated_at.strftime('%b %d, %Y') if e.shop_updated_at else (timezone.datetime.fromtimestamp(int(e.shop_updated_ts)).strftime('%b %d, %Y') if e.shop_updated_ts else None)

        listing_reviews = list(e.reviews or [])
        if not listing_reviews and e.listing_id:
            listing_reviews = [rv for rv in (e.shop_reviews or []) if (rv.get('listing_id') == e.listing_id)]
        # Normalize listing-specific review_count (int); fallback to computed length
        try:
            rc_raw = e.review_count
            if rc_raw is None:
                rc = None
            elif isinstance(rc_raw, (int, float)) and rc_raw >= 0:
                rc = int(rc_raw)
            elif isinstance(rc_raw, str):
                digits = ''.join(ch for ch in rc_raw if ch.isdigit())
                rc = int(digits) if digits else None
            else:
                rc = None
        except Exception:
            rc = None
        if rc is None:
            try:
                rc = len(listing_reviews) if listing_reviews else None
            except Exception:
                rc = None
        review_count = rc
        review_average = e.review_average
        if review_average is None and listing_reviews:
            try:
                review_average = round(sum((rv.get('rating') or 0) for rv in listing_reviews) / len(listing_reviews), 2)
            except Exception:
                review_average = None

        # Shop-level review metrics
        shop_review_count = None
        shop_review_average = None
        try:
            sd = e.shop_details or {}
            shop_review_count = sd.get('review_count')
            shop_review_average = sd.get('review_average')
        except Exception:
            pass
        if shop_review_count is None:
            shop_review_count = len(e.shop_reviews or [])
        if shop_review_average is None and (e.shop_reviews or []):
            try:
                shop_review_average = round(sum((rv.get('rating') or 0) for rv in e.shop_reviews) / max(len(e.shop_reviews), 1), 2)
            except Exception:
                shop_review_average = None

        sd = e.shop_details or {}
        shop_obj = {
            'shop_id': e.shop_id or sd.get('shop_id'),
            'shop_name': sd.get('shop_name'),
            'user_id': sd.get('user_id'),
            'created_timestamp': e.shop_created_ts,
            'created_iso': shop_created_iso,
            'created': shop_created_disp,
            'title': sd.get('title'),
            'announcement': sd.get('announcement'),
            'currency_code': sd.get('currency_code'),
            'is_vacation': sd.get('is_vacation'),
            'vacation_message': sd.get('vacation_message'),
            'sale_message': sd.get('sale_message'),
            'digital_sale_message': sd.get('digital_sale_message'),
            'updated_timestamp': e.shop_updated_ts,
            'updated_iso': shop_updated_iso,
            'updated': shop_updated_disp,
            'listing_active_count': sd.get('listing_active_count'),
            'digital_listing_count': sd.get('digital_listing_count'),
            'login_name': sd.get('login_name'),
            'accepts_custom_requests': sd.get('accepts_custom_requests'),
            'vacation_autoreply': sd.get('vacation_autoreply'),
            'url': sd.get('url'),
            'image_url_760x100': sd.get('image_url_760x100'),
            'icon_url_fullxfull': sd.get('icon_url_fullxfull'),
            'num_favorers': sd.get('num_favorers'),
            'languages': e.shop_languages or [],
            'review_average': shop_review_average,
            'review_count': shop_review_count,
            'sections': e.shop_sections or [],
            'reviews': e.shop_reviews or [],
            'shipping_from_country_iso': sd.get('shipping_from_country_iso'),
            'transaction_sold_count': sd.get('transaction_sold_count'),
        }

        sale_info = {
            'subtotal_after_discount': e.sale_price_display or '',
            'original_price': e.sale_original_price_display or '',
            'free_shipping': e.free_shipping,
            'active_promotion': {
                'id': e.sale_active_promotion_id or '',
                'start_timestamp': e.sale_active_promotion_start_ts,
                'end_timestamp': e.sale_active_promotion_end_ts,
                'created_timestamp': e.sale_active_promotion_created_ts,
                'updated_timestamp': e.sale_active_promotion_updated_ts,
                'buyer_promotion_name': e.buyer_promotion_name or '',
                'buyer_shop_promotion_name': e.buyer_shop_promotion_name or '',
                'buyer_promotion_description': e.buyer_promotion_description or '',
                'buyer_applied_promotion_description': e.buyer_applied_promotion_description or '',
            }
        }

        return {
            'listing_id': e.listing_id or '',
            'title': e.title or '',
            'url': e.url or '',
            'demand': e.demand,
            'ranking': e.ranking,
            'made_at': made_disp,
            'made_at_iso': made_iso,
            'made_at_ts': e.created_at_ts,
            'original_creation_timestamp': e.created_at_ts,
            'created_timestamp': e.created_at_ts,
            'last_modified_timestamp': e.last_modified_ts,
            'last_modified_iso': last_iso,
            'last_modified': last_disp,
            'primary_image': {'image_url': e.image_url or '', 'srcset': e.image_srcset or ''},
            'variations_cleaned': {'variations': e.variations or []},
            'variations': e.variations or [],
            'has_variations': bool(e.variations),
            'tags': e.tags or [],
            'materials': e.materials or [],
            'keywords': e.keywords or [],
            'sections': e.shop_sections or [],
            'reviews': listing_reviews,
            'review_average': review_average,
            'review_count': review_count,
            'keyword_insights': e.keyword_insights or [],
            'buyer_promotion_name': e.buyer_promotion_name or '',
            'buyer_shop_promotion_name': e.buyer_shop_promotion_name or '',
            'buyer_promotion_description': e.buyer_promotion_description or '',
            'buyer_applied_promotion_description': e.buyer_applied_promotion_description or '',
            'sale_percent': e.sale_percent,
            'sale_price_value': e.sale_price_value,
            'sale_price_display': e.sale_price_display or '',
            'sale_subtotal_after_discount': e.sale_price_display or '',
            'sale_original_price': e.sale_original_price,
            'price_amount': e.price_amount_raw,
            'price_divisor': e.price_divisor_raw,
            'price_currency': e.price_currency or '',
            'price': {'amount': e.price_amount_raw, 'divisor': e.price_divisor_raw, 'currency_code': e.price_currency or ''},
            'price_value': e.price_value,
            'price_display': e.price_display or '',
            'quantity': e.quantity,
            'num_favorers': e.num_favorers,
            'listing_type': e.listing_type or '',
            'file_data': e.file_data or '',
            'views': e.views,
            'demand_extras': e.demand_extras or {},
            'user_id': e.user_id or '',
            'shop_id': e.shop_id or '',
            'state': e.state or '',
            'shop': shop_obj,
            'sale_info': sale_info,
        }

    return JsonResponse({
        'page': page,
        'page_size': page_size,
        'total': total,
        'done': end >= total,
        'entries': [to_client(r) for r in rows],
    })

@login_required
@require_POST
def bulk_research_check(request, session_id: int):
    """
    Check if session's result_file has entries. If empty, perform a reconnect to
    fetch and save final entries. Always returns a 'checked' flag; includes counts
    and progress when available.
    """
    try:
        session = BulkResearchSession.objects.get(id=session_id, user=request.user)
    except BulkResearchSession.DoesNotExist:
        raise Http404("Session not found")

    existing_qs_count = BulkResearchEntry.objects.filter(session_id=session.id).count()
    if existing_qs_count > 0:
        _ensure_completed_if_result_exists(session)
        return JsonResponse({
            'checked': True,
            'status': session.status,
            'entries_count': existing_qs_count,
            'progress': session.progress or BulkResearchSession.build_initial_progress(session.desired_total),
            'source': 'db'
        })

    # Ensure background worker is running so we can join stream updates and persist to DB
    try:
        bulk_stream_manager.ensure_worker(session, user_id=request.user.username)
    except Exception:
        pass

    payload = {
        'user_id': request.user.username,
        'keyword': session.keyword,
        'desired_total': session.desired_total,
        'session_id': session.external_session_id,
    }
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    try:
        resp = requests.post(UPSTREAM_RECONNECT_URL, json=payload, headers=headers, stream=True, timeout=300)
    except Exception as exc:
        return JsonResponse({'checked': False, 'error': f'Upstream reconnect error: {exc}'}, status=502)

    if not resp.ok:
        msg = None
        try:
            msg = resp.json().get('error')
        except Exception:
            msg = (resp.text or '')[:400]
        return JsonResponse({'checked': False, 'error': msg or f'Upstream failed ({resp.status_code})'}, status=resp.status_code)

    progress = session.progress or BulkResearchSession.build_initial_progress(session.desired_total)
    final_raw = None
    attached_to_stream = False
    buffer_acc = ''

    # Try full-body JSON first (stop reconnecting immediately if entries present)
    try:
        full_json = resp.json()
    except Exception:
        full_json = None

    if isinstance(full_json, dict) and (
        isinstance(full_json.get('entries'), list) or
        (isinstance(full_json.get('megafile'), dict) and isinstance(full_json['megafile'].get('entries'), list))
    ):
        final_raw = full_json
    else:
        # Stream incrementally; wait until we fully parse final JSON, or detect stream reattachment
        try:
            for chunk in resp.iter_lines(decode_unicode=True):
                if not chunk:
                    continue
                line = (chunk or '').strip()
                if not line:
                    continue
                if line.startswith('data:'):
                    line = line[5:].strip()

                obj = None
                try:
                    obj = json.loads(line)
                except Exception:
                    # Accumulate non-JSON fragments; attempt to parse assembled buffer
                    buffer_acc += line
                    try:
                        obj = json.loads(buffer_acc)
                        # Only accept assembled JSON if it has entries or megafile entries
                        if not (isinstance(obj.get('entries'), list) or (obj.get('megafile') and isinstance(obj['megafile'].get('entries'), list))):
                            obj = None
                    except Exception:
                        obj = None

                if not obj:
                    continue

                # Progress updates
                stage = (obj.get('stage') or obj.get('phase') or '').lower()
                key = _map_stage_key(stage)
                if key:
                    total = obj.get('total') or obj.get('entries_total')
                    remaining = obj.get('remaining') or obj.get('entries_remaining')
                    p = progress.get(key) or {'total': session.desired_total, 'remaining': session.desired_total}
                    if isinstance(total, (int, float)):
                        p['total'] = int(total)
                    if isinstance(remaining, (int, float)):
                        p['remaining'] = int(remaining)
                    progress[key] = p
                    try:
                        BulkResearchSession.objects.filter(id=session.id).update(progress=progress)
                    except Exception:
                        pass

                # Final payload containing entries — stop reconnecting
                if isinstance(obj.get('entries'), list) or (obj.get('megafile') and isinstance(obj['megafile'].get('entries'), list)):
                    final_raw = obj
                    break

                # Detect stream reattachment; stop reconnecting and let worker continue
                status_lower = (obj.get('status') or '').lower()
                stage_lower = (obj.get('stage') or '').lower()
                if status_lower in ('attached', 'rejoined', 'streaming', 'ongoing') or stage_lower in ('attached', 'stream'):
                    attached_to_stream = True
                    break

            # Try parsing any remaining buffered large JSON
            if not final_raw and buffer_acc.strip():
                try:
                    buf_obj = json.loads(buffer_acc)
                except Exception:
                    buf_obj = None
                if buf_obj and (isinstance(buf_obj.get('entries'), list) or (buf_obj.get('megafile') and isinstance(buf_obj['megafile'].get('entries'), list))):
                    final_raw = buf_obj
        except Exception as exc:
            try:
                resp.close()
            except Exception:
                pass
            return JsonResponse({'checked': False, 'error': f'Reconnect stream error: {exc}'}, status=502)

    try:
        resp.close()
    except Exception:
        pass

    entries_count = 0
    if final_raw:
        try:
            entries = []
            if isinstance(final_raw.get('entries'), list):
                entries = final_raw['entries']
            elif final_raw.get('megafile') and isinstance(final_raw['megafile'].get('entries'), list):
                entries = final_raw['megafile']['entries']

            _save_entries_to_db(session, entries)
            entries_count = BulkResearchEntry.objects.filter(session_id=session.id).count()

            try:
                BulkResearchSession.objects.filter(id=session.id).update(
                    status='completed',
                    completed_at=timezone.now()
                )
                session.status = 'completed'
                session.completed_at = timezone.now()
                session.progress = {
                    'search':    {'total': session.desired_total, 'remaining': 0},
                    'splitting': {'total': max(progress.get('splitting', {}).get('total') or 2, 2), 'remaining': 0},
                    'demand':    {'total': entries_count, 'remaining': 0},
                    'keywords':  {'total': progress.get('keywords', {}).get('total') or 0, 'remaining': 0},
                }
                BulkResearchSession.objects.filter(id=session.id).update(progress=session.progress)
                session.save(update_fields=['status', 'completed_at', 'progress'])
            except Exception:
                pass

            simplified = _simplify_entries(entries)
            return JsonResponse({
                'checked': True,
                'updated': True,
                'status': session.status,
                'entries_count': entries_count,
                'progress': session.progress,
                'source': 'reconnect',
                'entries': simplified
            })
        except Exception:
            pass

    if attached_to_stream:
        # Joined stream: reflect current snapshot/DB while the worker continues updating backend
        snap = bulk_stream_manager.get_snapshot(session.id) or {}
        entries_count = BulkResearchEntry.objects.filter(session_id=session.id).count()
        return JsonResponse({
            'checked': True,
            'updated': False,
            'status': snap.get('status') or session.status or 'ongoing',
            'entries_count': entries_count,
            'progress': snap.get('progress') or progress,
            'source': 'stream',
            'joined_stream': True,
        })

    return JsonResponse({
        'checked': True,
        'updated': False,
        'status': session.status,
        'entries_count': entries_count,
        'progress': progress,
        'source': 'none'
    })

@login_required
@require_POST
def bulk_research_replace_listing(request):
    try:
        payload = json.loads(request.body or '{}')
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    listing_id = payload.get('listing_id')
    session_id = payload.get('session_id')
    forced_personalize = bool(payload.get('forced_personalize'))

    if not listing_id or not session_id:
        return HttpResponseBadRequest("Missing listing_id or session_id")

    try:
        session = BulkResearchSession.objects.get(id=int(session_id), user=request.user)
    except (BulkResearchSession.DoesNotExist, ValueError):
        raise Http404("Session not found")

    upstream_body = {
        'listing_id': listing_id,
        'user_id': request.user.username,
        'session_id': session.external_session_id or str(session_id),
        'forced_personalize': forced_personalize,
    }
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    try:
        resp = requests.post("http://0.0.0.0:8001/replace-listing", json=upstream_body, headers=headers, timeout=60)
    except Exception as e:
        return JsonResponse({'error': f'Upstream request failed: {str(e)}'}, status=502)

    if not resp.ok:
        # Try to extract message
        msg = None
        try:
            msg = resp.json().get('error')
        except Exception:
            msg = (resp.text or '')[:400]
        return JsonResponse({'error': msg or f'Upstream failed ({resp.status_code})'}, status=resp.status_code)

    try:
        full_json = resp.json()
    except Exception:
        full_json = {}

    # Persist to DB instead of local files
    try:
        if isinstance(full_json.get('entries'), list):
            _save_entries_to_db(session, full_json['entries'])
        elif isinstance(full_json.get('megafile'), dict) and isinstance(full_json['megafile'].get('entries'), list):
            _save_entries_to_db(session, full_json['megafile']['entries'])
        elif isinstance(full_json.get('entry'), dict):
            BulkResearchEntry.upsert_single(session.id, full_json['entry'])
        elif isinstance(full_json.get('item'), dict):
            BulkResearchEntry.upsert_single(session.id, full_json['item'])
    except Exception:
        pass

    # If entries present in DB, mark completed and normalize progress
    _ensure_completed_if_result_exists(session)

    # Build response using DB counts
    entries_count = BulkResearchEntry.objects.filter(session_id=session.id).count()
    return JsonResponse({
        'status': 'ok',
        'session_id': session.id,
        'entries_count': entries_count,
    })

def _api_url(name: str, job_id: Optional[str] = None) -> str:
    url = getattr(settings, name, None)
    if not url:
        raise RuntimeError(f"Missing settings.{name}")
    return url.format(job_id=job_id) if (job_id and '{job_id}' in url) else url

def _map_stage_key(stage: str) -> Optional[str]:
    s = (stage or '').lower()
    if s == 'search':
        return 'search'
    if s == 'splitting':
        return 'splitting'
    if s == 'demand_extraction':
        return 'demand'
    if s in ('ai_keywords', 'keywords_research'):
        return 'keywords'
    return None

def _normalize_progress_full(total: int):
    return {
        'search':    {'total': total, 'remaining': 0},
        'splitting': {'total': total, 'remaining': 0},
        'demand':    {'total': total, 'remaining': 0},
        'keywords':  {'total': total, 'remaining': 0},
    }

def _extract_entries_from_result_file(session) -> list:
    try:
        raw = _load_result_json(session.id)
        if isinstance(raw, dict):
            if isinstance(raw.get('entries'), list):
                return raw['entries']
            if raw.get('megafile') and isinstance(raw['megafile'].get('entries'), list):
                return raw['megafile']['entries']
    except Exception:
        return []
    return []

def _ensure_completed_if_result_exists(session) -> Optional[int]:
    """
    If the session has DB entries and is not yet completed,
    mark it completed and normalize progress. Returns entries_count when updated.
    """
    entries_count = BulkResearchEntry.objects.filter(session_id=session.id).count()
    if entries_count > 0 and (session.status != 'completed'):
        progress = _normalize_progress_full(session.desired_total)
        BulkResearchSession.objects.filter(id=session.id).update(
            status='completed',
            completed_at=timezone.now(),
            progress=progress
        )
        # also update instance in-memory for immediate response usage
        session.status = 'completed'
        session.progress = progress
        session.completed_at = timezone.now()
        return entries_count
    return None

def _simplify_entries(entries):
    simplified = []
    for entry in entries:
        popular = entry.get('popular_info') or {}

        listing_id = entry.get('listing_id') or popular.get('listing_id') or ''
        title = popular.get('title') or entry.get('title') or ''
        url = popular.get('url') or entry.get('url') or ''
        demand = popular.get('demand', entry.get('demand', None))
        ranking_raw = (entry.get('ranking') or popular.get('ranking') or entry.get('Ranking') or popular.get('Ranking') or entry.get('rank') or popular.get('rank'))
        try:
            ranking = float(ranking_raw) if isinstance(ranking_raw, (int, float, str)) and str(ranking_raw).strip() else None
        except Exception:
            ranking = None

        user_id = entry.get('user_id') or popular.get('user_id')
        shop_id = entry.get('shop_id') or popular.get('shop_id')
        state = entry.get('state') or popular.get('state') or ''
        description = popular.get('description') or entry.get('description') or ''

        ts = (popular.get('original_creation_timestamp')
              or popular.get('created_timestamp')
              or entry.get('original_creation_timestamp')
              or entry.get('created_timestamp'))
        made_at_iso = None
        made_at_display = None
        if ts is not None:
            try:
                dt = timezone.datetime.fromtimestamp(int(ts))
                made_at_iso = dt.isoformat()
                made_at_display = dt.strftime('%b %d, %Y')
            except Exception:
                made_at_display = str(ts)

        primary_image = popular.get('primary_image') or entry.get('primary_image') or {}
        image_url = primary_image.get('image_url') or ''
        srcset = primary_image.get('srcset') or ''

        variations_cleaned = popular.get('variations_cleaned') or entry.get('variations_cleaned') or {}
        var_variations = []
        try:
            vlist = variations_cleaned.get('variations') or []
            if isinstance(vlist, list):
                for v in vlist:
                    if not isinstance(v, dict):
                        continue
                    vid = v.get('id')
                    vtitle = v.get('title')
                    vopts = v.get('options') or []
                    opts_out = []
                    if isinstance(vopts, list):
                        for o in vopts:
                            if isinstance(o, dict):
                                opts_out.append({
                                    'value': o.get('value'),
                                    'label': o.get('label'),
                                })
                    var_variations.append({
                        'id': vid,
                        'title': vtitle,
                        'options': opts_out,
                    })
        except Exception:
            pass

        last_modified_ts = popular.get('last_modified_timestamp') or entry.get('last_modified_timestamp')
        last_modified_iso = None
        last_modified_display = None
        if last_modified_ts is not None:
            try:
                dt = timezone.datetime.fromtimestamp(int(last_modified_ts))
                last_modified_iso = dt.isoformat()
                last_modified_display = dt.strftime('%b %d, %Y')
            except Exception:
                last_modified_display = str(last_modified_ts)

        quantity = entry.get('quantity') or popular.get('quantity')
        num_favorers = entry.get('num_favorers') or popular.get('num_favorers')
        listing_type = entry.get('listing_type') or popular.get('listing_type') or ''
        file_data = entry.get('file_data') or popular.get('file_data') or ''
        views = entry.get('views') or popular.get('views')

        tags = popular.get('tags') or entry.get('tags') or []
        if not isinstance(tags, list):
            tags = []
        materials = popular.get('materials') or entry.get('materials') or []
        if not isinstance(materials, list):
            materials = []
        keywords = entry.get('keywords') or popular.get('keywords') or []
        if not isinstance(keywords, list):
            keywords = []

        price = popular.get('price') or entry.get('price') or {}
        price_amount = price.get('amount')
        price_divisor = price.get('divisor')
        price_currency = price.get('currency_code') or ''
        price_value = None
        price_display = None
        try:
            if isinstance(price_amount, (int, float)) and isinstance(price_divisor, int) and price_divisor:
                price_value = float(price_amount) / int(price_divisor)
                disp = ('{:.2f}'.format(price_value)).rstrip('0').rstrip('.')
                price_display = f'{disp} {price_currency}'.strip()
        except Exception:
            pass

        import re
        sale_info = popular.get('sale_info') or entry.get('sale_info') or {}
        active_promo = sale_info.get('active_promotion') or {}
        buyer_promotion_name = active_promo.get('buyer_promotion_name') or ''
        buyer_shop_promotion_name = active_promo.get('buyer_shop_promotion_name') or ''
        buyer_promotion_description = active_promo.get('buyer_promotion_description') or ''
        buyer_applied_promotion_description = active_promo.get('buyer_applied_promotion_description') or ''

        promo_text = buyer_applied_promotion_description or buyer_promotion_description or ''
        sale_percent = None
        m = re.search(r'(\d+(?:\.\d+)?)\s*%', promo_text)
        if m:
            try:
                sale_percent = float(m.group(1))
            except Exception:
                sale_percent = None
        if sale_percent is None and isinstance(active_promo.get('seller_marketing_promotion'), dict):
            pct = active_promo['seller_marketing_promotion'].get('order_discount_pct')
            if isinstance(pct, (int, float)):
                sale_percent = float(pct)

        sale_subtotal_after_discount = sale_info.get('subtotal_after_discount')
        sale_original_price = sale_info.get('original_price')

        sale_price_value = None
        sale_price_display = None
        if isinstance(sale_subtotal_after_discount, str) and sale_subtotal_after_discount.strip():
            sale_price_display = sale_subtotal_after_discount.strip()
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
                sale_price_display = f'{disp} {price_currency}'.strip()
            except Exception:
                sale_price_value = None
                sale_price_display = None

        shop_obj = popular.get('shop') or entry.get('shop') or {}
        shop_details = shop_obj.get('details') or {}
        sh_shop_id = shop_obj.get('shop_id') or shop_details.get('shop_id')

        shop_created_ts = (shop_details.get('created_timestamp')
                           or shop_details.get('create_date')
                           or shop_obj.get('created_timestamp'))
        shop_created_iso = None
        shop_created_display = None
        if shop_created_ts is not None:
            try:
                dt = timezone.datetime.fromtimestamp(int(shop_created_ts))
                shop_created_iso = dt.isoformat()
                shop_created_display = dt.strftime('%b %d, %Y')
            except Exception:
                shop_created_display = str(shop_created_ts)

        shop_updated_ts = (shop_details.get('updated_timestamp')
                           or shop_details.get('update_date')
                           or shop_obj.get('updated_timestamp'))
        shop_updated_iso = None
        shop_updated_display = None
        if shop_updated_ts is not None:
            try:
                dt = timezone.datetime.fromtimestamp(int(shop_updated_ts))
                shop_updated_iso = dt.isoformat()
                shop_updated_display = dt.strftime('%b %d, %Y')
            except Exception:
                shop_updated_display = str(shop_updated_ts)

        shop_sections = shop_obj.get('sections')
        if not isinstance(shop_sections, list):
            shop_sections = []

        shop_reviews = shop_obj.get('reviews')
        shop_reviews_simplified = []
        if isinstance(shop_reviews, list):
            for rv in shop_reviews:
                if not isinstance(rv, dict):
                    continue
                cts = rv.get('created_timestamp') or rv.get('create_timestamp')
                uts = rv.get('updated_timestamp') or rv.get('update_timestamp')
                c_iso = None
                c_disp = None
                u_iso = None
                u_disp = None
                if cts is not None:
                    try:
                        dt = timezone.datetime.fromtimestamp(int(cts))
                        c_iso = dt.isoformat()
                        c_disp = dt.strftime('%b %d, %Y')
                    except Exception:
                        c_disp = str(cts)
                if uts is not None:
                    try:
                        dt = timezone.datetime.fromtimestamp(int(uts))
                        u_iso = dt.isoformat()
                        u_disp = dt.strftime('%b %d, %Y')
                    except Exception:
                        u_disp = str(uts)
                shop_reviews_simplified.append({
                    'shop_id': rv.get('shop_id'),
                    'listing_id': rv.get('listing_id'),
                    'transaction_id': rv.get('transaction_id'),
                    'buyer_user_id': rv.get('buyer_user_id'),
                    'rating': rv.get('rating'),
                    'review': rv.get('review'),
                    'language': rv.get('language'),
                    'image_url_fullxfull': rv.get('image_url_fullxfull'),
                    'created_timestamp': cts,
                    'created_iso': c_iso,
                    'created': c_disp,
                    'updated_timestamp': uts,
                    'updated_iso': u_iso,
                    'updated': u_disp,
                })

        relevant_reviews = [rv for rv in shop_reviews_simplified if rv.get('listing_id') == listing_id]
        review_count_listing = len(relevant_reviews)
        review_average_listing = None
        if review_count_listing:
            try:
                review_average_listing = round(
                    sum((rv.get('rating') or 0) for rv in relevant_reviews) / review_count_listing, 2
                )
            except Exception:
                review_average_listing = None

        shop_languages = shop_details.get('languages')
        if not isinstance(shop_languages, list):
            shop_languages = []

        everbee = entry.get('everbee') or popular.get('everbee') or {}
        everbee_results = everbee.get('results') or []
        keyword_insights = []
        if isinstance(everbee_results, list):
            for res in everbee_results:
                if not isinstance(res, dict):
                    continue
                kw = res.get('keyword') or res.get('query') or ''
                metrics = res.get('metrics') or {}
                vol = metrics.get('vol')
                comp = metrics.get('competition')
                resp = res.get('response') or {}
                stats_obj = resp.get('stats') or {}
                if vol is None:
                    sv = stats_obj.get('searchVolume')
                    if isinstance(sv, (int, float)):
                        vol = sv
                if comp is None:
                    atl = stats_obj.get('avgTotalListings')
                    if isinstance(atl, (int, float)):
                        comp = atl
                daily_block = resp.get('dailyStats') or {}
                daily_stats_list = daily_block.get('stats') or []
                daily_stats = []
                if isinstance(daily_stats_list, list):
                    for d in daily_stats_list:
                        if isinstance(d, dict):
                            daily_stats.append({
                                'date': d.get('date'),
                                'searchVolume': d.get('searchVolume')
                            })
                keyword_insights.append({
                    'keyword': kw,
                    'vol': vol,
                    'competition': comp,
                    'stats': stats_obj,
                    'dailyStats': daily_stats,
                })

        shop_result = {
            'shop_id': sh_shop_id,
            'shop_name': shop_details.get('shop_name'),
            'user_id': shop_details.get('user_id'),
            'created_timestamp': shop_created_ts,
            'created_iso': shop_created_iso,
            'created': shop_created_display,
            'title': shop_details.get('title'),
            'announcement': shop_details.get('announcement'),
            'currency_code': shop_details.get('currency_code'),
            'is_vacation': shop_details.get('is_vacation'),
            'vacation_message': shop_details.get('vacation_message'),
            'sale_message': shop_details.get('sale_message'),
            'digital_sale_message': shop_details.get('digital_sale_message'),
            'updated_timestamp': shop_updated_ts,
            'updated_iso': shop_updated_iso,
            'updated': shop_updated_display,
            'listing_active_count': shop_details.get('listing_active_count'),
            'digital_listing_count': shop_details.get('digital_listing_count'),
            'login_name': shop_details.get('login_name'),
            'accepts_custom_requests': shop_details.get('accepts_custom_requests'),
            'vacation_autoreply': shop_details.get('vacation_autoreply'),
            'url': shop_details.get('url') or shop_obj.get('url'),
            'image_url_760x100': shop_details.get('image_url_760x100'),
            'icon_url_fullxfull': shop_details.get('icon_url_fullxfull'),
            'num_favorers': shop_details.get('num_favorers'),
            'languages': shop_languages,
            'review_average': shop_details.get('review_average'),
            'review_count': shop_details.get('review_count'),
            'sections': shop_sections,
            'reviews': shop_reviews_simplified,
            'shipping_from_country_iso': shop_details.get('shipping_from_country_iso'),
            'transaction_sold_count': shop_details.get('transaction_sold_count'),
        }

        simplified.append({
            'listing_id': listing_id,
            'title': title,
            'url': url,
            'demand': demand,
            'ranking': ranking,
            'made_at': made_at_display,
            'made_at_iso': made_at_iso,
            'primary_image': { 'image_url': image_url, 'srcset': srcset },
            'variations': var_variations,
            'has_variations': (len(var_variations) > 0),
            'user_id': user_id,
            'shop_id': shop_id or sh_shop_id,
            'state': state,
            'description': description,
            'tags': tags,
            'materials': materials,
            'keywords': keywords,
            'sections': shop_sections,
            'reviews': shop_reviews_simplified,
            'review_average': review_average_listing,
            'review_count': review_count_listing,
            'keyword_insights': keyword_insights,
            'buyer_promotion_name': buyer_promotion_name,
            'buyer_shop_promotion_name': buyer_shop_promotion_name,
            'buyer_promotion_description': buyer_promotion_description,
            'buyer_applied_promotion_description': buyer_applied_promotion_description,
            'sale_percent': sale_percent,
            'sale_price_value': sale_price_value,
            'sale_price_display': sale_price_display,
            'sale_subtotal_after_discount': sale_subtotal_after_discount,
            'sale_original_price': sale_original_price,
            'price_amount': price_amount,
            'price_divisor': price_divisor,
            'price_currency': price_currency,
            'price_value': price_value,
            'price_display': price_display,
            'last_modified_timestamp': last_modified_ts,
            'last_modified_iso': last_modified_iso,
            'last_modified': last_modified_display,
            'quantity': quantity,
            'num_favorers': num_favorers,
            'listing_type': listing_type,
            'file_data': file_data,
            'views': views,
            'shop': shop_result,
        })
    return simplified

@login_required
@require_POST
def bulk_research_reconnect(request, session_id: int):
    try:
        session = BulkResearchSession.objects.get(id=session_id, user=request.user)
    except BulkResearchSession.DoesNotExist:
        raise Http404("Session not found")

    # BEFORE contacting upstream: if DB already has final results, finalize and return immediately
    existing_count = BulkResearchEntry.objects.filter(session_id=session.id).count()
    if existing_count > 0:
        changed_count = _ensure_completed_if_result_exists(session)
        return JsonResponse({
            'status': 'completed',
            'progress': session.progress or _normalize_progress_full(session.desired_total),
            'entries_count': existing_count,
            'source': 'db'
        })

    payload = {
        'user_id': request.user.username,
        'keyword': session.keyword,
        'desired_total': session.desired_total,
        'session_id': session.external_session_id,
    }
    headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

    try:
        resp = requests.post(UPSTREAM_RECONNECT_URL, json=payload, headers=headers, stream=True, timeout=300)
    except Exception as exc:
        return JsonResponse({'error': f'Upstream reconnect error: {exc}'}, status=502)

    progress = session.progress or BulkResearchSession.build_initial_progress(session.desired_total)
    final_raw = None
    buffer_acc = ''

    try:
        for chunk in resp.iter_lines(decode_unicode=True):
            if not chunk:
                continue
            line = chunk.strip()
            if not line:
                continue
            # Handle SSE style: "data: {...}"
            if line.startswith('data:'):
                line = line[5:].strip()

            obj = None
            try:
                obj = json.loads(line)
            except Exception:
                buffer_acc += line
                continue

            stage = (obj.get('stage') or obj.get('phase') or '').lower()
            key = _map_stage_key(stage)
            if key:
                total = obj.get('total') or obj.get('entries_total')
                remaining = obj.get('remaining') or obj.get('entries_remaining')
                p = progress.get(key) or {'total': session.desired_total, 'remaining': session.desired_total}
                if isinstance(total, (int, float)):
                    p['total'] = int(total)
                if isinstance(remaining, (int, float)):
                    p['remaining'] = int(remaining)
                progress[key] = p
                BulkResearchSession.objects.filter(id=session.id).update(progress=progress)

            # Detect final payload containing entries
            if isinstance(obj.get('entries'), list) or (obj.get('megafile') and isinstance(obj['megafile'].get('entries'), list)):
                final_raw = obj
                break

            # Mark completed only if upstream explicitly indicates completion
            if (obj.get('status') or '').lower() == 'completed':
                session.status = 'completed'
                session.completed_at = timezone.now()
                session.progress = progress
                session.save(update_fields=['status', 'completed_at', 'progress'])
        # Try parsing buffered large JSON (if sent as one big chunk)
        if not final_raw and buffer_acc.strip():
            try:
                final_raw = json.loads(buffer_acc)
            except Exception:
                final_raw = None
    except Exception:
        # Swallow streaming errors; rely on any final payload or partial updates
        pass

    entries_count = 0
    if final_raw:
        try:
            entries = []
            if isinstance(final_raw.get('entries'), list):
                entries = final_raw['entries']
            elif final_raw.get('megafile') and isinstance(final_raw['megafile'].get('entries'), list):
                entries = final_raw['megafile']['entries']

            # Persist to DB
            _save_entries_to_db(session, entries)
            entries_count = BulkResearchEntry.objects.filter(session_id=session.id).count()

            # Mark completed and normalize progress
            try:
                BulkResearchSession.objects.filter(id=session.id).update(
                    status='completed',
                    completed_at=timezone.now()
                )
                session.status = 'completed'
                session.completed_at = timezone.now()
                session.progress = {
                    'search':    {'total': session.desired_total, 'remaining': 0},
                    'splitting': {'total': session.desired_total, 'remaining': 0},
                    'demand':    {'total': entries_count, 'remaining': 0},
                    'keywords':  {'total': progress.get('keywords', {}).get('total') or 0, 'remaining': 0},
                }
                session.save(update_fields=['status', 'completed_at', 'progress'])
            except Exception:
                pass

            # Build simplified for client convenience
            simplified = _simplify_entries(entries)

            return JsonResponse({
                'status': session.status,
                'progress': session.progress,
                'entries_count': entries_count,
                'entries': simplified,
                'source': 'reconnect'
            })
        except Exception:
            pass

    # Do NOT force-complete simply because entries exist during reconnect
    # if session.status != 'completed' and entries_count > 0:
    #     session.status = 'completed'
    #     session.completed_at = timezone.now()
    #     session.save(update_fields=['status', 'completed_at'])

    # Finalize progress only when the session is actually completed
    try:
        prog = progress or {}
        if session.status == 'completed':
            # Search: equals desired_total, remaining 0
            prog['search'] = {
                'total': int(session.desired_total or 0),
                'remaining': 0
            }
            # Splitting: preserve discovered total if present, else default to 2; remaining 0
            sp = prog.get('splitting') or {}
            sp_total = sp.get('total')
            prog['splitting'] = {
                'total': int(sp_total if isinstance(sp_total, int) and sp_total > 0 else 2),
                'remaining': 0
            }
            # Demand: total equals entries_count if known, else preserve; remaining 0
            dp = prog.get('demand') or {}
            dp_total = dp.get('total') or 0
            prog['demand'] = {
                'total': int(entries_count if entries_count > 0 else dp_total),
                'remaining': 0
            }
            # Keywords: preserve discovered total; mark remaining 0
            kp = prog.get('keywords') or {}
            kp_total = kp.get('total') or 0
            prog['keywords'] = {
                'total': int(kp_total),
                'remaining': 0
            }
            BulkResearchSession.objects.filter(id=session.id).update(progress=prog)
            session.progress = prog
            session.save(update_fields=['progress'])
        else:
            # Keep best-effort partial progress (do not zero remaining values)
            BulkResearchSession.objects.filter(id=session.id).update(progress=progress)
            session.progress = progress
            session.save(update_fields=['progress'])
    except Exception:
        # keep best-effort progress
        pass

    return JsonResponse({
        'ok': True,
        'session_id': session.id,
        'status': session.status,
        'entries_count': entries_count,
        'progress': progress,
        'source': 'reconnect'
    })

@login_required
@require_POST
def bulk_research_start(request):
    try:
        payload = json.loads(request.body.decode('utf-8'))
    except Exception:
        return HttpResponseBadRequest("Invalid JSON")

    keyword = (payload.get('keyword') or '').strip()
    desired_total = int(payload.get('desired_total') or 0)
    if not keyword or desired_total <= 0:
        return HttpResponseBadRequest("Keyword and desired_total are required")

    # Create local session only; upstream kicks off in stream attach
    session = BulkResearchSession.objects.create(
        user=request.user,
        keyword=keyword,
        desired_total=desired_total,
        status='ongoing',
        progress=BulkResearchSession.build_initial_progress(desired_total),
        started_at=timezone.now(),
    )
    # Generate external session id: user, keyword, desired_total, time, random 5 digits
    try:
        import re, random
        ts = timezone.now().strftime('%Y%m%d%H%M%S')
        slug = re.sub(r'[^a-z0-9]+', '-', keyword.lower()).strip('-')[:30]
        rand5 = ''.join(random.choice('0123456789') for _ in range(5))
        external_id = f"sess-{request.user.username}-{slug}-{desired_total}-{ts}-{rand5}"
        session.external_session_id = external_id
        session.save(update_fields=['external_session_id'])
    except Exception:
        pass

    # Start background listener so it keeps going across refresh/offline
    try:
        bulk_stream_manager.ensure_worker(session, user_id=request.user.username)
    except Exception:
        pass
    return JsonResponse({'session_id': session.id})

@login_required
def bulk_research_stream(request, session_id: int):
    try:
        session = BulkResearchSession.objects.get(id=session_id, user=request.user)
    except BulkResearchSession.DoesNotExist:
        raise Http404("Session not found")

    def to_event(obj):
        return f"data: {json.dumps(obj)}\n\n"

    # IMPORTANT: do not auto-start worker here to avoid duplicate upstream runs.
    sub = bulk_stream_manager.subscribe_events(session.id)

    def proxy():
        try:
            # Send initial snapshot first (unchanged)
            snap = bulk_stream_manager.get_snapshot(session.id)
            if snap:
                yield to_event({
                    'stage': 'snapshot',
                    'status': snap.get('status'),
                    'progress': snap.get('progress'),
                    'entries_count': len(snap.get('entries') or []),
                })
            else:
                # Fallback from local file when no worker is active
                try:
                    rf = _load_result_json(session.id)
                except Exception:
                    rf = {}
                entries = rf.get('entries') or []
                progress = session.progress or BulkResearchSession.build_initial_progress(session.desired_total)
                yield to_event({
                    'stage': 'snapshot',
                    'status': session.status,
                    'progress': progress,
                    'entries_count': len(entries),
                })

            # Gate 'completed' until after final JSON/snapshot is sent
            completed_pending = False
            completed_evt = None

            if sub is not None:
                for evt in sub:
                    stage_lower = str((evt.get('stage') or '')).lower()
                    status_lower = str((evt.get('status') or '')).lower()
                    is_completed_status = (stage_lower == 'status' and status_lower == 'completed')
                    has_entries = (
                        isinstance(evt.get('entries'), list) or
                        (isinstance(evt.get('megafile'), dict) and isinstance(evt['megafile'].get('entries'), list))
                    )
                    is_snapshot = (stage_lower == 'snapshot')

                    if is_completed_status:
                        # Hold completed; do not emit yet
                        completed_pending = True
                        completed_evt = evt
                        continue

                    # Emit normal events
                    yield to_event(evt)

                    # Once entries or snapshot appear, release the held completed
                    if completed_pending and (has_entries or is_snapshot):
                        yield to_event(completed_evt or {'stage': 'status', 'status': 'completed'})
                        completed_pending = False
                        completed_evt = None
            else:
                # Avoid client auto-reconnect loops by keeping a live heartbeat
                while True:
                    yield to_event({'stage': 'heartbeat', 'ts': int(time.time())})
                    time.sleep(15)
        except (GeneratorExit, BrokenPipeError, ConnectionResetError, OSError):
            # Client disconnected; stop streaming quietly
            return

    resp = StreamingHttpResponse(proxy(), content_type='text/event-stream')
    resp['Cache-Control'] = 'no-cache'
    resp['X-Accel-Buffering'] = 'no'
    return resp

@login_required
@require_POST
def bulk_research_delete(request, session_id: int):
    try:
        session = BulkResearchSession.objects.get(id=session_id, user=request.user)
    except BulkResearchSession.DoesNotExist:
        raise Http404("Session not found")

    if session.status == 'ongoing':
        return JsonResponse({'error': 'Cannot delete ongoing session'}, status=400)

    # Remove local result file if exists
    try:
        p = _result_file_path(session_id)
        if p.exists():
            p.unlink()
    except Exception:
        pass

    session.delete()
    return JsonResponse({'deleted': True})

@login_required
def bulk_research_result(request, session_id: int):
    try:
        session = BulkResearchSession.objects.get(id=session_id, user=request.user)
    except BulkResearchSession.DoesNotExist:
        raise Http404("Session not found")

    entries = []
    if session.status == 'ongoing':
        snap = bulk_stream_manager.get_snapshot(session_id)
        if snap and isinstance(snap.get('entries'), list) and snap['entries']:
            entries = snap['entries']

    if not entries:
        rows = list(BulkResearchEntry.objects.filter(session_id=session.id).order_by('-ranking', '-demand'))

        def to_client(e: BulkResearchEntry):
            made_iso = e.created_at.isoformat() if e.created_at else (timezone.datetime.fromtimestamp(int(e.created_at_ts)).isoformat() if e.created_at_ts else None)
            made_disp = e.created_at.strftime('%b %d, %Y') if e.created_at else (timezone.datetime.fromtimestamp(int(e.created_at_ts)).strftime('%b %d, %Y') if e.created_at_ts else None)
            last_iso = e.last_modified_at.isoformat() if e.last_modified_at else (timezone.datetime.fromtimestamp(int(e.last_modified_ts)).isoformat() if e.last_modified_ts else None)
            last_disp = e.last_modified_at.strftime('%b %d, %Y') if e.last_modified_at else (timezone.datetime.fromtimestamp(int(e.last_modified_ts)).strftime('%b %d, %Y') if e.last_modified_ts else None)

            shop_created_iso = e.shop_created_at.isoformat() if e.shop_created_at else (timezone.datetime.fromtimestamp(int(e.shop_created_ts)).isoformat() if e.shop_created_ts else None)
            shop_created_disp = e.shop_created_at.strftime('%b %d, %Y') if e.shop_created_at else (timezone.datetime.fromtimestamp(int(e.shop_created_ts)).strftime('%b %d, %Y') if e.shop_created_ts else None)
            shop_updated_iso = e.shop_updated_at.isoformat() if e.shop_updated_at else (timezone.datetime.fromtimestamp(int(e.shop_updated_ts)).isoformat() if e.shop_updated_ts else None)
            shop_updated_disp = e.shop_updated_at.strftime('%b %d, %Y') if e.shop_updated_at else (timezone.datetime.fromtimestamp(int(e.shop_updated_ts)).strftime('%b %d, %Y') if e.shop_updated_ts else None)

            listing_reviews = list(e.reviews or [])
            if (not listing_reviews) and e.listing_id:
                lid_str = str(e.listing_id)
                try:
                    listing_reviews = [rv for rv in (e.shop_reviews or []) if str(rv.get('listing_id')) == lid_str]
                except Exception:
                    listing_reviews = []

            # Normalize listing-specific review_count (int); fallback to computed length
            try:
                rc_raw = e.review_count
                if rc_raw is None:
                    rc = None
                elif isinstance(rc_raw, (int, float)) and rc_raw >= 0:
                    rc = int(rc_raw)
                elif isinstance(rc_raw, str):
                    digits = ''.join(ch for ch in rc_raw if ch.isdigit())
                    rc = int(digits) if digits else None
                else:
                    rc = None
            except Exception:
                rc = None
            if rc is None:
                try:
                    rc = len(listing_reviews) if listing_reviews else None
                except Exception:
                    rc = None
            review_count = rc

            review_average = e.review_average
            if review_average is None and listing_reviews:
                try:
                    review_average = round(sum((rv.get('rating') or 0) for rv in listing_reviews) / len(listing_reviews), 2)
                except Exception:
                    review_average = None

            # Shop-level review metrics
            shop_review_count = None
            shop_review_average = None
            try:
                sd = e.shop_details or {}
                shop_review_count = sd.get('review_count')
                shop_review_average = sd.get('review_average')
            except Exception:
                pass
            if shop_review_count is None:
                shop_review_count = len(e.shop_reviews or [])
            if shop_review_average is None and (e.shop_reviews or []):
                try:
                    shop_review_average = round(sum((rv.get('rating') or 0) for rv in e.shop_reviews) / max(len(e.shop_reviews), 1), 2)
                except Exception:
                    shop_review_average = None

            sd = e.shop_details or {}
            shop_obj = {
                'shop_id': e.shop_id or sd.get('shop_id'),
                'shop_name': sd.get('shop_name'),
                'user_id': sd.get('user_id'),
                'shop_name': sd.get('shop_name'),
                'user_id': sd.get('user_id'),
                'created_timestamp': e.shop_created_ts,
                'created_iso': shop_created_iso,
                'created': shop_created_disp,
                'title': sd.get('title'),
                'announcement': sd.get('announcement'),
                'currency_code': sd.get('currency_code'),
                'is_vacation': sd.get('is_vacation'),
                'vacation_message': sd.get('vacation_message'),
                'sale_message': sd.get('sale_message'),
                'digital_sale_message': sd.get('digital_sale_message'),
                'updated_timestamp': e.shop_updated_ts,
                'updated_iso': shop_updated_iso,
                'updated': shop_updated_disp,
                'listing_active_count': sd.get('listing_active_count'),
                'digital_listing_count': sd.get('digital_listing_count'),
                'login_name': sd.get('login_name'),
                'accepts_custom_requests': sd.get('accepts_custom_requests'),
                'vacation_autoreply': sd.get('vacation_autoreply'),
                'url': sd.get('url'),
                'image_url_760x100': sd.get('image_url_760x100'),
                'icon_url_fullxfull': sd.get('icon_url_fullxfull'),
                'num_favorers': sd.get('num_favorers'),
                'languages': e.shop_languages or [],
                'review_average': shop_review_average,
                'review_count': shop_review_count,
                'sections': e.shop_sections or [],
                'reviews': e.shop_reviews or [],
                'shipping_from_country_iso': sd.get('shipping_from_country_iso'),
                'transaction_sold_count': sd.get('transaction_sold_count'),
            }

            # Normalize demand_extras keys (snake_case vs camelCase)
            de = e.demand_extras or {}
            demand_extras = {
                'total_carts': (de.get('total_carts') if 'total_carts' in de else de.get('totalCarts')),
                'quantity': (
                    de.get('quantity') if 'quantity' in de
                    else (de.get('QuantityDE') if 'QuantityDE' in de else de.get('demand_quantity'))
                ),
                'estimated_delivery_date': (
                    de.get('estimated_delivery_date') if 'estimated_delivery_date' in de
                    else (de.get('estimatedDeliveryDate') if 'estimatedDeliveryDate' in de else de.get('est_delivery'))
                ),
                'free_shipping': (de.get('free_shipping') if 'free_shipping' in de else de.get('freeShipping')),
            }

            return {
                'listing_id': e.listing_id or '',
                'title': e.title or '',
                'url': e.url or '',
                'demand': e.demand,
                'ranking': e.ranking,
                'made_at': made_disp,
                'made_at_iso': made_iso,
                'made_at_ts': e.created_at_ts,
                'original_creation_timestamp': e.created_at_ts,
                'created_timestamp': e.created_at_ts,
                'last_modified_timestamp': e.last_modified_ts,
                'last_modified_iso': last_iso,
                'last_modified': last_disp,
                'primary_image': {'image_url': e.image_url or '', 'srcset': e.image_srcset or ''},
                'variations_cleaned': {'variations': e.variations or []},
                'variations': e.variations or [],
                'has_variations': bool(e.variations),
                'tags': e.tags or [],
                'materials': e.materials or [],
                'keywords': e.keywords or [],
                'sections': e.shop_sections or [],
                'reviews': listing_reviews,
                'review_average': review_average,
                'review_count': review_count,
                'keyword_insights': e.keyword_insights or [],
                'buyer_promotion_name': e.buyer_promotion_name or '',
                'buyer_shop_promotion_name': e.buyer_shop_promotion_name or '',
                'buyer_promotion_description': e.buyer_promotion_description or '',
                'buyer_applied_promotion_description': e.buyer_applied_promotion_description or '',
                'sale_percent': e.sale_percent,
                'sale_price_value': e.sale_price_value,
                'sale_price_display': e.sale_price_display or '',
                'sale_subtotal_after_discount': e.sale_price_display or '',
                'sale_original_price': e.sale_original_price,
                'price_amount': e.price_amount_raw,
                'price_divisor': e.price_divisor_raw,
                'price_currency': e.price_currency or '',
                'price': {'amount': e.price_amount_raw, 'divisor': e.price_divisor_raw, 'currency_code': e.price_currency or ''},
                'price_value': e.price_value,
                'price_display': e.price_display or '',
                'quantity': e.quantity,
                'num_favorers': e.num_favorers,
                'listing_type': e.listing_type or '',
                'file_data': e.file_data or '',
                'views': e.views,
                'demand_extras': demand_extras,
                'user_id': e.user_id or '',
                'shop_id': e.shop_id or '',
                'state': e.state or '',
                'description': e.description or '',
                'shop': shop_obj,
            }

        simplified = [to_client(r) for r in rows]
        return JsonResponse({'entries': simplified})

    simplified = []
    for entry in entries:
        popular = entry.get('popular_info') or {}

        # Basic fields
        listing_id = entry.get('listing_id') or popular.get('listing_id') or ''
        title = popular.get('title') or entry.get('title') or ''
        url = popular.get('url') or entry.get('url') or ''
        demand = popular.get('demand', entry.get('demand', None))
        ranking_raw = (entry.get('ranking') or popular.get('ranking') or entry.get('Ranking') or popular.get('Ranking') or entry.get('rank') or popular.get('rank'))
        try:
            ranking = float(ranking_raw) if isinstance(ranking_raw, (int, float, str)) and str(ranking_raw).strip() else None
        except Exception:
            ranking = None

        # IDs/state/description
        user_id = entry.get('user_id') or popular.get('user_id')
        shop_id = entry.get('shop_id') or popular.get('shop_id')
        state = entry.get('state') or popular.get('state') or ''
        description = popular.get('description') or entry.get('description') or ''

        # Made at (display + iso)
        ts = (popular.get('original_creation_timestamp')
            or popular.get('created_timestamp')
            or entry.get('original_creation_timestamp')
            or entry.get('created_timestamp'))
        made_at_iso = None
        made_at_display = None
        made_at_ts = None
        if ts is not None:
            try:
                ts_int = int(ts)
                dt = timezone.datetime.fromtimestamp(ts_int)
                made_at_iso = dt.isoformat()
                made_at_display = dt.strftime('%b %d, %Y')
                made_at_ts = ts_int
            except Exception:
                made_at_display = str(ts)

        # Primary image
        primary_image = popular.get('primary_image') or entry.get('primary_image') or {}
        image_url = primary_image.get('image_url') or ''
        srcset = primary_image.get('srcset') or ''

        # Variations (variations_cleaned.variations)
        variations_cleaned = popular.get('variations_cleaned') or entry.get('variations_cleaned') or {}
        var_variations = []
        try:
            vlist = variations_cleaned.get('variations') or []
            if isinstance(vlist, list):
                for v in vlist:
                    if not isinstance(v, dict):
                        continue
                    vid = v.get('id')
                    vtitle = v.get('title')
                    vopts = v.get('options') or []
                    opts_out = []
                    if isinstance(vopts, list):
                        for o in vopts:
                            if isinstance(o, dict):
                                opts_out.append({
                                    'value': o.get('value'),
                                    'label': o.get('label'),
                                })
                    var_variations.append({
                        'id': vid,
                        'title': vtitle,
                        'options': opts_out,
                    })
        except Exception:
            pass

        # Extra product detail fields
        last_modified_ts = popular.get('last_modified_timestamp') or entry.get('last_modified_timestamp')
        last_modified_iso = None
        last_modified_display = None
        if last_modified_ts is not None:
            try:
                dt = timezone.datetime.fromtimestamp(int(last_modified_ts))
                last_modified_iso = dt.isoformat()
                last_modified_display = dt.strftime('%b %d, %Y')
            except Exception:
                last_modified_display = str(last_modified_ts)

        quantity = entry.get('quantity') or popular.get('quantity')
        num_favorers = entry.get('num_favorers') or popular.get('num_favorers')
        listing_type = entry.get('listing_type') or popular.get('listing_type') or ''
        file_data = entry.get('file_data') or popular.get('file_data') or ''
        views = entry.get('views') or popular.get('views')

        # Tags, materials, keywords
        tags = popular.get('tags') or entry.get('tags') or []
        if not isinstance(tags, list):
            tags = []
        materials = popular.get('materials') or entry.get('materials') or []
        if not isinstance(materials, list):
            materials = []
        keywords = entry.get('keywords') or popular.get('keywords') or []
        if not isinstance(keywords, list):
            keywords = []

        # Price (base)
        price = popular.get('price') or entry.get('price') or {}
        price_amount = price.get('amount')
        price_divisor = price.get('divisor')
        price_currency = price.get('currency_code') or ''
        price_value = None
        price_display = None
        try:
            if isinstance(price_amount, (int, float)) and isinstance(price_divisor, int) and price_divisor:
                price_value = float(price_amount) / int(price_divisor)
                disp = ('{:.2f}'.format(price_value)).rstrip('0').rstrip('.')
                price_display = f'{disp} {price_currency}'.strip()
        except Exception:
            pass

        # Sale info and computed sale price
        import re
        sale_info = popular.get('sale_info') or entry.get('sale_info') or {}
        active_promo = sale_info.get('active_promotion') or {}
        buyer_promotion_name = active_promo.get('buyer_promotion_name') or ''
        buyer_shop_promotion_name = active_promo.get('buyer_shop_promotion_name') or ''
        buyer_promotion_description = active_promo.get('buyer_promotion_description') or ''
        buyer_applied_promotion_description = active_promo.get('buyer_applied_promotion_description') or ''

        promo_text = buyer_applied_promotion_description or buyer_promotion_description or ''
        sale_percent = None
        m = re.search(r'(\d+(?:\.\d+)?)\s*%', promo_text)
        if m:
            try:
                sale_percent = float(m.group(1))
            except Exception:
                sale_percent = None
        if sale_percent is None and isinstance(active_promo.get('seller_marketing_promotion'), dict):
            pct = active_promo['seller_marketing_promotion'].get('order_discount_pct')
            if isinstance(pct, (int, float)):
                sale_percent = float(pct)

        sale_subtotal_after_discount = sale_info.get('subtotal_after_discount')
        sale_original_price = sale_info.get('original_price')

        sale_price_value = None
        sale_price_display = None
        if isinstance(sale_subtotal_after_discount, str) and sale_subtotal_after_discount.strip():
            sale_price_display = sale_subtotal_after_discount.strip()
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
                sale_price_display = f'{disp} {price_currency}'.strip()
            except Exception:
                sale_price_value = None
                sale_price_display = None

        # Shop details
        shop_obj = popular.get('shop') or entry.get('shop') or {}
        shop_details = shop_obj.get('details') or {}
        sh_shop_id = shop_obj.get('shop_id') or shop_details.get('shop_id')

        shop_created_ts = (shop_details.get('created_timestamp')
                           or shop_details.get('create_date')
                           or shop_obj.get('created_timestamp'))
        shop_created_iso = None
        shop_created_display = None
        if shop_created_ts is not None:
            try:
                dt = timezone.datetime.fromtimestamp(int(shop_created_ts))
                shop_created_iso = dt.isoformat()
                shop_created_display = dt.strftime('%b %d, %Y')
            except Exception:
                shop_created_display = str(shop_created_ts)

        shop_updated_ts = (shop_details.get('updated_timestamp')
                           or shop_details.get('update_date')
                           or shop_obj.get('updated_timestamp'))
        shop_updated_iso = None
        shop_updated_display = None
        if shop_updated_ts is not None:
            try:
                dt = timezone.datetime.fromtimestamp(int(shop_updated_ts))
                shop_updated_iso = dt.isoformat()
                shop_updated_display = dt.strftime('%b %d, %Y')
            except Exception:
                shop_updated_display = str(shop_updated_ts)

        # Shop sections
        shop_sections = shop_obj.get('sections')
        if not isinstance(shop_sections, list):
            shop_sections = []

        # Shop reviews
        shop_reviews = popular.get('shop_reviews') or entry.get('shop_reviews') or []
        shop_reviews_simplified = []
        if isinstance(shop_reviews, list):
            for rv in shop_reviews:
                if not isinstance(rv, dict):
                    continue
                cts = rv.get('created_timestamp') or rv.get('create_timestamp')
                uts = rv.get('updated_timestamp') or rv.get('update_timestamp')
                c_iso = None
                c_disp = None
                u_iso = None
                u_disp = None
                if cts is not None:
                    try:
                        dt = timezone.datetime.fromtimestamp(int(cts))
                        c_iso = dt.isoformat()
                        c_disp = dt.strftime('%b %d, %Y')
                    except Exception:
                        c_disp = str(cts)
                if uts is not None:
                    try:
                        dt = timezone.datetime.fromtimestamp(int(uts))
                        u_iso = dt.isoformat()
                        u_disp = dt.strftime('%b %d, %Y')
                    except Exception:
                        u_disp = str(uts)
                shop_reviews_simplified.append({
                    'shop_id': rv.get('shop_id'),
                    'listing_id': rv.get('listing_id'),
                    'transaction_id': rv.get('transaction_id'),
                    'buyer_user_id': rv.get('buyer_user_id'),
                    'rating': rv.get('rating'),
                    'review': rv.get('review'),
                    'language': rv.get('language'),
                    'image_url_fullxfull': rv.get('image_url_fullxfull'),
                    'created_timestamp': cts,
                    'created_iso': c_iso,
                    'created': c_disp,
                    'updated_timestamp': uts,
                    'updated_iso': u_iso,
                    'updated': u_disp,
                })

        # Compute listing-level review metrics (use string-safe comparison)
        listing_id_str = str(listing_id) if listing_id is not None else ''
        relevant_reviews = []
        try:
            for rv in shop_reviews_simplified:
                li = rv.get('listing_id')
                if li is not None and str(li) == listing_id_str:
                    relevant_reviews.append(rv)
        except Exception:
            relevant_reviews = []
        review_count_listing = len(relevant_reviews)
        review_average_listing = None
        if review_count_listing:
            try:
                review_average_listing = round(
                    sum((rv.get('rating') or 0) for rv in relevant_reviews) / review_count_listing, 2
                )
            except Exception:
                review_average_listing = None

        # Shop languages
        shop_languages = shop_details.get('languages')
        if not isinstance(shop_languages, list):
            shop_languages = []

        # Keyword insights
        everbee = entry.get('everbee') or popular.get('everbee') or {}
        everbee_results = everbee.get('results') or []
        keyword_insights = []
        if isinstance(everbee_results, list):
            for res in everbee_results:
                if not isinstance(res, dict):
                    continue
                kw = res.get('keyword') or res.get('query') or ''
                metrics = res.get('metrics') or {}
                vol = metrics.get('vol')
                comp = metrics.get('competition')
                resp = res.get('response') or {}
                stats_obj = resp.get('stats') or {}
                if vol is None:
                    sv = stats_obj.get('searchVolume')
                    if isinstance(sv, (int, float)):
                        vol = sv
                if comp is None:
                    atl = stats_obj.get('avgTotalListings')
                    if isinstance(atl, (int, float)):
                        comp = atl
                daily_block = resp.get('dailyStats') or {}
                daily_stats_list = daily_block.get('stats') or []
                daily_stats = []
                if isinstance(daily_stats_list, list):
                    for d in daily_stats_list:
                        if isinstance(d, dict):
                            daily_stats.append({
                                'date': d.get('date'),
                                'searchVolume': d.get('searchVolume')
                            })
                keyword_insights.append({
                    'keyword': kw,
                    'vol': vol,
                    'competition': comp,
                    'stats': stats_obj,
                    'dailyStats': daily_stats,
                })

        shop_result = {
            'shop_id': sh_shop_id,
            'shop_name': shop_details.get('shop_name'),
            'user_id': shop_details.get('user_id'),
            'created_timestamp': shop_created_ts,
            'created_iso': shop_created_iso,
            'created': shop_created_display,
            'title': shop_details.get('title'),
            'announcement': shop_details.get('announcement'),
            'currency_code': shop_details.get('currency_code'),
            'is_vacation': shop_details.get('is_vacation'),
            'vacation_message': shop_details.get('vacation_message'),
            'sale_message': shop_details.get('sale_message'),
            'digital_sale_message': shop_details.get('digital_sale_message'),
            'updated_timestamp': shop_updated_ts,
            'updated_iso': shop_updated_iso,
            'updated': shop_updated_display,
            'listing_active_count': shop_details.get('listing_active_count'),
            'digital_listing_count': shop_details.get('digital_listing_count'),
            'login_name': shop_details.get('login_name'),
            'accepts_custom_requests': shop_details.get('accepts_custom_requests'),
            'vacation_autoreply': shop_details.get('vacation_autoreply'),
            'url': shop_details.get('url') or shop_obj.get('url'),
            'image_url_760x100': shop_details.get('image_url_760x100'),
            'icon_url_fullxfull': shop_details.get('icon_url_fullxfull'),
            'num_favorers': shop_details.get('num_favorers'),
            'languages': shop_languages,
            'review_average': shop_details.get('review_average'),
            'review_count': shop_details.get('review_count'),
            'sections': shop_sections,
            'reviews': shop_reviews_simplified,
            'shipping_from_country_iso': shop_details.get('shipping_from_country_iso'),
            'transaction_sold_count': shop_details.get('transaction_sold_count'),
        }

        simplified.append({
        'listing_id': listing_id,
        'title': title,
        'url': url,
        'demand': demand,
        'ranking': ranking,
        'made_at': made_at_display,
        'made_at_iso': made_at_iso,
        'made_at_ts': made_at_ts,
        'primary_image': { 'image_url': image_url, 'srcset': srcset },
        'variations': var_variations,
        'user_id': user_id,
        'shop_id': shop_id or sh_shop_id,
        'state': state,
        'description': description,
        'tags': tags,
        'materials': materials,
        'keywords': keywords,
        'sections': shop_sections,
        'reviews': relevant_reviews,
        'review_average': review_average_listing,
        'review_count': review_count_listing,
        'keyword_insights': keyword_insights,
        'demand_extras': (entry.get('demand_extras') or popular.get('demand_extras') or {
            'total_carts': None,
            'quantity': None,
            'estimated_delivery_date': None,
            'free_shipping': None
        }),
        'buyer_promotion_name': buyer_promotion_name,
        'buyer_shop_promotion_name': buyer_shop_promotion_name,
        'buyer_promotion_description': buyer_promotion_description,
        'buyer_applied_promotion_description': buyer_applied_promotion_description,
        'sale_percent': sale_percent,
        'sale_price_value': sale_price_value,
        'sale_price_display': sale_price_display,
        'sale_subtotal_after_discount': sale_subtotal_after_discount,
        'sale_original_price': sale_original_price,
        'price_amount': price_amount,
        'price_divisor': price_divisor,
        'price_currency': price_currency,
        'price_value': price_value,
        'price_display': price_display,
        'last_modified_timestamp': last_modified_ts,
        'last_modified_iso': last_modified_iso,
        'last_modified': last_modified_display,
        'quantity': quantity,
        'num_favorers': num_favorers,
        'listing_type': listing_type,
        'file_data': file_data,
        'views': views,
        'shop': shop_result,
    })
    return JsonResponse({
        'entries_count': len(simplified),
        'entries': simplified,
        'source': 'snapshot' if used_snapshot else 'result_file',
    })
@login_required
def bulk_research_list(request):
    qs = BulkResearchSession.objects.filter(user=request.user).order_by('-created_at')
    data = []
    for s in qs:
        # Always check DB for persisted final results and auto-complete if present
        _ensure_completed_if_result_exists(s)

        # Compute current entries count for confidence about zero/non-zero
        try:
            count = BulkResearchEntry.objects.filter(session_id=s.id).count()
        except Exception:
            count = 0

        data.append({
            'id': s.id,
            'keyword': s.keyword,
            'desired_total': s.desired_total,
            'status': s.status,
            'progress': s.progress or BulkResearchSession.build_initial_progress(s.desired_total),
            'created_at': s.created_at.isoformat(),
            'entries_count': count,
        })
    return JsonResponse({'sessions': data})

def _candidate_start_urls():
    base = UPSTREAM_BASE.rstrip('/')
    return [
        f"{base}/bulk-research/start",
        f"{base}/bulk-research/start/",
        f"{base}/api/bulk-research/start",
        f"{base}/api/bulk-research/start/",
        f"{base}/api/bulk_research/start",
        f"{base}/api/bulk_research/start/",
        f"{base}/bulk_research/start",
        f"{base}/bulk_research/start/",
    ]

def _candidate_start_payloads(user_id: str, keyword: str, desired_total: int):
    return [
        {'user_id': user_id, 'keyword': keyword, 'desired_total': desired_total},
        {'user_id': user_id, 'keyword': keyword, 'total': desired_total},
        {'user_id': user_id, 'keyword': keyword, 'products_count': desired_total},
        {'keyword': keyword, 'desired_total': desired_total},
    ]

def _try_upstream_start(user_id: str, keyword: str, desired_total: int, timeout_sec: int = 20):
    headers = {'Accept': 'application/json'}
    attempts = []
    for url in _candidate_start_urls():
        for payload in _candidate_start_payloads(user_id, keyword, desired_total):
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=timeout_sec)
            except Exception as e:
                attempts.append({'url': url, 'error': str(e)})
                continue
            if 200 <= resp.status_code < 300:
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                job_id = data.get('job_id') or data.get('id') or data.get('session_id') or data.get('jobId')
                if job_id:
                    return {'job_id': str(job_id), 'url': url, 'payload': payload, 'raw': data}
                attempts.append({'url': url, 'status': resp.status_code, 'raw': resp.text[:300]})
            else:
                attempts.append({'url': url, 'status': resp.status_code, 'raw': resp.text[:200]})
    return {'error': 'No upstream start endpoint accepted', 'attempts': attempts}

