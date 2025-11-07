import os
import mimetypes
import pathlib
import requests
from typing import Optional, Dict, List
import shutil

BUCKET_NAME = os.getenv("SUPABASE_BUCKET_NAME") or "Product_Research_Outputs"

def get_bucket_name() -> str:
    return os.getenv("SUPABASE_BUCKET_NAME", "Product_Research_Outputs")

def _auth_header() -> Dict[str, str]:
    url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip() or os.getenv("SUPABASE_ANON_KEY", "").strip())
    if not url or not key:
        raise RuntimeError("Supabase URL or Key not configured in environment")
    return {"Authorization": f"Bearer {key}"}

def _base_url() -> str:
    url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    if not url:
        raise RuntimeError("Supabase URL not configured")
    return f"{url}/storage/v1"

def ensure_bucket(bucket_name: Optional[str] = None, public: bool = True) -> Dict:
    bucket = bucket_name or get_bucket_name()
    base = _base_url()
    hdr = _auth_header()
    r = requests.get(f"{base}/bucket/{bucket}", headers=hdr, timeout=15)
    if r.status_code == 200:
        existing = r.json()
        if bool(existing.get("public")) != bool(public):
            requests.put(
                f"{base}/bucket/{bucket}",
                headers={**hdr, "Content-Type": "application/json"},
                json={"public": public},
                timeout=15,
            )
        return existing
    if r.status_code == 404:
        cr = requests.post(
            f"{base}/bucket",
            headers={**hdr, "Content-Type": "application/json"},
            json={"name": bucket, "public": public},
            timeout=15,
        )
        cr.raise_for_status()
        return cr.json()
    r.raise_for_status()
    return {}

def public_url(bucket: str, object_path: str) -> str:
    url = os.getenv("SUPABASE_URL", "").strip().rstrip("/")
    return f"{url}/storage/v1/object/public/{bucket}/{object_path.lstrip('/')}"

def upload_bytes(
    bucket: Optional[str],
    object_path: str,
    data: bytes,
    content_type: Optional[str] = None,
    upsert: bool = True,
) -> Dict:
    bkt = bucket or get_bucket_name()
    base = _base_url()
    hdr = _auth_header()
    hdr["Content-Type"] = content_type or "application/octet-stream"
    if upsert:
        hdr["x-upsert"] = "true"
    url = f"{base}/object/{bkt}/{object_path.lstrip('/')}"
    resp = requests.post(url, headers=hdr, data=data, timeout=60)
    ok = 200 <= resp.status_code < 300
    return {
        "status_code": resp.status_code,
        "ok": ok,
        "path": object_path,
        "public_url": public_url(bkt, object_path),
        "error": None if ok else (resp.text[:500] if resp.text else "upload error"),
    }

def upload_file(
    bucket: Optional[str],
    src_path: str,
    dest_path: Optional[str] = None,
    content_type: Optional[str] = None,
    upsert: bool = True,
) -> Dict:
    src = pathlib.Path(src_path)
    if dest_path is None:
        dest_path = src.name
    if content_type is None:
        ct, _ = mimetypes.guess_type(src.name)
        content_type = ct or "application/octet-stream"
    data = src.read_bytes()
    return upload_bytes(bucket, dest_path, data, content_type=content_type, upsert=upsert)

def upload_directory(
    bucket_name: Optional[str],
    src_dir: str | os.PathLike,
    dest_prefix: str,
    include_extensions: tuple[str, ...] = (".json", ".jsonl", ".txt", ".html", ".csv", ".png", ".jpg", ".jpeg", ".gif", ".webp"),
    exclude_dirs: tuple[str, ...] = ("__pycache__", ".git", ".venv", "node_modules", "models"),
    public: bool = True,
    upsert: bool = True,
) -> Dict:
    bkt = bucket_name or get_bucket_name()
    root = pathlib.Path(src_dir)
    ensure_bucket(bkt, public=public)

    uploaded: List[Dict] = []
    errors: List[Dict] = []

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        parent_names = {p.name for p in path.parents}
        if any(d in parent_names for d in exclude_dirs):
            continue
        if include_extensions and path.suffix.lower() not in include_extensions:
            continue

        rel = path.relative_to(root).as_posix()
        obj_path = "/".join([dest_prefix.strip("/"), rel]).strip("/")

        try:
            res = upload_file(bkt, str(path), dest_path=obj_path, upsert=upsert)
            if res.get("ok"):
                uploaded.append(res)
            else:
                errors.append(res)
        except Exception as e:
            errors.append({"path": str(path), "error": str(e)})

    return {
        "uploaded_count": len(uploaded),
        "errors_count": len(errors),
        "uploaded": uploaded,
        "errors": errors,
    }

def upload_cart_runs_and_cleanup(
    run_dir: str | os.PathLike,
    dest_prefix: Optional[str] = None,
    bucket_name: Optional[str] = None,
    public: bool = True,
    upsert: bool = True,
) -> Dict:
    """
    Upload the tmp 'cart_runs' subtree for a given run, then delete it locally.
    - Reads from '/tmp/ai_keywords_cart_runs/{basename(run_dir)}'
    - Uploads to 'cart_runs/{basename(run_dir)}' in Supabase (by default)
    """
    run_dir_name = pathlib.Path(run_dir).name
    src_dir = pathlib.Path("/tmp/ai_keywords_cart_runs") / run_dir_name
    if not src_dir.exists():
      return {
          "uploaded_count": 0,
          "errors_count": 0,
          "uploaded": [],
          "errors": [],
          "skipped": True,
          "reason": "tmp cart_runs not found",
          "src_dir": src_dir.as_posix(),
      }

    dest = dest_prefix or f"cart_runs/{run_dir_name}"
    ensure_bucket(bucket_name or get_bucket_name(), public=public)
    res = upload_directory(bucket_name, str(src_dir), dest_prefix=dest, public=public, upsert=upsert)

    try:
        shutil.rmtree(src_dir)
    except Exception as e:
        res["cleanup_error"] = str(e)

    res["src_dir"] = src_dir.as_posix()
    res["dest_prefix"] = dest
    return res

def upload_tmp_cart_runs_and_cleanup(
    run_dir_name: str,
    dest_prefix: Optional[str] = None,
    bucket_name: Optional[str] = None,
    public: bool = True,
    upsert: bool = True,
) -> Dict:
    src_root = pathlib.Path("/tmp/ai_keywords_cart_runs") / run_dir_name
    if not src_root.exists():
        return {
            "uploaded_count": 0,
            "errors_count": 0,
            "uploaded": [],
            "errors": [],
            "skipped": True,
            "reason": "tmp cart_runs not found",
            "src_dir": src_root.as_posix(),
        }

    dest = dest_prefix or f"cart_runs/{run_dir_name}"
    ensure_bucket(bucket_name or get_bucket_name(), public=public)
    res = upload_directory(bucket_name, str(src_root), dest_prefix=dest, public=public, upsert=upsert)

    try:
        shutil.rmtree(src_root)
    except Exception as e:
        res["cleanup_error"] = str(e)

    res["src_dir"] = src_root.as_posix()
    res["dest_prefix"] = dest
    return res