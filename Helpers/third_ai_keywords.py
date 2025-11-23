#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Verbose batch processor for SEO keywords with dynamic path discovery:
- Defaults to using outputs/popular_listings_full_pdf.json under the project root (auto-discovered).
- Extracts listing titles from payload["popular_results"][i]["title"].
- Prompts for how many products to process (top-to-bottom ordering).
- Automatically finds the first available ".gguf" model under the project root (prefers gemma-3n/gemma3n/gemma).
- No hard-coded absolute model paths; model resolved from the folder containing this script if not provided.

You can override discovery with CLI arguments:
    python third_ai_keywords.py --input outputs/popular_listings_full_pdf.json --model <path_to_model.gguf>
"""

import os
import sys
import json
import time
import re
import traceback
import argparse
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from typing import List

# Generation parameters (tuned for short keyword lists)
GEN_TEMPERATURE = 0.2
GEN_TOP_P = 0.9
GEN_MAX_TOKENS = 128
N_CTX = 4096
N_GPU_LAYERS = 35  # On Apple Silicon (Metal build), this utilizes GPU layers; otherwise ignored
N_THREADS = max(2, os.cpu_count() // 2 if os.cpu_count() else 2)

# Strict keyword list cap
MAX_KEYWORDS = 4

# System prompt designed for concise, seller-style SEO keywords with light detail (2–4 words), no generic fluff.
SEO_SYSTEM_PROMPT = """
Buyer-Style Keyword Phrase Generator for Etsy Product Titles

PURPOSE:
Generate realistic buyer search queries that people actually type into marketplace search bars. The buyer knows the overall thing they want but not all specifics yet. Produce natural, readable phrases — not keyword-stuffed product labels.

OUTPUT REQUIREMENTS:
- Format: Strict JSON array of strings. No markdown, explanations, or extra text.
- Count: Maximum 5 phrases. Aim for exactly 5 when meaningful.
- Order: First 3 = general buyer queries; Last 2 = more specific follow-ups.
- Length per phrase: 2–5 words (occasionally 6 if natural).
- Casing: Use lower-case except proper nouns/abbreviations (e.g., "Google Docs", "SVG").

HOW TO THINK (buyer mindset):
- Start broad: what a buyer would initially type to find options.
- Then refine: add one clear qualifier present in the title after seeing results.
- Keep grammar natural and simple; avoid unnatural Title Case strings or marketing tone.

PHRASE CONSTRUCTION:
General buyer queries (first 3):
- Combine the core subject/theme + simple action/category.
- Avoid format qualifiers ("template", "pattern", "printable", "pdf") unless they are truly the way buyers start.
- Prefer natural word order and plain phrasing.

Specific follow-ups (last 2):
- Add exactly one qualifier found in the title (format, material, technique, audience, size, character).
- Examples of acceptable qualifiers: template, pattern, printable, pdf, svg, digital, crochet, amigurumi, toddler, spiderman, christmas.
- Keep it readable and natural; do not stack multiple qualifiers.

DO:
- Extract nouns and key subjects from the title (e.g., "turkey", "spiderman", "fairy", "resume").
- Convert hyphens, slashes, pipes into spaces; remove duplicates.
- Vary the angles: theme-first, subject-first, action-first.
- Use singular/plural naturally as implied by the title.

DO NOT:
- Invent attributes not present in the title.
- Add fluff or superlatives ("best", "amazing", "ultimate").
- Keyword-stuff or produce unnatural phrases ("crochet doll pdf download") ❌
- Mix languages in the same phrase, add emojis, or special characters.

QUALITY CHECK:
- Would an actual buyer type this?
- Is grammar and casing natural?
- Are the phrases distinct (no near-duplicates)?
- Do the specific follow-ups add a clear qualifier?

INPUT FORMAT:
A single Etsy product title as plain text string.

OUTPUT FORMAT:
Strictly a valid JSON array of strings. Example: ["phrase one","phrase two","phrase three"]

EXAMPLES:

Input: "Disguise a Turkey-Spiderman Template | Thanksgiving Craft Printable | PDF | blank | cut out | Turkey project | Turkey decor | Spidey Turkey"
Output: ["disguise a turkey","disguise a turkey spiderman","spiderman disguise a turkey","disguise a turkey spiderman template","disguise a turkey template"]

Input: "Christmas Fairy Crochet Doll Pattern, Eiralia Amigurumi Doll PDF"
Output: ["christmas fairy crochet","fairy crochet doll","christmas fairy","christmas fairy crochet doll pattern","fairy amigurumi pattern"]

Input: "Vintage Floral Cross Stitch Pattern PDF, Cottage Core Embroidery, Wildflower Design"
Output: ["vintage floral cross stitch","wildflower embroidery","cottagecore embroidery","vintage floral cross stitch pattern","wildflower cross stitch pattern"]

Input: "Modern Minimalist Resume Template, Professional CV Design, Google Docs & Word"
Output: ["minimalist resume","modern cv","professional resume","minimalist resume template","google docs resume template"]
""".strip()

def resolve_model_path_from_env(project_root: Path) -> Optional[Path]:
    """
    Resolve model path from env vars:
      - MODEL_PATH: direct path to .gguf
      - MODEL_DIR: directory to search for .gguf candidates
    """
    p_env = os.getenv("MODEL_PATH")
    if p_env:
        p = Path(p_env)
        if not p.is_absolute():
            p = project_root / p
        try:
            if p.exists() and p.is_file() and p.stat().st_size > 0:
                return p.resolve()
        except Exception:
            pass

    d_env = os.getenv("MODEL_DIR")
    if d_env:
        d = Path(d_env)
        if not d.is_absolute():
            d = project_root / d
        try:
            candidates = []
            for f in d.rglob("*.gguf"):
                if f.stat().st_size > 0:
                    candidates.append(f)
            if candidates:
                candidates.sort(key=lambda x: (_model_score(x.name), -x.stat().st_size))
                return candidates[0].resolve()
        except Exception:
            pass
    return None

def discover_popular_json(outputs_dir: Path) -> Path:
    try:
        candidates: List[Path] = []
        # Support both a run root and its 'outputs' subfolder
        search_bases = [outputs_dir]
        run_outputs = outputs_dir / "outputs"
        if run_outputs.exists():
            search_bases.append(run_outputs)
        for base in search_bases:
            if not base.exists():
                continue
            exact = base / "popular_listings_full.json"
            if exact.exists():
                candidates.append(exact)
            candidates.extend(base.glob("popular_listings_full_*.json"))
        if candidates:
            candidates = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
            return candidates[0]
    except Exception:
        pass
    # Fallback
    return outputs_dir / "popular_listings_full_pdf.json"

def _parse_run_folder_name(name: str) -> Dict[str, Optional[str]]:
    # Pattern: "<keyword>_<timestamp>_<count>"; keyword may contain underscores
    parts = name.split("_")
    if len(parts) >= 3:
        # Count is last; timestamp is second to last
        count = parts[-1]
        ts = parts[-2]
        keyword = "_".join(parts[:-2])
        # Basic validation
        if re.fullmatch(r"\d{8}", ts[:8]) and re.fullmatch(r"\d{6}", ts[9:]) if len(ts) >= 15 else True:
            return {"keyword": keyword, "ts": ts, "count": count}
        return {"keyword": keyword, "ts": ts, "count": count}
    return {"keyword": name, "ts": None, "count": None}

def select_run_and_get_paths(outputs_root: Path) -> Dict[str, Path]:
    if not outputs_root.exists():
        raise FileNotFoundError(f"Outputs root not found: {outputs_root}")

    run_dirs = [p for p in outputs_root.iterdir() if p.is_dir()]
    if not run_dirs:
        raise RuntimeError(f"No run folders found under {outputs_root}")

    run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    print("Available runs:")
    for i, rd in enumerate(run_dirs, start=1):
        info = _parse_run_folder_name(rd.name)
        keyword_display = (info.get("keyword") or rd.name).replace("_", " ")
        ts_display = info.get("ts") or "unknown time"
        count_display = info.get("count") or "unknown"
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
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    popular_json_path = discover_popular_json(run_dir)
    return {
        "run_dir": run_dir,
        "outputs_dir": outputs_dir,
        "popular_json_path": popular_json_path,
    }

def init_run_logging(outputs_dir: Path, base_json_path: Path) -> Path:
    log_name = f"{base_json_path.stem}_keywords.log"
    log_path = outputs_dir / log_name
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        # Touch file to ensure existence
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"[" + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "] [INFO] Log initialized.\n")
        globals()["LOG_FILE_PATH"] = log_path
    except Exception:
        # If logging setup fails, continue without file logging
        pass
    return log_path

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"
    print(line, flush=True)
    # Append to log file if a path has been initialized
    try:
        log_path = globals().get("LOG_FILE_PATH", None)
        if isinstance(log_path, Path):
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        # Never block on logging errors
        pass

def abort(msg: str, code: int = 1) -> None:
    log(msg, level="ERROR")
    sys.exit(code)


def read_json_file(path: str) -> Dict[str, Any]:
    log(f"Step 1: Validating input JSON path: {path}")
    if not os.path.exists(path):
        abort(f"Input file does not exist: {path}")
    if not os.path.isfile(path):
        abort(f"Path is not a file: {path}")
    if os.path.getsize(path) == 0:
        abort(f"Input file is empty: {path}")

    log("Step 2: Reading and parsing JSON ...")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        log("OK: JSON parsed successfully.")
        return data
    except json.JSONDecodeError as e:
        abort(f"Failed to parse JSON (line {e.lineno} col {e.colno}): {e.msg}")
    except Exception as e:
        abort(f"Unexpected error reading JSON: {e}")


def _recursive_collect_titles(obj: Any, titles: List[str]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() == "title" and isinstance(v, str) and v.strip():
                titles.append(v.strip())
            else:
                _recursive_collect_titles(v, titles)
    elif isinstance(obj, list):
        for item in obj:
            _recursive_collect_titles(item, titles)


def extract_titles(payload: Dict[str, Any]) -> List[str]:
    log("Step 3: Extracting product titles ...")

    titles: List[str] = []

    # Primary schema path for this dataset: payload["popular_results"][i]["title"]
    if isinstance(payload, dict) and isinstance(payload.get("popular_results"), list):
        for i, item in enumerate(payload["popular_results"]):
            title = item.get("title") if isinstance(item, dict) else None
            if isinstance(title, str) and title.strip():
                titles.append(title.strip())

    # Fallback: scan entire payload for any 'title' keys
    if not titles:
        log("WARN: 'popular_results' not found or empty. Falling back to recursive scan.", level="WARN")
        _recursive_collect_titles(payload, titles)

    # De-duplicate while preserving order
    seen = set()
    deduped: List[str] = []
    for t in titles:
        key = t.strip()
        if key and key not in seen:
            seen.add(key)
            deduped.append(key)

    if not deduped:
        abort("No product titles found in the input JSON.")

    log(f"OK: Extracted {len(deduped)} unique titles.")
    sample = deduped[:3]
    for idx, s in enumerate(sample, 1):
        log(f"  Sample {idx}: {s}")
    if len(deduped) > 3:
        log("  ...")

    return deduped


def discover_project_root() -> Path:
    # Use the folder containing this script as the project root
    try:
        return Path(__file__).resolve().parent.parent
    except Exception:
        # Fallback to current working directory
        return Path.cwd()


def discover_input_json(project_root: Path) -> Optional[Path]:
    """
    Prefer the outputs/popular_listings_full_pdf.json under the project root.
    Then fallback to project_root / 'popular_listings_full_pdf.json'.
    As a last resort, try a recursive scan for any JSON with 'popular_results'.
    """
    # 1) Preferred path: outputs/popular_listings_full_pdf.json
    try:
        outputs_path = project_root / "data" / "outputs" / "popular_listings_full_pdf.json"
        if outputs_path.exists() and outputs_path.is_file() and outputs_path.stat().st_size > 0:
            return outputs_path.resolve()
    except Exception:
        pass

    # 2) Project-local fallback (if someone placed it at the root)
    try:
        local = project_root / "popular_listings_full_pdf.json"
        if local.exists() and local.is_file() and local.stat().st_size > 0:
            return local.resolve()
    except Exception:
        pass

    # 3) Last resort: pick any JSON under outputs/ that appears to contain 'popular_results'
    candidates: List[Path] = []
    try:
        outputs_dir = project_root / "data" / "outputs"
        if outputs_dir.exists():
            for p in outputs_dir.rglob("*.json"):
                try:
                    if p.stat().st_size > 0:
                        candidates.append(p)
                except Exception:
                    continue
    except Exception:
        pass

    for p in sorted(candidates, key=lambda x: -x.stat().st_size):
        try:
            with open(p, "r", encoding="utf-8") as f:
                head = f.read(20000)
            if "\"popular_results\"" in head:
                return p.resolve()
        except Exception:
            continue

    return None


def _model_score(name: str) -> int:
    """
    Lower score is better.
    Prefer gemma-3n/gemma3n, then gemma3/gemma, then olmo, then others.
    """
    n = name.lower()
    if "gemma-3n" in n or "gemma3n" in n:
        return 0
    if "gemma3" in n or "gemma" in n:
        return 1
    if "olmo" in n:
        return 2
    return 5


def discover_model_path(project_root: Path) -> Optional[Path]:
    """
    Recursively search for .gguf files under project_root.
    Choose by preferred family first, then by larger file size (proxy for capacity).
    """
    ggufs: List[Path] = []
    for root, _dirs, files in os.walk(project_root):
        for f in files:
            if f.lower().endswith(".gguf"):
                p = Path(root) / f
                # Ensure non-empty file
                try:
                    if p.stat().st_size > 0:
                        ggufs.append(p)
                except Exception:
                    pass

    if not ggufs:
        return None

    ggufs.sort(
        key=lambda p: (_model_score(p.name), -p.stat().st_size)
    )
    return ggufs[0]


def load_model(model_path: str):
    log("Step 5: Importing llama-cpp-python ...")
    try:
        from llama_cpp import Llama  # type: ignore
    except Exception as e:
        abort(
            "Failed to import llama-cpp-python. Ensure it is installed and compatible with your Python version.\n"
            f"Import error: {e}"
        )

    log(f"Step 6: Loading model into memory (this may take a moment) ...")
    t0 = time.time()
    try:
        llm = Llama(
            model_path=model_path,
            n_ctx=N_CTX,
            n_gpu_layers=N_GPU_LAYERS,
            n_threads=N_THREADS,
            logits_all=False,
            verbose=False,  # suppress low-level C logs; we print our own progress
        )
        dt = time.time() - t0
        log(f"OK: Model loaded in {dt:.2f}s (n_ctx={N_CTX}, n_gpu_layers={N_GPU_LAYERS}, n_threads={N_THREADS}).")
        return llm
    except Exception as e:
        tb = traceback.format_exc()
        abort(f"Failed to load model: {e}\nTraceback:\n{tb}")
    return None  # Unreachable


def _extract_json_array(text: str) -> Optional[str]:
    # Try to extract the first top-level JSON array from the text
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        return text[start : end + 1]
    return None


def _normalize_keyword(kw: str) -> str:
    s = kw.strip()
    # Collapse multiple spaces
    s = re.sub(r"\s+", " ", s)
    # Remove wrapping quotes if any
    s = s.strip("\"'“”‘’")
    # Disallow trailing commas or periods
    s = s.rstrip(",.")
    return s


def parse_keywords_from_response(raw: str) -> List[str]:
    raw = raw.strip()
    # Direct parse
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            items = [x for x in data if isinstance(x, str)]
            return [_normalize_keyword(x) for x in items if _normalize_keyword(x)]
    except Exception:
        pass

    # Extract array and parse
    arr = _extract_json_array(raw)
    if arr:
        try:
            data = json.loads(arr)
            if isinstance(data, list):
                items = [x for x in data if isinstance(x, str)]
                return [_normalize_keyword(x) for x in items if _normalize_keyword(x)]
        except Exception:
            pass

    # Fallback: naive comma/line split (last resort)
    parts = re.split(r"[\n,]+", raw)
    items = [_normalize_keyword(x) for x in parts if _normalize_keyword(x)]
    # Heuristic: if this produced too many non-sense parts, return empty to signal failure
    if len(items) >= 2:
        return items
    return []

def extract_titles_from_run_summary(payload: Dict[str, Any]) -> List[str]:
    log("Step 3: Extracting product titles from demand run_summary ...")

    titles: List[str] = []
    entries = payload.get("entries", [])

    if isinstance(entries, list):
        for e in entries:
            pop = e.get("popular_info")
            t = pop.get("title") if isinstance(pop, dict) else None
            if isinstance(t, str) and t.strip():
                titles.append(t.strip())

    # De-duplicate while preserving order
    seen = set()
    deduped: List[str] = []
    for t in titles:
        key = t.strip()
        if key and key not in seen:
            seen.add(key)
            deduped.append(key)

    if not deduped:
        abort("No product titles found in the run_summary 'entries'.")

    log(f"OK: Extracted {len(deduped)} titles from demand summary.")
    sample = deduped[:3]
    for idx, s in enumerate(sample, 1):
        log(f"  Sample {idx}: {s}")
    if len(deduped) > 3:
        log("  ...")

    return deduped

def dedup_and_cap_keywords(keywords: List[str], cap: int = MAX_KEYWORDS) -> List[str]:
    seen_ci = set()
    result: List[str] = []
    for kw in keywords:
        key = kw.lower()
        if key not in seen_ci and kw:
            seen_ci.add(key)
            result.append(kw)
        if len(result) >= cap:
            break
    return result


def generate_keywords_for_title(llm, title: str) -> Tuple[List[str], str]:
    """
    Returns (keywords, raw_model_output)
    """
    sem = globals().get("_LLM_INFER_SEMAPHORE")
    if sem is None:
        import threading, os
        sem = threading.Semaphore(int(os.getenv("AI_CONCURRENCY", "1")))
        globals()["_LLM_INFER_SEMAPHORE"] = sem

    user_prompt = (
        f"Product title:\n{title}\n\n"
        f"Return ONLY a JSON array of 3–8 concise, non-generic, commercial-intent keyword phrases (2–4 words)."
    )

    try:
        with sem:
            # Prefer chat completion if available
            has_chat = hasattr(llm, "create_chat_completion")
            if has_chat:
                resp = llm.create_chat_completion(
                    messages=[
                        {"role": "system", "content": SEO_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=GEN_TEMPERATURE,
                    top_p=GEN_TOP_P,
                    max_tokens=GEN_MAX_TOKENS,
                )
                content = resp["choices"][0]["message"]["content"]
            else:
                # Fallback to plain completion
                prompt = (
                    f"[SYSTEM]\n{SEO_SYSTEM_PROMPT}\n\n"
                    f"[USER]\n{user_prompt}\n\n"
                    f"[ASSISTANT]\n"
                )
                resp = llm.create_completion(
                    prompt=prompt,
                    temperature=GEN_TEMPERATURE,
                    top_p=GEN_TOP_P,
                    max_tokens=GEN_MAX_TOKENS,
                    stop=["[USER]", "[SYSTEM]"],
                )
                content = resp["choices"][0]["text"]

        parsed = parse_keywords_from_response(content)
        cleaned = dedup_and_cap_keywords(parsed, cap=MAX_KEYWORDS)

        return cleaned, content
    except Exception as e:
        tb = traceback.format_exc()
        log(f"Generation error for title: {title}\n{e}\n{tb}", level="ERROR")
        return [], ""


def write_output(
    input_path: Path,
    model_path: Path,
    titles: List[str],
    results: List[Dict[str, Any]],
) -> Path:
    out_path = input_path.parent / f"{input_path.stem}_keywords.json"
    payload = {
        "meta": {
            "input_file": str(input_path),
            "model_path": str(model_path),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "title_count": len(titles),
            "max_keywords_per_title": MAX_KEYWORDS,
            "params": {
                "temperature": GEN_TEMPERATURE,
                "top_p": GEN_TOP_P,
                "max_tokens": GEN_MAX_TOKENS,
                "n_ctx": N_CTX,
                "n_gpu_layers": N_GPU_LAYERS,
                "n_threads": N_THREADS,
            },
        },
        "results": results,
    }
    log(f"Step 9: Writing output JSON: {out_path}")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log("OK: Output saved.")
    except Exception as e:
        abort(f"Failed to write output JSON: {e}")
    return out_path

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SEO Keyword Batch Processor (Popular Listings JSON, auto-discovery)")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Input JSON file path (absolute or relative). If omitted, defaults to outputs/popular_listings_full_pdf.json under the project root if present.",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="GGUF model file path (relative to project root or absolute). If omitted, auto-discover a '.gguf' model under the project root.",
    )
    parser.add_argument(
        "--demand_summary",
        type=str,
        default=None,
        help="Demand run_summary JSON path. If omitted, auto-discover under project_root/outputs/*/demand extracted/.",
    )
    parser.add_argument(
        "--max_keywords",
        type=int,
        default=MAX_KEYWORDS,
        help=f"Maximum number of keywords per title (default: {MAX_KEYWORDS})",
    )
    return parser.parse_args()

def find_demand_summary(run_dir: Path) -> Optional[Path]:
    demand_dir = run_dir / "demand extracted"
    if not demand_dir.exists():
        return None
    candidates = [
        p for p in demand_dir.glob("run_summary_*.json")
        if p.is_file() and not p.name.endswith("_keywords.json")
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def discover_demand_summary(project_root: Path) -> Optional[Path]:
    outputs_root = project_root / "outputs"
    # Try interactive run selection if possible
    try:
        sel = select_run_and_get_paths(outputs_root)
        run_dir = sel["run_dir"]
        p = find_demand_summary(run_dir)
        if p:
            return p
    except Exception:
        pass

    # Fallback: scan all runs under outputs
    candidates: List[Path] = []
    if outputs_root.exists():
        for run_dir in outputs_root.iterdir():
            if not run_dir.is_dir():
                continue
            p = find_demand_summary(run_dir)
            if p:
                candidates.append(p)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)

def resolve_path(project_root: Path, maybe_path: Optional[str]) -> Optional[Path]:
    if not maybe_path:
        return None
    p = Path(maybe_path)
    if not p.is_absolute():
        p = project_root / p
    return p.resolve()


def prompt_for_limit(total: int) -> int:
    """
    Ask the user how many products to process (from top to bottom).
    Returns the validated count (1..total). Defaults to 'total' on invalid/empty input.
    """
    if total <= 0:
        return 0
    prompt = f"How many products should I process (1..{total})? Press Enter for all: "
    try:
        raw = input(prompt).strip()
    except EOFError:
        raw = ""
    if not raw:
        return total
    try:
        n = int(raw)
        if n < 1:
            log("WARN: Count must be at least 1. Using all.", level="WARN")
            return total
        if n > total:
            log(f"WARN: Count exceeds total ({total}). Clamping to {total}.", level="WARN")
            return total
        return n
    except Exception:
        log("WARN: Invalid number. Using all.", level="WARN")
        return total


def main() -> None:
    log("=== SEO Keyword Batch Processor (Demand Summary) ===")
    log(f"Working directory: {os.getcwd()}")
    log(f"Python: {sys.version.split()[0]} | Platform: {sys.platform}")

    args = parse_args()
    project_root = discover_project_root()
    log(f"Project root: {project_root}")

    # Resolve demand run_summary path (CLI override or auto-discover)
    demand_path = resolve_path(project_root, getattr(args, "demand_summary", None))
    if demand_path is None:
        demand_path = discover_demand_summary(project_root)

    if demand_path is None or not demand_path.exists():
        abort(
            "Demand run summary not found under project outputs. "
            "Pass --demand_summary or ensure outputs/*/demand extracted/run_summary_*.json exists."
        )

    # Outputs next to the demand summary; run dir is its parent
    outputs_dir: Path = demand_path.parent
    run_dir: Path = outputs_dir.parent
    init_run_logging(outputs_dir, demand_path)

    log(f"Using demand run_summary JSON: {demand_path}")

    # Resolve model path (CLI override or auto-discover)
    env_model_path = resolve_model_path_from_env(project_root)
    model_path = env_model_path or resolve_path(project_root, getattr(args, "model", None))
    if model_path is None:
        model_path = discover_model_path(project_root)
        if model_path is None:
            abort(
                "Could not auto-discover a GGUF model under the project root. "
                "Place a '.gguf' file in the project and/or pass --model."
            )
        log(f"Auto-discovered GGUF model: {model_path}")
    else:
        log(f"Using GGUF model: {model_path}")

    # Apply keyword cap from CLI
    global MAX_KEYWORDS
    if isinstance(args.max_keywords, int) and args.max_keywords > 0:
        MAX_KEYWORDS = args.max_keywords

    # 1) Read demand run summary and extract product titles
    data = read_json_file(str(demand_path))
    titles = extract_titles_from_run_summary(data)
    total_titles = len(titles)
    log(f"Detected {total_titles} product titles in demand run_summary.")

    # 2) Ask how many products to process (top-to-bottom)
    count = prompt_for_limit(total_titles)
    titles = titles[:count]
    log(f"Processing {len(titles)} of {total_titles} titles (top-to-bottom).")

    # 3) Load model
    llm = load_model(str(model_path))

    # 4) Generate keywords per title
    log("Step 7: Generating keywords for each title (sequential) ...")
    results: List[Dict[str, Any]] = []
    start_time = time.time()

    for idx, title in enumerate(titles, start=1):
        log(f"[{idx}/{len(titles)}] Title: {title}")
        t0 = time.time()
        keywords, raw = generate_keywords_for_title(llm, title)
        dt = time.time() - t0

        if not keywords:
            log("WARN: No keywords parsed. Recording empty list.", level="WARN")
        else:
            log(f"OK: {len(keywords)} keywords parsed in {dt:.2f}s: {keywords}")

        results.append(
            {
                "title": title,
                "keywords": keywords,
                "raw_model_output": raw,
            }
        )

    total_dt = time.time() - start_time
    log(f"Step 8: Completed generation for {len(titles)} titles in {total_dt:.2f}s.")

    # 5) Write output JSON next to the demand run summary
    out_path = write_output(demand_path, model_path, titles, results)

    # 6) Summary
    produced = sum(1 for r in results if r.get("keywords"))
    missing = len(results) - produced
    log("=== Summary ===")
    log(f"Titles processed: {len(titles)}")
    log(f"With keywords: {produced}")
    log(f"Empty/failed: {missing}")
    log(f"Output file: {out_path}")
    log("Done.")

# Singleton LLM for fast reuse in API-style calls
_LLM_SINGLETON = None

def ensure_llm_loaded(project_root: Optional[Path] = None):
    """
    Resolve and load a llama-cpp .gguf model once; reuse across calls.
    """
    global _LLM_SINGLETON
    if _LLM_SINGLETON is not None:
        return _LLM_SINGLETON

    # Initialize a module-global lock exactly once
    lock = globals().get("_LLM_LOCK")
    if lock is None:
        import threading
        lock = threading.Lock()
        globals()["_LLM_LOCK"] = lock

    with lock:
        if _LLM_SINGLETON is not None:
            return _LLM_SINGLETON
        try:
            pr = project_root or discover_project_root()
        except Exception:
            pr = Path.cwd()

        env_model = resolve_model_path_from_env(pr)
        model_p = env_model or discover_model_path(pr)
        if not model_p:
            raise RuntimeError("No .gguf model found under project root or MODEL_DIR for AI keywords.")
        _LLM_SINGLETON = load_model(str(model_p))
        return _LLM_SINGLETON

def generate_keywords_for_title_api(title: str) -> List[str]:
    """
    Convenience API to produce keywords for a single product title quickly.
    """
    llm = ensure_llm_loaded()
    kws, _raw = generate_keywords_for_title(llm, title)
    return dedup_and_cap_keywords(kws, cap=MAX_KEYWORDS)


if __name__ == "__main__":
    main()