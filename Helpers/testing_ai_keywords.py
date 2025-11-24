# /Users/aayan/Development/API/AI_KEYWORDS_API/Helpers/testing_ai_keywords.py
import os
import sys
import json
import time
import re
import argparse
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import requests

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
GEN_TEMPERATURE = 0.2
GEN_TOP_P = 0.9
GEN_MAX_TOKENS = int(os.getenv("GEN_MAX_TOKENS", "128"))
MAX_KEYWORDS = int(os.getenv("MAX_KEYWORDS", "4"))

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

def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)

def abort(msg: str, code: int = 1) -> None:
    log(msg, level="ERROR")
    sys.exit(code)

def discover_project_root() -> Path:
    try:
        return Path(__file__).resolve().parent.parent
    except Exception:
        return Path.cwd()

def _extract_json_array(text: str) -> Optional[str]:
    s = text.find("[")
    e = text.rfind("]")
    if s != -1 and e != -1 and e > s:
        return text[s:e + 1]
    return None

def parse_keywords_from_response(raw: str) -> List[str]:
    raw = (raw or "").strip()
    arr = _extract_json_array(raw)
    if arr:
        try:
            data = json.loads(arr)
            out = []
            for x in data:
                if isinstance(x, str):
                    k = x.strip().strip("\"'“”‘’").rstrip(",.")
                    k = re.sub(r"\s+", " ", k)
                    if k:
                        out.append(k)
            return out
        except Exception:
            pass
    parts = []
    for line in raw.splitlines():
        t = line.strip().strip("-•*").strip()
        t = t.strip("\"'“”‘’").rstrip(",.")
        t = re.sub(r"\s+", " ", t)
        if t and len(t.split()) <= 8 and "[" not in t and "]" not in t:
            parts.append(t)
    return parts

def dedup_and_cap_keywords(keywords: List[str], cap: int = MAX_KEYWORDS) -> List[str]:
    seen = set()
    out: List[str] = []
    for kw in keywords:
        k = kw.lower()
        if k and k not in seen:
            seen.add(k)
            out.append(kw)
        if len(out) >= cap:
            break
    return out

def find_demand_summary(run_dir: Path) -> Optional[Path]:
    d = run_dir / "demand extracted"
    if not d.exists():
        return None
    candidates = [p for p in d.glob("run_summary_*.json") if p.is_file() and not p.name.endswith("_keywords.json")]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]

def discover_demand_summary(project_root: Path) -> Optional[Path]:
    outputs_root = project_root / "outputs"
    try:
        run_dirs = [p for p in outputs_root.iterdir() if p.is_dir()]
        run_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        for rd in run_dirs:
            p = find_demand_summary(rd)
            if p:
                return p
    except Exception:
        pass
    return None

def read_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def extract_titles_from_run_summary(payload: Dict[str, Any]) -> List[str]:
    titles: List[str] = []
    entries = payload.get("entries", [])
    if isinstance(entries, list):
        for e in entries:
            pop = e.get("popular_info")
            t = pop.get("title") if isinstance(pop, dict) else None
            if isinstance(t, str) and t.strip():
                titles.append(t.strip())
    seen = set()
    deduped: List[str] = []
    for t in titles:
        k = t.strip()
        if k and k not in seen:
            seen.add(k)
            deduped.append(k)
    if not deduped:
        abort("No product titles found in the run_summary 'entries'.")
    return deduped

def prompt_for_limit(total: int) -> int:
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
            return total
        if n > total:
            return total
        return n
    except Exception:
        return total

def ensure_llm_loaded(project_root: Optional[Path] = None):
    api_key = (os.getenv("GOOGLE_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("GOOGLE_API_KEY is missing")
    model_name = (os.getenv("GOOGLE_MODEL_NAME") or "gemini-2.5-flash-lite-preview-09-2025").strip()
    endpoint = (os.getenv("GOOGLE_GENAI_ENDPOINT") or "https://generativelanguage.googleapis.com/v1beta").strip()
    sess = requests.Session()
    return {"api_key": api_key, "model_name": model_name, "endpoint": endpoint, "session": sess}

def generate_keywords_for_title(llm, title: str) -> Tuple[List[str], str]:
    user_prompt = (
        f"Product title:\n{title}\n\n"
        f"Return ONLY a JSON array of 3–8 concise, non-generic, commercial-intent keyword phrases (2–4 words)."
    )
    try:
        url = f"{llm['endpoint']}/models/{llm['model_name']}:generateContent?key={llm['api_key']}"
        body = {
            "systemInstruction": {"parts": [{"text": SEO_SYSTEM_PROMPT}]},
            "contents": [{"role": "user", "parts": [{"text": user_prompt}]}],
            "generationConfig": {
                "temperature": GEN_TEMPERATURE,
                "topP": GEN_TOP_P,
                "maxOutputTokens": GEN_MAX_TOKENS,
            },
        }
        r = llm["session"].post(url, json=body, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        resp = r.json()
        text = ""
        try:
            cands = resp.get("candidates") or []
            if cands:
                content = cands[0].get("content") or {}
                parts = content.get("parts") or []
                texts = []
                for p in parts:
                    t = p.get("text")
                    if isinstance(t, str):
                        texts.append(t)
                text = "\n".join(texts).strip()
        except Exception:
            text = json.dumps(resp, ensure_ascii=False)
        parsed = parse_keywords_from_response(text)
        cleaned = dedup_and_cap_keywords(parsed, cap=MAX_KEYWORDS)
        return cleaned, text
    except Exception as e:
        tb = traceback.format_exc()
        log(f"Generation error for title: {title}\n{e}\n{tb}", level="ERROR")
        return [], ""

def write_output(input_path: Path, model_name: str, titles: List[str], results: List[Dict[str, Any]]) -> Path:
    out_path = input_path.parent / f"{input_path.stem}_keywords.json"
    payload = {
        "meta": {
            "input_file": str(input_path),
            "model_name": str(model_name),
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "title_count": len(titles),
            "max_keywords_per_title": MAX_KEYWORDS,
            "params": {
                "temperature": GEN_TEMPERATURE,
                "top_p": GEN_TOP_P,
                "max_tokens": GEN_MAX_TOKENS,
            },
        },
        "results": results,
    }
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SEO Keyword Batch Processor via Google API (Demand Summary)")
    p.add_argument("--demand_summary", type=str, default=None, help="Demand run_summary JSON path")
    p.add_argument("--max_keywords", type=int, default=MAX_KEYWORDS, help=f"Maximum keywords per title (default: {MAX_KEYWORDS})")
    p.add_argument("--model_name", type=str, default=os.getenv("GOOGLE_MODEL_NAME") or "gemini-2.5-flash-lite-preview-09-2025", help="Google model name")
    return p.parse_args()

def main() -> None:
    log("=== SEO Keyword Batch Processor (Google API) ===")
    log(f"Working directory: {os.getcwd()}")
    log(f"Python: {sys.version.split()[0]} | Platform: {sys.platform}")
    project_root = discover_project_root()
    log(f"Project root: {project_root}")
    args = parse_args()
    demand_path = Path(args.demand_summary) if args.demand_summary else discover_demand_summary(project_root)
    if not demand_path or not demand_path.exists():
        abort("Demand run_summary JSON not found under outputs/*/demand extracted/")
    log(f"Using demand run_summary JSON: {demand_path}")
    data = read_json(demand_path)
    titles = extract_titles_from_run_summary(data)
    total_titles = len(titles)
    log(f"Detected {total_titles} product titles in demand run_summary.")
    count = prompt_for_limit(total_titles)
    titles = titles[:count]
    log(f"Processing {len(titles)} of {total_titles} titles (top-to-bottom).")
    llm = ensure_llm_loaded(project_root)
    log(f"Model: {llm['model_name']}")
    log("Generating keywords for each title ...")
    results: List[Dict[str, Any]] = []
    start_time = time.time()
    for idx, title in enumerate(titles, start=1):
        log(f"[{idx}/{len(titles)}] Title: {title}")
        t0 = time.time()
        keywords, raw = generate_keywords_for_title(llm, title)
        dt = time.time() - t0
        if not keywords:
            log("No keywords parsed", level="WARN")
        else:
            log(f"{len(keywords)} keywords parsed in {dt:.2f}s: {keywords}")
        results.append({"title": title, "keywords": keywords, "raw_model_output": raw})
    total_dt = time.time() - start_time
    log(f"Completed generation for {len(titles)} titles in {total_dt:.2f}s.")
    out_path = write_output(demand_path, llm["model_name"], titles, results)
    produced = sum(1 for r in results if r.get("keywords"))
    missing = len(results) - produced
    log("=== Summary ===")
    log(f"Titles processed: {len(titles)}")
    log(f"With keywords: {produced}")
    log(f"Empty/failed: {missing}")
    log(f"Output file: {out_path}")
    log("Done.")

_LLM_SINGLETON = None

def ensure_llm_loaded_singleton(project_root: Optional[Path] = None):
    global _LLM_SINGLETON
    if _LLM_SINGLETON is None:
        _LLM_SINGLETON = ensure_llm_loaded(project_root)
    return _LLM_SINGLETON

def generate_keywords_for_title_api(title: str) -> List[str]:
    llm = ensure_llm_loaded_singleton()
    kws, _raw = generate_keywords_for_title(llm, title)
    return dedup_and_cap_keywords(kws, cap=MAX_KEYWORDS)

if __name__ == "__main__":
    main()