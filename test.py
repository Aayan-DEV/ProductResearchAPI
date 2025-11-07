import sys
import json
from datetime import datetime
from pathlib import Path
import requests

API_URL = "https://productresearchapi-production.up.railway.app/run/stream"

STAGE_LABELS = {
    "splitting": "Splitting",
    "split": "Splitting",
    "keywords_split": "Splitting",
    "keyword_splitting": "Splitting",
    "demand_extraction": "Demand Extraction",
    "demand": "Demand Extraction",
    "ai_keywords": "Ai keywords",
    "keywords_ai": "Ai keywords",
    "ai": "Ai keywords",
    "keywords_research": "Keyword analysis",
    "keyword_analysis": "Keyword analysis",
    "analysis": "Keyword analysis",
}

def map_stage(stage: str):
    if not stage:
        return None
    s = stage.lower()
    return STAGE_LABELS.get(s)

def print_progress(progress, force_init=False):
    lines = [
        f"Splitting : {progress['Splitting']['done']}/{progress['Splitting']['total']}",
        f"Demand Extraction : {progress['Demand Extraction']['done']}/{progress['Demand Extraction']['total']}",
        f"Ai keywords : {progress['Ai keywords']['done']}/{progress['Ai keywords']['total']}",
        f"Keyword analysis : {progress['Keyword analysis']['done']}/{progress['Keyword analysis']['total']}",
    ]
    if not getattr(print_progress, "initialized", False) or force_init:
        for line in lines:
            sys.stdout.write(line + "\n")
        sys.stdout.flush()
        print_progress.initialized = True
    else:
        sys.stdout.write("\x1b[4A")
        for line in lines:
            sys.stdout.write("\x1b[2K")
            sys.stdout.write(line + "\n")
        sys.stdout.flush()

def save_final_response(obj, user_id: str, keyword: str):
    if obj is None:
        return None
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe_user = "".join(c for c in user_id if c.isalnum() or c in ("-", "_"))
    safe_keyword = "".join(c for c in keyword if c.isalnum() or c in ("-", "_"))
    filename = f"final_{safe_user}_{safe_keyword}_{ts}.json"
    path = Path.cwd() / filename
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        return path
    except Exception as e:
        sys.stderr.write(f"Failed to write final JSON: {e}\n")
        return None

def stream_run(user_id: str, keyword: str, desired_total: int):
    payload = {"user_id": user_id, "keyword": keyword, "desired_total": desired_total}
    headers = {"Content-Type": "application/json"}

    progress = {
        "Splitting": {"done": 0, "total": 0},
        "Demand Extraction": {"done": 0, "total": 0},
        "Ai keywords": {"done": 0, "total": 0},
        "Keyword analysis": {"done": 0, "total": 0},
    }

    print_progress(progress, force_init=True)
    last_obj = None

    with requests.post(API_URL, json=payload, headers=headers, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        for raw in resp.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            line = raw.strip()
            if not line or line.startswith(":"):
                continue
            if line.startswith("data:"):
                data_str = line[len("data:"):].strip()
                try:
                    obj = json.loads(data_str)
                except Exception:
                    continue

                # keep the latest JSON event to save at the end
                last_obj = obj

                stage = obj.get("stage")
                remaining = obj.get("remaining")
                total = obj.get("total")

                label = map_stage(stage)
                if label and total is not None and remaining is not None:
                    try:
                        t = int(total)
                        r = int(remaining)
                    except Exception:
                        continue
                    done = max(0, t - r)
                    progress[label]["total"] = t
                    progress[label]["done"] = done
                    print_progress(progress)

        print_progress(progress)

    saved_path = save_final_response(last_obj, user_id, keyword)
    if saved_path:
        sys.stdout.write(f"Saved final response to {saved_path}\n")
        sys.stdout.flush()

def main():
    try:
        user_id = input("Enter user_id: ").strip()
        keyword = input("Enter keyword: ").strip()
        desired_total_input = input("Enter desired_total: ").strip()
        desired_total = int(desired_total_input)

        stream_run(user_id, keyword, desired_total)
    except KeyboardInterrupt:
        sys.stdout.write("\n")
        sys.stdout.flush()
    except ValueError:
        sys.stderr.write("desired_total must be an integer.\n")
    except requests.RequestException as e:
        sys.stderr.write(f"Request failed: {e}\n")
    except Exception as e:
        sys.stderr.write(f"Error: {e}\n")

if __name__ == "__main__":
    main()