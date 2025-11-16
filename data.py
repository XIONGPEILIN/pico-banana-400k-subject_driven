import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime

import requests
from tqdm import tqdm

path = "openimages/jsonl/sft_with_local_source_image_path.jsonl"
data_all = []
with open(path, 'r', encoding='utf-8') as f:
    for line in f:
        data_all.append(json.loads(line))

MODEL_NAME = "Qwen/Qwen3-30B-A3B-Instruct-2507-FP8"
SERVER_URL = os.environ.get("SGLANG_SERVER_URL", "http://127.0.0.1:12345/v1/chat/completions")
API_KEY = os.environ.get("SGLANG_API_KEY", "EMPTY")
MAX_NEW_TOKENS = int(os.environ.get("LLM_MAX_NEW_TOKENS", "1024"))
DEFAULT_MAX_WORKERS = 32
MAX_WORKERS = int(os.environ.get("LLM_MAX_WORKERS", str(DEFAULT_MAX_WORKERS)))
REQUEST_TIMEOUT = int(os.environ.get("LLM_REQUEST_TIMEOUT", "120"))
MAX_RETRIES = int(os.environ.get("LLM_MAX_RETRIES", "3"))
RETRY_BACKOFF = float(os.environ.get("LLM_RETRY_BACKOFF", "2.0"))
ERROR_LOG_PATH = os.environ.get("LLM_ERROR_LOG", "analysis_errors.json")

OBJECT_ADD_KEYWORDS = ("add a new object", "add an object")
OBJECT_REMOVE_KEYWORDS = ("remove an existing object", "remove an object")

_error_records = []
_error_lock = threading.Lock()

# Ensure local requests bypass proxies so we always hit the on-host server directly.
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")
_REQUEST_PROXIES = {"http": None, "https": None}


def _build_prompt(text_description: str, summary_description: str) -> str:
    return f"""
You are an expert vision editor. Analyze the following image editing instruction.
- Determine if the primary action is to \"add\" or \"remove\" a single, concrete physical object. If the change is NOT about a specific object (e.g., sky/background/lighting/weather/color tone/texture) or it involves multiple objects, use \"other\".
- Identify the object being added or removed (only one object; if invalid or non-object, output \"[[OBJECT_NAME:INVALID]]\").
- Provide a confidence score from 0 to 10 (lower confidence for non-object or multi-object instructions).
Return ONLY valid JSON with keys \"action\", \"object_name\", \"confidence_score\". The value of \"object_name\" must wrap the object in double brackets as [[OBJECT_NAME:<name>]]. If multiple or invalid objects are mentioned, use exactly \"[[OBJECT_NAME:INVALID]]\" and set action=\"other\".

Instruction: \"{text_description}\"
Summary: \"{summary_description}\"
""".strip()


def _record_error(message: str, item_idx: int | None = None, details: dict | None = None):
    entry = {
        "timestamp": datetime.now(UTC).isoformat(),
        "item_index": item_idx,
        "message": message,
    }
    if details:
        entry["details"] = details
    with _error_lock:
        _error_records.append(entry)


def _request_completion(prompt: str, *, item_idx: int | None = None) -> str | None:
    payload = {
        "model": MODEL_NAME,
        "messages": [
            {"role": "user", "content": prompt}
        ],
        "max_tokens": MAX_NEW_TOKENS,
        "temperature": 0.0,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {API_KEY}",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(
                SERVER_URL,
                headers=headers,
                json=payload,
                proxies=_REQUEST_PROXIES,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            msg = f"[ERROR] Request to LLM server failed on attempt {attempt}/{MAX_RETRIES}: {exc}"
            print(msg)
            _record_error(msg, item_idx=item_idx)
        else:
            content = ""
            if isinstance(data, dict):
                choices = data.get("choices")
                if choices:
                    content = choices[0].get("message", {}).get("content", "") or ""
                else:
                    content = data.get("content", "") or ""
            if content:
                print("content:", content.strip())
                return content.strip()
            warn_msg = "[WARN] Empty response content."
            print(warn_msg)
            _record_error(warn_msg, item_idx=item_idx, details={"response": data})

        if attempt < MAX_RETRIES:
            sleep_time = RETRY_BACKOFF * attempt
            time.sleep(sleep_time)
    return None


def analyze_edit(text_description: str, summary_description: str, *, item_idx: int | None = None):
    prompt = _build_prompt(text_description, summary_description)
    content = _request_completion(prompt, item_idx=item_idx)
    if not content:
        return None
    match = re.search(r'\{.*\}', content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            err_msg = "[ERROR] Failed to parse JSON from content."
            print(err_msg)
            _record_error(err_msg, item_idx=item_idx, details={"content": content})
            return None
    warn_msg = "[WARN] No JSON block found in response."
    print(warn_msg)
    _record_error(warn_msg, item_idx=item_idx, details={"content": content})
    return None


def process_item(idx: int, item: dict):
    edit_type = item.get('edit_type', '')
    print(f"Processing item #{idx} with edit_type: '{edit_type}'")
    raw_text = item.get('text') or ""
    summary_text = item.get('summarized_text') or ""
    try:
        analysis_result = analyze_edit(raw_text, summary_text, item_idx=idx)
    except Exception as exc:
        print(f"[ERROR] Unexpected failure for item #{idx}: {exc}")
        _record_error("Unexpected failure", item_idx=idx, details={"exception": str(exc)})
        analysis_result = None

    processed = dict(item)
    if analysis_result:
        processed['analysis'] = analysis_result
        print(f"  - Action: {analysis_result.get('action')}")
        print(f"  - Object Name: {analysis_result.get('object_name')}")
        print(f"  - Confidence Score: {analysis_result.get('confidence_score')}/10\n")
    else:
        processed['analysis'] = {"error": "Failed to get a valid analysis from the model."}
        print("  - Failed to get a valid analysis from the model.\n")
    return idx, processed


def main():
    analysis_results = []
    filtered_items = list(enumerate(data_all))

    if not filtered_items:
        print("No entries matched the add/remove object criteria.")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_item, idx, item): idx
            for idx, item in filtered_items
        }
        with tqdm(total=len(filtered_items), desc="Analyzing edits", unit="item") as progress:
            for future in as_completed(futures):
                idx, processed_item = future.result()
                analysis_results.append((idx, processed_item))
                progress.update(1)

    analysis_results.sort(key=lambda x: x[0])
    ordered_results = [item for _, item in analysis_results]

    output_json_path = "analysis_results.json"
    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(ordered_results, f, ensure_ascii=False, indent=4)

    print(f"Analysis complete. Processed {len(ordered_results)} entries. Results saved to {output_json_path}")
    if _error_records:
        with open(ERROR_LOG_PATH, 'w', encoding='utf-8') as f:
            json.dump(_error_records, f, ensure_ascii=False, indent=2)
        print(f"Encountered {len(_error_records)} issues. Details saved to {ERROR_LOG_PATH}")
    else:
        print("No errors encountered.")


if __name__ == "__main__":
    main()
