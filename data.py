import base64
import json
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from pathlib import Path
import re # Added for robust JSON extraction
from typing import Tuple

from openai import OpenAI, OpenAIError
from tqdm import tqdm

# Model / API config (aligned with the provided template)
MODEL_NAME = os.environ.get("LLM_MODEL_NAME", "Qwen/Qwen3-VL-235B-A22B-Instruct-FP8")
SERVER_URL = os.environ.get("SGLANG_SERVER_URL", "http://localhost:7512/v1")
API_KEY = os.environ.get("SGLANG_API_KEY", "EMPTY")
MAX_NEW_TOKENS = int(os.environ.get("LLM_MAX_NEW_TOKENS", "4096"))
DEFAULT_MAX_WORKERS = 8
MAX_WORKERS = int(os.environ.get("LLM_MAX_WORKERS", str(DEFAULT_MAX_WORKERS)))
REQUEST_TIMEOUT = int(os.environ.get("LLM_REQUEST_TIMEOUT", "3600"))
MAX_RETRIES = int(os.environ.get("LLM_MAX_RETRIES", "3"))
RETRY_BACKOFF = float(os.environ.get("LLM_RETRY_BACKOFF", "2.0"))
DEBUG_SAMPLE_SIZE = 100   # 0 means use all

# I/O config
DATA_PATH = os.environ.get("DATA_JSONL", "openimages/jsonl/sft_with_local_source_image_path.jsonl")
OUTPUT_JSON_PATH = os.environ.get("BBOX_OUTPUT_PATH", "test/test_bbox_results.json")
ERROR_LOG_PATH = os.environ.get("BBOX_ERROR_LOG", "All/test_bbox_errors.json")
RAW_OUTPUT_DIR = os.environ.get("RAW_OUTPUT_DIR", "All/raw_model_outputs")
PARSED_OUTPUT_DIR = os.environ.get("PARSED_OUTPUT_DIR", "All/parsed_model_outputs") # New constant

# Ensure the All directory exists
os.makedirs("All", exist_ok=True)

# Ensure local requests bypass proxies so we always hit the  on-host server directly.
os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
os.environ.setdefault("no_proxy", "127.0.0.1,localhost")

_error_records = []
_error_lock = threading.Lock()

# Client per template

client = OpenAI(
    api_key="EMPTY",
    base_url="http://localhost:7512/v1",
    timeout=3600
)


def _load_jsonl(path: str) -> list[dict]:
    # Target edit types to process
    target_edit_types = {
        "Add a new object to the scene",
        "Add/Remove/Replace Accessories (glasses, hats, jewelry, masks)",
        "Clothing edit (change color/outfit)",
        "Remove an existing object",
        "Replace one object category with another",
    }
    
    data = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)
            # Only keep items with target edit types
            if item.get('edit_type') in target_edit_types:
                data.append(item)
    
    if DEBUG_SAMPLE_SIZE > 0 and DEBUG_SAMPLE_SIZE < len(data):
        # Group by edit_type and sample 2 from each type
        from collections import defaultdict
        random.seed(0)  # Use 0 as seed for reproducibility
        grouped = defaultdict(list)
        for item in data:
            edit_type = item.get('edit_type', 'unknown')
            grouped[edit_type].append(item)
        
        sampled_data = []
        for edit_type, items in grouped.items():
            # Sample up to 2 items from each edit type
            sample_size = min(2, len(items))
            sampled_data.extend(random.sample(items, sample_size))
        
        # If we still need more to reach DEBUG_SAMPLE_SIZE, add more randomly
        if len(sampled_data) < DEBUG_SAMPLE_SIZE:
            remaining = [item for item in data if item not in sampled_data]
            needed = DEBUG_SAMPLE_SIZE - len(sampled_data)
            if remaining and needed > 0:
                sampled_data.extend(random.sample(remaining, min(needed, len(remaining))))
        
        data = sampled_data
    
    return data


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


def _encode_image(path: str) -> str:
    with open(path, "rb") as f:
        data = f.read()
    encoded = base64.b64encode(data).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


def _build_prompt(edit_prompt: str | None = None) -> str:
    # 动态插入用户指令，如果存在，给予最高优先级
    if edit_prompt:
        context_instruction = (
            f"CONTEXT: The 'Edited Image' is the result of modifying the 'Original Image' based on this prompt: '{edit_prompt}'. "
            "Identify the specific object that has changed (removed, added, or modified) in response to this instruction."
        )
    else:
        context_instruction = "CONTEXT: Identify the most salient object that has changed (removed, added, or modified) between the two images."

    return f"""
{context_instruction}

TASK:
1. Compare the 'Original Image' and 'Edited Image' meticulously.
2. Identify ALL objects that have changed (removed, added, or modified).
3. For EACH changed object, create a separate entry in the "objects" list.
4. Determine if the object was REMOVED (present in Original, absent in Edited).
5. Locate the object in the **Original Image** (if removed/modified) or **Edited Image** (if added).

OUTPUT REQUIREMENTS:
- Output a valid JSON object strictly following the schema below.
- **Label**: Provide a concise, visually distinctive label (2-10 words).
    - **CRITICAL:** If multiple similar objects exist, YOU MUST use simple location words ("left", "right", "center", "foreground") or unique visual attributes ("red", "metal", "tall") to distinguish the target.
    - **Keep it simple:** "orange balloon on the left" or "man in white shirt".
    - **AVOID** flowery language, complex sentences, or text reading (e.g., avoid "sign that says 'Shop'").
    - **AVOID** subjective adjectives like "sophisticated" or "beautiful".
- **Confidence**: 0-10 integer score.

If there is no visible change, return an empty list for "objects" and set confidence to 0.

Output format:
```json
{{
  "objects": [
    {{
      "label": "red ceramic coffee mug",
      "is_remove": true, 
      "description": "The red mug has been removed from the table.",
      "confidence": 5
    }},
    {{
      "label": "blue glass vase with flowers",
      "is_remove": false, 
      "description": "A blue vase has been added to the shelf.",
      "confidence": 2
    }}
  ],
}}
```""".strip()



def _request_completion(before_b64: str, after_b64: str, *, item_idx: int | None = None, edit_prompt: str | None = None) -> str | None:
    messages = [
        {
            "role": "system",
            "content": (
                "You are a sophisticated visual difference Analyzer. "
                "You are an expert in Object Grounding and Change Detection."
            )
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text", 
                    "text": "Analyze the changes between these two images.\n\nImage 1 (Original):"
                },
                {
                    "type": "image_url", 
                    "image_url": {"url": before_b64}
                },
                {
                    "type": "text", 
                    "text": "\nImage 2 (Edited):"
                },
                {
                    "type": "image_url", 
                    "image_url": {"url": after_b64}
                },
                {
                    "type": "text", 
                    "text": _build_prompt(edit_prompt)
                },
            ],
        }
    ]

    for attempt in range(1, MAX_RETRIES + 1):
        start = time.time()
        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=messages,
                max_tokens=MAX_NEW_TOKENS,
            )
            elapsed = time.time() - start
            print(f"[INFO] Request {item_idx if item_idx is not None else '-'} attempt {attempt} cost: {elapsed:.2f}s")
        except OpenAIError as exc:
            msg = f"[ERROR] Request to LLM server failed on attempt {attempt}/{MAX_RETRIES}: {exc}"
            print(msg)
            _record_error(msg, item_idx=item_idx)
        else:
            content = ""
            if response and response.choices:
                content = response.choices[0].message.content or ""
            if content:
                return content.strip()
            warn_msg = "[WARN] Empty response content."
            _record_error(warn_msg, item_idx=item_idx, details={"response": str(response)})

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF * attempt)
    return None


def analyze_change(before_path: str, after_path: str, *, item_idx: int | None = None, edit_prompt: str | None = None):
    before_b64 = _encode_image(before_path)
    after_b64 = _encode_image(after_path)
    content = _request_completion(before_b64, after_b64, item_idx=item_idx, edit_prompt=edit_prompt)
    if not content:
        return None
    
    # Save raw model output for debugging
    if item_idx is not None:
        raw_output_dir_path = Path(RAW_OUTPUT_DIR)
        raw_output_dir_path.mkdir(parents=True, exist_ok=True)
        raw_output_path = raw_output_dir_path / f"raw_output_{item_idx}.txt"
        with open(raw_output_path, "w", encoding="utf-8") as f:
            f.write(content)
    
    # Clean up markdown code blocks if present
    if "```" in content:
        content = content.replace("```json", "").replace("```", "").strip()

    # Robustly extract JSON object using regex
    # This handles cases where the model wraps JSON in <tool_call>, <think>, or other text
    json_match = re.search(r'(\{.*\})', content, re.DOTALL)
    if json_match:
        content = json_match.group(1)
        
    try:
        data = json.loads(content)
        # Save parsed JSON data for debugging
        if data and item_idx is not None:
            parsed_output_dir_path = Path(PARSED_OUTPUT_DIR)
            parsed_output_dir_path.mkdir(parents=True, exist_ok=True)
            parsed_output_path = parsed_output_dir_path / f"parsed_output_{item_idx}.json"
            with open(parsed_output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        return data
    except json.JSONDecodeError:
        _record_error("[ERROR] Failed to parse JSON from model response.", item_idx=item_idx, details={"content": content})
        return None


def _resolve_paths(item: dict) -> Tuple[str, str] | None:
    before_path = item.get("local_input_image") or item.get("before_image")
    after_path = item.get("output_image") or item.get("after_image")
    if not before_path or not after_path:
        return None
    return str(before_path), str(after_path)


def process_item(idx: int, item: dict):
    resolved = _resolve_paths(item)
    processed = dict(item)
    if not resolved:
        msg = "Missing before/after image paths."
        _record_error(msg, item_idx=idx, details={"item": item})
        processed["bbox"] = {"error": msg}
        return idx, processed

    before_path, after_path = resolved
    if not Path(before_path).exists() or not Path(after_path).exists():
        msg = "Before or after image file not found."
        missing = {"before_exists": Path(before_path).exists(), "after_exists": Path(after_path).exists()}
        _record_error(msg, item_idx=idx, details={"paths": [before_path, after_path], **missing})
        processed["bbox"] = {"error": msg, **missing}
        return idx, processed

    edit_prompt = item.get("text")
    try:
        bbox_info = analyze_change(before_path, after_path, item_idx=idx, edit_prompt=edit_prompt)
    except Exception as exc:
        msg = f"Unexpected failure computing bbox: {exc}"
        _record_error(msg, item_idx=idx, details={"paths": [before_path, after_path]})
        processed["bbox"] = {"error": msg}
        return idx, processed

    if not bbox_info:
        processed["bbox"] = {"error": "Failed to get a valid bbox from the model."}
    else:
        processed["bbox"] = bbox_info
    return idx, processed


def _save_errors():
    if _error_records:
        with open(ERROR_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(_error_records, f, ensure_ascii=False, indent=2)
        print(f"Encountered {len(_error_records)} issues. Details saved to {ERROR_LOG_PATH}")


def main():
    print(f"[INFO] Using model {MODEL_NAME} via {SERVER_URL}")
    data_all = _load_jsonl(DATA_PATH)
    if not data_all:
        print("No data to process.")
        return

    results: list[tuple[int, dict]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_item, idx, item): idx for idx, item in enumerate(data_all)}
        with tqdm(total=len(futures), desc="Detecting changes", unit="item") as progress:
            for future in as_completed(futures):
                idx, processed_item = future.result()
                results.append((idx, processed_item))
                progress.update(1)

    results.sort(key=lambda x: x[0])
    ordered_results = [item for _, item in results]

    with open(OUTPUT_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(ordered_results, f, ensure_ascii=False, indent=4)

    _save_errors()


if __name__ == "__main__":
    main()
