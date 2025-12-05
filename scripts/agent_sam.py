import json
import os
import sys
import torch
import numpy as np
from PIL import Image, ImageDraw
from transformers import Sam3Processor, Sam3Model
from tqdm import tqdm
import re
from datetime import datetime
import base64
from openai import OpenAI
import io
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
# Raw Data Path (The source of truth)
DATA_JSONL = "openimages/jsonl/sft_with_local_source_image_path.jsonl"
# Output Directory
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = f"test/agent_full_pipeline_merged_ALL"
LOG_FILE = os.path.join(OUTPUT_DIR, "processing_log.json")
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MAX_RETRIES = 3
MAX_ITEMS = None # Limit for testing, set to None for full run
MAX_WORKERS = 16 # IO threads for Qwen API

# LLM Config
MODEL_NAME = os.environ.get("LLM_MODEL_NAME", "Qwen/Qwen3-VL-235B-A22B-Instruct-FP8")
SERVER_URL = "http://localhost:7512/v1"
API_KEY = "EMPTY"

# Global lock for GPU model inference (Single Model Instance)
model_lock = threading.Lock()

# --- Helper Functions ---

def encode_image_to_base64(image: Image.Image) -> str:
    buffered = io.BytesIO()
    image.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode("utf-8")

def overlay_mask_on_image(image, mask, color=(255, 0, 0), alpha=0.5):
    if mask is None: return image
    mask_layer = Image.new("RGBA", image.size, color + (0,))
    mask_pil = Image.fromarray(mask).convert("L")
    mask_rgba = Image.new("RGBA", image.size, color + (int(255 * alpha),))
    overlay = Image.composite(mask_rgba, Image.new("RGBA", image.size, (0,0,0,0)), mask_pil)
    image = image.convert("RGBA")
    combined = Image.alpha_composite(image, overlay)
    return combined.convert("RGB")

def create_cutout(image, mask):
    """Creates an image with only the masked area visible, background is black/transparent."""
    image = image.convert("RGBA")
    mask_pil = Image.fromarray(mask).convert("L")
    
    # Create a black background image
    black_bg = Image.new("RGBA", image.size, (0, 0, 0, 255))
    
    # Composite: Where mask is white, use image; where mask is black, use black_bg
    cutout = Image.composite(image, black_bg, mask_pil)
    return cutout.convert("RGB")

def create_comparison_image(before_img, after_img, mask_remove, mask_add):
    # Apply Red Mask to Before Image (Remove)
    if mask_remove is not None:
        masked_before = overlay_mask_on_image(before_img, mask_remove, color=(255, 0, 0))
    else:
        masked_before = before_img

    # Apply Green Mask to After Image (Add)
    if mask_add is not None:
        masked_after = overlay_mask_on_image(after_img, mask_add, color=(0, 255, 0))
    else:
        masked_after = after_img
        
    left_img = masked_before
    right_img = masked_after
        
    if left_img.height != right_img.height:
        new_height = left_img.height
        new_width = int(right_img.width * (new_height / right_img.height))
        right_img = right_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
    total_width = left_img.width + right_img.width
    combined_img = Image.new("RGB", (total_width, left_img.height))
    combined_img.paste(left_img, (0, 0))
    combined_img.paste(right_img, (left_img.width, 0))
    return combined_img

class QwenAgent:
    def __init__(self):
        self.client = OpenAI(api_key=API_KEY, base_url=SERVER_URL)

    def discover_changes(self, before_path, after_path, text_instruction):
        before_img = Image.open(before_path).convert("RGB")
        after_img = Image.open(after_path).convert("RGB")
        before_b64 = encode_image_to_base64(before_img)
        after_b64 = encode_image_to_base64(after_img)

        if text_instruction:
            context_instruction = (
                f"CONTEXT: The 'Edited Image' is the result of modifying the 'Original Image' based on this prompt: '{text_instruction}'. "
                "Identify the specific object that has changed (removed, added, or modified) in response to this instruction."
            )
        else:
            context_instruction = "CONTEXT: Identify the most salient object that has changed (removed, added, or modified) between the two images."

        prompt = f"""
{context_instruction}

TASK:
1. Compare the 'Original Image' and 'Edited Image' meticulously.
2. Identify ALL objects that have changed (removed, added, or modified).
3. For EACH changed object, create a separate entry in the "objects" list.
4. Determine if the object was REMOVED (present in Original, absent in Edited).
5. Locate the object in the **Original Image** (if removed/modified) or **Edited Image** (if added).

OUTPUT REQUIREMENTS:
- Output a valid JSON object strictly following the schema below.
- **Label**: Provide a concise, visually distinctive label (1-6 words).
    - **Keep it simple:** "orange balloon" or "man in white shirt".
    - **AVOID** flowery language, complex sentences, or text reading (e.g., avoid "sign that says 'Shop'").
    - **AVOID** subjective adjectives like "sophisticated" or "beautiful".
- **Confidence**: 0-10 integer score.

If there is no visible change, return an empty list for "objects" and set confidence to 0.

Output format:
```json
{{
  "objects": [
    {{ "label": "...", "is_remove": true, "confidence": 5 }},
    {{ "label": "...", "is_remove": false, "confidence": 2 }}
  ]
}}
```
"""
        messages = [
            {"role": "system", "content": "You are an expert visual difference analyzer."},
            {"role": "user", "content": [
                {"type": "text", "text": "Image 1 (Original):"},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{before_b64}"}},
                {"type": "text", "text": "Image 2 (Edited):"},
                {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{after_b64}"}},
                {"type": "text", "text": prompt}
            ]}
        ]

        try:
            response = self.client.chat.completions.create(
                model=MODEL_NAME, messages=messages, max_tokens=1024
            )
            content = response.choices[0].message.content
            json_match = re.search(r'(\{.*\})', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(1))
            return json.loads(content)
        except Exception as e:
            print(f"[Discovery Error] {e}")
            return {"objects": []}

    def ask_for_better_prompt(self, target_img, current_prompt):
        target_b64 = encode_image_to_base64(target_img)
        prompt = f"""
I failed to find any object using the prompt: "{current_prompt}".
Please look at the image and provide a DIFFERENT, SIMPLER, and MORE DISTINCTIVE text prompt (1-6 words) to help me find the target object.
Focus on the most obvious visual feature (color, large shape).

Output Format: Just the new prompt.
"""
        try:
            response = self.client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{target_b64}"}}
                    ]}
                ],
                max_tokens=50
            )
            return response.choices[0].message.content.strip().replace('"', '')
        except Exception:
            return current_prompt

    def verify_segmentation(self, cutout_img, before_img, after_img, current_prompt):
        cutout_b64 = encode_image_to_base64(cutout_img)
        before_b64 = encode_image_to_base64(before_img)
        after_b64 = encode_image_to_base64(after_img)

        prompt = f"""
I segmented an object using prompt: "{current_prompt}".

Image 1: Original.
Image 2: Edited.
Image 3: Segmentation Candidate (Cutout).

TASK:
1. Identify the object change between Img 1 and Img 2.
2. Check if Img 3 (Cutout) covers the WHOLE changed object.

Decision Options:
- **PASSED**: The cutout is accurate and complete.
- **ADD: <prompt>**: The cutout is correct BUT MISSING PARTS. Provide a prompt for the MISSING part only (e.g., "ADD: the handle of the mug").
- **RETRY: <prompt>**: The cutout is WRONG (wrong object, too much background). Provide a better prompt to start over (e.g., "RETRY: red mug").

Output Format: One of the above options.
"""
        try:
            response = self.client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "user", "content": [
                        {"type": "text", "text": prompt},
                        {"type": "text", "text": "Image 1:"},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{before_b64}"}},
                        {"type": "text", "text": "Image 2:"},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{after_b64}"}},
                        {"type": "text", "text": "Cutout:"},
                        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{cutout_b64}"}}
                    ]}
                ],
                max_tokens=100
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"[Verification Error] {e}")
            return "PASSED"

def _load_jsonl(path):
    data = []
    target_types = {
        "Add a new object to the scene", "Add/Remove/Replace Accessories (glasses, hats, jewelry, masks)",
        "Clothing edit (change color/outfit)", "Remove an existing object", "Replace one object category with another"
    }
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            item = json.loads(line)
            if item.get('edit_type') in target_types:
                data.append(item)
    return data

def process_item(item_idx, item, agent, model, processor, output_dir):
    # Model and Processor are passed in, shared among threads.
    # Access to model(**inputs) MUST be guarded by model_lock
    
    before_path = item.get("local_input_image") or item.get("before_image")
    after_path = item.get("output_image") or item.get("after_image")
    instruction = item.get("text")
    
    log_entry = {
        "item_idx": item_idx,
        "instruction": instruction,
        "objects_processed": []
    }

    # print(f"\n=== Processing Item {item_idx} ===")
    
    discovery_result = agent.discover_changes(before_path, after_path, instruction)
    objects = discovery_result.get("objects", [])
    
    if not objects:
        # print(f"  [Item {item_idx}] No changes detected by Agent.")
        return log_entry

    # print(f"  [Item {item_idx}] Agent detected {len(objects)} objects.")

    merged_mask_remove = None
    merged_mask_add = None
    
    try:
        img_before = Image.open(before_path).convert("RGB")
        img_after = Image.open(after_path).convert("RGB")
    except Exception as e:
        # print(f"Image load error for item {item_idx}: {e}")
        return log_entry

    for obj_idx, obj in enumerate(objects):
        is_remove = obj.get("is_remove", False)
        target_path = before_path if is_remove else after_path
        target_image = img_before if is_remove else img_after
        other_image = img_after if is_remove else img_before

        initial_prompt = obj.get("label", "object")
        current_prompt = initial_prompt
        
        # print(f"  [Item {item_idx} Object {obj_idx}] Initial: '{current_prompt}' (Remove: {is_remove})")
        
        obj_log = {
            "initial_label": initial_prompt,
            "is_remove": is_remove,
            "attempts": [],
            "final_status": "FAILED",
            "final_prompt": None
        }

        cumulative_mask = None

        for attempt in range(MAX_RETRIES + 1):
            mask_uint8 = None
            try:
                # --- CRITICAL: GPU LOCK ---
                with model_lock:
                    inputs = processor(images=target_image, text=[current_prompt], return_tensors="pt").to(DEVICE)
                    with torch.no_grad():
                        outputs = model(**inputs)
                    results = processor.post_process_instance_segmentation(
                        outputs, threshold=0.4, mask_threshold=0.5, 
                        target_sizes=inputs.get("original_sizes").tolist()
                    )[0]
                # --- END LOCK ---
                
                if len(results["masks"]) > 0:
                    best_idx = torch.argmax(results["scores"]).item()
                    mask_tensor = results["masks"][best_idx]
                    mask_np = mask_tensor.cpu().numpy()
                    
                    if mask_np.ndim == 3 and mask_np.shape[0] == 1:
                            mask_np = mask_np.squeeze(0)
                    elif mask_np.ndim == 3 and mask_np.shape[0] == 3:
                            mask_np = mask_np[0] 

                    if mask_np.ndim != 2:
                        mask_uint8 = None
                    else:
                        mask_uint8 = (mask_np > 0).astype(np.uint8) * 255
                else:
                    mask_uint8 = None
            except Exception as e:
                print(f"      SAM3 Error item {item_idx}: {e}")
                mask_uint8 = None

            if mask_uint8 is None:
                if attempt < MAX_RETRIES:
                    new_prompt = agent.ask_for_better_prompt(target_image, current_prompt)
                    obj_log["attempts"].append({"attempt": attempt+1, "prompt": current_prompt, "result": "NO_MASK", "feedback": new_prompt})
                    
                    def clean_str(s): return re.sub(r'[^\w\s]', '', s).lower().strip()
                    if clean_str(new_prompt) == clean_str(current_prompt):
                            break
                    current_prompt = new_prompt
                    continue 
                else:
                    obj_log["attempts"].append({"attempt": attempt+1, "prompt": current_prompt, "result": "NO_MASK", "feedback": "MAX_RETRIES"})
                    break
            else:
                current_step_mask = (mask_uint8 > 0)

            if cumulative_mask is None:
                vis_mask = current_step_mask
            else:
                vis_mask = np.logical_or(cumulative_mask, current_step_mask)
            
            vis_mask_uint8 = (vis_mask.astype(np.uint8) * 255)
            vis_cutout = create_cutout(target_image, vis_mask_uint8)
            
            if is_remove:
                img_b_v = target_image
                img_a_v = other_image
            else:
                img_b_v = other_image
                img_a_v = target_image

            feedback = agent.verify_segmentation(vis_cutout, img_b_v, img_a_v, current_prompt)
            
            obj_log["attempts"].append({"attempt": attempt+1, "prompt": current_prompt, "result": "MASK_FOUND", "feedback": feedback})
            
            if "PASSED" in feedback.upper():
                if cumulative_mask is None:
                    cumulative_mask = current_step_mask
                else:
                    cumulative_mask = np.logical_or(cumulative_mask, current_step_mask)
                
                obj_log["final_status"] = "PASSED"
                obj_log["final_prompt"] = current_prompt
                break 
            
            elif "ADD:" in feedback.upper():
                if cumulative_mask is None:
                    cumulative_mask = current_step_mask
                else:
                    cumulative_mask = np.logical_or(cumulative_mask, current_step_mask)
                
                new_prompt = feedback.split(":", 1)[1].strip()
                current_prompt = new_prompt
                
            elif "RETRY:" in feedback.upper():
                new_prompt = feedback.split(":", 1)[1].strip()
                current_prompt = new_prompt
                
            else:
                if attempt < MAX_RETRIES:
                    new_prompt = feedback.replace('"', '').strip()
                    if len(new_prompt) > 50: new_prompt = new_prompt[:50]
                    current_prompt = new_prompt
                else:
                    if cumulative_mask is None:
                            cumulative_mask = current_step_mask 

        log_entry["objects_processed"].append(obj_log)

        if cumulative_mask is not None:
            if is_remove:
                if merged_mask_remove is None:
                    merged_mask_remove = cumulative_mask
                else:
                    merged_mask_remove = np.logical_or(merged_mask_remove, cumulative_mask)
            else:
                if merged_mask_add is None:
                    merged_mask_add = cumulative_mask
                else:
                    merged_mask_add = np.logical_or(merged_mask_add, cumulative_mask)

    merged_mask_remove_uint8 = (merged_mask_remove.astype(np.uint8) * 255) if merged_mask_remove is not None else None
    merged_mask_add_uint8 = (merged_mask_add.astype(np.uint8) * 255) if merged_mask_add is not None else None
    
    final_comp = create_comparison_image(
        img_before,
        img_after,
        merged_mask_remove_uint8,
        merged_mask_add_uint8
    )
    
    save_path = os.path.join(output_dir, f"item_{item_idx}_MERGED.png")
    final_comp.save(save_path)
    
    if merged_mask_remove is not None:
        mask_rem_img = Image.fromarray((merged_mask_remove.astype(np.uint8) * 255), mode='L')
        mask_rem_path = os.path.join(output_dir, f"item_{item_idx}_MASK_REMOVE.png")
        mask_rem_img.save(mask_rem_path)
        
    if merged_mask_add is not None:
        mask_add_img = Image.fromarray((merged_mask_add.astype(np.uint8) * 255), mode='L')
        mask_add_path = os.path.join(output_dir, f"item_{item_idx}_MASK_ADD.png")
        mask_add_img.save(mask_add_path)
    
    # Save per-item log
    item_log_path = os.path.join(output_dir, f"item_{item_idx}_log.json")
    with open(item_log_path, "w") as f:
        json.dump(log_entry, f, indent=2)
    
    return log_entry

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Load SAM3 Globally
    print(f"Loading SAM3 model on {DEVICE}...")
    try:
        global_model = Sam3Model.from_pretrained("sam3").to(DEVICE)
        global_processor = Sam3Processor.from_pretrained("sam3")
    except Exception as e:
        print(f"Error loading SAM3: {e}")
        return

    # 2. Init Agent
    agent = QwenAgent()

    # 3. Load Raw Data
    raw_data = _load_jsonl(DATA_JSONL)
    print(f"Loaded {len(raw_data)} raw items.")

    # 4. Filter Valid Items
    valid_items = []
    for idx, item in enumerate(raw_data):
        before_path = item.get("local_input_image") or item.get("before_image")
        after_path = item.get("output_image") or item.get("after_image")
        if before_path and after_path and os.path.exists(before_path) and os.path.exists(after_path):
            valid_items.append((idx, item))
            if MAX_ITEMS and len(valid_items) >= MAX_ITEMS:
                break
    
    print(f"Processing {len(valid_items)} items using {MAX_WORKERS} threads.")

    # 5. Thread Pool Execution
    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks, passing the GLOBAL model/processor
        futures = [
            executor.submit(process_item, idx, item, agent, global_model, global_processor, OUTPUT_DIR) 
            for idx, item in valid_items
        ]
        
        # Collect results
        for future in tqdm(as_completed(futures), total=len(futures), desc="Pipeline"):
            try:
                res = future.result()
                if res:
                    all_results.append(res)
            except Exception as e:
                print(f"Thread execution error: {e}")

    # Final log save
    with open(LOG_FILE, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"Logs saved to {LOG_FILE}")

if __name__ == "__main__":
    main()
