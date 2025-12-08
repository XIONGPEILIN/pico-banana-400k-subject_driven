import json
import os
os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"] = "1,2,3,4"  # Adjust as per your GPU setup
import sys
import torch
import numpy as np
from PIL import Image
from tqdm import tqdm
import re
from datetime import datetime
import base64
from openai import OpenAI
import io
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import pycocotools.mask as mask_utils
import shutil
from typing import Dict, List

# --- SAM3 Setup ---
# Ensure tools/sam3 is in sys.path to import sam3
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Assuming script is in scripts/
SAM3_TOOL_PATH = os.path.join(PROJECT_ROOT, "tools", "sam3")
if SAM3_TOOL_PATH not in sys.path:
    sys.path.insert(0, SAM3_TOOL_PATH)

# Import SAM3 components
import sam3
from sam3 import build_sam3_image_model
from sam3.model.sam3_image_processor import Sam3Processor
from sam3.model.box_ops import box_xyxy_to_xywh
from sam3.train.masks_ops import rle_encode


# --- Configuration ---
# GPU Setup for SAM3
os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"

# Raw Data Path
DATA_JSONL = "openimages/jsonl/sft_with_local_source_image_path.jsonl"
# Output Directory
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = f"openimages/pico_sam_output_ALL_{TIMESTAMP}"
LOG_FILE = os.path.join(OUTPUT_DIR, "processing_log.json")
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
MAX_ITEMS = 0
MAX_WORKERS = 4  # Use 4 threads, each with its own SAM instance

# LLM Config
MODEL_NAME = os.environ.get("LLM_MODEL_NAME", "Qwen/Qwen3-VL-32B-Instruct-FP8")
SERVER_URL = "http://localhost:7512/v1"
API_KEY = "EMPTY"

# SAM3 Paths (Hardcoded as per environment)
SAM3_ROOT = "/home/yanai-lab/xiong-p/test/qwen-image-edit/pico-banana-400k/pico-banana-400k-subject_driven/sam3"
BPE_PATH = os.path.join(SAM3_ROOT, "bpe_simple_vocab_16e6.txt.gz")
CHECKPOINT_PATH = os.path.join(SAM3_ROOT, "sam3.pt")

thread_local_data = threading.local()
_device_lock = threading.Lock()
_next_device_idx = 0


def _available_devices():
    """
    Resolve visible CUDA devices or fallback to CPU.
    Respects CUDA_VISIBLE_DEVICES ordering.
    """
    if not torch.cuda.is_available():
        return ["cpu"]
    visible_count = torch.cuda.device_count()
    env_devices = os.environ.get("CUDA_VISIBLE_DEVICES")
    if env_devices:
        # With CUDA_VISIBLE_DEVICES, PyTorch renumbers visible GPUs to 0..N-1
        tokens = [t.strip() for t in env_devices.split(",") if t.strip() != ""]
        if len(tokens) != visible_count:
            print(f"[WARN] CUDA_VISIBLE_DEVICES count ({len(tokens)}) != torch visible count ({visible_count}); using torch count.")
        return list(range(visible_count))
    return list(range(visible_count))


AVAILABLE_DEVICES = _available_devices()


def get_thread_sam_processor() -> Sam3Processor:
    """
    Lazily create one SAM processor per worker thread.
    """
    if hasattr(thread_local_data, "sam_processor"):
        return thread_local_data.sam_processor
    if not AVAILABLE_DEVICES:
        raise RuntimeError("No devices available for SAM3.")

    global _next_device_idx
    with _device_lock:
        device_idx = _next_device_idx
        _next_device_idx += 1

    device_choice = AVAILABLE_DEVICES[device_idx % len(AVAILABLE_DEVICES)]
    device_str = device_choice if device_choice == "cpu" else f"cuda:{device_choice}"

    print(f"[SAM3] Initializing processor on thread {threading.current_thread().name} using {device_str}")
    if device_choice != "cpu":
        torch.cuda.set_device(device_choice)
        build_device = "cuda"
    else:
        build_device = "cpu"

    model = build_sam3_image_model(
        bpe_path=BPE_PATH,
        checkpoint_path=CHECKPOINT_PATH,
        device=build_device,
    )
    processor = Sam3Processor(model, device=device_str, confidence_threshold=0.5)
    thread_local_data.sam_processor = processor
    return processor

# --- Agent and SAM Logic Integration ---

# Copied from tools/sam3/sam3/agent/helpers/mask_overlap_removal.py
def mask_intersection(
    masks1: torch.Tensor, masks2: torch.Tensor, block_size: int = 16
) -> torch.Tensor:
    assert masks1.shape[1:] == masks2.shape[1:]
    assert masks1.dtype == torch.bool and masks2.dtype == torch.bool
    N, M = masks1.shape[0], masks2.shape[0]
    out = torch.zeros(N, M, device=masks1.device, dtype=torch.long)
    for i in range(0, N, block_size):
        for j in range(0, M, block_size):
            a = masks1[i : i + block_size]
            b = masks2[j : j + block_size]
            inter = (a[:, None] & b[None, :]).flatten(-2).sum(-1)
            out[i : i + block_size, j : j + block_size] = inter
    return out

def mask_iom(masks1: torch.Tensor, masks2: torch.Tensor) -> torch.Tensor:
    assert masks1.shape[1:] == masks2.shape[1:]
    assert masks1.dtype == torch.bool and masks2.dtype == torch.bool
    inter = mask_intersection(masks1, masks2)
    area1 = masks1.flatten(-2).sum(-1)  # (N,)
    area2 = masks2.flatten(-2).sum(-1)  # (M,)
    min_area = torch.min(area1[:, None], area2[None, :]).clamp_min(1)
    return inter.float() / (min_area.float() + 1e-8)

def _decode_single_mask_for_overlap(mask_repr, h: int, w: int) -> np.ndarray:
    if isinstance(mask_repr, (list, tuple, np.ndarray)):
        arr = np.array(mask_repr)
        if arr.ndim != 2:
            raise ValueError("Mask array must be 2D (H, W).")
        return (arr > 0).astype(np.uint8)
    if not isinstance(mask_repr, (str, bytes)):
        raise ValueError("Unsupported mask representation type for RLE decode.")
    rle = {
        "counts": mask_repr if isinstance(mask_repr, (str, bytes)) else str(mask_repr),
        "size": [h, w],
    }
    decoded = mask_utils.decode(rle)
    if decoded.ndim == 3:
        decoded = decoded[:, :, 0]
    return (decoded > 0).astype(np.uint8)

def _decode_masks_to_torch_bool(pred_masks: List, h: int, w: int) -> torch.Tensor:
    bin_masks = [_decode_single_mask_for_overlap(m, h, w) for m in pred_masks]
    masks_np = np.stack(bin_masks, axis=0).astype(np.uint8)  # (N, H, W)
    return torch.from_numpy(masks_np > 0)

def remove_overlapping_masks(sample: Dict, iom_thresh: float = 0.3) -> Dict:
    if "pred_masks" not in sample or not isinstance(sample["pred_masks"], list):
        return sample
    pred_masks = sample["pred_masks"]
    N = len(pred_masks)
    if N <= 1:
        return sample
    h, w = int(sample["orig_img_h"]), int(sample["orig_img_w"])
    pred_scores = sample.get("pred_scores", [1.0] * N)
    pred_boxes = sample.get("pred_boxes", None)
    masks_bool = _decode_masks_to_torch_bool(pred_masks, h, w)
    order = sorted(range(N), key=lambda i: float(pred_scores[i]), reverse=True)
    kept_idx: List[int] = []
    kept_masks: List[torch.Tensor] = []
    for i in order:
        cand = masks_bool[i].unsqueeze(0)
        if len(kept_masks) == 0:
            kept_idx.append(i)
            kept_masks.append(masks_bool[i])
            continue
        kept_stack = torch.stack(kept_masks, dim=0)
        iom_vals = mask_iom(cand, kept_stack).squeeze(0)
        if torch.any(iom_vals > iom_thresh):
            continue
        kept_idx.append(i)
        kept_masks.append(masks_bool[i])
    kept_idx_sorted = sorted(kept_idx)
    out = dict(sample)
    out["pred_masks"] = [pred_masks[i] for i in kept_idx_sorted]
    out["pred_scores"] = [pred_scores[i] for i in kept_idx_sorted]
    if pred_boxes is not None:
        out["pred_boxes"] = [pred_boxes[i] for i in kept_idx_sorted]
    return out

def get_sam_output_in_memory(sam_processor, image_path, text_prompt):
    """
    Performs SAM inference directly and returns the full processed output dict.
    """
    image = Image.open(image_path).convert("RGB")
    orig_img_w, orig_img_h = image.size

    inference_state = sam_processor.set_image(image)
    inference_state = sam_processor.set_text_prompt(state=inference_state, prompt=text_prompt)

    pred_boxes_xyxy = torch.stack([
        inference_state["boxes"][:, 0] / orig_img_w,
        inference_state["boxes"][:, 1] / orig_img_h,
        inference_state["boxes"][:, 2] / orig_img_w,
        inference_state["boxes"][:, 3] / orig_img_h,
    ], dim=-1)
    
    outputs = {
        "original_image_path": image_path,
        "orig_img_h": orig_img_h,
        "orig_img_w": orig_img_w,
        "pred_boxes": box_xyxy_to_xywh(pred_boxes_xyxy).tolist(),
        "pred_masks": [m["counts"] for m in rle_encode(inference_state["masks"].squeeze(1))],
        "pred_scores": inference_state["scores"].tolist(),
    }

    processed_outputs = remove_overlapping_masks(outputs)
    if "pred_scores" in processed_outputs and processed_outputs["pred_scores"]:
        score_indices = sorted(range(len(processed_outputs["pred_scores"])), key=lambda i: processed_outputs["pred_scores"][i], reverse=True)
        for key in ["pred_scores", "pred_boxes", "pred_masks"]:
            if key in processed_outputs:
                processed_outputs[key] = [processed_outputs[key][i] for i in score_indices]

    valid_masks = [rle for rle in processed_outputs.get("pred_masks", []) if len(rle) > 4]
    processed_outputs["pred_masks"] = valid_masks
    return processed_outputs

# --- Helper Functions ---

def encode_image_to_base64(image: Image.Image) -> str:
    buffered = io.BytesIO()
    image.save(buffered, format="JPEG")
    return base64.b64encode(buffered.getvalue()).decode("utf-8")

def decode_rle_mask(rle_string, height, width):
    """Decodes a COCO RLE string into a binary mask."""
    if not rle_string:
        return None
    rle = {"counts": rle_string, "size": [height, width]}
    mask = mask_utils.decode(rle)
    return mask

def overlay_multiple_masks(image, masks, color=(255, 0, 0), alpha=0.5):
    """Overlays a list of binary masks onto an image."""
    if not masks:
        return image.convert("RGB")
    
    overlay = image.copy().convert("RGBA")
    
    for mask_array in masks:
        if mask_array is None:
            continue
        # Create a color image for the current mask
        mask_img = Image.new('RGBA', image.size, color + (0,))
        
        # Create a PIL Image from the numpy array of the mask
        bool_mask_pil = Image.fromarray(mask_array.astype(np.uint8) * 255)

        # Composite the color onto the overlay using the mask
        overlay = Image.composite(Image.new('RGBA', image.size, color + (int(255 * alpha),)), overlay, bool_mask_pil)

    # Combine the original image with the overlay
    return Image.alpha_composite(image.convert("RGBA"), overlay).convert("RGB")

def create_zoomed_view(image, mask_array, target_size=(512, 512), padding=10):
    """
    Crops the image based on the mask's bounding box and resizes it to create a zoomed-in view.
    """
    if mask_array is None or not np.any(mask_array):
        return None

    rows = np.any(mask_array, axis=1)
    cols = np.any(mask_array, axis=0)
    ymin, ymax = np.where(rows)[0][[0, -1]]
    xmin, xmax = np.where(cols)[0][[0, -1]]

    # Add padding
    ymin = max(0, ymin - padding)
    xmin = max(0, xmin - padding)
    ymax = min(image.height, ymax + padding + 1)
    xmax = min(image.width, xmax + padding + 1)

    cropped_image = image.crop((xmin, ymin, xmax, ymax))

    # Resize to target size for a consistent "zoomed" view
    zoomed_image = cropped_image.resize(target_size, Image.Resampling.LANCZOS)

    return zoomed_image

def create_comparison_image(before_img, after_img, remove_masks, add_masks):
    before_img_viz = overlay_multiple_masks(before_img, remove_masks, color=(255, 0, 0))
    after_img_viz = overlay_multiple_masks(after_img, add_masks, color=(0, 255, 0))
    
    if before_img_viz.height != after_img_viz.height:
        new_height = before_img_viz.height
        new_width = int(after_img_viz.width * (new_height / after_img_viz.height))
        after_img_viz = after_img_viz.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
    total_width = before_img_viz.width + after_img_viz.width
    combined_img = Image.new("RGB", (total_width, before_img_viz.height))
    combined_img.paste(before_img_viz, (0, 0))
    combined_img.paste(after_img_viz, (before_img_viz.width, 0))
    return combined_img


class QwenDiscoveryAgent:
    def __init__(self):
        self.client = OpenAI(api_key=API_KEY, base_url=SERVER_URL)

    def generate_sub_prompts(
        self,
        total_prompt: str,
        text_instruction: str = "",
        before_path: str = None,
        after_path: str = None,
        is_remove: bool = False,
    ) -> List[str]:
        """
        Second-stage VLM call: given a single total_prompt, emit short noun phrases (1-4 words)
        for the changed object or its parts, with before/after context.
        """
        source_label = "before/original image" if is_remove else "after/edited image"
        prompt_text = f"""
You are given a changed-object description:
"{total_prompt}"

This concept is marked as {"removal" if is_remove else "addition"} and should be found on the {source_label}.

TASK:
- Each entry must be a short noun phrase (1-4 words), referring to one physical object/part/accessory.
- No relations, no spatial words, no actions, no counts; do not describe background/supporting surfaces unless that surface itself is the changed object.
- Avoid texture/lighting/style/mood words.
- Only output parts belonging to the changed object itself; if the change is a background (e.g., wall/sky), use a simple phrase (e.g., "blue sky", "plain wall").
- Keep words simple and general but flexible enough to match visual objects (e.g., “open book”), but safe from producing words segmentation tools cannot use.

OUTPUT:
Return a JSON array of strings (only as many as needed; do not pad). Example:
["mirror", "red hat"]
"""
        user_content = []
        try:
            if before_path and os.path.exists(before_path):
                before_b64 = encode_image_to_base64(Image.open(before_path).convert("RGB"))
                user_content.append({"type": "text", "text": "Before (Original) Image:"})
                user_content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{before_b64}"}})
        except Exception as e:
            print(f"[SubPrompt Warn] Failed to load before image {before_path}: {e}")
        try:
            if after_path and os.path.exists(after_path):
                after_b64 = encode_image_to_base64(Image.open(after_path).convert("RGB"))
                user_content.append({"type": "text", "text": "After (Edited) Image:"})
                user_content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{after_b64}"}})
        except Exception as e:
            print(f"[SubPrompt Warn] Failed to load after image {after_path}: {e}")

        user_content.append({"type": "text", "text": prompt_text})

        messages = [
            {"role": "user", "content": user_content},
        ]
        try:
            resp = self.client.chat.completions.create(
                model=MODEL_NAME,
                messages=messages,
                max_tokens=512,
            )
            content = resp.choices[0].message.content or ""
            json_match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                json_str = content[content.find("[") : content.rfind("]") + 1]
            sub_prompts = json.loads(json_str)
            if isinstance(sub_prompts, list):
                clean = []
                for sp in sub_prompts:
                    if not isinstance(sp, str):
                        continue
                    sp = sp.strip()
                    if not sp:
                        continue
                    # Strip quotes/punctuation and enforce length 1-4 words
                    sp = " ".join([s for s in sp.replace('"', " ").replace("'", " ").replace(",", " ").replace(".", " ").split() if s])
                    words = sp.split()
                    if len(words) == 0 or len(words) > 4:
                        continue
                    if "arabic" in sp.lower():
                        sp = "arabic text"
                    clean.append(sp)
                return clean
        except Exception as e:
            print(f"[SubPrompt Error] {e}")
        return []

    def discover_changes(self, before_path, after_path, text_instruction):
        before_img = Image.open(before_path).convert("RGB")
        after_img = Image.open(after_path).convert("RGB")
        before_b64 = encode_image_to_base64(before_img)
        after_b64 = encode_image_to_base64(after_img)

        if text_instruction:
            context_instruction = (f"CONTEXT: The 'Edited Image' is the result of modifying the 'Original Image' based on this prompt: '{text_instruction}'. Your task is to analyze this change.")
        else:
            context_instruction = "CONTEXT: Your task is to identify the most salient object that has been changed (removed, added, or modified) between the 'Original Image' and the 'Edited Image'."

        prompt = f"""
{context_instruction}


TASK:
**Analyze**: Carefully compare the two images to find distinct **object-level** changes. For each change, write `total_prompt` as one concise phrase (aim for 8–16 words) describing only the changed object: category + 1–2 key attributes + spatial location. Do not describe surrounding background/supporting surfaces unless that surface itself is the changed object. For background changes (e.g., wall/sky), use a simple phrase (e.g., "plain blue sky").

OUTPUT REQUIREMENTS:
- Your output must be a single JSON array `[]`.
- Each element represents **one distinct object-level change**.
- Each element must contain:

    1. `"total_prompt"`:  
       A **detailed natural-language description** of the changed object, including:  
       - object category  
       - **spatial location or orientation** within the scene (e.g., "on the left side", "in the upper-right corner").
       - written **in 8–16 words**

       Example:  
       `"a small brown dog on the right side of the image"`

    2. `"is_remove"`:  
       A boolean:  
       - `true` if the object was removed  
       - `false` if the object was added


- Include **as many atomic object parts as required**; do not reduce or merge details.
- Keep words simple and general: use common nouns and plain attributes; avoid brand names, jargon, or niche style words.

EXAMPLE:
```json
[
  {{
    "total_prompt": "a small brown dog on the right side of the image ",
    "is_remove": true,
  }},
  {{
    "total_prompt": " a big brown dog on the right side of the image",
    "is_remove": false,
  }}
]
```
"""
        messages = [
            {"role": "user", "content": [
                {"type": "text", "text": "Image 1 (Original):"},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{before_b64}"}},
                {"type": "text", "text": "Image 2 (Edited):"},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{after_b64}"}},
                {"type": "text", "text": prompt}
            ]}
        ]
        try:
            response = self.client.chat.completions.create(model=MODEL_NAME, messages=messages, max_tokens=4096)
            content = response.choices[0].message.content
            print(f"[DEBUG] VLM Content: {content}")
            json_str = None
            json_match = re.search(r"```json\s*(.*?)\s*```", content, re.DOTALL)
            if json_match:
                json_str = json_match.group(1)
            else:
                start = content.find('[')
                end = content.rfind(']')
                if start != -1 and end != -1 and end > start:
                    json_str = content[start:end+1]
            if json_str:
                try:
                    # The output is expected to be a list of objects
                    return json.loads(json_str)
                except json.JSONDecodeError as e:
                    print(f"[Discovery Error] Failed to parse JSON string: {json_str}. Error: {e}")
            print("[Discovery Error] No valid JSON array found in the response.")
            return []
        except Exception as e:
            print(f"[Discovery Error] {e}")
            return []

def score_mask_with_vlm(
    client,
    image_path: str,
    mask_rle: str,
    total_prompt: str,
    item_output_dir: str,
    sub_prompt: str,
    candidate_idx: int,
    before_path: str = None,
    after_path: str = None,
    is_remove: bool = False,
) -> float:
    """
    Asks the VLM to score a single mask based on its relevance to a sub-prompt.
    It sends the original image and a zoomed-in view of the masked object.
    """
    try:
        original_image = Image.open(image_path).convert("RGB")
        h, w = original_image.height, original_image.width

        # Decode the single mask
        mask_array = decode_rle_mask(mask_rle, h, w)
        if mask_array is None:
            return 0.0
        
        # Create a zoomed-in view of the masked object
        zoomed_image = create_zoomed_view(original_image, mask_array)
        if zoomed_image is None:
            return 0.0
        
        # Save visualization for debugging
        debug_dir = os.path.join(item_output_dir, "debug_scoring")
        os.makedirs(debug_dir, exist_ok=True)
        safe_sub_prompt = "".join(c for c in sub_prompt if c.isalnum() or c in " ._").rstrip().replace(" ", "_")
        debug_filename = f"candidate_{candidate_idx}_{safe_sub_prompt}.jpg"
        zoomed_image.save(os.path.join(debug_dir, debug_filename))

        # Encode images: original (current), before, after, and zoomed
        original_b64 = encode_image_to_base64(original_image)
        zoomed_b64 = encode_image_to_base64(zoomed_image)
        before_b64 = None
        after_b64 = None
        try:
            if before_path and os.path.exists(before_path):
                before_b64 = encode_image_to_base64(Image.open(before_path).convert("RGB"))
        except Exception as e:
            print(f"[Scoring Warn] Failed to load before image {before_path}: {e}")
        try:
            if after_path and os.path.exists(after_path):
                after_b64 = encode_image_to_base64(Image.open(after_path).convert("RGB"))
        except Exception as e:
            print(f"[Scoring Warn] Failed to load after image {after_path}: {e}")
        # Fallbacks
        if before_b64 is None:
            before_b64 = original_b64
        if after_b64 is None:
            after_b64 = original_b64

        # New prompt using only sub_prompt and referencing the two images
        # New prompt using both total_prompt and sub_prompt
        source_label = "before/original" if is_remove else "after/edited"
        prompt = f"""You are given an 'Original Image' and a 'Zoomed-in Image' showing a specific object.

CONTEXT:
- The entire changed object is described as: "{total_prompt}".
- The current component to evaluate is: "{sub_prompt}".
 - This component comes from a task that compares 'Original' vs 'Edited' to find object-level differences; focus on whether this masked object matches the described changed part.
 - The candidate mask you are scoring was generated on the {source_label} image.

TASK:
Evaluate how well the object shown in the 'Zoomed-in Image' corresponds to the component description "{sub_prompt}".
Use the 'Original Image' only for additional scene context if needed.

OUTPUT:
Provide a single floating point score between 0.0 and 1.0, representing:
- 1.0 = perfectly matches the component description
- 0.0 = does not match at all

Example output:
0.85
"""


        messages = [
            {"role": "user", "content": [
                {"type": "text", "text": "Before (Original) Image:"},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{before_b64}"}},
                {"type": "text", "text": "After (Edited) Image:"},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{after_b64}"}},
                {"type": "text", "text": "Zoomed-in Image:"},
                {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{zoomed_b64}"}},
                {"type": "text", "text": prompt}
            ]}
        ]
        
        response = client.chat.completions.create(model=MODEL_NAME, messages=messages, max_tokens=20)
        content = response.choices[0].message.content
        
        # Extract the float score
        scores = re.findall(r"(\d+(?:\.\d+)?)", content)
        if scores:
            return float(scores[-1])
        else:
            print(f"[Scoring WARN] Could not parse score from VLM response: '{content}'")
            return 0.0
            
    except Exception as e:
        print(f"[Scoring Error] {e}")
        return 0.0


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


def process_item(item_idx, item, discovery_agent, output_dir):
    before_path = item.get("local_input_image") or item.get("before_image")
    after_path = item.get("output_image") or item.get("after_image")
    instruction = item.get("text")
    
    item_output_dir = os.path.join(output_dir, f"item_{item_idx}")
    os.makedirs(item_output_dir, exist_ok=True)

    log_entry = {
        "item_idx": item_idx,
        "original_item": item,
        "instruction": instruction,
        "change_concepts": []
    }

    # Step 1: VLM call to discover and decompose changes
    change_concepts = discovery_agent.discover_changes(before_path, after_path, instruction)
    if not change_concepts:
        print(f"[Item {item_idx}] No changes detected by VLM.")
        return log_entry
    
    print(f"[Item {item_idx}] VLM discovered {len(change_concepts)} change concepts.")
    log_entry["change_concepts"] = change_concepts # Log the raw discovery (will annotate generated sub-prompts)

    try:
        img_before = Image.open(before_path).convert("RGB")
        img_after = Image.open(after_path).convert("RGB")
    except Exception as e:
        print(f"[Item {item_idx}] Image load error: {e}")
        return log_entry

    final_add_masks_rle = []
    final_remove_masks_rle = []

    # Step 2: Iterate through each discovered change concept
    for i, concept in enumerate(change_concepts):
        total_prompt = concept.get("total_prompt")
        is_remove = concept.get("is_remove")
        image_to_process = before_path if is_remove else after_path

        # Always regenerate sub_prompts in a second stage using the total_prompt + instruction
        sub_prompts = discovery_agent.generate_sub_prompts(
            total_prompt,
            instruction,
            before_path=before_path,
            after_path=after_path,
            is_remove=is_remove,
        )
        if sub_prompts:
            print(f"  -> Concept {i+1}: generated {len(sub_prompts)} sub-prompts via VLM.")
        else:
            # Fallback to any provided sub_prompts if regeneration fails
            sub_prompts = concept.get("sub_prompts", [])

        # Record the generated (or fallback) sub-prompts into the logged concept for transparency
        concept["sub_prompts_generated"] = sub_prompts

        if not sub_prompts:
            print(f"  -> Concept {i+1}: no sub-prompts provided; skipping.")
            continue
        
        print(f"  -> Processing Concept {i+1}/{len(change_concepts)}: '{total_prompt}' (Remove: {is_remove})")

        h, w = (img_before.height, img_before.width) if is_remove else (img_after.height, img_after.width)

        concept_masks_rle = []
        
        # Step 2a: For each sub_prompt, find the best mask
        sam_processor = get_thread_sam_processor()

        for j, sub_prompt in enumerate(sub_prompts):
            print(f"    -> Sub-prompt {j+1}/{len(sub_prompts)}: '{sub_prompt}'")

            # Get candidate masks from SAM
            sam_outputs = get_sam_output_in_memory(sam_processor, image_to_process, sub_prompt)
            candidate_masks = sam_outputs.get("pred_masks", [])
            
            if not candidate_masks:
                print(f"      - No candidate masks found by SAM for '{sub_prompt}'.")
                continue
            
            print(f"      - SAM found {len(candidate_masks)} candidates. Scoring...")

            # Step 2b: Score each candidate mask with the VLM scorer
            best_mask_rle = None
            best_score = -1.0
            kept_high = []
            
            for k, mask_rle in enumerate(candidate_masks):
                score = score_mask_with_vlm(
                    discovery_agent.client,
                    image_to_process,
                    mask_rle,
                    total_prompt,
                    item_output_dir,
                    sub_prompt,
                    k,
                    before_path=before_path,
                    after_path=after_path,
                    is_remove=is_remove,
                )
                print(f"      - Candidate {k+1} score: {score:.2f}")
                if score >= 0.95:
                    kept_high.append(mask_rle)
                if score > best_score:
                    best_score = score
                    best_mask_rle = mask_rle

            if kept_high:
                print(f"      - Keeping {len(kept_high)} mask(s) scoring >=0.95 for '{sub_prompt}'.")
                concept_masks_rle.extend(kept_high)
            elif best_mask_rle and best_score > 0.5:
                print(f"      - Best mask for '{sub_prompt}' found with score {best_score:.2f}.")
                concept_masks_rle.append(best_mask_rle)
            else:
                print(f"      - No suitable mask found for '{sub_prompt}' (best score: {best_score:.2f}).")

        # Aggregate masks for the concept
        if is_remove:
            final_remove_masks_rle.extend(concept_masks_rle)
        else:
            final_add_masks_rle.extend(concept_masks_rle)

    # Step 3: Decode all final RLEs to binary masks for visualization
    final_remove_masks = [decode_rle_mask(rle, img_before.height, img_before.width) for rle in final_remove_masks_rle]
    final_add_masks = [decode_rle_mask(rle, img_after.height, img_after.width) for rle in final_add_masks_rle]

    # Save individual final masks for inspection
    def _save_masks(masks, dir_path, prefix):
        os.makedirs(dir_path, exist_ok=True)
        saved = 0
        for idx, mask in enumerate(masks):
            if mask is None:
                continue
            Image.fromarray(mask.astype(np.uint8) * 255).save(os.path.join(dir_path, f"{prefix}_{idx}.png"))
            saved += 1
        return saved

    remove_saved = _save_masks(final_remove_masks, os.path.join(item_output_dir, "final_masks_remove"), "remove_mask")
    add_saved = _save_masks(final_add_masks, os.path.join(item_output_dir, "final_masks_add"), "add_mask")

    # Save the final RLEs used
    with open(os.path.join(item_output_dir, f"item_{item_idx}_final_masks_rle.json"), "w") as f:
        json.dump({"remove": final_remove_masks_rle, "add": final_add_masks_rle}, f, indent=2)

    # Visualization
    final_comp = create_comparison_image(img_before, img_after, final_remove_masks, final_add_masks)
    final_comp.save(os.path.join(item_output_dir, f"item_{item_idx}_MERGED.png"))
    
    # Save a merged mask for remove and add for compatibility
    merged_masks_rle = {}
    if final_remove_masks:
        merged_remove = np.logical_or.reduce(final_remove_masks)
        Image.fromarray(merged_remove.astype(np.uint8) * 255).save(os.path.join(item_output_dir, f"item_{item_idx}_MASK_REMOVE.png"))
        rle = mask_utils.encode(np.asfortranarray(merged_remove.astype(np.uint8)))
        merged_masks_rle["remove"] = {"size": rle["size"], "counts": rle["counts"].decode("ascii") if isinstance(rle["counts"], bytes) else rle["counts"]}

    if final_add_masks:
        merged_add = np.logical_or.reduce(final_add_masks)
        Image.fromarray(merged_add.astype(np.uint8) * 255).save(os.path.join(item_output_dir, f"item_{item_idx}_MASK_ADD.png"))
        rle = mask_utils.encode(np.asfortranarray(merged_add.astype(np.uint8)))
        merged_masks_rle["add"] = {"size": rle["size"], "counts": rle["counts"].decode("ascii") if isinstance(rle["counts"], bytes) else rle["counts"]}

    if merged_masks_rle:
        with open(os.path.join(item_output_dir, f"item_{item_idx}_final_merged_masks_rle.json"), "w") as f:
            json.dump(merged_masks_rle, f, indent=2)
    
    # Log final results for this item
    log_entry["final_results"] = {
        "removals_found": len(final_remove_masks_rle),
        "additions_found": len(final_add_masks_rle),
        "remove_masks_saved": remove_saved,
        "add_masks_saved": add_saved,
    }
    with open(os.path.join(item_output_dir, f"item_{item_idx}_log.json"), "w") as f:
        json.dump(log_entry, f, indent=2)
        
    return log_entry

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Checking VLLM connection at {SERVER_URL}...")
    try:
        client = OpenAI(api_key=API_KEY, base_url=SERVER_URL)
        models = client.models.list()
        print(f"VLLM Connected. Available models: {[m.id for m in models.data]}")
    except Exception as e:
        print(f"Error connecting to VLLM: {e}")
        return
    
    print(f"Using per-thread SAM3 loading from {CHECKPOINT_PATH} with {MAX_WORKERS} workers.")

    discovery_agent = QwenDiscoveryAgent()
    raw_data = _load_jsonl(DATA_JSONL)
    print(f"Loaded {len(raw_data)} raw items.")

    valid_items = []
    for idx, item in enumerate(raw_data):
        before_path = item.get("local_input_image") or item.get("before_image")
        after_path = item.get("output_image") or item.get("after_image")
        if before_path and after_path and os.path.exists(before_path) and os.path.exists(after_path):
            valid_items.append((idx, item))
            if MAX_ITEMS and len(valid_items) >= MAX_ITEMS:
                break
    print(f"Processing {len(valid_items)} items.")

    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_item, idx, item, discovery_agent, OUTPUT_DIR) for idx, item in valid_items]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Pipeline"):
            try:
                res = future.result()
                if res:
                    all_results.append(res)
            except Exception as e:
                print(f"Thread execution error: {e}")

    with open(LOG_FILE, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"Logs saved to {LOG_FILE}")

if __name__ == "__main__":
    main()
