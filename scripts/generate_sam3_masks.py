import json
import os
import torch
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from transformers import Sam3Processor, Sam3Model
from tqdm import tqdm
import re
from datetime import datetime
import sys

# Configuration
JSON_PATH = "test/test_bbox_results.json"
# Generate a timestamped output directory
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = f"test/sam3_visualizations_text_only_merged_{TIMESTAMP}" 
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)

def overlay_mask_on_image(image, mask, color=(255, 0, 0), alpha=0.5):
    """
    Overlays a binary mask on an image with a specific color and transparency.
    """
    mask_layer = Image.new("RGBA", image.size, color + (0,))
    mask_pil = Image.fromarray(mask).convert("L")
    mask_rgba = Image.new("RGBA", image.size, color + (int(255 * alpha),))
    overlay = Image.composite(mask_rgba, Image.new("RGBA", image.size, (0,0,0,0)), mask_pil)
    image = image.convert("RGBA")
    combined = Image.alpha_composite(image, overlay)
    return combined.convert("RGB")

def create_comparison_image(before_img, after_img, mask, is_remove):
    """
    Creates a side-by-side comparison.
    """
    if is_remove:
        masked_before = overlay_mask_on_image(before_img, mask, color=(255, 0, 0)) # Red for remove
        left_img = masked_before
        right_img = after_img
    else:
        masked_after = overlay_mask_on_image(after_img, mask, color=(0, 255, 0)) # Green for add/edit
        left_img = before_img
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

def main():
    # 1. Setup
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Loading SAM3 model on {DEVICE}...")
    try:
        # Ensure we use the local 'sam3' folder
        model_path = "sam3"
        print(f"正在从本地文件夹加载 SAM3 模型: {model_path}")
        
        model = Sam3Model.from_pretrained(model_path).to(DEVICE)
        processor = Sam3Processor.from_pretrained(model_path)
    except Exception as e:
        print(f"Error loading SAM3: {e}")
        sys.exit(1)

    # 2. Load Data
    data = load_json(JSON_PATH)
    print(f"Loaded {len(data)} items from {JSON_PATH}")

    # 3. Process Items
    for item_idx, item in enumerate(tqdm(data, desc="Processing Images")):
        if "bbox" not in item or "objects" not in item["bbox"]:
            continue

        objects = item["bbox"]["objects"]
        if not objects:
            continue

        local_input_path = item.get("local_input_image")
        output_path = item.get("output_image")
        
        if not local_input_path or not output_path:
            continue
            
        if not os.path.exists(local_input_path) or not os.path.exists(output_path):
            continue

        image_groups = {}
        path_to_type = {} 

        for obj_idx, obj in enumerate(objects):
            is_remove = obj.get("is_remove", False)
            
            if is_remove:
                target_path = local_input_path
                path_to_type[target_path] = True 
            else:
                target_path = output_path
                path_to_type[target_path] = False
                
            if target_path not in image_groups:
                image_groups[target_path] = []
            
            image_groups[target_path].append((obj, obj_idx))

        for target_path, obj_list in image_groups.items():
            target_image = None
            try:
                target_image = Image.open(target_path).convert("RGB")
                # target_image = target_image.resize((1024, 1024), Image.Resampling.LANCZOS) # Removed forced resize
            except Exception as e:
                print(f"Error opening {target_path}: {e}")
                raise e # STOP on error
            
            merged_mask = None
            labels_processed = []

            # --- Iterate over EACH object/label individually ---
            for obj, original_idx in obj_list:
                text_prompt = obj.get("label")
                if not text_prompt:
                    continue
                
                # Keep track of processed labels for filename
                cleaned_label = re.sub(r'[\\/:*?"<>|]', '', text_prompt).replace(' ', '_')
                labels_processed.append(cleaned_label)

                try:
                    # Call model for THIS specific label
                    inputs = processor(
                        images=target_image,
                        text=[text_prompt], # Single label list
                        return_tensors="pt"
                    ).to(DEVICE)

                    with torch.no_grad():
                        outputs = model(**inputs)

                    results = processor.post_process_instance_segmentation(
                        outputs,
                        threshold=0.5,
                        mask_threshold=0.5,
                        target_sizes=inputs.get("original_sizes").tolist()
                    )[0]
                    
                    masks = results["masks"]
                    scores = results["scores"]
                    
                    # STRICT CHECK: Stop if no mask is found
                    if len(masks) == 0:
                        error_msg = f"STOPPING: No mask found for label '{text_prompt}' in image '{target_path}' (Item {item_idx})."
                        print(error_msg)
                        raise RuntimeError(error_msg)
                            
                    # Select the mask with the HIGHEST score for this label
                    best_idx = torch.argmax(scores).item()
                    
                    # Removed score threshold check. We take the best mask regardless of score.
                    
                    best_mask_tensor = masks[best_idx]
                    
                    mask_np = best_mask_tensor.cpu().numpy()
                    
                    if mask_np.ndim == 3 and mask_np.shape[0] == 1:
                            mask_np = mask_np.squeeze(0)
                    elif mask_np.ndim == 3 and mask_np.shape[0] == 3:
                            mask_np = mask_np[0] 

                    if mask_np.ndim != 2:
                        raise RuntimeError(f"Unexpected mask shape {mask_np.shape} for '{text_prompt}'")
                        
                    mask_bool = (mask_np > 0)
                    
                    # Merge into the cumulative mask
                    if merged_mask is None:
                        merged_mask = mask_bool
                    else:
                        merged_mask = np.logical_or(merged_mask, mask_bool)
                        
                except Exception as e:
                    print(f"CRITICAL ERROR processing label '{text_prompt}' for {target_path}: {e}")
                    raise e # STOP on error

            # --- After processing all labels for this image group ---
            
            if merged_mask is None:
                 # This should not happen if we raise error on empty masks above, 
                 # unless obj_list was empty or prompts were empty
                 continue
                
            merged_mask_uint8 = merged_mask.astype(np.uint8) * 255
            
            # Load other image for comparison
            other_image = None
            if target_path == local_input_path:
                other_image_path = output_path
            else:
                other_image_path = local_input_path
                
            try:
                if os.path.exists(other_image_path):
                    other_image = Image.open(other_image_path).convert("RGB")
                    # other_image = other_image.resize((1024, 1024), Image.Resampling.LANCZOS) # Removed forced resize
                else:
                    print(f"Other image not found: {other_image_path}.")
                    raise FileNotFoundError(f"Other image not found: {other_image_path}")
            except Exception as e:
                print(f"Failed to load other image {other_image_path}: {e}.")
                raise e
            
            is_remove_group = path_to_type[target_path]
            
            if is_remove_group:
                before_img = target_image
                after_img = other_image
            else:
                before_img = other_image
                after_img = target_image
                    
            comparison_img = create_comparison_image(before_img, after_img, merged_mask_uint8, is_remove_group)
            
            type_str = "REMOVE" if is_remove_group else "ADD"
            
            # Combine labels for filename (limit length)
            all_labels_str = "_".join(labels_processed)[:100]
            
            filename = f"item_{item_idx:04d}_{type_str}_merged_{all_labels_str}.png"
            save_path = os.path.join(OUTPUT_DIR, filename)
            
            try:
                comparison_img.save(save_path)
            except Exception as e:
                print(f"Error saving comparison image {save_path}: {e}")
                raise e

if __name__ == "__main__":
    main()
