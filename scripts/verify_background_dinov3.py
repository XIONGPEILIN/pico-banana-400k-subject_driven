import os
os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"] = "5"
import json
import glob
import random
import shutil
import torch
import torch.nn.functional as F
import numpy as np
from PIL import Image
from tqdm import tqdm
from transformers import AutoImageProcessor, AutoModel
# --- Configuration ---
# Model Name provided by user
MODEL_NAME = "facebook/dinov3-vit7b16-pretrain-lvd1689m"
# Directory containing the logs and masks
WORK_DIR = "openimages/agent_full_pipeline_merged_ALL"

# Thresholds (can be tuned later based on distribution)
# GLOBAL_SIM_THRESHOLD = 0.85 

def load_dinov3():
    print(f"Loading model: {MODEL_NAME}...")
    try:
        processor = AutoImageProcessor.from_pretrained(MODEL_NAME)
        model = AutoModel.from_pretrained(MODEL_NAME, device_map="auto")
        model.eval()
        return processor, model
    except Exception as e:
        print(f"Error loading DINOv3 model: {e}")
        exit(1)

def get_dino_features(processor, model, image, device):
    """
    Returns:
        pooler_output: (1, hidden_dim) - Global CLS feature
        patch_tokens: (1, H_grid, W_grid, hidden_dim) - Spatial features
    """
    # Ensure image is RGB
    if image.mode != "RGB":
        image = image.convert("RGB")
        
    inputs = processor(images=image, return_tensors="pt").to(device)
    
    with torch.inference_mode():
        outputs = model(**inputs)
        
    pooler_output = outputs.pooler_output
    last_hidden_state = outputs.last_hidden_state
    
    # Determine grid size
    # ViT output: (Batch, Seq_Len, Dim). Seq_Len = Num_Patches + 1 (CLS)
    
    # Attempt to get image_size and patch_size from processor config
    H_img = inputs['pixel_values'].shape[2]
    W_img = inputs['pixel_values'].shape[3]
    
    patch_size = None
    if hasattr(model.config, 'patch_size'):
        patch_size = model.config.patch_size
    elif hasattr(model.config, 'vision_config') and hasattr(model.config.vision_config, 'patch_size'):
        patch_size = model.config.vision_config.patch_size
    else:
        # Fallback for patch_size if not found in config - common for ViT-B/16 is 16
        patch_size = 16 

    if patch_size == 0: 
        raise ValueError("Patch size is zero, cannot determine grid dimensions.")

    H_grid = H_img // patch_size
    W_grid = W_img // patch_size
    
    expected_spatial_patches = H_grid * W_grid
    
    # Determine the number of special tokens to skip
    num_cls_tokens = 1 # Always 1 CLS token
    num_register_tokens = getattr(model.config, 'num_register_tokens', 0) # Get from config, default to 0 if not present
    
    start_index_for_patches = num_cls_tokens + num_register_tokens
    
    # Extract patch tokens (skip CLS and register tokens)
    patch_tokens = last_hidden_state[:, start_index_for_patches:, :]
    
    if patch_tokens.shape[1] != expected_spatial_patches:
        raise ValueError(f"After accounting for CLS ({num_cls_tokens}) and register tokens ({num_register_tokens}), "
                         f"patch_tokens sequence length ({patch_tokens.shape[1]}) still does not match "
                         f"expected spatial grid size ({expected_spatial_patches}). Cannot reshape."
                         f"Total tokens in last_hidden_state: {last_hidden_state.shape[1]}")
    
    # Reshape to spatial grid
    patch_tokens = patch_tokens.reshape(1, H_grid, W_grid, -1)
    grid_size = H_grid # For compatibility with existing return signature which expects a single grid_size
    
    return pooler_output, patch_tokens, grid_size

def create_masked_visualization(image, mask_image, color=(255, 0, 0), opacity=0.5):
    """Overlays a mask on an image with a given color and opacity."""
    if image.mode != "RGB":
        image = image.convert("RGB")
    if mask_image.mode != "L":
        mask_image = mask_image.convert("L")

    # Ensure mask is the same size as the image
    if image.size != mask_image.size:
        mask_image = mask_image.resize(image.size, Image.NEAREST)

    # Create a color overlay from the mask
    mask_arr = np.array(mask_image) > 0
    color_overlay = np.zeros((image.height, image.width, 3), dtype=np.uint8)
    color_overlay[mask_arr] = color

    # Blend the image with the color overlay
    image_arr = np.array(image)
    blended_arr = image_arr.copy()
    
    # Apply opacity
    blended_arr[mask_arr] = (
        (1 - opacity) * image_arr[mask_arr] + opacity * color_overlay[mask_arr]
    ).astype(np.uint8)

    return Image.fromarray(blended_arr)

def combine_images_side_by_side(img1, img2):
    """Combines two images horizontally."""
    if img1.size != img2.size:
        img2 = img2.resize(img1.size, Image.LANCZOS)
    
    width, height = img1.size
    combined_img = Image.new('RGB', (width * 2, height))
    combined_img.paste(img1, (0, 0))
    combined_img.paste(img2, (width, 0))
    return combined_img

def calculate_metrics(processor, model, img_path_a, img_path_b, mask_paths):
    device = model.device
    mask_img = None
    
    # 1. Load Images
    try:
        img_a = Image.open(img_path_a).convert("RGB")
        img_b = Image.open(img_path_b).convert("RGB")
        
        # Ensure img_b has the same size as img_a
        if img_a.size != img_b.size:
            img_b = img_b.resize(img_a.size, Image.LANCZOS) # Use a good quality downsampling filter
            
        # Load and Merge Masks by BBox
        merged_bbox = None
        base_size = img_a.size # (W, H)

        for mp in mask_paths:
            if os.path.exists(mp):
                m = Image.open(mp).convert("L")
                # Resize to match img_a
                if m.size != base_size:
                    m = m.resize(base_size, Image.NEAREST)
                m_arr = np.array(m)

                # Get bbox of current mask
                rows, cols = np.where(m_arr > 0)
                if len(rows) == 0:
                    continue # empty mask

                x1, y1 = cols.min(), rows.min()
                x2, y2 = cols.max(), rows.max()

                if merged_bbox is None:
                    merged_bbox = [x1, y1, x2, y2]
                else:
                    merged_bbox[0] = min(merged_bbox[0], x1)
                    merged_bbox[1] = min(merged_bbox[1], y1)
                    merged_bbox[2] = max(merged_bbox[2], x2)
                    merged_bbox[3] = max(merged_bbox[3], y2)

        if merged_bbox is None:
            return {"error": "No valid masks with content found to create a bbox"}, None

        # Create a new mask from the merged bbox
        combined_mask_arr = np.zeros((base_size[1], base_size[0]), dtype=np.uint8)
        x1, y1, x2, y2 = merged_bbox
        combined_mask_arr[y1:y2+1, x1:x2+1] = 255
        mask_img = Image.fromarray(combined_mask_arr)
        
    except Exception as e:
        return {"error": str(e)}, None

    # 2. Get Features (Original Images)
    # Processor will handle resizing of images to model input size
    global_a, patches_a, grid_size = get_dino_features(processor, model, img_a, device)
    global_b, patches_b, _         = get_dino_features(processor, model, img_b, device)
    
    # 3. Global Similarity (Structure/Layout Check)
    global_sim = F.cosine_similarity(global_a, global_b, dim=-1).item()
    
    # --- NEW: Input-level Masking Analysis ---
    # Create BG-only and FG-only images for global comparison
    
    # Normalize mask to 0-1 for multiplication
    mask_norm = combined_mask_arr.astype(float) / 255.0
    # Binarize for clean cutting (optional, but often better for DINO to see black void)
    mask_binary = (mask_norm > 0.5).astype(float)
    mask_binary_inv = 1.0 - mask_binary
    
    # Expand to 3 channels
    mask_3ch = np.stack([mask_binary]*3, axis=-1)
    mask_inv_3ch = np.stack([mask_binary_inv]*3, axis=-1)
    
    img_a_arr = np.array(img_a).astype(float)
    img_b_arr = np.array(img_b).astype(float)
    
    # Apply masks
    # Backgrounds (Multiply by Inverse Mask)
    img_a_bg = Image.fromarray((img_a_arr * mask_inv_3ch).astype(np.uint8))
    img_b_bg = Image.fromarray((img_b_arr * mask_inv_3ch).astype(np.uint8))
    
    # Foregrounds (Multiply by Mask)
    img_a_fg = Image.fromarray((img_a_arr * mask_3ch).astype(np.uint8))
    img_b_fg = Image.fromarray((img_b_arr * mask_3ch).astype(np.uint8))
    
    # Compute features for masked inputs
    global_a_bg, _, _ = get_dino_features(processor, model, img_a_bg, device)
    global_b_bg, _, _ = get_dino_features(processor, model, img_b_bg, device)
    
    global_a_fg, _, _ = get_dino_features(processor, model, img_a_fg, device)
    global_b_fg, _, _ = get_dino_features(processor, model, img_b_fg, device)
    
    input_masked_bg_sim = F.cosine_similarity(global_a_bg, global_b_bg, dim=-1).item()
    input_masked_fg_sim = F.cosine_similarity(global_a_fg, global_b_fg, dim=-1).item()
    
    # -----------------------------------------
    
    # 4. Patch-wise Analysis (Existing Logic)
    patch_sim_map = F.cosine_similarity(patches_a, patches_b, dim=-1) # (1, Grid, Grid)
    
    # 5. Align Mask to Feature Grid
    # Resize mask to match the feature grid size
    mask_tensor = torch.from_numpy(np.array(mask_img.resize((grid_size, grid_size), Image.NEAREST))).float().to(device) / 255.0
    mask_tensor = mask_tensor.unsqueeze(0) # (1, Grid, Grid)
    
    # Binarize mask for calculation (soft threshold)
    fg_mask = (mask_tensor > 0.5).float()
    bg_mask = (mask_tensor <= 0.5).float()
    
    # 6. Calculate Scores
    bg_area = bg_mask.sum()
    if bg_area > 0:
        bg_sim_score = (patch_sim_map * bg_mask).sum() / bg_area
        bg_sim_score = bg_sim_score.item()
    else:
        bg_sim_score = 0.0 

    fg_area = fg_mask.sum()
    if fg_area > 0:
        fg_sim_score = (patch_sim_map * fg_mask).sum() / fg_area
        fg_sim_score = fg_sim_score.item()
    else:
        fg_sim_score = 1.0 

    return {
        "global_sim": global_sim,
        "bg_sim": bg_sim_score,
        "fg_sim": fg_sim_score,
        "input_masked_bg_sim": input_masked_bg_sim,
        "input_masked_fg_sim": input_masked_fg_sim,
        "grid_size": grid_size
    }, mask_img

def main():
    # Setup
    processor, model = load_dinov3()
    
    # Find all log files
    log_files = glob.glob(os.path.join(WORK_DIR, "item_*", "item_*_log.json"))
    # Fallback to flat layout if any
    log_files += [p for p in glob.glob(os.path.join(WORK_DIR, "item_*_log.json")) if p not in log_files]
    
    total_found_logs = len(log_files)
    num_to_process = min(100, total_found_logs) # Process max 100, or all if fewer
    
    if num_to_process == 0:
        print("No log files found to process. Exiting.")
        return

    # Randomly sample num_to_process files
    random_log_files = random.sample(log_files, num_to_process)
    print(f"Processing {num_to_process} randomly selected items from {total_found_logs} total logs in {WORK_DIR}")
    
    # Create destination directory for samples
    DEST_DIR = os.path.join("openimages", "random_100_analyzed")
    os.makedirs(DEST_DIR, exist_ok=True)
    print(f"Copying analyzed samples to {DEST_DIR}")
    
    updated_count = 0
    
    for log_path in tqdm(random_log_files, desc="Verifying & Copying"):
        with open(log_path, 'r') as f:
            data = json.load(f)
            
        item_idx = data.get("item_idx")
        merged_mask_img = None
        item_dir = os.path.dirname(log_path)
        
        # Determine paths
        if "original_item" in data:
            img_before_path = data["original_item"].get("local_input_image")
            img_after_path = data["original_item"].get("output_image")
        else:
            print(f"Skipping {log_path}: 'original_item' data missing.")
            continue

        # Find Masks (Merge if both exist)
        mask_remove = os.path.join(item_dir, f"item_{item_idx}_MASK_REMOVE.png")
        mask_add = os.path.join(item_dir, f"item_{item_idx}_MASK_ADD.png")
        
        found_masks = []
        if os.path.exists(mask_remove): found_masks.append(mask_remove)
        if os.path.exists(mask_add): found_masks.append(mask_add)
            
        if not found_masks:
            # No mask found
            data["dino_analysis"] = {"error": "No mask found"}
        else:
            if os.path.exists(img_before_path) and os.path.exists(img_after_path):
                metrics, merged_mask_img = calculate_metrics(processor, model, img_before_path, img_after_path, found_masks)
                data["dino_analysis"] = metrics
            else:
                 data["dino_analysis"] = {"error": "Source images not found"}
        
        # Save back to log
        with open(log_path, 'w') as f:
            json.dump(data, f, indent=2)
            updated_count += 1
            
        # --- Copy to Test Directory ---
        try:
            # 1. Copy Log (now contains updated analysis)
            shutil.copy(log_path, os.path.join(DEST_DIR, f"item_{item_idx}_log.json"))
            
            # 2. Copy Input Image
            if os.path.exists(img_before_path):
                ext = os.path.splitext(img_before_path)[1]
                shutil.copy(img_before_path, os.path.join(DEST_DIR, f"item_{item_idx}_input{ext}"))
                
            # 3. Copy Output Image
            if os.path.exists(img_after_path):
                ext = os.path.splitext(img_after_path)[1]
                shutil.copy(img_after_path, os.path.join(DEST_DIR, f"item_{item_idx}_output{ext}"))
                
            # 4. Copy Mask(s)
            for mp in found_masks:
                mask_name = os.path.basename(mp)
                shutil.copy(mp, os.path.join(DEST_DIR, mask_name))

            # 5. Save and Copy Merged Mask
            if merged_mask_img:
                merged_mask_path = os.path.join(DEST_DIR, f"item_{item_idx}_MERGED_BBOX_MASK.png")
                merged_mask_img.save(merged_mask_path)

                # Create and save combined visualization of the mask on before and after images
                try:
                    if os.path.exists(img_before_path) and os.path.exists(img_after_path):
                        img_a = Image.open(img_before_path).convert("RGB")
                        img_b = Image.open(img_after_path).convert("RGB")

                        if img_a.size != img_b.size:
                            img_b = img_b.resize(img_a.size, Image.LANCZOS)

                        before_masked = create_masked_visualization(img_a, merged_mask_img)
                        after_masked = create_masked_visualization(img_b, merged_mask_img)
                        
                        combined_viz = combine_images_side_by_side(before_masked, after_masked)
                        viz_path = os.path.join(DEST_DIR, f"item_{item_idx}_MERGED_MASK_VISUALIZATION.png")
                        combined_viz.save(viz_path)
                except Exception as e:
                    print(f"Error creating visualization for item {item_idx}: {e}")
                
        except Exception as e:
            print(f"Error copying files for item {item_idx}: {e}")

    print(f"Finished. Updated {updated_count} logs and copied them to {DEST_DIR}.")

if __name__ == "__main__":
    main()
