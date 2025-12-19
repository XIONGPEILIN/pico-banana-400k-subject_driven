import os
import time
import json
import glob
import torch
import torch.nn.functional as F
import numpy as np
from PIL import Image
from tqdm import tqdm
from transformers import pipeline, AutoImageProcessor, AutoModel
import torch.multiprocessing as mp
import torchvision.transforms as transforms
from torch.utils.data import Dataset, DataLoader
from concurrent.futures import ProcessPoolExecutor, as_completed

# --- Configuration ---
MODEL_NAME = "facebook/dinov3-vit7b16-pretrain-lvd1689m"
# Directory containing logs, masks, images
WORK_DIR = "openimages/pico_sam_output_ALL_20251206_032609"
# Where to save audit JSONs (no file copying)
DEST_DIR = os.path.join("openimages", "dino_mask_audit_1")

# Thresholds
OBJECT_SIM_THRESHOLD = 0.9        # object unchanged if >=
BACKGROUND_SIM_THRESHOLD = 0.9    # background changed if <

# Limit number of items to process (None for all)
MAX_ITEMS = None

def get_available_devices():
    if torch.cuda.is_available():
        vis = os.environ.get("CUDA_VISIBLE_DEVICES")
        if vis:
            ids = [d.strip() for d in vis.split(",") if d.strip()]
            devices = [f"cuda:{i}" for i in range(len(ids))]
        else:
            devices = [f"cuda:{i}" for i in range(torch.cuda.device_count())]
    else:
        devices = ["cpu"]
    return devices


# Helper functions
def _load_image(path):
    return Image.open(path).convert("RGB")

def _apply_mask_to_image(image_pil, mask_tensor, fill_color=(0, 0, 0)):
    """
    Applies a binary mask tensor to a PIL image. Areas where the mask is 0 are filled with fill_color.
    Args:
        image_pil: PIL Image (RGB).
        mask_tensor: Torch tensor of shape (1, 1, H, W) with values 0 or 1, on the same device as the model.
        fill_color: RGB tuple for the color to fill masked-out regions.
    Returns:
        PIL Image with mask applied.
    """
    # Ensure mask_tensor is on CPU and converted to numpy
    # Squeeze to (H, W) for image operations
    mask_np = mask_tensor.squeeze().cpu().numpy()
    
    # Resize mask_np to match image_pil size if necessary
    # (mask_tensor could be at a different resolution if it was interpolated from a sub-mask)
    if mask_np.shape[0] != image_pil.height or mask_np.shape[1] != image_pil.width:
        # Resize mask to image size using nearest-neighbor interpolation to maintain binary values
        mask_pil = Image.fromarray((mask_np * 255).astype(np.uint8), mode='L')
        mask_pil = mask_pil.resize(image_pil.size, Image.NEAREST)
        mask_np = np.array(mask_pil) / 255.0 # Convert back to 0-1 float
    
    # Convert image to numpy array (normalize to 0-1 float for calculation)
    image_np = np.array(image_pil).astype(np.float32) / 255.0
    
    # Create a fill color array (normalize to 0-1 float)
    fill_np = np.array(fill_color, dtype=np.float32) / 255.0
    
    # Apply mask: where mask_np is 0, use fill_np, otherwise use image_np
    # This assumes mask_np is 0 for regions to be filled, 1 for regions to keep.
    masked_image_np = image_np * mask_np[:, :, np.newaxis] + (1 - mask_np[:, :, np.newaxis]) * fill_np
    
    # Convert back to PIL Image (scale to 0-255 and convert to uint8)
    masked_image_pil = Image.fromarray((masked_image_np * 255).astype(np.uint8), mode='RGB')
    
    return masked_image_pil


def _get_patch_features(processor, model, image_pil, device):
    """
    Extracts DINO patch features from a PIL image.
    Args:
        processor: The DINO image processor.
        model: The DINO model.
        image_pil: The PIL Image to process.
        device: The torch device.
    Returns:
        tuple: (patch_features_reshaped, grid_h, grid_w) or (None, None, None) on error.
    """
    try:
        inputs = processor(images=image_pil, return_tensors="pt")
        inputs = {k: v.to(device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)
        
        # Get Last Hidden State: (Batch, Seq, Dim)
        feat = outputs.last_hidden_state[0] # Assuming batch size 1
        
        # Calculate grid size from input tensor shape
        h_in, w_in = inputs["pixel_values"].shape[-2:]
        patch_size = 14 # Default for DINOv2, but model name says vit7b16 (likely 16)
        if "16" in MODEL_NAME:
            patch_size = 16
        
        grid_h = h_in // patch_size
        grid_w = w_in // patch_size
        
        expected_tokens = grid_h * grid_w
        
        # Handling CLS token (and potential registers)
        num_tokens = feat.shape[0]
        if num_tokens == expected_tokens + 1: # 1 CLS token
            patch_feat = feat[1:]
        elif num_tokens == expected_tokens + 5: # 1 CLS + 4 Registers (common in DINOv2)
            patch_feat = feat[5:]
        else:
             # Fallback: assume 1 CLS token if unsure, or return None if a mismatch
             # For robustness, we'll try to use the most common case or just slice.
             # A more robust check might involve logging a warning.
             if num_tokens > expected_tokens: # Likely has CLS or registers
                 patch_feat = feat[num_tokens - expected_tokens:]
             else: # Mismatch, return None
                 return None, None, None

        # Reshape to grid for spatial mapping: (Grid_H, Grid_W, Dim)
        if patch_feat.shape[0] == expected_tokens:
             patch_feat = patch_feat.reshape(grid_h, grid_w, -1)
             return patch_feat, grid_h, grid_w
        else:
             return None, None, None

    except Exception as e:
        import traceback
        traceback.print_exc()
        return None, None, None


def _load_mask_tensor(path, device, size=None):
    """
    Loads a mask as a tensor on the specified device.
    Args:
        path: Path to mask image.
        device: Torch device.
        size: Tuple (W, H) to resize to (matching PIL size convention).
    Returns:
        Tensor of shape (1, 1, H, W) with values in [0, 1].
    """
    m = Image.open(path).convert("L") # Convert to grayscale
        
    # Use ToTensor to convert PIL to (C, H, W) tensor, then move to device
    transform = transforms.ToTensor()
    t = transform(m).to(device) # (1, H, W) on GPU
    
    # Add batch dimension: (1, 1, H, W)
    t = t.unsqueeze(0)
    
    if size is not None:
        # PIL size is (W, H), interpolate needs (H, W)
        target_h, target_w = size[1], size[0]
        if t.shape[-2:] != (target_h, target_w):
            t = F.interpolate(t, size=(target_h, target_w), mode='nearest')
            
    return t


def _get_bbox_mask(mask_tensor):
    """
    Given a mask tensor, returns a new tensor with 1s in the bounding box
    of the original mask (aligned to patch size), and 0s elsewhere.
    """
    # Find non-zero indices (i.e., object pixels)
    pts = torch.nonzero(mask_tensor[0, 0] > 0.5, as_tuple=True)
    if len(pts[0]) == 0:
        return torch.zeros_like(mask_tensor) # Return empty mask if no object found
    
    y_min, y_max = pts[0].min().item(), pts[0].max().item()
    x_min, x_max = pts[1].min().item(), pts[1].max().item()
    
    # --- Align to 16x16 grid ---
    patch_size = 16 

    # Round down y_min and x_min to nearest multiple of patch_size
    y_min_exp = (y_min // patch_size) * patch_size
    x_min_exp = (x_min // patch_size) * patch_size
    
    # Round up y_max and x_max to nearest multiple of patch_size, then subtract 1 for inclusive indexing
    y_max_exp = ((y_max + patch_size) // patch_size) * patch_size - 1 
    x_max_exp = ((x_max + patch_size) // patch_size) * patch_size - 1
    
    # Boundary checks
    H, W = mask_tensor.shape[-2:]
    y_min_exp = max(0, y_min_exp)
    y_max_exp = min(H - 1, y_max_exp)
    x_min_exp = max(0, x_min_exp)
    x_max_exp = min(W - 1, x_max_exp)
    
    # Create mask with 1s in the box
    box_mask = torch.zeros_like(mask_tensor)
    box_mask[:, :, y_min_exp : y_max_exp + 1, x_min_exp : x_max_exp + 1] = 1.0
    
    return box_mask


# Helper to calculate cosine similarity between two tensor embeddings
def _cosine_similarity_embeddings(emb_a, emb_b):
    sim = F.cosine_similarity(emb_a, emb_b, dim=-1)
    if sim.dim() > 0:
        return sim.mean().item()
    return sim.item()


def audit_item(processor, model, img_before, img_after, item_dir, item_idx, device):
    result = {"item_idx": item_idx, "results": {}}
    
    t0 = time.time()
    
    if img_before.size != img_after.size:
        img_after = img_after.resize(img_before.size, Image.LANCZOS)

    # 1. Pre-fill structure
    # Global background check (using the union of ADD and REMOVE bboxes)
    # We will compute the global background request dynamically later
    result["results"]["global"] = {
        "background_bbox_sim": None,
        "background_changed": None,
        "add_mask_path": None,
        "remove_mask_path": None
    }

    for kind in ["remove", "add"]:
        # Check for specific masks
        kind_merged_path = os.path.join(item_dir, f"item_{item_idx}_MASK_{kind.upper()}.png")
        if os.path.exists(kind_merged_path):
             masks_dir = os.path.join(item_dir, f"final_masks_{kind}")
             result["results"][kind] = {
                "kind_merged_mask_path": kind_merged_path,
                "sub_masks_dir": masks_dir if os.path.isdir(masks_dir) else None,
                "sub_mask_results": []
             }
             # Store path in global for reference
             if kind == "add":
                 result["results"]["global"]["add_mask_path"] = kind_merged_path
             else:
                 result["results"]["global"]["remove_mask_path"] = kind_merged_path

    # 2. Extract Features (ONCE for original images)
    # Use the new helper function
    patch_feat_a, grid_h, grid_w = _get_patch_features(processor, model, img_before, device)
    patch_feat_b, _, _ = _get_patch_features(processor, model, img_after, device)

    if patch_feat_a is None or patch_feat_b is None:
        print(f"Warning: Could not extract features for item {item_idx}. Skipping.")
        return result

    # Helper to calculate similarity for a mask (now takes features as args)
    def process_mask(current_patch_feat_a, current_patch_feat_b, current_grid_h, current_grid_w, mask_input, mask_type="sub_mask", return_map=False):
        try:
            # Prepare mask tensor (1, 1, H, W)
            if isinstance(mask_input, str):
                # Load from path
                mask_tensor = _load_mask_tensor(mask_input, device, size=img_before.size) # Pass img_before.size to load mask at correct initial size
            elif torch.is_tensor(mask_input):
                mask_tensor = mask_input
            else:
                return None if not return_map else (None, None)
            
            # Resize mask to Feature Grid Size: (Gh, Gw)
            # interpolate expects (Batch, Channels, H, W) -> mask_tensor is already (1, 1, H, W)
            mask_small = F.interpolate(mask_tensor, size=(current_grid_h, current_grid_w), mode='bilinear', align_corners=False)
            
            # Create boolean mask (threshold)
            keep_mask = (mask_small[0, 0] > 0.4) # Slightly lower threshold for soft resizing
            
            if keep_mask.sum() == 0:
                return None if not return_map else (None, None)
                
            # Compute dense similarity map if requested or needed
            if return_map:
                # Dense cosine similarity (Grid_H, Grid_W)
                sim_map = F.cosine_similarity(current_patch_feat_a, current_patch_feat_b, dim=-1)
                # Apply mask to get average
                avg_sim = sim_map[keep_mask].mean().item()
                return avg_sim, sim_map
            else:
                # Average Pooling
                emb_a = current_patch_feat_a[keep_mask].mean(dim=0)
                emb_b = current_patch_feat_b[keep_mask].mean(dim=0)
                return _cosine_similarity_embeddings(emb_a, emb_b)
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None if not return_map else (None, None)


    # 3. Process Requests (Masks)
    
    # Initialize total_pixel_mask for global background (will accumulate ALL modified regions pixel-wise)
    total_pixel_mask = None

    # Process sub-masks first to filter out unmodified objects
    for kind in ["remove", "add"]:
        has_valid_submasks = False

        if kind in result["results"]:
            data = result["results"][kind]
            
            # Check for sub-masks
            if data["sub_masks_dir"] and os.path.isdir(data["sub_masks_dir"]):
                sub_masks = sorted(glob.glob(os.path.join(data["sub_masks_dir"], "*.png")))
                if sub_masks:
                    has_valid_submasks = True
                    for sm in sub_masks:
                        # Load tensor
                        m_tensor = _load_mask_tensor(sm, device, size=img_before.size)
                        # Check similarity using original image features
                        sim = process_mask(patch_feat_a, patch_feat_b, grid_h, grid_w, m_tensor, "sub_mask")
                        
                        if sim is not None:
                            data["sub_mask_results"].append({
                                "mask_path": sm,
                                "cos_sim": sim,
                                "object_unchanged": sim >= OBJECT_SIM_THRESHOLD
                            })
                            
                            # Only add to union if MODIFIED (sim < Threshold)
                            if sim < OBJECT_SIM_THRESHOLD:
                                # Accumulate precise mask
                                if total_pixel_mask is None:
                                    total_pixel_mask = m_tensor
                                else:
                                    total_pixel_mask = torch.max(total_pixel_mask, m_tensor)

            # Fallback: If no sub-masks found, use the merged mask (assume modified)
            if not has_valid_submasks:
                merged_path = data["kind_merged_mask_path"]
                if merged_path and os.path.exists(merged_path):
                     m_tensor = _load_mask_tensor(merged_path, device, size=img_before.size)
                     # Accumulate precise mask
                     if total_pixel_mask is None:
                         total_pixel_mask = m_tensor
                     else:
                         total_pixel_mask = torch.max(total_pixel_mask, m_tensor)

    # Combine into global exclusion mask (One BBox of the Union of Masks)
    union_mask = None
    if total_pixel_mask is not None:
        union_mask = _get_bbox_mask(total_pixel_mask)

    # --- Global Background Logic ---
    # At this point, 'union_mask' represents the combined EXCLUSION zones (expanded object BBoxes).
    if union_mask is not None:
        # The background mask is simply the inverse of the exclusion mask.
        bg_mask_tensor = 1.0 - union_mask
        
        # Ensure bg_mask_tensor is not all zeros (meaning union_mask didn't cover the whole image)
        # If the entire image is excluded by expanded bboxes, then there's no background to audit.
        if bg_mask_tensor.sum() == 0: 
            bg_mask_tensor = None 
        
        if bg_mask_tensor is not None:
             # Calculate percentage of image covered by background mask
             # bg_mask_tensor is (1, 1, H, W) with 0s and 1s
             bg_pixel_count = (bg_mask_tensor > 0.5).sum().item()
             total_pixel_count = bg_mask_tensor.numel()
             bg_ratio = bg_pixel_count / total_pixel_count if total_pixel_count > 0 else 0
             result["results"]["global"]["bg_mask_ratio"] = bg_ratio

             # Apply background mask to original images *before* feature extraction
             masked_img_before = _apply_mask_to_image(img_before, bg_mask_tensor)
             masked_img_after = _apply_mask_to_image(img_after, bg_mask_tensor)

             # Extract features from the masked images for background comparison
             bg_patch_feat_a, _, _ = _get_patch_features(processor, model, masked_img_before, device)
             bg_patch_feat_b, _, _ = _get_patch_features(processor, model, masked_img_after, device)

             if bg_patch_feat_a is not None and bg_patch_feat_b is not None:
                 # Calculate similarity using features from masked images
                 sim, sim_map = process_mask(bg_patch_feat_a, bg_patch_feat_b, grid_h, grid_w, bg_mask_tensor, "background", return_map=True)
                 
                 if sim is not None:
                     result["results"]["global"]["background_bbox_sim"] = sim
                     is_changed = sim < BACKGROUND_SIM_THRESHOLD
                     result["results"]["global"]["background_changed"] = is_changed
                     
                     # If changed, save a debug mask visualization
                     if is_changed and sim_map is not None:
                         try:
                             # sim_map is (Grid_H, Grid_W) on device
                             # 1. Resize background mask to grid size to mask out object area in visualization
                             bg_mask_small = F.interpolate(bg_mask_tensor, size=(grid_h, grid_w), mode='nearest')
                             bg_bool = (bg_mask_small[0,0] > 0.5)
                             
                             # 2. Filter sim_map: Keep only background area, set rest to high similarity (1.0)
                             # We want to highlight LOW similarity in the BACKGROUND.
                             vis_map = torch.ones_like(sim_map) # Default 1.0 (White)
                             vis_map[bg_bool] = sim_map[bg_bool]
                             
                             # 3. Convert to Image (0..1 -> 0..255)
                             vis_map = torch.clamp(vis_map, 0, 1)
                             vis_np = (vis_map.cpu().numpy() * 255).astype(np.uint8)
                             vis_img = Image.fromarray(vis_np, mode='L')
                             
                             # Resize back to original image size
                             vis_img = vis_img.resize(img_before.size, Image.NEAREST)
                             
                             # Save
                             fail_mask_path = os.path.join(DEST_DIR, f"item_{item_idx}_bg_fail_mask.png")
                             vis_img.save(fail_mask_path)
                             result["results"]["global"]["fail_mask_path"] = fail_mask_path
                         except Exception as e:
                             import traceback
                             traceback.print_exc()

    t_end = time.time()
    return result

class AuditDataset(Dataset):
    def __init__(self, items):
        self.items = items

    def __len__(self):
        return len(self.items)

    def __getitem__(self, idx):
        item = self.items[idx].copy()
        try:
            # Preload images
            item["img_before_obj"] = _load_image(item["img_before"])
            item["img_after_obj"] = _load_image(item["img_after"])
            item["load_success"] = True
        except Exception as e:
            item["load_error"] = str(e)
            item["load_success"] = False
        return item


def collate_fn(batch):
    return batch[0]


def parse_log_file(log_path):
    try:
        with open(log_path, "r") as f:
            data = json.load(f)
        item_idx = data.get("item_idx")
        item_dir = os.path.dirname(log_path)
        if "original_item" not in data:
            return None
        img_before_path = data["original_item"].get("local_input_image")
        img_after_path = data["original_item"].get("output_image")
        if not (img_before_path and img_after_path and os.path.exists(img_before_path) and os.path.exists(img_after_path)):
            return None
        return {
            "log_path": log_path,
            "item_idx": item_idx,
            "item_dir": item_dir,
            "img_before": img_before_path,
            "img_after": img_after_path,
        }
    except Exception:
        return None


def thread_worker(rank, devices, chunks):
    device = devices[rank]
    items = chunks[rank]
    
    # Determine position for tqdm based on device index (rank)
    idx = rank
    
    model = None
    processor = None
    try:
        processor = AutoImageProcessor.from_pretrained(MODEL_NAME)
        model = AutoModel.from_pretrained(MODEL_NAME, torch_dtype=torch.float16)
        model.to(device)
        model.eval()
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return

    # Create Dataset and DataLoader
    dataset = AuditDataset(items)
    
    # Optimize DataLoader for faster I/O
    # Calculate appropriate num_workers per GPU process
    total_cores = os.cpu_count() or 4
    workers_per_gpu = max(1, min(4, total_cores // len(devices)))
    
    loader = DataLoader(
        dataset, 
        batch_size=1, 
        shuffle=False, 
        num_workers=workers_per_gpu, 
        collate_fn=collate_fn,
        prefetch_factor=2,
        persistent_workers=True,
        pin_memory=True if torch.cuda.is_available() else False
    )

    # Process items
    for item in tqdm(loader, desc=f"GPU {idx}", position=idx, leave=True):
        try:
            if not item.get("load_success", False):
                continue

            img_before = item["img_before_obj"]
            img_after = item["img_after_obj"]

            audit = {
                "item_idx": item["item_idx"],
                "log_path": item["log_path"],
                "before_image": item["img_before"],
                "after_image": item["img_after"],
            }
            # Pass the processor and model instance
            audit.update(audit_item(processor, model, img_before, img_after, item["item_dir"], item["item_idx"], device))

            audit_path = os.path.join(DEST_DIR, f"item_{item['item_idx']}_dino_audit.json")
            with open(audit_path, "w") as f:
                json.dump(audit, f, indent=2)
        except Exception as e:
            import traceback
            traceback.print_exc()


def main():
    devices = get_available_devices()
    print(f"Running on {len(devices)} devices: {devices}")

    # Find all log files
    log_files = glob.glob(os.path.join(WORK_DIR, "item_*", "item_*_log.json"))
    log_files += [p for p in glob.glob(os.path.join(WORK_DIR, "item_*_log.json")) if p not in log_files]
    log_files = sorted(log_files)
    if MAX_ITEMS:
        log_files = log_files[:MAX_ITEMS]

    if not log_files:
        print(f"No log files found in {WORK_DIR}. Exiting.")
        return

    os.makedirs(DEST_DIR, exist_ok=True)
    print(f"Processing {len(log_files)} items from {WORK_DIR}. Saving audit to {DEST_DIR}")

    # Parse dataset upfront (fast enough for filenames)
    all_items = []
    print("Parsing logs...")
    
    # Use ProcessPoolExecutor to parse logs in parallel
    # Adjust max_workers as needed, usually cpu_count is good
    num_cpus = os.cpu_count() or 4
    with ProcessPoolExecutor(max_workers=num_cpus) as executor:
        futures = [executor.submit(parse_log_file, log_path) for log_path in log_files]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Parsing Logs"):
            result = future.result()
            if result:
                all_items.append(result)

    if not all_items:
        print("No valid items found after parsing. Exiting.")
        return

    # Multi-threading
    chunks = [[] for _ in range(len(devices))]
    for i, item in enumerate(all_items):
        chunks[i % len(devices)].append(item)

    print(f"Spawning {len(devices)} processes with mp.spawn...")
    
    # Use mp.spawn which handles process lifecycle better
    # Note: mp.spawn passes 'i' (rank) as the first argument automatically
    mp.spawn(thread_worker, args=(devices, chunks), nprocs=len(devices), join=True)

    print(f"Finished. Audit files saved to {DEST_DIR}.")


if __name__ == "__main__":
    main()