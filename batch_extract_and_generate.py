import argparse
import json
import torch
import numpy as np
from diffusers import FluxKontextPipeline
from transformers import Sam2Processor, Sam2Model
from PIL import Image
from pathlib import Path
from tqdm import tqdm
import os
import torch.multiprocessing as mp
import random # Import random module for dynamic seeding

# --- Helper Functions (adapted from view_bbox.ipynb) ---

def _bbox_to_pixels(coords, w, h):
    """Converts bounding box from various formats to absolute pixel coordinates."""
    if not coords or len(coords) != 4:
        return None
    max_c = max(coords)
    if max_c <= 1.5:
        norm = coords
    elif max_c <= 1000:
        norm = [c / 1000.0 for c in coords]
    else:
        left, top, right, bottom = coords
        return (int(max(0, left)), int(max(0, top)), int(min(right, w)), int(min(bottom, h)))
    
    left, top, right, bottom = [int(norm[0] * w), int(norm[1] * h), int(norm[2] * w), int(norm[3] * h)]
    return (int(max(0, left)), int(max(0, top)), int(min(right, w)), int(min(bottom, h)))


def get_masks_from_objects(objects, image, sam_model, sam_processor, device):
    """Processes a list of objects on a given image to generate segmentation masks using SAM2."""
    if not objects:
        return []
        
    all_points_per_object = []
    all_labels_per_object = []

    for obj in objects:
        points_from_llm = obj.get('region_points')
        # Expecting points_from_llm to be a list of 3 [x, y] pixel coordinates
        if not points_from_llm or len(points_from_llm) != 3:
            continue
        
        points_for_sam = [[float(px), float(py)] for px, py in points_from_llm]
        labels_for_sam = [1] * len(points_for_sam) # All foreground points

        all_points_per_object.append(points_for_sam)
        all_labels_per_object.append(labels_for_sam)

    if not all_points_per_object:
        return []

    inputs = sam_processor(images=image, input_points=[all_points_per_object], input_labels=[all_labels_per_object], return_tensors='pt')
    
    for k, v in inputs.items():
        if isinstance(v, torch.Tensor):
            inputs[k] = v.to(device)

    with torch.no_grad():
        outputs = sam_model(**inputs)

    pred_masks = sam_processor.post_process_masks(outputs.pred_masks.cpu(), inputs["original_sizes"])[0]
    iou_scores = outputs.iou_scores.cpu().numpy()

    best_masks = []
    for i in range(pred_masks.shape[0]):
        best_mask_idx = np.argmax(iou_scores[0, i])
        best_masks.append(pred_masks[i, best_mask_idx].numpy())
        
    return best_masks

def extract_object_from_image(image, masks):
    """Extracts object(s) from an image using merged masks and returns the cropped object and the full-size merged mask."""
    if not masks:
        return None, None

    img_array = np.array(image)
    h, w = img_array.shape[:2]
    
    merged_mask = np.zeros((h, w), dtype=bool)
    for mask in masks:
        # Ensure mask is boolean
        mask_bool = mask.astype(bool)
        if mask_bool.shape != (h,w):
            print(f"Warning: Mask shape {mask_bool.shape} does not match image shape {(h,w)}. Resizing.")
            pil_mask = Image.fromarray(mask_bool)
            pil_mask = pil_mask.resize((w,h), Image.NEAREST)
            mask_bool = np.array(pil_mask)
        merged_mask = merged_mask | mask_bool

    extracted_array = np.full((h, w, 3), [0, 0, 255], dtype=np.uint8) # RGB, blue background
    extracted_array[merged_mask] = img_array[merged_mask]

    mask_coords = np.argwhere(merged_mask)
    if len(mask_coords) == 0:
        return None, None

    top, left = mask_coords.min(axis=0)
    bottom, right = mask_coords.max(axis=0)
    
    padding = 10
    top = max(0, top - padding)
    left = max(0, left - padding)
    bottom = min(h, bottom + padding + 1)
    right = min(w, right + padding + 1)
    
    cropped_array = extracted_array[top:bottom, left:right]

    return Image.fromarray(cropped_array), merged_mask

# --- Worker Function for Multiprocessing ---

def worker_process(process_id, data_chunk, num_gpus, top_down_prompt, subject_dir, overwrite):
    """The main worker function that runs in its own process."""
    device_id = process_id % num_gpus
    device = f"cuda:{device_id}"
    
    # Define output directories within the worker
    top_down_output_dir = subject_dir / "top_down_output"
    masks_output_dir = subject_dir / "masks_output"
    
    print(f"Process {process_id} starting on device: {device}")

    # Each process loads its own model instances onto its assigned GPU
    pipe = FluxKontextPipeline.from_pretrained("black-forest-labs/FLUX.1-Kontext-dev", torch_dtype=torch.bfloat16).to(device)
    # pipe.load_lora_weights("prithivMLmods/Kontext-Top-Down-View", weight_name="Kontext-Top-Down-View.safetensors", adapter_name="top-down")
    # pipe.set_adapters(["top-down"], adapter_weights=[1.0])
    
    sam_model = Sam2Model.from_pretrained("facebook/sam2.1-hiera-large").to(device)
    sam_processor = Sam2Processor.from_pretrained("facebook/sam2.1-hiera-large")
    
    for item in tqdm(data_chunk, desc=f"Process {process_id} on {device}", position=process_id):
        idx = item['original_index']
        objects = item.get('bbox', {}).get('objects')

        if not objects:
            continue

        has_remove = any(o.get('is_remove') for o in objects)
        has_add = any(not o.get('is_remove') for o in objects)

        img_to_generate = None
        img_path_for_generation = None
        
        # Generate a new random seed for each image to ensure varied backgrounds
        seed = random.randint(0, 2**32 - 1)
        generator = torch.Generator(device=device).manual_seed(seed)

        try:
            # --- Branching Logic based on is_remove flags ---
            if has_remove and has_add: # REPLACE
                before_path_str = item.get('local_input_image')
                if before_path_str and Path(before_path_str).exists():
                    before_img = Image.open(before_path_str).convert("RGB")
                    removed_objs = [o for o in objects if o.get('is_remove')]
                    before_masks = get_masks_from_objects(removed_objs, before_img, sam_model, sam_processor, device)
                    _, before_merged_mask = extract_object_from_image(before_img, before_masks)
                    if before_merged_mask is not None:
                        Image.fromarray((before_merged_mask * 255).astype(np.uint8)).save(masks_output_dir / f"idx_{idx}_before_mask.png")

                after_path_str = item.get('output_image')
                if not after_path_str or not Path(after_path_str).exists(): continue
                after_img = Image.open(after_path_str).convert("RGB")
                added_objs = [o for o in objects if not o.get('is_remove')]
                after_masks = get_masks_from_objects(added_objs, after_img, sam_model, sam_processor, device)
                extracted_obj_img, after_merged_mask = extract_object_from_image(after_img, after_masks)
                if extracted_obj_img and after_merged_mask is not None:
                    Image.fromarray((after_merged_mask * 255).astype(np.uint8)).save(masks_output_dir / f"idx_{idx}_after_mask.png")
                    img_to_generate = extracted_obj_img
                    img_path_for_generation = Path(after_path_str)

            elif has_remove: # REMOVE ONLY
                before_path_str = item.get('local_input_image')
                if not before_path_str or not Path(before_path_str).exists(): continue
                image = Image.open(before_path_str).convert("RGB")
                removed_objs = [o for o in objects if o.get('is_remove')]
                masks = get_masks_from_objects(removed_objs, image, sam_model, sam_processor, device)
                extracted_obj_img, merged_mask = extract_object_from_image(image, masks)
                if extracted_obj_img and merged_mask is not None:
                    Image.fromarray((merged_mask * 255).astype(np.uint8)).save(masks_output_dir / f"idx_{idx}_before_mask.png")
                    img_to_generate = extracted_obj_img
                    img_path_for_generation = Path(before_path_str)

            elif has_add: # ADD ONLY
                after_path_str = item.get('output_image')
                if not after_path_str or not Path(after_path_str).exists(): continue
                image = Image.open(after_path_str).convert("RGB")
                added_objs = [o for o in objects if not o.get('is_remove')]
                masks = get_masks_from_objects(added_objs, image, sam_model, sam_processor, device)
                extracted_obj_img, merged_mask = extract_object_from_image(image, masks)
                if extracted_obj_img and merged_mask is not None:
                    Image.fromarray((merged_mask * 255).astype(np.uint8)).save(masks_output_dir / f"idx_{idx}_after_mask.png")
                    img_to_generate = extracted_obj_img
                    img_path_for_generation = Path(after_path_str)
            
            # --- Generation Step ---
            if img_to_generate and img_path_for_generation:
                output_path = top_down_output_dir / f"idx_{idx}_{img_path_for_generation.stem}_generated.png"
                if not overwrite and output_path.exists():
                    continue

                img_to_generate_rgb = img_to_generate.convert("RGB")
                
                # Save the input image to the pipe for debugging/verification
                input_to_pipe_path = top_down_output_dir / f"idx_{idx}_{img_path_for_generation.stem}_input_to_pipe.png"
                img_to_generate_rgb.save(input_to_pipe_path)
                
                prompt = top_down_prompt 

                generated_image = pipe(
                    image=img_to_generate_rgb, 
                    prompt=prompt,
                    guidance_scale=2.5,
                    width=1024,
                    height=1024,
                    num_inference_steps=50,
                    generator=generator, # Use the dynamically generated seed
                ).images[0]
                
                generated_image.save(output_path)

        except Exception as e:
            print(f"Process {process_id} FAILED on item original_index {idx}: {e}")
            continue

# --- Main Logic ---

def main():
    parser = argparse.ArgumentParser(description="Batch process images for generation.")
    parser.add_argument('--no-overwrite', dest='overwrite', action='store_false', help='Do not overwrite existing generated files (default: overwrite).')
    args = parser.parse_args()

    num_gpus = torch.cuda.device_count()
    if num_gpus == 0:
        print("No CUDA devices found. Exiting.")
        return
        
    print(f"Found {num_gpus} CUDA devices.")
    num_processes = num_gpus

    print("Loading data from bbox_results.json...")
    with open('bbox_results.json', 'r', encoding='utf-8') as f:
        all_data = json.load(f)
    
    # --- Filtering Logic ---
    print("Filtering data based on confidence score (9 or 10) and presence of 3 region points...")
    filtered_data = []
    for i, item in tqdm(enumerate(all_data), desc="Filtering", total=len(all_data)):
        confidence = item.get('bbox', {}).get('confidence')
        if confidence not in [9, 10]:
            continue
        
        # We are no longer filtering by bbox area ratio as the LLM will not provide bboxes.
        # Ensure there are objects with region_points to process
        objects = item.get('bbox', {}).get('objects')
        if not objects:
            continue

        all_points_exist_and_correct_count = True
        for obj in objects:
            region_points = obj.get('region_points')
            if not region_points or len(region_points) != 3:
                all_points_exist_and_correct_count = False
                break
        
        if all_points_exist_and_correct_count:
            filtered_data.append(item)
    
    # Use the filtered data for processing
    all_data = filtered_data
    
    # Assign original_index AFTER filtering to ensure it's contiguous for workers
    for i, item in enumerate(all_data):
        item['original_index'] = i
    
    print(f"Processing a total of {len(all_data)} images after filtering.")

    # Create output directories
    SUBJECT_DIR = Path("openimages")
    SUBJECT_DIR.mkdir(exist_ok=True)
    (SUBJECT_DIR / "top_down_output").mkdir(exist_ok=True)
    (SUBJECT_DIR / "masks_output").mkdir(exist_ok=True)

    # Revert top_down_prompt to a more general "natural background"
    # top_down_prompt = "[photo content], from a top-down perspective. Place it in a realistic and fitting environment, ensuring the background is consistent with the object. Maintain accurate lighting, proportions, and shadows."
    top_down_prompt = "Place it in a realistic and fitting environment, ensuring the background is consistent with the object. Maintain accurate lighting, proportions, and shadows."
    
    data_chunks = [[] for _ in range(num_processes)]
    for i, item in enumerate(all_data):
        data_chunks[i % num_processes].append(item)

    # Use 'spawn' start method for CUDA compatibility
    mp.set_start_method('spawn', force=True)
    processes = []
    
    for i in range(num_processes):
        process = mp.Process(target=worker_process, args=(
            i, data_chunks[i], num_gpus, top_down_prompt, SUBJECT_DIR, args.overwrite
        ))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
    
    print("\n--- Batch processing complete. ---")
    print(f"Generated images are in '{SUBJECT_DIR / 'top_down_output'}'.")
    print(f"Extracted masks are in '{SUBJECT_DIR / 'masks_output'}'.")

if __name__ == "__main__":
    main()
