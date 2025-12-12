import os
import json
import glob
import re
import torch
from PIL import Image
from tqdm import tqdm
import random # For random seeds
from diffsynth.pipelines.qwen_image import QwenImagePipeline, ModelConfig

# --- Configuration ---
# Update this to match your actual directory pattern
LOG_DIR_PATTERN = "openimages/pico_sam_output_ALL_*" 
OUTPUT_DIR = "openimages/qwen_minimal_output"
DEVICE = "cuda"

# Number of variations to generate per subject-image pair
NUM_VARIATIONS_PER_SUBJECT = 3

# Viewpoints to randomly select from
VIEWPOINTS = [
    "A top-down view of",
    "A side view of",
    "A low-angle view of",
    "A close-up view of",
    "An isometric view of",
]

def clean_subject_prompt(prompt):
    """
    Removes spatial descriptions to get the core object name.
    E.g., "a red hat on the left" -> "a red hat"
    """
    # Remove common spatial phrases
    spatial_phrases = [
        r"on the left side of the image", r"on the right side of the image", r"in the center of the image", 
        r"at the bottom of the image", r"at the top of the image", r"on the left", r"on the right",
        r"in the center", r"at the bottom", r"at the top"
    ]
    cleaned = prompt
    for pattern in spatial_phrases:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE).strip()
    return cleaned

def load_data(log_dir_pattern):
    """Loads all relevant log files and extracts subjects and image paths."""
    # Try to find the specific directory mentioned by user first for safety, else generic
    specific_dir = "openimages/pico_sam_output_ALL_20251206_032609"
    if os.path.exists(specific_dir):
        base_dir = specific_dir
    else:
        dirs = sorted(glob.glob(log_dir_pattern))
        if not dirs:
            print("No log directories found.")
            return []
        base_dir = dirs[-1] # Use the most recent directory
        print(f"Using latest directory: {base_dir}")

    log_files = glob.glob(os.path.join(base_dir, "item_*", "*_log.json"))
    items = []
    
    for log_file in log_files:
        try:
            with open(log_file, 'r') as f:
                data = json.load(f)
            
            item_idx = data.get("item_idx")
            original_item = data.get("original_item", {})
            input_image_path = original_item.get("local_input_image")
            
            if not input_image_path or not os.path.exists(input_image_path):
                print(f"Skipping item {item_idx}: Input image not found at {input_image_path}")
                continue

            change_concepts = data.get("change_concepts", [])
            subjects_to_generate = [] # Subjects that we want to re-generate (added objects) 
            
            for concept in change_concepts:
                total_prompt = concept.get("total_prompt", "").strip()
                is_remove = concept.get("is_remove", False)
                
                # We are interested in generating *new* images of the added objects.
                # If is_remove=False, it means this object was *added* to the image,
                # so it represents the target object we want to re-contextualize.
                if not is_remove and total_prompt:
                    cleaned = clean_subject_prompt(total_prompt)
                    if cleaned:
                        subjects_to_generate.append(cleaned)
            
            # Remove duplicates for subjects within the same item
            subjects_to_generate = list(set(subjects_to_generate))
            
            if subjects_to_generate:
                items.append({
                    "item_idx": item_idx,
                    "input_image_path": input_image_path,
                    "subjects": subjects_to_generate,
                    "original_item": original_item # Store original_item here to avoid re-reading
                })
            
        except Exception as e:
            print(f"Error reading {log_file}: {e}")
            
    return items

def main():
    # 1. Implement data loading and subject cleaning (remove spatial terms)
    items = load_data(LOG_DIR_PATTERN)
    if not items:
        print("No items to process. Exiting.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 2. Implement model initialization
    vram_config = {
        "offload_dtype": "disk", "offload_device": "disk",
        "onload_dtype": torch.float8_e4m3fn, "onload_device": "cpu",
        "preparing_dtype": torch.float8_e4m3fn, "preparing_device": "cuda",
        "computation_dtype": torch.bfloat16, "computation_device": "cuda",
    }
    
    print("Loading Qwen-Image-Edit pipeline...")
    try:
        pipe = QwenImagePipeline.from_pretrained(
            torch_dtype=torch.bfloat16,
            device=DEVICE,
            model_configs=[
                ModelConfig(model_id="Qwen/Qwen-Image-Edit", origin_file_pattern="transformer/diffusion_pytorch_model*.safetensors", **vram_config),
                ModelConfig(model_id="Qwen/Qwen-Image", origin_file_pattern="text_encoder/model*.safetensors", **vram_config),
                ModelConfig(model_id="Qwen/Qwen-Image", origin_file_pattern="vae/diffusion_pytorch_model.safetensors", **vram_config),
            ],
            processor_config=ModelConfig(model_id="Qwen/Qwen-Image-Edit", origin_file_pattern="processor/"),
            vram_limit=torch.cuda.mem_get_info(DEVICE)[1] / (1024 ** 3) - 0.5,
        )
    except Exception as e:
        print(f"Failed to load model: {e}")
        import traceback
        traceback.print_exc()
        return

    # 3. Implement inference loop
    for item in tqdm(items, desc="Processing Items"):
        item_idx = item['item_idx']
        # The 'input_image_path' from load_data is the 'local_input_image' (Before image).
        # We need the 'output_image' (After image) if the subject was added (is_remove=False).
        # The 'original_item' was stored in the 'items' list during load_data.
        original_item_data = item['original_item']
        input_image_for_pipe_path = original_item_data.get("output_image") # Object we want to regenerate is here

        if not input_image_for_pipe_path or not os.path.exists(input_image_for_pipe_path):
            print(f"Skipping item {item_idx}: Output image for pipe not found at {input_image_for_pipe_path}")
            continue

        try:
            base_image_for_pipe = Image.open(input_image_for_pipe_path).convert("RGB")
        except Exception as e:
            print(f"Error loading image {input_image_for_pipe_path} for pipe: {e}")
            continue

        for subject in item['subjects']: # Iterate through each unique subject found in the item
            subject_slug = re.sub(r'[^a-zA-Z0-9_]', '_', subject)[:50] # Sanitize for filename

            for i in range(NUM_VARIATIONS_PER_SUBJECT):
                viewpoint = random.choice(VIEWPOINTS)
                random_seed = random.randint(0, 2**32 - 1)
                
                # Construct the minimalist prompt
                final_prompt = f"{viewpoint} {subject}, high quality, realistic"
                
                output_filename = f"item_{item_idx}_{subject_slug}_var{i}_{viewpoint.split(' ')[1]}.jpg" # Simplified viewpoint for filename
                output_path = os.path.join(OUTPUT_DIR, output_filename)
                
                if os.path.exists(output_path): # Skip if already generated
                    continue

                try:
                    # The QwenImagePipeline expects 'edit_image' to be the image to edit.
                    # If the subject was added (is_remove=False), the 'output_image' contains it.
                    generated_image = pipe(
                        prompt=final_prompt, 
                        edit_image=base_image_for_pipe, # Input image containing the subject
                        seed=random_seed, 
                        num_inference_steps=50, 
                        height=1024, 
                        width=1024, 
                        edit_image_auto_resize=True
                    )
                    generated_image.save(output_path)
                    
                    # Save prompt for reference
                    with open(output_path.replace(".jpg", ".txt"), "w") as f:
                        f.write(final_prompt)
                        f.write(f"\nSeed: {random_seed}")
                        f.write(f"\nInput Image: {input_image_for_pipe_path}")
                        
                except Exception as e:
                    print(f"Error processing item {item_idx}, subject '{subject}', variation {i}: {e}")
                    import traceback
                    traceback.print_exc()

    print(f"Processing complete. Results saved to {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
