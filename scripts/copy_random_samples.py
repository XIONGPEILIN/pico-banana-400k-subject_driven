import os
import glob
import random
import shutil
import json

# Config
SOURCE_LOG_DIR = "openimages/agent_full_pipeline_merged_ALL"
DEST_DIR = "test/random_100_samples"
NUM_SAMPLES = 100

def main():
    # 1. Get all log files
    log_patterns = os.path.join(SOURCE_LOG_DIR, "item_*_log.json")
    all_logs = glob.glob(log_patterns)
    
    total_files = len(all_logs)
    print(f"Found {total_files} total log files.")
    
    if total_files == 0:
        print("No log files found. Exiting.")
        return

    # 2. Random Sample
    if total_files < NUM_SAMPLES:
        print(f"Total files ({total_files}) is less than requested samples ({NUM_SAMPLES}). Copying all.")
        selected_logs = all_logs
    else:
        selected_logs = random.sample(all_logs, NUM_SAMPLES)
        print(f"Randomly selected {NUM_SAMPLES} files.")

    # 3. Copy Files
    success_count = 0
    
    for log_path in selected_logs:
        try:
            with open(log_path, 'r') as f:
                data = json.load(f)
            
            item_idx = data.get("item_idx")
            if item_idx is None:
                # Try to extract from filename if missing in json
                basename = os.path.basename(log_path)
                item_idx = basename.split('_')[1]

            # file paths from log
            # Note: local_input_image path in log might be relative to project root
            input_img_path = data.get("original_item", {}).get("local_input_image")
            output_img_path = data.get("original_item", {}).get("output_image")
            
            # Construct Mask paths (in the same dir as log)
            mask_remove = os.path.join(SOURCE_LOG_DIR, f"item_{item_idx}_MASK_REMOVE.png")
            mask_add = os.path.join(SOURCE_LOG_DIR, f"item_{item_idx}_MASK_ADD.png")
            
            # --- Copy Operations ---
            
            # 1. Log
            shutil.copy(log_path, os.path.join(DEST_DIR, f"item_{item_idx}_log.json"))
            
            # 2. Input Image
            if input_img_path and os.path.exists(input_img_path):
                ext = os.path.splitext(input_img_path)[1]
                shutil.copy(input_img_path, os.path.join(DEST_DIR, f"item_{item_idx}_input{ext}"))
            
            # 3. Output Image
            if output_img_path and os.path.exists(output_img_path):
                ext = os.path.splitext(output_img_path)[1]
                shutil.copy(output_img_path, os.path.join(DEST_DIR, f"item_{item_idx}_output{ext}"))
            
            # 4. Mask
            if os.path.exists(mask_remove):
                shutil.copy(mask_remove, os.path.join(DEST_DIR, f"item_{item_idx}_mask.png"))
            elif os.path.exists(mask_add):
                shutil.copy(mask_add, os.path.join(DEST_DIR, f"item_{item_idx}_mask.png"))
                
            success_count += 1
            
        except Exception as e:
            print(f"Error processing {log_path}: {e}")

    print(f"Successfully copied {success_count} sample sets to {DEST_DIR}")

if __name__ == "__main__":
    main()
