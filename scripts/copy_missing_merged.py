import os
import glob
import shutil
import json

DEST_DIR = "test/random_100_analyzed"
SOURCE_DIR = "openimages/agent_full_pipeline_merged_ALL"

def main():
    # 1. Find existing logs in the destination folder to identify which items we have
    log_files = glob.glob(os.path.join(DEST_DIR, "item_*_log.json"))
    
    if not log_files:
        print(f"No log files found in {DEST_DIR}. Nothing to copy.")
        return
        
    print(f"Found {len(log_files)} items in {DEST_DIR}. Checking for MERGED images...")
    
    copied_count = 0
    
    for log_path in log_files:
        # Extract item_idx from filename: item_{idx}_log.json
        basename = os.path.basename(log_path)
        try:
            # item_123_log.json -> 123
            item_idx = basename.split('_')[1]
        except IndexError:
            print(f"Skipping malformed filename: {basename}")
            continue
            
        # Construct source path for MERGED image
        src_merged = os.path.join(SOURCE_DIR, f"item_{item_idx}_MERGED.png")
        
        if os.path.exists(src_merged):
            dst_merged = os.path.join(DEST_DIR, f"item_{item_idx}_MERGED.png")
            shutil.copy(src_merged, dst_merged)
            copied_count += 1
        else:
            # Optional: print if missing
            # print(f"Merged image not found for item {item_idx}")
            pass
            
    print(f"Successfully copied {copied_count} MERGED images to {DEST_DIR}.")

if __name__ == "__main__":
    main()
