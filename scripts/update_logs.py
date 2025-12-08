import json
import os
import glob
from tqdm import tqdm

# Paths
# Assuming running from project root
LOG_DIR = "openimages/agent_full_pipeline_merged_ALL"
SOURCE_JSONL = "openimages/jsonl/sft_with_local_source_image_path.jsonl"

def load_filtered_data(path):
    """
    Loads and filters the data exactly as agent_sam.py does to ensure indices match.
    """
    data = []
    target_types = {
        "Add a new object to the scene", 
        "Add/Remove/Replace Accessories (glasses, hats, jewelry, masks)",
        "Clothing edit (change color/outfit)", 
        "Remove an existing object", 
        "Replace one object category with another"
    }
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"Source file not found: {path}")

    print("Loading and filtering source JSONL...")
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                if item.get('edit_type') in target_types:
                    data.append(item)
            except json.JSONDecodeError:
                continue
    return data

def main():
    # 1. Load the Source Data (The "Truth")
    try:
        original_items = load_filtered_data(SOURCE_JSONL)
        print(f"Loaded {len(original_items)} items from source.")
    except Exception as e:
        print(f"Error loading source data: {e}")
        return

    # 2. Find existing log files
    search_pattern = os.path.join(LOG_DIR, "item_*_log.json")
    log_files = glob.glob(search_pattern)
    
    if not log_files:
        print(f"No log files found in {LOG_DIR}")
        return

    print(f"Found {len(log_files)} log files to process.")

    updated_count = 0
    
    # 3. Update each file
    for log_path in tqdm(log_files, desc="Updating logs"):
        try:
            # Read existing log
            with open(log_path, 'r', encoding='utf-8') as f:
                log_data = json.load(f)
            
            # Determine Index
            # Priority 1: "item_idx" key inside the JSON
            # Priority 2: Filename "item_{idx}_log.json"
            item_idx = log_data.get("item_idx")
            
            if item_idx is None:
                try:
                    filename = os.path.basename(log_path)
                    # Extract 123 from item_123_log.json
                    item_idx = int(filename.split("_")[1])
                except (IndexError, ValueError):
                    print(f"Skipping {filename}: Could not determine item_idx")
                    continue
            
            # Retrieve Original Data
            if 0 <= item_idx < len(original_items):
                original_item = original_items[item_idx]
                
                # Merge / Update
                log_data["original_item"] = original_item
                
                # Check if we also need to verify/add local paths if they were missing in log
                # (Optional, but good for completeness based on user request)
                
                # Write back
                with open(log_path, 'w', encoding='utf-8') as f:
                    json.dump(log_data, f, indent=2, ensure_ascii=False)
                
                updated_count += 1
            else:
                print(f"Warning: Index {item_idx} in {log_path} is out of range (Max {len(original_items)-1})")

        except Exception as e:
            print(f"Error processing {log_path}: {e}")

    print(f"Successfully updated {updated_count} files.")

if __name__ == "__main__":
    main()
