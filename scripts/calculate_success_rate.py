import os
import json
import glob
import concurrent.futures
from tqdm import tqdm

def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

def process_file(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        # New pico_sam log format
        final_results = data.get('final_results')
        if isinstance(final_results, dict):
            removals_found = _as_int(final_results.get('removals_found'))
            remove_masks_saved = _as_int(final_results.get('remove_masks_saved'))
            additions_found = _as_int(final_results.get('additions_found'))
            add_masks_saved = _as_int(final_results.get('add_masks_saved'))

            success_remove = removals_found > 0 and removals_found == remove_masks_saved
            success_add = additions_found > 0 and additions_found == add_masks_saved
            success = success_remove or success_add
            return 1, 1 if success else 0

        # Legacy agent_sam log format
        if 'objects_processed' in data:
            for obj in data['objects_processed']:
                if obj.get('final_status') == 'PASSED':
                    return 1, 1
            return 1, 0

        # Unknown format; count but mark as failed
        return 1, 0
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0, 0

def calculate_success_rate(directory):
    # Restrict pattern to item subfolders to avoid slow deep recursion
    json_files = glob.glob(os.path.join(directory, 'item_*', 'item_*_log.json'))
    
    total_objects = 0
    passed_objects = 0
    
    print(f"Found {len(json_files)} JSON files. Processing with multi-threading...")
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = tqdm(executor.map(process_file, json_files), total=len(json_files), desc="Processing JSON files")
        
        for t, p in results:
            total_objects += t
            passed_objects += p

    if total_objects == 0:
        print("No objects processed found.")
        return

    success_rate = (passed_objects / total_objects) * 100
    print(f"Total Files: {total_objects}")
    print(f"Passed Files: {passed_objects}")
    print(f"Success Rate: {success_rate:.2f}%")

if __name__ == "__main__":
    directory = "openimages/pico_sam_output_ALL_20251206_032609"
    calculate_success_rate(directory)
