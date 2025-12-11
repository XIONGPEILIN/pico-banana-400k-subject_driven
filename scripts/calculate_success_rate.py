import os
import json
import glob
import concurrent.futures
from tqdm import tqdm

AUDIT_DIR = os.path.join("openimages", "dino_mask_audit")

def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0

# Helper to collect raw data once for each item
def _get_item_data(log_file_path):
    try:
        with open(log_file_path, 'r') as f:
            log_data = json.load(f)
        
        item_idx = log_data.get('item_idx')
        
        mask_success = False
        final_results = log_data.get('final_results')
        if isinstance(final_results, dict):
            removals_found = _as_int(final_results.get('removals_found'))
            remove_masks_saved = _as_int(final_results.get('remove_masks_saved'))
            additions_found = _as_int(final_results.get('additions_found'))
            add_masks_saved = _as_int(final_results.get('add_masks_saved'))
            
            success_remove = removals_found > 0 and removals_found == remove_masks_saved
            success_add = additions_found > 0 and additions_found == add_masks_saved
            mask_success = success_remove or success_add
        elif 'objects_processed' in log_data:
            for obj in log_data['objects_processed']:
                if obj.get('final_status') == 'PASSED':
                    mask_success = True
                    break

        bg_sim = None
        if item_idx is not None:
            audit_path = os.path.join(AUDIT_DIR, f"item_{item_idx}_dino_audit.json")
            if os.path.exists(audit_path):
                with open(audit_path, 'r') as af:
                    audit_data = json.load(af)
                bg_sim = audit_data.get("results", {}).get("global", {}).get("background_bbox_sim")
        
        return {
            'item_idx': item_idx,
            'mask_success': mask_success,
            'bg_sim': bg_sim
        }
    except Exception as e:
        # print(f"Error processing {log_file_path}: {e}")
        return None

def calculate_success_rate(directory, thresholds):
    log_files = glob.glob(os.path.join(directory, 'item_*', 'item_*_log.json'))
    
    if not log_files:
        print(f"No log files found in {directory}. Exiting.")
        return

    print(f"Found {len(log_files)} JSON log files. Collecting all item data...")
    
    all_item_data = []
    num_cpus = os.cpu_count() or 4
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_cpus) as executor:
        futures = {executor.submit(_get_item_data, log_file): log_file for log_file in log_files}
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Collecting item data"):
            result_data = future.result()
            if result_data:
                all_item_data.append(result_data)

    if not all_item_data:
        print("No valid item data collected. Exiting.")
        return

    print("\n" + "=" * 40)
    print("DINOv3 Background Audit Success Rates by Threshold")
    print("=" * 40)

    for threshold in sorted(thresholds):
        total_items = len(all_item_data)
        mask_passed_count = 0
        audited_count = 0
        audit_passed_count = 0
        total_strict_passed = 0 # Mask Success AND Audit Success

        for item in all_item_data:
            mask_success = item['mask_success']
            bg_sim = item['bg_sim']

            mask_passed_count += 1 if mask_success else 0

            if bg_sim is not None:
                audited_count += 1
                current_audit_passed = bg_sim >= threshold
                if current_audit_passed:
                    audit_passed_count += 1
                
                if mask_success and current_audit_passed:
                    total_strict_passed += 1

        print(f"\n--- Threshold: {threshold:.2f} ---")
        print(f"Total Items Processed: {total_items}")
        
        if total_items > 0:
            mask_rate = (mask_passed_count / total_items) * 100
            print(f"Mask Generation Success: {mask_passed_count}/{total_items} ({mask_rate:.2f}%)")
        else:
            print("No items to calculate mask success rate.")

        if audited_count > 0:
            audit_rate = (audit_passed_count / audited_count) * 100
            print(f"Background Audit Pass:   {audit_passed_count}/{audited_count} ({audit_rate:.2f}%)")
            
            strict_rate = (total_strict_passed / audited_count) * 100
            print(f"Strict Success (Mask+BG):{total_strict_passed}/{audited_count} ({strict_rate:.2f}%)")
        else:
            print("No audit data available for this threshold.")
    
    print("\n" + "=" * 40)


if __name__ == "__main__":
    directory = "openimages/pico_sam_output_ALL_20251206_032609"
    
    # Define the range of thresholds from 0.90 to 0.99
    thresholds_to_check = [i / 100.0 for i in range(80, 100)] 
    
    calculate_success_rate(directory, thresholds_to_check)