import json
import sys
from PIL import Image
from pathlib import Path

def analyze_filtered_confidence(file_path, area_threshold=0.9):
    """
    Counts confidence scores of 9 and 10, but only for items where no
    single bounding box's area exceeds a certain percentage of the image area.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: The file '{file_path}' is not a valid JSON file.")
        sys.exit(1)

    if not isinstance(data, list):
        print(f"Error: The JSON data in '{file_path}' is not a list of objects.")
        sys.exit(1)

    # Initialize counters
    count_confidence_9 = 0
    count_confidence_10 = 0
    skipped_items = 0
    
    # --- Helper function from view_bbox.ipynb to calculate area ratio ---
    def _calculate_area_ratio(coords):
        if not coords or len(coords) != 4:
            return 0.0
        
        # Determine if coordinates are normalized [0-1] or scaled [0-1000]
        max_c = max(coords)
        if max_c <= 1.5:
            norm = coords  # Assumed to be normalized [0-1]
        elif max_c <= 1000:
            norm = [c / 1000.0 for c in coords] # Assumed to be on a 1000px grid
        else:
            # This logic branch requires image dimensions, which we want to avoid
            # for performance. We will assume coordinates are either normalized
            # or on a 1000-scale grid as seen in the notebook.
            # If they are absolute pixels, this logic won't work without reading the image.
            # We'll treat them as invalid for this analysis.
            return -1 # Invalid coordinate system for this function

        left, top, right, bottom = norm
        return max(0.0, right - left) * max(0.0, bottom - top)

    total_items = len(data)
    print(f"Starting analysis of {total_items} items...")

    # Iterate through each main item in the list
    for i, item in enumerate(data):
        if (i + 1) % 5000 == 0:
            print(f"  ...processed {i+1}/{total_items} items")

        if not isinstance(item, dict):
            continue
        
        should_skip = False
        
        objects = item.get('bbox', {}).get('objects')
        if not isinstance(objects, list):
            continue
            
        # Check the area of each bounding box in the item
        for obj in objects:
            bbox_coords = obj.get('bbox')
            if bbox_coords:
                area_ratio = _calculate_area_ratio(bbox_coords)
                if area_ratio > area_threshold:
                    should_skip = True
                    break # No need to check other boxes in this item
        
        if should_skip:
            skipped_items += 1
            continue # Move to the next item

        # If not skipped, check the confidence score
        confidence = item.get('bbox', {}).get('confidence')
        if isinstance(confidence, (int, float)):
            if confidence == 9:
                count_confidence_9 += 1
            elif confidence == 10:
                count_confidence_10 += 1
    
    return count_confidence_9, count_confidence_10, skipped_items, total_items

def main():
    """
    Main function to execute the analysis and print the results.
    """
    file_to_analyze = 'bbox_results.json'
    print(f"Analyzing '{file_to_analyze}'...")
    print("Filtering out items where any bounding box area is > 90% of the image.")
    
    count9, count10, skipped, total = analyze_filtered_confidence(file_to_analyze, area_threshold=0.9)
    
    print("\n--- Filtered Confidence Score Counts ---")
    print(f"Total items analyzed: {total}")
    print(f"Items skipped (due to large bbox): {skipped}")
    print(f"Items included in count: {total - skipped}")
    print("----------------------------------------")
    print(f"Count of confidence score 9:  {count9}")
    print(f"Count of confidence score 10: {count10}")
    print("----------------------------------------\n")

if __name__ == "__main__":
    main()
