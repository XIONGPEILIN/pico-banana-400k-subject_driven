import json
import argparse
import random
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

from tqdm import tqdm

# Generate a list of visually distinct colors
COLORS = [
    (255, 59, 59), (255, 159, 59), (255, 235, 59), (159, 255, 59),
    (59, 255, 59), (59, 255, 159), (59, 255, 235), (59, 159, 255),
    (59, 59, 255), (159, 59, 255), (235, 59, 255), (255, 59, 159),
    (255, 100, 100), (255, 179, 100), (255, 240, 100), (179, 255, 100),
    (100, 255, 100), (100, 255, 179), (100, 255, 240), (100, 179, 255),
    (100, 100, 255), (179, 100, 255), (240, 100, 255), (255, 100, 179),
]

def get_font(size: int = 20) -> ImageFont.FreeTypeFont:
    """Tries to load a default font, falls back to a basic one."""
    try:
        font = ImageFont.truetype("DejaVuSans.ttf", size)
    except IOError:
        font = ImageFont.load_default()
    return font

def draw_visualizations(item_idx: int, item: dict, output_dir: Path):
    """
    For a single item, creates a side-by-side comparison image showing 'before' and 'after'.
    """
    bbox_data = item.get("bbox")
    if not bbox_data or "objects" not in bbox_data or not isinstance(bbox_data["objects"], list):
        return

    before_path = item.get("local_input_image") or item.get("before_image")
    after_path = item.get("output_image") or item.get("after_image")

    if not (before_path and Path(before_path).exists() and after_path and Path(after_path).exists()):
        # print(f"Skipping item {item_idx}: Before or after image is missing.")
        return

    try:
        with Image.open(before_path).convert("RGBA") as before_img, \
             Image.open(after_path).convert("RGBA") as after_img:
            
            # For simplicity, resize both images to a fixed height to align them
            target_height = 720
            before_ratio = target_height / before_img.height
            after_ratio = target_height / after_img.height
            
            before_img = before_img.resize((int(before_img.width * before_ratio), target_height), Image.Resampling.LANCZOS)
            after_img = after_img.resize((int(after_img.width * after_ratio), target_height), Image.Resampling.LANCZOS)

            # Create a new canvas to hold both images side-by-side with padding
            padding = 40
            total_width = before_img.width + after_img.width + padding * 3
            total_height = target_height + padding * 2

            comparison_img = Image.new('RGBA', (total_width, total_height), (40, 40, 40, 255))
            
            # Paste images
            comparison_img.paste(before_img, (padding, padding))
            comparison_img.paste(after_img, (before_img.width + padding * 2, padding))

            draw = ImageDraw.Draw(comparison_img)
            font_small = get_font(18)
            font_title = get_font(24)

            # Draw titles for each side
            draw.text((padding, 10), "Before (Removed)", font=font_title, fill="white")
            draw.text((before_img.width + padding * 2, 10), "After (Added/Modified)", font=font_title, fill="white")
            
            # Draw Edit Type
            edit_type = item.get("edit_type", "Unknown Edit Type")
            draw.text((padding, total_height - 30), f"Edit Type: {edit_type}", font=font_small, fill="lightgray")

            objects = bbox_data["objects"]
            removed_objects = [obj for obj in objects if obj.get("is_remove") is True]
            added_modified_objects = [obj for obj in objects if obj.get("is_remove") is False]

            # Draw on 'before' side
            _draw_points_on_canvas(draw, removed_objects, before_img.size, (padding, padding), font_small)
            
            # Draw on 'after' side
            _draw_points_on_canvas(draw, added_modified_objects, after_img.size, (before_img.width + padding * 2, padding), font_small)

            output_path = output_dir / f"{item_idx:04d}_comparison.png"
            comparison_img.save(output_path)

    except Exception as e:
        print(f"Error creating comparison for item {item_idx}: {e}")

def _draw_points_on_canvas(draw: ImageDraw.Draw, objects: list, img_size: tuple, offset: tuple, font: ImageFont.FreeTypeFont):
    """Helper function to draw scaled points and text on the main canvas."""
    point_radius = 5
    img_width, img_height = img_size
    offset_x, offset_y = offset
    
    for i, obj in enumerate(objects):
        color = COLORS[i % len(COLORS)]
        points = obj.get("point_2d")
        description = obj.get("description", "No description")
        object_name = obj.get("label", "N/A")

        if not points or not isinstance(points, list):
            continue

        scaled_points = []
        
        # Handle flat list [x, y] for a single point
        if len(points) == 2 and isinstance(points[0], (int, float)) and isinstance(points[1], (int, float)):
             points = [points] # Wrap in list to make it [[x, y]]

        for pt in points:
            if isinstance(pt, list) and len(pt) == 2:
                x = int((pt[0] / 1000.0) * img_width) + offset_x
                y = int((pt[1] / 1000.0) * img_height) + offset_y
                scaled_points.append((x, y))
                draw.ellipse((x - point_radius, y - point_radius, x + point_radius, y + point_radius), fill=color, outline=(0, 0, 0))
        
        if scaled_points:
            first_point = scaled_points[0]
            text_pos = (first_point[0] + 15, first_point[1])
            draw.text(text_pos, f"({i+1}) {object_name}", fill=color, font=font, stroke_width=1, stroke_fill=(0, 0, 0, 200))






def main():
    parser = argparse.ArgumentParser(description="Visualize bounding box results from a JSON file.")
    parser.add_argument(
        "json_path",
        type=str,
        nargs="?",  # Make it optional
        default="test/test_bbox_results.json",  # Set default value
        help="Path to the input JSON file (e.g., bbox_results.json).",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="test/visualized_results",
        help="Directory to save the visualization images.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional: Limit the number of items to process (after random sampling if enabled).",
    )
    parser.add_argument(
        "--random_sample",
        action="store_true",  # This makes it a boolean flag
        help="If set, randomly sample 50 items from the data for visualization.",
    )
    args = parser.parse_args()

    json_file = Path(args.json_path)
    if not json_file.exists():
        print(f"Error: JSON file not found at {json_file}")
        return

    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    print(f"Loading data from {json_file}...")
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    initial_data_size = len(data)

    if args.random_sample:
        sample_size = min(50, initial_data_size)
        if sample_size < initial_data_size:
            print(f"Randomly sampling {sample_size} items from {initial_data_size} total items.")
            data = random.sample(data, sample_size)
        else:
            print(f"Data size {initial_data_size} is less than or equal to 50, processing all items.")

    if args.limit is not None: # Use is not None to allow 0 or other falsy values if needed later
        limit_size = min(args.limit, len(data))
        if limit_size < len(data):
            print(f"Limiting processed items to {limit_size}.")
            data = data[:limit_size]
        else:
            print(f"Limit {args.limit} is greater than or equal to current data size, processing all items.")

    if not data:
        print("No items to process after filtering.")
        return

    print(f"Found {len(data)} items to process. Visualizations will be saved to '{output_dir}'.")
    
    # Process each item with a progress bar
    for i, item in enumerate(tqdm(data, desc="Generating Visualizations")):
        draw_visualizations(i, item, output_dir)

    print(f"\nProcessing complete. Check the '{output_dir}' directory for results.")
    print("To run this script, use a command like:")
    print(f"python visualize_bbox_results.py {args.json_path} --output_dir {args.output_dir}")


if __name__ == "__main__":
    main()
