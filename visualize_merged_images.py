import os
import glob
import random
import time
import json
from PIL import Image, ImageOps
import matplotlib.pyplot as plt

# --- Configuration ---
# Directory containing DINO audit JSONs
AUDIT_DIR = "openimages/dino_mask_audit"
# Number of random merged images to visualize
NUM_SAMPLES = 5
# Threshold for visualization
VIEW_THRESHOLD = 0.9

def overlay_heatmap(image_path, mask_path, alpha=0.5):
    """
    Overlays the inverted mask (low values = red) onto the image.
    Low similarity areas (dark in mask) become Red.
    """
    try:
        if not image_path or not os.path.exists(image_path):
            return Image.new("RGB", (256, 256), (200, 200, 200)) # Placeholder
            
        img = Image.open(image_path).convert("RGB")
        
        if mask_path and os.path.exists(mask_path):
            mask = Image.open(mask_path).convert("L")
            if mask.size != img.size:
                mask = mask.resize(img.size, Image.NEAREST)
            
            # Invert mask: Dark (0, bad) becomes 255 (High Opacity)
            # We want to highlight LOW similarity areas with Red.
            heatmap_alpha = ImageOps.invert(mask)
            
            # Scale alpha so it's not fully opaque (max 255 * alpha)
            # e.g. alpha=0.5 -> max opacity 128
            heatmap_alpha = heatmap_alpha.point(lambda p: int(p * alpha))
            
            # Create a solid red color layer
            red_layer = Image.new("RGB", img.size, (255, 0, 0))
            
            # Composite: Result = Original * (1-alpha) + Red * alpha
            out = Image.composite(red_layer, img, heatmap_alpha)
            return out
            
        return img
    except Exception as e:
        print(f"Overlay error for {image_path}: {e}")
        return Image.new("RGB", (256, 256), (128, 128, 128))

def visualize_merged_images():
    # Seed the random number generator
    random.seed(time.time())

    # 1. Find all audit JSON files
    audit_files = glob.glob(os.path.join(AUDIT_DIR, "*.json"))
    print(f"Scanning {len(audit_files)} audit files for low background similarity (< {VIEW_THRESHOLD})...")
    
    candidates = []
    
    # 2. Filter items
    for fpath in audit_files:
        try:
            with open(fpath, "r") as f:
                data = json.load(f)
            
            bg_sim = None
            fail_mask_path = None
            before_image = data.get("before_image")
            after_image = data.get("after_image")
            
            # Check global results
            if "global" in data.get("results", {}):
                 global_res = data["results"]["global"]
                 bg_sim = global_res.get("background_bbox_sim")
                 fail_mask_path = global_res.get("fail_mask_path")

            # Check threshold and file existence
            if bg_sim is not None and bg_sim < VIEW_THRESHOLD:
                if before_image and after_image:
                     # We don't strictly check if image files exist here to speed up, 
                     # but `overlay_heatmap` handles missing files.
                    candidates.append({
                        "item_idx": data["item_idx"],
                        "bg_sim": bg_sim,
                        "before_path": before_image,
                        "after_path": after_image,
                        "fail_mask_path": fail_mask_path
                    })
        except Exception:
            pass

    print(f"Found {len(candidates)} items with background similarity < {VIEW_THRESHOLD}")

    if not candidates:
        print("No candidates found.")
        return

    # 3. Randomly select samples
    samples_to_display = random.sample(candidates, min(len(candidates), NUM_SAMPLES))
    
    print(f"Displaying {len(samples_to_display)} random samples...")

    # 4. Create a figure
    # 1 column of stitched images
    plt.figure(figsize=(12, 6 * len(samples_to_display))) 
    
    for i, item in enumerate(samples_to_display):
        try:
            # Create Overlays
            img_before_vis = overlay_heatmap(item["before_path"], item["fail_mask_path"])
            img_after_vis = overlay_heatmap(item["after_path"], item["fail_mask_path"])
            
            # Stitch: Before | After
            w1, h1 = img_before_vis.size
            w2, h2 = img_after_vis.size
            
            # Ensure same height for stitching
            max_h = max(h1, h2)
            if h1 != max_h:
                img_before_vis = img_before_vis.resize((int(w1 * max_h / h1), max_h), Image.LANCZOS)
            if h2 != max_h:
                img_after_vis = img_after_vis.resize((int(w2 * max_h / h2), max_h), Image.LANCZOS)
                
            w1, h1 = img_before_vis.size
            w2, h2 = img_after_vis.size
            
            dst = Image.new('RGB', (w1 + w2, max_h))
            dst.paste(img_before_vis, (0, 0))
            dst.paste(img_after_vis, (w1, 0))
            
            # Display
            plt.subplot(len(samples_to_display), 1, i + 1)
            plt.imshow(dst)
            plt.title(f"Item {item['item_idx']} (Sim: {item['bg_sim']:.4f})\nLeft: Before + Mask Overlay | Right: After + Mask Overlay")
            plt.axis('off')
            
        except Exception as e:
            print(f"Error displaying item {item['item_idx']}: {e}")

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    visualize_merged_images()