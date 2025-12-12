import os
import json
import random
import glob
from PIL import Image
import matplotlib.pyplot as plt

# --- Configuration ---
AUDIT_DIR = "openimages/dino_mask_audit"
# Number of samples to visualize
NUM_SAMPLES = 5
# Threshold to define "Background Changed" (visualize items below this similarity)
VIEW_THRESHOLD = 0.9

def visualize_background_changes():
    # 1. Find all audit files
    audit_files = glob.glob(os.path.join(AUDIT_DIR, "*.json"))
    print(f"Scanning {len(audit_files)} audit files for background changes...")
    
    candidates = []
    
    # 2. Filter for items with low background similarity
    for fpath in audit_files:
        try:
            with open(fpath, "r") as f:
                data = json.load(f)
            
            # Check global results first
            bg_sim = None
            if "global" in data.get("results", {}):
                 bg_sim = data["results"]["global"].get("background_bbox_sim")
            
            # Fallback to remove/add if global is missing (backward compatibility)
            # This part is mostly for compatibility with older JSONs, newer ones should have "global"
            if bg_sim is None:
                for kind in ["remove", "add"]:
                    if kind in data.get("results", {}):
                        res = data["results"][kind]
                        temp_sim = res.get("background_bbox_sim")
                        if temp_sim is not None:
                            bg_sim = temp_sim
                            break

            if bg_sim is not None and bg_sim < VIEW_THRESHOLD:
                candidates.append({
                    "item_idx": data["item_idx"],
                    "bg_sim": bg_sim,
                    "before_path": data["before_image"],
                    "after_image": data["after_image"],
                    "log_path": data["log_path"]
                })
        except Exception:
            pass
            
    print(f"Found {len(candidates)} items where background similarity < {VIEW_THRESHOLD}")
    
    if not candidates:
        print("No candidates found.")
        return

    # 3. Randomly select samples
    samples = random.sample(candidates, min(len(candidates), NUM_SAMPLES))
    
    # 4. Plot
    plt.figure(figsize=(15, 5 * len(samples)))
    
    for i, item in enumerate(samples):
        try:
            img_a = Image.open(item["before_path"]).convert("RGB")
            img_b = Image.open(item["after_image"]).convert("RGB")
            
            # Column 1: Before
            plt.subplot(len(samples), 2, i*2 + 1)
            plt.imshow(img_a)
            plt.title(f"Before (Item {item['item_idx']})")
            plt.axis('off')
            
            # Column 2: After
            plt.subplot(len(samples), 2, i*2 + 2)
            plt.imshow(img_b)
            plt.title(f"After\nBG Sim: {item['bg_sim']:.4f} (< {VIEW_THRESHOLD})")
            plt.axis('off')
            
        except Exception as e:
            print(f"Error loading images for item {item['item_idx']}: {e}")

    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    visualize_background_changes()
