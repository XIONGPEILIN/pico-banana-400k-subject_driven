import os
import glob
import json
import numpy as np
import matplotlib.pyplot as plt

AUDIT_DIR = "openimages/dino_mask_audit"

def analyze():
    files = glob.glob(os.path.join(AUDIT_DIR, "*.json"))
    print(f"Found {len(files)} audit files.")
    
    total_items = 0
    
    # Object Stats
    obj_sims = []
    obj_changed_counts = 0
    obj_total_counts = 0
    
    # Background Stats
    bg_sims = []
    bg_preserved_counts = 0
    bg_total_counts = 0
    
    for fpath in files:
        try:
            with open(fpath, "r") as f:
                data = json.load(f)
            
            total_items += 1
            results = data.get("results", {})
            
            # --- Global Background Stats ---
            if "global" in results:
                bg_sim = results["global"].get("background_bbox_sim")
                if bg_sim is not None:
                    bg_sims.append(bg_sim)
                    bg_total_counts += 1
                    if bg_sim >= 0.9:
                        bg_preserved_counts += 1

            # --- Object Stats (per kind) ---
            for kind in ["remove", "add"]:
                if kind in results:
                    r = results[kind]
                    
                    # Sub-masks (Objects)
                    if "sub_mask_results" in r:
                        for sm in r["sub_mask_results"]:
                            sim = sm.get("cos_sim")
                            if sim is not None:
                                obj_sims.append(sim)
                                obj_total_counts += 1
                                # Logic: If sim < 0.9, it changed (Success for editing)
                                if sim < 0.9:
                                    obj_changed_counts += 1
                                    
        except Exception as e:
            print(f"Error reading {fpath}: {e}")

    print("-" * 30)
    print(f"Total Audit Items: {total_items}")
    print("-" * 30)
    
    # Object Analysis
    if obj_total_counts > 0:
        avg_obj_sim = np.mean(obj_sims)
        success_rate_obj = (obj_changed_counts / obj_total_counts) * 100
        print(f"OBJECT MASKS (Target Regions):")
        print(f"  Count: {obj_total_counts}")
        print(f"  Avg Similarity: {avg_obj_sim:.4f} (Lower is better for editing)")
        print(f"  'Changed' Rate (Sim < 0.9): {success_rate_obj:.2f}%")
        print(f"  (Interpretation: {success_rate_obj:.2f}% of target regions were significantly modified)")
    else:
        print("No object masks found.")
        
    print("-" * 30)

    # Background Analysis
    if bg_total_counts > 0:
        avg_bg_sim = np.mean(bg_sims)
        success_rate_bg = (bg_preserved_counts / bg_total_counts) * 100
        print(f"BACKGROUND MASKS (Context Regions):")
        print(f"  Count: {bg_total_counts}")
        print(f"  Avg Similarity: {avg_bg_sim:.4f} (Higher is better for preservation)")
        print(f"  'Preserved' Rate (Sim >= 0.9): {success_rate_bg:.2f}%")
        print(f"  (Interpretation: {success_rate_bg:.2f}% of backgrounds remained stable)")
    else:
        print("No background masks found.")

    print("-" * 30)
    
    # Plotting
    try:
        plt.figure(figsize=(10, 5))
        
        plt.subplot(1, 2, 1)
        plt.hist(obj_sims, bins=50, color='blue', alpha=0.7, label='Object (Target)')
        plt.axvline(0.9, color='red', linestyle='dashed', linewidth=1, label='Threshold 0.9')
        plt.title('Object Similarity Distribution\n(Expect Low)')
        plt.xlabel('Cosine Similarity')
        plt.ylabel('Count')
        plt.legend()
        
        plt.subplot(1, 2, 2)
        plt.hist(bg_sims, bins=50, color='green', alpha=0.7, label='Background')
        plt.axvline(0.9, color='red', linestyle='dashed', linewidth=1, label='Threshold 0.9')
        plt.title('Background Similarity Distribution\n(Expect High)')
        plt.xlabel('Cosine Similarity')
        
        plt.tight_layout()
        plt.savefig('analysis_dino_distribution.png')
        print("Saved distribution plot to analysis_dino_distribution.png")
    except Exception as e:
        print(f"Could not generate plot: {e}")

if __name__ == "__main__":
    analyze()
