# Project Context: Subject-Driven Image Editing (Pico-Banana)

## Overview
This project focuses on subject-driven image editing, likely utilizing the **Qwen** model for editing and **SAM 3 (Segment Anything Model 3)** for segmentation and masking. The working directory is related to the `pico-banana-400k` dataset.

## Key Components

### 1. Core Logic & Processing
- **`data.py`**: (Active File) Handles data loading, dataset management, and likely image preprocessing.
- **`batch_extract_and_generate.py` / `batch_top_down.py`**: Scripts for batch processing images, likely running the editing or generation pipeline.
- **`object_extractor.py`**: Logic for extracting specific objects from images, possibly using SAM.
- **`scripts/generate_sam3_masks.py`**: Specific script for generating masks using SAM 3.

### 2. Analysis & Visualization
- **`analyze_json_data.py`**: Analyzes the output results (JSON format).
- **`visualize_bbox_results.py`**: Visualizes bounding box results.
- **`view.ipynb` / `view_bbox.ipynb`**: Jupyter notebooks for interactive visualization.

### 3. Outputs & Data
- **`parsed_model_outputs/`**: Directory containing processed JSON outputs from the model.
- **`analysis_results_box.json` / `bbox_results_processed.json`**: Aggregated analysis results.
- **Images**: `before.png`, `after.png`, `mask.png`, `overlay.png` serve as examples or debug outputs of the editing process.

## Technical Stack / Keywords
- **Python**: Primary language.
- **Qwen**: Vision/Language model used for editing.
- **SAM 3**: Segment Anything Model 3 used for masks.
- **JSON**: Heavy reliance on JSON for structured output and analysis.

## Guidelines for Interaction
- When discussing code changes, prioritize consistency with `data.py` (current context) and `batch_extract_and_generate.py`.
- Assume the workflow involves: Input Image -> Object Segmentation (SAM) -> Masking -> Generative Editing -> Output Analysis.
