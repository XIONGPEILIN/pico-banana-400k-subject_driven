import argparse
import json
import torch
from diffusers import FluxKontextPipeline
from PIL import Image
from pathlib import Path
from tqdm import tqdm
from accelerate import Accelerator, split_processes
from huggingface_hub import hf_hub_download

def main():
    # 1. Setup Accelerator for distributed processing
    accelerator = Accelerator()
    device = accelerator.device
    
    accelerator.print("Initializing distributed processing...")
    accelerator.print(f"Found {accelerator.num_processes} processes.")

    # 2. Load Model and Adapters
    if accelerator.is_main_process:
        accelerator.print("Loading FLUX.1-Kontext-dev pipeline...")
    
    # All processes will load the model
    pipe = FluxKontextPipeline.from_pretrained(
        "black-forest-labs/FLUX.1-Kontext-dev", 
        torch_dtype=torch.bfloat16
    )
    
    if accelerator.is_main_process:
        accelerator.print("Loading Kontext-Top-Down-View LoRA adapter...")

    # Load the specific LoRA adapter required
    pipe.load_lora_weights(
        "prithivMLmods/Kontext-Top-Down-View", 
        weight_name="Kontext-Top-Down-View.safetensors", 
        adapter_name="top-down"
    )
    pipe.set_adapters(["top-down"], adapter_weights=[1.0])
    
    # Move pipeline to the correct device
    pipe = pipe.to(device)

    # 3. Load and distribute data
    if accelerator.is_main_process:
        accelerator.print("Loading and distributing data from bbox_results.json...")
        with open('bbox_results.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
    else:
        data = None

    # Broadcast data from main process to all other processes
    # Using a simple object list for broadcasting
    data_list = [data]
    torch.distributed.broadcast_object_list(data_list, src=0)
    data = data_list[0]
    
    # Split the dataset among processes
    with accelerator.split_for_execution(data) as chunk_of_data:
        
        # Define output directory
        output_dir = Path("top_down_output")
        output_dir.mkdir(exist_ok=True)
        
        # Define the prompt for top-down conversion
        top_down_prompt = "[photo content], recreate the scene from a top-down perspective. Maintain all visual proportions, lighting consistency, and realistic spatial relationships. Ensure the background, textures, and environmental shadows remain naturally aligned from this elevated angle."
        
        accelerator.print(f"Process {accelerator.process_index} starting processing on {len(chunk_of_data)} images.")

        # 4. Processing Loop
        for item in tqdm(chunk_of_data, desc=f"Process {accelerator.process_index}"):
            input_image_path_str = item.get('local_input_image')
            if not input_image_path_str:
                accelerator.print(f"Skipping item due to missing 'local_input_image': {item.get('open_image_input_url')}")
                continue
                
            input_image_path = Path(input_image_path_str)
            if not input_image_path.exists():
                accelerator.print(f"Image not found, skipping: {input_image_path}")
                continue

            try:
                # Load the image
                original_image = Image.open(input_image_path).convert("RGB")
                
                # Define output path
                output_filename = f"{input_image_path.stem}_top_down.png"
                output_path = output_dir / output_filename
                
                # Skip if already processed
                if output_path.exists():
                    continue

                # Run inference
                image = pipe(
                    image=original_image, 
                    prompt=top_down_prompt,
                    guidance_scale=2.5, # from app.py
                    width=original_image.size[0],
                    height=original_image.size[1],
                    num_inference_steps=50, # As requested
                    generator=torch.Generator(device=device).manual_seed(42),
                ).images[0]
                
                # Save the image
                image.save(output_path)

            except Exception as e:
                accelerator.print(f"Error processing {input_image_path}: {e}")
                continue
    
    accelerator.wait_for_everyone()
    if accelerator.is_main_process:
        accelerator.print("Batch processing complete.")
        accelerator.print(f"Output images are saved in the '{output_dir}' directory.")

if __name__ == "__main__":
    main()
