# Project Documentation for Gemini

This document provides an overview and instructions for running the project related to Qwen image editing.

## 1. Project Overview

This project appears to be focused on subject-driven image editing, likely leveraging the Qwen/Qwen3-VL-235B-A22B-Thinking-FP8 model for image generation or manipulation. The codebase suggests operations such as object extraction, mask generation, and analysis of model outputs.

## 2. Setup

Detailed setup instructions will go here. This typically includes:
*   **Prerequisites:** List any necessary software, libraries, or hardware requirements (e.g., Python version, CUDA, specific GPU models).
*   **Installation:** Steps to install dependencies (e.g., `pip install -r requirements.txt`).
*   **Data Preparation:** Instructions on how to set up the dataset, if applicable.

## 3. Usage

Instructions on how to use the various scripts and functionalities of the project. This could include:
*   Running specific scripts (e.g., `python batch_extract_and_generate.py`).
*   Explanation of command-line arguments.
*   Examples of typical workflows.

## 4. Server Command

The following command is used to start a VLLM server for the Qwen3-VL-235B-A22B-Thinking-FP8 model. This server likely handles model inference requests.

```bash
CUDA_DEVICE_ORDER="PCI_BUS_ID" CUDA_VISIBLE_DEVICES="1,2,3,4" vllm serve Qwen/Qwen3-VL-235B-A22B-Thinking-FP8   --tensor-parallel-size 4   --limit-mm-per-prompt.video 0 --port 7512
```

**Explanation of parameters:**
*   `CUDA_DEVICE_ORDER="PCI_BUS_ID"`: Ensures that CUDA devices are ordered according to their PCI bus ID.
*   `CUDA_VISIBLE_DEVICES="1,2,3,4"`: Specifies that CUDA devices with IDs 1, 2, 3, and 4 should be visible and used by the process.
*   `vllm serve Qwen/Qwen3-VL-235B-A22B-Thinking-FP8`: Initiates the VLLM serving process for the specified Qwen model.
*   `--tensor-parallel-size 4`: Configures the model to use tensor parallelism across 4 GPUs, distributing the model's layers for faster inference.
*   `--limit-mm-per-prompt.video 0`: This parameter might be related to limiting video input per prompt, with `0` indicating no video input or a specific handling.
*   `--port 7512`: Specifies the port on which the VLLM server will listen for incoming requests.

## 5. Additional Notes

*   **Further Development:** Areas for future work or improvements.
*   **Troubleshooting:** Common issues and their solutions.

---
*Original Notes from project initiation:*

These notes, originally in Chinese, appear to be internal directives or thoughts during the project's early stages.
*   "中文回答问题" (Answer questions in Chinese): This likely indicates a preference or requirement for responses or documentation to be in Chinese.
*   "仔细检查代码" (Carefully check the code): This is a standard reminder for thorough code review and quality assurance.