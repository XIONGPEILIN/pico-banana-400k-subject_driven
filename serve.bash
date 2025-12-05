CUDA_DEVICE_ORDER="PCI_BUS_ID" CUDA_VISIBLE_DEVICES="1,2,3,4" vllm serve Qwen/Qwen3-VL-235B-A22B-Instruct-FP8   --tensor-parallel-size 4   --limit-mm-per-prompt.video 0 --port 7512





