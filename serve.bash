GPU_MEM_UTIL=0.84     
CUDA_DEVICE_ORDER="PCI_BUS_ID" CUDA_VISIBLE_DEVICES="1,2,3,4" vllm serve Qwen/Qwen3-VL-32B-Instruct-FP8  --gpu-memory-utilization "$GPU_MEM_UTIL" --max-model-len 170000 --data-parallel-size 4   --limit-mm-per-prompt.video 0 --port 7512


