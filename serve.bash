export CUDA_VISIBLE_DEVICES="0,1"

python -m sglang.launch_server \
    --model-path Qwen/Qwen3-30B-A3B-Instruct-2507-FP8 \
    --context-length 32768 \
    --dp 2 \
    --port 12345
