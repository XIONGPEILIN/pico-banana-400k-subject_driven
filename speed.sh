#!/bin/bash

# 检查是否安装了 curl
if ! command -v curl &> /dev/null; then
    echo "错误: 未找到 curl，请先安装它 (apt/yum/brew install curl)"
    exit 1
fi

echo "========================================="
echo "    代理网络连接测试 (基于 curl)"
echo "========================================="

# 1. 检查当前代理变量
echo "[1] 检查环境配置..."
if [ -z "$http_proxy" ] && [ -z "$https_proxy" ] && [ -z "$ALL_PROXY" ]; then
    echo "⚠️  警告: 未检测到代理环境变量 (http_proxy/https_proxy)。"
    echo "    请确保你已经 source ~/.bashrc 或手动 export 了代理。"
else
    echo "✅ 检测到代理配置:"
    echo "    HTTP_PROXY:  ${http_proxy:-未设置}"
    echo "    HTTPS_PROXY: ${https_proxy:-未设置}"
fi
echo "-----------------------------------------"

# 2. 测试延迟 (Latency)
# 使用 Google 或 GitHub 作为目标，计算 TCP 建立连接的时间 (time_connect)
TARGET_URL="https://www.google.com"
echo "[2] 测试延迟 (目标: $TARGET_URL)..."

# -w 参数用于自定义输出格式，只提取时间指标
LATENCY=$(curl -o /dev/null -s -w "%{time_connect}" $TARGET_URL)

if [ -z "$LATENCY" ] || [ "$LATENCY" == "0.000000" ]; then
    echo "❌ 连接失败，请检查代理节点是否可用。"
else
    # 将秒转换为毫秒
    LATENCY_MS=$(echo "$LATENCY * 1000" | bc 2>/dev/null || awk "{print $LATENCY * 1000}")
    echo "✅ 连接延迟 (TCP握手): ${LATENCY_MS} ms"
fi
echo "-----------------------------------------"

# 3. 测试下载速度 (Speed)
# 使用 Cloudflare 的测速文件 (100MB)，只下载 10秒 进行估算
SPEED_TARGET="https://speed.cloudflare.com/__down?bytes=100000000"
echo "[3] 测试下载速度 (目标: Cloudflare CDN)..."
echo "    正在下载测速文件 (限时 10 秒)..."

# -m 10: 最多运行10秒
# /dev/null: 不保存文件
curl -L -o /dev/null -m 10 --progress-bar $SPEED_TARGET

echo ""
echo "========================================="
echo "测试完成。"