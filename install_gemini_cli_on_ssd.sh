#!/bin/bash

# --- Gemini CLI 全新安装到 SSD ---

# 1. 设置目标 SSD 目录
export SSD_NVM_DIR="$HOME/ssd/xiong-p/.nvm"

echo "将 NVM 和 Node.js 环境安装到: $SSD_NVM_DIR"

# 2. 清理旧的 NVM (如果有的话，确保环境干净)
echo "清理旧的 NVM 目录 (~/.nvm)..."
mv ~/.nvm ~/.nvm_backup_$(date +%s)
echo "备份并清理 .bashrc 中的旧 NVM 配置..."
cp ~/.bashrc ~/.bashrc.bak
sed -i '/NVM_DIR/d' ~/.bashrc
sed -i '/nvm.sh/d' ~/.bashrc
sed -i '/nvm_bash_completion/d' ~/.bashrc

# 3. 创建 SSD 目录
echo "创建 SSD 目标目录: $SSD_NVM_DIR"
mkdir -p "$SSD_NVM_DIR"

# 4. 在 SSD 路径下安装 NVM
echo "下载并安装 NVM..."
# 我们手动设置 NVM_DIR 环境变量指向 SSD
export NVM_DIR="$SSD_NVM_DIR"
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash

# 5. 确保 .bashrc 使用新的 SSD 路径
# (安装脚本通常会自动添加，但为了保险起见，我们强制追加正确的配置)
echo "更新 .bashrc 文件以加载 SSD 上的 NVM 配置..."
if ! grep -q "$SSD_NVM_DIR" ~/.bashrc; then
  echo "" >> ~/.bashrc
  echo "# NVM Config on SSD" >> ~/.bashrc
  echo "export NVM_DIR=\"$SSD_NVM_DIR\"" >> ~/.bashrc
  echo '[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm' >> ~/.bashrc
  echo '[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion' >> ~/.bashrc
fi

# 6. 加载新的 NVM 配置
echo "加载新的 NVM 配置..."
source ~/.bashrc

# 7. 安装最新稳定版 Node.js
echo "安装最新稳定版 Node.js..."
nvm install node
nvm use node

# 8. 重新安装 Gemini CLI
echo "重新安装 Gemini CLI..."
# 根据你之前的环境，这里假设包名是标准的 'gemini-chat-cli'
# 如果你的 Gemini CLI 有不同的 npm 包名，请修改下面这行
npm install -g gemini-chat-cli

# 9. 验证安装位置
echo "验证 Gemini CLI 安装位置..."
which gemini
echo "安装脚本执行完毕。"
