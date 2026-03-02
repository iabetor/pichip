#!/usr/bin/env python3
"""启动股票看盘应用"""

import subprocess
import sys
import os

# 确保项目根目录在路径中
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

if __name__ == "__main__":
    # 启动Streamlit应用
    app_path = os.path.join(project_root, "pichip", "viewer", "app.py")
    subprocess.run([sys.executable, "-m", "streamlit", "run", app_path, "--server.port=8501"])
