#!/usr/bin/env python3
"""
启动脚本 - 极简全能持续运行Agent
"""
import sys
import os

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from main import main

if __name__ == "__main__":
    main()
