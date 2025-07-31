#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Vercel部署入口文件
将Flask应用适配为Vercel Serverless函数
"""

import os
import sys

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 设置环境变量
os.environ.setdefault('VERCEL_ENV', '1')

try:
    from web_interface_vercel import app
except ImportError as e:
    print(f"Import error: {e}")
    # 创建一个简单的Flask应用作为fallback
    from flask import Flask, jsonify
    app = Flask(__name__)
    
    @app.route('/')
    def index():
        return jsonify({"error": "Application failed to initialize", "details": str(e)})

# 如果直接运行此文件，启动开发服务器
if __name__ == '__main__':
    app.run(debug=True)

# 导出app供Vercel使用（这是Vercel识别的标准方式）