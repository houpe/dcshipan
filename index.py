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

from web_interface_vercel import app

# Vercel需要的handler函数
def handler(request):
    return app(request.environ, lambda status, headers: None)

# 如果直接运行此文件，启动开发服务器
if __name__ == '__main__':
    app.run(debug=True)

# 导出app供Vercel使用
app = app