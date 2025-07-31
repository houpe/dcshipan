#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票调仓记录系统 - 跨平台启动器
兼容 macOS 和 Windows 系统
使用方法：python start.py
"""

import os
import sys
import time
import platform
import subprocess
import webbrowser
from pathlib import Path

def check_python_version():
    """检查Python版本"""
    if sys.version_info < (3, 7):
        print("❌ 错误: 需要Python 3.7或更高版本")
        print(f"当前版本: {sys.version}")
        return False
    return True

def check_dependencies():
    """检查依赖包是否安装"""
    required_packages = ['flask', 'aiohttp', 'asyncio']
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"❌ 缺少依赖包: {', '.join(missing_packages)}")
        print("请运行以下命令安装依赖:")
        print(f"pip install {' '.join(missing_packages)}")
        print("或者运行: pip install -r requirements.txt")
        return False
    
    return True

def get_system_info():
    """获取系统信息"""
    system = platform.system()
    return {
        'system': system,
        'is_windows': system == 'Windows',
        'is_macos': system == 'Darwin',
        'is_linux': system == 'Linux'
    }

def check_port_available(port):
    """检查端口是否可用"""
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', port))
            return True
    except OSError:
        return False

def find_available_port():
    """查找可用端口，按优先级尝试"""
    ports = [8888, 8080, 8889]
    
    for port in ports:
        if check_port_available(port):
            return port
    
    # 如果所有预设端口都被占用，尝试系统分配
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]

def start_web_server():
    """启动Web服务器"""
    try:
        # 导入并启动Flask应用
        from web_interface import app
        
        print("🚀 正在启动股票调仓记录系统...")
        print("📊 系统信息:")
        
        sys_info = get_system_info()
        print(f"   操作系统: {sys_info['system']}")
        print(f"   Python版本: {sys.version.split()[0]}")
        print(f"   工作目录: {os.getcwd()}")
        
        # 查找可用端口
        print("\n🔍 检查端口可用性...")
        port = find_available_port()
        
        if port == 8888:
            print("   ✅ 端口8888可用")
        elif port == 8080:
            print("   ⚠️  端口8888被占用，使用端口8080")
        elif port == 8889:
            print("   ⚠️  端口8888和8080被占用，使用端口8889")
        else:
            print(f"   ⚠️  预设端口都被占用，使用系统分配端口{port}")
        
        print("\n🌐 Web服务启动中...")
        print(f"   访问地址: http://localhost:{port}")
        print("   按 Ctrl+C 停止服务")
        
        # 延迟打开浏览器
        def open_browser():
            time.sleep(3)
            try:
                webbrowser.open(f'http://localhost:{port}')
                print("\n✅ 浏览器已自动打开")
            except Exception as e:
                print(f"\n⚠️  无法自动打开浏览器: {e}")
                print(f"请手动访问: http://localhost:{port}")
        
        # 在后台线程中打开浏览器
        import threading
        browser_thread = threading.Thread(target=open_browser, daemon=True)
        browser_thread.start()
        
        # 启动Flask应用
        app.run(
            debug=False,
            host='0.0.0.0',
            port=port,
            threaded=True,
            use_reloader=False  # 避免重载器问题
        )
        
    except KeyboardInterrupt:
        print("\n\n🛑 服务已停止")
        print("感谢使用股票调仓记录系统！")
    except Exception as e:
        print(f"\n❌ 启动失败: {e}")
        print("\n🔧 故障排除建议:")
        print("1. 检查是否安装了所有依赖包")
        print("2. 确认防火墙设置")
        print("3. 重启程序重新分配端口")
        return False
    
    return True

def main():
    """主函数"""
    print("="*60)
    print("📈 股票调仓记录系统 - 跨平台版本")
    print("🔗 兼容 Windows / macOS / Linux")
    print("="*60)
    
    # 检查Python版本
    if not check_python_version():
        input("\n按回车键退出...")
        return
    
    # 检查依赖包
    if not check_dependencies():
        input("\n按回车键退出...")
        return
    
    # 启动Web服务器
    start_web_server()

if __name__ == '__main__':
    main()