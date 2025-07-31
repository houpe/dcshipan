#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票调仓记录Web界面 - Vercel版本
提供调仓记录和关注组合的Web展示
"""

import asyncio
import json
import os
import tempfile
from datetime import datetime
from flask import Flask, render_template, jsonify
from rank_crawler import RankCrawler
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Vercel环境下使用临时目录
CACHE_DIR = os.environ.get('VERCEL_CACHE_DIR', tempfile.gettempdir())

def create_crawler():
    """创建爬虫实例"""
    return RankCrawler(cache_dir=CACHE_DIR, enable_deduplication=True)

@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')

@app.route('/api/stock_summary')
def get_stock_summary():
    """获取股票调仓汇总数据API"""
    try:
        # 获取股票汇总数据
        async def get_data():
            crawler = create_crawler()
            try:
                return await crawler.get_stock_summary(days_back=0, max_days_search=1)
            finally:
                # 确保关闭会话
                if hasattr(crawler, 'session') and crawler.session and not crawler.session.closed:
                    await crawler.session.close()
        
        stock_data = asyncio.run(get_data())
        
        return jsonify({
            'success': True,
            'data': stock_data,
            'total': len(stock_data),
            'update_time': datetime.now().strftime('%H:%M:%S')
        })
            
    except Exception as e:
        logger.error(f"获取股票汇总数据失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'data': [],
            'total': 0
        })

@app.route('/api/tc_list')
def get_tc_list():
    """获取调仓记录API"""
    try:
        # 获取调仓记录
        async def get_data():
            crawler = create_crawler()
            try:
                return await crawler.tc_list(days_back=0, max_days_search=1)
            finally:
                # 确保关闭会话
                if hasattr(crawler, 'session') and crawler.session and not crawler.session.closed:
                    await crawler.session.close()
        
        tc_data = asyncio.run(get_data())
        
        return jsonify({
            'success': True,
            'data': tc_data,
            'total': len(tc_data),
            'update_time': datetime.now().strftime('%H:%M:%S')
        })
            
    except Exception as e:
        logger.error(f"获取调仓记录失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'data': [],
            'total': 0
        })

@app.route('/api/portfolio_detail/<portfolio_id>')
def get_portfolio_detail(portfolio_id):
    """获取组合详情API"""
    try:
        # 获取组合详情
        async def get_data():
            crawler = create_crawler()
            try:
                return await crawler.get_portfolio_detail(portfolio_id)
            finally:
                # 确保关闭会话
                if hasattr(crawler, 'session') and crawler.session and not crawler.session.closed:
                    await crawler.session.close()
        
        portfolio_data = asyncio.run(get_data())
        
        return jsonify({
            'success': True,
            'data': portfolio_data,
            'update_time': datetime.now().strftime('%H:%M:%S')
        })
            
    except Exception as e:
        logger.error(f"获取组合详情失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'data': None
        })

# 健康检查端点
@app.route('/api/health')
def health_check():
    """健康检查"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'cache_dir': CACHE_DIR
    })

if __name__ == '__main__':
    print("启动股票调仓记录Web界面 (Vercel版本)...")
    print("访问地址: http://localhost:8889")
    print(f"缓存目录: {CACHE_DIR}")
    app.run(debug=False, host='0.0.0.0', port=8889, threaded=True)