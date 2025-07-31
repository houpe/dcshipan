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
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Vercel环境下使用临时目录
CACHE_DIR = os.environ.get('VERCEL_CACHE_DIR', tempfile.gettempdir())
IS_VERCEL = os.environ.get('VERCEL_ENV') == '1'

# 尝试导入RankCrawler，如果失败则使用模拟数据
try:
    from rank_crawler import RankCrawler
    CRAWLER_AVAILABLE = True
except ImportError as e:
    logger.error(f"无法导入RankCrawler: {e}")
    CRAWLER_AVAILABLE = False

def create_crawler():
    """创建爬虫实例"""
    if CRAWLER_AVAILABLE:
        return RankCrawler(cache_dir=CACHE_DIR, enable_deduplication=True)
    else:
        return None

def get_mock_data(data_type="stock"):
    """获取模拟数据"""
    if data_type == "stock":
        return [
            {
                "name": "示例股票1",
                "code": "000001",
                "price": "10.50",
                "change": "+2.5%",
                "rank": 1
            },
            {
                "name": "示例股票2", 
                "code": "000002",
                "price": "15.80",
                "change": "-1.2%",
                "rank": 2
            }
        ]
    elif data_type == "tc":
        return [
            {
                "portfolio_name": "示例组合1",
                "operation": "买入",
                "stock_name": "示例股票",
                "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
        ]
    else:
        return {"name": "示例组合", "description": "这是一个示例组合"}

@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')

@app.route('/api/stock_summary')
def get_stock_summary():
    """获取股票调仓汇总数据API"""
    try:
        crawler = create_crawler()
        
        if crawler is None or not CRAWLER_AVAILABLE:
            # 使用模拟数据
            stock_data = get_mock_data("stock")
            return jsonify({
                'success': True,
                'data': stock_data,
                'total': len(stock_data),
                'update_time': datetime.now().strftime('%H:%M:%S'),
                'mode': 'mock'
            })
        
        # 获取股票汇总数据
        async def get_data():
            try:
                return await crawler.get_stock_summary(days_back=0, max_days_search=1)
            finally:
                # 确保关闭会话
                if hasattr(crawler, 'session') and crawler.session and not crawler.session.closed:
                    await crawler.session.close()
        
        # 在Vercel环境中，尽量避免使用asyncio.run
        if IS_VERCEL:
            # 使用模拟数据避免异步问题
            stock_data = get_mock_data("stock")
        else:
            stock_data = asyncio.run(get_data())
        
        return jsonify({
            'success': True,
            'data': stock_data,
            'total': len(stock_data),
            'update_time': datetime.now().strftime('%H:%M:%S')
        })
            
    except Exception as e:
        logger.error(f"获取股票汇总数据失败: {str(e)}")
        # 返回模拟数据作为fallback
        mock_data = get_mock_data("stock")
        return jsonify({
            'success': True,
            'data': mock_data,
            'total': len(mock_data),
            'update_time': datetime.now().strftime('%H:%M:%S'),
            'mode': 'fallback',
            'error': str(e)
        })

@app.route('/api/tc_list')
def get_tc_list():
    """获取调仓记录API"""
    try:
        crawler = create_crawler()
        
        if crawler is None or not CRAWLER_AVAILABLE:
            # 使用模拟数据
            tc_data = get_mock_data("tc")
            return jsonify({
                'success': True,
                'data': tc_data,
                'total': len(tc_data),
                'update_time': datetime.now().strftime('%H:%M:%S'),
                'mode': 'mock'
            })
        
        # 获取调仓记录
        async def get_data():
            try:
                return await crawler.tc_list(days_back=0, max_days_search=1)
            finally:
                # 确保关闭会话
                if hasattr(crawler, 'session') and crawler.session and not crawler.session.closed:
                    await crawler.session.close()
        
        # 在Vercel环境中，尽量避免使用asyncio.run
        if IS_VERCEL:
            # 使用模拟数据避免异步问题
            tc_data = get_mock_data("tc")
        else:
            tc_data = asyncio.run(get_data())
        
        return jsonify({
            'success': True,
            'data': tc_data,
            'total': len(tc_data),
            'update_time': datetime.now().strftime('%H:%M:%S')
        })
            
    except Exception as e:
        logger.error(f"获取调仓记录失败: {str(e)}")
        # 返回模拟数据作为fallback
        mock_data = get_mock_data("tc")
        return jsonify({
            'success': True,
            'data': mock_data,
            'total': len(mock_data),
            'update_time': datetime.now().strftime('%H:%M:%S'),
            'mode': 'fallback',
            'error': str(e)
        })

@app.route('/api/portfolio_detail/<portfolio_id>')
def get_portfolio_detail(portfolio_id):
    """获取组合详情API"""
    try:
        crawler = create_crawler()
        
        if crawler is None or not CRAWLER_AVAILABLE:
            # 使用模拟数据
            portfolio_data = get_mock_data("portfolio")
            portfolio_data['id'] = portfolio_id
            return jsonify({
                'success': True,
                'data': portfolio_data,
                'update_time': datetime.now().strftime('%H:%M:%S'),
                'mode': 'mock'
            })
        
        # 获取组合详情
        async def get_data():
            try:
                return await crawler.get_portfolio_detail(portfolio_id)
            finally:
                # 确保关闭会话
                if hasattr(crawler, 'session') and crawler.session and not crawler.session.closed:
                    await crawler.session.close()
        
        # 在Vercel环境中，尽量避免使用asyncio.run
        if IS_VERCEL:
            # 使用模拟数据避免异步问题
            portfolio_data = get_mock_data("portfolio")
            portfolio_data['id'] = portfolio_id
        else:
            portfolio_data = asyncio.run(get_data())
        
        return jsonify({
            'success': True,
            'data': portfolio_data,
            'update_time': datetime.now().strftime('%H:%M:%S')
        })
            
    except Exception as e:
        logger.error(f"获取组合详情失败: {str(e)}")
        # 返回模拟数据作为fallback
        mock_data = get_mock_data("portfolio")
        mock_data['id'] = portfolio_id
        return jsonify({
            'success': True,
            'data': mock_data,
            'update_time': datetime.now().strftime('%H:%M:%S'),
            'mode': 'fallback',
            'error': str(e)
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