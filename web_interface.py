#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票调仓记录Web界面
提供调仓记录和关注组合的Web展示
"""

import json
from datetime import datetime
from flask import Flask, render_template, jsonify
from rank_crawler import RankCrawler
import logging
import diskcache as dc

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 全局爬虫实例
crawler = None

# 初始化缓存
cache = dc.Cache('./data/cache/portfolio_cache')

def init_crawler():
    """初始化爬虫实例"""
    crawler = RankCrawler(cache_dir="./data/cache", enable_deduplication=True)
    # 确保每次都使用新的会话
    crawler.session = None
    return crawler

@app.route('/')
def index():
    """主页面"""
    return render_template('index.html')

@app.route('/api/stock_summary')
def get_stock_summary():
    """获取股票调仓汇总数据"""
    try:
        crawler = RankCrawler(cache_dir="./data/cache", enable_deduplication=True)
        try:
            stock_data = crawler.get_stock_summary(days_back=0, max_days_search=1)
        finally:
            # 确保关闭会话
            if hasattr(crawler, 'session') and crawler.session:
                crawler.session.close()
        
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
            'total': 0,
            'update_time': datetime.now().strftime('%H:%M:%S')
        })

@app.route('/api/tc_list')
def get_tc_list():
    """获取调仓记录API"""
    try:
        crawler = RankCrawler(cache_dir="./data/cache", enable_deduplication=True)
        try:
            tc_data = crawler.tc_list(days_back=0, max_days_search=1)
        finally:
            # 确保关闭会话
            if hasattr(crawler, 'session') and crawler.session:
                crawler.session.close()
        
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
            'total': 0,
            'update_time': datetime.now().strftime('%H:%M:%S')
        })

@app.route('/api/portfolio_detail/<portfolio_id>')
def get_portfolio_detail(portfolio_id):
    """获取组合详情API（带缓存）"""
    try:
        cache_key = f"portfolio_{portfolio_id}"
        
        # 尝试从缓存获取数据
        cached_data = cache.get(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取组合详情: {portfolio_id}")
            return jsonify({
                'success': True,
                'data': cached_data,
                'update_time': datetime.now().strftime('%H:%M:%S'),
                'from_cache': True
            })
        
        # 缓存未命中，从东财接口获取数据
        logger.info(f"缓存未命中，从接口获取组合详情: {portfolio_id}")
        crawler = RankCrawler(cache_dir="./data/cache", enable_deduplication=True)
        try:
            portfolio_data = crawler.get_portfolio_detail(portfolio_id)
        finally:
            # 确保关闭会话
            if hasattr(crawler, 'session') and crawler.session:
                crawler.session.close()
        
        if portfolio_data:
            # 将数据存入缓存，过期时间60秒（1分钟）
            cache.set(cache_key, portfolio_data, expire=60)
            logger.info(f"组合详情已缓存: {portfolio_id}")
            
            return jsonify({
                'success': True,
                'data': portfolio_data,
                'update_time': datetime.now().strftime('%H:%M:%S'),
                'from_cache': False
            })
        else:
            return jsonify({
                'success': False,
                'error': '获取组合详情失败',
                'data': None
            })
            
    except Exception as e:
        logger.error(f"获取组合详情失败: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e),
            'data': None
        })

if __name__ == '__main__':
    print("启动股票调仓记录Web界面...")
    print("访问地址: http://localhost:8889")
    # 生产模式运行，避免调试器冲突
    app.run(debug=False, host='0.0.0.0', port=8889, threaded=True)