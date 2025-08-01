#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


功能：
1. 获取5个排行榜数据（日榜、周榜、月榜、年榜、总榜）
2. 使用DiskCache缓存数据
3. 添加排名信息
4. 定时执行（交易时间内每30秒执行一次）
5. 显示进度条和统计信息
"""

import asyncio
import aiohttp
import time
import logging
import os
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from tqdm import tqdm
import diskcache as dc
import schedule
import threading

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RankCrawler:
    def __init__(self, cache_dir: str = "data/cache", enable_deduplication: bool = True, max_concurrent_requests: int = 5):
        """初始化爬虫
        
        Args:
            cache_dir: 缓存目录路径
            enable_deduplication: 是否启用去重功能，默认为True
            max_concurrent_requests: 最大并发请求数，默认为5，避免被封IP
        """
        self.base_url = "https://emdcspzhapi.dfcfs.cn/rtV1"
        self.cache_dir = cache_dir
        self.enable_deduplication = enable_deduplication
        self.max_concurrent_requests = max_concurrent_requests
        self.ensure_cache_dir()
        
        # 初始化DiskCache
        self.cache = dc.Cache(os.path.join(self.cache_dir, 'rank_cache'))
        
        # 初始化并发控制信号量
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # 排行榜类型配置
        count = 100
        self.rank_types = {
            "daily": {"rankType": "10005", "name": "日榜", "recCnt": count},
            "weekly": {"rankType": "10000", "name": "周榜", "recCnt": count},
            "monthly": {"rankType": "10001", "name": "月榜", "recCnt": count},
            "yearly": {"rankType": "10003", "name": "年榜", "recCnt": count},
            "total": {"rankType": "10004", "name": "总榜", "recCnt": count}
        }
        
        self.session = None
        self.running = False
        
        logger.info(f"爬虫初始化完成，去重功能: {'启用' if self.enable_deduplication else '禁用'}，最大并发数: {max_concurrent_requests}")
        
    def ensure_cache_dir(self):
        """确保缓存目录存在
        
        检查缓存目录是否存在，如果不存在则创建该目录。
        这是初始化过程中的重要步骤，确保后续的缓存操作能够正常进行。
        """
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    async def create_session(self):
        """创建HTTP会话
        
        创建一个异步HTTP会话，用于发送网络请求。
        配置了连接池限制和超时设置以优化性能和稳定性。
        
        配置参数:
            - 总超时时间: 30秒
            - 连接池总限制: 100个连接
            - 单主机连接限制: 30个连接
        """
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    
    async def close_session(self):
        """关闭HTTP会话
        
        安全地关闭HTTP会话，释放相关资源。
        在程序结束或需要重新创建会话时调用。
        """
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
    
    async def fetch_with_retry(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """带重试机制的HTTP请求
        
        发送HTTP GET请求并在失败时自动重试。
        这个方法提供了网络请求的容错机制，提高了数据获取的可靠性。
        使用信号量控制并发数量，避免被服务器封IP。
        
        Args:
            url (str): 要请求的URL地址
            max_retries (int): 最大重试次数，默认为3次
            
        Returns:
            Optional[Dict[str, Any]]: 成功时返回JSON响应数据，失败时返回None
            
        Note:
            - 每次重试前会等待1秒
            - 只有当API返回result='0'时才认为请求成功
            - 会自动处理JSON解析错误
            - 使用信号量限制并发请求数量，保护服务器资源
        """
        await self.create_session()
        
        # 使用信号量控制并发数量
        async with self.semaphore:
            for attempt in range(max_retries):
                try:
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            text = await response.text()
                            try:
                                data = json.loads(text)
                                if isinstance(data, dict) and data.get('result') == '0':
                                    return data
                                else:
                                    logger.warning(f"API返回错误: {data.get('message', 'Unknown error')}")
                            except json.JSONDecodeError:
                                logger.warning(f"无法解析JSON响应: {text[:100]}...")
                        else:
                            logger.warning(f"HTTP请求失败: {response.status}")
                except Exception as e:
                    logger.warning(f"请求失败 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(1)  # 重试前等待1秒
            
            # 在信号量保护下添加请求间隔，进一步降低被封IP的风险
            await asyncio.sleep(0.1)  # 每个请求间隔100ms
        
        return None
    
    async def fetch_rank_data(self, rank_type: str, rank_config: Dict) -> Optional[Dict[str, Any]]:
        """获取单个排行榜数据（支持分页）
        
        根据排行榜类型和配置获取对应的组合排行数据。
        支持分页获取，可以获取超过API单次限制的数据量。
        
        Args:
            rank_type (str): 排行榜类型标识符
                           (如 'daily', 'weekly', 'monthly', 'yearly', 'total')
            rank_config (Dict): 排行榜配置信息，包含:
                - rankType: API需要的排行榜类型参数
                - name: 排行榜中文名称
                - recCnt: 需要获取的记录数量
                           
        Returns:
            Optional[Dict[str, Any]]: 成功时返回包含排行数据的字典，失败时返回None
                返回格式: {
                    'result': '0',
                    'data': [排行数据列表]
                }
                
        Note:
            - API单次最多返回20条数据，本方法会自动分页获取
            - 会为每条数据添加rank、rank_type、rank_name字段
            - 如果某页返回数据少于请求数量，会停止分页
        """
        all_data = []
        target_count = rank_config['recCnt']
        page_size = 20  # API限制每次最多20条
        current_page = 0
        
        while len(all_data) < target_count:
            # 计算本次请求的数量
            remaining = target_count - len(all_data)
            current_count = min(page_size, remaining)
            
            url = f"{self.base_url}?type=rt_get_rank&rankType={rank_config['rankType']}&recIdx={current_page}&recCnt={current_count}&rankid=0"
            
            data = await self.fetch_with_retry(url)
            if data and 'data' in data and len(data['data']) > 0:
                all_data.extend(data['data'])
                current_page += 1
                
                # 如果返回的数据少于请求的数量，说明已经到底了
                if len(data['data']) < current_count:
                    break
            else:
                # 如果请求失败或没有数据，停止分页
                break
        
        if all_data:
            # 添加排名信息
            for i, item in enumerate(all_data, 1):
                item['rank'] = i
                item['rank_type'] = rank_type
                item['rank_name'] = rank_config['name']
            
            # 构造返回数据格式
            result_data = {
                'result': '0',
                'data': all_data[:target_count]  # 确保不超过目标数量
            }
            
            return result_data
    
    def _parse_combo_html(self, combo_html):
        """解析组合HTML字符串，提取结构化信息
        
        从HTML格式的组合信息中提取结构化数据，包括组合ID、名称、显示名称和排名标签。
        这是处理前端传递的组合HTML数据的核心解析函数，确保数据的准确提取和格式化。
        
        Args:
            combo_html (str): HTML格式的组合信息字符串，包含组合的基本信息和排名标签
                             格式示例: '<span class="combo-name" data-combo-id="123" data-combo-name="测试组合">测试组合</span><span class="rank-tag" data-rank-type="daily">日榜第1</span>'
                             
        Returns:
            Optional[Dict[str, Any]]: 成功时返回结构化的组合信息，失败时返回None
                返回的数据结构:
                {
                    'id': '组合ID',
                    'name': '组合原始名称', 
                    'display_name': '显示名称',
                    'ranks': [
                        {
                            'type': '排名类型(daily/weekly/monthly等)',
                            'text': '排名文本(如"日榜第1")'
                        },
                        ...
                    ]
                }
                
        Process:
            1. 使用正则表达式提取组合的基本信息(ID、名称、显示名称)
            2. 提取所有排名标签信息
            3. 构造并返回结构化的数据对象
            
        Note:
            - 使用正则表达式解析HTML，确保准确提取数据属性
            - 支持多个排名标签的解析
            - 如果HTML格式不匹配，返回None
            - 排名标签是可选的，组合可能没有排名信息
        """
        import re
        
        # 提取组合名称和ID
        combo_match = re.search(r'data-combo-id="([^"]+)"[^>]*data-combo-name="([^"]+)"[^>]*>([^<]+)', combo_html)
        if not combo_match:
            return None
        
        combo_id = combo_match.group(1)
        combo_name = combo_match.group(2)
        display_name = combo_match.group(3)
        
        # 提取排名标签
        ranks = []
        rank_matches = re.findall(r'<span class="rank-tag"[^>]*data-rank-type="([^"]+)"[^>]*>([^<]+)</span>', combo_html)
        for rank_type, rank_text in rank_matches:
            ranks.append({
                'type': rank_type,
                'text': rank_text
            })
        
        return {
            'id': combo_id,
            'name': combo_name,
            'display_name': display_name,
            'ranks': ranks
        }
    
    async def fetch_all_rank_data(self) -> Dict[str, Any]:
        """获取所有排行榜数据
        
        并发获取所有配置的排行榜数据，包括日榜、周榜、月榜、年榜、总榜等。
        这是数据获取的核心方法，使用异步并发提高获取效率，并提供实时进度显示。
        
        Returns:
            Dict[str, Any]: 包含所有排行榜数据的字典，格式为:
                {
                    'daily': {'result': '0', 'data': [...]},
                    'weekly': {'result': '0', 'data': [...]},
                    'monthly': {'result': '0', 'data': [...]},
                    'yearly': {'result': '0', 'data': [...]},
                    'total': {'result': '0', 'data': [...]}
                }
                
        Process:
            1. 创建HTTP会话连接
            2. 为每个排行榜类型创建异步获取任务
            3. 并发执行所有任务，实时更新进度条
            4. 收集所有成功获取的数据
            5. 关闭HTTP会话并返回结果
            
        Note:
            - 使用异步并发提高数据获取效率
            - 显示详细的进度信息，包括完成数量和耗时
            - 自动处理HTTP会话的创建和关闭
            - 失败的请求不会影响其他数据的获取
            - 返回的数据结构与单个排行榜获取保持一致
            - 适用于批量数据更新和初始化场景
            
        Example:
            >>> data = await crawler.fetch_all_rank_data()
            >>> print(f"获取到 {len(data)} 个排行榜的数据")
            >>> for rank_type, rank_data in data.items():
            >>>     print(f"{rank_type}: {len(rank_data.get('data', []))} 条记录")
        """
        await self.create_session()
        
        results = {}
        total_tasks = len(self.rank_types)
        
        with tqdm(total=total_tasks, desc="fetch_all_rank_data 获取排行榜数据", unit="个") as pbar:
            start_time = time.time()
            
            # 并发获取所有排行榜数据
            tasks = []
            for rank_type, rank_config in self.rank_types.items():
                task = self.fetch_rank_data(rank_type, rank_config)
                tasks.append((rank_type, task))
            
            # 等待所有任务完成
            for rank_type, task in tasks:
                data = await task
                if data:
                    results[rank_type] = data
                
                pbar.update(1)
                elapsed_time = time.time() - start_time
                pbar.set_postfix({
                    '已完成': f"{pbar.n}/{total_tasks}",
                    '耗时': f"{elapsed_time:.2f}s"
                })
        
        await self.close_session()
        return results
    
    def deduplicate_rank_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """对排行榜数据进行去重，优先级：总榜>年榜>月榜>周榜>日榜
        
        对多个排行榜中的重复组合进行去重处理，确保每个组合只在优先级最高的榜单中出现。
        这样可以避免同一个组合在多个榜单中重复显示，提供更清晰的数据视图。
        
        Args:
            data (Dict[str, Any]): 包含多个排行榜数据的字典，格式为:
                {
                    'daily': {'result': '0', 'data': [...]},
                    'weekly': {'result': '0', 'data': [...]},
                    ...
                }
                
        Returns:
            Dict[str, Any]: 去重后的排行榜数据，保持原有的数据结构
            
        Note:
            - 优先级顺序：总榜(1) > 年榜(2) > 月榜(3) > 周榜(4) > 日榜(5)
            - 使用zjzh字段（组合ID）作为去重的唯一标识符
            - 去重后会重新分配每个榜单内的排名序号
            - 保持原有的数据结构和字段不变
        """
        # 定义榜单优先级（数字越小优先级越高）
        rank_priority = {
            'total': 1,   # 总榜
            'yearly': 2,  # 年榜
            'monthly': 3, # 月榜
            'weekly': 4,  # 周榜
            'daily': 5    # 日榜
        }
        
        # 存储所有组合数据，key为组合ID，value为(优先级, 榜单类型, 数据)
        portfolio_map = {}
        
        # 遍历所有榜单数据
        for rank_type, rank_data in data.items():
            priority = rank_priority.get(rank_type, 999)  # 未知类型设为最低优先级
            
            for item in rank_data.get('data', []):
                portfolio_id = item.get('zjzh')  # 组合ID
                if portfolio_id:
                    # 如果该组合还未记录，或当前榜单优先级更高，则更新记录
                    if (portfolio_id not in portfolio_map or 
                        priority < portfolio_map[portfolio_id][0]):
                        portfolio_map[portfolio_id] = (priority, rank_type, item)
        
        # 重新构建去重后的数据结构
        deduplicated_data = {}
        for rank_type in data.keys():
            deduplicated_data[rank_type] = {
                'result': '0',
                'data': []
            }
        
        # 将去重后的数据分配回对应的榜单
        for portfolio_id, (priority, rank_type, item) in portfolio_map.items():
            deduplicated_data[rank_type]['data'].append(item)
        
        # 重新排序每个榜单的数据（按原有排名）
        for rank_type, rank_data in deduplicated_data.items():
            rank_data['data'].sort(key=lambda x: x.get('rank', 999))
            # 重新分配排名
            for i, item in enumerate(rank_data['data'], 1):
                item['rank'] = i
        
        return deduplicated_data
    
    def save_rank_data(self, data: Dict[str, Any]) -> str:
        """保存排行榜数据到DiskCache
        
        将获取到的所有排行榜数据保存到磁盘缓存中，支持数据持久化和快速访问。
        同时提供数据去重、过期清理等功能，确保缓存数据的质量和存储效率。
        
        Args:
            data (Dict[str, Any]): 包含所有排行榜数据的字典，格式为:
                {
                    'daily': {'result': '0', 'data': [...]},
                    'weekly': {'result': '0', 'data': [...]},
                    ...
                }
                
        Returns:
            str: 缓存键名，格式为 'rank_data_YYYYMMDD'
            
        Note:
            - 根据enable_deduplication配置决定是否进行去重处理
            - 使用当前日期作为缓存键，同一天的数据会覆盖之前的缓存
            - 同时保存为latest_rank_data键，便于快速访问最新数据
            - 自动清理过期缓存，释放存储空间
            - 记录详细的统计信息，包括总记录数、更新时间等
        """
        date_str = datetime.now().strftime("%Y%m%d")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 根据配置决定是否进行去重处理
        if self.enable_deduplication:
            deduplicated_data = self.deduplicate_rank_data(data)
        else:
            deduplicated_data = data
        
        # 统计总记录数
        total_records = sum(len(rank_data.get('data', [])) for rank_data in deduplicated_data.values())
        
        # 合并所有数据
        merged_data = {
            "timestamp": timestamp,
            "date": datetime.now().strftime("%Y-%m-%d"),
            "update_time": datetime.now().isoformat(),
            "total_records": total_records,
            "data": deduplicated_data
        }
        
        # 使用日期作为缓存键
        cache_key = f"rank_data_{date_str}"
        
        # 清除当天的旧缓存（如果存在）
        if cache_key in self.cache:
            del self.cache[cache_key]
            logger.info(f"🗑️ 清除旧缓存: {cache_key}")
        
        # 保存到DiskCache
        self.cache.set(cache_key, merged_data)
        
        # 保存最新数据
        self.cache.set("latest_rank_data", merged_data)
        
        # 清理过期缓存
        self.clean_expired_cache(CACHE_EXPIRE_HOURS)
        
        logger.info(f"💾 数据已缓存: {cache_key}, 记录数: {total_records}条")
        return cache_key
    
    def load_latest_rank_data(self) -> Optional[Dict[str, Any]]:
        """从DiskCache加载最新的排行榜数据
        
        从磁盘缓存中加载最新保存的排行榜数据，提供快速的数据访问能力。
        这个方法是获取缓存数据的主要入口，支持离线数据访问和减少网络请求。
        
        Returns:
            Optional[Dict[str, Any]]: 成功时返回完整的排行榜数据结构，失败时返回None
                返回格式: {
                    'timestamp': '20231201_143022',
                    'date': '2023-12-01',
                    'update_time': '2023-12-01T14:30:22.123456',
                    'total_records': 500,
                    'data': {
                        'daily': {'result': '0', 'data': [...]},
                        'weekly': {'result': '0', 'data': [...]},
                        ...
                    }
                }
                
        Note:
            - 通过latest_rank_data键访问最新数据
            - 包含完整的元数据信息：时间戳、日期、记录数等
            - 如果缓存不存在或读取失败，返回None
            - 异常情况会被捕获并记录到日志中
        """
        try:
            data = self.cache.get("latest_rank_data")
            if data is not None:
                return data
        except Exception as e:
            logger.error(f"从DiskCache加载排行榜数据失败: {str(e)}")
        
        return None
    
    def clean_expired_cache(self, expire_hours: int = 1):
        """清理过期的缓存数据
        
        删除超过指定小时数的缓存数据，释放存储空间并保持缓存的整洁。
        这是一个维护性功能，定期清理可以防止缓存无限增长。
        
        Args:
            expire_hours (int): 缓存保留小时数，默认为1小时
                               超过这个时间的缓存数据将被删除
                               
        Note:
            - 只清理以'rank_data_'开头的缓存键
            - 支持两种日期格式：YYYYMMDD 和 YYYYMMDD_HHMMSS
            - 基于缓存键中的时间戳判断是否过期
            - 清理操作是安全的，不会影响最新数据的访问
            - 会记录清理的缓存条目数量
            - 格式错误的缓存键会被跳过，不会影响清理流程
        """
        try:
            current_time = datetime.now()
            expire_threshold = current_time - timedelta(hours=expire_hours)
            
            # 获取所有以rank_data_开头的缓存键
            expired_keys = []
            for key in self.cache:
                if isinstance(key, str) and key.startswith('rank_data_'):
                    try:
                        # 从键名中提取日期
                        date_str = key.replace('rank_data_', '')
                        # 支持两种格式：YYYYMMDD 和 YYYYMMDD_HHMMSS
                        if len(date_str) == 8:  # 新格式：YYYYMMDD
                            cache_time = datetime.strptime(date_str, '%Y%m%d')
                        elif len(date_str) == 15:  # 旧格式：YYYYMMDD_HHMMSS
                            cache_time = datetime.strptime(date_str, '%Y%m%d_%H%M%S')
                        else:
                            continue
                        
                        # 检查是否过期
                        if cache_time < expire_threshold:
                            expired_keys.append(key)
                    except ValueError:
                        # 如果时间戳格式不正确，跳过
                        continue
            
            # 删除过期的缓存
            if expired_keys:
                for key in expired_keys:
                    del self.cache[key]
                logger.info(f"🧹 清理过期缓存: {len(expired_keys)}个项目")
                
        except Exception as e:
            logger.error(f"清理过期缓存失败: {str(e)}")
    
    def is_trading_time(self) -> bool:
        """检查当前是否为交易时间
        
        判断当前时间是否在股票市场的交易时间范围内。
        用于控制数据更新的频率，避免在非交易时间进行不必要的数据获取。
        
        Returns:
            bool: 如果当前时间在交易时间内返回True，否则返回False
            
        Note:
            交易时间定义:
            - 交易日：周一至周五（排除周末）
            - 上午交易时间：09:15 - 11:30
            - 下午交易时间：13:00 - 15:00
            - 不考虑节假日，实际使用时可能需要额外的节假日判断
            - 包含集合竞价时间（09:15开始）
        """
        now = datetime.now()
        current_time = now.time()
        
        # 交易时间：9:15-11:30, 13:00-15:00
        morning_start = datetime.strptime('09:15', '%H:%M').time()
        morning_end = datetime.strptime('11:30', '%H:%M').time()
        afternoon_start = datetime.strptime('13:00', '%H:%M').time()
        afternoon_end = datetime.strptime('15:00', '%H:%M').time()
        
        # 检查是否为工作日（周一到周五）
        if now.weekday() >= 5:  # 周六、周日
            return False
            
        # 检查是否在交易时间内
        is_morning = morning_start <= current_time <= morning_end
        is_afternoon = afternoon_start <= current_time <= afternoon_end
        
        return is_morning or is_afternoon
    
    async def run_update(self) -> bool:
        """执行一次数据更新
        
        获取所有配置的排行榜数据并保存到缓存中。
        这是数据更新的核心方法，负责协调整个数据获取和存储流程。
        
        Returns:
            bool: 更新成功返回True，失败返回False
            
        Process:
            1. 调用fetch_all_rank_data()获取所有排行榜数据
            2. 如果数据获取成功，调用save_rank_data()保存到缓存
            3. 记录详细的性能统计信息（耗时、榜单数、记录数）
            4. 异常处理确保程序稳定性
            
        Note:
            - 会记录开始和完成的日志信息
            - 统计并显示获取的榜单数量和总记录数
            - 计算并记录整个更新过程的耗时
            - 所有异常都会被捕获并记录，不会中断程序运行
            - 用于定时任务和手动更新两种场景
        """
        try:
            logger.info("🌐 我是定时==任务，开始实时请求排行榜数据...")
            start_time = time.time()
            
            print("run_update调用的")
            # 获取所有排行榜数据
            rank_data = await self.fetch_all_rank_data()
            
            if rank_data:
                # 保存数据
                cache_key = self.save_rank_data(rank_data)
                
                elapsed_time = time.time() - start_time
                total_records = sum(len(data.get('data', [])) for data in rank_data.values())
                
                logger.info(f"🌐 我是定时==任务实时数据获取完成 - 耗时: {elapsed_time:.2f}秒, 榜单: {len(rank_data)}个, 记录: {total_records}条")
                
                return True
            else:
                logger.error("未获取到任何排行榜数据")
                return False
                
        except Exception as e:
            logger.error(f"数据更新失败: {str(e)}")
            return False
    
    def update_job(self):
        """定时更新任务
        
        定时任务的入口方法，由调度器定期调用。
        包含交易时间检查和异步任务的同步执行逻辑。
        
        Note:
            - 首先检查当前是否为交易时间，非交易时间会跳过更新
            - 创建新的事件循环来执行异步的数据更新任务
            - 确保事件循环在使用后被正确关闭，避免资源泄漏
            - 所有异常都会被捕获并记录，不会影响调度器的正常运行
            - 适用于APScheduler等定时任务框架
            
        Process:
            1. 检查交易时间
            2. 创建新的事件循环
            3. 在事件循环中执行run_update()
            4. 记录执行结果
            5. 清理事件循环资源
        """
        if ENABLE_TRADING_TIME_CHECK and not self.is_trading_time():
            logger.info("当前非交易时间，跳过数据更新")
            return
            
        logger.info("开始定时数据更新...")
        
        # 在新的事件循环中运行异步任务
        def run_async_update():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                success = loop.run_until_complete(self.run_update())
                if success:
                    logger.info("定时数据更新成功")
                else:
                    logger.warning("定时数据更新失败")
            except Exception as e:
                logger.error(f"定时数据更新异常: {str(e)}")
            finally:
                loop.close()
        
        # 在线程中运行以避免阻塞
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            try:
                future = executor.submit(run_async_update)
                future.result(timeout=120)  # 2分钟超时
            except concurrent.futures.TimeoutError:
                logger.error("定时更新任务超时")
            except Exception as e:
                logger.error(f"定时更新任务失败: {str(e)}")
    
    def setup_schedule(self, refresh_interval=30):
        """设置定时任务
        
        配置定时任务调度器，设置数据更新的执行间隔和调度策略。
        这是定时数据更新系统的配置入口，支持灵活的时间间隔设置。
        
        Args:
            refresh_interval (int): 数据刷新间隔，单位为秒，默认30秒
                                   建议设置范围：10-300秒
                                   过短可能导致频繁请求，过长可能数据不够实时
                                   
        Note:
            - 使用schedule库进行任务调度
            - 任务会在交易时间内按指定间隔执行
            - 非交易时间会自动跳过更新（如果启用交易时间检查）
            - 每次调用会清除之前的调度设置
            - 需要调用start_scheduler()才能开始执行
            
        Process:
            1. 清除现有的调度任务
            2. 设置新的定时任务
            3. 记录调度配置信息
            
        Example:
            >>> crawler.setup_schedule(60)  # 每60秒更新一次
            >>> crawler.start_scheduler()   # 开始执行调度
        """
        # 交易时间内按指定间隔执行
        schedule.every(refresh_interval).seconds.do(self.update_job)
        
        logger.info(f"定时任务已设置：交易时间内每{refresh_interval}秒更新一次数据")
        logger.info("交易时间：周一至周五 9:15-11:30, 13:00-15:00")
    
    def start_scheduler(self):
        """启动定时任务调度器
        
        开始执行定时任务调度，进入持续运行状态直到被停止。
        这是定时数据更新系统的执行入口，会阻塞当前线程直到调度器被停止。
        
        Note:
            - 这是一个阻塞方法，会持续运行直到stop_scheduler()被调用
            - 每秒检查一次是否有待执行的任务
            - 设置running标志为True，表示调度器处于活跃状态
            - 适合在主线程中运行，或者使用start_scheduler_in_thread()在后台运行
            - 支持优雅停止，通过running标志控制循环退出
            
        Process:
            1. 设置运行状态标志
            2. 记录启动日志
            3. 进入调度循环，每秒检查待执行任务
            4. 执行到期的任务
            5. 直到running标志被设为False时退出
            
        Example:
            >>> crawler.setup_schedule(60)
            >>> crawler.start_scheduler()  # 阻塞运行
            
            # 或者在后台运行
            >>> crawler.start_scheduler_in_thread()
        """
        self.running = True
        
        logger.info("定时任务调度器已启动")
        
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def stop_scheduler(self):
        """停止定时任务调度器
        
        优雅地停止定时任务调度器，清理所有调度任务和资源。
        这是定时数据更新系统的停止入口，确保系统能够安全退出。
        
        Note:
            - 设置running标志为False，使调度循环退出
            - 清除所有已设置的定时任务
            - 不会中断正在执行的任务，等待其自然完成
            - 可以安全地多次调用，不会产生副作用
            - 停止后可以重新调用setup_schedule()和start_scheduler()重启
            
        Process:
            1. 设置运行状态标志为False
            2. 清除所有调度任务
            3. 记录停止日志
            
        Example:
            >>> crawler.stop_scheduler()  # 停止调度器
            >>> # 可以重新启动
            >>> crawler.setup_schedule(30)
            >>> crawler.start_scheduler()
        """
        self.running = False
        schedule.clear()
        logger.info("定时任务调度器已停止")
    
    def start_scheduler_in_thread(self):
        """在后台线程中启动定时任务调度器
        
        在独立的后台线程中启动定时任务调度器，避免阻塞主线程。
        这是非阻塞启动调度器的便捷方法，适用于需要同时处理其他任务的场景。
        
        Returns:
            threading.Thread: 运行调度器的线程对象，可用于监控线程状态
            
        Note:
            - 创建守护线程(daemon=True)，主程序退出时会自动终止
            - 非阻塞方法，调用后立即返回，调度器在后台运行
            - 返回的线程对象可用于检查线程状态或等待线程结束
            - 调度器仍然可以通过stop_scheduler()方法停止
            - 适合在Web应用或GUI应用中使用
            
        Process:
            1. 定义内部函数包装start_scheduler()调用
            2. 创建守护线程
            3. 启动线程
            4. 记录启动日志
            5. 返回线程对象
            
        Example:
            >>> crawler.setup_schedule(60)
            >>> thread = crawler.start_scheduler_in_thread()
            >>> # 主线程可以继续执行其他任务
            >>> # ...
            >>> crawler.stop_scheduler()  # 停止后台调度器
            >>> thread.join()  # 等待线程结束
        """
        def run_scheduler():
            self.start_scheduler()
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("定时任务调度器已在后台线程中启动")
        return scheduler_thread
    
    def print_latest_data_summary(self):
        """打印最新数据摘要
        
        从缓存中读取最新的排行榜数据并以格式化的方式打印摘要信息。
        这是一个便捷的数据查看工具，用于快速了解当前缓存的数据状态和内容概览。
        
        Note:
            - 显示数据更新时间和总记录数
            - 按榜单类型分别显示记录数量
            - 展示每个榜单的前3名组合信息
            - 如果没有缓存数据，会显示相应提示
            - 输出格式友好，便于人工查看
            
        Output Format:
            === 最新排行榜数据摘要 ===
            更新时间: 2024-01-01T12:00:00
            总记录数: 500条
              日榜: 100条
                前3名:
                  1. 组合名称 (累计收益率: 15.5%)
                  2. 组合名称 (累计收益率: 12.3%)
                  3. 组合名称 (累计收益率: 10.8%)
              ...
            
        Example:
            >>> crawler.print_latest_data_summary()
        """
        data = self.load_latest_rank_data()
        if data:
            print(f"\n=== 最新排行榜数据摘要 ===")
            print(f"更新时间: {data.get('update_time', 'Unknown')}")
            print(f"总记录数: {data.get('total_records', 0)}条")
            
            for rank_type, rank_data in data.get('data', {}).items():
                rank_name = self.rank_types.get(rank_type, {}).get('name', rank_type)
                record_count = len(rank_data.get('data', []))
                print(f"  {rank_name}: {record_count}条")
                
                # 显示前3名
                if rank_data.get('data'):
                    print(f"    前3名:")
                    for i, item in enumerate(rank_data['data'][:3], 1):
                        name = item.get('zhuheName', 'Unknown')
                        rate = item.get('rateForApp', 'N/A')
                        rate_title = item.get('rateTitle', '')
                        print(f"      {i}. {name} ({rate_title}: {rate}%)")
            print("=" * 40)
        else:
            print("暂无缓存数据")
    
    async def get_cached_rank_list_with_auto_update(self) -> Dict[str, Any]:
        """读取缓存中的组合列表，若获取的数据为0，尝试重新更新组合列表
        
        智能缓存数据获取方法，优先使用缓存数据，缓存无效时自动触发实时更新。
        这是一个高可用性的数据获取接口，确保在任何情况下都能提供有效的排行榜数据。
        
        Returns:
            Dict[str, Any]: 排行榜数据字典，包含以下结构:
                {
                    'timestamp': '时间戳',
                    'date': '日期',
                    'update_time': '更新时间',
                    'total_records': 总记录数,
                    'data': {
                        'daily': {'result': '0', 'data': [...]},
                        'weekly': {'result': '0', 'data': [...]},
                        ...
                    }
                }
                
        Strategy:
            1. 优先尝试从缓存加载数据
            2. 检查缓存数据的有效性（记录数 > 0）
            3. 如果缓存无效，自动触发实时数据更新
            4. 返回更新后的数据或空字典
            
        Performance:
            - 缓存命中：通常 < 0.1秒
            - 实时更新：通常 10-30秒（取决于网络和数据量）
            - 包含详细的性能统计和状态提示
            
        Note:
            - 自动处理缓存失效和数据为空的情况
            - 提供友好的进度提示和错误处理
            - 异常情况下返回空字典，不会抛出异常
            - 适用于需要高可用性的数据获取场景
            - 记录详细的执行时间和数据统计
            
        Example:
            >>> data = await crawler.get_cached_rank_list_with_auto_update()
            >>> if data:
            >>>     print(f"获取到 {data['total_records']} 条记录")
        """
        import time
        start_time = time.time()
        
        # 首先尝试从缓存加载数据
        cached_data = self.load_latest_rank_data()
        
        if cached_data:
            total_records = cached_data.get('total_records', 0)
            
            # 检查数据是否为空
            if total_records > 0:
                elapsed_time = time.time() - start_time
                print(f"📋 使用缓存数据 - 耗时: {elapsed_time:.2f}秒, 数据: {total_records}条")
                return cached_data
        
        # 如果缓存为空或不存在，重新获取数据
        try:
            print("🔄 缓存无效，开始实时请求排行榜数据...")
            update_start = time.time()
            success = await self.run_update()
            
            if success:
                # 重新加载更新后的数据
                updated_data = self.load_latest_rank_data()
                if updated_data:
                    total_records = updated_data.get('total_records', 0)
                    elapsed_time = time.time() - update_start
                    print(f"✅ 实时数据获取完成 - 耗时: {elapsed_time:.2f}秒, 数据: {total_records}条")
                    return updated_data
                else:
                    print("❌ 实时请求后仍无法获取数据")
                    return {}
            else:
                print("❌ 实时数据获取失败")
                return {}
                
        except Exception as e:
            elapsed_time = time.time() - start_time
            print(f"❌ 实时数据获取异常 - 耗时: {elapsed_time:.2f}秒, 错误: {str(e)}")
            logger.error(f"重新获取数据失败: {str(e)}")
            return {}
    
    async def fetch_portfolio_holdings(self, portfolio_id: str, rec_count: int = 50) -> Optional[Dict[str, Any]]:
        """获取单个组合的调仓记录
        
        从API获取指定投资组合的持仓变化记录，包括买入、卖出等交易操作。
        这是获取组合交易明细的核心方法，用于分析组合的调仓行为和交易策略。
        
        Args:
            portfolio_id (str): 组合ID，用于唯一标识一个投资组合
            rec_count (int): 获取记录数量，默认50条
                           建议范围：10-100条
                           过多可能影响性能，过少可能遗漏重要信息
                           
        Returns:
            Optional[Dict[str, Any]]: 成功时返回调仓记录数据，失败时返回None
                返回的数据结构:
                {
                    'result': '0',  # 成功标识
                    'data': [
                        {
                            'tzrq': '调仓日期',
                            'stkMktCode': '股票代码',
                            'stkName': '股票名称',
                            'cwhj_mr': '买入持仓变化',
                            'cwhj_mc': '卖出持仓变化',
                            'cjjg_mr': '买入成交价格',
                            'cjjg_mc': '卖出成交价格',
                            ...
                        },
                        ...
                    ]
                }
                
        Process:
            1. 构造API请求URL
            2. 发送HTTP请求获取调仓数据
            3. 解析响应数据并验证结果
            4. 返回结构化的调仓记录
            
        Note:
            - 使用rt_hold_change72接口获取调仓记录
            - 使用信号量控制并发数量，避免被服务器封IP
            - 自动处理HTTP会话管理
            - 包含完整的错误处理和日志记录
            - 超时设置为10秒，避免长时间等待
            - 每个请求间隔100ms，进一步降低被封IP的风险
            - 异常情况会被捕获并记录，不会影响其他组合的处理
            - 用于tc_list方法的数据获取
            
        Example:
            >>> data = await crawler.fetch_portfolio_holdings('12345', 30)
            >>> if data:
            >>>     records = data.get('data', [])
            >>>     print(f"获取到 {len(records)} 条调仓记录")
        """
        url = f"https://emdcspzhapi.dfcfs.cn/rtV1?type=rt_hold_change72&zh={portfolio_id}&recIdx=1&recCnt={rec_count}"
        
        # 使用信号量控制并发数量
        async with self.semaphore:
            try:
                if not self.session:
                    await self.create_session()
                
                async with self.session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('result') == '0':
                            return data
                        else:
                            logger.warning(f"获取组合{portfolio_id}调仓记录失败: {data.get('message', 'Unknown error')}")
                            return None
                    else:
                        logger.error(f"获取组合{portfolio_id}调仓记录HTTP错误: {response.status}")
                        return None
                        
            except Exception as e:
                logger.error(f"获取组合{portfolio_id}调仓记录异常: {str(e)}")
                return None
            finally:
                # 在信号量保护下添加请求间隔，进一步降低被封IP的风险
                await asyncio.sleep(0.1)  # 每个请求间隔100ms
    
    def parse_trading_action(self, cwhj_mr: str, cwhj_mc: str) -> Optional[str]:
        """解析交易动作（买入/卖出）
        
        根据持仓变化数据判断交易动作类型，用于识别组合的买入或卖出操作。
        这是交易记录分析的核心函数，确保准确识别投资组合的交易行为。
        
        Args:
            cwhj_mr (str): 持仓变化买入数据，来自API的cwhj_mr字段
                          - 非"-"值表示有买入操作
                          - 可能包含百分比数据（如"10.5%"）或"成"字符
                          - "-"表示无买入操作
            cwhj_mc (str): 持仓变化卖出数据，来自API的cwhj_mc字段
                          - 非"-"值表示有卖出操作
                          - 可能包含百分比数据（如"8.3%"）或"成"字符
                          - "-"表示无卖出操作
                          
        Returns:
            Optional[str]: 交易动作类型
                - "买入": 检测到买入操作
                - "卖出": 检测到卖出操作  
                - None: 无有效的交易操作
                
        Logic:
            1. 优先检查买入操作：cwhj_mr不为空且不为"-"
            2. 其次检查卖出操作：cwhj_mc不为空且不为"-"
            3. 都不满足则返回None
            
        Note:
            - 买入操作优先级高于卖出操作
            - 包含"成"字符的数据也被视为有效操作
            - 空字符串和"-"都被视为无操作
            - 用于tc_list数据处理和交易记录统计
        """
        # cwhj_mr 若不是-，则代表买入（包含"成"的比例也是有效的买入操作）
        if cwhj_mr and cwhj_mr != "-":
            return "买入"
        
        # cwhj_mc 若不是-，则代表卖出（包含"成"的比例也是有效的卖出操作）
        if cwhj_mc and cwhj_mc != "-":
            return "卖出"
        
        return None
    
    def is_a_stock_trading_day(self, date: datetime) -> bool:
        """判断是否为A股交易日
        
        A股交易日规则：
        - 周一到周五
        - 排除法定节假日（这里简化处理，只排除周末）
        """
        # 周一=0, 周日=6
        weekday = date.weekday()
        # A股交易日：周一到周五（0-4）
        return weekday < 5
    
    def get_last_trading_day(self, from_date: datetime) -> datetime:
        """获取指定日期之前的最近一个A股交易日"""
        current_date = from_date
        while True:
            current_date = current_date - timedelta(days=1)
            if self.is_a_stock_trading_day(current_date):
                return current_date
    
    def get_target_date(self, days_back: int = 0) -> str:
        """获取目标日期（根据A股交易日规则）
        
        根据当前时间和A股交易规则计算应该获取数据的目标日期。
        主要用于调仓记录查询，确保获取到有效的交易日数据。
        
        Args:
            days_back (int): 往前推几天开始计算，默认为0（当天）
            
        Returns:
            str: 目标日期字符串，格式为YYYYMMDD
            
        Rules:
            - 0点到8点30分：调仓日期是前一个A股交易日
            - 8点30分到24点：调仓日期是当天，若当天不是A股交易日，则取上一个最近的交易日
            
        Note:
            - 考虑了A股的交易时间特点和数据更新规律
            - 自动处理周末和非交易日的情况
            - 支持历史日期查询（通过days_back参数）
            - 返回的日期格式与API接口要求保持一致
        """
        now = datetime.now()
        current_time = now.time()
        
        # 应用days_back偏移
        base_date = now - timedelta(days=days_back)
        
        # 根据时间规则确定目标日期
        if current_time < datetime.strptime('08:30', '%H:%M').time():
            # 0点到8点30分：取前一个A股交易日
            target_date = self.get_last_trading_day(base_date)
        else:
            # 8点30分到24点：取当天，若不是交易日则取上一个交易日
            if self.is_a_stock_trading_day(base_date):
                target_date = base_date
            else:
                target_date = self.get_last_trading_day(base_date)
        
        return target_date.strftime('%Y%m%d')
    
    async def tc_list(self, days_back: int = 0, max_days_search: int = 3, use_cache: bool = True) -> List[Dict[str, Any]]:
        """获取组合调仓记录列表
        
        获取指定时间范围内所有投资组合的调仓记录，包括买入和卖出操作。
        这是获取市场调仓动态的核心接口，支持灵活的时间范围和缓存策略。
        
        Args:
            days_back (int): 往前推几天开始查找，默认为0（当天或前一天）
            max_days_search (int): 最多搜索几天的数据，默认为3天
            use_cache (bool): 是否使用缓存数据，默认为True
                             False表示实时请求最新数据
                             
        Returns:
            List[Dict[str, Any]]: 调仓记录列表，每条记录包含:
                {
                    '日期': '20231201',
                    '组合ID': 'portfolio_id',
                    '组合名称': '组合名称',
                    '调仓股票': '000001',
                    '股票名称': '平安银行',
                    '调仓情况': '买入'/'卖出',
                    '成交价格': '10.123',
                    '持仓比例': '15.5%'
                }
                
        Process:
            1. 获取组合列表数据（缓存或实时）
            2. 计算目标日期范围
            3. 收集所有组合的ID和名称
            4. 异步并发获取每个组合的调仓记录
            5. 解析和过滤目标日期范围内的记录
            6. 统计并返回结果
            
        Note:
            - 使用异步并发提高数据获取效率
            - 支持进度条显示获取进度
            - 自动处理异常，单个组合失败不影响整体结果
            - 根据A股交易规则智能计算目标日期
        """
        import time
        start_time = time.time()
        
        # 1. 获取组合列表数据
        if use_cache:
            print("📋 使用缓存数据获取组合列表")
            cached_data = await self.get_cached_rank_list_with_auto_update()
        else:
            print("🌐 tclist接口，实时请求组合列表数据")
            print("tc_list_调用的")
            cached_data = await self.fetch_all_rank_data()
            
            # 如果实时请求失败，回退到缓存数据
            if not cached_data or not cached_data.get('data'):
                print("⚠️ 实时请求失败，回退到缓存数据")
                cached_data = await self.get_cached_rank_list_with_auto_update()
        
        if not cached_data or not cached_data.get('data'):
            print("❌ 无法获取组合列表数据")
            return []
        
        # 2. 获取目标日期列表
        target_dates = []
        for i in range(max_days_search):
            date = self.get_target_date(days_back + i)
            target_dates.append(date)
        
        # 3. 收集所有组合ID和名称
        portfolios = []
        for rank_type, rank_data in cached_data.get('data', {}).items():
            for item in rank_data.get('data', []):
                portfolio_id = item.get('zjzh')
                portfolio_name = item.get('zhuheName')
                if portfolio_id and portfolio_name:
                    portfolios.append({
                        'id': portfolio_id,
                        'name': portfolio_name
                    })
        
        # 4. 异步获取所有组合的调仓记录
        # 创建异步任务
        tasks = []
        for portfolio in portfolios:
            task = self.fetch_portfolio_holdings(portfolio['id'])
            tasks.append((portfolio, task))
        
        # 执行异步任务
        results = []
        with tqdm(total=len(tasks), desc="获取调仓记录", unit="个") as pbar:
            for portfolio, task in tasks:
                try:
                    holdings_data = await task
                    pbar.set_postfix(组合=portfolio['name'][:10])
                    
                    if holdings_data and holdings_data.get('data'):
                        # 解析调仓记录
                        for record in holdings_data['data']:
                            tzrq = record.get('tzrq', '')
                            
                            # 只要目标日期范围内的数据
                            if tzrq in target_dates:
                                cwhj_mr = record.get('cwhj_mr', '-')
                                cwhj_mc = record.get('cwhj_mc', '-')
                                
                                # 解析交易动作
                                action = self.parse_trading_action(cwhj_mr, cwhj_mc)
                                
                                if action:
                                    result_record = {
                                        '日期': tzrq,
                                        '组合ID': portfolio['id'],
                                        '组合名称': portfolio['name'],
                                        '调仓股票': record.get('stkMktCode', ''),
                                        '股票名称': record.get('stkName', ''),
                                        '调仓情况': action,
                                        '成交价格': record.get('cjjg_mr' if action == '买入' else 'cjjg_mc', '-'),
                                        '持仓比例': cwhj_mr if action == '买入' else cwhj_mc
                                    }
                                    results.append(result_record)
                    
                except Exception as e:
                    logger.error(f"处理组合{portfolio['name']}调仓记录失败: {str(e)}")
                
                pbar.update(1)
        
        # 5. 输出结果统计
        elapsed_time = time.time() - start_time
        print(f"📈 调仓记录获取完成 - 耗时: {elapsed_time:.2f}秒, 组合: {len(portfolios)}个, 记录: {len(results)}条")
        
        return results
    
    async def get_stock_summary(self, days_back=0, max_days_search=3):
        """获取股票调仓汇总数据
        
        按股票维度汇总调仓数据，统计每只股票的买入和卖出组合数量。
        这是分析市场热点和资金流向的重要工具。
        
        Args:
            days_back (int): 往前推几天开始统计，默认为0
            max_days_search (int): 最多搜索几天的数据，默认为3天
            
        Returns:
            List[Dict[str, Any]]: 股票汇总数据列表，按买入组合数降序排序
                每条记录包含:
                {
                    '股票代码': '000001',
                    '股票名称': '平安银行',
                    '买入组合': [组合信息列表],
                    '卖出组合': [组合信息列表],
                    '买入组合数': 5,
                    '卖出组合数': 2
                }
                
        Process:
            1. 获取指定时间范围的调仓记录（实时数据）
            2. 获取组合排名信息用于标签显示
            3. 按股票代码汇总买入和卖出操作
            4. 为组合名称添加排行榜标签
            5. 计算统计数据并排序
            
        Note:
            - 强制使用实时数据确保准确性
            - 自动去重相同组合的重复操作
            - 组合名称包含排行榜标签（日榜、周榜等）
            - 结果按买入组合数量降序排列，便于发现热门股票
        """
        # 获取调仓记录（实时请求，不使用缓存）
        records = await self.tc_list(days_back=days_back, max_days_search=max_days_search, use_cache=False)
        
        # 获取组合排名信息
        cached_data = await self.get_cached_rank_list_with_auto_update()
        portfolio_ranks = {}
        if cached_data and cached_data.get('data'):
            for rank_type, rank_data in cached_data.get('data', {}).items():
                rank_name = self.rank_types.get(rank_type, {}).get('name', rank_type)
                for idx, item in enumerate(rank_data.get('data', []), 1):
                    portfolio_id = item.get('zjzh')
                    portfolio_name = item.get('zhuheName')
                    if portfolio_id and portfolio_name:
                        if portfolio_name not in portfolio_ranks:
                            portfolio_ranks[portfolio_name] = []
                        # 精简排名信息："日榜第3名" -> "日榜3"
                        portfolio_ranks[portfolio_name].append(f"{rank_name}{idx}")
        
        # 按股票汇总
        stock_summary = {}
        
        for record in records:
            stock_code = record['调仓股票']
            stock_name = record['股票名称']
            action = record['调仓情况']
            portfolio_name = record['组合名称']
            portfolio_id = record['组合ID']
            
            # 创建股票键
            stock_key = f"{stock_code}_{stock_name}"
            
            if stock_key not in stock_summary:
                stock_summary[stock_key] = {
                    '股票代码': stock_code,
                    '股票名称': stock_name,
                    '买入组合': [],
                    '卖出组合': [],
                    '买入组合数': 0,
                    '卖出组合数': 0
                }
            
            # 获取组合排名信息并添加到显示名称中
            portfolio_rank_info = portfolio_ranks.get(portfolio_name, [])
            if portfolio_rank_info:
                # 格式化为带tag的HTML格式
                tags_html = ''
                for rank in portfolio_rank_info:
                    # 提取榜单类型（日榜、周榜、月榜、年榜、总榜）
                    rank_type = ''
                    if '日榜' in rank:
                        rank_type = '日榜'
                    elif '周榜' in rank:
                        rank_type = '周榜'
                    elif '月榜' in rank:
                        rank_type = '月榜'
                    elif '年榜' in rank:
                        rank_type = '年榜'
                    elif '总榜' in rank:
                        rank_type = '总榜'
                    
                    if rank_type:
                        tags_html += f'<span class="rank-tag" data-rank-type="{rank_type}">{rank}</span>'
                
                portfolio_display = f'<span class="combo-name" data-combo-id="{portfolio_id}" data-combo-name="{portfolio_name}">{portfolio_name}</span>{tags_html}'
            else:
                portfolio_display = f'<span class="combo-name" data-combo-id="{portfolio_id}" data-combo-name="{portfolio_name}">{portfolio_name}</span>'
            
            # 统计买入卖出组合（使用原始组合名称进行去重，显示时使用带标签的名称）
            if action == '买入':
                if portfolio_name not in [name.split('<span')[0] for name in stock_summary[stock_key]['买入组合']]:
                    stock_summary[stock_key]['买入组合'].append(portfolio_display)
            elif action == '卖出':
                if portfolio_name not in [name.split('<span')[0] for name in stock_summary[stock_key]['卖出组合']]:
                    stock_summary[stock_key]['卖出组合'].append(portfolio_display)
        
        # 转换为列表并计算组合数
        result = []
        for stock_data in stock_summary.values():
            # 将HTML格式的组合信息转换为结构化数组
            buy_combos = []
            sell_combos = []
            
            # 处理买入组合
            for combo_html in stock_data['买入组合']:
                combo_info = self._parse_combo_html(combo_html)
                if combo_info:
                    buy_combos.append(combo_info)
            
            # 处理卖出组合
            for combo_html in stock_data['卖出组合']:
                combo_info = self._parse_combo_html(combo_html)
                if combo_info:
                    sell_combos.append(combo_info)
            
            # 更新数据结构
            stock_data['买入组合'] = buy_combos
            stock_data['卖出组合'] = sell_combos
            stock_data['买入组合数'] = len(buy_combos)
            stock_data['卖出组合数'] = len(sell_combos)
            result.append(stock_data)
        
        # 按买入组合数排序（降序）
        result.sort(key=lambda x: x['买入组合数'], reverse=True)
        
        return result

    async def get_portfolio_detail(self, portfolio_id: str) -> Optional[Dict[str, Any]]:
        """获取组合详情
        
        根据组合ID获取指定投资组合的详细信息，包括基本信息和持仓明细。
        这是获取单个投资组合完整信息的主要接口。
        
        Args:
            portfolio_id (str): 组合ID（zjzh字段值），用于唯一标识一个投资组合
            
        Returns:
            Optional[Dict[str, Any]]: 成功时返回组合的完整信息，失败或未找到时返回None
                  返回的数据包含组合基本信息和持仓明细:
                  {
                      '组合ID': 'portfolio_id',
                      '组合名称': '组合名称',
                      '管理人': '管理人昵称',
                      '总收益率': '累计收益率',
                      '今日收益率': '当日收益率',
                      '持仓信息': [
                          {
                              '股票代码': '000001',
                              '股票名称': '平安银行',
                              '成本价': '10.123',
                              '现价': '11.456',
                              '盈亏比': '13.12%',
                              '仓位': '15.5%'
                          },
                          ...
                      ]
                  }
                  
        Process:
            1. 构造API请求URL
            2. 发送HTTP请求获取组合详情数据
            3. 解析持仓信息，计算格式化数据
            4. 提取组合基本信息
            5. 构造并返回标准化的结果数据
            
        Note:
            - 使用rt_zhuhe_detail72接口获取详细数据
            - 自动处理数据类型转换和格式化
            - 持仓数据包含成本价、现价、盈亏比例、仓位比例等
            - 异常情况会被捕获并记录到日志中
            - 数据解析失败的持仓记录会被跳过，不影响其他数据
        """
        url = f"{self.base_url}?type=rt_zhuhe_detail72&zh={portfolio_id}"
        
        try:
            data = await self.fetch_with_retry(url)
            if data and data.get('result') == '0' and 'data' in data:
                portfolio_data = data['data']
                
                # 处理持仓信息
                positions = portfolio_data.get('position', [])
                processed_positions = []
                
                for pos in positions:
                    # 计算盈亏金额（假设按比例计算）
                    try:
                        current_price = float(pos.get('__zxjg', 0))
                        cost_price = float(pos.get('cbj', 0))
                        position_rate = float(pos.get('positionRateDetail', 0))
                        profit_rate = float(pos.get('webYkRate', 0))
                        
                        processed_pos = {
                            '股票代码': pos.get('__code', ''),
                            '股票名称': pos.get('__name', ''),
                            '成本价': f"{cost_price:.3f}",
                            '现价': f"{current_price:.3f}",
                            '盈亏比': f"{profit_rate:.2f}%",
                            '仓位': f"{position_rate:.1f}%"
                        }
                        processed_positions.append(processed_pos)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"处理持仓数据时出错: {e}")
                        continue
                
                # 获取组合基本信息
                detail = portfolio_data.get('detail', {})
                result = {
                    '组合ID': portfolio_id,
                    '组合名称': detail.get('zuheName', ''),
                    '管理人': detail.get('uidNick', ''),
                    '总收益率': detail.get('rate', ''),
                    '今日收益率': detail.get('rateDay', ''),
                    '持仓信息': processed_positions
                }
                
                return result
            else:
                logger.warning(f"获取组合详情失败: {data.get('message', 'Unknown error') if data else 'No data'}")
                return None
                
        except Exception as e:
            logger.error(f"获取组合详情异常: {str(e)}")
            return None


# 主程序入口
if __name__ == "__main__":
    """
    主程序入口
    
    当脚本直接运行时执行的代码，提供完整的股票组合排行榜数据爬取和定时更新功能。
    支持灵活的配置选项，可以根据需要调整爬取策略、更新频率和缓存设置。
    
    主要功能:
        1. 配置系统参数（去重、更新间隔、交易时间检查等）
        2. 初始化RankCrawler爬虫实例
        3. 执行首次数据更新
        4. 启动定时任务调度器
        5. 提供优雅的程序退出机制
        
    配置选项:
        - ENABLE_DEDUPLICATION: 是否启用数据去重功能
        - REFRESH_INTERVAL: 数据刷新间隔（秒）
        - ENABLE_TRADING_TIME_CHECK: 是否仅在交易时间更新数据
        - CACHE_EXPIRE_HOURS: 缓存数据过期时间（小时）
        - CACHE_DIR: 缓存文件存储目录
        
    运行流程:
        1. 读取配置参数
        2. 创建爬虫实例
        3. 执行首次数据更新
        4. 如果首次更新成功，启动定时任务
        5. 持续运行直到用户中断或异常退出
        
    Note:
        - 支持Ctrl+C优雅退出
        - 所有异常都会被捕获并记录
        - 适用于生产环境的长期运行
        - 可以通过修改配置参数调整运行行为
    """
    # ==================== 配置区域 ====================
    # 控制是否启用去重功能（True=启用，False=禁用）
    ENABLE_DEDUPLICATION = True
    
    # 数据刷新间隔（秒）
    REFRESH_INTERVAL = 100
    
    # 是否启用交易时间检查（True=仅交易时间更新，False=任何时间都更新）
    ENABLE_TRADING_TIME_CHECK = False
    
    # 缓存过期时间（小时）
    CACHE_EXPIRE_HOURS = 1
    
    # 缓存目录路径
    CACHE_DIR = "./data/cache"
    # ================================================
    
    # 根据配置初始化爬虫
    crawler = RankCrawler(cache_dir=CACHE_DIR, enable_deduplication=ENABLE_DEDUPLICATION)
    
    async def main():
        """主程序异步入口函数
        
        负责程序的完整生命周期管理，包括初始化、首次数据更新、定时任务启动和优雅退出。
        这是整个爬虫系统的核心控制逻辑，确保系统稳定可靠地运行。
        
        Process:
            1. 打印启动信息
            2. 执行首次数据更新（验证系统可用性）
            3. 如果首次更新成功，启动定时任务调度器
            4. 处理用户中断信号，优雅停止服务
            5. 记录程序状态和异常信息
            
        Note:
            - 首次更新失败会导致程序退出，避免无效的定时任务
            - 支持Ctrl+C优雅中断，确保资源正确释放
            - 所有关键操作都有日志记录
            - 定时任务使用配置的刷新间隔
            - 异常情况会被捕获并记录，不会导致程序崩溃
        """
        print("股票组合排行榜爬虫启动")
        
        # 程序启动时执行首次更新
        logger.info("程序启动，执行首次数据更新")
        success = await crawler.run_update()
        
        if success:
            # 启动定时任务
            print(f"定时任务启动 - 各榜单，组合列表，更新间隔: {REFRESH_INTERVAL}秒")
            
            try:
                # 使用配置的刷新间隔
                crawler.setup_schedule(REFRESH_INTERVAL)
                crawler.start_scheduler()
            except KeyboardInterrupt:
                print("\n正在停止...")
                crawler.stop_scheduler()
                print("程序已停止")
        else:
            logger.error("首次数据更新失败，程序退出")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.error(f"程序运行异常: {str(e)}")
    finally:
        logger.info("程序已退出")