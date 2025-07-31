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
    def __init__(self, cache_dir: str = "data/cache", enable_deduplication: bool = True):
        """初始化爬虫
        
        Args:
            cache_dir: 缓存目录路径
            enable_deduplication: 是否启用去重功能，默认为True
        """
        self.base_url = "https://emdcspzhapi.dfcfs.cn/rtV1"
        self.cache_dir = cache_dir
        self.enable_deduplication = enable_deduplication
        self.ensure_cache_dir()
        
        # 初始化DiskCache
        self.cache = dc.Cache(os.path.join(self.cache_dir, 'rank_cache'))
        
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
        
        logger.info(f"爬虫初始化完成，去重功能: {'启用' if self.enable_deduplication else '禁用'}")
        
    def ensure_cache_dir(self):
        """确保缓存目录存在"""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    async def create_session(self):
        """创建HTTP会话"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=100, limit_per_host=30)
            self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
    
    async def close_session(self):
        """关闭HTTP会话"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
    
    async def fetch_with_retry(self, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """带重试机制的HTTP请求"""
        await self.create_session()
            
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
        
        return None
    
    async def fetch_rank_data(self, rank_type: str, rank_config: Dict) -> Optional[Dict[str, Any]]:
        """获取单个排行榜数据（支持分页）"""
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
        """解析组合HTML字符串，提取结构化信息"""
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
        """获取所有排行榜数据"""
        await self.create_session()
        
        results = {}
        total_tasks = len(self.rank_types)
        
        with tqdm(total=total_tasks, desc="获取排行榜数据", unit="个") as pbar:
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
        """对排行榜数据进行去重，优先级：总榜>年榜>月榜>周榜>日榜"""
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
        """保存排行榜数据到DiskCache"""
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
        """从DiskCache加载最新的排行榜数据"""
        try:
            data = self.cache.get("latest_rank_data")
            if data is not None:
                return data
        except Exception as e:
            logger.error(f"从DiskCache加载排行榜数据失败: {str(e)}")
        
        return None
    
    def clean_expired_cache(self, expire_hours: int = 1):
        """清理过期的缓存数据"""
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
        """检查当前是否为交易时间"""
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
        """执行一次数据更新"""
        try:
            logger.info("🌐 开始实时请求排行榜数据...")
            start_time = time.time()
            
            # 获取所有排行榜数据
            rank_data = await self.fetch_all_rank_data()
            
            if rank_data:
                # 保存数据
                cache_key = self.save_rank_data(rank_data)
                
                elapsed_time = time.time() - start_time
                total_records = sum(len(data.get('data', [])) for data in rank_data.values())
                
                logger.info(f"🌐 实时数据获取完成 - 耗时: {elapsed_time:.2f}秒, 榜单: {len(rank_data)}个, 记录: {total_records}条")
                
                return True
            else:
                logger.error("未获取到任何排行榜数据")
                return False
                
        except Exception as e:
            logger.error(f"数据更新失败: {str(e)}")
            return False
    
    def update_job(self):
        """定时更新任务"""
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
        """设置定时任务"""
        # 交易时间内按指定间隔执行
        schedule.every(refresh_interval).seconds.do(self.update_job)
        
        logger.info(f"定时任务已设置：交易时间内每{refresh_interval}秒更新一次数据")
        logger.info("交易时间：周一至周五 9:15-11:30, 13:00-15:00")
    
    def start_scheduler(self):
        """启动定时任务调度器"""
        self.running = True
        
        logger.info("定时任务调度器已启动")
        
        while self.running:
            schedule.run_pending()
            time.sleep(1)
    
    def stop_scheduler(self):
        """停止定时任务调度器"""
        self.running = False
        schedule.clear()
        logger.info("定时任务调度器已停止")
    
    def start_scheduler_in_thread(self):
        """在后台线程中启动定时任务调度器"""
        def run_scheduler():
            self.start_scheduler()
        
        scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        scheduler_thread.start()
        logger.info("定时任务调度器已在后台线程中启动")
        return scheduler_thread
    
    def print_latest_data_summary(self):
        """打印最新数据摘要"""
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
        """读取缓存中的组合列表，若获取的数据为0，尝试重新更新组合列表"""
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
        """获取单个组合的调仓记录"""
        url = f"https://emdcspzhapi.dfcfs.cn/rtV1?type=rt_hold_change72&zh={portfolio_id}&recIdx=1&recCnt={rec_count}"
        
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
    
    def parse_trading_action(self, cwhj_mr: str, cwhj_mc: str) -> Optional[str]:
        """解析交易动作（买入/卖出）"""
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
        
        规则：
        - 0点到8点30分：调仓日期是前一个A股交易日
        - 8点30分到24点：调仓日期是当天，若当天不是A股交易日，则取上一个最近的交易日
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
        
        Args:
            days_back: 往前推几天开始查找（0表示当天或前一天）
            max_days_search: 最多搜索几天的数据
            use_cache: 是否使用缓存数据（False=实时请求）
        """
        import time
        start_time = time.time()
        
        # 1. 获取组合列表数据
        if use_cache:
            print("📋 使用缓存数据获取组合列表")
            cached_data = await self.get_cached_rank_list_with_auto_update()
        else:
            print("🌐 实时请求组合列表数据")
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
        """
        获取股票调仓汇总数据，按股票汇总买入组合数和卖出组合数
        
        Args:
            days_back: 往前推几天
            max_days_search: 最多搜索几天的数据
            
        Returns:
            list: 股票汇总数据列表，按买入组合数排序
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
        print("股票组合排行榜爬虫启动")
        
        # 程序启动时执行首次更新
        logger.info("程序启动，执行首次数据更新")
        success = await crawler.run_update()
        
        if success:
            # 启动定时任务
            print(f"定时任务启动 - 更新间隔: {REFRESH_INTERVAL}秒")
            
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