#https://emdcspzhapi.dfcfs.cn/rtV1?type=rt_get_rank&rankType=10005&recIdx=0&recCnt=50&rankid=0

import asyncio, aiohttp
import hashlib
import json
import diskcache

URL_TEMPLATE = "https://emdcspzhapi.dfcfs.cn/rtV1?type=rt_get_rank&rankType=10005&recIdx=0&recCnt=50&rankid=0"

sem = asyncio.Semaphore(10)               # 🔒 限制并发数
timeout = aiohttp.ClientTimeout(total=10)

async def fetch(session, zh):
    async with sem:                      # 控制并发
        async with session.get(URL_TEMPLATE, timeout=timeout) as resp:
            if resp.status == 200:
                js = await resp.json()
                return zh, js.get("data")
            return zh, None

async def main():
    # 初始化DiskCache
    import os
    cache_dir = os.path.join(os.getcwd(), 'data', 'cache')
    os.makedirs(cache_dir, exist_ok=True)
    cache = diskcache.Cache(cache_dir)
    
    async with aiohttp.ClientSession() as session:
        tasks = [fetch(session, "zh")]  # 将单个任务放在列表中
        result = dict(await asyncio.gather(*tasks))
        
        # 处理结果中的data数据
        for key, data in result.items():
            if data is not None:
                # 将data转换为JSON字符串并计算MD5
                data_json = json.dumps(data, sort_keys=True, ensure_ascii=False)
                md5_key = hashlib.md5(data_json.encode('utf-8')).hexdigest()
                
                # 存储到DiskCache中
                cache[md5_key] = data
                print(f"数据已存储到缓存，MD5 key: {md5_key}")
                print(f"数据条数: {len(data) if isinstance(data, list) else 'N/A'}")
        
        print(f"结果: {result}")
        return result

asyncio.run(main())