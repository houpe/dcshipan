#!/usr/bin/env python3
import diskcache
import os
import json

# 初始化缓存
cache_dir = os.path.join(os.getcwd(), 'data', 'cache')
cache = diskcache.Cache(cache_dir)

print(f"缓存目录: {cache_dir}")
print(f"缓存中的键数量: {len(cache)}")

# 列出所有键
for key in cache:
    print(f"\nMD5 Key: {key}")
    data = cache[key]
    if isinstance(data, list):
        print(f"数据类型: 列表，包含 {len(data)} 个元素")
        if len(data) > 0:
            print(f"第一个元素: {json.dumps(data[0], ensure_ascii=False, indent=2)}")
    else:
        print(f"数据类型: {type(data)}")
        print(f"数据内容: {data}")