# dcshipan

一个基于Python Flask的Web应用项目。

## 功能特性

- 基于Flask框架开发
- 支持数据爬取和排名功能
- 提供Web界面交互
- 支持Vercel云部署

## 快速部署

点击下面的按钮可以一键部署到Vercel：

[![Deploy with Vercel](https://vercel.com/button)](https://vercel.com/new/clone?repository-url=https://github.com/YOUR_USERNAME/dcshipan)

## 本地运行

1. 克隆项目
```bash
git clone https://github.com/YOUR_USERNAME/dcshipan.git
cd dcshipan
```

2. 安装依赖
```bash
pip install -r requirements.txt
```

3. 运行应用
```bash
python start.py
```

## 项目结构

- `index.py` - 主应用入口
- `web_interface.py` - Web界面逻辑
- `web_interface_vercel.py` - Vercel部署版本
- `rank_crawler.py` - 排名爬虫功能
- `templates/` - HTML模板文件
- `data/` - 数据存储目录
- `vercel.json` - Vercel部署配置

## 技术栈

- Python 3.x
- Flask
- HTML/CSS/JavaScript
- Vercel (部署平台)