# Vercel 部署指南

本指南将帮助您将股票调仓记录Web应用部署到Vercel平台。

## 前置条件

1. 拥有Vercel账户（可通过GitHub、GitLab或Bitbucket注册）
2. 项目代码已推送到Git仓库（GitHub、GitLab或Bitbucket）
3. 安装了Vercel CLI（可选，用于本地测试）

## 部署步骤

### 方法一：通过Vercel网站部署（推荐）

1. **登录Vercel**
   - 访问 [vercel.com](https://vercel.com)
   - 使用GitHub/GitLab/Bitbucket账户登录

2. **导入项目**
   - 点击 "New Project"
   - 选择您的Git仓库
   - 选择包含此项目的仓库

3. **配置项目**
   - Project Name: 输入项目名称
   - Framework Preset: 选择 "Other"
   - Root Directory: 如果项目在子目录中，请指定路径
   - Build and Output Settings: 保持默认

4. **环境变量设置**
   - 在项目设置中添加以下环境变量（如果需要）：
     ```
     VERCEL_CACHE_DIR=/tmp
     ```

5. **部署**
   - 点击 "Deploy" 开始部署
   - 等待部署完成

### 方法二：通过Vercel CLI部署

1. **安装Vercel CLI**
   ```bash
   npm install -g vercel
   ```

2. **登录Vercel**
   ```bash
   vercel login
   ```

3. **部署项目**
   ```bash
   cd /path/to/your/project
   vercel
   ```

4. **按照提示配置**
   - 选择项目设置
   - 确认部署配置

## 项目文件说明

### 新增的部署文件

1. **vercel.json** - Vercel配置文件
   ```json
   {
     "version": 2,
     "builds": [
       {
         "src": "./index.py",
         "use": "@vercel/python"
       }
     ],
     "routes": [
       {
         "src": "/(.*)",
         "dest": "/"
       }
     ]
   }
   ```

2. **index.py** - Vercel入口文件
   - 将Flask应用适配为Vercel Serverless函数

3. **web_interface_vercel.py** - Vercel优化版本
   - 适配无状态环境
   - 使用临时目录作为缓存

4. **.vercelignore** - 部署时忽略的文件
   - 排除数据目录、缓存文件等

## 注意事项

### 限制和考虑

1. **无状态环境**
   - Vercel是无状态的Serverless环境
   - 每次请求都会创建新的实例
   - 本地缓存在请求间不会保持

2. **执行时间限制**
   - Hobby计划：10秒执行时间限制
   - Pro计划：60秒执行时间限制
   - 如果爬虫操作耗时较长，可能需要优化

3. **存储限制**
   - 无法持久化存储数据
   - 建议使用外部数据库或存储服务

4. **冷启动**
   - 首次访问可能较慢（冷启动）
   - 后续访问会更快

### 优化建议

1. **缓存策略**
   - 考虑使用Redis或其他外部缓存服务
   - 减少重复的网络请求

2. **异步处理**
   - 对于耗时操作，考虑使用后台任务
   - 可以结合Vercel的Edge Functions

3. **错误处理**
   - 增强错误处理和重试机制
   - 提供降级方案

## 访问应用

部署成功后，您将获得一个Vercel提供的URL，格式类似：
```
https://your-project-name.vercel.app
```

您也可以配置自定义域名。

## 故障排除

### 常见问题

1. **部署失败**
   - 检查requirements.txt中的依赖
   - 确保所有必要文件都已提交到Git仓库

2. **运行时错误**
   - 查看Vercel的Function Logs
   - 检查环境变量配置

3. **超时错误**
   - 优化爬虫逻辑，减少执行时间
   - 考虑分批处理数据

### 调试方法

1. **本地测试**
   ```bash
   vercel dev
   ```

2. **查看日志**
   - 在Vercel Dashboard中查看Function Logs
   - 使用vercel logs命令

3. **健康检查**
   - 访问 `/api/health` 端点检查应用状态

## 更新部署

每次推送到Git仓库的主分支时，Vercel会自动重新部署应用。您也可以手动触发部署。

## 成本考虑

- Vercel提供免费的Hobby计划
- 对于生产环境，建议使用Pro计划
- 注意Function执行时间和带宽使用量

---

如有问题，请参考[Vercel官方文档](https://vercel.com/docs)或联系技术支持。