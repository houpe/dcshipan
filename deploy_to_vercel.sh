#!/bin/bash

# Vercel部署准备脚本
# 此脚本帮助准备项目以便部署到Vercel

echo "🚀 准备Vercel部署..."

# 检查是否安装了git
if ! command -v git &> /dev/null; then
    echo "❌ 错误: 未找到git命令，请先安装git"
    exit 1
fi

# 检查是否在git仓库中
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "📁 初始化Git仓库..."
    git init
    echo "✅ Git仓库已初始化"
fi

# 检查是否有.gitignore文件
if [ ! -f ".gitignore" ]; then
    echo "📝 创建.gitignore文件..."
    cat > .gitignore << EOF
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# 虚拟环境
venv/
env/
.venv/
.env/

# 数据和缓存
data/
*.log
*.cache

# IDE
.vscode/
.idea/
*.swp
*.swo

# 系统文件
.DS_Store
Thumbs.db
EOF
    echo "✅ .gitignore文件已创建"
fi

# 添加所有文件到git
echo "📦 添加文件到Git..."
git add .

# 检查是否有提交
if git diff --cached --quiet; then
    echo "ℹ️  没有新的更改需要提交"
else
    echo "💾 提交更改..."
    git commit -m "准备Vercel部署: 添加配置文件和优化代码"
    echo "✅ 更改已提交"
fi

echo ""
echo "🎉 Vercel部署准备完成！"
echo ""
echo "接下来的步骤:"
echo "1. 将代码推送到GitHub/GitLab/Bitbucket:"
echo "   git remote add origin <your-repo-url>"
echo "   git branch -M main"
echo "   git push -u origin main"
echo ""
echo "2. 访问 https://vercel.com 并导入您的仓库"
echo ""
echo "3. 或者使用Vercel CLI:"
echo "   npm install -g vercel"
echo "   vercel login"
echo "   vercel"
echo ""
echo "📖 详细部署指南请查看: VERCEL_DEPLOYMENT.md"
echo ""
echo "🔗 部署后的应用将包含以下功能:"
echo "   - 股票调仓汇总: /api/stock_summary"
echo "   - 调仓记录: /api/tc_list"
echo "   - 组合详情: /api/portfolio_detail/<id>"
echo "   - 健康检查: /api/health"
echo ""
echo "⚠️  注意事项:"
echo "   - Vercel有执行时间限制(免费版10秒)"
echo "   - 建议优化爬虫逻辑以减少执行时间"
echo "   - 数据不会在请求间持久化"