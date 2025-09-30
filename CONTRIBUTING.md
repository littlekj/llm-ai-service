# 开发规范

## 环境准备

确保已安装 [Python 3.10+](https://www.python.org/downloads/) 和 [Poetry](https://python-poetry.org/docs/#installation)。

```bash
# 1. 克隆项目（如未完成）
git clone https://github.com/yourname/llm-ai-service.git
cd llm-ai-service

# 2. 安装依赖（Poetry 会自动创建并管理虚拟环境）
poetry install

# 3. 激活虚拟环境（进入 Poetry Shell）
poetry shell

# 或者不进入 shell，后续命令前加 `poetry run`
# 例如：poetry run uvicorn src.main:app --reload
```

## 代码规范

- **格式化 & 修复**：`poetry run ruff check . --fix`
- **类型检查**：`poetry run mypy src`
- **单元测试**：`poetry run pytest tests/unit -v --cov=src`
- **推荐提交前，全部运行检查**


## 提交流程

- 从 main 或 develop 分支创建 feature 分支：

```bash
git checkout -b feature/your-feature-name
```

- 完成功能开发 + 单元测试 + 文档更新
- 确保所有代码检查通过
- 提交代码并推送：

```bash
git add .
git commit -m "feat: 实现XXX功能"
git push origin feature/your-feature-name
```

- 在 GitHub/GitLab 发起 Pull Request / Merge Request
- 等待 CI 通过，团队 Review 后合并
