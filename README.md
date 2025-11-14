# LLM-AI-Service — 生产级 RAG 知识库系统

> 支持文档上传、智能问答、权限控制、配额管理、审计日志、高可用部署

## 快速开始

```bash
docker-compose up -d
curl http://localhost:8000/api/v1/health
```

## 项目结构

```
src/
├── api/          # 路由与控制器
├── core/         # 配置、依赖注入、中间件
├── models/       # SQLAlchemy 模型
├── schemas/      # Pydantic 输入输出模型
├── services/     # 业务逻辑
├── utils/        # 工具函数
└── workers/      # Celery 任务
```

## 开发指南

详见 CONTRIBUTING.md