from logging.config import fileConfig

from sqlalchemy import engine_from_config
from sqlalchemy import pool, create_engine

from alembic import context

# 项目模型导入
from src.config.settings import settings
from src.models import Base
from src import models  # 确保所有模型被加载

if not settings.DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in environment or settings")
url = settings.DATABASE_URL


# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
# from myapp import mymodel
# target_metadata = mymodel.Base.metadata
target_metadata = Base.metadata   # 用于自动生成迁移

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # url = config.get_main_option("sqlalchemy.url")
    
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,            # 自动检测字段类型变化
        compare_server_default=True,  # 自动检测字段默认值变化
        render_as_batch=True,         # 批量模式（对 SQLite/MySQL 友好）
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # connectable = engine_from_config(
    #     config.get_section(config.config_ini_section, {}),
    #     prefix="sqlalchemy.",
    #     poolclass=pool.NullPool,
    # )
    
    # 直接使用 DATABASE_URL 创建引擎（绕过 alembic.ini）
    connectable = create_engine(
        url,
        poolclass=pool.NullPool,
        echo=False,  # 生产建议关闭
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata,
            compare_type=True,            # 自动检测字段类型变化
            compare_server_default=True,  # 自动检测字段默认值变化
            render_as_batch=True,         # 批量模式（对 SQLite/MySQL 友好）
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
