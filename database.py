from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import models
from contextlib import asynccontextmanager
import os

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://postgres:postgres@localhost:5434/mydb"
    )

if not DATABASE_URL:
    raise ValueError("DATABASE_URL не установлен в переменных окружения")


engine = create_async_engine(
        url=DATABASE_URL,
        pool_pre_ping = True,
        echo = False
        )
    
Session = async_sessionmaker(
    bind=engine, 
    expire_on_commit=False)


@asynccontextmanager
async def get_db():
    async with Session() as session: #управляем жизнью сессии
        try:
            async with session.begin(): # начинает транзакцию внутри существующей сессии
                yield session
        except Exception as e:
            print('Ошибка', e)
            raise
        
        
async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.drop_all)
        await conn.run_sync(models.Base.metadata.create_all)

async def close_db_engine() -> None:
    """Закрываем соединение с бд"""
    await engine.dispose()
    print('Соединение закрыто')