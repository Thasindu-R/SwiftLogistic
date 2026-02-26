"""
Async SQLAlchemy engine and session factory shared by all services.
Each service imports `get_db` for dependency injection and `init_db`
to create tables on startup.
"""

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase
from .config import settings

# Ensure the URL uses the asyncpg driver
_url = settings.DATABASE_URL
if _url.startswith("postgresql://"):
    _url = _url.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(_url, echo=settings.DEBUG, pool_size=5, max_overflow=10)

async_session_factory = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""
    pass


async def get_db():
    """FastAPI dependency – yields an async DB session."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def init_db():
    """Create all tables that inherit from Base."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
