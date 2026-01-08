"""Database connection management with async SQLAlchemy."""

import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


class Database:
    """Async database connection manager with connection pooling."""

    def __init__(self, database_url: str | None = None):
        """Initialize database connection.

        Args:
            database_url: PostgreSQL connection URL. If not provided, reads from
                          DATABASE_URL environment variable.
        """
        url = database_url or os.environ.get("DATABASE_URL", "")
        if not url:
            raise ValueError("DATABASE_URL environment variable not set")

        # Convert postgres:// to postgresql+asyncpg://
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)

        self._engine: AsyncEngine = create_async_engine(
            url,
            echo=False,  # Set to True for SQL logging
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        self._session_factory = async_sessionmaker(
            self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    @property
    def engine(self) -> AsyncEngine:
        """Get the SQLAlchemy engine."""
        return self._engine

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get a database session context manager.

        Usage:
            async with db.session() as session:
                result = await session.execute(...)
        """
        async with self._session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def health_check(self) -> bool:
        """Check if database connection is healthy."""
        from sqlalchemy import text

        try:
            async with self._session_factory() as session:
                await session.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    async def warmup(self) -> int:
        """Pre-create pool connections to avoid first-query latency.

        Creates connections up to pool_size concurrently, then returns them
        to the pool for reuse.

        Returns:
            Number of connections warmed up.
        """
        import asyncio
        from sqlalchemy import text

        async def create_connection():
            async with self._session_factory() as session:
                await session.execute(text("SELECT 1"))

        # Create pool_size connections concurrently
        pool_size = self._engine.pool.size()
        tasks = [create_connection() for _ in range(pool_size)]
        await asyncio.gather(*tasks)
        return pool_size

    async def close(self) -> None:
        """Close the database connection pool."""
        await self._engine.dispose()


# Global database instance
_database: Database | None = None


def get_database(database_url: str | None = None) -> Database:
    """Get or create the global database instance.

    Args:
        database_url: Optional database URL. Only used on first call.

    Returns:
        Database instance.
    """
    global _database
    if _database is None:
        _database = Database(database_url)
    return _database


async def close_database() -> None:
    """Close the global database connection."""
    global _database
    if _database is not None:
        await _database.close()
        _database = None
