from datetime import date

import pytest
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from quasar_api.context.manager import ContextManager
from quasar_api.resources.manager import ResourceManager


class Base(AsyncAttrs, DeclarativeBase):
    pass


@pytest.fixture
def engine():
    """Generate an in-memory Sqlite and connects the engine to it."""
    return create_async_engine('sqlite+aiosqlite:///:memory:', echo=False)


@pytest.fixture
def session_maker(engine):
    """Creates a database session maker."""
    return async_sessionmaker(bind=engine)


@pytest.fixture
def context_manager(session_maker):
    """Creates a full context manager."""
    cm = ContextManager(None, session_maker)
    return cm


@pytest.fixture
def res_man(session_maker):
    red = Redis()
    return ResourceManager(session_maker, red)


@pytest.fixture
def model_people():

    class Person(Base):

        __tablename__ = 'person'

        id: Mapped[int] = mapped_column(primary_key=True)
        first_name: Mapped[str]
        last_name: Mapped[str]
        date_of_birth: Mapped[date]

        def __str__(self):
            return f"{self.first_name} {self.last_name.upper()}"

        def __repr__(self):
            return f"Person({self.first_name}, {self.last_name}, {self.date_of_birth})"

    return Person