from sqlalchemy import Integer, Column
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped


class BaseModel(AsyncAttrs, DeclarativeBase):
    """Base model for all models."""

    id: Mapped[int] = Column(Integer, primary_key=True, autoincrement=True)

    def __repr__(self):
        return f'<{self.__class__.__name__}[{self.id}]>'

    __str__ = __repr__

