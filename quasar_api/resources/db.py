from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql.operators import eq
from urllib3.util.wait import select_wait_for_socket

from .base import WebResource, verb
from quasar_api import context


class DBResource(WebResource):
    """Web Resource based on sqlalchemy model."""

    def __init__(self, name: str, model: DeclarativeBase, permissions: dict = None):
        super(DBResource, self).__init__()
        self._name = name
        self._model = model
        self._permissions = permissions or {}

    @verb
    async def get(self, filter: dict | None = None) -> list[DeclarativeBase]:
        """Returns the list of `model`."""
        query = select(self._model)
        if filter:
            query.where(*(eq(getattr(self._model, name), value) for name, value in filter.items()))
        return (await context.context.db.execute(query)).scalars().all()

    @verb
    async def post(self, record: dict) -> None:
        """Create a new `model` instance on on the database."""
        instance = self._model(**record)
        context.context.db.add(instance)
        await context.context.db.commit()


    @verb
    def put(self, pk: str, record: dict) -> None:
        """Update the record on the DB."""


    @verb
    def delete(self, pk: str) -> None:
        """Delete the record on the DB."""

