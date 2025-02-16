import asyncio
from operator import attrgetter, itemgetter
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute
from sqlalchemy.sql.operators import eq

from .base import WebResource, verb
from ..context import db

def to_js_type(field_type) -> str:
    """Transform a SQLAlchemy type into a JS type adapter."""
    if isinstance(field_type, type):
        return field_type.__name__.lower()
    return type(field_type).__name__.lower()

def attributes(model) -> dict[str, InstrumentedAttribute]:
    """Introspect a model and return a dictionary of DB attributes."""
    attrs = ((name, getattr(model, name)) for name in dir(model))
    return {name: attr for name, attr in attrs
            if isinstance(attr, InstrumentedAttribute)}

class DBResource(WebResource):
    """Web Resource based on sqlalchemy model."""

    def __init__(self, resource_manager: 'ResourceManager', name: str,
                 model: DeclarativeBase, permissions: dict = None):
        super(DBResource, self).__init__()
        self.name = name
        self.model = model
        self._permissions = permissions or {}
        self.resource_manager = resource_manager

    @property
    def one_to_many(self) -> List[dict]:
        """Checks all the "One-to-many" relations."""
        return [
            {'resource': self.resource_manager.tables[fk.column.table.name].name,
             'description': 'TODO',
             'type': 'in',
             'attribute': fk.constraint.column_keys}
            for fk in self.resource_manager.foreign_keys
            if fk.constraint.table == self.model.__table__
        ]

    @property
    def many_to_one(self) -> List[dict]:
        return [
            {'resource': self.resource_manager.tables[fk.constraint.table.name].name,
             'description': 'TODO',
             'type': 'out',
             'attribute': fk.column.table.name}
            for fk in self.resource_manager.foreign_keys
            if fk.column.table == self.model.__table__
        ]


    @property
    def references(self) -> List[dict]:
        """List all the relations for this Model."""
        return list(sorted(self.one_to_many + self.many_to_one, key=itemgetter('resource')))


    @property
    def description(self):
        ret = {}
        ret['name'] = self.name
        ret['description'] = self.model.__doc__
        ret['permissions'] = self._permissions or []
        ret['fields'] = [{
            'name': field.name,
            'description': field.comment,
            'type': to_js_type(field.type),
            'widget': None,
            'constraints': ['TODO'],
            'validators': [],
            } for field in self.model.__table__.columns]
        ret['UID'] = [f.name for f in self.model.__table__.primary_key]
        ret['references'] = self.references
        return ret

    @verb
    async def describe(self):
        await asyncio.sleep(0)
        return self.description

    @verb
    async def get(self, filter: dict | None = None) -> list[DeclarativeBase]:
        """Returns the list of `model`."""
        query = select(self.model)
        if filter:
            query.where(*(eq(getattr(self.model, name), value) for name, value in filter.items()))
        data = (await db.execute(query))
        return data.scalars().all()

    @verb
    async def post(self, record: dict) -> None:
        """Create a new `model` instance on on the database."""
        instance = self.model(**record)
        context.context.db.add(instance)
        await context.context.db.commit()


    @verb
    def put(self, pk: str, record: dict) -> None:
        """Update the record on the DB."""


    @verb
    def delete(self, pk: str) -> None:
        """Delete the record on the DB."""
