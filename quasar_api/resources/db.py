import asyncio
from functools import reduce
from typing import List

from sqlalchemy import select
from sqlalchemy.orm import DeclarativeBase, InstrumentedAttribute, Relationship, ColumnProperty, RelationshipDirection
from sqlalchemy.sql.operators import eq, contains, and_

from .base import WebResource, verb
from ..context import db

from pluralizer import Pluralizer

from ..exceptions import RecordNotFound
from ..utils import col2attr

pluralizer = Pluralizer()
pluralize = pluralizer.plural

def to_js_type(field_type) -> str:
    """Transform a SQLAlchemy type into a JS type adapter."""
    if isinstance(field_type, type):
        return field_type.__name__.lower()
    return type(field_type).__name__.lower()


class DBResource(WebResource):
    """Web Resource based on sqlalchemy model."""

    def __init__(self, resource_manager: 'ResourceManager', name: str,
                 model: DeclarativeBase, permissions: dict = None, columns: list = None):
        super(DBResource, self).__init__()
        self.name = name
        self.model = model
        self._permissions = permissions or {}
        self.resource_manager = resource_manager
        self.columns = columns or [col.name for col in self.model.__table__.columns]
        def serialize(obj):
            return {col: getattr(obj, col) for col in self.columns}
        model.__serialize__ = serialize
        self.pk = model.__mapper__.primary_key[0]

    @property
    def one_to_many(self) -> List[dict]:
        """Checks all the "One-to-many" relations."""

        def get_attribute_name(self, fk):
            return pluralize(self.resource_manager.tables[fk.column.table.name].name)

        def serialize(name, field):
            return {'resource': self.resource_manager[field.prop.argument],
                     'type': 'in',
                     'foreign_field': next(iter(field.prop.synchronize_pairs))[0].name,
                     'attribute': name,
                     'description': 'TODO'}

        return [
            serialize(name, field)
            for name, field in self._relationships()
            if field.prop.argument in self.resource_manager
        ]

    @property
    def many_to_one(self) -> List[dict]:

        def serialize(fk):
            return {'resource': self.resource_manager.tables[fk.constraint.table.name].name,
                     'type': 'out',
                     'foreign_field': fk.constraint.name,
                     'attribute': fk.column.table.name,
                     'description': 'TODO'}

        return [
            serialize(fk)
        for fk in self.resource_manager.foreign_keys
            if fk.column.table == self.model.__table__
        ]

    @property
    def references(self) -> List[dict]:
        """List all the relations for this Model."""

        # return list(sorted(self.one_to_many + self.many_to_one, key=itemgetter('resource')))
        def get_remote(prop):
            pair = tuple(prop.remote_side)
            if len(pair) == 1:
                return pair[0]
            if next(iter(pair[0].foreign_keys)).constraint.referred_table == self.model.__table__:
                local, remote = pair
            else:
                local, remote = reversed(pair)
            return remote

        def resolve(prop) -> str:
            if prop.direction == RelationshipDirection.MANYTOMANY:
                table_name = next(iter(get_remote(prop).foreign_keys)).constraint.referred_table.name
            else:
                table_name = next(iter(prop.remote_side)).table.name
            return self.resource_manager.tables[table_name]

        def serialize(name, prop):
            directions = {
                RelationshipDirection.MANYTOONE: 'in',
                RelationshipDirection.ONETOMANY: 'out',
                RelationshipDirection.MANYTOMANY: 'bi',
            }

            return dict(
                resource=resolve(prop).name,
                type=directions[prop.direction],
                attribute=name,
                foreign_attribute=col2attr(resolve(prop).model)[get_remote(prop).name],  # TODO multifields
                description=prop.doc,
                local_attribute=col2attr(self.model)[next(iter(prop.local_columns)).name],
            )

        for prop in self.model.__mapper__.relationships:
            if prop.direction == RelationshipDirection.MANYTOMANY:
                continue
            tab_name = get_remote(prop).table.name
            if tab_name not in self.resource_manager.tables:
                continue
            yield serialize(prop.key, prop)

    @property
    def description(self):

        def serialize(name, field):
            return {
                'name': name,
                'description': field.doc,
                'type': to_js_type(field.type),
                'extra': {},
                'validators': [],  # TODO add validators
            }

        ret = {}
        ret['name'] = self.name
        ret['description'] = self.model.__doc__
        # ret['permissions'] = self._permissions or []
        ret['fields'] = [serialize(col.key, col) for col in self.model.__mapper__.columns]
        ret['UID'] = [f.name for f in self.model.__table__.primary_key]
        ret['references'] = tuple(self.references)
        return ret

    @verb
    async def describe(self):
        await asyncio.sleep(0)
        return {"DESCRIPTION": self.description}

    async def by_pk(self, pk):
        """Get the record object by its primary key."""
        return (await db.execute(select(self.model).where(self.pk == pk))).scalar()

    @verb
    async def get(self, filter: dict | None = None) -> list[DeclarativeBase]:
        """Returns the list of `model`."""
        query = select(self.model)
        if filter:
            query = query.where(reduce(and_, (
                getattr(self.model, name).in_(val) for name, val in filter.items())))
        data = (await db.execute(query))
        return data.scalars().all()

    @verb
    async def post(self, record: dict) -> None:
        """Create a new `model` instance on on the database."""
        item = self.model(**record)
        db.add(item)
        await db.flush()
        return item

    @verb
    async def put(self, pk: str, record: dict) -> None:
        """Update the record on the DB."""
        rec = await self.by_pk(pk)
        if not rec:
            raise RecordNotFound(f'Record {pk} not found')
        for attr, value in record.items():
            # TODO limit the updates to the writable fields
            setattr(rec, attr, value)
        db.flush()
        return rec


    @verb
    async def delete(self, pk: str) -> None:
        """Delete the record on the DB."""
        rec = await self.by_pk(pk)
        if not rec:
            raise RecordNotFound(f'Record {pk} not found')
        db.delete(rec)
        await db.flush()
        return rec
