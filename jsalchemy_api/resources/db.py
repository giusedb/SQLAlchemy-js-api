import asyncio
from functools import reduce
from typing import List

from sqlalchemy import select, delete, or_, and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, RelationshipDirection, RelationshipProperty

from .base import WebResource, verb
from ..context import db

from pluralizer import Pluralizer

from ..exceptions import RecordNotFound, ResourceNotFoundException
from ..utils import col2attr, memoize

pluralizer = Pluralizer()
pluralize = pluralizer.plural

def to_js_type(field_type) -> str:
    """Transform a SQLAlchemy type into a JS type adapter."""
    if isinstance(field_type, type):
        return field_type.__name__.lower()
    return type(field_type).__name__.lower()


def _get_remote(model, prop):
    pair = tuple(prop.remote_side)
    if len(pair) == 1:
        return pair[0]
    if next(iter(pair[0].foreign_keys)).constraint.referred_table == model.__table__:
        local, remote = pair
    else:
        local, remote = reversed(pair)
    return remote


class DBResource(WebResource):
    """Web Resource based on sqlalchemy model."""

    def __init__(self, resource_manager: 'ResourceManager', name: str,
                 model: DeclarativeBase, permissions: dict = None, columns: list = None,
                 extras: dict = None, format_string: str = None):
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
        self.m2ms = { prop.key: M2MResource(self, prop) for prop in model.__mapper__.relationships
                      if prop.direction == RelationshipDirection.MANYTOMANY }
        self.extras = extras or {}
        self.format_string = format_string


    @property
    def one_to_many(self) -> List[dict]:
        """Checks all the "One-to-many" relations."""

        def serialize(name, field):
            return {'resource': self.resource_manager[field.prop.argument],
                     'type': 'many',
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
                     'type': 'one',
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

        def resolve(prop) -> str:
            if prop.direction == RelationshipDirection.MANYTOMANY:
                table_name = next(iter(_get_remote(self.model, prop).foreign_keys)).constraint.referred_table.name
            else:
                table_name = next(iter(prop.remote_side)).table.name
            return self.resource_manager.tables[table_name]

        def serialize_m2m(prop):
            remote_resource = next(iter(_get_remote(self.model, prop).foreign_keys)).column
            return dict(
                resource=resolve(prop).name,
                type='m2m',
                attribute=prop.key,
                foreign_attribute=col2attr(resolve(prop).model)[next(iter(_get_remote(self.model, prop).foreign_keys)).column.name],  # TODO multifields
                description=prop.doc,
                local_attribute=col2attr(self.model)[next(iter(prop.local_columns)).name],
            )

        def serialize(name, prop):
            directions = {
                RelationshipDirection.MANYTOONE: 'one',
                RelationshipDirection.ONETOMANY: 'many',
            }

            return dict(
                resource=resolve(prop).name,
                type=directions[prop.direction],
                attribute=name,
                foreign_attribute=col2attr(resolve(prop).model)[_get_remote(self.model, prop).name],  # TODO multifields
                description=prop.doc,
                local_attribute=col2attr(self.model)[next(iter(prop.local_columns)).name],
            )

        for prop in self.model.__mapper__.relationships:
            tab_name = _get_remote(self.model, prop).table.name
            if prop.direction == RelationshipDirection.MANYTOMANY:
                yield serialize_m2m(prop)
            if tab_name not in self.resource_manager.tables:
                continue
            yield serialize(prop.key, prop)

    @property
    # @memoize
    def description(self):

        def serialize(name, field):
            return {
                'name': name,
                'description': field.doc,
                'type': to_js_type(field.type),
                'extra': self.extras.get(name, {}),
                'validators': [],  # TODO add validators
            }

        ret = {}
        ret['name'] = self.name
        ret['description'] = self.model.__doc__
        # ret['permissions'] = self._permissions or []
        ret['fields'] = [serialize(col.key, col) for col in self.model.__mapper__.columns]
        ret['UID'] = [f.name for f in self.model.__table__.primary_key]
        ret['references'] = tuple(self.references)
        ret['format_string'] = self.format_string
        return ret

    @verb
    # @async_memoize
    async def describe(self):
        await asyncio.sleep(0)
        return {"DESCRIPTION": [self.description]}

    async def by_pk(self, *pks):
        """Get the record object by its primary key."""
        return (await db.execute(select(self.model).where(self.pk.in_(pks)))).scalar()

    @verb
    async def get(self, filter: dict | None = None) -> list[DeclarativeBase]:
        """Returns the list of `model`."""
        query = select(self.model)
        if filter:
            query = query.where(reduce(and_, (
                getattr(self.model, name).in_(val) for name, val in filter.items())))
        data = await db.execute(query)
        return data.scalars().all()

    @verb
    async def post(self, **record: dict) -> None:
        """Create a new `model` instance on on the database."""
        item = self.model(**record)
        db.add(item)
        await db.flush()
        return item

    @verb
    async def put(self, **record: dict) -> None:
        """Update the record on the DB."""
        pk = record.pop(self.description['UID'][0])
        rec = await self.by_pk(pk)
        if not rec:
            raise RecordNotFound(f'Record {pk} not found')
        for attr, value in record.items():
            # TODO limit the updates to the writable fields
            setattr(rec, attr, value)
        db.flush()
        return rec

    @verb
    async def delete(self, pks: List[str]) -> None:
        """Delete the record on the DB."""
        ids = tuple((await db.execute(select(self.pk).where(self.pk.in_(pks)))).scalars())
        if not ids:
            if len(pks) > 1:
                RecordNotFound(f'Records {pks} not found')
            raise RecordNotFound(f'Record {pks[0]} not found')
        await db.execute(delete(self.model).where(self.pk.in_(pks)))
        return {"DELETED": {self.name.lower(): ids}}

    @verb
    async def m2m(self, attribute: str, method: str, keys):
        if attribute not in self.m2ms:
            raise ResourceNotFoundException(404, f'Attribute {attribute} not found on {self.name} resource')
        m2m = self.m2ms[attribute]
        if not m2m:
            raise ResourceNotFoundException(404, f'Verb {self.name}.m2m.{attribute} not found on {self.name} resource')
        verb = getattr(m2m, method)
        if not verb:
            raise ResourceNotFoundException(404, f'Method {method} not found on {self.name} resource')
        ret = await verb(keys)
        if ret:
            return {'MANYTOMANY': {self.name.lower(): {attribute: ret } } }
        return ret


class M2MResource:

    def __init__(self, resource: DBResource, prop: RelationshipProperty):
        """Many to Many container for the DBResource."""
        self.resource_manager = resource.resource_manager
        self.primary_resource = resource
        remote_field = _get_remote(resource.model, prop)
        local_field = next(iter((fk.parent for c in remote_field.table.columns
                                 for fk in c.foreign_keys if fk.column.table == resource.model.__table__)))
        self.fields = [local_field, remote_field]

    async def get(self, keys) -> list[list[str]]:
        """Query the DB to fetch the result items related to `keys."""
        db_result = await db.execute(select(*self.fields).where(self.fields[0].in_(keys)))
        return list(map(list, db_result))

    async def add(self, keys: List[tuple[str, str]]) -> list[tuple[str, str]]:
        """Associate a list of related resources to the current one.

        keys: list of pairs of local and remote keys.
        """
        loc, rem = self.fields
        pairs = set(await db.execute(select(*self.fields).where(
            or_(*(and_(loc == l, rem == r) for l, r in keys)))))
        pairs = set(map(tuple, keys)) - pairs
        if pairs:
            values = [{loc.name: l, rem.name: r} for l, r in pairs]
            try:
                await db.execute(loc.table.insert(), values)
            except IntegrityError:
                raise
        return await self.get(sorted({k[0] for k in keys}))
    
    async def delete(self, keys: List[tuple[str, str]]) -> None:
        """Dissociate a list of related resources from the current one.

        keys: list of pairs of local and remote keys."""
        loc, rem = self.fields
        try:
            await db.execute(delete(loc.table).where(or_(*((and_(loc == l, rem == r) for l, r in keys)))))
        except IntegrityError as e:
            raise
        return await self.get(sorted({k[0] for k in keys}))
