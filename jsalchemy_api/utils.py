import asyncio
from datetime import datetime
from decimal import Decimal
from functools import wraps
from typing import Tuple

from orjson.orjson import dumps
from sqlalchemy.orm import ColumnProperty, DeclarativeBase
from sqlalchemy import DateTime, Date, Interval, LargeBinary, DECIMAL

TYPE_SERIALIZERS = {
    DateTime: lambda x: x and x.timestamp(),
    Date: lambda x: x and datetime.fromordinal(x.toordinal()).timestamp(),
    Interval: lambda x: x.total_seconds(),
    Decimal: lambda x: float(x),
    DECIMAL: lambda x: float(x),
}

UNSERIALIZABLE_TYPES = {LargeBinary}

def memoize(func):
    cache = {}

    @wraps(func)
    def wrapper(*args):
        if args not in cache:
            cache[args] = func(*args)
        return cache[args]

    return wrapper

def async_memoize(func):
    cache = {}
    @wraps(func)
    async def wrapper(*args):
        if args not in cache:
            result = await func(*args)
            cache[args] = result
            return result
        await asyncio.sleep(0)
        return cache[args]
    return wrapper

def all_model(base) -> Tuple[DeclarativeBase]:
    return tuple(mapper.target for mapper in base.registry.mappers)


def attributes(model):
    return { p.key: p for p in model.__mapper__.attrs }


@memoize
def relationships(model):
    return tuple((r.key, r.entity.entity, r.direction, r.remote_side) for r in model.__mapper__.relationships)

@memoize
def columns(model):
    return {c.name: c for c in model.__mapper__.columns}
    # return tuple((name, attr) for name, attr in attributes(model) if isinstance(attr.prop, ColumnProperty))


@memoize
def col_names(model):
    """Return the names of all columns of the model."""
    return tuple(columns(model))

@memoize
def col2attr(model) -> dict:
    """Returns a dict which associate column names with attribute names."""
    return { next(iter(p.columns)).name: p.key
             for p in model.__mapper__.attrs if isinstance(p, ColumnProperty)}

def type_converter(model):
    # TODO add limit fields to visible fields (`__exposed__`)
    colnames = col2attr(model)
    return tuple(
        (name, colnames[name], TYPE_SERIALIZERS.get(type(c.type), lambda x: x))
        for name, c in columns(model).items()
    )

class JSONMixin:

    def to_dict(self) -> dict:
        """Transform any Database object into a dictionary."""
        return {
            r_name: convert(getattr(self, name, None))
            for name, r_name, convert in type_converter(type(self))
        }

    def to_json(self) -> bytes:
        """Transform any Database object into a JSON string."""
        return dumps(self.to_dict())
