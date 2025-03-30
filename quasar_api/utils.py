from functools import wraps
from typing import List, Tuple

from orjson.orjson import dumps
from sqlalchemy.orm import InstrumentedAttribute, RelationshipProperty, Relationship, ColumnProperty, DeclarativeBase


def memoize(func):
    cache = {}

    @wraps(func)
    def wrapper(*args):
        if args not in cache:
            cache[args] = func(*args)
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

class JSONMixin:

    def to_dict(self) -> dict:
        """Transform any Database object into a dictionary."""
        return {name: getattr(self, name) for name in col_names(self.__class__)}

    def to_json(self) -> bytes:
        """Transform any Database object into a JSON string."""
        return dumps(self.to_dict())
