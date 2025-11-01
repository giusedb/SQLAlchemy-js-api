import asyncio
import re
from datetime import datetime
from decimal import Decimal
from functools import wraps
from itertools import groupby
from typing import Tuple, Iterable, Callable

# from orjson.orjson import dumps
from sqlalchemy.orm import ColumnProperty, DeclarativeBase
from sqlalchemy import DateTime, Date, Interval, LargeBinary, DECIMAL

TYPE_SERIALIZERS = {
    DateTime: lambda x: x and x.timestamp(),
    Date: lambda x: x and datetime.fromordinal(x.toordinal()).timestamp(),
    Interval: lambda x: x and x.total_seconds(),
    Decimal: lambda x: x and float(x),
    DECIMAL: lambda x: x and float(x),
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

CAP_WORD = re.compile(r'[A-Z][a-z]')

def kebab_case(camel: str) -> str:
    """Transform any canel case string into a kebab case"""
    ret = CAP_WORD.sub(lambda x: f'-{x.group().lower()}', camel).lower()
    return ret[1:] if ret.startswith('-') else ret

def camelize(snake: str) -> str:
    """Transform any snake case string into a camel case"""
    ret = ''.join(x.title() for x in snake.split('_'))
    return ret[0].lower() + ret[1:]


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

def dict_diff(a: dict, b: dict):
    """Returns the difference between two dictionaries."""
    return {k: v for k, v in b.items() if k not in a or a[k] != b[k]}

def model_group(items: Iterable[DeclarativeBase], resource_manager=None):
    """Group items by model."""
    sorted_items = sorted(items, key=lambda o: type(o).__name__)
    ret = {}
    if resource_manager:
        for model, items in groupby(sorted_items, type):
            resource = resource_manager.resources[model]
            ret[resource.name] = list(map(resource.serialize, items))
    else:
        ret = {type(o).__name__: list(items) for o in sorted_items}
    return ret

def _dict_merge(a: dict, b: dict, reduce_func: Callable = None) -> dict:
    sa, sb = map(set, (a, b))
    a_only, b_only = sa - sb, sb - sa
    both = sa.intersection(sb)
    for key in a_only:
        yield key, a[key]
    for key in b_only:
        yield key, b[key]
    for key in both:
        value = a[key]
        if isinstance(value, dict):
            yield key, dict(_dict_merge(value, b[key], reduce_func))
        elif reduce_func:
            yield key, reduce_func(value, b[key])
        else:
            yield key, value

def dict_merge(a: dict, b: dict, reduce_func: Callable = None) -> dict:
    return dict(_dict_merge(a, b, reduce_func))

def load_class(class_path: str) -> type:
    full_path = class_path.rsplit('.')
    class_name = full_path.pop()
    module = __import__('.'.join(full_path), fromlist=[class_name])
    return getattr(module, class_name)
