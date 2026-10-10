import asyncio
import importlib
import logging
import os
import re
from datetime import datetime
from decimal import Decimal
from functools import wraps
from itertools import groupby
from typing import Tuple, Iterable

from click import style
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

def dict_merge(a: dict, b: dict):
    """Merge two dictionaries."""
    return {**a, **b}

def load_class(class_path: str):
    """Load a class from a string."""
    module, class_name = class_path.rsplit('.', 1)
    module = importlib.import_module(module)
    return getattr(module, class_name)

def deep_scan_modules(path=None, _base=None):
    """Scan a directory with all submodules and yield all python modules available."""
    import pkgutil

    if not path:
        path = str(importlib.import_module('modules').__path__[0])
    if _base is None:
        _base = path
    for mod in pkgutil.iter_modules([path]):
        if mod.ispkg:
            yield from deep_scan_modules(mod.module_finder.path + os.sep + mod.name, _base)
            continue
        package_name = os.path.relpath(mod.module_finder.path, _base).replace(os.sep, '.')
        if package_name == '.':
            package_name = ''
        if package_name:
            yield f'{package_name}.{mod.name}', [mod.name]
        else:
            yield mod.name, [mod.name]

def get_all_models(BaseModelClass, excludes:Iterable[str]=()):
    """Scan all `models` packages within the `modules` package and yield SQLAlchemy model classes.

    Recursively discovers modules containing model definitions, imports them,
    and yields every class that is a subclass of `BaseModelClass` (excluding
    `BaseModelClass` itself).

    Args:
        BaseModelClass: The base class to check subclasses against.
        excludes: Optional iterable of module names to skip.

    Yields:
        Tuples of (attribute_name, attribute_class) for each discovered model.
    """
    log = logging.getLogger('jsa.web.init')
    for name, fromlist in deep_scan_modules():
        if fromlist == ['models']:
            log.info(f"importing {style('model', fg='cyan')} module from {style(name + '.' + fromlist[0], fg='yellow')}")
            mod = __import__(name, fromlist=fromlist)
            for attr_name, attr in mod.__dict__.items():
                if attr_name.startswith('_'):
                    continue
                if isinstance(attr, type) and issubclass(attr, BaseModelClass) and attr is not BaseModelClass:
                    log.info(f" -> {style(attr_name, fg='green')} ✅")
                    yield attr_name, attr


def init_all_resources(rm: 'jsalchemy_api.ResourceManager', excludes:Iterable[str]=(), path: str=None):
    """Scan all `resources` packages within the `modules` package and register them with a ResourceManager.

    Recursively discovers modules containing resource definitions, imports them,
    and registers every `WebResource` subclass (excluding `WebResource` and
    `DBResource` themselves) by instantiating them with the provided manager.

    Args:
        rm: The ResourceManager instance to register resources with.
        excludes: Optional iterable of module names to skip.
        path: Optional directory path to scan instead of the default `modules` package.
    """
    from jsalchemy_api import WebResource, DBResource
    log = logging.getLogger('jsa.web.init')
    for name, fromlist in deep_scan_modules(path=path):
        if fromlist == ['resources']:
            log.info(f"importing {style('resource', fg='cyan')} module from {style(name + '.' + fromlist[0], fg='yellow')} ")
            mod = __import__(name, fromlist=fromlist)
            for attr_name, attr in mod.__dict__.items():
                if attr_name.startswith('_'):
                    continue
                if isinstance(attr, type) and issubclass(attr, WebResource) and attr not in {WebResource, DBResource}:
                    log.info(f" -> registering {style(attr_name, fg='green')} ✅")
                    instance = attr(rm)
                    rm.register(instance)
            for attr_name, attr in mod.__dict__.items():
                if attr_name.startswith('_'):
                    continue
                if isinstance(attr, type) and issubclass(attr, WebResource) and attr not in {WebResource, DBResource}:
                    log.info(f" -> registering {style(attr_name, fg='green')} ✅")
                    instance = attr(rm)
                    rm.register(instance)
