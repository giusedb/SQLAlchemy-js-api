from functools import wraps
from itertools import groupby
from operator import itemgetter
from types import FunctionType

from jsalchemy_api.utils import model_group, dict_diff
from jsalchemy_web_context import request


def verb(name: str | FunctionType = None,
         return_mode: str = 'rpc',
         detached_instance: bool = False) -> FunctionType:

    available_modes = 'rpc', 'supervised', 'action'
    if return_mode not in available_modes:
        raise ValueError(f'Invalid return mode {return_mode}. Available modes are {available_modes}')

    def decorator(func):
        @wraps(name)
        async def get_instance(self, pk, *args, **kwargs):
            instance = await self.by_pk(pk)
            return await func(self, instance, *args, **kwargs)

        if not detached_instance:
            get_instance.orig_func = func
            func = get_instance
        func.detached_instance = detached_instance
        func.is_verb = True
        func.orig_func = func
        func.serialize_results = return_mode == 'supervised'
        return func

    return decorator(name) if type(name) is FunctionType else decorator


class Verbal(type):

    def __new__(cls, name, bases, attrs):
        attrs['_verbs'] = {name: attr for name, attr in attrs.items() if callable(attr)}
        return super().__new__(cls, name, bases, attrs)


class WebResource(metaclass=Verbal):

    def get(self, filter: dict = {}):
        """Get the list of `model` you want to get"""
        raise NotImplementedError()

    def put(self, pk: str, record: dict) -> None:
        raise NotImplementedError()

    def post(self, record: dict) -> None:
        raise NotImplementedError()

    def delete(self, pk: str) -> None:
        raise NotImplementedError()

    def describe(self):
        raise NotImplementedError()

    @property
    def permissions(self):
        raise NotImplementedError()

    @property
    def pydantic(self):
        raise NotImplementedError()

    def serialize(self, obj) -> dict:
        raise NotImplementedError()


class ResultData:
    def __init__(self):
        self.new = set()
        self.update = set()
        self.delete = set()
        self.description = []
        self.m2m = []

    def to_dict(self, resource_manager: 'ResourceManager'):
        """Generate the main response"""
        ret = {}
        if self.description:
            ret['description'] = {desc['name']: desc for desc in self.description}
        if self.update:
            loaded = {model: {x['id']: x for x in items} for model, items in request.loaded.items() }
            ret['update'] = {}
            for model, records in groupby(sorted(self.update, key=lambda x: type(x).__name__), type):
                resource = resource_manager.resources[model]
                previous = loaded.get(model.__name__, {})
                update_chunk = []
                for record in records:
                    prev = previous.get(record.id)
                    if not prev:
                        self.new.add(record)
                        continue
                    diff = dict_diff(prev, resource.serialize(record))
                    if diff:
                        diff['id'] = record.id
                        update_chunk.append(diff)
                ret['update'][resource.name] = update_chunk

        if self.new:
            ret['new'] = model_group(self.new, resource_manager)

        if self.delete:
            ret['delete'] = {
                res: list(map(itemgetter(1), grp))
                for res, grp in groupby(sorted(self.delete, key=itemgetter(0)), itemgetter(0)) }
        if self.m2m:
            ret['m2m'] = self.m2m
        return ret

    def __repr__(self):
        summary = {k: v for k, v in ((k, len(getattr(self, k))) for k in self.__slots__) if v}
        return f"ResultData: {summary}"

    __slots__ = ('description', 'delete', 'm2m', 'new', 'update')
