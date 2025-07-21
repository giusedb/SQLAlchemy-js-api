from abc import ABCMeta
from functools import wraps
from types import FunctionType


def verb(name: str | FunctionType = None, direct_return: bool = False,
         detached_instance: bool = False) -> FunctionType:

    def decorator(func):
        @wraps(name)
        async def get_instance(self, pk, *args, **kwargs):
            instance = await self.by_pk(pk)
            return await func(self, instance, *args, **kwargs)

        if not detached_instance:
            get_instance.is_verb = True
            get_instance.serialize_results = not direct_return
            get_instance.orig_func = func
            return get_instance

        func.is_verb = True
        func.orig_func = func
        func.serialize_results = not direct_return
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