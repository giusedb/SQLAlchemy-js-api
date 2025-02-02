from abc import ABCMeta
from types import FunctionType


def verb(func: FunctionType) -> FunctionType:
    def serialize(result):
        return []

    def wrapper(*args, **kwargs):
        return serialize(func(*args, **kwargs))

    func.is_verb = True
    func.call = wrapper
    return func


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
