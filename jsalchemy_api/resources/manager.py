import time
from collections import defaultdict
from functools import reduce
from typing import Iterable, Tuple

from click import style
from jinja2.runtime import exported
from redis import Redis
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase
from typing_extensions import Callable

from jsalchemy_authorization import permissions
from ..exceptions import JSAlchemyException

from jsalchemy_api.context.manager import ContextManager, session
from jsalchemy_authentication.manager import AuthenticationManager
from jsalchemy_authorization.models import UserMixin
from .base import WebResource  # pylint disable=relative-beyond-top-level
from .db import DBResource
from ..exceptions import ResourceNotFoundException
import logging

from ..utils import JSONMixin

logger = logging.getLogger(__package__)


class ResourceManager:
    """The global resource manager, which manages all registered resources."""
    _instance: 'ResourceManager' = None
    resources: dict = {}
    tables: dict = {}

    def __init__(self,
                 auth_man: AuthenticationManager,
                 session_maker: Callable | None = None,
                 redis_connection: Redis | str | None = None,
                 name: str | None = None):
        self.context = ContextManager(self, session_maker, redis_connection)
        self.auth_man = auth_man
        self.last_run = time.time()
        self.app_name = name or 'no-name'

    def __call__(self, token=None):
        return self.context(token)

    def register(self, resource: WebResource):
        """Register a web resource for getting exposed to the web endpoints."""
        self.resources[resource.name] = resource
        if isinstance(resource, DBResource):
            self.resources[resource.model] = resource
            self.resources[resource.model.__table__] = resource
            self.tables[resource.model.__table__.name] = resource

    @property
    def foreign_keys(self):
        return (fk for r in self.resources.values()
                for fk in r.model.__table__.foreign_keys
                if fk.constraint.table.name in self.tables
                and fk.column.table.name in self.tables)

    def serialize_results(self, result: Iterable) -> dict:
        if not result:
            return {}
        if isinstance(result, dict):
            return result

        ret = defaultdict(list)
        if not isinstance(result, Iterable):
            result = [result]
        for item in result:
            ret[self[item.__table__].name].append(item.to_dict())
        return dict(ret)

    async def action(self, token: str, resource: str, verb: str, *args, **kwargs) -> dict:
        """Finds the correct `resource` and call the right `verb` with `args`."""
        logger.info(f"received request to {style(verb, 'red')} on {style(resource, 'blue')} from {style(token, 'yellow')}.")
        res = self.resources.get(resource)
        if not res:
            raise ResourceNotFoundException(f'Resource "{resource}" not found')
        action = getattr(res, verb, None)
        if not action:
            raise ResourceNotFoundException(f'Verb {resource}.{verb} not found')

        async with self.context(token) as ctx:
            result = await action(*args, **kwargs)
            return self.serialize_results(result)

    async def login(self, username: str, password: str) -> dict | None:
        """Log in the user and return the status object."""
        user = await self.auth_man.login(username, password)
        token, _ = await self.context.web_session_man.new()
        async with self.context(token) as ctx:
            session.user = user
        if user:
            return {
                'user': user.to_dict(),
                'last_build': self.last_run,
                'token': token,
                'application': self.app_name
            }

    async def logout(self, token: str) -> dict | None:
        return await self.context.web_session_man.logout(token)

    def expose(self, model):
        """Model decorator to register the model"""
        resource = DBResource(self, model=model **getattr(model, '__JSAlchemy__', {}))
        self.register(resource)
        return model

    def __getitem__(self, item: str) -> WebResource:
        """Checks if there is a `Resource` or a `table` with that nema and return the `Resouce"""
        return self.resources.get(item) or self.tables.get(item)

    def __contains__(self, item):
        """Check if the resource is in the resource list"""
        return item in self.resources

    def expose(self, name: str = None, permissions: dict = None, columns: Tuple[str] = (),
               format_string:str = None, read_only_columns: Tuple[str]= ()) -> type | Callable:
        """Expose the model to API."""
        def wrapper(cls):
            nonlocal name, permissions, columns, format_string, read_only_columns
            params = ('name', 'permissions', 'columns', 'format_string', 'read_only_columns')
            exposed = [c.__dict__['__expose__'] for c in reversed(cls.mro()) if '__expose__' in c.__dict__]
            reducible = {key: tuple(filter(bool, (c.get(key) for c in exposed))) for key in params}
            if not name:
                name = reducible['name'] and reducible['name'][0] or cls.__name__.lower()
            read_only_columns = reduce(lambda x, y: x.union(y),
                                       map(set, reducible['read_only_columns']), set(read_only_columns))
            resource = DBResource(self, name=name, model=cls, permissions=permissions,
                                  columns=columns, format_string=format_string, read_only_columns=read_only_columns)
            self.register(resource)
            return cls
        if name and type(name) is type and issubclass(name, DeclarativeBase):
            cls, name = name, name.__name__.lower()
            return wrapper(cls)
        return wrapper
