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
from utils import dict_merge
from ..exceptions import JSAlchemyException, HandledValidation

from jsalchemy_api.context.manager import ContextManager, session
from jsalchemy_authentication.manager import AuthenticationManager
from jsalchemy_authorization.models import UserMixin
from .base import WebResource  # pylint disable=relative-beyond-top-level
from .db import DBResource
from ..exceptions import ResourceNotFoundException
import logging

from ..utils import kebab_case

log = logging.getLogger('JSAlchemy')


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
        log.debug('registering resource "%s"', resource.name)
        self.resources[kebab_case(resource.name)] = resource
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
            resource: WebResource = self.resources.get(type(item))
            if resource:
                ret[resource.name].append(resource.serialize(item))
            if isinstance(item, dict):
                ret = dict_merge(ret, item)
        return ret

    async def action(self, token: str, resource: str, verb: str, *args, **kwargs) -> dict:
        """Finds the correct `resource` and call the right `verb` with `args`."""
        log.info(f"received request to {style(verb, 'red')} on {style(resource, 'blue')} from {style(token, 'yellow')}.")
        res = self.resources.get(resource)
        if not res:
            raise ResourceNotFoundException(f'Resource "{resource}" not found')
        action = getattr(res, verb, None)
        if not action:
            raise ResourceNotFoundException(f'Verb {resource}.{verb} not found')

        async with self.context(token) as ctx:
            try:
                result = await action(*args, **kwargs)
                if action.serialize_results:
                    return self.serialize_results(result)
                return result
            except HandledValidation as e:
                return {
                    '$validation': {
                        'errors': e.errors,
                        'resource': resource,
                        'verb': verb
                    }
                }

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

    def __getitem__(self, item: str) -> WebResource:
        """Checks if there is a `Resource` or a `table` with that nema and return the `Resouce"""
        return self.resources.get(item) or self.tables.get(item)

    def __contains__(self, item):
        """Check if the resource is in the resource list"""
        return item in self.resources

    def expose(self, name: str = None, permissions: dict = None, columns: Tuple[str] = (),
               format_string:str = None, read_only_columns: Tuple[str]= (), extras: dict[str, dict[str, object]]=None) -> type | Callable:
        """Expose the model to API."""
        def wrapper(cls):
            nonlocal name, permissions, columns, format_string, read_only_columns, extras
            params = ('name', 'permissions', 'columns', 'format_string', 'read_only_columns', 'extras')
            exposed = [c.__dict__['__expose__'] for c in reversed(cls.mro()) if '__expose__' in c.__dict__]
            exposed_fields = [c.__dict__['__expose_fields__'] for c in reversed(cls.mro()) if '__expose_fields__' in c.__dict__]
            reducible = {key: tuple(filter(bool, (c.get(key) for c in exposed))) for key in params}
            fields_options = reduce(dict_merge, exposed_fields, {})
            if not name:
                name = reducible['name'] and reducible['name'][0] or cls.__name__
            read_only_columns = set(reduce(lambda x, y: x.union(y),
                                           map(set, reducible['read_only_columns']), set(read_only_columns)))
            read_only_columns.update(
                {col_name for col_name, option in fields_options.items() if option.get('readonly', False) == True})
            extras = reduce(lambda x, y: dict_merge(y, x),
                            reducible['extras'], extras or {})
            log.info('Extras per %s is %s', name, extras)
            resource = DBResource(self, name=name, model=cls, permissions=permissions, extras=extras,
                                  columns=columns, format_string=format_string, read_only_columns=read_only_columns,
                                  client_field_options=fields_options)
            self.register(resource)
            return cls
        if name and type(name) is type and issubclass(name, DeclarativeBase):
            cls, name = name, name.__name__
            return wrapper(cls)
        return wrapper
