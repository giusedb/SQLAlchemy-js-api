import time
from copy import deepcopy
from functools import reduce
from itertools import groupby
from typing import Iterable, Tuple, Dict

from click import style
from sqlalchemy.orm import DeclarativeBase
from typing_extensions import Callable

from jsalchemy_web_context.interceptors import ChangeInterceptor, ResultData
from .propagation import Messanger
from ..exceptions import HandledValidation

from jsalchemy_web_context import ContextManager, session, request, db
from jsalchemy_authentication.manager import AuthenticationManager
from .base import WebResource
from .db import DBResource
from ..exceptions import ResourceNotFoundException
import logging

from ..utils import kebab_case, dict_merge, dict_diff, model_group

log = logging.getLogger('JSAlchemy')

class ResourceManager:
    """The global resource manager, which manages all registered resources."""
    _instance: 'ResourceManager' = None
    resources: dict = {}
    tables: dict = {}

    def __init__(self,
                 auth_man: AuthenticationManager,
                 context: ContextManager,
                 name: str | None = None,
                 description: str = '',
                 realtime_queue: str = None,
                 disable_interceptor: bool = False):
        self.context = context
        if not context.change_interceptor:
            self.interceptor = context.change_interceptor = ChangeInterceptor(self.on_message)
        self.auth_man = auth_man
        self.last_run = time.time()
        self.app_name = name or 'no-name'
        self.description = description
        self.messanger = Messanger(self, realtime_queue) if realtime_queue else None

    def __call__(self, token=None):
        return self.context(token)

    def register(self, resource: WebResource):
        """Register a web resource for getting exposed to the web endpoints."""
        log.debug('registering resource "%s"', resource.name)
        self.resources[kebab_case(resource.name)] = resource
        if isinstance(resource, DBResource) and resource.model not in self.resources:
            self.resources[resource.model] = resource
            self.resources[resource.model.__table__] = resource
            self.tables[resource.model.__table__.name] = resource
            self.interceptor.register_model(resource.model)

    @property
    def foreign_keys(self):
        return (fk for r in self.resources.values()
                for fk in r.model.__table__.foreign_keys
                if fk.constraint.table.name in self.tables
                and fk.column.table.name in self.tables)

    def _deep_serialize(self, item):
        if isinstance(item, dict):
            return {k: self._deep_serialize(v) for k, v in item.items()}
        elif isinstance(item, (list, tuple, set)):
            return [self._deep_serialize(v) for v in item]
        elif isinstance(item, DeclarativeBase):
            titem = type(item)
            resource = self.resources.get(type(item))
            if not resource:
                raise TypeError(f'type {type(item)} not serializable.')
            if item in request.result.new.union(request.result.update):
                return {'$ref': [titem.__name__, item.id]}
            return self.resources[type(item)].serialize(item)
        else:
            return item

    @property
    def changes(self) -> Dict[str, Iterable]:
        """Generates the change dictionary from the intercepted `result`."""
        result: ResultData = request.result
        ret = {}
        if result.update:
            loaded = {model: {x['id']: x for x in items} for model, items in request.loaded.items() }
            ret['update'] = {}
            for model, records in groupby(sorted(result.update, key=lambda x: type(x).__name__), type):
                resource = self.resources[model]
                previous = loaded.get(model.__name__, {})
                update_chunk = []
                for record in records:
                    prev = previous.get(record.id)
                    if not prev:
                        result.new.add(record)
                        continue
                    diff = dict_diff(prev, resource.serialize(record))
                    if diff:
                        diff['id'] = record.id
                        update_chunk.append(diff)
                ret['update'][resource.name] = update_chunk

        if result.new:
            ret['new'] = model_group(result.new, self)

        if result.delete:
            ret['delete'] = {self[k].name: list(v) for k, v in result.delete.items()}
        if result.m2m:
            ret['m2m'] = result.m2m
        return ret


    def serialize_results(self, change_dict: Dict[str, Iterable], result: Iterable) -> dict:
        """Compose the action's return dict"""
        ret = deepcopy(change_dict)
        if type(result) is dict and '__' in result:
            ret.update(result.pop('__'))
        if result:
            if isinstance(result, dict) and tuple(result) == ('_',):
                return dict_merge(ret, result['_'])
            ret['payload'] = self._deep_serialize(result)
        return ret

    @property
    def models(self):
        """Return all registered models."""
        return {r.model for r in self.resources.values() if isinstance(r, DBResource)}

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
                # collect the direct result
                result = await action(*args, **kwargs)
                await db.flush()
                # get what has been changed over the verb's execution.
                change_dict = self.changes
                # execute the message propagation if any
                propagation = self.messanger.propagate(change_dict)
                propagation and await propagation
                # combine the result with the data changes
                return self.serialize_results(change_dict, result)
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
        if not user:
            return None
        token, _ = await self.context.web_session_man.new()
        async with self.context(token) as ctx:
            session.user_id = user.id
        if user:
            return {
                'last_build': self.last_run,
                'token': token,
                'application': self.app_name,
                'user_id': user.id,
                'wsConnection': 'ws://localhost:7998/',
                'user': { c.name: getattr(user, c.name) for c in user.__table__.columns if c.name != 'password'}
            }

    async def logout(self, token: str) -> dict | None:
        try:
            await self.context.destroy(token)
            return 'Ok'
        except Exception:
            return 'Session not found'

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
