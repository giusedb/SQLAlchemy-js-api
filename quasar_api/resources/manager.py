from redis import Redis
from typing_extensions import Callable

from quasar_api.context.manager import ContextManager
from .base import WebResource  # pylint disable=relative-beyond-top-level
from .db import DBResource
from ..exceptions import ResourceNotFoundException


class ResourceManager:
    """The global resource manager, which manages all registered resources."""
    _instance: 'ResourceManager' = None
    resources: dict = {}
    tables: dict = {}

    def __init__(self, session_maker: Callable | None = None,
                 redis_connection: Redis | str | None = None):
        self.context = ContextManager(self, session_maker, redis_connection)

    def __call__(self, token=None):
        return self.context(token)

    def register(self, resource: WebResource):
        """Register a web resource for getting exposed to the web endpoints."""
        self.resources[resource.name] = resource
        if isinstance(resource, DBResource):
            self.tables[resource.model.__table__.name] = resource

    @property
    def foreign_keys(self):
        return (fk for r in self.resources.values()
                for fk in r.model.__table__.foreign_keys
                if fk.constraint.table.name in self.tables
                and fk.column.table.name in self.tables)

    async def action(self, token: str, resource: str, verb: str, *args, **kwargs):
        """Finds the correct `resource` and call the right `verb` with `args`."""
        res = self.resources.get(resource)
        if not res:
            raise ResourceNotFoundException(f'Resource {resource} not found')
        action = getattr(res, verb, None)
        if not action:
            raise ResourceNotFoundException(f'Verb {resource}.{verb} not found')

        async with self.context(token):
            result = await action(*args, **kwargs)
            print(result)
            return result

    def expose(self, model):
        """Model decorator to register the model"""
        resource = DBResource(self, model=model **getattr(model, '__quasar__', {}))
        self.register(resource)
        return model

    def __getitem__(self, item: str) -> WebResource:
        """Checks if there is a `Resource` or a `table` with that nema and return the `Resouce"""
        return self.resources.get(item) or self.tables.get(item)