from .base import WebResource  # pylint disable=relative-beyond-top-level
from ..exceptions import ResourceNotFoundException
from quasar_api import context

class ResourceManager:
    """The global resource manager, which manages all registered resources."""
    _instance: 'ResourceManager' = None
    _resources: dict = {}

    def register(self, resource: WebResource):
        """Register a web resource for getting exposed to the web endpoints."""
        self._resources[resource._name] = resource

    async def action(self, token: str, resource: str, verb: str, *args, **kwargs):
        """Finds the correct `resource` and call the right `verb` with `args`."""
        res = self._resources.get(resource)
        if not res:
            raise ResourceNotFoundException(f'Resource {resource} not found')
        action = getattr(res, verb, None)
        if not action:
            raise ResourceNotFoundException(f'Verb {resource}.{verb} not found')

        async with context.context(token) as ctx:
            result = await action(*args, **kwargs)
            print(result)
            return result

