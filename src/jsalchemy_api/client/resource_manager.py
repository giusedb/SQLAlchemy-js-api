import logging
from threading import Thread
from time import time, sleep
from typing import List, Dict, Any

from .base import Config, Resource, ResourceDefinition
from .classgen import make_resouce_class
from .collection import JSACollection
from .connection import JSAConnection
from .rset import RSet
from .named_events import NamedEventManager

log = logging.getLogger('jsa.resource_manager')


class JSAResourceManager(Thread):

    def __init__(self, conf: Config):
        super().__init__()
        Thread.name = 'jsa-reference-resolver'
        self._resources = {}
        self.collections = {}
        self._events = NamedEventManager()
        self._conf = conf
        self.connection = JSAConnection(conf, self._events)

        self.class_cache = {}
        self.on = self._events.on
        self.emit = self._events.emit
        self._active = True
        self.start()
        log.debug('Resource manager initialized ✅')

    def run(self):
        period = self._conf.resolution_period / 1000
        log.info(f"Reference resolver started with period {period}ms")
        while self._active:
            t = time()
            self.emit('resolve-reference-timeout')
            sleep(time() - t + period)

    def stop(self):
        self.connection.logout()
        self._active = False

    def verb(self, resource_name: str, verb: str, kwargs: Dict, skip: bool=False):
        """Call a verb on the remote resource."""
        data = self.connection.fetch(resource_name, verb, kwargs)
        if not skip:
            self._got_data(data)
        return data

    def query(self, resource_name: str, filter: Dict[str, List | str | int | None]) -> RSet:
        """Query the local DB feed the missing data with a server Query if needed."""

    def get(self, resource_name: str, *pks: List[str], blocking: bool=False) -> List[Resource]:
        """Get the Resource object identified by the primary keys."""
        coll = self.get_collection(resource_name)
        ret = coll.get(*pks, blocking=blocking)
        if len(ret) == 1:
            return ret[0]
        return ret

    def _got_data(self, data: Dict[str, Any]):
        """parse the incoming data from the server"""
        for key, value in data.items():
            if key == 'description':
                for description in value:
                    self._got_model(description)
            elif key == 'read':
                for resource_name, items in value.items():
                    self.get_collection(resource_name).insert(items)
            elif key == 'update':
                for resource_name, items in value.items():
                    self.get_collection(resource_name).update(items)
            elif key == 'new':
                for resource_name, items in value.items():
                    self.get_collection(resource_name).insert(items)

    def _got_model(self, definition: Dict[str, Any]) -> None:
        """parse the incoming model definition from the server and store the generated class"""
        log.info('Got model %s', definition.get('name'))
        log.debug(definition)
        definition['PK'] = definition.pop('$pk')
        # definition['fields'] = [FieldDef(**field) for field in definition.pop('fields')]
        desc = ResourceDefinition(**definition)
        self.class_cache[desc.name] = make_resouce_class(self, desc)

    def describe(self, resource_name: str) -> type:
        """Get the description of the resource."""
        if resource_name not in self.class_cache:
            self.verb(resource_name, 'describe', {})
        return self.class_cache[resource_name]

    def get_collection(self, resource_name: str) -> JSACollection:
        """Get or create a collection for the resource."""
        if resource_name not in self.collections:
            self.collections[resource_name] = JSACollection(self, resource_name)
        return self.collections[resource_name]