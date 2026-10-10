import logging
from typing import List, Dict, Tuple, Set

from .base import Resource
from .utils import wait_until

log = logging.getLogger('jsa.collection')


class JSACollection:

    def __init__(self, resource_manager: 'jsa._resource_manager.JSAResourceManager',
                 resource_name: str):
        self._resource_manager = resource_manager
        self._index = {}
        self._missings_keys = set()
        self._requested = set()
        self.resource_name = resource_name
        self._resource_manager.on('resolve-reference-timeout', self.fetch_missings)

    def fetch_missings(self):
        """Fetch the missing items from the server."""
        if self._missings_keys:
            log.debug(f"Fetching missing items for %s: %s", self.resource_name, self._missings_keys)
            missing, self._missings_keys = self._missings_keys, set()
            self._resource_manager.verb(self.resource_name, 'get', {'pks': list(missing)})
            self._requested.update(missing)


    def get(self, *pks: List[str], blocking: bool=False) -> List[Resource]:
        """Get the Resource object identified by the primary keys."""
        for pk in pks:
            if pk not in self._index:
                self._missings_keys.add(pk)
        if blocking:
            wait_until(lambda: not all(pk in self._requested for pk in pks))
        return [self._index.get(pk) for pk in pks]

    def _split_missings(self, items: List[dict]) -> Tuple[List[Tuple[str, dict]], List[Tuple[str, dict]]]:
        """Splits the list of items into existing and missing."""
        cls = self._resource_manager.class_cache[self.resource_name]
        get_pk = cls.get_pk
        existing, missing = [], []
        for item in items:
            pk = get_pk(item)
            if pk in self._index:
                existing.append((pk, item))
            else:
                missing.append((pk, item))
        return existing, missing

    def insert(self, items: List[Dict[str, Any]], hydrate_pagers: bool=False) -> List[Resource]:
        """Bulk insert items into the collection."""
        cls = self._resource_manager.class_cache[self.resource_name]
        existing, missing = self._split_missings(items)
        for pk, item in missing:
            self._index[pk] = cls.create(item)
        for pk, item in existing:
            obj = self._index[pk]
            obj.UPDATE(item)

    def update(self, items: List[List[Dict[str, [str, int]], Dict[str, [str, int]]]]):
        """Bulk update objects in the collection."""
        cls = self._resource_manager.class_cache[self.resource_name]
        get_pk = cls.get_pk
        for new, prev_update in items:
            pk = get_pk(prev_update)
            obj = self._index.get(pk)
            if obj:
                obj.UPDATE(new)


    def __repr__(self):
        return f"JSACollection({self.resource_name})"

    __str__ = __repr__