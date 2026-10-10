from typing import Dict, List

from .base import Resource


class RSet:

    def __init__(self, resource_manager: 'jsa.JSAResourceManager',
                 resource_name: str,
           filter: Dict[str, str], sorting: List[str]=('id',), rpp: int=25, page: int=1):
        self.resource_manager = resource_manager
        self.resource_name = resource_name
        self._filter: Dict[str, List[str]] = filter
        self._sorting: List[str] = sorting
        self._rpp: int = rpp
        self._page: int = page
        self._items: List[Resource] = []

        self.resource_manager

    def fetch(self) -> List[Resource]:
        """Fetch the resources from the server."""


    @property
    def items(self) -> List[Resource]:
        return self._items

    def set_page(self, page: int) -> 'RSet':
        self._page = page
        return self

    def set_rpp(self, rpp: int) -> 'RSet':
        self._rpp = rpp
        return self

    def set_filter(self, filter: Dict[str, str]) -> 'RSet':
        self._filter = filter
        return self

    def set_sorting(self, sorting: List[str]) -> 'RSet':
        self._sorting = sorting
        return self

    @property
    def page(self) -> int:
        return self._page

    @property
    def sorting(self) -> int:
        return self._sorting

    @property
    def rpp(self):
        return self._rpp

    @property
    def filter(self):
        return self._filter

    @page.setter
    def page(self, page: int):
        return self.set_page(page)

    @sorting.setter
    def sorting(self, value: List[str]):
        return self.set_sorting(value)

    @rpp.setter
    def rpp(self, rpp: int):
        return self.set_rpp(rpp)

    @filter.setter
    def filter(self, filter: Dict[str, str]):
        return self.set_filter(filter)
