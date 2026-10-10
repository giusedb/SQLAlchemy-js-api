from typing import List


class SimplePager:

    def __init__(self, resource_manager: 'jsa._resource_manager.JSAResourceManager', resource_name: str,
                 filter: Dict[str, str], sorting: List[str], rpp: int, page: int):
        self._resource_manager = resource_manager
        self._resource_name = resource_name
        self._filter = filter
        self._sorting = sorting
        self._rpp = rpp
        self._page = page
        self._sorted = []

    def get(self, min: int, max: int) -> List[str]:
        """return a list of PKs for the given range"""
        return self._sorted[min:max]