import signal
from typing import Callable, Dict, List

from jsa.base import Config, Resource
from jsa.resource_manager import JSAResourceManager
from jsa.rset import RSet


class ORM:

    get: Callable[[str, str], Resource]
    get_model: Callable[[str], Resource]

    def __init__(self, config: Config, on: Dict[str, Callable]):
        self.config = config
        self._resource_manager = JSAResourceManager(config)
        self.on = self._resource_manager.on
        self.emit = self._resource_manager.emit
        self.get = self._resource_manager.get
        self.get_model = self._resource_manager.describe
        self.stop = self._resource_manager.stop
        for name, call_back in on.items():
            self.on(name, call_back)
        if config.login:
            self._resource_manager.connection.login()
        signal.signal(signal.SIGINT, lambda *args, **kwargs: self.stop())
        signal.signal(signal.SIGTERM, lambda *args, **kwargs: self.stop())

    def query(self, resource_name: str, filter: Dict[str, List | str]) -> RSet:
        """Build a RSet with the provided filters"""
        return RSet(self._resource_manager, resource_name, filter)

    def login(self, username: str, password: str) -> None:
        """log in to the JSA server"""
        self._resource_manager.connection.login(username, password)

    def logout(self) -> None:
        """log out from the JSA server"""
        self._resource_manager.connection.logout()


