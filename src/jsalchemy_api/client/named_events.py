from typing import Callable
from logging import getLogger

log = getLogger('jsa.events')

class NamedEventManager:

    def __init__(self):
        self._events = {}
        self._idx = 0;
        self._handlers = {}

    def on(self, name: str, func: Callable) -> int:
        """Register a handler for a named event."""
        log.debug(f"Binding handler %s for %s", func.__qualname__, name)
        self._idx += 1
        id = self._idx
        if name not in self._events:
            self._events[name] = []
        self._events[name].append(func)
        self._handlers[id] = (func, name)
        return id

    def emit(self, name: str, *args, **kwargs) -> None:
        """Emit a named event with arguments.

        The event handler gets called with the arguments (args, and kwargs).
        """
        if name not in self._events:
            return
        for func in self._events[name]:
            # log.debug(f"Emitting %s to %s", name, func.__qualname__)
            try:
                func(*args, **kwargs)
            except Exception as exc:
                log.error(f"Error emitting %s: %s", name, exc, exc_info=True)

    def unbind(self, handler: int) -> bool:
        """Unregister a handler for a named event Either by name or by id."""
        if handler not in self._handlers:
            return False
        func, name = self._handlers.pop(handler)
        self._events[name].remove(func)
        return True

    def once(self, name: str, func: Callable) -> None:
        """Register a handler for a named event that will be called only once."""
        def wrapper(*args, **kwargs):
            self.unbind(handler)
            return func(*args, **kwargs)
        handler = self.on(name, wrapper)