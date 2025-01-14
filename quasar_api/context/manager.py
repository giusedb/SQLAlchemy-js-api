from typing import Callable

from redis.asyncio import Redis

from .base import FixedContextManger
from .redis import RedisSessionManager




class QuasarContextManager(FixedContextManger):
    """Quasar request context manager."""

    vars = ('db', 'session', 'token', 'update_log')

    def __init__(self, resource_manager, session_maker: Callable, web_session_cpnnection: Redis):
        self.resource_manager = resource_manager
        self.session_maker = session_maker
        self.web_session_manager = RedisSessionManager(web_session_cpnnection)

    async def __aenter__(self):
        self.session = await self.web_session_manager.connect(self.token)
        self.db = self.session_maker()
        self.update_log = []
        self.rt_permissions = []
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.session.disconnect()
        if not any((exc_val, exc_tb, exc_type)):
            await self.db.commit()
        else:
            await self.db.rollback()

    def __call__(self, token):
        self.token = token
        return self
