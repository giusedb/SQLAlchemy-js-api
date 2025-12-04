from marshal import dumps as dumps
from orjson import dumps as jdumps

from redis.asyncio import Redis

from jsalchemy_api.resources.base import ResultData
from jsalchemy_web_context import request

class Messanger:
    """Manage the propagation queue in Redis."""

    def __init__(self, resource_manager: 'ResourceManager', queue_name: str):
        """Manage the propagation queue in Redis"""
        self.redis = resource_manager.context.redis
        self.q_name = queue_name
        self.res_man = resource_manager

    async def rt_send(self, message: str, users: list[str] = None, groups: list[str] = None):
        """Send a message to a list of users and groups"""
        encapsulated = dumps((users, groups, message))
        await self.redis.lpush(self.q_name, encapsulated)

    def propagate(self, message: dict = None) -> None:
        """Propagate any message available on `request.results`."""
        if message is None:
            message = request.result.to_dict(self.res_man)
            message.pop('description', None)
        if message:
            return self.rt_send(jdumps(message), 'all')

