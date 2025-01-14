from uuid import uuid4

from redis import Redis
from pickle import loads, dumps, UnpicklingError

from .base import Session, SessionManager


class RedisSession(Session):

    _content = None
    _token = None

    def __init__(self, token: str, content: dict, session_manager: SessionManager):
        self._content = content
        self._token = token
        self._manager = session_manager

    def __getitem__(self, item):
        if item in self._content:
            return self._content[item]
        return None

    def __setitem__(self, key, value):
        self._content[key] = value

    def __delitem__(self, key):
        self._content.pop(key)

    def __iter__(self):
        return iter(self._content)

    def __contains__(self, item):
        return item in self._content

    def __len__(self):
        return len(self._content)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._content})"

    def dumps(self):
        return dumps(self._content)

    async def disconnect(self):
        await self._manager.disconnect(self)


class RedisSessionManager(SessionManager):

    def __init__(self, redis_connection: Redis = None, key: str='session',
                 duration: int = 3600):
        self.connection = redis_connection
        self.key = key
        self.duration = duration
        self.session_format = "{self.key}:{token}"

    async def connect(self, token):
        raw = await self.connection.get(self.session_format.format(token=token, self=self))
        if raw is None:
            raise SessionNotFound(token)
        try:
            return RedisSession(token, loads(raw), self)
        except UnpicklingError:
            raise SessionNotFound('Session corrupted')

    async def disconnect(self, session: RedisSession):
        await self.connection.set(self.session_format.format(self=self, token=session._token),
                                  session.dumps(), ex=self.duration)

    async def new(self, token:str = None):
        token = token or str(uuid4())
        while await self.connection.keys(self.session_format.format(self=self, token=token)):
            token = str(uuid4())
        await self.connection.set(self.session_format.format(self=self, token=token), dumps({}))
        return RedisSession(token, {}, self)

    def destroy(self, token):
        return self.connection.delete(self.session_format.format(self=self, token=token))
