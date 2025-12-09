import asyncio
from collections import defaultdict

from marshal import loads
from pickle import loads as ploads
from typing import List, Dict

from click import style
from redis.asyncio import Redis
from websockets import ConnectionClosedOK, ConnectionClosedError
from websockets.asyncio.server import serve, ServerConnection

from logging import getLogger
logger = getLogger('web.realtime')

class WSServer:
    """WebSocket powered server."""
    redis_channel: str

    def __init__(self, host: str = '0.0.0.0', port: int =   7998,
                 redis_url: str | Redis = 'redis://localhost:6379',
                 redis_channel: str = 'js-router'):
        """WebSocket server.

        Args:
            host (str, optional): The host to bind to. Defaults to '0.0.0.0'.
            port (int, optional): The port to bind to. Defaults to 7998.
            redis_url (str | Redis, optional): The Redis URL or instance. Defaults to 'redis://localhost:6379'.
            redis_channel (str, optional): The Redis channel to use. Defaults to 'js-router'.
        """
        self.host: str = host
        self.port: int = port
        self.redis_channel: str = redis_channel
        if (isinstance(redis_url, str)):
            self.redis = Redis.from_url(redis_url)
        elif (isinstance(redis_url, Redis)):
            self.redis = redis_url
        else:
            raise ValueError('redis_url must be a string or a Redis instance')
        self.users: Dict[int, ServerConnection] = {}
        self.groups: Dict[int, ServerConnection] = defaultdict(list)

    def connection_close(self, session, status_code: int, reason: str):
        """Safely closes the WebSocket connection."""
        if session:
            if 'user_id' in session:
                ws = self.users.pop(session['user_id'], None)
                if ws:
                    ws.close(status_code, reason)
            if 'group_ids' in session:
                self.groups.pop(session['group_ids'], None)

    async def message_handler(self, ws: ServerConnection) -> None:
        """Handle any message coming form the client."""
        session = None
        try:
            while True:
                try:
                    message = await ws.recv()
                    if ':' not in message:
                        logger.warning('Message out of prodocol %s', style(str(message), fg='red'))
                        return self.connection_close(session, 403, 'Unauthorized')
                    command, *args = message.split(':')
                    if command == 'TOKEN':
                        session_value = await self.redis.get(f'session:{args[0]}')
                        if not session_value:
                            logger.info('Client Session %s not found', style(str(args[0]), fg='red'))
                            return self.connection_close(session, 403, 'Unauthorized')
                        try:
                            session = ploads(session_value)
                        except ValueError:
                            logger.warning(f'Session corrupted {args[0]}.')
                            return self.connection_close(session, 403, 'Connection corrupted')
                        if 'user_id' not in session:
                            logger.warning('Client %s connected but user not found on %s',
                                        style(str(args[0]), fg='yellow'),
                                        style(str(session), fg='green'))
                            return self.connection_close(session, 403, 'Unauthorized')
                        logger.info('User %s connected', style(str(session['user_id']), fg='green'))
                        user_id = session.get('user_id')
                        if user_id:
                            self.users[user_id] = ws
                        group_ids = session.get('group_ids')
                        if group_ids:
                            for group_id in group_ids:
                                self.groups[group_id].append(ws)

                except ConnectionClosedError:
                    return logger.info('Client disconnected abnormally')
                except ConnectionClosedOK:
                    return logger.info('Client %s disconnected', style(str(session['user_id']), fg='green'))
        finally:
            if session:
                if 'user_id' in session:
                    self.users.pop(session['user_id'], None)
                if 'group_ids' in session:
                    self.groups.pop(session['group_ids'], None)
            await ws.close()

    async def to_clients(self, users: List[int], groups: List[int], message: str):
        """Sends the message to the logged-in users and groups"""
        if users == 'all':
            logger.debug('Sending message to all clients')
            users = self.users.keys()
        logger.debug('Sending message to %s users and %s groups',
                     style(str(users), fg='green'), style(str(groups), fg='green'))
        for user in users or ():
            ws: ServerConnection = self.users.get(user)
            if ws:
                await ws.send(message, text=True)
        for group in groups or ():
            ws: ServerConnection = self.groups.get(group)
            if ws:
                await ws.send(message)

    async def read_redis(self):
        """Reads messages from Redis and sends them to the clients."""
        while True:
            channel, message = await self.redis.brpop(self.redis_channel)
            try:
                if message:
                    logger.debug('Received message from %s %s',
                                 style(channel, fg='yellow'), style(message, fg='green'))
                    await self.to_clients(*loads(message))
            except Exception as e:
                logger.error('Error receiving message', exc_info=True)

    def start(self):
        async def run():
            async with serve(self.message_handler, self.host, self.port) as server:
                await server.serve_forever();

        asyncio.run(asyncio.gather(run(), self.read_redis(), self.read_redis()))
