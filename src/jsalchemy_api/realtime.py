import asyncio
from asyncio import gather
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

RECONNECT_DELAY = 5  # seconds between redis reconnection attempts

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
                            self.connection_close(session, 403, 'Unauthorized')
                            return None
                        try:
                            session = ploads(session_value)
                        except ValueError:
                            logger.warning(f'Session corrupted {args[0]}.')
                            self.connection_close(session, 403, 'Connection corrupted')
                        if 'user_id' not in session:
                            logger.warning('Client %s connected but user not found on %s',
                                        style(str(args[0]), fg='yellow'),
                                        style(str(session), fg='green'))
                            self.connection_close(session, 403, 'Unauthorized')
                            return None
                        logger.info('User %s connected (%d users connected)',
                                    style(str(session['user_id']), fg='green'), len(self.users))
                        user_id = session.get('user_id')
                        if user_id:
                            self.users[user_id] = ws
                        group_ids = session.get('group_ids')
                        if group_ids:
                            for group_id in group_ids:
                                self.groups[group_id].append(ws)

                except ConnectionClosedError:
                    logger.info('Client disconnected abnormally (%d users connected)',len(self.users))
                except ConnectionClosedOK:
                    logger.info('Client %s disconnected (%d users connected)',
                                style(str(session['user_id']), fg='green'), len(self.users))
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
                     style(', '.join(map(str, users)), fg='green'), style(str(groups), fg='green'))
        for user in users or ():
            ws: ServerConnection = self.users.get(user)
            if ws:
                await ws.send(message, text=True)
        for group in groups or ():
            ws: ServerConnection = self.groups.get(group)
            if ws:
                await ws.send(message)

    async def read_redis(self):
        """Subscribes to Redis pub/sub and broadcasts messages to all connected clients.

        Each WSServer instance gets its own copy of every message, so horizontal
        scaling works correctly — every client receives every notification.
        Automatically re-subscribes on connection loss.
        """
        while True:
            try:
                pubsub = self.redis.pubsub()
                await pubsub.subscribe(self.redis_channel)
                logger.info('Subscribed to Redis channel %s',
                            style(self.redis_channel, fg='green'))
                async for message in pubsub.listen():
                    if message['type'] == 'message':
                        data = message['data']
                        if data:
                            logger.debug('Received pub/sub message from %s',
                                         style(self.redis_channel, fg='yellow'))
                            await self.to_clients(*loads(data))
            except (ConnectionError, OSError, asyncio.TimeoutError) as e:
                logger.error('Redis connection lost (pub/sub): %s. Reconnecting in %ds...',
                             e, RECONNECT_DELAY)
                await asyncio.sleep(RECONNECT_DELAY)
            except Exception:
                logger.exception('Unexpected error in pub/sub loop. Reconnecting in %ds...',
                                 RECONNECT_DELAY)
                await asyncio.sleep(RECONNECT_DELAY)

    def start(self):
        async def run_server():
            async with serve(self.message_handler, self.host, self.port) as server:
                await server.serve_forever();

        async def both():
            return await asyncio.gather(run_server(), self.read_redis())

        asyncio.run(both())
