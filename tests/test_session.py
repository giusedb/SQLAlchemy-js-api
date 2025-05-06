import asyncio

import pytest


def test_session_new():
    from jsalchemy_api.context.redis import RedisSessionManager

    async def cycle():
        session_manager = RedisSessionManager()
        token, session = await session_manager.new()
        session.foo = 'foo'
        assert session.foo == 'foo'
        await session_manager.disconnect(session, token)

    asyncio.run(cycle())


def test_session_connect():
    from jsalchemy_api.context.redis import RedisSessionManager

    token = None

    session_manager = RedisSessionManager()

    async def create():
        nonlocal token
        token, session = await session_manager.new()
        session.foo = 'foo'
        await session_manager.disconnect(session, token)

    async def reconnect():
        nonlocal token
        session = await session_manager.connect(token)
        assert session.foo == 'foo'
        with pytest.raises(AttributeError):
            _ = session.bar

    async def main():
        await create()
        await reconnect()

    asyncio.run(main())

