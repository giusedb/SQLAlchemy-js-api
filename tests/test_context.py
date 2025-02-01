import asyncio
from asyncio import gather, run

import pytest

from quasar_api.context import request
from quasar_api.context.manager import ContextProxy


def test_proxy_dict_isolation():

    prop = ContextProxy('property')
    elements = [{'foo': x} for x in range(100)]
    result = []

    def connect():
        prop.update(elements.pop())

    async def task():
        result.append(prop['foo'])
        await asyncio.sleep(0)
        prop['bar'] = []
        prop['bar'].append('-')
        await asyncio.sleep(0)
        result.append(''.join(prop['bar']))

    def prepare():
        connect()
        return asyncio.create_task(task())

    async def main():
        await asyncio.gather(*(prepare() for _ in range(10)))
        await asyncio.gather(*(prepare() for _ in range(10)))

    asyncio.run(main())
    max_dashes = max(map(len, filter(lambda x: type(x) is str and x.startswith('-'), result)))
    assert max_dashes == 1
    assert len(result) == 40



def test_context_enter(context_manager):
    async def init():
        token, session = await context_manager.web_session_man.new('mytoken')
        await context_manager.web_session_man.disconnect(session, token)
        async with context_manager(token=token) as ctx:
            request.foo = 10

    asyncio.run(init())

def test_context_segregation(context_manager):
    from quasar_api.context import request, session

    async def request1(x):
        token = None
        async def req(y):
            nonlocal token
            async with context_manager(token) as ctx:
                print(f'r1:{x}:{y} {ctx.token}')
                assert bool(token) == bool(y)
                if token:
                    _ = session.foo == 'foobar', 'Session not connected'
                    assert token == ctx.token, "Session doesn't reconnect"
                else:
                    with pytest.raises(AttributeError):
                        _ = session.foo
                    request.foo = 'bar'
                    session.foo = 'foobar'
                token = ctx.token

            async with context_manager(token) as ctx:
                print(f'r2:{x}:{y} {ctx.token}')
                assert session.foo == 'foobar', 'Session not connected'
                with pytest.raises(AttributeError):
                    _ = request.foo
                request.foo = 'bar'
                assert request.foo == 'bar', 'Request not connected'
            print(f"Done {x}:{y}")
        await req(0)
        await req(1)
        await req(2)

    async def main():
        await gather(*(asyncio.create_task(request1(_)) for _ in range(4)))

    run(main())
