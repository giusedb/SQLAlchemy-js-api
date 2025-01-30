from asyncio import gather, run

from quasar_api.context.manager import ContextProxy, ContextManager
import pytest
import asyncio

@pytest.fixture
def context_manager():

    class MockSession:
        async def commit(self):
            await asyncio.sleep(0)

        async def rollback(self):
            await asyncio.sleep(0)

    cm = ContextManager(None, lambda *x: MockSession())
    return cm


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
    from quasar_api.context import request

    async def init():
        token, session = await context_manager.web_session_man.new('mytoken')
        await context_manager.web_session_man.disconnect(session, token)
        async with context_manager(token=token) as ctx:
            request.foo = 10

    asyncio.run(init())

def test_context_segregation(context_manager):
    from quasar_api.context import request, session

    token = None

    async def request1(tok):
        nonlocal token
        async with context_manager(tok) as ctx:
            print(f'r1: {ctx.token}')
            if token:
                with pytest.raises(AttributeError):
                    _ = session.foo
                assert token == ctx.token, "Session doesn't reconnect"
            else:
                assert session.foo == 'foobar', 'Session is not persistent'
            token = ctx.token
            request.foo = 'bar'
            session.foo = 'foobar'


    async def request2():
        async with context_manager() as ctx:
            print(f"r2: {ctx.token}")
            with pytest.raises(AttributeError):
                _ = request.foo

    async def main():
        await gather(request1(token), request2(), request1(token))

    run(main())