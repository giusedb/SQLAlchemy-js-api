from quasar_api.context.manager import ContextManager, session, request

if __name__ == '__main__':
    import asyncio

    class MockSession:
        async def commit(self):
            await asyncio.sleep(0)

        async def rollback(self):
            await asyncio.sleep(0)


    cm = ContextManager(None, lambda *x: MockSession())

    token = None

    async def request1():
        token = None
        async def req():
            nonlocal token
            async with cm(token) as ctx:
                print(f'r1: {ctx.token}')
                if token:
                    try:
                        _ = session.foo
                    except AttributeError:
                        pass
                    assert token == ctx.token, "Session doesn't reconnect"
                else:
                    request.foo = 'bar'
                    session.foo = 'foobar'
                token = ctx.token

        await req()
        await req()
        await req()



    async def request2():
        async with cm() as ctx:
            print(f"r2: {ctx.token}")
            try:
                _ = request.foo
            except AttributeError:
                pass



    async def main():
        await asyncio.gather(request2()) # , request2(), request1())

    asyncio.run(main())