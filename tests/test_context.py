import asyncio
from asyncio import gather, create_task

from quasar_api.resources.manager import AsyncSafeContext


def test_isolation():

    item = AsyncSafeContext('item')
    n = 0

    def init():
        nonlocal n
        item.init()
        item.n = n
        n += 1

    async def task():
        init()
        await asyncio.sleep(0)
        print(item.n, n)
        return [item.n, n]

    async def main():
        x = 10
        result = await asyncio.gather(*(create_task(task()) for _ in range(x)))
        safe, unsafe = map(set, zip(*result))
        assert len(safe) == x, 'Non thread safe'
        assert len(unsafe) == 1, 'Unsafe variable are actually safe'

    asyncio.run(main())
