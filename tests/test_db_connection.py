from typing import List

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column
from datetime import date
from sqlalchemy import select
from jsalchemy_api.context import db
import asyncio

from jsalchemy_api.resources.manager import ResourceManager
from tests.fixtures import Base


def _test_connection(engine, session_maker):

    class Base(AsyncAttrs, DeclarativeBase):
        pass

    class Person(Base):

        __tablename__ = 'person'

        id: Mapped[int] = mapped_column(primary_key=True)
        first_name: Mapped[str]
        last_name: Mapped[str]
        date_of_birth: Mapped[date]

        def __str__(self):
            return f"{self.first_name} {self.last_name.upper()}"

        def __repr__(self):
            return f"Person({self.first_name}, {self.last_name}, {self.date_of_birth})"


    rm = ResourceManager(session_maker, Redis())

    async def add_person(**data) -> None:
        async with rm.context() as ctx:
            person = Person(**data)
            db.add(person)


    async def all_people() -> List[Person]:
        async with rm.context() as ctx:
            query = select(Person)
            result = await db.execute(query)
            people = result.scalars().all()
            return people

    async def init():
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def main():
        await init()
        assert [] == await all_people()
        await add_person(first_name='Foo', last_name='Bar', date_of_birth=date(2020, 1, 1))
        people = await all_people()
        assert ['Foo Bar'] == [f"{p.first_name} {p.last_name}" for p in people]

    asyncio.run(main())


def test_person(engine, res_man, model_people):
    Person = model_people

    async def init():
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)


    async def add():
        async with res_man() as ctx:
            db.add(Person(first_name='Foo', last_name='Bar', date_of_birth=date(2020, 1, 1)))
            db.add(Person(first_name='FooBar', last_name='Bar', date_of_birth=date(2020, 1, 2)))
            db.add(Person(first_name='Bar', last_name='Foo', date_of_birth=date(2020, 1, 3)))

    async def people():
        async with res_man() as ctx:
            result = await db.execute(select(Person))
            return result.scalars().all()

    async def main():
        await init()
        await add()
        population = await people()
        assert len(population) == 3

    asyncio.run(main())
