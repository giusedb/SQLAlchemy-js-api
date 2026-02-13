from datetime import date

import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from sqlalchemy import Select, create_engine, ForeignKey, Column, Integer, Table, String
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker, relationship

from jsalchemy_auth.models import UserMixin
from jsalchemy_authentication.manager import AuthenticationManager
from jsalchemy_authentication.mixins import IdentityMixin
from jsalchemy_web_context import ContextManager, db


def print_SQL(query):
    return str(query.compile(compile_kwargs={'literal_binds': True}))

Select.__str__ = Select.__repr__ = print_SQL


@pytest.fixture
def create_tables(Base, db_engine):
    async def define_tables():
        """Define the tables."""
        async with db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    return define_tables

@pytest.fixture
def sync_db_engine():
    """Create a test SQLAlchemy database engine."""
    engine = create_engine('sqlite:///:memory:')
    return engine

@pytest.fixture()
def db_engine():
    """Create a test SQLAlchemy database engine."""
    engine = create_async_engine('sqlite+aiosqlite:///:memory:')
    return engine

@pytest.fixture()
def session(db_engine):
    """Create a SQLAlchemy database session."""
    return async_sessionmaker(bind=db_engine, expire_on_commit=False, autoflush=True)

@pytest.fixture()
def sync_session(sync_db_engine):
    """Create a SQLAlchemy database session."""
    return sessionmaker(bind=sync_db_engine)()

@pytest.fixture()
def context(session):
    """Build the jsalchemy_web_context context manager."""
    return ContextManager(session, FakeRedis(), auto_commit=True)

# @pytest.fixture
# def context(context_manager):
#     """Build the jsalchemy_web_context context manager."""
#     return context_manager()

@pytest.fixture
def Base():
    """Create the base model"""
    class Base(AsyncAttrs, DeclarativeBase):
        id: Mapped[int] = mapped_column(primary_key=True)

        def __str__(self):
            return f"{self.__class__.__name__}:[{self.id}]"

        def __repr__(self):
            return f"{self.__class__.__name__}({self.id})"
    return Base


@pytest.fixture
def cls_Person(Base):

    class Person(AsyncAttrs, Base):

        __tablename__ = 'person'

        id: Mapped[int] = mapped_column(primary_key=True)
        first_name: Mapped[str]
        last_name: Mapped[str]
        date_of_birth: Mapped[date]

        def __str__(self):
            return f"{self.first_name} {self.last_name.upper()}"

        def __repr__(self):
            return f"Person({self.first_name}, {self.last_name}, {self.date_of_birth})"

    return Person

@pytest.fixture
def cls_filesystem(Base):
    """Provide a DB-based filesystem emulation."""
    class Folder(Base):
        __tablename__ = 'folders'
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        parent_id: Mapped[int] = mapped_column(ForeignKey('folders.id'), nullable=True)
        parent: Mapped['Folder'] = relationship('Folder', remote_side=[id])
        tags: Mapped[list['Tag']] = relationship('Tag', secondary='folder_tag', back_populates="folders")


    class File(Base):
        __tablename__ = 'files'
        name: Mapped[str]
        folder_id: Mapped[int] = mapped_column(ForeignKey('folders.id'))
        folder: Mapped['Folder'] = relationship('Folder', remote_side=[Folder.id])
        tags: Mapped[list['Tag']] = relationship('Tag', secondary='file_tag', back_populates="files")

    file_tag = Table(
        'file_tag',
        Base.metadata,
        Column('file_id', Integer, ForeignKey('files.id')),
        Column('tag_id', Integer, ForeignKey('tags.id')),
    )

    folder_tag = Table(
        'folder_tag',
        Base.metadata,
        Column('folder_id', Integer, ForeignKey('folders.id')),
        Column('tag_id', Integer, ForeignKey('tags.id')),
    )

    class Tag(Base):
        __tablename__ = 'tags'
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str]
        files: Mapped[list[File]] = relationship(File, secondary=file_tag, back_populates='tags')
        folders: Mapped[list[Folder]] = relationship(Folder, secondary=folder_tag, back_populates='tags')

    return Folder, File, Tag


@pytest_asyncio.fixture
async def Person(cls_Person, create_tables):
    """Create Person model and person table."""
    await create_tables()
    return cls_Person


@pytest_asyncio.fixture
async def filesystem(cls_filesystem, create_tables):
    """Create Filesystem model and filesystem tables."""
    await create_tables()
    return cls_filesystem

@pytest.fixture
def cls_auth(Base):
    class User(UserMixin, Base):
        __tablename__ = 'users'
        id: Mapped[int] = mapped_column(primary_key=True)
        first_name: Mapped[str] = mapped_column(String(150), nullable=True)
        last_name: Mapped[str] = mapped_column(String(150), nullable=True)

        def __str__(self):
            return f"User:{self.id}"

        def __repr__(self):
            return f"User({self.first_name} {self.last_name})"

    class Identity(IdentityMixin, Base):
        __tablename__ = 'identities'
        id: Mapped[int] = mapped_column(primary_key=True)
        username: Mapped[str]
        password: Mapped[str]
        user_id: Mapped[int] = mapped_column(ForeignKey('users.id'))
        user: Mapped[User] = relationship('User', backref='idents')

    return Identity, User

@pytest_asyncio.fixture
async def auth_classes(cls_auth, create_tables):
    await create_tables()
    return cls_auth

@pytest.fixture
def auth(auth_classes, context):
    """Create an authentication manager"""
    Identity, User = auth_classes
    return AuthenticationManager(Identity, context, 'secret')

@pytest_asyncio.fixture
async def base_users(auth_classes, context):
    Identity, User = auth_classes
    async with context():
        db.add(User(id=1, first_name='Alice', last_name='Smith'))
        db.add(User(id=2, first_name='Bob', last_name='Smith'))
        db.add(User(id=3, first_name='Charlie', last_name='Smith'))
        db.flush()
        db.add(Identity(username='alice', password='alice', user_id=1))
        db.add(Identity(username='bob', password='bob', user_id=2))
        db.add(Identity(username='charlie', password='charlie', user_id=3))

@pytest.fixture
def all_types(Base):
    from sqlalchemy.types import Integer, String, CLOB, BLOB, BigInteger, Double, Float, Boolean, Interval, LargeBinary,\
        PickleType, SmallInteger, TupleType, Uuid, JSON

    class AllTypes(Base):
        __tablename__ = 'all_types'
        clob: Mapped[str] = mapped_column(CLOB, nullable=False, comment='this contains a long text')
        integer: Mapped[int] = mapped_column(Integer, nullable=False)
        string: Mapped[str] = mapped_column(String, nullable=False)
        blob: Mapped[str] = mapped_column(BLOB, nullable=False)
        big_integer: Mapped[int] = mapped_column(BigInteger, nullable=False)
        double: Mapped[float] = mapped_column(Double, nullable=False)
        flt: Mapped[float] = mapped_column(Float, nullable=False)
        boolean: Mapped[bool] = mapped_column(Boolean, nullable=False)
        interval: Mapped[tuple] = mapped_column(Interval, nullable=False)
        large_binary: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
        obj: Mapped[any] = mapped_column(PickleType, nullable=False)
        small_int: Mapped[int] = mapped_column(SmallInteger, nullable=False)
        tuple_type: Mapped[tuple] = mapped_column(TupleType, nullable=False)
        uuid: Mapped[str] = mapped_column(Uuid, nullable=False)
        json: Mapped[dict] = mapped_column(JSON, nullable=False)

    return AllTypes

