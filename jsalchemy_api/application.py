import urllib
from types import FunctionType
from typing import List

from data.models.base import BaseModel
from jsalchemy_api.context.manager import ContextManager
from jsalchemy_api.resources.manager import ResourceManager

from redis.asyncio import Redis


def setup_application(db_url: str = None, session_maker: FunctionType = None,
                      session_url: str = None, redis_connection: Redis = None,
                      auto_discover_models: bool = True, models: List[BaseModel] = []) -> ResourceManager:
    """Set up the application and returns the resource manager."""
    from jsalchemy_api import context

    db_parsed = urllib.parse.urlparse(db_url)
    session_parsed = urllib.parse.urlparse(session_url)

    if auto_discover_models and models:
        raise ValueError('You cannot use auto-discver together with a specific list of models.')

    # if auto_discover_models:
    #     import gc
    #     models = {m for m in gc.get_objects() if m and type(m) == BaseModel}

    # TODO validate the parsed URLs with the available options
    if db_url:
        from sqlalchemy.ext.asyncio import async_sessionmaker
        from sqlalchemy.ext.asyncio import create_async_engine
        engine = create_async_engine(db_url)
        session_maker = async_sessionmaker(bind=engine)

    if session_url:
        from redis.asyncio import Redis
        redis_connection = Redis.from_url(session_url)

    resource_manager = ResourceManager()
    context.context = ContextManager(resource_manager, session_maker, redis_connection)
    return resource_manager
