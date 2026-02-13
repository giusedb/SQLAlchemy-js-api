import importlib
from typing import Dict, Any

from sqlalchemy import Select, false

from jsalchemy_api import ResourceManager
from jsalchemy_auth.models import UserMixin
from jsalchemy_api.utils import load_class

def print_SQL(query):
    return str(query.compile(compile_kwargs={'literal_binds': True}))

Select.__str__ = Select.__repr__ = print_SQL


def base_environment(config: Dict[str, Any], sync: bool = False, init_db=False):
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
    if sync:
        from redis import Redis
        from jsalchemy_web_context.sync import ContextManager
    else:
        from redis.asyncio import Redis
        from jsalchemy_web_context import ContextManager

    db_config = config['context']['db']
    db_uri = db_config.pop('url', None)
    if db_uri:
        engine = create_async_engine(db_uri, **db_config)
    else:
        engine = create_async_engine(**db_config)

    session_maker =async_sessionmaker(bind=engine, expire_on_commit=False)

    redis_config = config['context']['redis']
    redis_url = redis_config.pop('url', None)
    if redis_url:
        redis_connection = Redis.from_url(redis_url, **redis_config)
    else:
        redis_connection = Redis(**redis_config)
    context_manager = ContextManager(session_maker, redis_connection, auto_commit=not init_db)
    return context_manager

def setup_application(config: Dict[str, Any], init_db=False) -> ResourceManager:
    """Set up the application and returns the resource manager."""
    from jsalchemy_authentication.manager import AuthenticationManager

    context_manager = base_environment(config, init_db=init_db)

    authentication_config = config['authentication']
    identity_model = None
    identified_by = None
    password_field = 'password'
    if 'identity-model' in authentication_config:
        identity_model: UserMixin = load_class(authentication_config['identity-model'])
    if 'identified-by' in authentication_config:
        identified_by = authentication_config['identified-by']
    if 'password-field' in authentication_config:
        password_field = authentication_config['password-field']
    authentication_manager = AuthenticationManager(identity_model, context=context_manager, password_field=password_field,
                                                   salt=authentication_config['salt'], identified_by=identified_by)
    if 'authorization' in config:
        auth_config = config['authorization']
        from jsalchemy_auth.auth import Auth
        auth = Auth(**auth_config)
    realtime = config.get('web', {}).get('realtime')
    resource_manager = ResourceManager(context=context_manager, auth_man=authentication_manager,
                                       realtime_queue=realtime.get('redis_channel'), disable_interceptor=init_db)


    return resource_manager
