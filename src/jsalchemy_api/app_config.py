import os
import sys
import traceback
from datetime import datetime
from typing import Dict, Any

import click
from pydantic import BaseModel, ConfigDict
from jsalchemy_web_context import ContextManager

from .exceptions import HandledValidation, JSAlchemyException, SessionNotFound
from .resources.manager import ResourceManager
from .utils import load_class, init_all_resources
import logging

logger = logging.getLogger('jsa.web')


class WebConfig(BaseModel):
    port: int = 7999,
    host:str = '0.0.0.0',
    use_cookies:bool = False,
    token_name: str = '__token__',
    realtime: bool = False,
    extra: dict = {}
    root_endpoint: str = '/jsalchemy'
    backend: str = 'blacksheep'
    backend_extra: dict = {}

class RedisConfig(BaseModel):
    url: str = 'redis://localhost:6379'
    extra: dict = {}

class DBConfig(BaseModel):
    url: str = 'sqlite+aiosqlite:///:memory:'
    extra: dict = {}

class RealtimeConfig(BaseModel):
    enabled: bool = False
    redis: RedisConfig = RedisConfig()
    port: int = 7999

class ContextConfig(BaseModel):
    db: DBConfig = DBConfig()
    redis: RedisConfig = RedisConfig()

class APIConfig(BaseModel):
    max_records_per_query: int = 200

class AuthenticationConfig(BaseModel):
    salt: str
    identity_model: str
    identified_by: str
    password_field: str

class AuthorizationConfig(BaseModel):
    enabled: bool = False
    redis: RedisConfig = RedisConfig()

class LogConfig(BaseModel):
    path: str = 'logging.yaml'
    embedded: dict | None = None


class AppConfig(BaseModel):
    model_config = ConfigDict(strict=True)

    web: WebConfig
    api: APIConfig
    context: ContextConfig
    realtime: RealtimeConfig
    authentication: AuthenticationConfig
    authorization: AuthorizationConfig | None = None
    module_path: str = "modules"
    logging: LogConfig = LogConfig()




def setup_context(config: AppConfig, sync: bool=False) -> ContextManager:
    """set up all components downstream and return the context manager"""

    if sync:
        from redis import Redis
        from jsalchemy_web_context.sync import ContextManager
        from sqlalchemy import create_engine, sessionmaker
        engine = create_engine(config.context.db.url, **config.db.extra)
        session_maker = sessionmaker(bind=engine, expire_on_commit=False)
    else:
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        from redis.asyncio import Redis
        from jsalchemy_web_context import ContextManager
        engine = create_async_engine(config.context.db.url, **config.context.db.extra)
        session_maker =async_sessionmaker(bind=engine, expire_on_commit=False)


    redis_connection = Redis.from_url(config.realtime.redis.url, **config.realtime.redis.extra)
    context_manager = ContextManager(session_maker, redis_connection, auto_commit=True, trace_changes=True)
    return context_manager

def blacksheep_app_setup(config: AppConfig, resource_manager: ResourceManager):
    """set up a web application"""
    from blacksheep import Application, Response, Content, get, post, HTTPException
    import orjson

    app = Application()
    root_endpoint = config.web.root_endpoint
    token_key = config.web.token_name

    def json_response(d: dict, status=200):
        return Response(
            status, content=Content(
                content_type=b'application/json',
                data=orjson.dumps(d),
            ))

    @get('/api/health')
    async def health():
        """Health check endpoint"""
        return json_response({
            'status': 'ok',
            'service': 'JSAlchemy Backend',
            'timestamp': datetime.now().isoformat(),
        })

    @post(f'/auth/register')
    async def register(body: dict) -> str:
        """Register a new user"""
        try:
            if await resource_manager.auth_man.user_exists(body['email']):
                return json_response({'error': 'Username already exists', 'ts': datetime.now()}, 401)
            return await resource_manager.auth_man.register(body)
        except HandledValidation as e:
            return json_response({ 'error': e.errors, 'ts': datetime.now()}, 400)
        except Exception as e:
            traceback.print_tb(sys.exc_info()[2])
            raise HTTPException(500, 'suca')

    @post(root_endpoint + f'/auth/login')
    async def login(body: dict) -> str:
        """Login and return the session token."""
        username, password = body.get('username'), body.get('password')
        ret = await resource_manager.login(username, password)
        if not ret:
            return json_response({'error': 'Invalid username or password.'}, 403)
        return ret

    @post(root_endpoint + f'/auth/logout')
    async def logout(body: dict) -> str:
        token = body.get(token_key)
        ret = await resource_manager.logout(token)
        return ret

    @post(root_endpoint + r"/{action}")
    async def jsalchemy(action: str, body: dict) -> Response:
        if '.' not in action:
            return json_response(
                {'error': "Invalid URL it must be in the for /jsalchemy/{model}.{verb}",
                 'ts': datetime.utcnow()}, 400)
        resource, verb = action.split('.', 1)
        token = body.pop(token_key, None)
        if not token:
            return json_response({'error': "Connection unidentified. Please login", 'ts': datetime.utcnow()}, 403)
        try:
            return json_response(await rm.action(token, resource, verb, **body))
        except JSAlchemyException as e:
            return json_response({ 'error': e.message, 'ts': datetime.now()}, e.status_code)
        except SessionNotFound:
            return json_response({'error': 'Session not found', 'ts': datetime.now()}, 403)
        except Exception as e:
            tb = traceback.format_tb(sys.exc_info()[2])
            logger.error('Error while executing action %s.%s', resource, verb, exc_info=e)
            return json_response({'error': str(e), 'ts': datetime.now(), 'traceback': tb}, 500)

    return app

def fastapi_app_setup(config: dict) -> ResourceManager:
    """set up a web application"""


BACKENDS = {
    'blacksheep': blacksheep_app_setup,
    'fastapi': fastapi_app_setup
}

def log_setup(config: AppConfig):
    import logging

    if config.logging.embedded:
        logging.config.dictConfig(config.logging.embedded)
    else:
        import yaml
        if not os.path.exists(config.logging.logfile):
            click.echo(f'Log file {config.logging.logfile} not found', err=True)
            raise click.FileError(config.logging.logfile, 'Log file not found')
        with open(config.logging.path) as f:
            logging_config = yaml.safe_load(f)
        logging.config.dictConfig(logging_config)

def app_setup(config: AppConfig, backend: str=None, autodiscover: bool=False) -> ResourceManager:
    """set up a web application"""
    from jsalchemy_authentication.manager import AuthenticationManager
    from jsalchemy_auth.models import UserMixin

    log_setup(config)

    context_manager = setup_context(config, sync=False)

    identity_model: UserMixin = load_class(config.authentication.identity_model)
    authentication_manager = AuthenticationManager(identity_model, context=context_manager,
                                                   password_field=config.authentication.password_field,
                                                   salt=config.authentication.salt, identified_by=config.authentication.identified_by)

    # TODO Integrate the authorization module
    if config.authorization:
        auth_config = config.authorization
        from jsalchemy_auth.auth import Auth
        auth = Auth(**auth_config)


    resource_manager = ResourceManager(context=context_manager, auth_man=authentication_manager)
    if autodiscover:
        init_all_resources(resource_manager, path=config.module_path)
    backend = backend or config.backend.backend
    if backend not in BACKENDS:
        raise ValueError(f'Invalid backend: {backend}')
    app = BACKENDS[backend](config, resource_manager)
    return app

