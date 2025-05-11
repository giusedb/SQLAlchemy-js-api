from typing import List

from jsalchemy_api.resources.manager import ResourceManager

default_config = dict(
    web=dict(
        port=7999,
        host='0.0.0.0',
        use_cookies = False,
        token_name = '__token__',
        realtime = False,
    ),
    db_engine=dict(
        url='sqlite+aiosqlite:///:memory:',
    ),
    realtime=dict(
        enabled=False,
        redis_url=None,
    ),
)

def app_setup(config: dict) -> ResourceManager:
