import logging

import httpx

from .named_events import NamedEventManager
from .base import Config
from .utils import kebab_case

log = logging.getLogger('jsa.connection')

class JSAWSConnection:
    """Connect the client to the WebSocket ends."""
    def __init__(self, ws_info: dict):
        self._ws_info = ws_info

class JSAConnection:
    def __init__(self, conf: Config, event_manager: NamedEventManager):
        self._conf = conf
        self._token: str | None = None
        self._status: dict | None = {}
        self.evt = event_manager
        self.is_connected: bool = False
        self.ws: JSAWSConnection = None
        self.client = httpx.Client()
        self._base_url = self._conf.base_url
        if not self._base_url.endswith('/'):
            self._base_url += '/'


    def autologin(self, ttl: int) -> None:
        """Auto login to JSA server and keep session alive for ttl seconds"""

    def fetch(self, resource_name: str, verb: str, kwargs: dict):
        """Fetch from the remote service."""
        return self.post(f'{kebab_case(resource_name)}.{verb}', kwargs)

    def post(self, url: str, data: dict | None=None, headers: dict | None=None):
        """Post to the remote service."""
        data['__token__'] = self._token
        ret = self.client.post(self._base_url + url, json=data, headers=headers)
        if ret.status_code != 200:
            log.error('Falie')
            raise Exception(f"Failed to post to {url}: {ret.status_code}")
        return ret.json()

    def login(self):
        """Login to the remote service."""
        status = self.client.post(self._base_url + 'auth/login', json=self._conf.login.model_dump())
        if status.status_code != 200:
            log.error('Failed to login')
            raise Exception(f"Failed to login: {status.status_code}")
        self._update_status(status.json())

    def logout(self):
        """Logout from the remote service."""
        try:
            log.info('Logging out')
            status = self.post('auth/logout', {})
            self._update_status(status)
            self.evt.emit('logged-out', status)
        except Exception as e:
            log.error(f'Failed to logout: {e}')


    def _update_status(self, status: dict) -> None:
        """Update the status of the connection."""
        last_build = status['last_build']
        old_status, self._status = self._status, status
        if last_build > old_status.get('last_build', 1000000000000):
            self.evt.emit('sytem-upgrade')
        self.is_connected = 'token' in status
        if self.is_connected:
            self._token = status['token']
        if not old_status.get('user') and status.get('user'):
            self.evt.emit('logged-in', status['user']);
        self._token = status['token']
        ws_info = status.get('wsConnection')
        if ws_info:
            self.ws = JSAWSConnection(ws_info)