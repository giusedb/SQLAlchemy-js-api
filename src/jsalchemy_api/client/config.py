import os

import yaml

from jsa.base import Config, LoginInfo


def from_file(filepath: str=None):
    if filepath is None:
        filepath = os.sep.join(os.path.abspath(__file__).split(os.sep)[:-3] + ['config', 'conf.yaml'])
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Configuration file not found: {filepath}")
    with open(filepath, 'r') as f:
        data = yaml.safe_load(f)
        data['login'] = LoginInfo(**data['login'])
        return Config(**data)

def setup_log(filepath: str=None):
    """Set up the client logging."""
    import os
    from logging import config as log_config
    import yaml

    if not filepath:
        filepath = os.sep.join(__file__.split(os.sep)[:-3] + ['config', 'logging.yaml'])
    if os.path.exists(filepath):
        with open(filepath) as f:
            conf_dict = yaml.safe_load(f)
        log_config.dictConfig(conf_dict)
