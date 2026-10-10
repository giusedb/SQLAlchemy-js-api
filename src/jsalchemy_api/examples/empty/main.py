import os.path

import click
import yaml
from pydantic import ValidationError

from jsalchemy_api.app_config import AppConfig


@click.command()
@click.option('--config', '-c', type=click.Path(), default='config.yaml', help='Path to the configuration file')
@click.option('--backend', '-b', type=click.Choice(['blacksheep', 'fastapi']), default='blacksheep', help='Backend to use')
@click.option('--autodiscover', '-A', is_flag=True, default=False, help='Autodiscover models and resources')
def web(config, backend, autodiscover: bool):
    """Run the web application"""
    from jsalchemy_api.app_config import app_setup
    if os.path.exists(config):
        with open(config) as f:
            conf = yaml.safe_load(f)
        try:
            conf = AppConfig(**conf)
        except ValidationError as e:
            click.FileError(config, 'Invalid configuration file')
            for error in e.errors():
                click.echo('.'.join(error['loc']), err=True)
                click.echo(' - ' + error['msg'], err=True)
                click.echo(f"   got: {error['input']} instead", err=True)
            raise click.FileError(config, 'Invalid configuration file')
    else:
        raise click.FileError(config, 'Configuration file not found')
    app = app_setup(conf, backend, autodiscover)

    import uvicorn
    uvicorn.run(app, **conf.web.backend_extra)

if __name__ == '__main__':
    web()