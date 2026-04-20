"""The ``celery upgrade`` command, used to upgrade from previous versions."""
import codecs
import sys

import click

from celery.app import defaults
from celery.bin.base import CeleryCommand, CeleryOption, handle_preload_options
from celery.utils.functional import pass1


@click.group()
@click.pass_context
@handle_preload_options
def upgrade(ctx):
    """Perform upgrade between versions."""


def _slurp(filename):
    # TODO: Handle case when file does not exist
    pass


def _compat_key(key, namespace='CELERY'):
    pass


def _backup(filename, suffix='.orig'):
    pass


def _to_new_key(line, keyfilter=pass1, source=defaults._TO_NEW_KEY):
    # sort by length to avoid, for example, broker_transport overriding
    # broker_transport_options.
    pass


@upgrade.command(cls=CeleryCommand)
@click.argument('filename')
@click.option('--django',
              cls=CeleryOption,
              is_flag=True,
              help_group='Upgrading Options',
              help='Upgrade Django project.')
@click.option('--compat',
              cls=CeleryOption,
              is_flag=True,
              help_group='Upgrading Options',
              help='Maintain backwards compatibility.')
@click.option('--no-backup',
              cls=CeleryOption,
              is_flag=True,
              help_group='Upgrading Options',
              help="Don't backup original files.")
def settings(filename, django, compat, no_backup):
    """Migrate settings from Celery 3.x to Celery 4.x."""
    pass
