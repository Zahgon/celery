"""The default loader used when no custom app has been initialized."""
import os
import warnings

from celery.exceptions import NotConfigured
from celery.utils.collections import DictAttribute
from celery.utils.serialization import strtobool

from .base import BaseLoader

__all__ = ('Loader', 'DEFAULT_CONFIG_MODULE')

DEFAULT_CONFIG_MODULE = 'celeryconfig'

#: Warns if configuration file is missing if :envvar:`C_WNOCONF` is set.
C_WNOCONF = strtobool(os.environ.get('C_WNOCONF', False))


class Loader(BaseLoader):
    """The loader used by the default app."""

    def setup_settings(self, settingsdict):
        pass

    def read_configuration(self, fail_silently=True):
        """Read configuration from :file:`celeryconfig.py`."""
        pass
