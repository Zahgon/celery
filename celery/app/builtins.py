"""Built-in Tasks.

The built-in tasks are always available in all app instances.
"""
from celery._state import connect_on_app_finalize
from celery.utils.log import get_logger

__all__ = ()
logger = get_logger(__name__)


@connect_on_app_finalize
def add_backend_cleanup_task(app):
    """Task used to clean up expired results.

    If the configured backend requires periodic cleanup this task is also
    automatically configured to run every day at 4am (requires
    :program:`celery beat` to be running).
    """
    pass


@connect_on_app_finalize
def add_accumulate_task(app):
    """Task used by Task.replace when replacing task with group."""
    pass


@connect_on_app_finalize
def add_unlock_chord_task(app):
    """Task used by result backends without native chord support.

    Will joins chord by creating a task chain polling the header
    for completion.
    """
    pass


@connect_on_app_finalize
def add_map_task(app):
    pass


@connect_on_app_finalize
def add_starmap_task(app):
    pass


@connect_on_app_finalize
def add_chunk_task(app):
    pass


@connect_on_app_finalize
def add_group_task(app):
    """No longer used, but here for backwards compatibility."""
    pass


@connect_on_app_finalize
def add_chain_task(app):
    """No longer used, but here for backwards compatibility."""
    pass


@connect_on_app_finalize
def add_chord_task(app):
    """No longer used, but here for backwards compatibility."""
    pass
