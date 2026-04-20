"""Worker remote control command implementations."""
import io
import tempfile
from collections import UserDict, defaultdict, namedtuple

from billiard.common import TERM_SIGNAME
from kombu.utils.encoding import safe_repr

from celery.exceptions import WorkerShutdown
from celery.platforms import EX_OK
from celery.platforms import signals as _signals
from celery.utils.functional import maybe_list
from celery.utils.log import get_logger
from celery.utils.serialization import jsonify, strtobool
from celery.utils.time import rate

from . import state as worker_state
from .request import Request

__all__ = ('Panel',)

DEFAULT_TASK_INFO_ITEMS = ('exchange', 'routing_key', 'rate_limit')
logger = get_logger(__name__)

controller_info_t = namedtuple('controller_info_t', [
    'alias', 'type', 'visible', 'default_timeout',
    'help', 'signature', 'args', 'variadic',
])


def ok(value):
    return {'ok': value}


def nok(value):
    pass


class Panel(UserDict):
    """Global registry of remote control commands."""

    data = {}      # global dict.
    meta = {}      # -"-

    @classmethod
    def register(cls, *args, **kwargs):
        if args:
            return cls._register(**kwargs)(*args)
        return cls._register(**kwargs)

    @classmethod
    def _register(cls, name=None, alias=None, type='control',
                  visible=True, default_timeout=1.0, help=None,
                  signature=None, args=None, variadic=None):

        def _inner(fun):
            pass
        return _inner


def control_command(**kwargs):
    return Panel.register(type='control', **kwargs)


def inspect_command(**kwargs):
    return Panel.register(type='inspect', **kwargs)

# -- App


@inspect_command()
def report(state):
    """Information about Celery installation for bug reports."""
    pass


@inspect_command(
    alias='dump_conf',  # XXX < backwards compatible
    signature='[include_defaults=False]',
    args=[('with_defaults', strtobool)],
)
def conf(state, with_defaults=False, **kwargs):
    """List configuration."""
    pass


def _wanted_config_key(key):
    pass


# -- Task

@inspect_command(
    variadic='ids',
    signature='[id1 [id2 [... [idN]]]]',
)
def query_task(state, ids, **kwargs):
    """Query for task information by id."""
    pass


def _find_requests_by_id(ids,
                         get_request=worker_state.requests.__getitem__):
    for task_id in ids:
        try:
            yield get_request(task_id)
        except KeyError:
            pass


def _state_of_task(request,
                   is_active=worker_state.active_requests.__contains__,
                   is_reserved=worker_state.reserved_requests.__contains__):
    pass


@control_command(
    variadic='task_id',
    signature='[id1 [id2 [... [idN]]]]',
)
def revoke(state, task_id, terminate=False, signal=None, **kwargs):
    """Revoke task by task id (or list of ids).

    Keyword Arguments:
        terminate (bool): Also terminate the process if the task is active.
        signal (str): Name of signal to use for terminate (e.g., ``KILL``).
    """
    # pylint: disable=redefined-outer-name
    # XXX Note that this redefines `terminate`:
    #     Outside of this scope that is a function.
    # supports list argument since 3.1
    task_ids, task_id = set(maybe_list(task_id) or []), None
    task_ids = _revoke(state, task_ids, terminate, signal, **kwargs)
    if isinstance(task_ids, dict) and 'ok' in task_ids:
        return task_ids
    return ok(f'tasks {task_ids} flagged as revoked')


@control_command(
    variadic='headers',
    signature='[key1=value1 [key2=value2 [... [keyN=valueN]]]]',
)
def revoke_by_stamped_headers(state, headers, terminate=False, signal=None, **kwargs):
    """Revoke task by header (or list of headers).

    Keyword Arguments:
        headers(dictionary): Dictionary that contains stamping scheme name as keys and stamps as values.
                             If headers is a list, it will be converted to a dictionary.
        terminate (bool): Also terminate the process if the task is active.
        signal (str): Name of signal to use for terminate (e.g., ``KILL``).
    Sample headers input:
        {'mtask_id': [id1, id2, id3]}
    """
    pass


def _revoke(state, task_ids, terminate=False, signal=None, **kwargs):
    size = len(task_ids)
    terminated = set()

    worker_state.revoked.update(task_ids)

    for task_id in task_ids:
        try:
            state.app.backend.mark_as_revoked(task_id, reason='revoked', store_result=True)
        except Exception as exc:
            logger.warning('Failed to mark task %s as revoked in backend: %s', task_id, exc)

    if terminate:
        signum = _signals.signum(signal or TERM_SIGNAME)
        for request in _find_requests_by_id(task_ids):
            if request.id not in terminated:
                terminated.add(request.id)
                logger.info('Terminating %s (%s)', request.id, signum)
                request.terminate(state.consumer.pool, signal=signum)
                if len(terminated) >= size:
                    break

        if not terminated:
            return ok('terminate: tasks unknown')
        return ok('terminate: {}'.format(', '.join(terminated)))

    idstr = ', '.join(task_ids)
    logger.info('Tasks flagged as revoked: %s', idstr)
    return task_ids


@control_command(
    variadic='task_id',
    args=[('signal', str)],
    signature='<signal> [id1 [id2 [... [idN]]]]'
)
def terminate(state, signal, task_id, **kwargs):
    """Terminate task by task id (or list of ids)."""
    return revoke(state, task_id, terminate=True, signal=signal)


@control_command(
    args=[('task_name', str), ('rate_limit', str)],
    signature='<task_name> <rate_limit (e.g., 5/s | 5/m | 5/h)>',
)
def rate_limit(state, task_name, rate_limit, **kwargs):
    """Tell worker(s) to modify the rate limit for a task by type.

    See Also:
        :attr:`celery.app.task.Task.rate_limit`.

    Arguments:
        task_name (str): Type of task to set rate limit for.
        rate_limit (int, str): New rate limit.
    """
    pass


@control_command(
    args=[('task_name', str), ('soft', float), ('hard', float)],
    signature='<task_name> <soft_secs> [hard_secs]',
)
def time_limit(state, task_name=None, hard=None, soft=None, **kwargs):
    """Tell worker(s) to modify the time limit for task by type.

    Arguments:
        task_name (str): Name of task to change.
        hard (float): Hard time limit.
        soft (float): Soft time limit.
    """
    pass


# -- Events


@inspect_command()
def clock(state, **kwargs):
    """Get current logical clock value."""
    pass


@control_command()
def election(state, id, topic, action=None, **kwargs):
    """Hold election.

    Arguments:
        id (str): Unique election id.
        topic (str): Election topic.
        action (str): Action to take for elected actor.
    """
    pass


@control_command()
def enable_events(state):
    """Tell worker(s) to send task-related events."""
    pass


@control_command()
def disable_events(state):
    """Tell worker(s) to stop sending task-related events."""
    pass


@control_command()
def heartbeat(state):
    """Tell worker(s) to send event heartbeat immediately."""
    logger.debug('Heartbeat requested by remote.')
    dispatcher = state.consumer.event_dispatcher
    dispatcher.send('worker-heartbeat', freq=5, **worker_state.SOFTWARE_INFO)


# -- Worker

@inspect_command(visible=False)
def hello(state, from_node, revoked=None, **kwargs):
    """Request mingle sync-data."""
    # pylint: disable=redefined-outer-name
    # XXX Note that this redefines `revoked`:
    #     Outside of this scope that is a function.
    if from_node != state.hostname:
        logger.info('sync with %s', from_node)
        if revoked:
            worker_state.revoked.update(revoked)
        # Do not send expired items to the other worker.
        worker_state.revoked.purge()
        return {
            'revoked': worker_state.revoked._data,
            'clock': state.app.clock.forward(),
        }


@inspect_command(default_timeout=0.2)
def ping(state, **kwargs):
    """Ping worker(s)."""
    return ok('pong')


@inspect_command()
def stats(state, **kwargs):
    """Request worker statistics/information."""
    pass


@inspect_command(alias='dump_schedule')
def scheduled(state, **kwargs):
    """List of currently scheduled ETA/countdown tasks."""
    pass


def _iter_schedule_requests(timer):
    pass


@inspect_command(alias='dump_reserved')
def reserved(state, **kwargs):
    """List of currently reserved tasks, not including scheduled/active."""
    pass


@inspect_command(alias='dump_active')
def active(state, safe=False, **kwargs):
    """List of tasks currently being executed."""
    pass


@inspect_command(alias='dump_revoked')
def revoked(state, **kwargs):
    """List of revoked task-ids."""
    return list(worker_state.revoked)


@inspect_command(
    alias='dump_tasks',
    variadic='taskinfoitems',
    signature='[attr1 [attr2 [... [attrN]]]]',
)
def registered(state, taskinfoitems=None, builtins=False, **kwargs):
    """List of registered tasks.

    Arguments:
        taskinfoitems (Sequence[str]): List of task attributes to include.
            Defaults to ``exchange,routing_key,rate_limit``.
        builtins (bool): Also include built-in tasks.
    """
    pass


# -- Debugging

@inspect_command(
    default_timeout=60.0,
    args=[('type', str), ('num', int), ('max_depth', int)],
    signature='[object_type=Request] [num=200 [max_depth=10]]',
)
def objgraph(state, num=200, max_depth=10, type='Request'):  # pragma: no cover
    """Create graph of uncollected objects (memory-leak debugging).

    Arguments:
        num (int): Max number of objects to graph.
        max_depth (int): Traverse at most n levels deep.
        type (str): Name of object to graph.  Default is ``"Request"``.
    """
    pass


@inspect_command()
def memsample(state, **kwargs):
    """Sample current RSS memory usage."""
    pass


@inspect_command(
    args=[('samples', int)],
    signature='[n_samples=10]',
)
def memdump(state, samples=10, **kwargs):  # pragma: no cover
    """Dump statistics of previous memsample requests."""
    pass

# -- Pool


@control_command(
    args=[('n', int)],
    signature='[N=1]',
)
def pool_grow(state, n=1, **kwargs):
    """Grow pool by n processes/threads."""
    pass


@control_command(
    args=[('n', int)],
    signature='[N=1]',
)
def pool_shrink(state, n=1, **kwargs):
    """Shrink pool by n processes/threads."""
    pass


@control_command()
def pool_restart(state, modules=None, reload=False, reloader=None, **kwargs):
    """Restart execution pool."""
    pass


@control_command(
    args=[('max', int), ('min', int)],
    signature='[max [min]]',
)
def autoscale(state, max=None, min=None):
    """Modify autoscale settings."""
    autoscaler = state.consumer.controller.autoscaler
    if autoscaler:
        max_, min_ = autoscaler.update(max, min)
        return ok(f'autoscale now max={max_} min={min_}')
    raise ValueError('Autoscale not enabled')


@control_command()
def shutdown(state, msg='Got shutdown from remote', **kwargs):
    """Shutdown worker(s)."""
    logger.warning(msg)
    raise WorkerShutdown(EX_OK)


# -- Queues

@control_command(
    args=[
        ('queue', str),
        ('exchange', str),
        ('exchange_type', str),
        ('routing_key', str),
    ],
    signature='<queue> [exchange [type [routing_key]]]',
)
def add_consumer(state, queue, exchange=None, exchange_type=None,
                 routing_key=None, **options):
    """Tell worker(s) to consume from task queue by name."""
    pass


@control_command(
    args=[('queue', str)],
    signature='<queue>',
)
def cancel_consumer(state, queue, **_):
    """Tell worker(s) to stop consuming from task queue by name."""
    pass


@inspect_command()
def active_queues(state):
    """List the task queues a worker is currently consuming from."""
    pass
