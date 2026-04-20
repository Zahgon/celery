"""Version of multiprocessing.Pool using Async I/O.

.. note::

    This module will be moved soon, so don't use it directly.

This is a non-blocking version of :class:`multiprocessing.Pool`.

This code deals with three major challenges:

#. Starting up child processes and keeping them running.
#. Sending jobs to the processes and receiving results back.
#. Safely shutting down this system.
"""
import errno
import gc
import inspect
import os
import select
import time
from collections import Counter, deque, namedtuple
from io import BytesIO
from numbers import Integral
from pickle import HIGHEST_PROTOCOL
from struct import pack, unpack, unpack_from
from time import sleep
from weakref import WeakValueDictionary, ref

from billiard import pool as _pool
from billiard.compat import isblocking, setblocking
from billiard.pool import ACK, NACK, RUN, TERMINATE, WorkersJoined
from billiard.queues import _SimpleQueue
from kombu.asynchronous import ERR, WRITE
from kombu.serialization import pickle as _pickle
from kombu.utils.eventio import SELECT_BAD_FD
from kombu.utils.functional import fxrange
from vine import promise

from celery.signals import worker_before_create_process
from celery.utils.functional import noop
from celery.utils.log import get_logger
from celery.worker import state as worker_state

# pylint: disable=redefined-outer-name
# We cache globals and attribute lookups, so disable this warning.

try:
    from _billiard import read as __read__
    readcanbuf = True

except ImportError:

    def __read__(fd, buf, size, read=os.read):
        chunk = read(fd, size)
        n = len(chunk)
        if n != 0:
            buf.write(chunk)
        return n
    readcanbuf = False

    def unpack_from(fmt, iobuf, unpack=unpack):  # noqa
        return unpack(fmt, iobuf.getvalue())  # <-- BytesIO

__all__ = ('AsynPool',)

logger = get_logger(__name__)
error, debug = logger.error, logger.debug

UNAVAIL = frozenset({errno.EAGAIN, errno.EINTR})

#: Constant sent by child process when started (ready to accept work)
WORKER_UP = 15

#: A process must've started before this timeout (in secs.) expires.
PROC_ALIVE_TIMEOUT = 4.0

SCHED_STRATEGY_FCFS = 1
SCHED_STRATEGY_FAIR = 4

SCHED_STRATEGIES = {
    None: SCHED_STRATEGY_FAIR,
    'default': SCHED_STRATEGY_FAIR,
    'fast': SCHED_STRATEGY_FCFS,
    'fcfs': SCHED_STRATEGY_FCFS,
    'fair': SCHED_STRATEGY_FAIR,
}
SCHED_STRATEGY_TO_NAME = {v: k for k, v in SCHED_STRATEGIES.items()}

Ack = namedtuple('Ack', ('id', 'fd', 'payload'))


def gen_not_started(gen):
    """Return true if generator is not started."""
    return inspect.getgeneratorstate(gen) == "GEN_CREATED"


def _get_job_writer(job):
    try:
        writer = job._writer
    except AttributeError:
        pass
    else:
        return writer()  # is a weakref


def _ensure_integral_fd(fd):
    pass


if hasattr(select, 'poll'):
    def _select_imp(readers=None, writers=None, err=None, timeout=0,
                    poll=select.poll, POLLIN=select.POLLIN,
                    POLLOUT=select.POLLOUT, POLLERR=select.POLLERR):
        pass
else:
    def _select_imp(readers=None, writers=None, err=None, timeout=0):
        pass


def _select(readers=None, writers=None, err=None, timeout=0,
            poll=_select_imp):
    """Simple wrapper to :class:`~select.select`, using :`~select.poll`.

    Arguments:
        readers (Set[Fd]): Set of reader fds to test if readable.
        writers (Set[Fd]): Set of writer fds to test if writable.
        err (Set[Fd]): Set of fds to test for error condition.

    All fd sets passed must be mutable as this function
    will remove non-working fds from them, this also means
    the caller must make sure there are still fds in the sets
    before calling us again.

    Returns:
        Tuple[Set, Set, Set]: of ``(readable, writable, again)``, where
        ``readable`` is a set of fds that have data available for read,
        ``writable`` is a set of fds that's ready to be written to
        and ``again`` is a flag that if set means the caller must
        throw away the result and call us again.
    """
    readers = set() if readers is None else readers
    writers = set() if writers is None else writers
    err = set() if err is None else err
    try:
        return poll(readers, writers, err, timeout)
    except OSError as exc:
        _errno = exc.errno

        if _errno == errno.EINTR:
            return set(), set(), 1
        elif _errno in SELECT_BAD_FD:
            for fd in readers | writers | err:
                try:
                    select.select([fd], [], [], 0)
                except OSError as exc:
                    _errno = exc.errno

                    if _errno not in SELECT_BAD_FD:
                        raise
                    readers.discard(fd)
                    writers.discard(fd)
                    err.discard(fd)
            return set(), set(), 1
        else:
            raise


def iterate_file_descriptors_safely(fds_iter, source_data,
                                    hub_method, *args, **kwargs):
    """Apply hub method to fds in iter, remove from list if failure.

    Some file descriptors may become stale through OS reasons
    or possibly other reasons, so safely manage our lists of FDs.
    :param fds_iter: the file descriptors to iterate and apply hub_method
    :param source_data: data source to remove FD if it renders OSError
    :param hub_method: the method to call with each fd and kwargs
    :*args to pass through to the hub_method;
    with a special syntax string '*fd*' represents a substitution
    for the current fd object in the iteration (for some callers).
    :**kwargs to pass through to the hub method (no substitutions needed)
    """
    def _meta_fd_argument_maker():
        # uses the current iterations value for fd
        call_args = args
        if "*fd*" in call_args:
            call_args = [fd if arg == "*fd*" else arg for arg in args]
        return call_args
    # Track stale FDs for cleanup possibility
    stale_fds = []
    for fd in fds_iter:
        # Handle using the correct arguments to the hub method
        hub_args, hub_kwargs = _meta_fd_argument_maker(), kwargs
        try:  # Call the hub method
            hub_method(fd, *hub_args, **hub_kwargs)
        except (OSError, FileNotFoundError):
            logger.warning(
                "Encountered OSError when accessing fd %s ",
                fd, exc_info=True)
            stale_fds.append(fd)  # take note of stale fd
    # Remove now defunct fds from the managed list
    if source_data:
        for fd in stale_fds:
            try:
                if hasattr(source_data, 'remove'):
                    source_data.remove(fd)
                else:  # then not a list/set ... try dict
                    source_data.pop(fd, None)
            except ValueError:
                logger.warning("ValueError trying to invalidate %s from %s",
                               fd, source_data)


class Worker(_pool.Worker):
    """Pool worker process."""

    def on_loop_start(self, pid):
        # our version sends a WORKER_UP message when the process is ready
        # to accept work, this will tell the parent that the inqueue fd
        # is writable.
        pass


class ResultHandler(_pool.ResultHandler):
    """Handles messages from the pool processes."""

    def __init__(self, *args, **kwargs):
        self.fileno_to_outq = kwargs.pop('fileno_to_outq')
        self.on_process_alive = kwargs.pop('on_process_alive')
        super().__init__(*args, **kwargs)
        # add our custom message handler
        self.state_handlers[WORKER_UP] = self.on_process_alive

    def _recv_message(self, add_reader, fd, callback,
                      __read__=__read__, readcanbuf=readcanbuf,
                      BytesIO=BytesIO, unpack_from=unpack_from,
                      load=_pickle.load):
        pass

    def _make_process_result(self, hub):
        """Coroutine reading messages from the pool processes."""
        fileno_to_outq = self.fileno_to_outq
        on_state_change = self.on_state_change
        add_reader = hub.add_reader
        remove_reader = hub.remove_reader
        recv_message = self._recv_message

        def on_result_readable(fileno):
            pass
        return on_result_readable

    def register_with_event_loop(self, hub):
        self.handle_event = self._make_process_result(hub)

    def handle_event(self, *args):
        # pylint: disable=method-hidden
        #   register_with_event_loop overrides this
        raise RuntimeError('Not registered with event loop')

    def on_stop_not_started(self):
        # This is always used, since we do not start any threads.
        pass

    def _flush_outqueue(self, fd, remove, process_index, on_state_change):
        pass


class AsynPool(_pool.Pool):
    """AsyncIO Pool (no threads)."""

    ResultHandler = ResultHandler
    Worker = Worker

    #: Set by :meth:`register_with_event_loop` after running the first time.
    _registered_with_event_loop = False

    def WorkerProcess(self, worker):
        pass

    def __init__(self, processes=None, synack=False,
                 sched_strategy=None, proc_alive_timeout=None,
                 *args, **kwargs):
        self.sched_strategy = SCHED_STRATEGIES.get(sched_strategy,
                                                   sched_strategy)
        processes = self.cpu_count() if processes is None else processes
        self.synack = synack
        # create queue-pairs for all our processes in advance.
        self._queues = {
            self.create_process_queues(): None for _ in range(processes)
        }

        # inqueue fileno -> process mapping
        self._fileno_to_inq = {}
        # outqueue fileno -> process mapping
        self._fileno_to_outq = {}
        # synqueue fileno -> process mapping
        self._fileno_to_synq = {}

        # We keep track of processes that haven't yet
        # sent a WORKER_UP message.  If a process fails to send
        # this message within _proc_alive_timeout we terminate it
        # and hope the next process will recover.
        self._proc_alive_timeout = (
            PROC_ALIVE_TIMEOUT if proc_alive_timeout is None
            else proc_alive_timeout
        )
        self._waiting_to_start = set()

        # denormalized set of all inqueues.
        self._all_inqueues = set()

        # Set of fds being written to (busy)
        self._active_writes = set()

        # Set of active co-routines currently writing jobs.
        self._active_writers = set()

        # Set of fds that are busy (executing task)
        self._busy_workers = set()
        self._mark_worker_as_available = self._busy_workers.discard

        # Holds jobs waiting to be written to child processes.
        self.outbound_buffer = deque()

        self.write_stats = Counter()

        super().__init__(processes, *args, synack=synack, **kwargs)

        for proc in self._pool:
            # create initial mappings, these will be updated
            # as processes are recycled, or found lost elsewhere.
            self._fileno_to_outq[proc.outqR_fd] = proc
            self._fileno_to_synq[proc.synqW_fd] = proc

        self.on_soft_timeout = getattr(
            self._timeout_handler, 'on_soft_timeout', noop,
        )
        self.on_hard_timeout = getattr(
            self._timeout_handler, 'on_hard_timeout', noop,
        )

    def _create_worker_process(self, i):
        pass

    def _event_process_exit(self, hub, proc):
        # This method is called whenever the process sentinel is readable.
        pass

    def _track_child_process(self, proc, hub):
        """Helper method determines appropriate fd for process."""
        try:
            fd = proc._sentinel_poll
        except AttributeError:
            # we need to duplicate the fd here to carefully
            # control when the fd is removed from the process table,
            # as once the original fd is closed we cannot unregister
            # the fd from epoll(7) anymore, causing a 100% CPU poll loop.
            fd = proc._sentinel_poll = os.dup(proc._popen.sentinel)
        # Safely call hub.add_reader for the determined fd
        iterate_file_descriptors_safely(
            [fd], None, hub.add_reader,
            self._event_process_exit, hub, proc)

    def _untrack_child_process(self, proc, hub):
        sentinel_poll = getattr(proc, '_sentinel_poll', None)
        if sentinel_poll is not None:
            proc._sentinel_poll = None
            hub.remove(sentinel_poll)
            os.close(sentinel_poll)

    def register_with_event_loop(self, hub):
        """Register the async pool with the current event loop."""
        self._result_handler.register_with_event_loop(hub)
        self.handle_result_event = self._result_handler.handle_event
        self._create_timelimit_handlers(hub)
        self._create_process_handlers(hub)
        self._create_write_handlers(hub)

        # Add handler for when a process exits (calls maintain_pool)
        [self._track_child_process(w, hub) for w in self._pool]
        # Handle_result_event is called whenever one of the
        # result queues are readable.
        iterate_file_descriptors_safely(
            self._fileno_to_outq, self._fileno_to_outq, hub.add_reader,
            self.handle_result_event, '*fd*')

        # Timers include calling maintain_pool at a regular interval
        # to be certain processes are restarted.
        for handler, interval in self.timers.items():
            hub.call_repeatedly(interval, handler)

        # Add on_poll_start to the event loop only once to prevent duplication
        # when the Consumer restarts due to a connection error.
        if not self._registered_with_event_loop:
            hub.on_tick.add(self.on_poll_start)
            self._registered_with_event_loop = True

    def _create_timelimit_handlers(self, hub):
        """Create handlers used to implement time limits."""
        call_later = hub.call_later
        trefs = self._tref_for_id = WeakValueDictionary()

        def on_timeout_set(R, soft, hard):
            pass
        self.on_timeout_set = on_timeout_set

        def _discard_tref(job):
            try:
                tref = trefs.pop(job)
                tref.cancel()
                del tref
            except (KeyError, AttributeError):
                pass  # out of scope
        self._discard_tref = _discard_tref

        def on_timeout_cancel(R):
            pass
        self.on_timeout_cancel = on_timeout_cancel

    def _on_soft_timeout(self, job, soft, hard, hub):
        # only used by async pool.
        pass

    def _on_hard_timeout(self, job):
        # only used by async pool.
        pass

    def on_job_ready(self, job, i, obj, inqW_fd):
        pass

    def _create_process_handlers(self, hub):
        """Create handlers called on process up/down, etc."""
        add_reader, remove_reader, remove_writer = (
            hub.add_reader, hub.remove_reader, hub.remove_writer,
        )
        cache = self._cache
        all_inqueues = self._all_inqueues
        fileno_to_inq = self._fileno_to_inq
        fileno_to_outq = self._fileno_to_outq
        fileno_to_synq = self._fileno_to_synq
        busy_workers = self._busy_workers
        handle_result_event = self.handle_result_event
        process_flush_queues = self.process_flush_queues
        waiting_to_start = self._waiting_to_start

        def verify_process_alive(proc):
            pass

        def on_process_up(proc):
            """Called when a process has started."""
            pass

        self.on_process_up = on_process_up

        def _remove_from_index(obj, proc, index, remove_fun, callback=None):
            # this remove the file descriptors for a process from
            # the indices.  we have to make sure we don't overwrite
            # another processes fds, as the fds may be reused.
            try:
                fd = obj.fileno()
            except OSError:
                return

            try:
                if index[fd] is proc:
                    # fd hasn't been reused so we can remove it from index.
                    index.pop(fd, None)
            except KeyError:
                pass
            else:
                remove_fun(fd)
                if callback is not None:
                    callback(fd)
            return fd

        def on_process_down(proc):
            """Called when a worker process exits."""
            pass
        self.on_process_down = on_process_down

    def _create_write_handlers(self, hub,
                               pack=pack, dumps=_pickle.dumps,
                               protocol=HIGHEST_PROTOCOL):
        """Create handlers used to write data to child processes."""
        fileno_to_inq = self._fileno_to_inq
        fileno_to_synq = self._fileno_to_synq
        outbound = self.outbound_buffer
        pop_message = outbound.popleft
        put_message = outbound.append
        all_inqueues = self._all_inqueues
        active_writes = self._active_writes
        active_writers = self._active_writers
        busy_workers = self._busy_workers
        diff = all_inqueues.difference
        add_writer = hub.add_writer
        hub_add, hub_remove = hub.add, hub.remove
        mark_write_fd_as_active = active_writes.add
        mark_write_gen_as_active = active_writers.add
        mark_worker_as_busy = busy_workers.add
        write_generator_done = active_writers.discard
        get_job = self._cache.__getitem__
        write_stats = self.write_stats
        is_fair_strategy = self.sched_strategy == SCHED_STRATEGY_FAIR
        revoked_tasks = worker_state.revoked
        getpid = os.getpid

        precalc = {ACK: self._create_payload(ACK, (0,)),
                   NACK: self._create_payload(NACK, (0,))}

        def _put_back(job, _time=time.time):
            # puts back at the end of the queue
            if job._terminated is not None or \
                    job.correlation_id in revoked_tasks:
                if not job._accepted:
                    job._ack(None, _time(), getpid(), None)
                job._set_terminated(job._terminated)
            else:
                # XXX linear lookup, should find a better way,
                # but this happens rarely and is here to protect against races.
                if job not in outbound:
                    outbound.appendleft(job)
        self._put_back = _put_back

        # called for every event loop iteration, and if there
        # are messages pending this will schedule writing one message
        # by registering the 'schedule_writes' function for all currently
        # inactive inqueues (not already being written to)

        # consolidate means the event loop will merge them
        # and call the callback once with the list writable fds as
        # argument.  Using this means we minimize the risk of having
        # the same fd receive every task if the pipe read buffer is not
        # full.

        def on_poll_start():
            # Determine which io descriptors are not busy
            pass
        self.on_poll_start = on_poll_start

        def on_inqueue_close(fd, proc):
            # Makes sure the fd is removed from tracking when
            # the connection is closed, this is essential as fds may be reused.
            pass
        self.on_inqueue_close = on_inqueue_close
        self.hub_remove = hub_remove

        def schedule_writes(ready_fds, total_write_count=None):
            pass
        hub.consolidate_callback = schedule_writes

        def send_job(tup):
            # Schedule writing job request for when one of the process
            # inqueues are writable.
            pass
        self._quick_put = send_job

        def on_not_recovering(proc, fd, job, exc):
            logger.exception(
                'Process inqueue damaged: %r %r: %r', proc, proc.exitcode, exc)
            if proc._is_alive():
                proc.terminate()
            hub.remove(fd)
            self._put_back(job)

        def _write_job(proc, fd, job):
            # writes job to the worker process.
            # Operation must complete if more than one byte of data
            # was written.  If the broker connection is lost
            # and no data was written the operation shall be canceled.
            header, body, body_size = job._payload
            errors = 0
            try:
                # job result keeps track of what process the job is sent to.
                job._write_to = proc
                send = proc.send_job_offset

                Hw = Bw = 0
                # write header
                while Hw < 4:
                    try:
                        Hw += send(header, Hw)
                    except Exception as exc:  # pylint: disable=broad-except
                        if getattr(exc, 'errno', None) not in UNAVAIL:
                            raise
                        # suspend until more data
                        errors += 1
                        if errors > 100:
                            on_not_recovering(proc, fd, job, exc)
                            raise StopIteration()
                        yield
                    else:
                        errors = 0

                # write body
                while Bw < body_size:
                    try:
                        Bw += send(body, Bw)
                    except Exception as exc:  # pylint: disable=broad-except
                        if getattr(exc, 'errno', None) not in UNAVAIL:
                            raise
                        # suspend until more data
                        errors += 1
                        if errors > 100:
                            on_not_recovering(proc, fd, job, exc)
                            raise StopIteration()
                        yield
                    else:
                        errors = 0
            finally:
                hub.remove_writer(fd)
                write_stats[proc.index] += 1
                # message written, so this fd is now available
                active_writes.discard(fd)
                write_generator_done(job._writer())  # is a weakref

        def send_ack(response, pid, job, fd):
            # Only used when synack is enabled.
            # Schedule writing ack response for when the fd is writable.
            pass
        self.send_ack = send_ack

        def _write_ack(fd, ack, callback=None):
            # writes ack back to the worker if synack enabled.
            # this operation *MUST* complete, otherwise
            # the worker process will hang waiting for the ack.
            header, body, body_size = ack[2]
            try:
                try:
                    proc = fileno_to_synq[fd]
                except KeyError:
                    # process died, we can safely discard the ack at this
                    # point.
                    raise StopIteration()
                send = proc.send_syn_offset

                Hw = Bw = 0
                # write header
                while Hw < 4:
                    try:
                        Hw += send(header, Hw)
                    except Exception as exc:  # pylint: disable=broad-except
                        if getattr(exc, 'errno', None) not in UNAVAIL:
                            raise
                        yield

                # write body
                while Bw < body_size:
                    try:
                        Bw += send(body, Bw)
                    except Exception as exc:  # pylint: disable=broad-except
                        if getattr(exc, 'errno', None) not in UNAVAIL:
                            raise
                        # suspend until more data
                        yield
            finally:
                if callback:
                    callback()
                # message written, so this fd is now available
                active_writes.discard(fd)

    def flush(self):
        if self._state == TERMINATE:
            return
        # cancel all tasks that haven't been accepted so that NACK is sent
        # if synack is enabled, otherwise discard them from the cache
        # since they will be redelivered by the broker.
        for job in tuple(self._cache.values()):
            if not job._accepted:
                if self.synack:
                    job._cancel()
                else:
                    job.discard()

        # clear the outgoing buffer as the tasks will be redelivered by
        # the broker anyway.
        if self.outbound_buffer:
            self.outbound_buffer.clear()

        self.maintain_pool()

        try:
            # ...but we must continue writing the payloads we already started
            # to keep message boundaries.
            # The messages may be NACK'ed later if synack is enabled.
            if self._state == RUN:
                # flush outgoing buffers
                intervals = fxrange(0.01, 0.1, 0.01, repeatlast=True)

                # TODO: Rewrite this as a dictionary comprehension once we drop support for Python 3.7
                #       This dict comprehension requires the walrus operator which is only available in 3.8.
                owned_by = {}
                for job in self._cache.values():
                    writer = _get_job_writer(job)
                    if writer is not None:
                        owned_by[writer] = job

                while self._active_writers:
                    writers = list(self._active_writers)
                    for gen in writers:
                        if (gen.__name__ == '_write_job' and
                                gen_not_started(gen)):
                            # hasn't started writing the job so can
                            # discard the task, but we must also remove
                            # it from the Pool._cache.
                            try:
                                job = owned_by[gen]
                            except KeyError:
                                pass
                            else:
                                # removes from Pool._cache
                                job.discard()
                            self._active_writers.discard(gen)
                        else:
                            try:
                                job = owned_by[gen]
                            except KeyError:
                                # Generator not in owned_by — not a _write_job
                                # (e.g. a _write_ack coroutine added by send_ack()).
                                # These *MUST* complete or the worker process will
                                # hang waiting for the ack.  Advance it one step;
                                # the generator raises StopIteration/OSError when
                                # done or when the peer process has already died.
                                try:
                                    next(gen)
                                except (StopIteration, OSError, EOFError):
                                    self._active_writers.discard(gen)
                            else:
                                job_proc = job._write_to
                                if job_proc._is_alive():
                                    # _flush_writer calls
                                    # _active_writers.discard(gen) in its finally.
                                    self._flush_writer(job_proc, gen)
                                else:
                                    # Process is dead, job will never
                                    # complete - discard from cache.
                                    job.discard()
                                    self._active_writers.discard(gen)
                    # workers may have exited in the meantime.
                    self.maintain_pool()
                    sleep(next(intervals))  # don't busyloop
        finally:
            self.outbound_buffer.clear()
            self._active_writers.clear()
            self._active_writes.clear()
            self._busy_workers.clear()

    def _flush_writer(self, proc, writer):
        fds = {proc.inq._writer}
        try:
            while fds:
                if not proc._is_alive():
                    break  # process exited
                readable, writable, again = _select(
                    writers=fds, err=fds, timeout=0.5,
                )
                if not again and (writable or readable):
                    try:
                        next(writer)
                    except (StopIteration, OSError, EOFError):
                        break
        finally:
            self._active_writers.discard(writer)

    def get_process_queues(self):
        """Get queues for a new process.

        Here we'll find an unused slot, as there should always
        be one available when we start a new process.
        """
        pass

    def on_grow(self, n):
        """Grow the pool by ``n`` processes."""
        pass

    def on_shrink(self, n):
        """Shrink the pool by ``n`` processes."""

    def create_process_queues(self):
        """Create new in, out, etc. queues, returned as a tuple."""
        # NOTE: Pipes must be set O_NONBLOCK at creation time (the original
        # fd), otherwise it won't be possible to change the flags until
        # there's an actual reader/writer on the other side.
        inq = _SimpleQueue(wnonblock=True)
        outq = _SimpleQueue(rnonblock=True)
        synq = None
        assert isblocking(inq._reader)
        assert not isblocking(inq._writer)
        assert not isblocking(outq._reader)
        assert isblocking(outq._writer)
        if self.synack:
            synq = _SimpleQueue(wnonblock=True)
            assert isblocking(synq._reader)
            assert not isblocking(synq._writer)
        return inq, outq, synq

    def on_process_alive(self, pid):
        """Called when receiving the :const:`WORKER_UP` message.

        Marks the process as ready to receive work.
        """
        pass

    def on_job_process_down(self, job, pid_gone):
        """Called for each job when the process assigned to it exits."""
        pass

    def on_job_process_lost(self, job, pid, exitcode):
        """Called when the process executing job' exits.

        This happens when the process job'
        was assigned to exited by mysterious means (error exitcodes and
        signals).
        """
        pass

    def human_write_stats(self):
        pass

    def _process_cleanup_queues(self, proc):
        """Called to clean up queues after process exit."""
        pass

    @staticmethod
    def _stop_task_handler(task_handler):
        """Called at shutdown to tell processes that we're shutting down."""
        pass

    def create_result_handler(self):
        pass

    def _process_register_queues(self, proc, queues):
        """Mark new ownership for ``queues`` to update fileno indices."""
        pass

    def _find_worker_queues(self, proc):
        """Find the queues owned by ``proc``."""
        pass

    def _setup_queues(self):
        # this is only used by the original pool that used a shared
        # queue for all processes.
        pass

    def process_flush_queues(self, proc):
        """Flush all queues.

        Including the outbound buffer, so that
        all tasks that haven't been started will be discarded.

        In Celery this is called whenever the transport connection is lost
        (consumer restart), and when a process is terminated.
        """
        resq = proc.outq._reader
        on_state_change = self._result_handler.on_state_change
        fds = {resq}
        while fds and not resq.closed and self._state != TERMINATE:
            readable, _, _ = _select(fds, None, fds, timeout=0.01)
            if readable:
                try:
                    task = resq.recv()
                except (OSError, EOFError) as exc:
                    _errno = getattr(exc, 'errno', None)
                    if _errno == errno.EINTR:
                        continue
                    elif _errno == errno.EAGAIN:
                        break
                    elif _errno not in UNAVAIL:
                        debug('got %r while flushing process %r',
                              exc, proc, exc_info=1)
                    break
                else:
                    if task is None:
                        debug('got sentinel while flushing process %r', proc)
                        break
                    else:
                        on_state_change(task)
            else:
                break

    def on_partial_read(self, job, proc):
        """Called when a job was partially written to exited child."""
        pass

    def destroy_queues(self, queues, proc):
        """Destroy queues that can no longer be used.

        This way they can be replaced by new usable sockets.
        """
        pass

    def _create_payload(self, type_, args,
                        dumps=_pickle.dumps, pack=pack,
                        protocol=HIGHEST_PROTOCOL):
        body = dumps((type_, args), protocol=protocol)
        size = len(body)
        header = pack('>I', size)
        return header, body, size

    @classmethod
    def _set_result_sentinel(cls, _outqueue, _pool):
        # unused
        pass

    def _help_stuff_finish_args(self):
        # Pool._help_stuff_finished is a classmethod so we have to use this
        # trick to modify the arguments passed to it.
        pass

    @classmethod
    def _help_stuff_finish(cls, pool):
        # pylint: disable=arguments-differ
        pass

    @property
    def timers(self):
        pass
