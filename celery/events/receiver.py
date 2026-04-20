"""Event receiver implementation."""
import time
from operator import itemgetter

from kombu import Queue
from kombu.connection import maybe_channel
from kombu.mixins import ConsumerMixin

from celery import uuid
from celery.app import app_or_default
from celery.exceptions import ImproperlyConfigured
from celery.utils.time import adjust_timestamp

from .event import get_exchange

__all__ = ('EventReceiver',)

CLIENT_CLOCK_SKEW = -1

_TZGETTER = itemgetter('utcoffset', 'timestamp')


class EventReceiver(ConsumerMixin):
    """Capture events.

    Arguments:
        connection (kombu.Connection): Connection to the broker.
        handlers (Mapping[Callable]): Event handlers.
            This is  a map of event type names and their handlers.
            The special handler `"*"` captures all events that don't have a
            handler.
    """

    app = None

    def __init__(self, channel, handlers=None, routing_key='#',
                 node_id=None, app=None, queue_prefix=None,
                 accept=None, queue_ttl=None, queue_expires=None,
                 queue_exclusive=None,
                 queue_durable=None):
        self.app = app_or_default(app or self.app)
        self.channel = maybe_channel(channel)
        self.handlers = {} if handlers is None else handlers
        self.routing_key = routing_key
        self.node_id = node_id or uuid()
        self.queue_prefix = queue_prefix or self.app.conf.event_queue_prefix
        self.exchange = get_exchange(
            self.connection or self.app.connection_for_write(),
            name=self.app.conf.event_exchange)
        if queue_ttl is None:
            queue_ttl = self.app.conf.event_queue_ttl
        if queue_expires is None:
            queue_expires = self.app.conf.event_queue_expires
        if queue_exclusive is None:
            queue_exclusive = self.app.conf.event_queue_exclusive
        if queue_durable is None:
            queue_durable = self.app.conf.event_queue_durable
        if queue_exclusive and queue_durable:
            raise ImproperlyConfigured(
                'Queue cannot be both exclusive and durable, '
                'choose one or the other.'
            )
        self.queue = Queue(
            '.'.join([self.queue_prefix, self.node_id]),
            exchange=self.exchange,
            routing_key=self.routing_key,
            auto_delete=not queue_durable,
            durable=queue_durable,
            exclusive=queue_exclusive,
            message_ttl=queue_ttl,
            expires=queue_expires,
        )
        self.clock = self.app.clock
        self.adjust_clock = self.clock.adjust
        self.forward_clock = self.clock.forward
        if accept is None:
            accept = {self.app.conf.event_serializer, 'json'}
        self.accept = accept

    def process(self, type, event):
        """Process event by dispatching to configured handler."""
        pass

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=[self.queue],
                         callbacks=[self._receive], no_ack=True,
                         accept=self.accept)]

    def on_consume_ready(self, connection, channel, consumers,
                         wakeup=True, **kwargs):
        pass

    def itercapture(self, limit=None, timeout=None, wakeup=True):
        pass

    def capture(self, limit=None, timeout=None, wakeup=True):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never stop
        unless :attr:`EventDispatcher.should_stop` is set to True, or
        forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.
        """
        for _ in self.consume(limit=limit, timeout=timeout, wakeup=wakeup):
            pass

    def wakeup_workers(self, channel=None):
        pass

    def event_from_message(self, body, localize=True,
                           now=time.time, tzfields=_TZGETTER,
                           adjust_timestamp=adjust_timestamp,
                           CLIENT_CLOCK_SKEW=CLIENT_CLOCK_SKEW):
        pass

    def _receive(self, body, message, list=list, isinstance=isinstance):
        pass

    @property
    def connection(self):
        return self.channel.connection.client if self.channel else None
