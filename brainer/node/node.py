# -*- coding: utf8 -*-
import sys
import uuid

import umsgpack
from twisted.python import log
from twisted.internet import reactor

from brainer.lib.mixins import SerializerMixin
from brainer.lib.base import BaseREP
from brainer.lib.cache import InMemoryCache
from brainer.broker.client import BrokerClient


class Node(BaseREP, SerializerMixin):
    """This is a Node.
    """
    def __init__(self, factory, endpoint, **kwargs):
        """Creates a node.

        :param factory: A `txzmq.ZmqFactory` object.
        :param endpoint: A `txzmq.ZmqEndpoint` object.
        :param debug: If True, will log debug information.
        :param cache_class: Defaults to `cache.InMemoryCache`.
        :param client_class: Defaults to `BrokerClient`.
        """
        self._init_instance(endpoint.address, **kwargs)
        super(Node, self).__init__(factory, endpoint)

    def _init_instance(self, address, **kwargs):
        self._id = None
        self._address = address
        self._is_registered = False
        self._broker_address = kwargs['broker']
        self._serializer = kwargs.get('serializer', umsgpack)
        self._debug = kwargs.get('debug', False)
        self._cache_class = kwargs.get('cache_class', InMemoryCache)
        self._cache = self._cache_class()
        self._client_class = kwargs.get('client_class', BrokerClient)
        self._allowed_actions = ('get', 'set', 'remove', 'ping', 'snapshot')

    @property
    def id(self):
        """Returns an unique id for the server.

        :returns: A string representation of an uuid4 id.
        """
        if self._id is None:
            self._id = str(uuid.uuid4())

        return self._id

    def gotMessage(self, message_id, *messageParts):
        """Called when any reply arrives after a request. Routes
        it to correct method. Example: if we receive a reply with
        'register' action, we route it to a method called register_reply.

        :param reply: Non-unpacked reply from server.
        :param request: The original request message.
        """
        message = self.unpack(messageParts[0])
        if self._debug:
            log.msg('Message for Node: {}'.format(message))

        action = message['action']
        if action not in self._allowed_actions:
            self.reply_error("FORBBIDEN")

        method = getattr(self, action)
        reply = method(message)

        if self._debug:
            log.msg('Current Cache State: {}'.format(self._cache))

        self.reply(message_id, reply)

    def snapshot(self, message):
        """Returns a snapshot of the cache.

        :param message: The message itself.
        """
        return self._cache.snapshot()

    def ping(self):
        """When Broker asks for a confirmation we are alive.
        """
        return True

    def register(self):
        """Register a node with the broker.
        """
        self._broker = self._client_class.create(
            self._broker_address, debug=self._debug)
        d = self._broker.register(self.id, self._address)
        d.addCallback(self._connected)
        return d

    def _connected(self, message):
        """Called after the Broker replies that we registered
        successfully.

        :param message: The message reply.
        """
        log.msg('Node connected! Node ID {}'.format(self.id))
        self._is_registered = True

        snapshot = message.get('snapshot', None)
        if snapshot is not None:
            self._cache.replay(snapshot)

    def set(self, message):
        """Sets a key-value pair in the cache.

        :param message: The message itself.
        """
        return self._cache.set(message['key'], message['value'])

    def get(self, message):
        """Gets a value (if any) in the cache based on the key.

        :param message: The message itself.
        :returns: A value or None if there isn't any.
        """
        return self._cache.get(message['key'])

    def remove(self, message):
        """Removes a key from the cache.

        :param message: The Message itself.
        """
        return self._cache.remove(message['key'])

    def unregister(self):
        """Unregisters a node with a broker.
        """
        log.msg('Unregistering with the Broker...')
        if not self._is_registered:
            log.msg('Never registered! Shutting down...')
            return

        return self._broker.unregister()

    def on_shutdown(self):
        """Called when the reactor captures the shutdown signal. It calls
        the broker to cleanly unregister the node.
        """
        log.msg('We are going to shut down NOW!')
        self.unregister()


def run_node(host, broker, debug=False):
    log.startLogging(sys.stdout)
    node = Node.create(host, broker=broker, debug=debug)
    reactor.callLater(0.1, node.register)
    reactor.addSystemEventTrigger('before', 'shutdown', node.on_shutdown)
    reactor.run()

if __name__ == '__main__':
    run_node()
