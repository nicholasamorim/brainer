# -*- coding: utf8 -*-
import sys
import uuid

import umsgpack
from twisted.python import log
from twisted.internet import reactor

from lib.mixins import SerializerMixin
from lib.base import BaseREP
from lib.cache import InMemoryCache
from broker.client import BrokerClient


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
        self._id = None
        self._broker_address = kwargs['broker']
        self._serializer = kwargs.get('serializer', umsgpack)
        self._address = endpoint.address
        self._debug = kwargs.get('debug', False)
        self._cache_class = kwargs.get('cache_class', InMemoryCache)
        self._cache = self._cache_class()

        self._client_class = kwargs.get('client_class', BrokerClient)
        self._is_registered = False

        self._allowed_actions = ('get', 'set', 'remove', 'ping')

        super(Node, self).__init__(factory, endpoint)

    @property
    def id(self):
        """
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

    def ping(self):
        """When Broker asks for a confirmation we are alive.
        """
        return True

    def register(self):
        """
        """
        self._broker = self._client_class.create(
            self._broker_address, debug=self._debug)
        d = self._broker.register(self.id, self._address)
        d.addCallback(self._connected)
        return d

    def _connected(self, node_id):
        """
        """
        log.msg('Node connected! Node ID {}'.format(self.id))
        self._is_registered = True

    def set(self, message):
        """
        """
        return self._cache.set(message['key'], message['value'])

    def get(self, message):
        """
        """
        return self._cache.get(message['key'])

    def remove(self, message):
        """
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
