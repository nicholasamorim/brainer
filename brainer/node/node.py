# -*- coding: utf8 -*-

import sys
import uuid
import umsgpack

from twisted.python import log
from twisted.internet import reactor

from txzmq import ZmqREQConnection, ZmqREPConnection, ZmqEndpoint, ZmqFactory

from lib.mixins import SerializerMixin

from replica import Replica
from lib.cache import InMemoryCache


class BrokerClient(ZmqREQConnection, SerializerMixin):
    """
    """
    def __init__(self, *args, **kwargs):
        """
        """
        self._debug = kwargs.pop('debug', False)
        self._serializer = kwargs.pop('serializer', umsgpack)

        super(BrokerClient, self).__init__(*args, **kwargs)

    def sendMsg(self, message):
        """
        """
        d = super(BrokerClient, self).sendMsg(
            self.pack(message))
        d.addCallback(self.gotMessage, message)
        return d

    def register(self, node_id, address):
        """
        """
        self.id = node_id
        message = {'action': 'register', "id": self.id, "address": address}
        if self._debug:
            log.msg('Sending register: {}'.format(message))
        d = self.sendMsg(message)
        return d

    def register_reply(self, message, request):
        """Deals with the reply for the register packet.
        """
        self._node = message['node']
        return self._node

    def unregister(self):
        """
        """
        message = {
            "action": "unregister", "id": self.id, "node": self._node}
        if self._debug:
            log.msg('Sending unregister: {}'.format(message))

        d = self.sendMsg(message)
        return d

    def gotMessage(self, reply, request):
        """Called when any reply arrives after a request. Routes
        it to correct method. Example: if we receive a reply with
        'register' action, we route it to a method called register_reply.

        :param reply: Non-unpacked reply from server.
        :param request: The original request message.
        """
        reply = self.unpack(reply[0])
        if self._debug:
            log.msg("New Reply: {} ... For request: {}".format(reply, request))

        action = reply['action']
        method = getattr(self, '{}_reply'.format(action), None)
        if method is None:
            log.err('Reply method {} not implemented'.format(action))
            return

        return method(reply, request)

    @classmethod
    def create(cls, host, port=None, debug=False):
        """Factory that returns a BrokerClient class.

        :param host: A host.
        :param debug: If True, will log debug messages.
        """
        factory = ZmqFactory()
        endpoint = ZmqEndpoint('connect', host)
        return cls(factory, endpoint, debug=debug)


class Node(ZmqREPConnection, SerializerMixin):
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
        self._serializer = kwargs.get('serializer', umsgpack)
        self._address = endpoint.address
        self._debug = kwargs.get('debug', False)
        self._cache_class = kwargs.get('cache_class', InMemoryCache)
        self._cache = self._cache_class()

        self._client_class = kwargs.get('client_class', BrokerClient)
        self._is_registered = False

        super(Node, self).__init__(factory, endpoint)

    @classmethod
    def create(cls, address, **kwargs):
        """Factory method to create a Node.

        :param address: The address to bind the Node.
        """
        factory = ZmqFactory()
        endpoint = ZmqEndpoint('bind', address)
        return cls(factory, endpoint, **kwargs)

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
        print 'message on node server'
        print message

    def register(self):
        """
        """
        self._broker = self._client_class.create(
            'ipc:///tmp/broker.sock', debug=self._debug)
        d = self._broker.register(self.id, self._address)
        d.addCallback(self._connected)
        return d

    def _connected(self, node_id):
        """
        """
        if self._debug:
            log.msg('Node connected! Assigned number {}'.format(node_id))
        self._node_id = node_id
        self._is_registered = True

    def set(self, key, value):
        """
        """
        return self._cache.set(key, value)

    def get(self, key):
        """
        """
        return self._cache.get(key)

    def remove(self, key):
        """
        """
        return self._cache.remove(key)

    def unregister(self):
        """
        """
        if not self._is_registered:
            return

        return self._broker.unregister()


def run_node(replica=True):
    log.startLogging(sys.stdout)
    node = Node.create('ipc:///tmp/node1.sock', debug=True)
    if replica:
        replica = Replica('ipc:///tmp/publisher.sock', "")
    reactor.callLater(1.5, node.register)
    reactor.run()

if __name__ == '__main__':
    run_node()
