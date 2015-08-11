# -*- coding: utf8 -*-

import sys
import uuid
import umsgpack

from twisted.python import log
from twisted.internet import reactor

from txzmq import ZmqREQConnection, ZmqSubConnection, ZmqEndpoint, ZmqFactory

from lib.mixins import SerializerMixin

from cache import InMemoryCache


class Subscriber(ZmqSubConnection, SerializerMixin):
    pass


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
            self._pack(message))
        d.addCallback(self.gotMessage, message)
        return d

    def register(self, node_id):
        """
        """
        self.id = node_id
        message = {'action': 'register', "id": self.id}
        if self._debug:
            log.msg('Sending register: {}'.format(message))
        d = self.sendMsg(message)
        return d

    # def replicate(self, key, value):
    #     message = {
    #         "action": "replicate",
    #         "id": self.id,
    #         "node": self._node,
    #         "key": key,
    #         "value": value}

    #     d = self.sendMsg(message)
    #     return d

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
        reply = self._unpack(reply[0])
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


class Node(object):
    """This is a Node.
    """
    def __init__(self, **kwargs):
        """Creates a node.

        :param debug: If True, will log debug information.
        :param cache_class: Defaults to `cache.InMemoryCache`.
        :param client_class: Defaults to `BrokerClient`.
        """
        self._id = None
        self._debug = kwargs.get('debug', False)
        self._cache_class = kwargs.get('cache_class', InMemoryCache)
        self._cache = self._cache_class()

        self._client_class = kwargs.get('client_class', BrokerClient)
        self._is_registered = False

    @property
    def id(self):
        """
        """
        if self._id is None:
            self._id = str(uuid.uuid4())

        return self._id

    def register(self):
        self._broker = self._client_class.create(
            'ipc:///tmp/brokersock', debug=self._debug)
        return self._broker.register(self.id).addCallback(self._connected)

    def _connected(self, node_id):
        if self._debug:
            log.msg('Node connected! Assigned number {}'.format(node_id))
        self._node_id = node_id
        self._is_registered = True

    def set(self, key, value):
        return self._cache.set(key, value)

    def get(self, key):
        return self._cache.get(key)

    def remove(self, key):
        return self._cache.remove(key)

    def unregister(self):
        if not self._is_registered:
            return

        return self._broker.unregister()


def whatevs():
    x = Node(debug=True)
    reactor.callLater(0.5, x.register)
    reactor.callLater(1.0, x.unregister)


def run_node():
    log.startLogging(sys.stdout)
    reactor.callLater(0.1, whatevs)
    reactor.run()

if __name__ == '__main__':
    run_node()
