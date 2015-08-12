# -*- coding: utf8 -*-
import umsgpack
from twisted.python import log

from txzmq import ZmqEndpoint, ZmqFactory, ZmqREQConnection

from lib.mixins import SerializerMixin


class NodeClient(ZmqREQConnection, SerializerMixin):
    def __init__(self, factory, endpoint, *args, **kwargs):
        """A Node Client.

        :param factory: A `txzmq.ZmqFactory` object.
        :param endpoint: A `txzmq.ZmqEndpoint` object.
        """
        self._serializer = kwargs.pop('serializer', umsgpack)
        self._timeout = kwargs.pop('timeout', 5)
        super(NodeClient, self).__init__(factory, endpoint)

    @classmethod
    def create(cls, address):
        factory = ZmqFactory()
        endpoint = ZmqEndpoint('connect', address)
        return cls(factory, endpoint)

    def _on_error(self, f):
        """Log a failure and re-raise it.

        :param f: A `twisted.python.failure.Failure` object.
        """
        log.err(
            f,
            'Something happened speaking with node {} (timeout: {}).'.format(
                self.node_number, self._timeout))
        raise f

    def sendMsg(self, message):
        """Sends a message.

        :param message: Message to be sent.
        """
        d = super(NodeClient, self).sendMsg(
            self.pack(message), timeout=self._timeout)
        d.addCallback(lambda reply: self.unpack(reply[0]))
        d.addErrback(self._on_error)
        return d

    def get(self, message):
        return self.sendMsg(message)

    def set(self, message):
        return self.sendMsg(message)

    def remove(self, message):
        return self.sendMsg(message)
        
