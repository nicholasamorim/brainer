# -*- coding: utf8 -*-

import umsgpack
from txzmq import ZmqSubConnection, ZmqEndpoint, ZmqFactory

from lib.mixins import SerializerMixin


class Subscriber(ZmqSubConnection, SerializerMixin):
    def __init__(self, factory, endpoint, serializer):
        """
        :param factory: A `txzmq.ZmqFactory` object.
        :param endpoint: A `txzmq.ZmqEndpoint` object.
        :param serializer: A serializer class. Defaults to umsgpack.
        """
        self._serializer = serializer
        super(Subscriber, self).__init__(factory, endpoint)

    @classmethod
    def create(cls, address, debug=False, **kwargs):
        """Factory that returns a BrokerClient class.

        :param address: A host address to connect.
        :param debug: If True, will log debug messages.
        """
        factory = ZmqFactory()
        endpoint = ZmqEndpoint('connect', address)
        serializer = kwargs.pop('serializer', umsgpack)
        return cls(factory, endpoint, serializer=serializer)

    def subscribe(self, tag, d):
        """
        """
        self._user_callback = d
        super(Subscriber, self).subscribe(tag)

    def gotMessage(self, message, tag):
        """
        """
        return self._user_callback(self.unpack(message), tag)
