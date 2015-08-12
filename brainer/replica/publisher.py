# -*- coding: utf8 -*-
import umsgpack
from twisted.python import log

from txzmq import ZmqPubConnection, ZmqEndpoint, ZmqFactory

from lib.mixins import SerializerMixin


class Publisher(ZmqPubConnection, SerializerMixin):
    """Publishes information to all nodes connected.
    By giving out the information in a fan-out fashion,
    all nodes can replicate it, while the responsible node
    will actually store it in its cache.
    """
    def __init__(self, *args, **kwargs):
        self._debug = kwargs.pop('debug', False)
        self._serializer = kwargs.pop('serializer', umsgpack)

        super(Publisher, self).__init__(*args, **kwargs)

        log.msg('Publisher started!!!')

    def publish(self, message, tag=b''):
        """
        """
        if self._debug:
            log.msg('Publishing message: {}'.format(message))
        return super(Publisher, self).publish(
            self.pack(message))

    @classmethod
    def create(cls, host, debug=False):
        """
        """
        factory = ZmqFactory()
        endpoint = ZmqEndpoint('bind', host)
        return cls(factory, endpoint, debug=debug)
