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
        log.msg('Publisher started!!!')
        self._debug = kwargs.pop('debug', False)

    def publish(self, message, tag=b''):
        if self._debug:
            log.msg('Publishing message: {}'.format(message))
        return super(Publisher, self).publish(
            self._pack(message))

    @classmethod
    def create(cls, host, port=None):
        factory = ZmqFactory()
        endpoint = ZmqEndpoint('bind', host)
        return cls(factory, endpoint)
