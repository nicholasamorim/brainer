import umsgpack
from twisted.python import log

from subscriber import Subscriber
from lib.cache import InMemoryCache


class Replica(object):
    def __init__(self, host, tag,
                 cache_class=InMemoryCache, serializer_class=umsgpack):
        self._cache = cache_class()
        self._serializer = serializer_class
        self._subscriber = Subscriber.create(host)
        self._subscriber.subscribe(tag, self.message_received)

        log.msg('Replica Subscriber started!!!')

    def message_received(self, message, tag):
        log.msg(message)
