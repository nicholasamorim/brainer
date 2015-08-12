# -*- coding: utf8 -*-

import umsgpack
from twisted.python import log

from subscriber import Subscriber
from lib.cache import InMemoryCache


class Replica(object):
    """
    """
    def __init__(self, host, tag,
                 cache_class=InMemoryCache, serializer_class=umsgpack):
        """
        :param host: The Publisher endpoint.
        :param tag: A specifig tag to listen.
        :param cache_class: A Caching class that implements
        get, set and remove.
        :param serializer_class: A Serializer class that implements
        pack and unpack.
        """
        self._cache = cache_class()
        self._serializer = serializer_class
        self._subscriber = Subscriber.create(host)
        self._subscriber.subscribe(tag, self.message_received)

        log.msg('Replica Subscriber started!!!')
        self._allowed_actions = ('set', 'get', 'remove')

    def message_received(self, message, tag):
        """
        :param message: The message received.
        :param tag: A tag to potentially filter out messages.
        """
        action, key = message['action'], message['key']
        if action not in self._allowed_actions:
            return

        args = [key]
        if action == 'set':
            value = message['value']
            args.append(value)

        method = getattr(self._cache, action)
        method(*args)

    def on_shutdown(self):
        """This could potentially do any set of action, like save to disk.
        """