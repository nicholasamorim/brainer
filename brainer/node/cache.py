# -*- coding: utf8 -*-
from datetime import datetime, timedelta


class BaseCache(object):
    def set(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError


class InMemoryCache(BaseCache):
    """
    """
    def __init__(self):
        self._cache = {}
        self._expiration = {}

    @staticmethod
    def calculate_expiration(seconds):
        """Gets now and adds seconds to record expiration timestamp.
        """
        return datetime.now() + timedelta(seconds=seconds)

    def set(self, key, value, expires=None):
        """
        :param expires: Key expiration in seconds.
        """
        self._cache[key] = value
        if expires:
            self._expiration[key] = self.calculate_expiration(expires)

    def remove(self, key, expires=False):
        del self._cache[key]
        if expires:
            del self._expiration[key]

    def get(self, key):
        """Gets a key if available and if it has expiration,
        only if it hasn't been expired.

        :param key: A key string.
        """
        if key in self._expiration:
            if self._expiration[key] < datetime.now():
                self.remove(key, expires=True)
                return

        return self._cache.get(key, None)
