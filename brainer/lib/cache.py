# -*- coding: utf8 -*-
from datetime import datetime, timedelta


class BaseCache(object):
    """A basic Cache must implement set, get and remove.

    The only implementation currently is in-memory. See `InMemoryCache`.
    """
    def set(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def remove(self, key):
        raise NotImplementedError


class InMemoryCache(BaseCache):
    """This is an InMemoryCache.
    """
    def __init__(self):
        self._cache = {}
        self._expiration = {}

    def __repr__(self):
        """Returns a representation of the cache.
        """
        return str(self._cache)

    @staticmethod
    def calculate_expiration(seconds):
        """Gets now and adds seconds to record expiration timestamp.
        """
        return datetime.now() + timedelta(seconds=seconds)

    def set(self, key, value, expires=None):
        """Sets the value onto the key with an optional expiration date.

        :param key: A key.
        :param value: The value.
        :param expires: Key expiration in seconds (optional).
        """
        self._cache[key] = value
        if expires:
            self._expiration[key] = self.calculate_expiration(expires)

        return True

    def remove(self, key):
        """Removes key from cache.

        :param key: The key.
        """
        if key in self._expiration:
            del self._expiration[key]

        try:
            del self._cache[key]
        except KeyError:
            return False

        return True

    def get(self, key):
        """Gets a key if available and if it has expiration,
        only if it hasn't been expired.

        :param key: A key string.
        """
        if key in self._expiration:
            if self._expiration[key] < datetime.now():
                self.remove(key)
                return

        return self._cache.get(key, None)
