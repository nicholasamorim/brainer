# -*- coding: utf8 -*-
from datetime import datetime, timedelta

from mock import patch
from twisted.trial import unittest

from brainer.lib.cache import InMemoryCache


class InMemoryCacheTest(unittest.TestCase):
    def setUp(self):
        self.cache = InMemoryCache()

    def test_repr(self):
        self.assertEqual(self.cache.__repr__(), '{}')

        self.cache.set('test', None)

        self.assertEqual(self.cache.__repr__(), "{'test': None}")

    def test_set(self):
        self.assertRaises(TypeError, self.cache.set, [], 1)
        self.cache.set('test', {'a dict': 'to test'})
        self.assertIn('test', self.cache._cache)
        self.assertEqual(self.cache._cache['test'], {'a dict': 'to test'})

    def test_set_and_get_with_expiration(self):
        self.cache.set('will_be_expired', True, 10)
        my_value = self.cache.get('will_be_expired')
        self.assertEqual(my_value, True)
        with patch('brainer.lib.cache.datetime') as mock_datetime:
            # It's 2 hours later now.
            mock_datetime.now.return_value = (
                datetime.now() + timedelta(hours=2))

            my_value = self.cache.get('will_be_expired')
            self.assertEqual(my_value, None)

    def test_remove(self):
        self.cache.set('test', 1)
        self.assertEqual(self.cache.remove('test'), True)
        self.assertEqual(self.cache.remove('test'), False)

    def test_get(self):
        self.assertEqual(self.cache.get(None), None)
        self.assertRaises(TypeError, self.cache.get, [])

        self.cache.set('test', [1, 2, 3])
        self.assertEqual(
            self.cache.get('test'), [1, 2, 3])
