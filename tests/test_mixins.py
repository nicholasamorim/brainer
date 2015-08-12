# -*- coding: utf8 -*-
import json

import umsgpack
from twisted.trial import unittest

from brainer.lib.mixins import SerializerMixin


class AClassToBeMixed(object):
    pass


class MyClass(AClassToBeMixed, SerializerMixin):
    _serializer = umsgpack


class SerializerMixinTest(unittest.TestCase):
    def setUp(self):
        self.mixin = SerializerMixin()

    def test_pack_no_serializer(self):
        self.assertRaises(AttributeError, self.mixin.pack, 'data')

    def test_unpack_no_serializer(self):
        self.assertRaises(AttributeError, self.mixin.unpack, 'data')

    def _test_pack(self, data, serializer):
        obj = MyClass()
        obj._serializer = serializer
        packed = obj.pack(data)
        self.assertEqual(packed, serializer.dumps(data))

    def _test_unpack(self, data, serializer):
        obj = MyClass()
        obj._serializer = serializer
        packed = serializer.dumps(data)
        self.assertEqual(obj.unpack(packed), data)

    def test_pack_msgpack(self):
        self._test_pack({'test': 123}, umsgpack)

    def test_pack_json(self):
        self._test_pack({'test': 123}, json)

    def test_unpack_msgpack(self):
        self._test_unpack({'test': 123}, umsgpack)

    def test_unpack_json(self):
        self._test_unpack({'test': 123}, json)
